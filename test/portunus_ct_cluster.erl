%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_ct_cluster).

%% Shared harness for the multi-node suites: bootstrap Erlang distribution,
%% start a cluster of peer nodes each running `portunus`, and the cluster
%% introspection, fault injection, and polling the suites have in common. It
%% follows `ra`'s `erlang_node_helpers` and `khepri`'s `test/helpers`: peers are
%% `peer` nodes carrying the runner's code path, each with its own data dir, and
%% the cluster is formed with `portunus:start_cluster/3` then waited on for a
%% leader.
%%
%% `ensure_distribution/0` starts distribution in-process when the runner is not
%% already a distributed node, so the suites run under plain `rebar3 ct`, not
%% only under `gmake tests` (which names the runner via `-sname`). The proxied
%% partition suite keeps its own setup, since its controller drives peers over
%% stdio rather than joining their mesh.

-include_lib("common_test/include/ct.hrl").

-export([ensure_distribution/0,
         start/3, start/4, stop/1,
         start_node/2, mesh/1,
         wait_leader/2, cluster_info/2, member_count/2,
         stop_ra_server/2, restart_ra_server/2,
         start_client/1, ccall/3, until_quorum/3, until_quorum/4,
         papi/3,
         await_owner/3, await_owner/4, await_owner/5, await_released/3,
         wait_until/1, wait_until/2]).
%% Spawned on the peer nodes, so it must be exported.
-export([client_loop/0]).

-define(SYS, portunus).
-define(TICK_MS, 200).
%% Each retry waits 100ms, so this bounds a wait at ~15s, generous since an
%% ownership transfer and re-election take a second or two.
-define(RETRIES, 150).

%%----------------------------------------------------------------------
%% Distribution bootstrap
%%----------------------------------------------------------------------

%% Make the runner a distributed node if it is not one already, so peers can
%% connect. Returns `{skip, Reason}` rather than crashing when distribution
%% cannot be brought up, so a suite degrades to a skip.
-spec ensure_distribution() -> ok | {skip, string()}.
ensure_distribution() ->
    case is_alive() of
        true ->
            ok;
        false ->
            _ = start_epmd(),
            case net_kernel:start(portunus_ct, #{name_domain => shortnames}) of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok;
                {error, Reason} ->
                    {skip, lists:flatten(
                             io_lib:format("could not start distribution: ~p",
                                           [Reason]))}
            end
    end.

start_epmd() ->
    Erts = "erts-" ++ erlang:system_info(version),
    Epmd0 = filename:join([code:root_dir(), Erts, "bin", "epmd"]),
    Epmd = case os:type() of
               {win32, _} -> Epmd0 ++ ".exe";
               _ -> Epmd0
           end,
    Port = open_port({spawn_executable, Epmd}, [{args, ["-daemon"]}]),
    true = port_close(Port),
    ok.

%%----------------------------------------------------------------------
%% Cluster lifecycle
%%----------------------------------------------------------------------

%% Start an `N`-node portunus cluster named `Name`; returns
%% `#{peers => [{Peer, Node}], nodes => [Node]}` for the suite to keep in its
%% config and hand back to `stop/1`.
-spec start([{atom(), term()}], atom(), pos_integer()) -> map().
start(Config, Name, N) ->
    start(Config, Name, N, #{}).

-spec start([{atom(), term()}], atom(), pos_integer(), map()) -> map().
start(Config, Name, N, Opts) ->
    Peers = [start_node(Config, Opts) || _ <- lists:seq(1, N)],
    Nodes = [Node || {_, Node} <- Peers],
    mesh(Nodes),
    {ok, _, _} = rpc:call(hd(Nodes), portunus, start_cluster, [?SYS, Name, Nodes]),
    {Name, _} = wait_leader(Nodes, Name),
    #{peers => Peers, nodes => Nodes}.

-spec stop(map()) -> ok.
stop(#{peers := Peers}) ->
    _ = [catch peer:stop(P) || {P, _} <- Peers],
    ok;
stop(_) ->
    ok.

%% Start one peer node carrying the runner's code path, with its own data dir,
%% portunus loaded and its system started. `Opts` may set `name_prefix`,
%% `tick_ms`, and `env` (extra portunus application env pairs).
-spec start_node([{atom(), term()}], map()) -> {peer:server_ref(), node()}.
start_node(Config, Opts) ->
    PrivDir = ?config(priv_dir, Config),
    Prefix = maps:get(name_prefix, Opts, "portunus_node"),
    TickMs = maps:get(tick_ms, Opts, ?TICK_MS),
    {ok, Peer, Node} =
        ?CT_PEER(#{name => peer:random_name(Prefix),
                   %% The default 15s boot wait is too tight under parallel CI.
                   wait_boot => 60000,
                   args => ["-pa" | code:get_path()]}),
    DataDir = filename:join([PrivDir, atom_to_list(Node)]),
    _ = rpc:call(Node, application, load, [portunus]),
    ok = rpc:call(Node, application, set_env, [portunus, tick_interval_ms, TickMs]),
    _ = [ok = rpc:call(Node, application, set_env, [portunus, K, V])
         || {K, V} <- maps:get(env, Opts, [])],
    ok = rpc:call(Node, portunus, start_system, [?SYS, DataDir]),
    {Peer, Node}.

mesh(Nodes) ->
    _ = [[pong = rpc:call(A, net_adm, ping, [B]) || B <- Nodes, B =/= A]
         || A <- Nodes],
    ok.

%%----------------------------------------------------------------------
%% Fault injection (run on the owning node, which itself stays up)
%%----------------------------------------------------------------------

-spec stop_ra_server(node(), atom()) -> ok.
stop_ra_server(Node, Name) ->
    ok = rpc:call(Node, ra, stop_server, [?SYS, {Name, Node}]).

-spec restart_ra_server(node(), atom()) -> ok.
restart_ra_server(Node, Name) ->
    ok = rpc:call(Node, ra, restart_server, [?SYS, {Name, Node}]).

%%----------------------------------------------------------------------
%% A long-lived client process on a member node. Its pid is the lease holder
%% the machine monitors, so it must outlive individual API calls: the transient
%% process an `rpc:call` would use dies immediately and would drop the lease.
%%----------------------------------------------------------------------

-spec start_client(node()) -> pid().
start_client(Node) ->
    erlang:spawn(Node, ?MODULE, client_loop, []).

client_loop() ->
    receive
        {call, From, F, A} ->
            From ! {self(), apply(portunus, F, A)},
            client_loop();
        stop ->
            ok
    end.

-spec ccall(pid(), atom(), [term()]) -> term().
ccall(Pid, F, A) ->
    Pid ! {call, self(), F, A},
    receive
        {Pid, Reply} -> Reply
    after 15000 ->
        error({client_timeout, F})
    end.

%% Run a client call, retrying a transient loss of leader the way a real client
%% would, so a re-election mid-call does not fail the test.
-spec until_quorum(pid(), atom(), [term()]) -> term().
until_quorum(Pid, F, A) ->
    until_quorum(Pid, F, A, ?RETRIES).

-spec until_quorum(pid(), atom(), [term()], non_neg_integer()) -> term().
until_quorum(Pid, F, A, 0) ->
    ccall(Pid, F, A);
until_quorum(Pid, F, A, N) ->
    case ccall(Pid, F, A) of
        {error, no_quorum} -> timer:sleep(100), until_quorum(Pid, F, A, N - 1);
        Reply -> Reply
    end.

%% A stateless portunus query, run via a transient process on a member node.
-spec papi(node(), atom(), [term()]) -> term().
papi(Node, F, A) ->
    rpc:call(Node, portunus, F, A).

%%----------------------------------------------------------------------
%% Cluster introspection and waiting
%%----------------------------------------------------------------------

%% Members and leader, asking each node until one answers.
-spec cluster_info([node()], atom()) -> {[portunus:server_id()], portunus:server_id()}.
cluster_info(Nodes, Name) ->
    cluster_info(Nodes, Nodes, Name).

cluster_info([], All, _Name) ->
    ct:fail({no_member_responded, All});
cluster_info([Node | Rest], All, Name) ->
    case ra:members({Name, Node}, 5000) of
        {ok, Members, Leader} -> {Members, Leader};
        _ -> cluster_info(Rest, All, Name)
    end.

-spec member_count([node()], atom()) -> non_neg_integer().
member_count(Nodes, Name) ->
    case catch cluster_info(Nodes, Name) of
        {Members, _} -> length(Members);
        _ -> 0
    end.

%% Wait until every node in `Nodes` reports the same leader and that leader is
%% one of them. Insisting on agreement (not just "someone saw a leader") means a
%% follower's stale pointer to a downed leader cannot satisfy us, and that any of
%% these nodes will route a command correctly afterwards.
-spec wait_leader([node()], atom()) -> portunus:server_id().
wait_leader(Nodes, Name) ->
    wait_leader(Nodes, Name, ?RETRIES).

wait_leader(Nodes, _Name, 0) ->
    ct:fail({no_leader, Nodes});
wait_leader(Nodes, Name, N) ->
    Views = [rpc:call(Node, ra_leaderboard, lookup_leader, [Name])
             || Node <- Nodes],
    case lists:usort(Views) of
        [{Name, LeaderNode} = Leader] ->
            case lists:member(LeaderNode, Nodes) of
                true -> Leader;
                false -> timer:sleep(100), wait_leader(Nodes, Name, N - 1)
            end;
        _ ->
            timer:sleep(100),
            wait_leader(Nodes, Name, N - 1)
    end.

%% Poll the owner query on `Node` until it sees `Owner` (the read can briefly
%% fail or lag right after a re-election).
-spec await_owner(node(), atom(), term()) -> ok.
await_owner(Node, Name, Key) ->
    wait_until(fun() ->
                       case papi(Node, owner, [Name, Key]) of
                           {ok, #{owner := _}} -> true;
                           _ -> false
                       end
               end).

-spec await_owner(node(), atom(), term(), term()) -> ok.
await_owner(Node, Name, Key, Owner) ->
    wait_until(fun() ->
                       case papi(Node, owner, [Name, Key]) of
                           {ok, #{owner := Owner}} -> true;
                           _ -> false
                       end
               end).

-spec await_owner(node(), atom(), term(), term(), portunus:token()) -> ok.
await_owner(Node, Name, Key, Owner, Token) ->
    wait_until(fun() ->
                       case papi(Node, owner, [Name, Key]) of
                           {ok, #{owner := Owner, token := Token}} -> true;
                           _ -> false
                       end
               end).

-spec await_released(node(), atom(), term()) -> ok.
await_released(Node, Name, Key) ->
    wait_until(fun() -> papi(Node, owner, [Name, Key]) =:= {error, not_held} end).

-spec wait_until(fun(() -> boolean())) -> ok.
wait_until(Fun) ->
    wait_until(Fun, ?RETRIES).

-spec wait_until(fun(() -> boolean()), non_neg_integer()) -> ok.
wait_until(_Fun, 0) ->
    ct:fail(timeout);
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        _ -> timer:sleep(100), wait_until(Fun, N - 1)
    end.
