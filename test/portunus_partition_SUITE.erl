%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_partition_SUITE).

%% Jepsen-style tests: real network partitions, not just stopped servers.
%%
%% Members run on peer nodes whose Erlang distribution is the
%% `inet_tcp_proxy` proxy, so a pair of nodes can be cut off from each
%% other at runtime (`inet_tcp_proxy_dist:block/1`) and reconnected
%% (`allow/1`). Because a blocked pair keeps its TCP socket open but drops
%% traffic, Erlang only notices after `net_ticktime`; Ra reacts far sooner
%% on its own election timers, which is what these tests observe.
%%
%% The proxy is part of the RabbitMQ source tree, not a hex dependency, so
%% the suite finds its ebin via the sibling checkout (or the
%% INET_TCP_PROXY_EBIN environment variable) and skips when it is absent.
%%
%% A proxied peer cannot speak the plain distribution the test controller
%% uses, so peers are controlled over stdio and driven with `peer:call/4,5`;
%% the controller never joins their mesh.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([minority_isolated_preserves_single_owner/1,
         leader_isolated_majority_elects_new_leader/1,
         isolated_holder_retains_lock/1]).

%% Run on the peer nodes.
-export([holder_loop/0, holder_call/1]).

-define(SYS, portunus).
-define(NAME, portunus_partition_test).
-define(TICK_MS, 200).
-define(HOLDER, portunus_partition_holder).
-define(COOKIE, "portunus_proxy_test").
-define(RETRIES, 200).

all() ->
    [minority_isolated_preserves_single_owner,
     leader_isolated_majority_elects_new_leader,
     isolated_holder_retains_lock].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok ->
            case proxy_ebin() of
                {ok, Ebin} ->
                    [{proxy_ebin, Ebin} | Config];
                error ->
                    {skip, "inet_tcp_proxy ebin not found; set INET_TCP_PROXY_EBIN"}
            end;
        Skip ->
            Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    [{cluster, setup_cluster(Config, 3)} | Config].

end_per_testcase(_TC, Config) ->
    case ?config(cluster, Config) of
        #{peers := Peers} ->
            _ = [catch peer:stop(P) || {P, _} <- Peers],
            ok;
        _ ->
            ok
    end.

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

minority_isolated_preserves_single_owner(Config) ->
    #{peers := Peers, nodes := Nodes} = ?config(cluster, Config),
    Key = {res, minority},
    {?NAME, LeaderNode} = wait_leader(Peers),
    %% Isolate a follower, so the leader stays with the majority.
    [Minority | _] = Nodes -- [LeaderNode],
    Majority = Nodes -- [Minority],
    MajPeers = pairs(Peers, Majority),
    %% Take the lock from a majority node before the split.
    HolderPeer = peer_of(Peers, LeaderNode),
    ok = start_holder(HolderPeer),
    {ok, Lease} = hgrant(HolderPeer, 60000),
    {ok, Token} = hacquire(HolderPeer, Key, Lease, owner_a),
    partition(Peers, [Minority], Majority),
    %% The majority keeps a leader and the lock, untouched.
    {?NAME, MajLeader} = wait_leader(MajPeers),
    ?assert(lists:member(MajLeader, Majority)),
    await_owner(MajPeers, Key, owner_a, Token),
    %% The isolated minority has no quorum, so a contender there cannot
    %% mint a lease, let alone take the key: no split-brain second owner.
    MinPeer = peer_of(Peers, Minority),
    ?assertEqual({error, no_quorum},
                 peer:call(MinPeer, portunus, grant_lease, [?NAME, 60000], 30000)),
    %% Heal: the minority rejoins and the cluster reconverges on the one
    %% owner that was there all along.
    heal(Peers, [Minority], Majority),
    {?NAME, _} = wait_leader(Peers),
    await_owner(Peers, Key, owner_a, Token).

leader_isolated_majority_elects_new_leader(Config) ->
    #{peers := Peers, nodes := Nodes} = ?config(cluster, Config),
    Key = {res, leader_iso},
    {?NAME, OldLeader} = wait_leader(Peers),
    Majority = Nodes -- [OldLeader],
    MajPeers = pairs(Peers, Majority),
    %% Hold the lock from a node that will end up in the majority.
    HolderPeer = peer_of(Peers, hd(Majority)),
    ok = start_holder(HolderPeer),
    {ok, Lease} = hgrant(HolderPeer, 60000),
    {ok, Token} = hacquire(HolderPeer, Key, Lease, owner_a),
    partition(Peers, [OldLeader], Majority),
    %% The majority elects a fresh leader and keeps the lock.
    {?NAME, NewLeader} = wait_leader(MajPeers),
    ?assert(lists:member(NewLeader, Majority)),
    ?assertNotEqual(OldLeader, NewLeader),
    await_owner(MajPeers, Key, owner_a, Token),
    %% The majority still serves writes (liveness on the live side).
    {ok, L2} = hgrant(HolderPeer, 60000),
    ?assertMatch({ok, _}, hacquire(HolderPeer, {res, leader_iso2}, L2, owner_a)),
    %% The isolated old leader cannot commit anything.
    OldPeer = peer_of(Peers, OldLeader),
    ?assertEqual({error, no_quorum},
                 peer:call(OldPeer, portunus, grant_lease, [?NAME, 60000], 30000)),
    %% Heal: the old leader rejoins as a follower and the single owner holds.
    heal(Peers, [OldLeader], Majority),
    {?NAME, _} = wait_leader(Peers),
    await_owner(Peers, Key, owner_a, Token).

isolated_holder_retains_lock(Config) ->
    #{peers := Peers, nodes := Nodes} = ?config(cluster, Config),
    Key = {res, isolated_holder},
    {?NAME, LeaderNode} = wait_leader(Peers),
    %% Hold from a follower, then isolate that follower into the minority.
    [HolderNode | _] = Nodes -- [LeaderNode],
    Majority = Nodes -- [HolderNode],
    MajPeers = pairs(Peers, Majority),
    HolderPeer = peer_of(Peers, HolderNode),
    ok = start_holder(HolderPeer),
    {ok, Lease} = hgrant(HolderPeer, 60000),
    {ok, Token} = hacquire(HolderPeer, Key, Lease, owner_a),
    partition(Peers, [HolderNode], Majority),
    {?NAME, _} = wait_leader(MajPeers),
    %% The holder is unreachable, not dead, so the majority keeps the lock
    %% rather than reclaiming it, through the split and after it heals.
    await_owner(MajPeers, Key, owner_a, Token),
    timer:sleep(1500),
    await_owner(MajPeers, Key, owner_a, Token),
    heal(Peers, [HolderNode], Majority),
    {?NAME, _} = wait_leader(Peers),
    await_owner(Peers, Key, owner_a, Token).

%%----------------------------------------------------------------------
%% Cluster setup (proxied peers, controlled over stdio)
%%----------------------------------------------------------------------

setup_cluster(Config, N) ->
    Ebin = ?config(proxy_ebin, Config),
    PrivDir = ?config(priv_dir, Config),
    Args = ["-proto_dist", "inet_tcp_proxy",
            "-setcookie", ?COOKIE,
            %% Keep Erlang's own node-down detection brisk; Ra reacts
            %% sooner, but this avoids minute-long stale connections.
            "-kernel", "net_ticktime", "6",
            "-pa", Ebin | code:get_path()],
    Peers = [begin
                 {ok, Peer, Node} =
                     peer:start_link(#{name => peer:random_name("portunus_part"),
                                       connection => standard_io,
                                       %% The default 15s boot wait is too
                                       %% tight under parallel CI.
                                       wait_boot => 60000,
                                       args => Args}),
                 {Peer, Node}
             end || _ <- lists:seq(1, N)],
    Nodes = [Node || {_, Node} <- Peers],
    lists:foreach(
      fun({Peer, Node}) ->
              {ok, _} = peer:call(Peer, application, ensure_all_started,
                                  [inet_tcp_proxy_dist]),
              _ = peer:call(Peer, application, load, [portunus]),
              ok = peer:call(Peer, application, set_env,
                             [portunus, tick_interval_ms, ?TICK_MS]),
              DataDir = filename:join([PrivDir, atom_to_list(Node)]),
              ok = peer:call(Peer, portunus, start_system, [?SYS, DataDir])
      end, Peers),
    _ = [pong = peer:call(P, net_adm, ping, [Other])
         || {P, Self} <- Peers, Other <- Nodes, Other =/= Self],
    {P1, _} = hd(Peers),
    {ok, _, _} = peer:call(P1, portunus, start_cluster, [?SYS, ?NAME, Nodes]),
    {?NAME, _} = wait_leader(Peers),
    #{peers => Peers, nodes => Nodes}.

%%----------------------------------------------------------------------
%% Partition control
%%----------------------------------------------------------------------

partition(Peers, GroupA, GroupB) ->
    each_cross(Peers, GroupA, GroupB,
               fun(Pa, A, Pb, B) ->
                       ok = peer:call(Pa, inet_tcp_proxy_dist, block, [B]),
                       ok = peer:call(Pb, inet_tcp_proxy_dist, block, [A])
               end).

heal(Peers, GroupA, GroupB) ->
    each_cross(Peers, GroupA, GroupB,
               fun(Pa, A, Pb, B) ->
                       ok = peer:call(Pa, inet_tcp_proxy_dist, allow, [B]),
                       ok = peer:call(Pb, inet_tcp_proxy_dist, allow, [A]),
                       %% Nudge Erlang to reconnect rather than wait for traffic.
                       _ = peer:call(Pa, net_kernel, connect_node, [B], 10000)
               end).

each_cross(Peers, GroupA, GroupB, Fun) ->
    _ = [Fun(peer_of(Peers, A), A, peer_of(Peers, B), B)
         || A <- GroupA, B <- GroupB],
    ok.

%%----------------------------------------------------------------------
%% A long-lived lock holder, registered on its peer node so its pid (the
%% lease owner the machine monitors) survives across `peer:call/4` hops.
%%----------------------------------------------------------------------

start_holder(Peer) ->
    ok = peer:call(Peer, ?MODULE, holder_call, [start]).

holder_call(start) ->
    case whereis(?HOLDER) of
        undefined ->
            register(?HOLDER, spawn(fun holder_loop/0)),
            ok;
        _ ->
            ok
    end;
holder_call({F, A}) ->
    Pid = whereis(?HOLDER),
    Pid ! {call, self(), F, A},
    receive
        {Pid, Reply} -> Reply
    after 25000 ->
        {error, holder_timeout}
    end.

holder_loop() ->
    receive
        {call, From, F, A} ->
            From ! {self(), apply(portunus, F, A)},
            holder_loop();
        stop ->
            ok
    end.

%% grant/acquire on the holder, retrying a transient loss of leader the
%% way a real client would.
hgrant(Peer, TtlMs) ->
    until_quorum(Peer, grant_lease, [?NAME, TtlMs]).

hacquire(Peer, Key, Lease, Owner) ->
    until_quorum(Peer, acquire, [?NAME, Key, Lease, Owner]).

until_quorum(Peer, F, A) ->
    until_quorum(Peer, F, A, ?RETRIES).

until_quorum(Peer, F, A, 0) ->
    hcall(Peer, F, A);
until_quorum(Peer, F, A, N) ->
    case hcall(Peer, F, A) of
        {error, no_quorum} -> timer:sleep(100), until_quorum(Peer, F, A, N - 1);
        Reply -> Reply
    end.

hcall(Peer, F, A) ->
    peer:call(Peer, ?MODULE, holder_call, [{F, A}], 30000).

%%----------------------------------------------------------------------
%% Introspection and waiting
%%----------------------------------------------------------------------

peer_of(Peers, Node) ->
    {Peer, Node} = lists:keyfind(Node, 2, Peers),
    Peer.

pairs(Peers, Nodes) ->
    [lists:keyfind(N, 2, Peers) || N <- Nodes].

%% Wait until every peer in the group agrees on one leader that is itself
%% in the group.
wait_leader(Pairs) ->
    wait_leader(Pairs, ?RETRIES).

wait_leader(Pairs, 0) ->
    ct:fail({no_leader, [N || {_, N} <- Pairs]});
wait_leader(Pairs, N) ->
    Nodes = [Node || {_, Node} <- Pairs],
    Views = [peer:call(P, ra_leaderboard, lookup_leader, [?NAME], 10000)
             || {P, _} <- Pairs],
    case lists:usort(Views) of
        [{?NAME, LeaderNode} = Leader] ->
            case lists:member(LeaderNode, Nodes) of
                true -> Leader;
                false -> timer:sleep(100), wait_leader(Pairs, N - 1)
            end;
        _ ->
            timer:sleep(100),
            wait_leader(Pairs, N - 1)
    end.

await_owner(Pairs, Key, Owner, Token) ->
    {Peer, _} = hd(Pairs),
    ok = wait_until(
           fun() ->
                   case peer:call(Peer, portunus, owner, [?NAME, Key], 30000) of
                       {ok, #{owner := Owner, token := Token}} -> true;
                       _ -> false
                   end
           end).

wait_until(Fun) ->
    wait_until(Fun, ?RETRIES).

wait_until(_Fun, 0) ->
    ct:fail(timeout);
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        _ -> timer:sleep(100), wait_until(Fun, N - 1)
    end.

%%----------------------------------------------------------------------
%% Locating the inet_tcp_proxy ebin
%%----------------------------------------------------------------------

proxy_ebin() ->
    Candidates = [os:getenv("INET_TCP_PROXY_EBIN") | sibling_guesses()],
    case [D || D <- Candidates, is_list(D), filelib:is_dir(D)] of
        [Dir | _] -> {ok, Dir};
        [] -> error
    end.

%% The proxy lives in a sibling rabbitmq checkout, at an offset that
%% depends on the build tool's layout. Rather than hard-code it, try the
%% sibling path at every ancestor of the app dir and the working dir.
sibling_guesses() ->
    Tail = ["main.git", "deps", "inet_tcp_proxy", "ebin"],
    Bases = [B || B <- [safe_lib_dir(), safe_cwd()], B =/= undefined],
    [filename:join([Ancestor | Tail])
     || Base <- Bases, Ancestor <- ancestors(Base, 8)].

safe_lib_dir() ->
    case code:lib_dir(portunus) of
        {error, _} -> undefined;
        Dir -> Dir
    end.

safe_cwd() ->
    case file:get_cwd() of
        {ok, Dir} -> Dir;
        _ -> undefined
    end.

ancestors(Dir, 0) ->
    [Dir];
ancestors(Dir, N) ->
    case filename:dirname(Dir) of
        Dir -> [Dir];
        Parent -> [Dir | ancestors(Parent, N - 1)]
    end.
