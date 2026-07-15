%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_empty_log_recovery_multinode_SUITE).

%% A replica whose log carries no cluster configuration falls back to its
%% persisted `initial_members`, which `join_or_form/3` always writes as `[Self]`.
%% The seed would then elect itself with quorum 1, without an RPC leaving the
%% node, minting a term against a live leader. These tests drive the seed into
%% that state and assert it asks the other members before electing.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
%% Run on the peer under test.
-export([restart_and_converge/2, sample_state/2]).

-export([configless_seed_joins_rather_than_forming_a_rival/1,
         configless_seed_does_not_form_a_rival_cluster/1,
         all_configless_cluster_re_forms/1,
         removed_seed_rejoins_and_does_not_elect/1,
         configless_seed_with_a_multi_member_view_does_not_elect/1,
         leaderless_cluster_is_not_formed_against/1]).

-define(SYS, portunus).
-define(NAME, portunus_empty_log_recovery_multinode_test).
-define(TTL, 60000).
-define(SIZE, 3).
-define(RETRIES, 100).

all() ->
    [configless_seed_joins_rather_than_forming_a_rival,
     configless_seed_does_not_form_a_rival_cluster,
     all_configless_cluster_re_forms,
     removed_seed_rejoins_and_does_not_elect,
     configless_seed_with_a_multi_member_view_does_not_elect,
     leaderless_cluster_is_not_formed_against].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    Peers = [portunus_ct_cluster:start_node(Config, #{}) || _ <- lists:seq(1, ?SIZE)],
    Nodes = [Node || {_, Node} <- Peers],
    portunus_ct_cluster:mesh(Nodes),
    [{cluster, #{peers => Peers, nodes => Nodes}} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

%% The seed is configless while the other two run a real cluster that does not
%% list it. Electing here is the split brain: the seed's own view is `[Self]`, so
%% quorum is 1 and it wins unopposed. It must read the peers' views and join.
configless_seed_joins_rather_than_forming_a_rival(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    [Seed | Others] = lists:sort(Nodes),
    {ok, _, _} = rpc:call(hd(Others), portunus, start_cluster, [?SYS, ?NAME, Others]),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Others, ?NAME),
    Token = portunus_ct_cluster:place_lock(hd(Others), ?NAME, {res, hold}),
    ok = form_without_election(Seed),
    ?assertEqual(configless, local_state(Seed)),
    ok = rpc:call(Seed, portunus, join_or_form, [?SYS, ?NAME, Nodes]),
    ok = await_members_everywhere(Nodes, ?SIZE, ?RETRIES),
    %% Every node agreeing on one leader is what a rival would break.
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 rpc:call(Seed, portunus, owner, [?NAME, {res, hold}])).

%% The reported failure's shape: the seed is a member of a real cluster and its
%% log is emptied under it. The other two still list it, so it must wait for the
%% leader to replicate rather than elect.
configless_seed_does_not_form_a_rival_cluster(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Seed = hd(lists:sort(Nodes)),
    ok = portunus_ct_cluster:converge_all(Nodes, Nodes, ?NAME),
    {?NAME, Leader} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    Token = portunus_ct_cluster:place_lock(Leader, ?NAME, {res, hold}),
    Dir = empty_the_log(Config, Seed),
    never_leader(
      Seed,
      fun() ->
              %% The system restart and the converge run in one call on the node
              %% itself: the leader replicates the log back within a heartbeat,
              %% and every pass after that reads a full log and never reaches the
              %% branch under test.
              ok = rpc:call(Seed, ?MODULE, restart_and_converge, [Dir, Nodes]),
              ok = await_members_everywhere(Nodes, ?SIZE, ?RETRIES)
      end),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    %% The entries come back by replication, which is what a healthy member does
    %% instead of electing.
    ok = await_condition(
           fun() ->
                   case rpc:call(Seed, ra, key_metrics, [{?NAME, Seed}], 5000) of
                       #{last_index := I} -> I > 0;
                       _ -> false
                   end
           end, ?RETRIES),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 rpc:call(Seed, portunus, owner, [?NAME, {res, hold}])).

%% Every node configless: no peer reports a cluster, so the seed must elect and
%% the rest join it. Ra arms no election timeout for a server with an empty log,
%% so nothing but portunus's own trigger can start this cluster: a probe that
%% declined to elect on an unclear answer would wedge here forever.
all_configless_cluster_re_forms(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    _ = [ok = form_without_election(N) || N <- Nodes],
    _ = [?assertEqual(configless, local_state(N)) || N <- Nodes],
    ok = portunus_ct_cluster:converge_all(Nodes, Nodes, ?NAME),
    ok = await_members_everywhere(Nodes, ?SIZE, ?RETRIES),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    ?assertMatch({ok, _}, rpc:call(hd(Nodes), portunus, grant_lease, [?NAME, ?TTL])).

%% A removed seed's log is intact and says it is not a member: committed
%% knowledge, not the ignorance an empty log means. With no peer reachable it
%% must report an error the caller retries, not the `ok` a no-op election would
%% produce.
removed_seed_rejoins_and_does_not_elect(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    [Seed | Others] = lists:sort(Nodes),
    ok = portunus_ct_cluster:converge_all(Nodes, Nodes, ?NAME),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    ok = rpc:call(hd(Others), portunus, remove_member, [?NAME, Seed]),
    ok = await_condition(
           fun() -> not lists:member({?NAME, Seed}, local_members(Seed)) end,
           ?RETRIES),
    %% The seed may have been the leader when it removed itself, and a stopped
    %% cluster cannot depose it. Restart its replica so it recovers as a follower
    %% carrying the committed removal, which is the state under test.
    ok = portunus_ct_cluster:stop_ra_server(Seed, ?NAME),
    ok = portunus_ct_cluster:restart_ra_server(Seed, ?NAME),
    ?assertNotEqual(leader, state(Seed)),
    %% No peer reachable: the seed is alone with a log that says it is out.
    _ = [ok = portunus_ct_cluster:stop_ra_server(N, ?NAME) || N <- Others],
    never_leader(
      Seed,
      fun() ->
              ?assertNotEqual(ok, rpc:call(Seed, portunus, join_or_form,
                                           [?SYS, ?NAME, Nodes]))
      end).

%% A server created as one of an N-node cluster persists all N in
%% `initial_members`, so an emptied log leaves an N-member view it cannot reach
%% quorum in. Ra's own vote counting refuses here, not portunus's guard: the
%% coordinated-start path was never exposed.
configless_seed_with_a_multi_member_view_does_not_elect(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Seed = hd(lists:sort(Nodes)),
    Members = [{?NAME, N} || N <- lists:sort(Nodes)],
    ok = start_server_with_members(Seed, Members),
    ?assertEqual(Members, lists:sort(local_members(Seed))),
    ?assertEqual(0, last_index(Seed)),
    never_leader(
      Seed,
      fun() ->
              ok = rpc:call(Seed, portunus, join_or_form, [?SYS, ?NAME, Nodes])
      end).

%% A cluster without a leader still answers a local query, so the seed must see it
%% and not form a rival. A leader-redirected probe times out exactly here, when no
%% leader is what is being decided, and reads a live cluster as an absent one.
leaderless_cluster_is_not_formed_against(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    [Seed | Others] = lists:sort(Nodes),
    {ok, _, _} = rpc:call(hd(Others), portunus, start_cluster, [?SYS, ?NAME, Others]),
    {?NAME, Leader} = portunus_ct_cluster:wait_leader(Others, ?NAME),
    %% Stop the leader: one of two members cannot elect, so the survivor is left
    %% campaigning with no leader while still reporting both members from its log.
    [Survivor] = Others -- [Leader],
    ok = portunus_ct_cluster:stop_ra_server(Leader, ?NAME),
    ok = await_condition(fun() -> leader_query_times_out(Survivor) end, ?RETRIES),
    ?assertMatch([_, _], local_members(Survivor)),
    %% The seed has no replica, so it takes the `name_not_registered` path.
    ?assertNotEqual(ok, rpc:call(Seed, portunus, join_or_form, [?SYS, ?NAME, Nodes])),
    %% Forming here is the split brain: the cluster exists and is merely leaderless.
    ?assertEqual([], local_members(Seed)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% The leader-redirected query, which is the one that cannot answer without a
%% leader. `ra_leaderboard` holds the last leader this node saw, so it reports a
%% stale pointer rather than the absence.
leader_query_times_out(Node) ->
    case rpc:call(Node, ra, members, [{?NAME, Node}, 1000]) of
        {ok, _, _} -> false;
        _ -> true
    end.

machine() ->
    {module, portunus_machine,
     #{cluster => ?NAME, tick_interval_ms => 1000, snapshot_interval => 4096}}.

%% Sole-member server without an election: registered, leaderless, empty log, and
%% `initial_members => [Self]`, which is what `join_or_form/3` persists on every
%% node it creates.
form_without_election(Node) ->
    start_server_with_members(Node, [{?NAME, Node}]).

start_server_with_members(Node, Members) ->
    rpc:call(Node, ra, start_server, [?SYS, ?NAME, {?NAME, Node}, machine(), Members]).

local_members(Node) ->
    case rpc:call(Node, ra, members, [{local, {?NAME, Node}}, 5000]) of
        {ok, Members, _} -> Members;
        _ -> []
    end.

last_index(Node) ->
    case rpc:call(Node, ra, key_metrics, [{?NAME, Node}], 5000) of
        #{last_index := I} -> I;
        _ -> undefined
    end.

%% The replica's own Raft state. `ra_leaderboard` holds the last leader this node
%% saw, so it reports a stale pointer rather than what this server is now.
state(Node) ->
    case rpc:call(Node, ra, key_metrics, [{?NAME, Node}], 5000) of
        #{state := S} -> S;
        _ -> undefined
    end.

%% The state the guard is about: registered, an empty log, and a view of itself
%% alone. Asserted as a precondition, since a green run is otherwise the only
%% evidence and a wrong reconstruction is exactly what produces one.
local_state(Node) ->
    case {last_index(Node), local_members(Node)} of
        {0, [{?NAME, Node}]} -> configless;
        Other -> Other
    end.

%% Delete this node's log while leaving the registration intact.
%% `ra_log_pre_init` decides registration from the `config` file inside the UID
%% directory, not from `names.dets`, so the directory and that file must stay:
%% clearing the directory wholesale unregisters the name and routes the node
%% through `form_or_join_existing/3` instead of the branch under test.
empty_the_log(Config, Node) ->
    Dir = portunus_ct_cluster:data_dir(Config, Node),
    UId = rpc:call(Node, ra_directory, uid_of, [?SYS, ?NAME]),
    true = is_binary(UId),
    ok = rpc:call(Node, ra_system, stop, [?SYS]),
    ServerDir = filename:join(Dir, binary_to_list(UId)),
    ?assert(filelib:is_regular(filename:join(ServerDir, "config"))),
    _ = [ok = file:delete(F)
         || F <- filelib:wildcard(filename:join(Dir, "*.wal"))
                ++ filelib:wildcard(filename:join(ServerDir, "*.segment"))
                ++ filelib:wildcard(filename:join([ServerDir, "snapshots", "*", "*"]))],
    ?assertEqual([], filelib:wildcard(filename:join(Dir, "*.wal"))),
    ?assertEqual([], filelib:wildcard(filename:join(ServerDir, "*.segment"))),
    ?assert(filelib:is_regular(filename:join(ServerDir, "config"))),
    Dir.

%% `start_system/2` recovers the replica, so `join_or_form/3` has to follow it
%% without a round trip in between.
restart_and_converge(Dir, Members) ->
    ok = portunus:start_system(?SYS, Dir),
    portunus:join_or_form(?SYS, ?NAME, Members).

%% Run `Fun` while sampling the replica's state. A rival is transient: Ra kills it
%% on the same-term collision and the supervisor restarts it, so it rejoins and a
%% state read afterwards shows nothing.
never_leader(Node, Fun) ->
    Sampler = start_sampler(Node),
    _ = Fun(),
    timer:sleep(1000),
    ?assertNot(lists:member(leader, stop_sampler(Sampler))).

%% Poll this replica's state on the node itself, so a leader that exists for a
%% few hundred milliseconds is still seen.
sample_state(ServerId, Acc) ->
    Acc1 = case catch ra:key_metrics(ServerId) of
               #{state := S} -> ordsets:add_element(S, Acc);
               _ -> Acc
           end,
    receive
        {stop, From} -> From ! {states, Acc1}
    after 5 ->
            sample_state(ServerId, Acc1)
    end.

start_sampler(Node) ->
    erlang:spawn(Node, ?MODULE, sample_state, [{?NAME, Node}, ordsets:new()]).

stop_sampler(Pid) ->
    Pid ! {stop, self()},
    receive {states, States} -> States
    after 5000 -> ct:fail(sampler_timed_out)
    end.

%% Every node's own view, not just the first that answers: a rival cluster is
%% only visible from the node that formed it.
await_members_everywhere(Nodes, N, Retries) ->
    await_condition(
      fun() -> lists:all(fun(Node) -> length(local_members(Node)) =:= N end, Nodes) end,
      Retries).

await_condition(_Fun, 0) ->
    ct:fail(condition_timed_out);
await_condition(Fun, Retries) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(100), await_condition(Fun, Retries - 1)
    end.

