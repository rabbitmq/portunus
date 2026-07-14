%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_cluster_formation_multinode_SUITE).

%% Multi-node coverage of the merge in `join_or_form/3`: clusters that formed
%% without the seed converge on it, and query-before-wipe never resets a replica
%% that still contains the seed.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([merges_split_single_member_clusters/1,
         merges_node_whose_replica_had_stopped/1,
         unreachable_seed_does_not_wipe_replica/1,
         merges_once_seed_becomes_reachable/1,
         merges_multi_member_cluster_excluding_seed/1,
         established_member_is_never_reset/1,
         repeated_convergence_does_not_thrash/1]).

-define(SYS, portunus).
-define(NAME, portunus_cluster_formation_multinode_test).
-define(TTL, 60000).
-define(SIZE, 3).
-define(RETRIES, 100).

all() ->
    [merges_split_single_member_clusters,
     merges_node_whose_replica_had_stopped,
     unreachable_seed_does_not_wipe_replica,
     merges_once_seed_becomes_reachable,
     merges_multi_member_cluster_excluding_seed,
     established_member_is_never_reset,
     repeated_convergence_does_not_thrash].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    %% Peers with their Ra systems up but no cluster formed: each test drives
    %% formation and convergence itself.
    Peers = [portunus_ct_cluster:start_node(Config, #{}) || _ <- lists:seq(1, ?SIZE)],
    Nodes = [Node || {_, Node} <- Peers],
    portunus_ct_cluster:mesh(Nodes),
    [{cluster, #{peers => Peers, nodes => Nodes}} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

%% Each node forms its own single-member cluster, then all converge into one.
merges_split_single_member_clusters(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    ok = solo_form_all(Nodes),
    ok = converge_all(Nodes, ?RETRIES),
    ?assertEqual(?SIZE, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    {?NAME, LeaderNode} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    ?assert(lists:member(LeaderNode, Nodes)).

%% A non-seed's replica is stopped before convergence; it still merges into the
%% seed.
merges_node_whose_replica_had_stopped(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Down = hd(non_seed(Nodes)),
    ok = solo_form_all(Nodes),
    ok = portunus_ct_cluster:stop_ra_server(Down, ?NAME),
    ok = converge_all(Nodes, ?RETRIES),
    ?assertEqual(?SIZE, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME).

%% The seed's cluster is absent: convergence returns `{error, seed_unreachable}`
%% and leaves the replica and its lock intact.
unreachable_seed_does_not_wipe_replica(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    [NonSeed | _] = non_seed(Nodes),
    {ok, _, _} = rpc:call(NonSeed, portunus, start_cluster, [?SYS, ?NAME, [NonSeed]]),
    Token = place_lock(NonSeed, {res, keep}),
    ?assertEqual({error, seed_unreachable},
                 rpc:call(NonSeed, portunus, join_or_form, [?SYS, ?NAME, Nodes])),
    ?assert(rpc:call(NonSeed, portunus, is_member, [?NAME])),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 rpc:call(NonSeed, portunus, owner, [?NAME, {res, keep}])).

%% A non-seed whose seed has not formed yet retries with
%% `{error, seed_unreachable}` and merges once the seed appears.
merges_once_seed_becomes_reachable(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Seed = seed(Nodes),
    [NonSeed | _] = non_seed(Nodes),
    Pair = [Seed, NonSeed],
    {ok, _, _} = rpc:call(NonSeed, portunus, start_cluster, [?SYS, ?NAME, [NonSeed]]),
    ?assertEqual({error, seed_unreachable},
                 rpc:call(NonSeed, portunus, join_or_form, [?SYS, ?NAME, Pair])),
    {ok, _, _} = rpc:call(Seed, portunus, start_cluster, [?SYS, ?NAME, [Seed]]),
    ok = converge_all(Pair, ?RETRIES),
    ?assertEqual(2, portunus_ct_cluster:member_count(Pair, ?NAME)),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Pair, ?NAME).

%% Two non-seeds form a cluster without the seed; convergence merges it into the
%% seed's cluster: the seed's lock survives, the excluded cluster's is discarded.
merges_multi_member_cluster_excluding_seed(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Seed = seed(Nodes),
    [SubSeed, Other] = non_seed(Nodes),
    {ok, _, _} = rpc:call(SubSeed, portunus, start_cluster, [?SYS, ?NAME, [SubSeed]]),
    ok = rpc:call(Other, portunus, join_cluster, [?SYS, ?NAME, SubSeed]),
    {ok, _, _} = rpc:call(Seed, portunus, start_cluster, [?SYS, ?NAME, [Seed]]),
    SeedToken = place_lock(Seed, {res, hold}),
    _ = place_lock(SubSeed, {res, gone}),
    ok = converge_all(Nodes, ?RETRIES),
    ?assertEqual(?SIZE, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    ?assertMatch({ok, #{owner := owner_a, token := SeedToken}},
                 rpc:call(Seed, portunus, owner, [?NAME, {res, hold}])),
    ?assertEqual({error, not_held},
                 rpc:call(Seed, portunus, owner, [?NAME, {res, gone}])).

%% With the seed's server stopped, convergence on an established multi-member
%% member does not reset it.
established_member_is_never_reset(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Seed = seed(Nodes),
    [Member | _] = non_seed(Nodes),
    {ok, _, _} = rpc:call(Seed, portunus, start_cluster, [?SYS, ?NAME, Nodes]),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    Token = place_lock(Member, {res, hold}),
    ok = portunus_ct_cluster:stop_ra_server(Seed, ?NAME),
    ?assertEqual(ok,
                 rpc:call(Member, portunus, join_or_form, [?SYS, ?NAME, Nodes])),
    ?assertEqual(?SIZE, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 rpc:call(Member, portunus, owner, [?NAME, {res, hold}])).

%% A second convergence pass on the merged cluster changes nothing and keeps the
%% lock.
repeated_convergence_does_not_thrash(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    ok = solo_form_all(Nodes),
    ok = converge_all(Nodes, ?RETRIES),
    Leader = element(2, portunus_ct_cluster:wait_leader(Nodes, ?NAME)),
    Token = place_lock(Leader, {res, hold}),
    _ = [rpc:call(N, portunus, join_or_form, [?SYS, ?NAME, Nodes]) || N <- Nodes],
    ?assertEqual(?SIZE, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 rpc:call(Leader, portunus, owner, [?NAME, {res, hold}])).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

seed(Nodes) ->
    hd(lists:sort(Nodes)).

non_seed(Nodes) ->
    lists:sort(Nodes) -- [seed(Nodes)].

solo_form_all(Nodes) ->
    _ = [{ok, _, _} = rpc:call(N, portunus, start_cluster, [?SYS, ?NAME, [N]])
         || N <- Nodes],
    ok.

%% Call `join_or_form/3` on every node each round until all are one cluster.
converge_all(Nodes, 0) ->
    ct:fail({converge_timed_out, Nodes});
converge_all(Nodes, Retries) ->
    _ = [rpc:call(N, portunus, join_or_form, [?SYS, ?NAME, Nodes]) || N <- Nodes],
    case portunus_ct_cluster:member_count(Nodes, ?NAME) =:= length(Nodes) of
        true -> ok;
        false -> timer:sleep(100), converge_all(Nodes, Retries - 1)
    end.

%% A lock held by a long-lived client on `Node`.
place_lock(Node, Key) ->
    Client = portunus_ct_cluster:start_client(Node),
    {ok, Lease} = portunus_ct_cluster:until_quorum(Client, grant_lease, [?NAME, ?TTL]),
    {ok, Token} = portunus_ct_cluster:until_quorum(Client, acquire, [?NAME, Key, Lease, owner_a]),
    portunus_ct_cluster:await_owner(Node, ?NAME, Key, owner_a, Token),
    Token.
