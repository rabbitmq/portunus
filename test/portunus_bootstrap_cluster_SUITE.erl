%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_bootstrap_cluster_SUITE).

%% Multi-node test of `join_or_form/3`: the join path, where a node joins a
%% cluster another node formed, which the single-node suites cannot reach and no
%% other suite exercises (the rest form across all nodes at once). Each peer runs
%% the bootstrap independently from a retry loop, as a real host does. The
%% assertions are that the nodes converge on one cluster of every member with a
%% single agreed leader, never a split.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([independent_bootstrap_forms_one_cluster/1,
         reset_and_join_merges_split_clusters/1]).

-define(SYS, portunus).
-define(NAME, portunus_bootstrap_cluster_test).
-define(TTL, 60000).
-define(SIZE, 3).
-define(RETRIES, 100).

all() ->
    [independent_bootstrap_forms_one_cluster,
     reset_and_join_merges_split_clusters].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    %% Start the peers with their Ra systems up but no cluster formed, so the
    %% test drives the whole bootstrap through `join_or_form/3`.
    Peers = [portunus_ct_cluster:start_node(Config, #{}) || _ <- lists:seq(1, ?SIZE)],
    Nodes = [Node || {_, Node} <- Peers],
    portunus_ct_cluster:mesh(Nodes),
    [{cluster, #{peers => Peers, nodes => Nodes}} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

%% Every node runs `join_or_form/3` from a retry loop: the lowest forms a
%% single-node cluster and the rest join it, converging on one cluster of all
%% members with one agreed leader.
independent_bootstrap_forms_one_cluster(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    ok = bootstrap(Nodes, ?RETRIES),
    ?assertEqual(?SIZE, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    %% One leader every node agrees on, and it is a member: a split would make
    %% the nodes disagree here.
    {?NAME, LeaderNode} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    ?assert(lists:member(LeaderNode, Nodes)),
    %% The formed cluster accepts writes.
    ?assertMatch({ok, _},
                 portunus_ct_cluster:papi(LeaderNode, grant_lease, [?NAME, ?TTL])).

%% The split a concurrent boot can produce, then its repair: every node forms
%% its own single-node cluster, and the non-seed nodes each `reset_and_join` the
%% seed. They must converge on one cluster of every member with one agreed
%% leader, never remain separate single-node clusters. This is exactly how a host
%% merges a standalone node into a cluster on a join event.
reset_and_join_merges_split_clusters(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    [Seed | Rest] = Nodes,
    _ = [?assertMatch({ok, _, _},
                      portunus_ct_cluster:papi(N, start_cluster, [?SYS, ?NAME, [N]]))
         || N <- Nodes],
    _ = [?assertEqual(ok,
                      portunus_ct_cluster:papi(
                        N, reset_and_join_cluster, [?SYS, ?NAME, Seed]))
         || N <- Rest],
    ?assertEqual(?SIZE, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    {?NAME, LeaderNode} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    ?assert(lists:member(LeaderNode, Nodes)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% Call `join_or_form/3` on every node each round until all are members, the
%% idempotent retry a host runs. Concurrent joins serialise through Ra, so a
%% node that loses a round simply retries.
bootstrap(Nodes, 0) ->
    ct:fail({bootstrap_timed_out, Nodes});
bootstrap(Nodes, Retries) ->
    _ = [portunus_ct_cluster:papi(Node, join_or_form, [?SYS, ?NAME, Nodes])
         || Node <- Nodes],
    case portunus_ct_cluster:member_count(Nodes, ?NAME) of
        ?SIZE -> ok;
        _ -> timer:sleep(100), bootstrap(Nodes, Retries - 1)
    end.
