%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_metrics_cluster_SUITE).

%% A follower publishes its own gauges via the machine's `handle_aux` on the
%% per-member tick, so a non-leader reports real values rather than zero. The
%% cluster of peer nodes comes from the shared `portunus_ct_cluster` harness.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([follower_publishes_its_own_gauges/1,
         demoted_leader_clears_is_leader/1]).

-define(NAME, portunus_metrics_cluster_test).

all() ->
    [follower_publishes_its_own_gauges,
     demoted_leader_clears_is_leader].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    [{cluster, portunus_ct_cluster:start(Config, ?NAME, 3)} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

follower_publishes_its_own_gauges(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    {_Members, {?NAME, LeaderNode}} = portunus_ct_cluster:cluster_info(Nodes, ?NAME),
    [Follower | _] = [N || N <- Nodes, N =/= LeaderNode],
    %% A long-lived holder keeps the lease alive while we take one lock.
    Holder = portunus_ct_cluster:start_client(LeaderNode),
    {ok, Lease} = portunus_ct_cluster:until_quorum(Holder, grant_lease, [?NAME, 60000]),
    {ok, _Token} = portunus_ct_cluster:until_quorum(
                     Holder, acquire, [?NAME, {res, k}, Lease, owner_a]),
    ok = portunus_ct_cluster:wait_until(
           fun() ->
                   G = rpc:call(Follower, portunus_counters, overview, [?NAME]),
                   is_map(G)
                       andalso maps:get(is_leader, G, 1) =:= 0
                       andalso maps:get(has_quorum, G, 0) =:= 1
                       andalso maps:get(cluster_members, G, 0) =:= 3
                       andalso maps:get(raft_term, G, 0) >= 1
                       andalso maps:get(locks_held, G, 0) =:= 1
           end),
    Holder ! stop.

%% Ra runs `mod_call` effects only on the leader, so a demotion cannot clear
%% its own gauge; the per-node aux tick must.
demoted_leader_clears_is_leader(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    {_Members, {?NAME, OldLeader}} = portunus_ct_cluster:cluster_info(Nodes, ?NAME),
    [NewLeader | _] = [N || N <- Nodes, N =/= OldLeader],
    ok = rpc:call(OldLeader, ra, transfer_leadership,
                  [{?NAME, OldLeader}, {?NAME, NewLeader}]),
    ok = portunus_ct_cluster:wait_until(
           fun() ->
                   Old = rpc:call(OldLeader, portunus_counters, overview, [?NAME]),
                   New = rpc:call(NewLeader, portunus_counters, overview, [?NAME]),
                   is_map(Old) andalso is_map(New)
                       andalso maps:get(is_leader, Old, 1) =:= 0
                       andalso maps:get(is_leader, New, 0) =:= 1
           end).
