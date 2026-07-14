%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_seed_recovery_multinode_SUITE).

%% A leaderless empty-log seed must still let the other nodes converge onto it.
%% Non-seeds cannot merge into a leaderless seed, so before the fix the cluster
%% never reaches full membership.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([leaderless_empty_seed_reaches_full_membership/1]).

-define(SYS, portunus).
-define(NAME, portunus_seed_recovery_multinode_test).
-define(SIZE, 3).
-define(RETRIES, 100).

all() ->
    [leaderless_empty_seed_reaches_full_membership].

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

%% The seed is leaderless with an empty log; the non-seeds formed solo. All three
%% converge into one cluster with a leader.
leaderless_empty_seed_reaches_full_membership(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Seed = hd(lists:sort(Nodes)),
    ok = form_without_election(Seed),
    _ = [{ok, _, _} = rpc:call(N, portunus, start_cluster, [?SYS, ?NAME, [N]])
         || N <- Nodes, N =/= Seed],
    ok = converge_all(Nodes, ?RETRIES),
    ?assertEqual(?SIZE, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% Sole-member server without an election: leaderless, empty log.
form_without_election(Node) ->
    ServerId = {?NAME, Node},
    Machine = {module, portunus_machine,
               #{cluster => ?NAME, tick_interval_ms => 1000, snapshot_interval => 4096}},
    ok = rpc:call(Node, ra, start_server, [?SYS, ?NAME, ServerId, Machine, [ServerId]]).

converge_all(Nodes, 0) ->
    ct:fail({converge_timed_out, Nodes});
converge_all(Nodes, Retries) ->
    _ = [rpc:call(N, portunus, join_or_form, [?SYS, ?NAME, Nodes]) || N <- Nodes],
    case portunus_ct_cluster:member_count(Nodes, ?NAME) =:= length(Nodes) of
        true -> ok;
        false -> timer:sleep(100), converge_all(Nodes, Retries - 1)
    end.
