%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_cluster_formation_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([single_node_deployment_keeps_its_cluster/1,
         recovered_single_member_triggers_election/1]).

-define(SYS, portunus_cluster_formation_sys).
-define(NAME, portunus_cluster_formation_test).
-define(TTL, 60000).

all() ->
    [single_node_deployment_keeps_its_cluster,
     recovered_single_member_triggers_election].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Dir = filename:join([?config(priv_dir, Config), atom_to_list(TC), "ra"]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ok = portunus:start_system(?SYS, Dir),
    [{ra_dir, Dir} | Config].

end_per_testcase(_TC, _Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    catch ra_system:stop(?SYS),
    ok.

%% Repeated `join_or_form/3` on a single-node deployment keeps the cluster and its
%% lock.
single_node_deployment_keeps_its_cluster(_Config) ->
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, L} = portunus:grant_lease(?NAME, ?TTL),
    {ok, T} = portunus:acquire(?NAME, {res, keep}, L, owner_a),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ?assert(portunus:is_member(?NAME)),
    ?assertMatch({ok, #{owner := owner_a, token := T}},
                 portunus:owner(?NAME, {res, keep})).

%% The single member's server is stopped and then recovered. `join_or_form/3` triggers
%% the election, which is safe for the lock [owner].
recovered_single_member_triggers_election(_Config) ->
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, L} = portunus:grant_lease(?NAME, ?TTL),
    {ok, T} = portunus:acquire(?NAME, {res, elect}, L, owner_a),
    ok = ra:stop_server(?SYS, {?NAME, node()}),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertMatch({ok, #{owner := owner_a, token := T}},
                 portunus:owner(?NAME, {res, elect})).
