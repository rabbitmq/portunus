%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_registered_recovery_SUITE).

%% Ra recovers this node's replicas on system start, via
%% `server_recovery_strategy => registered`.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([recovers_replica_on_system_start/1,
         recovered_replica_keeps_uid/1,
         recovery_is_noop_without_registered_servers/1]).

-define(SYS, portunus_registered_sys).
-define(NAME, portunus_registered_test).
-define(TTL, 60000).

all() ->
    [recovers_replica_on_system_start,
     recovered_replica_keeps_uid,
     recovery_is_noop_without_registered_servers].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Dir = filename:join([?config(priv_dir, Config), atom_to_list(TC), "ra"]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    [{ra_dir, Dir} | Config].

end_per_testcase(_TC, _Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    catch ra_system:stop(?SYS),
    ok.

dir(Config) ->
    ?config(ra_dir, Config).

recovers_replica_on_system_start(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, L} = portunus:grant_lease(?NAME, ?TTL),
    {ok, T} = portunus:acquire(?NAME, {res, k}, L, owner_a),
    ok = restart_ra_app(),
    ok = portunus:start_system(?SYS, Dir),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(?NAME) end),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertMatch({ok, #{owner := owner_a, token := T}},
                 portunus:owner(?NAME, {res, k})).

recovered_replica_keeps_uid(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Before = ra_directory:uid_of(?SYS, ?NAME),
    ?assertNotEqual(undefined, Before),
    ok = restart_ra_app(),
    ok = portunus:start_system(?SYS, Dir),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(?NAME) end),
    ?assertEqual(Before, ra_directory:uid_of(?SYS, ?NAME)).

recovery_is_noop_without_registered_servers(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    ok = restart_ra_app(),
    ok = portunus:start_system(?SYS, Dir),
    ?assertEqual({error, name_not_registered},
                 portunus:restart_server(?SYS, ?NAME)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

restart_ra_app() ->
    ok = application:stop(ra),
    {ok, _} = application:ensure_all_started(ra),
    ok.
