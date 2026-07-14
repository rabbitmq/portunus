%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_seed_recovery_SUITE).

%% A sole-member seed can recover leaderless with an empty log. `join_or_form/3`
%% must elect it rather than misroute to `join_cluster/3`.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([elects_leaderless_empty_seed/1,
         elects_leaderless_empty_seed_after_ra_restart/1,
         non_empty_seed_still_elects/1]).

-define(SYS, portunus_seed_recovery_sys).
-define(NAME, portunus_seed_recovery_test).
-define(TTL, 60000).

all() ->
    [elects_leaderless_empty_seed,
     elects_leaderless_empty_seed_after_ra_restart,
     non_empty_seed_still_elects].

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

%% The reported bug: an empty-log seed is elected, not left leaderless.
elects_leaderless_empty_seed(_Config) ->
    ok = form_without_election(),
    ?assertEqual(0, last_index()),
    ?assert(not portunus:is_member(?NAME)),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assert(portunus:is_member(?NAME)).

%% The empty seed carried through a real `ra` restart, as
%% `server_recovery_strategy => registered` recovers it.
elects_leaderless_empty_seed_after_ra_restart(Config) ->
    ok = form_without_election(),
    ok = restart_ra_app(),
    ok = portunus:start_system(?SYS, ?config(ra_dir, Config)),
    ?assertEqual(0, last_index()),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assert(portunus:is_member(?NAME)).

%% A seed that committed before restarting still elects: no regression.
non_empty_seed_still_elects(_Config) ->
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, L} = portunus:grant_lease(?NAME, ?TTL),
    {ok, T} = portunus:acquire(?NAME, {res, keep}, L, owner_a),
    ok = ra:stop_server(?SYS, {?NAME, node()}),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertMatch({ok, #{owner := owner_a, token := T}},
                 portunus:owner(?NAME, {res, keep})).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% A sole-member server started without an election: leaderless, empty log, the
%% recovered-seed state. `start_cluster/3` would trigger the election.
form_without_election() ->
    ServerId = {?NAME, node()},
    Machine = {module, portunus_machine,
               #{cluster => ?NAME, tick_interval_ms => 200, snapshot_interval => 4096}},
    ra:start_server(?SYS, ?NAME, ServerId, Machine, [ServerId]).

last_index() ->
    maps:get(last_index, ra:key_metrics({?NAME, node()}), 0).

restart_ra_app() ->
    ok = application:stop(ra),
    {ok, _} = application:ensure_all_started(ra),
    ok.
