%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_hosted_system_SUITE).

%% `use_system/1` against a host-style Ra system: both path keys to one
%% directory, derived names, no `server_recovery_strategy` (the shape
%% RabbitMQ's `coordination` system has). `portunus` is a tenant here: it
%% never starts the system, and recovery rides the caller re-running
%% `join_or_form/3`.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([use_system_requires_a_running_system/1,
         use_system_detects_a_stopped_ra_application/1,
         use_system_is_idempotent/1,
         cluster_forms_on_a_hosted_system/1,
         restart_recovers_replica_without_recovery_strategy/1,
         start_system_refuses_a_hosted_system/1,
         lost_registration_reforms_with_higher_tokens/1]).

-define(SYS, portunus_hosted_sys).
-define(NAME, portunus_hosted_test).
-define(KEY, {res, hosted}).
-define(TTL, 60000).

all() ->
    [use_system_requires_a_running_system,
     use_system_detects_a_stopped_ra_application,
     use_system_is_idempotent,
     cluster_forms_on_a_hosted_system,
     restart_recovers_replica_without_recovery_strategy,
     start_system_refuses_a_hosted_system,
     lost_registration_reforms_with_higher_tokens].

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

start_host(Config) ->
    ok = portunus_ct_cluster:start_host_system(?SYS, dir(Config)).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

use_system_requires_a_running_system(_Config) ->
    ?assertEqual({error, {ra_system_not_running, ?SYS}},
                 portunus:use_system(?SYS)),
    %% Attaching starts applications, never a Ra system.
    ?assertEqual(undefined, ra_system:fetch(?SYS)).

%% An `ra` application stop leaves the system's config in its persistent_term
%% while the WAL process is gone: not running, and the config-present branch
%% must say so rather than trust the stale term.
use_system_detects_a_stopped_ra_application(Config) ->
    start_host(Config),
    ?assertEqual(ok, portunus:use_system(?SYS)),
    ok = application:stop(ra),
    ?assertMatch(#{}, ra_system:fetch(?SYS)),
    ?assertEqual({error, {ra_system_not_running, ?SYS}},
                 portunus:use_system(?SYS)),
    {ok, _} = application:ensure_all_started(ra),
    start_host(Config),
    ?assertEqual(ok, portunus:use_system(?SYS)).

use_system_is_idempotent(Config) ->
    start_host(Config),
    ?assertEqual(ok, portunus:use_system(?SYS)),
    ?assertEqual(ok, portunus:use_system(?SYS)).

cluster_forms_on_a_hosted_system(Config) ->
    start_host(Config),
    ok = portunus:use_system(?SYS),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, Lease} = portunus:grant_lease(?NAME, ?TTL),
    {ok, Token} = portunus:acquire(?NAME, ?KEY, Lease, owner_a),
    ?assertEqual(ok, portunus:release(?NAME, ?KEY, Token)).

restart_recovers_replica_without_recovery_strategy(Config) ->
    Token = form_and_lock(Config),
    ok = ra_system:stop(?SYS),
    start_host(Config),
    %% The host runs no recovery strategy: nothing restarts the replica.
    ?assertEqual(undefined, whereis(?NAME)),
    ok = portunus:use_system(?SYS),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(?NAME) end),
    ok = portunus_test_helpers:await_leader(?NAME),
    %% The log must come back, not just a server under the name.
    ?assertMatch(#{last_index := I} when I > 0, ra:key_metrics({?NAME, node()})),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 portunus:owner(?NAME, ?KEY)).

start_system_refuses_a_hosted_system(Config) ->
    start_host(Config),
    %% Same directories, but the host has no `server_recovery_strategy`.
    ?assertMatch({error, {ra_system_mismatch, ?SYS, _}},
                 portunus:start_system(?SYS, dir(Config))).

%% The remaining trade of hosted mode: with no repair possible before the
%% host's WAL recovery, a single-node registration loss re-forms fresh, so
%% lock state is gone. Its fencing tokens, however, land above the dead
%% incarnation's fences: the re-formed cluster's epoch (the wall clock at
%% its first command) exceeds the old one's, so packed tokens stay
%% monotonic across the loss. A change to either half should fail here.
lost_registration_reforms_with_higher_tokens(Config) ->
    Dir = dir(Config),
    start_host(Config),
    ok = portunus:use_system(?SYS),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    %% Push the log index well past a fresh cluster's first indices, so the
    %% index part alone would order the tokens the wrong way: only the epoch
    %% can put the re-formed cluster's tokens on top.
    [begin
         {ok, L} = portunus:grant_lease(?NAME, ?TTL),
         ok = portunus:revoke_lease(?NAME, L)
     end || _ <- lists:seq(1, 10)],
    {ok, Lease1} = portunus:grant_lease(?NAME, ?TTL),
    {ok, Token1} = portunus:acquire(?NAME, ?KEY, Lease1, owner_a),
    ok = ra_system:stop(?SYS),
    ok = file:delete(filename:join(Dir, "names.dets")),
    start_host(Config),
    ok = portunus:use_system(?SYS),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertEqual({error, not_held}, portunus:owner(?NAME, ?KEY)),
    {ok, Lease} = portunus:grant_lease(?NAME, ?TTL),
    {ok, Token2} = portunus:acquire(?NAME, ?KEY, Lease, owner_b),
    ?assert(Token2 > Token1),
    %% The two incarnations decompose to distinct epochs.
    ?assert(maps:get(epoch, portunus:token_info(Token2)) >
                maps:get(epoch, portunus:token_info(Token1))).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

form_and_lock(Config) ->
    start_host(Config),
    ok = portunus:use_system(?SYS),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, Lease} = portunus:grant_lease(?NAME, ?TTL),
    {ok, Token} = portunus:acquire(?NAME, ?KEY, Lease, owner_a),
    Token.
