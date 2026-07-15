%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_system_restart_SUITE).

%% `start_system/2` must survive a restart of the `ra` application, which a host
%% such as RabbitMQ does during boot. The Ra system's config lives in a
%% `persistent_term` that outlives the system's processes, so a guard on
%% `ra_system:fetch/1` would skip re-initialisation and leave a stale shell
%% whose ETS tables are gone. These tests restart `ra` under a running cluster
%% and assert the system and its locks come back.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([start_system_is_idempotent/1,
         start_system_reinitialises_after_ra_restart/1,
         cluster_and_locks_survive_ra_restart/1,
         fencing_token_increases_after_ra_restart/1,
         restart_server_is_ok_when_already_running/1,
         restart_server_reports_not_found_for_non_member/1,
         already_present_child_is_dropped_and_restarted/1]).

-define(SYS, portunus_restart_sys).
-define(NAME, portunus_restart_test).
-define(TTL, 60000).

all() ->
    [start_system_is_idempotent,
     start_system_reinitialises_after_ra_restart,
     cluster_and_locks_survive_ra_restart,
     fencing_token_increases_after_ra_restart,
     restart_server_is_ok_when_already_running,
     restart_server_reports_not_found_for_non_member,
     already_present_child_is_dropped_and_restarted].

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

%% Calling `start_system/2` again on a running system is a no-op, not a failure.
start_system_is_idempotent(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    ok = portunus:start_system(?SYS, Dir),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertMatch({ok, _}, portunus:grant_lease(?NAME, ?TTL)).

%% After the `ra` application restarts, the system's config still sits in a
%% `persistent_term`, but its processes and tables are gone. `start_system/2` must
%% rebuild it: forming a cluster and granting a lease would fail on a stale
%% shell.
start_system_reinitialises_after_ra_restart(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    ok = restart_ra_app(),
    ?assertNotEqual(undefined, ra_system:fetch(?SYS)),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, L} = portunus:grant_lease(?NAME, ?TTL),
    ?assertMatch({ok, _}, portunus:acquire(?NAME, {res, k}, L, owner_a)).

%% After a `ra` restart, `start_system/2` rebuilds the system and
%% `restart_server/2` recovers this node's replica from disk, so the node is a
%% member again and a lock it held before the restart is still held, with the
%% same fencing token.
cluster_and_locks_survive_ra_restart(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, L} = portunus:grant_lease(?NAME, ?TTL),
    {ok, T} = portunus:acquire(?NAME, {res, survive}, L, owner_a),
    ok = restart_ra_app(),
    ok = portunus:start_system(?SYS, Dir),
    ok = portunus:restart_server(?SYS, ?NAME),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(?NAME) end),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertMatch({ok, #{owner := owner_a, token := T}},
                 portunus:owner(?NAME, {res, survive})).

%% Fencing tokens are the Raft index, so a token minted after recovery must
%% exceed one minted before it. A token that restarted low would let a fenced
%% writer be mistaken for the current owner: the core failure for a lock server.
fencing_token_increases_after_ra_restart(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, L1} = portunus:grant_lease(?NAME, ?TTL),
    {ok, T1} = portunus:acquire(?NAME, {res, before}, L1, owner_a),
    ok = restart_ra_app(),
    ok = portunus:start_system(?SYS, Dir),
    ok = portunus:restart_server(?SYS, ?NAME),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(?NAME) end),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, L2} = portunus:grant_lease(?NAME, ?TTL),
    {ok, T2} = portunus:acquire(?NAME, {res, after_restart}, L2, owner_b),
    ?assert(T2 > T1).

%% `restart_server/2` is a no-op on a replica that is already running, so a
%% bootstrap retry loop can call it on a healthy node without disruption.
restart_server_is_ok_when_already_running(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ok = portunus:restart_server(?SYS, ?NAME),
    ok = portunus:restart_server(?SYS, ?NAME).

%% `restart_server/2` reports `name_not_registered` when this node has no
%% on-disk replica, which is how a bootstrap decides to form or join rather than
%% recover.
restart_server_reports_not_found_for_non_member(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    ?assertEqual({error, name_not_registered},
                 portunus:restart_server(?SYS, ?NAME)).

%% A system whose supervisor child lingers after its tree was torn down reports
%% `already_present`; `start_system/2` drops that child and starts a fresh one.
already_present_child_is_dropped_and_restarted(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    %% Terminate without deleting, which is exactly what `already_present` is: the
    %% child spec stays and its processes are gone.
    ok = supervisor:terminate_child(ra_systems_sup, ?SYS),
    ?assertEqual({error, already_present}, ra_system:start(ra_system:fetch(?SYS))),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertMatch({ok, _}, portunus:grant_lease(?NAME, ?TTL)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% Stop and restart the `ra` application, as a host does on a node restart. The
%% on-disk data dir survives; the `persistent_term` config survives; the
%% processes and ETS tables do not.
restart_ra_app() ->
    ok = application:stop(ra),
    {ok, _} = application:ensure_all_started(ra),
    ok.
