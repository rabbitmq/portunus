%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_registration_sync_SUITE).

%% Registration writes must be flushed to `names.dets` at the lifecycle call,
%% not left to Ra's 500 ms auto-save a hard kill can lose. The durability check
%% copies the DETS file aside without closing it and opens the copy cold: the
%% copy holds exactly what a kill at that instant would have left.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([registration_is_flushed_at_formation/1,
         unregistration_is_flushed_at_reset/1,
         sync_failure_is_swallowed/1]).

-define(SYS, portunus_registration_sync_sys).
-define(NAME, portunus_registration_sync_test).

all() ->
    [registration_is_flushed_at_formation,
     unregistration_is_flushed_at_reset,
     sync_failure_is_swallowed].

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

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

registration_is_flushed_at_formation(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    %% Copied before the auto-save can fire: red without the explicit sync.
    ?assertMatch([{?NAME, _}], cold_copy_lookup(Dir)).

unregistration_is_flushed_at_reset(Config) ->
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    %% The registration must be on disk first, or the absence below holds
    %% vacuously with the whole insert-delete pair still buffered.
    ?assertMatch([{?NAME, _}], cold_copy_lookup(Dir)),
    %% The join half fails (the local replica was just deleted and is its own
    %% seed); only the flushed delete is under test.
    {error, _} = portunus:reset_and_join_cluster(?SYS, ?NAME, node()),
    ?assertEqual([], cold_copy_lookup(Dir)).

sync_failure_is_swallowed(Config) ->
    ?assertEqual(ok, portunus:sync_registration(no_such_system)),
    Dir = dir(Config),
    ok = portunus:start_system(?SYS, Dir),
    %% Stopping the `ra` application (not the system) keeps the config's
    %% persistent_term while closing the table, so the sync itself fails and
    %% must be swallowed. `ra_system:stop/1` would erase the config and skip
    %% the sync entirely.
    ok = application:stop(ra),
    ?assertMatch(#{}, ra_system:fetch(?SYS)),
    ?assertEqual(ok, portunus:sync_registration(?SYS)),
    {ok, _} = application:ensure_all_started(ra).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

cold_copy_lookup(Dir) ->
    portunus_ct_cluster:cold_registration_lookup(Dir, ?NAME).
