%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_stop_noproc_unit_SUITE).

%% `unlock/1` and `with_lock/4` must survive a renewer that already stopped
%% itself after `lease_lost`: without the `noproc` catch the `gen_server:stop` on the
%% dead pid crashed the caller with `noproc`, precisely after the quorum
%% outage it most needed to survive.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([unlock_after_renewer_death_is_ok/1,
         with_lock_survives_renewer_death/1]).

-define(SYS, portunus).
-define(NAME, portunus_stop_noproc_test).

all() ->
    [unlock_after_renewer_death_is_ok,
     with_lock_survives_renewer_death].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

%% A renewer that declared `lease_lost` stops itself with reason `normal`;
%% stopping it here the same way leaves the handle pointing at a dead pid.
unlock_after_renewer_death_is_ok(_Config) ->
    {ok, #{renewer := Renewer} = Handle} =
        portunus:lock(?NAME, {res, dead_renewer}, 60000),
    ok = gen_server:stop(Renewer),
    ?assertEqual(ok, portunus:unlock(Handle)).

with_lock_survives_renewer_death(_Config) ->
    Key = {res, with_lock_dead_renewer},
    %% The fun stops its own renewer; the `after unlock(Handle)` must not
    %% turn the successful fun into a caller crash.
    Result = portunus:with_lock(
               ?NAME, Key, 60000,
               fun() ->
                       [KA] = [P || P <- processes(), is_renewer_label(P)],
                       ok = gen_server:stop(KA),
                       fun_ran
               end),
    ?assertEqual(fun_ran, Result),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:owner(?NAME, Key) =:= {error, not_held} end).

is_renewer_label(P) ->
    case proc_lib:get_label(P) of
        {portunus_keepalive, ?NAME, _} -> true;
        _ -> false
    end.
