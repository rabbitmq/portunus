%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_session_open_unit_SUITE).

%% `open/1,2` fails with an error tuple, never by killing a non-trapping
%% caller: init cannot fail, and the fallible grant runs in a synchronous
%% second phase. Double-close is a no-op. An immediate reopen with the same
%% `proposed_id` after a crash succeeds: Ra applies the dead incarnation's
%% monitor `DOWN` as a low-priority command, so `establish` retries
%% `id_in_use` over that milliseconds-wide window.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([open_failure_is_an_error_tuple/1,
         double_close_is_ok/1,
         immediate_reopen_with_proposed_id_succeeds/1]).

-define(SYS, portunus).
-define(NAME, portunus_session_open_test).

all() ->
    [open_failure_is_an_error_tuple,
     double_close_is_ok,
     immediate_reopen_with_proposed_id_succeeds].

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

%% No cluster under that name: the grant fails with `no_quorum`, and a
%% non-trapping caller must survive to report the tuple.
open_failure_is_an_error_tuple(_Config) ->
    Ctrl = self(),
    Caller = spawn(fun() ->
                           false = process_flag(trap_exit, false),
                           Res = portunus_session:open(no_such_cluster, #{}),
                           Ctrl ! {open_result, self(), Res}
                   end),
    receive
        {open_result, Caller, Res} ->
            ?assertEqual({error, no_quorum}, Res)
    after 15000 ->
            ct:fail(caller_died_instead_of_reporting)
    end.

double_close_is_ok(_Config) ->
    {ok, S} = portunus_session:open(?NAME, #{}),
    ok = portunus_session:close(S),
    ?assertEqual(ok, portunus_session:close(S)).

immediate_reopen_with_proposed_id_succeeds(_Config) ->
    Id = {session, reopen},
    Ctrl = self(),
    _First = spawn(fun() ->
                           {ok, S} = portunus_session:open(
                                       ?NAME, #{proposed_id => Id}),
                           Ctrl ! {opened, S},
                           receive never -> ok end
                   end),
    S1 = receive {opened, P} -> P after 10000 -> ct:fail(no_session) end,
    %% Kill the session itself: terminate is skipped, so the lease is
    %% cleaned up only through the machine's monitor `DOWN`, which Ra
    %% applies at low priority, behind this reopen's grant.
    exit(S1, kill),
    ok = portunus_test_helpers:await_condition(
           fun() -> not is_process_alive(S1) end),
    %% `establish` retries `id_in_use` over the low-priority window; the
    %% outer retry only guards against a slower-than-budget CI host.
    S2 = reopen_with_retry(Id, 10),
    ok = portunus_session:close(S2).

reopen_with_retry(_Id, 0) ->
    ct:fail(reopen_never_succeeded);
reopen_with_retry(Id, N) ->
    case portunus_session:open(?NAME, #{proposed_id => Id}) of
        {ok, S} -> S;
        {error, id_in_use} -> timer:sleep(100), reopen_with_retry(Id, N - 1)
    end.
