%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_keepalive_exit_unit_SUITE).

%% The renewer cannot outlive its holder: it traps exits, so a holder that
%% exits `normal` (a link alone would ignore that) stops it too, and it
%% cannot keep a dead lock alive. `stop/1` on an already-dead renewer is a
%% no-op, not a `noproc` crash.

-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([holder_normal_exit_stops_renewer/1,
         holder_crash_stops_renewer/1,
         stop_on_dead_renewer_is_ok/1]).

-define(NAME, portunus_ka_exit_test).

all() ->
    [holder_normal_exit_stops_renewer,
     holder_crash_stops_renewer,
     stop_on_dead_renewer_is_ok].

init_per_testcase(_TC, Config) ->
    ok = meck:new(portunus, [passthrough, no_link]),
    meck:expect(portunus, renew_leases,
                fun(_N, Ls, _T) -> [{L, ok} || L <- Ls] end),
    Config.

end_per_testcase(_TC, _Config) ->
    catch meck:unload(portunus),
    ok.

holder_normal_exit_stops_renewer(_Config) ->
    holder_exit_stops_renewer(fun() -> ok end).

holder_crash_stops_renewer(_Config) ->
    holder_exit_stops_renewer(fun() -> exit(boom) end).

holder_exit_stops_renewer(ExitFun) ->
    Ctrl = self(),
    Holder = spawn(fun() ->
                           {ok, KA} = portunus_keepalive:start_link(
                                        ?NAME, lease, 2000),
                           Ctrl ! {renewer, KA},
                           receive go -> ExitFun() end
                   end),
    KA = receive {renewer, P} -> P after 5000 -> ct:fail(no_renewer) end,
    Ref = monitor(process, KA),
    Holder ! go,
    receive
        {'DOWN', Ref, process, KA, _} -> ok
    after 5000 ->
            ct:fail(renewer_outlived_holder)
    end.

stop_on_dead_renewer_is_ok(_Config) ->
    Ctrl = self(),
    _Holder = spawn(fun() ->
                            {ok, KA} = portunus_keepalive:start_link(
                                         ?NAME, lease, 2000),
                            Ctrl ! {renewer, KA}
                    end),
    KA = receive {renewer, P} -> P after 5000 -> ct:fail(no_renewer) end,
    ok = portunus_test_helpers:await_condition(
           fun() -> not is_process_alive(KA) end),
    ?assertEqual(ok, portunus_keepalive:stop(KA)).
