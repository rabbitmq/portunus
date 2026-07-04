%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_monitor_unit_SUITE).

%% A lease revoke or expiry demonitors the holder once it has
%% no other lease and no watch, so an expired-but-alive holder leaks no monitor.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([expiry_demonitors_unreferenced_holder/1,
         expiry_keeps_holder_with_another_lease/1,
         demonitored_holder_is_remonitored/1,
         unwatch_demonitors_watch_only_pid/1,
         noconnection_clears_monitored/1]).

all() ->
    [expiry_demonitors_unreferenced_holder,
     expiry_keeps_holder_with_another_lease,
     demonitored_holder_is_remonitored,
     unwatch_demonitors_watch_only_pid,
     noconnection_clears_monitored].

expiry_demonitors_unreferenced_holder(_Config) ->
    Pid = spawn(fun idle/0),
    S0 = portunus_machine:init(#{cluster => test}),
    {{ok, l1}, S1, E1} = step({grant_lease, l1, 1000, o, Pid}, 1, 0, S0),
    ?assert(lists:member({monitor, process, Pid}, E1)),
    {ok, _S2, E2} = step({timeout, expire}, 2, 2000, S1),
    ?assert(lists:member({demonitor, process, Pid}, E2)),
    exit(Pid, kill).

expiry_keeps_holder_with_another_lease(_Config) ->
    Pid = spawn(fun idle/0),
    S0 = portunus_machine:init(#{cluster => test}),
    {{ok, l1}, S1, _} = step({grant_lease, l1, 1000, o, Pid}, 1, 0, S0),
    {{ok, l2}, S2, _} = step({grant_lease, l2, 5000, o, Pid}, 2, 0, S1),
    {ok, _S3, E3} = step({timeout, expire}, 3, 2000, S2),
    ?assertNot(lists:member({demonitor, process, Pid}, E3)),
    exit(Pid, kill).

demonitored_holder_is_remonitored(_Config) ->
    Pid = spawn(fun idle/0),
    S0 = portunus_machine:init(#{cluster => test}),
    {{ok, l1}, S1, _} = step({grant_lease, l1, 1000, o, Pid}, 1, 0, S0),
    {ok, S2, _} = step({timeout, expire}, 2, 2000, S1),
    {{ok, l2}, _S3, E3} = step({grant_lease, l2, 1000, o, Pid}, 3, 3000, S2),
    ?assert(lists:member({monitor, process, Pid}, E3)),
    exit(Pid, kill).

%% Unwatching a watch-only pid (no lease) demonitors it, like a lease revoke.
unwatch_demonitors_watch_only_pid(_Config) ->
    Pid = spawn(fun idle/0),
    S0 = portunus_machine:init(#{cluster => test}),
    {{ok, Ref}, S1, E1} = step({watch, k, Pid}, 1, 0, S0),
    ?assert(lists:member({monitor, process, Pid}, E1)),
    {ok, _S2, E2} = step({unwatch, Ref}, 2, 0, S1),
    ?assert(lists:member({demonitor, process, Pid}, E2)),
    exit(Pid, kill).

%% A noconnection down clears monitored without releasing the lease, so a later
%% grant re-arms the monitor the auto-cleared cross-node monitor dropped.
noconnection_clears_monitored(_Config) ->
    Pid = spawn(fun idle/0),
    S0 = portunus_machine:init(#{cluster => test}),
    {{ok, l1}, S1, _} = step({grant_lease, l1, 1000, o, Pid}, 1, 0, S0),
    {ok, S2, _} = step({down, Pid, noconnection}, 2, 0, S1),
    {{ok, l2}, _S3, E3} = step({grant_lease, l2, 1000, o, Pid}, 3, 0, S2),
    ?assert(lists:member({monitor, process, Pid}, E3)),
    exit(Pid, kill).

step(Cmd, Index, Time, State) ->
    case portunus_machine:apply(portunus_test_helpers:meta(Index, Time), Cmd, State) of
        {S, R} -> {R, S, []};
        {S, R, E} -> {R, S, E}
    end.

idle() -> receive _ -> idle() end.
