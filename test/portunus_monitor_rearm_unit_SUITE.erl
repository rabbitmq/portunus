%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_monitor_rearm_unit_SUITE).

%% After `{down, Pid, noconnection}` drops the `monitored` entry, the paths
%% a live holder actually exercises, `renew` and the idempotent re-grant,
%% must re-arm the monitor. Before the re-arm was added nothing did until a
%% leader change, so a holder that later died normally kept its locks.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([renew_rearms_after_noconnection/1,
         regrant_rearms_after_noconnection/1,
         steady_renew_emits_no_monitor/1]).

all() ->
    [renew_rearms_after_noconnection,
     regrant_rearms_after_noconnection,
     steady_renew_emits_no_monitor].

renew_rearms_after_noconnection(_Config) ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    S0 = portunus_machine:init(#{}),
    {S1, {ok, l1}, E1} = grant(l1, Pid, 1, S0),
    ?assert(has_monitor(Pid, E1)),
    {S2, ok, _} = down_noconnection(Pid, 2, S1),
    {S3, [{l1, ok}], E3} = renew([l1], 3, S2),
    ?assert(has_monitor(Pid, E3)),
    %% Re-armed for real: a second renew emits nothing again.
    {_S4, [{l1, ok}], E4} = renew([l1], 4, S3),
    ?assertNot(has_monitor(Pid, E4)),
    Pid ! stop.

regrant_rearms_after_noconnection(_Config) ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    S0 = portunus_machine:init(#{}),
    {S1, {ok, l1}, _} = grant_from(l1, o, Pid, 1, S0),
    {S2, ok, _} = down_noconnection(Pid, 2, S1),
    %% Idempotent re-grant by the same owner refreshes the deadline and
    %% re-arms the monitor.
    {_S3, {ok, l1}, E3} = grant_from(l1, o, Pid, 3, S2),
    ?assert(has_monitor(Pid, E3)),
    Pid ! stop.

steady_renew_emits_no_monitor(_Config) ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    S0 = portunus_machine:init(#{}),
    {S1, {ok, l1}, _} = grant(l1, Pid, 1, S0),
    {_S2, [{l1, ok}], E2} = renew([l1], 2, S1),
    ?assertNot(has_monitor(Pid, E2)),
    Pid ! stop.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

grant(Id, Pid, Ix, S) ->
    grant_from(Id, owner, Pid, Ix, S).

grant_from(Id, Owner, Pid, Ix, S) ->
    portunus_machine:apply(portunus_test_helpers:meta(Ix),
                           {grant_lease, Id, 5000, Owner, Pid}, S).

down_noconnection(Pid, Ix, S) ->
    portunus_machine:apply(portunus_test_helpers:meta(Ix),
                           {down, Pid, noconnection}, S).

renew(Ids, Ix, S) ->
    portunus_machine:apply(portunus_test_helpers:meta(Ix), {renew, Ids}, S).

has_monitor(Pid, Effects) ->
    lists:member({monitor, process, Pid}, Effects).
