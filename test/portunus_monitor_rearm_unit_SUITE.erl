%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_monitor_rearm_unit_SUITE).

%% After `{down, Pid, noconnection}` drops the `monitored` entry, the
%% idempotent same-owner re-grant re-arms the monitor. Renewals are
%% aux-side and cannot re-arm; a holder that never re-grants is
%% re-monitored at the next leader change, and its death meanwhile is
%% bounded by lease expiry.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([regrant_rearms_after_noconnection/1]).

all() ->
    [regrant_rearms_after_noconnection].

regrant_rearms_after_noconnection(_Config) ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    S0 = portunus_machine:init(#{}),
    {S1, {ok, l1}, E1} = grant(l1, Pid, 1, S0),
    ?assert(has_monitor(Pid, E1)),
    {S2, ok, _} = down_noconnection(Pid, 2, S1),
    {_S3, {ok, l1}, E3} = grant(l1, Pid, 3, S2),
    ?assert(has_monitor(Pid, E3)),
    Pid ! stop.

grant(Id, Pid, Ix, S) ->
    portunus_machine:apply(portunus_test_helpers:meta(Ix),
                           {grant_lease, Id, 5000, o, Pid}, S).

down_noconnection(Pid, Ix, S) ->
    portunus_machine:apply(portunus_test_helpers:meta(Ix),
                           {down, Pid, noconnection}, S).

has_monitor(Pid, Effects) ->
    lists:member({monitor, process, Pid}, Effects).
