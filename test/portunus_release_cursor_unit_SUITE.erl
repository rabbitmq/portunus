%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_release_cursor_unit_SUITE).

%% Log growth is bounded: every `snapshot_interval` entries the machine
%% emits a release cursor so Ra can snapshot and truncate. The machine
%% never arms timers: expiry runs through the aux sweep on Ra's tick.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([release_cursor_emitted_at_interval/1,
         no_timers_are_ever_armed/1]).

all() ->
    [release_cursor_emitted_at_interval,
     no_timers_are_ever_armed].

release_cursor_emitted_at_interval(_Config) ->
    S0 = portunus_machine:init(#{snapshot_interval => 5}),
    %% Indices 1..4: no cursor yet.
    S4 = lists:foldl(fun(Ix, S) ->
                             {ok_reply, S1, Effs} = noop(Ix, S),
                             ?assertEqual([], cursors(Effs)),
                             S1
                     end, S0, lists:seq(1, 4)),
    %% Index 5 crosses the interval and carries the post-command state.
    {ok_reply, S5, Effs5} = noop(5, S4),
    [{release_cursor, 5, S5}] = cursors(Effs5),
    %% The next window is measured from index 5.
    {ok_reply, S9, Effs9} = noop(9, S5),
    ?assertEqual([], cursors(Effs9)),
    {ok_reply, S10, Effs10} = noop(10, S9),
    [{release_cursor, 10, S10}] = cursors(Effs10).

no_timers_are_ever_armed(_Config) ->
    S0 = portunus_machine:init(#{}),
    {{ok, l1}, S1, E1} = step({grant_lease, l1, 1000, o1, undefined}, 1, 0, S0),
    ?assertNot(has_timer(E1)),
    ?assertNot(has_timer(portunus_machine:state_enter(leader, S1))).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% A membership signal: a legal command that changes nothing, to advance
%% the index.
noop(Ix, S) ->
    {S1, ok, Effs} = portunus_machine:apply(portunus_test_helpers:meta(Ix),
                                            {nodeup, node()}, S),
    {ok_reply, S1, Effs}.

cursors(Effs) ->
    [E || {release_cursor, _, _} = E <- Effs].

has_timer(Effs) ->
    lists:any(fun({timer, _, _}) -> true;
                 (_) -> false
              end, Effs).

step(Cmd, Ix, Time, S) ->
    {S1, Reply, Effs} = portunus_machine:apply(
                          portunus_test_helpers:meta(Ix, Time), Cmd, S),
    {Reply, S1, Effs}.
