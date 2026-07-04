%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_release_cursor_unit_SUITE).

%% Log growth is bounded: every `snapshot_interval` entries the machine
%% emits a release cursor so Ra can snapshot and truncate, and the expiry
%% timer runs only while leases exist, so an idle cluster stops appending
%% a tick entry every second.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([release_cursor_emitted_at_interval/1,
         expiry_timer_runs_only_while_leases_exist/1,
         leader_arms_timer_only_with_leases/1]).

all() ->
    [release_cursor_emitted_at_interval,
     expiry_timer_runs_only_while_leases_exist,
     leader_arms_timer_only_with_leases].

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

expiry_timer_runs_only_while_leases_exist(_Config) ->
    S0 = portunus_machine:init(#{}),
    %% The first lease arms the timer; the second does not re-arm.
    {{ok, l1}, S1, E1} = step({grant_lease, l1, 1000, o1, undefined}, 1, 0, S0),
    ?assert(has_timer(E1)),
    {{ok, l2}, S2, E2} = step({grant_lease, l2, 1000, o2, undefined}, 2, 0, S1),
    ?assertNot(has_timer(E2)),
    %% A tick that leaves leases behind re-arms; one that empties does not.
    {ok, S3, E3} = step({timeout, expire}, 3, 500, S2),
    ?assert(has_timer(E3)),
    {ok, S4, E4} = step({timeout, expire}, 4, 5000, S3),
    ?assertNot(has_timer(E4)),
    %% The next grant arms an idle machine again.
    {{ok, l3}, _S5, E5} = step({grant_lease, l3, 1000, o3, undefined}, 5, 6000, S4),
    ?assert(has_timer(E5)).

leader_arms_timer_only_with_leases(_Config) ->
    S0 = portunus_machine:init(#{}),
    ?assertNot(has_timer(portunus_machine:state_enter(leader, S0))),
    {{ok, l1}, S1, _} = step({grant_lease, l1, 1000, o1, undefined}, 1, 0, S0),
    ?assert(has_timer(portunus_machine:state_enter(leader, S1))).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% An empty renew: a legal command that changes nothing, to advance the index.
noop(Ix, S) ->
    {S1, [], Effs} = portunus_machine:apply(portunus_test_helpers:meta(Ix),
                                            {renew, []}, S),
    {ok_reply, S1, Effs}.

cursors(Effs) ->
    [E || {release_cursor, _, _} = E <- Effs].

has_timer(Effs) ->
    lists:any(fun({timer, expire, _}) -> true;
                 (_) -> false
              end, Effs).

step(Cmd, Ix, Time, S) ->
    {S1, Reply, Effs} = portunus_machine:apply(
                          portunus_test_helpers:meta(Ix, Time), Cmd, S),
    {Reply, S1, Effs}.
