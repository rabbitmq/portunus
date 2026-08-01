%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_joiner_unit_SUITE).

%% `portunus_joiner:decide/2`, the pure decision core, driven over event
%% sequences without a cluster.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([failed_join_doubles_and_caps_the_backoff/1,
         successful_join_notifies_and_arms_the_recheck/1,
         holding_membership_arms_only/1,
         failed_membership_check_rejoins_with_backoff_reset/1,
         trigger_rearms_the_one_timer/1,
         notifications_fire_on_transitions_only/1,
         start_link_refuses_invalid_options/1]).

-define(RECHECK_MS, 60000).

all() ->
    [failed_join_doubles_and_caps_the_backoff,
     successful_join_notifies_and_arms_the_recheck,
     holding_membership_arms_only,
     failed_membership_check_rejoins_with_backoff_reset,
     trigger_rearms_the_one_timer,
     notifications_fire_on_transitions_only,
     start_link_refuses_invalid_options].

failed_join_doubles_and_caps_the_backoff(_Config) ->
    S0 = initial(),
    {[{arm, 250}], S1} = portunus_joiner:decide({pass_failed, whatever}, S0),
    {[{arm, 500}], S2} = portunus_joiner:decide({pass_failed, whatever}, S1),
    {[{arm, 1000}], S3} = portunus_joiner:decide({pass_failed, whatever}, S2),
    %% Enough failures reach the cap and stay there.
    SN = lists:foldl(fun(_, S) ->
                             {_, S1N} = portunus_joiner:decide({pass_failed, x}, S),
                             S1N
                     end, S3, lists:seq(1, 10)),
    ?assertMatch({[{arm, 5000}], _},
                 portunus_joiner:decide({pass_failed, whatever}, SN)).

successful_join_notifies_and_arms_the_recheck(_Config) ->
    {_, Backed} = portunus_joiner:decide({pass_failed, x}, initial()),
    {Actions, S} = portunus_joiner:decide(pass_ok, Backed),
    ?assertEqual([{notify, joined}, {arm, ?RECHECK_MS}], Actions),
    ?assertMatch(#{status := member, backoff_ms := 250}, S).

holding_membership_arms_only(_Config) ->
    {_, Member} = portunus_joiner:decide(pass_ok, initial()),
    {Actions, S} = portunus_joiner:decide(pass_ok, Member),
    ?assertEqual([{arm, ?RECHECK_MS}], Actions),
    ?assertEqual(Member, S).

failed_membership_check_rejoins_with_backoff_reset(_Config) ->
    {_, Member} = portunus_joiner:decide(pass_ok, initial()),
    {Actions, S} = portunus_joiner:decide({pass_failed, not_a_member}, Member),
    %% The immediate re-arm makes the rejoin the very next pass.
    ?assertEqual([{notify, rejoining}, {arm, 0}], Actions),
    ?assertMatch(#{status := joining, backoff_ms := 250}, S).

trigger_rearms_the_one_timer(_Config) ->
    {JoiningActions, S0} = portunus_joiner:decide(trigger, initial()),
    ?assertEqual([{arm, 0}], JoiningActions),
    ?assertEqual(initial(), S0),
    {_, Member} = portunus_joiner:decide(pass_ok, initial()),
    ?assertEqual({[{arm, 0}], Member},
                 portunus_joiner:decide(trigger, Member)).

notifications_fire_on_transitions_only(_Config) ->
    Events = [{pass_failed, a}, {pass_failed, b}, pass_ok,
              pass_ok, trigger, pass_ok,
              {pass_failed, c}, {pass_failed, d}, pass_ok],
    {Notifications, _} =
        lists:foldl(fun(E, {Acc, S}) ->
                            {Actions, S1} = portunus_joiner:decide(E, S),
                            {Acc ++ [N || {notify, N} <- Actions], S1}
                    end, {[], initial()}, Events),
    %% Failures while joining and repeated confirmations while a member
    %% say nothing. Only the transitions do.
    ?assertEqual([joined, rejoining, joined], Notifications).

start_link_refuses_invalid_options(_Config) ->
    Base = #{system => some_sys, name => some_name,
             candidates => fun() -> [node()] end},
    ?assertEqual({error, {invalid_options, [recheck_interval_ms]}},
                 portunus_joiner:start_link(Base#{recheck_interval_ms => 0})),
    ?assertEqual({error, {invalid_options, [ensure_system, subscriber]}},
                 portunus_joiner:start_link(Base#{ensure_system => not_a_fun,
                                                  subscriber => not_a_pid})).

initial() ->
    #{status => joining, backoff_ms => 250, recheck_interval_ms => ?RECHECK_MS}.
