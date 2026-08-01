%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_joiner_prop_SUITE).

%% Property tests use PropEr, so this module includes only proper.hrl:
%% mixing it with eunit/ct headers redefines macros such as LET.
-include_lib("proper/include/proper.hrl").

-export([all/0, progress/1, notifications_on_transitions/1]).

-define(BACKOFF_MIN_MS, 250).
-define(BACKOFF_MAX_MS, 5000).
-define(RECHECK_MS, 60000).

all() ->
    [progress, notifications_on_transitions].

progress(_Config) ->
    true = portunus_test_helpers:quickcheck(fun prop_progress/0, 500).

%% After any event sequence every step arms exactly one timer and the
%% backoff stays within its bounds. A stalled joiner (no timer, so no pass
%% coming) is the bug class this battery exists to remove.
prop_progress() ->
    ?FORALL(Events, list(event()),
            begin
                {Ok, _} =
                    lists:foldl(
                      fun(_E, {false, S}) ->
                              {false, S};
                         (E, {true, S}) ->
                              {Actions, S1} = portunus_joiner:decide(E, S),
                              {step_ok(Actions, S1), S1}
                      end, {true, initial()}, Events),
                Ok
            end).

step_ok(Actions, #{backoff_ms := Backoff}) ->
    Arms = [Ms || {arm, Ms} <- Actions],
    length(Arms) =:= 1
        andalso lists:all(fun(Ms) ->
                                  Ms >= 0 andalso Ms =< ?RECHECK_MS
                          end, Arms)
        andalso Backoff >= ?BACKOFF_MIN_MS
        andalso Backoff =< ?BACKOFF_MAX_MS.

notifications_on_transitions(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_notifications_on_transitions/0, 500).

%% A notification is emitted exactly when the status changes, whatever the
%% event sequence: `joined` on entering `member`, `rejoining` on leaving
%% it, and nothing otherwise. A spurious notification would toggle a
%% subscriber's deferral flag needlessly. A missing one would leave it
%% stale.
prop_notifications_on_transitions() ->
    ?FORALL(Events, list(event()),
            begin
                {Ok, _} =
                    lists:foldl(
                      fun(_E, {false, S}) ->
                              {false, S};
                         (E, {true, #{status := Before} = S}) ->
                              {Actions, #{status := After} = S1} =
                                  portunus_joiner:decide(E, S),
                              Expected = case {Before, After} of
                                             {joining, member} -> [joined];
                                             {member, joining} -> [rejoining];
                                             _ -> []
                                         end,
                              Got = [N || {notify, N} <- Actions],
                              {Got =:= Expected, S1}
                      end, {true, initial()}, Events),
                Ok
            end).

initial() ->
    #{status => joining,
      backoff_ms => ?BACKOFF_MIN_MS,
      recheck_interval_ms => ?RECHECK_MS}.

event() ->
    oneof([trigger, pass_ok, {pass_failed, some_reason}]).
