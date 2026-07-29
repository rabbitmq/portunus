%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_rescore_unit_SUITE).

%% Tests succession re-scoring at the `ra` machine level using `apply/3` directly.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([rebid_raises_score_and_keeps_seq/1,
         rebid_lowers_score_and_keeps_seq/1,
         rebid_keeps_arrival_order_among_equal_scores/1,
         holder_rebid_stays_idempotent/1,
         replay_with_rebids_is_deterministic/1]).

all() ->
    [rebid_raises_score_and_keeps_seq,
     rebid_lowers_score_and_keeps_seq,
     rebid_keeps_arrival_order_among_equal_scores,
     holder_rebid_stays_idempotent,
     replay_with_rebids_is_deterministic].

%% l2 enqueued before l3 with a lower score. Its re-bid raises the score, so
%% the release promotes l2 using the refreshed value.
rebid_raises_score_and_keeps_seq(_Config) ->
    S0 = holder_and_two_waiters(1, 5),
    {{queued, 2}, S1, _} = step({acquire, l2, k, o2, undefined, wait, 9}, 10, S0),
    {T, S2} = holder_token(S1),
    {ok, S3, _} = step({release, k, T}, 11, S2),
    {ok, #{owner := o2}} = portunus_machine:query_owner(k, S3).

%% The refresh works in both directions: l3 held the top score at enqueue,
%% and its re-bid lowers it below l2's, so l2 is promoted.
rebid_lowers_score_and_keeps_seq(_Config) ->
    S0 = holder_and_two_waiters(1, 5),
    {{queued, 2}, S1, _} = step({acquire, l3, k, o3, undefined, wait, 0}, 10, S0),
    {T, S2} = holder_token(S1),
    {ok, S3, _} = step({release, k, T}, 11, S2),
    {ok, #{owner := o2}} = portunus_machine:query_owner(k, S3).

%% Both waiters carry equal scores, and the earlier arrival (l2) re-bids the
%% same value. Its kept `seq` wins the tie.
rebid_keeps_arrival_order_among_equal_scores(_Config) ->
    S0 = holder_and_two_waiters(3, 3),
    {{queued, 2}, S1, _} = step({acquire, l2, k, o2, undefined, wait, 3}, 10, S0),
    {T, S2} = holder_token(S1),
    {ok, S3, _} = step({release, k, T}, 11, S2),
    {ok, #{owner := o2}} = portunus_machine:query_owner(k, S3).

%% A wait-mode re-acquire by the lease that holds the key answers the
%% existing token and changes nothing: the machine half of the race between
%% a re-bid and a promotion already granted.
holder_rebid_stays_idempotent(_Config) ->
    S0 = holder_and_two_waiters(1, 5),
    {T, S1} = holder_token(S0),
    {{ok, T}, S2, _} = step({acquire, l1, k, o1, undefined, wait, 8}, 10, S1),
    ?assertEqual(portunus_machine:overview(S1),
                 portunus_machine:overview(S2)),
    {ok, #{owner := o1, token := T}} = portunus_machine:query_owner(k, S2).

%% The kept `seq` is read from machine state, a pure function of the log, so
%% a command sequence containing re-bids folds to identical state twice.
replay_with_rebids_is_deterministic(_Config) ->
    Log = [{grant_lease, l1, 100000, o1, dummy_pid()},
           {grant_lease, l2, 100000, o2, dummy_pid()},
           {grant_lease, l3, 100000, o3, dummy_pid()},
           {acquire, l1, k, o1, undefined, nowait},
           {acquire, l2, k, o2, undefined, wait, 1},
           {acquire, l3, k, o3, undefined, wait, 5},
           {acquire, l2, k, o2, undefined, wait, 9},
           {acquire, l3, k, o3, undefined, wait, 2},
           {release, k, 4},
           {acquire, l1, k, o1, undefined, wait, 7}],
    ?assertEqual(replay(Log), replay(Log)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% Holder o1 owns k; l2 waits with Score2, then l3 waits with Score3.
holder_and_two_waiters(Score2, Score3) ->
    S0 = portunus_machine:init(#{cluster => test}),
    {_, S1, _} = step({grant_lease, l1, 100000, o1, self()}, 1, S0),
    {_, S2, _} = step({grant_lease, l2, 100000, o2, self()}, 2, S1),
    {_, S3, _} = step({grant_lease, l3, 100000, o3, self()}, 3, S2),
    {{ok, _}, S4, _} = step({acquire, l1, k, o1, undefined, nowait}, 4, S3),
    {{queued, _}, S5, _} = step({acquire, l2, k, o2, undefined, wait, Score2}, 5, S4),
    {{queued, _}, S6, _} = step({acquire, l3, k, o3, undefined, wait, Score3}, 6, S5),
    S6.

holder_token(State) ->
    {ok, #{owner := o1, token := T}} = portunus_machine:query_owner(k, State),
    {T, State}.

step(Cmd, Ix, State) ->
    Meta = portunus_test_helpers:meta(Ix),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.

replay(Log) ->
    lists:foldl(
      fun({Cmd, Ix}, S0) ->
              {_, S, _} = step(Cmd, Ix, S0),
              S
      end,
      portunus_machine:init(#{cluster => replay}),
      lists:zip(Log, lists:seq(1, length(Log)))).

dummy_pid() ->
    spawn(fun() -> ok end).
