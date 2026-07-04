%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_succession_unit_SUITE).

%% Score-ordered succession, driving `apply/3` directly. A waiter's score
%% biases promotion; equal scores stay FIFO.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([higher_score_promoted_first/1,
         equal_score_is_fifo/1,
         negative_and_mixed_scores/1,
         stale_high_score_waiter_skipped/1,
         reacquire_refreshes_bid/1,
         revoked_waiter_is_purged/1,
         revoke_promotes_held_and_drops_waited/1]).

all() ->
    [higher_score_promoted_first,
     equal_score_is_fifo,
     negative_and_mixed_scores,
     stale_high_score_waiter_skipped,
     reacquire_refreshes_bid,
     revoked_waiter_is_purged,
     revoke_promotes_held_and_drops_waited].

higher_score_promoted_first(_Config) ->
    S0 = three_waiters(0, 5),
    {THolder, S1} = holder_token(S0),
    {ok, S2, _} = step({release, k, THolder}, 100, S1),
    %% l3 (score 5) jumps ahead of l2 (score 0), even though l2 queued first.
    {ok, #{owner := o3}} = portunus_machine:query_owner(k, S2).

equal_score_is_fifo(_Config) ->
    S0 = three_waiters(0, 0),
    {THolder, S1} = holder_token(S0),
    {ok, S2, _} = step({release, k, THolder}, 100, S1),
    %% Equal scores fall back to arrival order: l2 queued before l3.
    {ok, #{owner := o2}} = portunus_machine:query_owner(k, S2).

negative_and_mixed_scores(_Config) ->
    S0 = three_waiters(-1, 3),
    {THolder, S1} = holder_token(S0),
    {ok, S2, _} = step({release, k, THolder}, 100, S1),
    %% l3 (3) wins; releasing it next promotes l2 (0) over the -1 floor.
    {ok, #{owner := o3, token := T3}} = portunus_machine:query_owner(k, S2),
    {ok, S3, _} = step({release, k, T3}, 101, S2),
    {ok, #{owner := o2}} = portunus_machine:query_owner(k, S3).

stale_high_score_waiter_skipped(_Config) ->
    S0 = three_waiters(0, 9),
    {THolder, S1} = holder_token(S0),
    %% l3 has the top score but its lease is revoked before promotion.
    {ok, S2, _} = step({revoke_lease, l3}, 99, S1),
    {ok, S3, _} = step({release, k, THolder}, 100, S2),
    {ok, #{owner := o2}} = portunus_machine:query_owner(k, S3).

reacquire_refreshes_bid(_Config) ->
    S0 = portunus_machine:init(#{cluster => test}),
    {_, S1, _} = step({grant_lease, l1, 100000, o1, self()}, 1, S0),
    {_, S2, _} = step({grant_lease, l2, 100000, o2, self()}, 2, S1),
    {_, S3, _} = step({grant_lease, l3, 100000, o3, self()}, 3, S2),
    {{ok, TH}, S4, _} = step({acquire, l1, k, o1, undefined, nowait}, 4, S3),
    {{queued, _}, S5, _} = step({acquire, l2, k, o2, undefined, wait, 0}, 5, S4),
    %% l2 re-acquires with a higher score: this refreshes its one waiter
    %% rather than adding a duplicate.
    {{queued, _}, S6, _} = step({acquire, l2, k, o2, undefined, wait, 9}, 6, S5),
    {{queued, _}, S7, _} = step({acquire, l3, k, o3, undefined, wait, 5}, 7, S6),
    #{waiters := 2} = portunus_machine:overview(S7),
    {ok, S8, _} = step({release, k, TH}, 8, S7),
    %% The refreshed score (9) wins over l3 (5).
    {ok, #{owner := o2}} = portunus_machine:query_owner(k, S8).

revoked_waiter_is_purged(_Config) ->
    S0 = portunus_machine:init(#{cluster => test}),
    {_, S1, _} = step({grant_lease, l1, 100000, o1, self()}, 1, S0),
    {_, S2, _} = step({grant_lease, l2, 100000, o2, self()}, 2, S1),
    {{ok, TH}, S3, _} = step({acquire, l1, k, o1, undefined, nowait}, 3, S2),
    {{queued, _}, S4, _} = step({acquire, l2, k, o2, undefined, wait, 0}, 4, S3),
    #{waiters := 1} = portunus_machine:overview(S4),
    %% l2 waits but holds nothing; revoking it purges its waiter at once,
    %% not lazily at the holder's next release.
    {ok, S5, _} = step({revoke_lease, l2}, 5, S4),
    #{waiters := 0} = portunus_machine:overview(S5),
    {ok, S6, _} = step({release, k, TH}, 6, S5),
    {error, not_held} = portunus_machine:query_owner(k, S6).

revoke_promotes_held_and_drops_waited(_Config) ->
    S0 = portunus_machine:init(#{cluster => test}),
    {_, S1, _} = step({grant_lease, l1, 100000, o1, self()}, 1, S0),
    {_, S2, _} = step({grant_lease, l2, 100000, o2, self()}, 2, S1),
    {_, S3, _} = step({grant_lease, l3, 100000, o3, self()}, 3, S2),
    %% l1 holds ka; l2 holds kb while l1 waits on kb; l3 waits on ka.
    {{ok, _}, S4, _} = step({acquire, l1, ka, o1, undefined, nowait}, 4, S3),
    {{ok, _}, S5, _} = step({acquire, l2, kb, o2, undefined, nowait}, 5, S4),
    {{queued, _}, S6, _} = step({acquire, l1, kb, o1, undefined, wait, 0}, 6, S5),
    {{queued, _}, S7, _} = step({acquire, l3, ka, o3, undefined, wait, 0}, 7, S6),
    #{waiters := 2} = portunus_machine:overview(S7),
    %% Revoking l1 frees ka (promoting l3) and drops l1's waiter on kb in the
    %% one command.
    {ok, S8, _} = step({revoke_lease, l1}, 8, S7),
    {ok, #{owner := o3}} = portunus_machine:query_owner(ka, S8),
    {ok, #{owner := o2}} = portunus_machine:query_owner(kb, S8),
    #{waiters := 0} = portunus_machine:overview(S8).

%%----------------------------------------------------------------------
%% Fixtures and `apply/3` helpers
%%----------------------------------------------------------------------

%% Holder o1 owns k; o2 waits with Score2, o3 waits (later) with Score3.
three_waiters(Score2, Score3) ->
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
