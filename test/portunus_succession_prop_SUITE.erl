%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_succession_prop_SUITE).

%% Property tests use PropEr, so this module includes only proper.hrl:
%% mixing it with eunit/ct headers redefines macros such as LET.
-include_lib("proper/include/proper.hrl").

-export([all/0, priority_succession/1, transfer_reaches_named_target/1]).

all() ->
    [priority_succession, transfer_reaches_named_target].

priority_succession(_Config) ->
    true = portunus_test_helpers:quickcheck(fun prop_priority_succession/0, 500).

transfer_reaches_named_target(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_transfer_reaches_named_target/0, 500).

%% For a key with a held owner and a queue of waiters carrying random
%% scores, releasing the holder promotes exactly the live waiter with the
%% highest score, ties broken by arrival order. Scores are drawn from a
%% small range so ties are common, and some waiters are revoked before the
%% release so the stale-waiter path is exercised; if every waiter is gone
%% the key frees. This is succession safety (still at most one owner) plus
%% the score ordering.
prop_priority_succession() ->
    ?FORALL(Waiters, non_empty(list(waiter())),
            begin
                {THolder, S} = enqueue_scored(Waiters),
                {ok, SR, _} = step({release, k, THolder}, 1000000, S),
                Owner = case portunus_machine:query_owner(k, SR) of
                            {ok, #{owner := W}} -> W;
                            {error, not_held} -> none
                        end,
                Owner =:= expected_winner(Waiters)
            end).

%% For the same held-owner-and-queue setup, a transfer to a named contender's
%% owner promotes exactly that contender regardless of its score, or, if the
%% named owner is not a live waiter, is refused and the holder keeps the key.
%% This is the targeting guarantee, sampled against the real `apply/3`.
prop_transfer_reaches_named_target() ->
    ?FORALL({Waiters, TargetIx}, {non_empty(list(waiter())), choose(0, 5)},
            begin
                {THolder, S} = enqueue_scored(Waiters),
                Target = {w, TargetIx},
                {Reply, SR, _} = step({transfer, k, THolder, Target}, 1000000, S),
                Owner = case portunus_machine:query_owner(k, SR) of
                            {ok, #{owner := W}} -> W;
                            {error, not_held} -> none
                        end,
                case is_live_waiter(TargetIx, Waiters) of
                    true ->
                        Reply =:= ok andalso Owner =:= Target;
                    false ->
                        Reply =:= {error, {no_contender, Target}}
                            andalso Owner =:= o0
                end
            end).

%% Waiter index `I` was enqueued (in range) and its lease still lives.
is_live_waiter(I, Waiters) ->
    I < length(Waiters)
        andalso element(2, lists:nth(I + 1, Waiters)) =:= live.

%% A waiter is a score and whether its lease survives to the release.
waiter() ->
    {choose(-2, 3), frequency([{4, live}, {1, revoked}])}.

%% Holder o0 owns k; one waiter per element, queued in list order, with the
%% revoked ones' leases dropped after they enqueue.
enqueue_scored(Waiters) ->
    S0 = portunus_machine:init(#{cluster => test}),
    {_, S1, _} = step({grant_lease, l0, 100000, o0, self()}, 1, S0),
    {{ok, THolder}, S2, _} = step({acquire, l0, k, o0, undefined, nowait}, 2, S1),
    {SN, _} = lists:foldl(
                fun({Score, Fate}, {S, I}) ->
                        Lease = {l, I},
                        Owner = {w, I},
                        {_, Sa, _} = step({grant_lease, Lease, 100000, Owner,
                                           self()}, 100 + 3 * I, S),
                        {{queued, _}, Sb, _} =
                            step({acquire, Lease, k, Owner, undefined, wait, Score},
                                 101 + 3 * I, Sa),
                        Sc = case Fate of
                                 live -> Sb;
                                 revoked ->
                                     {ok, Sr, _} = step({revoke_lease, Lease},
                                                        102 + 3 * I, Sb),
                                     Sr
                             end,
                        {Sc, I + 1}
                end, {S2, 0}, Waiters),
    {THolder, SN}.

expected_winner(Waiters) ->
    Indexed = lists:zip(lists:seq(0, length(Waiters) - 1), Waiters),
    case [{I, Sc} || {I, {Sc, live}} <- Indexed] of
        [] ->
            none;
        [First | Rest] ->
            {BestI, _} = lists:foldl(
                           fun({J, Sc}, {_BI, BSc} = Best) ->
                                   case Sc > BSc of
                                       true -> {J, Sc};
                                       false -> Best
                                   end
                           end, First, Rest),
            {w, BestI}
    end.

step(Cmd, Ix, State) ->
    Meta = portunus_test_helpers:meta(Ix),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.
