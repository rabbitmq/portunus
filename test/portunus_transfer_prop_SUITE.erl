%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_transfer_prop_SUITE).

%% Property tests use PropEr, so this module includes only proper.hrl:
%% mixing it with eunit/ct headers redefines macros such as LET.
%%
%% `portunus_succession_prop_SUITE` samples a single transfer's targeting;
%% these properties cover what it does not: a refused or fenced transfer is
%% a pure no-op on the machine state, a random chain of transfers (valid,
%% stale, and self-transfer) keeps the key owned by one known owner with a
%% per-key monotonic token throughout, and `query_contenders/2` agrees with
%% the transfer command on which targets are viable.

-include_lib("proper/include/proper.hrl").

-export([all/0, failed_transfer_changes_nothing/1,
         transfer_chain_stays_safe/1,
         contenders_match_transfer_acceptance/1]).

all() ->
    [failed_transfer_changes_nothing, transfer_chain_stays_safe,
     contenders_match_transfer_acceptance].

failed_transfer_changes_nothing(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_failed_transfer_changes_nothing/0, 500).

transfer_chain_stays_safe(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_transfer_chain_stays_safe/0, 300).

contenders_match_transfer_acceptance(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_contenders_match_transfer_acceptance/0, 500).

%% Any transfer the machine does not accept (a stale or wrong token, an
%% unknown target, a dead target) returns an error and leaves the state
%% exactly as it was: no promotion, no token mint, no queue change.
prop_failed_transfer_changes_nothing() ->
    ?FORALL({Waiters, TargetIx, TokenSkew},
            {non_empty(list(waiter())), choose(0, 5), oneof([0, 1, -1, 999])},
            begin
                {THolder, S} = enqueue_scored(Waiters),
                Target = {w, TargetIx},
                %% Indices stay below the snapshot interval: a `release_cursor`
                %% effect rewrites `last_release`, which would break the
                %% unchanged-state comparison for reasons unrelated to transfer.
                {Reply, SR, _} = step({transfer, k, THolder + TokenSkew, Target},
                                      2000, S),
                case Reply of
                    {error, _} -> SR =:= S;
                    ok -> TokenSkew =:= 0
                end
            end).

%% Across a random chain of transfers, each drawn with a valid or stale
%% token and a random (possibly self) target: the key is owned at every
%% step by the original holder or an enqueued waiter (never free, never a
%% third party), and its token never decreases, increasing strictly
%% whenever ownership moves. A stale move replays the token the previous
%% owner held, the classic fencing scenario; before the first handoff it
%% falls back to a never-minted future token. This is the Quint model's
%% `targetedHandover` sampled against the real `apply/3` over chains
%% rather than one step.
prop_transfer_chain_stays_safe() ->
    ?FORALL({Waiters, Moves},
            {non_empty(list(waiter())),
             non_empty(list({choose(0, 5), oneof([current, stale])}))},
            begin
                {THolder, S0} = enqueue_scored(Waiters),
                Known = [o0 | [{w, I} || I <- lists:seq(0, length(Waiters) - 1)]],
                %% Indices count up from below the snapshot interval (see the
                %% comment in the other property).
                {_, _, _, _, Ok} =
                    lists:foldl(
                      fun(_, {_, _, _, _, false} = Acc) ->
                              Acc;
                         ({TargetIx, TokChoice}, {Ix, Tok, Prev, S, true}) ->
                              Cmd = {transfer, k,
                                     case {TokChoice, Prev} of
                                         {current, _} -> Tok;
                                         {stale, none} -> Tok + 1;
                                         {stale, P} -> P
                                     end, {w, TargetIx}},
                              {_, S1, _} = step(Cmd, Ix, S),
                              {ok, #{owner := Owner, token := Tok1}} =
                                  portunus_machine:query_owner(k, S1),
                              Safe = lists:member(Owner, Known)
                                  andalso Tok1 >= Tok
                                  andalso (Tok1 > Tok orelse S1 =:= S),
                              Prev1 = case Tok1 > Tok of
                                          true -> Tok;
                                          false -> Prev
                                      end,
                              {Ix + 1, Tok1, Prev1, S1, Safe}
                      end, {2000, THolder, none, S0, true}, Moves),
                Ok
            end).

%% `query_contenders/2` and the transfer command agree on the same state: a
%% valid-token transfer to owner X succeeds exactly when X is a listed
%% contender. This is the contract `portunus_election:transfer_to/2`'s
%% pre-check relies on (its remaining risk is only the state changing
%% between read and command, not the two disagreeing).
prop_contenders_match_transfer_acceptance() ->
    ?FORALL({Waiters, TargetIx}, {non_empty(list(waiter())), choose(0, 5)},
            begin
                {THolder, S} = enqueue_scored(Waiters),
                Target = {w, TargetIx},
                Listed = lists:member(Target,
                                      portunus_machine:query_contenders(k, S)),
                {Reply, _, _} = step({transfer, k, THolder, Target}, 2000, S),
                case Reply of
                    ok -> Listed;
                    {error, {no_contender, Target}} -> not Listed
                end
            end).

%% Helpers

%% A waiter is a score and whether its lease survives enqueueing.
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

step(Cmd, Ix, State) ->
    Meta = portunus_test_helpers:meta(Ix),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.
