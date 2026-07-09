%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_transfer_unit_SUITE).

%% Machine-level transfer coverage that `portunus_machine_unit_SUITE`'s
%% transfer cases do not assert: the counter effects a transfer emits, and
%% per-key token monotonicity across a transfer-then-release chain.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([transfer_counts_transfers_total/1,
         refusal_counts_no_contender_total/1,
         fenced_and_self_transfers_count_nothing/1,
         tokens_monotonic_across_transfer_and_release/1]).

all() ->
    [transfer_counts_transfers_total,
     refusal_counts_no_contender_total,
     fenced_and_self_transfers_count_nothing,
     tokens_monotonic_across_transfer_and_release].

%% A successful handoff emits exactly one `transfers_total` increment and no
%% refusal counter.
transfer_counts_transfers_total(_Config) ->
    {Tok, S} = held_with_waiter(),
    {ok, _, Effs} = at({transfer, k, Tok, o2}, 10, 0, S),
    [transfers_total] = counter_incrs(Effs).

%% A committed refusal emits exactly one `transfer_no_contender_total`
%% increment; the client-side pre-check counts its own refusals separately.
refusal_counts_no_contender_total(_Config) ->
    {Tok, S} = held_with_waiter(),
    {{error, {no_contender, o9}}, _, Effs} = at({transfer, k, Tok, o9}, 10, 0, S),
    [transfer_no_contender_total] = counter_incrs(Effs).

%% A fenced (stale token or free key) transfer or a self-transfer moves
%% neither transfer counter.
fenced_and_self_transfers_count_nothing(_Config) ->
    {Tok, S} = held_with_waiter(),
    {{error, not_owner}, _, E1} = at({transfer, k, Tok + 999, o2}, 10, 0, S),
    [] = counter_incrs(E1),
    {ok, _, E2} = at({transfer, k, Tok, o1}, 11, 0, S),
    [] = counter_incrs(E2),
    {{error, not_owner}, _, E3} = at({transfer, free_k, 1, o2}, 12, 0, S),
    [] = counter_incrs(E3).

%% The full planned-handoff cycle a rebalancer performs: the old owner
%% re-queues after transferring away and is promoted back on the new
%% owner's release, with the key's token strictly increasing at every
%% grant along the chain.
tokens_monotonic_across_transfer_and_release(_Config) ->
    {Tok1, S0} = held_with_waiter(),
    {ok, S1, _} = at({transfer, k, Tok1, o2}, 10, 0, S0),
    {ok, #{owner := o2, token := Tok2}} = portunus_machine:query_owner(k, S1),
    ?assert(Tok2 > Tok1),
    {{queued, 1}, S2, _} = at({acquire, l1, k, o1, undefined, wait}, 11, 0, S1),
    {ok, S3, _} = at({release, k, Tok2}, 12, 0, S2),
    {ok, #{owner := o1, token := Tok3}} = portunus_machine:query_owner(k, S3),
    ?assert(Tok3 > Tok2).

%% Helpers

%% One holder (o1, lease l1) of key k and one live waiter (o2, lease l2).
held_with_waiter() ->
    S0 = portunus_machine:init(#{cluster => test}),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, Tok}, S2, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, 0, S1),
    {{ok, l2}, S3, _} = at({grant_lease, l2, 100000, o2, dummy_pid()}, 3, 0, S2),
    {{queued, 1}, S4, _} = at({acquire, l2, k, o2, undefined, wait}, 4, 0, S3),
    {Tok, S4}.

at(Cmd, Ix, Now, State) ->
    Meta = portunus_test_helpers:meta(Ix, Now),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.

counter_incrs(Effects) ->
    [F || {mod_call, portunus_counters, incr, [_, F]} <- Effects,
          F =:= transfers_total orelse F =:= transfer_no_contender_total].

dummy_pid() ->
    spawn(fun() -> timer:sleep(infinity) end).
