%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_leave_queue_unit_SUITE).

%% The `{leave_queue, LockKey, LeaseId}` machine command: withdrawing a
%% succession bid removes exactly that bid, touches no holder and no token,
%% and a lease with no bid on the key is a `not_queued` no-op.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([leave_removes_only_the_named_bid/1,
         leave_without_a_bid_is_a_noop/1,
         leave_is_per_key/1,
         lease_that_left_is_never_promoted/1]).

all() ->
    [leave_removes_only_the_named_bid,
     leave_without_a_bid_is_a_noop,
     leave_is_per_key,
     lease_that_left_is_never_promoted].

%% Two waiters; one leaving keeps the other queued, the holder and its
%% token untouched, and bumps `queue_leaves_total`.
leave_removes_only_the_named_bid(_Config) ->
    {Tok, S0} = held_with_waiters(),
    {ok, S1, Effs} = at({leave_queue, k, lease2}, 10, S0),
    [queue_leaves_total] = [F || {mod_call, portunus_counters, incr, [_, F]} <- Effs],
    [o3] = portunus_machine:query_contenders(k, S1),
    {ok, #{owner := o1, token := Tok}} = portunus_machine:query_owner(k, S1).

%% A lease that never queued, and the holder itself, both get `not_queued`
%% and change nothing.
leave_without_a_bid_is_a_noop(_Config) ->
    {_Tok, S0} = held_with_waiters(),
    {{error, not_queued}, S0, _} = at({leave_queue, k, lease9}, 10, S0),
    {{error, not_queued}, S0, _} = at({leave_queue, k, lease1}, 11, S0),
    {{error, not_queued}, S0, _} = at({leave_queue, free_k, lease2}, 12, S0).

%% A lease queued on two keys withdraws one bid; the other stays.
leave_is_per_key(_Config) ->
    {_Tok, S0} = held_with_waiters(),
    {{ok, lease4}, S1, _} = at({grant_lease, lease4, 100000, o4, dummy_pid()}, 10, S0),
    {{ok, _}, S2, _} = at({acquire, lease4, k2, o4, undefined, nowait}, 11, S1),
    {{queued, 1}, S3, _} = at({acquire, lease2, k2, o2, undefined, wait}, 12, S2),
    {ok, S4, _} = at({leave_queue, k, lease2}, 13, S3),
    [] = [W || W <- portunus_machine:query_contenders(k, S4), W =:= o2],
    [o2] = portunus_machine:query_contenders(k2, S4).

%% After the bid is withdrawn, a release promotes the remaining waiter, and
%% with no waiters left the key frees: the lease that left can never be
%% granted the key again.
lease_that_left_is_never_promoted(_Config) ->
    {Tok, S0} = held_with_waiters(),
    {ok, S1, _} = at({leave_queue, k, lease2}, 10, S0),
    {ok, S2, _} = at({release, k, Tok}, 11, S1),
    {ok, #{owner := o3, token := Tok2}} = portunus_machine:query_owner(k, S2),
    {ok, S3, _} = at({release, k, Tok2}, 12, S2),
    {error, not_held} = portunus_machine:query_owner(k, S3).

%% Helpers

%% Holder o1 (under lease1) of key k, with waiters o2 (lease2, the higher
%% score) and o3 (lease3).
held_with_waiters() ->
    S0 = portunus_machine:init(#{cluster => test}),
    {{ok, lease1}, S1, _} = at({grant_lease, lease1, 100000, o1, dummy_pid()}, 1, S0),
    {{ok, Tok}, S2, _} = at({acquire, lease1, k, o1, undefined, nowait}, 2, S1),
    {{ok, lease2}, S3, _} = at({grant_lease, lease2, 100000, o2, dummy_pid()}, 3, S2),
    {{ok, lease3}, S4, _} = at({grant_lease, lease3, 100000, o3, dummy_pid()}, 4, S3),
    {{queued, 1}, S5, _} = at({acquire, lease2, k, o2, undefined, wait, 5}, 5, S4),
    {{queued, 2}, S6, _} = at({acquire, lease3, k, o3, undefined, wait, 0}, 6, S5),
    {Tok, S6}.

at(Cmd, Ix, State) ->
    Meta = portunus_test_helpers:meta(Ix, 0),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.

dummy_pid() ->
    spawn(fun() -> timer:sleep(infinity) end).
