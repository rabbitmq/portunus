%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_machine_SUITE).

%% Unit tests for the state machine, driving `apply/3` directly, with no Ra and no
%% timing, for the paths the integration suite reaches only indirectly.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([grant_acquire_release/1,
         id_in_use/1,
         clean_death_releases/1,
         unreachable_does_not_release/1,
         fifo_succession_and_grant_msg/1,
         watch_events/1,
         unwatch_prunes/1,
         release_errors/1,
         batch_renew_mixed/1,
         expiry/1]).

all() ->
    [grant_acquire_release,
     id_in_use,
     clean_death_releases,
     unreachable_does_not_release,
     fifo_succession_and_grant_msg,
     watch_events,
     unwatch_prunes,
     release_errors,
     batch_renew_mixed,
     expiry].

grant_acquire_release(_Config) ->
    S0 = new(),
    {{ok, l1}, S1, _} = step({grant_lease, l1, 1000, owner1, self()}, 1, S0),
    {{ok, T}, S2, _} = step({acquire, l1, k, owner1, undefined, nowait}, 2, S1),
    {ok, #{owner := owner1, token := T}} = portunus_machine:query_owner(k, S2),
    {ok, S3, _} = step({release, k, T}, 3, S2),
    {error, not_held} = portunus_machine:query_owner(k, S3).

id_in_use(_Config) ->
    S0 = new(),
    {{ok, l1}, S1, _} = step({grant_lease, l1, 1000, owner_a, self()}, 1, S0),
    %% Same proposed id, different owner: rejected.
    {{error, id_in_use}, _S2, _} =
        step({grant_lease, l1, 1000, owner_b, self()}, 2, S1).

clean_death_releases(_Config) ->
    P = dummy_pid(),
    S0 = new(),
    {{ok, l1}, S1, _} = step({grant_lease, l1, 1000, owner1, P}, 1, S0),
    {{ok, _T}, S2, _} = step({acquire, l1, k, owner1, undefined, nowait}, 2, S1),
    %% A genuine local death releases the holder's locks.
    {ok, S3, _} = step({down, P, normal}, 3, S2),
    {error, not_held} = portunus_machine:query_owner(k, S3).

unreachable_does_not_release(_Config) ->
    P = dummy_pid(),
    S0 = new(),
    {{ok, l1}, S1, _} = step({grant_lease, l1, 1000, owner1, P}, 1, S0),
    {{ok, T}, S2, _} = step({acquire, l1, k, owner1, undefined, nowait}, 2, S1),
    %% noconnection means "unreachable, not dead": the lock is kept.
    {ok, S3} = bare_step({down, P, noconnection}, 3, S2),
    {ok, #{token := T}} = portunus_machine:query_owner(k, S3).

fifo_succession_and_grant_msg(_Config) ->
    P1 = dummy_pid(), P2 = dummy_pid(), P3 = dummy_pid(),
    S0 = new(),
    {{ok, l1}, S1, _} = step({grant_lease, l1, 1000, o1, P1}, 1, S0),
    {{ok, l2}, S2, _} = step({grant_lease, l2, 1000, o2, P2}, 2, S1),
    {{ok, l3}, S3, _} = step({grant_lease, l3, 1000, o3, P3}, 3, S2),
    {{ok, T1}, S4, _} = step({acquire, l1, k, o1, undefined, nowait}, 4, S3),
    %% Two contenders queue, in order.
    {{queued, 1}, S5, _} = step({acquire, l2, k, o2, undefined, wait}, 5, S4),
    {{queued, 2}, S6, _} = step({acquire, l3, k, o3, undefined, wait}, 6, S5),
    %% Releasing the holder promotes the head (l2) and notifies its pid.
    {ok, S7, E7} = step({release, k, T1}, 7, S6),
    {ok, #{owner := o2}} = portunus_machine:query_owner(k, S7),
    %% The grant carries the promoted lease id so the waiter can match it.
    ?assertMatch({T, l2} when is_integer(T), grant_token(E7, P2, k)),
    %% Revoking l2 promotes l3 next (FIFO order preserved).
    {ok, S8, E8} = step({revoke_lease, l2}, 8, S7),
    {ok, #{owner := o3}} = portunus_machine:query_owner(k, S8),
    ?assertMatch({T, l3} when is_integer(T), grant_token(E8, P3, k)).

watch_events(_Config) ->
    W = dummy_pid(),
    S0 = new(),
    {{ok, Ref}, S1, _} = step({watch, k, W}, 1, S0),
    {{ok, l1}, S2, _} = step({grant_lease, l1, 1000, owner1, self()}, 2, S1),
    {{ok, T}, S3, E3} = step({acquire, l1, k, owner1, undefined, nowait}, 3, S2),
    ?assert(lists:member({send_msg, W, {portunus, watch, Ref, {acquired, owner1}}},
                         E3)),
    {ok, _S4, E4} = step({release, k, T}, 4, S3),
    ?assert(lists:member({send_msg, W, {portunus, watch, Ref, released}}, E4)).

unwatch_prunes(_Config) ->
    W = dummy_pid(),
    S0 = new(),
    {{ok, Ref}, S1, _} = step({watch, k, W}, 1, S0),
    #{watchers := 1} = portunus_machine:overview(S1),
    {ok, S2, _} = step({unwatch, Ref}, 2, S1),
    %% The key is dropped once its last watcher leaves, not kept empty.
    #{watchers := 0} = portunus_machine:overview(S2),
    {{ok, l1}, S3, _} = step({grant_lease, l1, 1000, o, self()}, 3, S2),
    {{ok, _T}, _S4, E4} = step({acquire, l1, k, o, undefined, nowait}, 4, S3),
    %% A watcher that already left receives nothing.
    [] = [M || {send_msg, P, _} = M <- E4, P =:= W].

release_errors(_Config) ->
    S0 = new(),
    %% Releasing a key nobody holds.
    {{error, not_held}, S1} = bare_step({release, k, 1}, 1, S0),
    {{ok, l1}, S2, _} = step({grant_lease, l1, 1000, o, self()}, 2, S1),
    {{ok, T}, S3, _} = step({acquire, l1, k, o, undefined, nowait}, 3, S2),
    %% A stale token does not release the current holder.
    {{error, not_owner}, _} = bare_step({release, k, T + 1}, 4, S3).

batch_renew_mixed(_Config) ->
    S0 = new(),
    {{ok, l1}, S1, _} = step({grant_lease, l1, 1000, o, self()}, 1, S0),
    {Results, _S2, _} = step({renew, [l1, missing]}, 2, S1),
    ?assertEqual([{l1, ok}, {missing, {error, lease_expired}}], Results).

expiry(_Config) ->
    %% Grant at t=0 with a 100ms TTL, sweep at t=200: the lease is gone.
    S0 = new(),
    {{ok, l1}, S1, _} = apply_at({grant_lease, l1, 100, o, self()}, 1, 0, S0),
    {{ok, _T}, S2, _} = apply_at({acquire, l1, k, o, undefined, nowait}, 2, 0, S1),
    {ok, S3, _} = apply_at({timeout, expire}, 3, 200, S2),
    {error, not_held} = portunus_machine:query_owner(k, S3).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

new() ->
    portunus_machine:init(#{cluster => test}).

%% Apply with system_time == index (fine for tests that don't exercise TTL).
step(Cmd, Ix, State) ->
    apply_at(Cmd, Ix, Ix, State).

apply_at(Cmd, Ix, Now, State) ->
    Meta = portunus_test_helpers:meta(Ix, Now),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.

%% For the two-tuple (no-effects) replies we want to assert on directly.
bare_step(Cmd, Ix, State) ->
    {Reply, S, _} = apply_at(Cmd, Ix, Ix, State),
    {Reply, S}.

dummy_pid() ->
    spawn(fun() -> timer:sleep(infinity) end).

%% Returns {Token, LeaseId} from the grant sent to Pid for Key.
grant_token(Effects, Pid, Key) ->
    case [{T, L} || {send_msg, P, {portunus, granted, K, T, L}} <- Effects,
                    P =:= Pid, K =:= Key] of
        [TL | _] -> TL;
        [] -> false
    end.
