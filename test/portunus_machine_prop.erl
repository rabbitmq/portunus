%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_machine_prop).

%% Property-based test of the core safety invariant, driving the state
%% machine's `apply/3` directly (no Ra needed) against a reference model:
%%
%%   * at most one owner per key, and the machine agrees with the model
%%   * fencing tokens strictly increase per key across re-grants
%%
%% This is the property the design cares about most (AGENTS.md).

-include_lib("proper/include/proper.hrl").

-export([prop_single_owner_and_monotonic_tokens/0,
         prop_fifo_succession/0]).

-define(KEYS, [k1, k2, k3]).
-define(LEASES, [la, lb, lc]).
-define(BIG_TTL, 100000000).

prop_single_owner_and_monotonic_tokens() ->
    ?FORALL(Ops, list(op()),
            begin
                S0 = portunus_machine:init(#{cluster => prop}),
                Model0 = #{leases => #{}, locks => #{}, max => #{}},
                {_S, _Model, Ok} = run(Ops, 1, S0, Model0, true),
                Ok
            end).

op() ->
    oneof([{grant, oneof(?LEASES)},
           {acquire, oneof(?LEASES), oneof(?KEYS)},
           {release, oneof(?LEASES), oneof(?KEYS)},
           {revoke, oneof(?LEASES)}]).

%% A key held by a blocker with N queued waiters must be granted to
%% those waiters in exact enqueue (FIFO) order as it is released.
prop_fifo_succession() ->
    ?FORALL(N, integer(1, 8),
            begin
                S0 = portunus_machine:init(#{cluster => prop}),
                {S1, Ix1, T0} = grant_acquire(blocker, key, S0, 1),
                {S2, Ix2} = enqueue_waiters(N, key, S1, Ix1),
                Owners = drain(N, key, S2, Ix2, T0),
                Owners =:= [{waiter, I} || I <- lists:seq(1, N)]
            end).

grant_acquire(Owner, Key, S0, Ix) ->
    {_, S1} = apply_cmd({grant_lease, Owner, ?BIG_TTL, Owner, undefined}, Ix, S0),
    {{ok, T}, S2} = apply_cmd({acquire, Owner, Key, Owner, undefined, nowait},
                              Ix + 1, S1),
    {S2, Ix + 2, T}.

enqueue_waiters(N, Key, S0, Ix0) ->
    lists:foldl(
      fun(I, {S, Ix}) ->
              W = {waiter, I},
              {_, S1} = apply_cmd({grant_lease, W, ?BIG_TTL, W, undefined}, Ix, S),
              {{queued, _}, S2} = apply_cmd({acquire, W, Key, W, undefined, wait},
                                            Ix + 1, S1),
              {S2, Ix + 2}
      end, {S0, Ix0}, lists:seq(1, N)).

%% Release the current holder N times, recording who holds the key after
%% each release. The owners must come out in enqueue order.
drain(N, Key, S0, Ix0, Token0) ->
    {_, _, {_, Acc}} =
        lists:foldl(
          fun(_, {S, Ix, {Token, A}}) ->
                  {ok, S1} = apply_cmd({release, Key, Token}, Ix, S),
                  {ok, #{owner := O, token := T}} =
                      portunus_machine:query_owner(Key, S1),
                  {S1, Ix + 1, {T, [O | A]}}
          end, {S0, Ix0, {Token0, []}}, lists:seq(1, N)),
    lists:reverse(Acc).

run([], _Ix, S, Model, Ok) ->
    {S, Model, Ok};
run([Op | Rest], Ix, S0, Model0, Ok0) ->
    {S1, Model1} = step(Op, Ix, S0, Model0),
    Ok1 = Ok0 andalso invariants_hold(S1, Model1),
    run(Rest, Ix + 1, S1, Model1, Ok1).

step({grant, L}, Ix, S0, Model0) ->
    {_Reply, S1} = apply_cmd({grant_lease, L, ?BIG_TTL, L, undefined}, Ix, S0),
    Leases = maps:put(L, true, maps:get(leases, Model0)),
    {S1, Model0#{leases := Leases}};
step({acquire, L, K}, Ix, S0, Model0) ->
    {Reply, S1} = apply_cmd({acquire, L, K, {owner, L}, undefined, nowait},
                            Ix, S0),
    Model1 = model_acquire(Reply, L, K, Model0),
    {S1, Model1};
step({release, L, K}, Ix, S0, Model0) ->
    %% Release by the model's current token for the key (if this lease holds it).
    Token = case maps:get(K, maps:get(locks, Model0), undefined) of
                {L, T} -> T;
                _ -> -1
            end,
    {Reply, S1} = apply_cmd({release, K, Token}, Ix, S0),
    Model1 = case Reply of
                 ok -> Model0#{locks := maps:remove(K, maps:get(locks, Model0))};
                 _ -> Model0
             end,
    {S1, Model1};
step({revoke, L}, Ix, S0, Model0) ->
    {_Reply, S1} = apply_cmd({revoke_lease, L}, Ix, S0),
    Leases = maps:remove(L, maps:get(leases, Model0)),
    Locks = maps:filter(fun(_K, {Owner, _T}) -> Owner =/= L end,
                        maps:get(locks, Model0)),
    {S1, Model0#{leases := Leases, locks := Locks}}.

model_acquire({ok, Token}, L, K, Model0) ->
    Locks0 = maps:get(locks, Model0),
    case maps:get(K, Locks0, undefined) of
        {L, _OldT} ->
            %% idempotent re-acquire: token must be unchanged
            Model0;
        undefined ->
            Max0 = maps:get(max, Model0),
            Model0#{locks := maps:put(K, {L, Token}, Locks0),
                    max := maps:put(K, max(Token, maps:get(K, Max0, -1)), Max0)};
        {_Other, _} ->
            %% machine reported ok but model thinks held by another:
            %% should not happen; leave model, invariants will flag it
            Model0
    end;
model_acquire(_Other, _L, _K, Model0) ->
    Model0.

apply_cmd(Cmd, Ix, State) ->
    Meta = portunus_test_helpers:meta(Ix),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S};
        {S, Reply, _Effects} -> {Reply, S}
    end.

%% The machine's view must match the model for every key, and tokens
%% must never go backwards for a key.
invariants_hold(S, Model) ->
    Locks = maps:get(locks, Model),
    Max = maps:get(max, Model),
    lists:all(
      fun(K) ->
              case {portunus_machine:query_owner(K, S), maps:get(K, Locks, undefined)} of
                  {{error, not_held}, undefined} ->
                      true;
                  {{ok, #{lease := L, token := T}}, {L, T}} ->
                      %% machine agrees, and token is at the per-key max
                      T >= maps:get(K, Max, -1) andalso T =:= maps:get(K, Max);
                  _ ->
                      false
              end
      end, ?KEYS).
