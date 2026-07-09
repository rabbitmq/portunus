%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_machine_model_prop_SUITE).

%% A model-based property over the whole lease lifecycle, driving `apply/3`
%% against a reference model. Where `portunus_machine_prop` checks acquire and
%% release, this interleaves grant, renew, revoke, and monitor-driven `down`
%% as well, so cross-command paths (release_pid, multi-key revoke) are
%% exercised together. Waiter succession is covered in
%% `portunus_succession_prop_SUITE`; this run uses `nowait` acquires only.

-include_lib("proper/include/proper.hrl").

-export([all/0, lifecycle_safety/1]).

-define(KEYS, [k1, k2]).
-define(LEASES, [la, lb, lc]).
-define(BIG_TTL, 100000000).

all() ->
    [lifecycle_safety].

lifecycle_safety(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_lifecycle_preserves_safety/0, 500).

%% However grant, renew, revoke, down, acquire, and release interleave, the
%% machine keeps at most one owner per key, fencing tokens that never go
%% backwards, and no key held by a lease that is gone.
prop_lifecycle_preserves_safety() ->
    ?FORALL(Ops, list(op()),
            begin
                %% One distinct pid per lease, so a `down` targets exactly
                %% that lease; killed at the end so runs leak nothing.
                Pids = maps:from_list(
                         [{L, spawn(fun() -> timer:sleep(infinity) end)}
                          || L <- ?LEASES]),
                S0 = portunus_machine:init(#{cluster => prop}),
                Model0 = #{leases => #{}, locks => #{}, max => #{}},
                {_, _, Ok} = run(Ops, 1, S0, Model0, Pids, true),
                _ = [exit(P, kill) || P <- maps:values(Pids)],
                Ok
            end).

op() ->
    oneof([{grant, oneof(?LEASES)},
           {renew, oneof(?LEASES)},
           {revoke, oneof(?LEASES)},
           {down, oneof(?LEASES)},
           {down_noconn, oneof(?LEASES)},
           {acquire, oneof(?LEASES), oneof(?KEYS)},
           {release, oneof(?LEASES), oneof(?KEYS)}]).

run([], _Ix, S, Model, _Pids, Ok) ->
    {S, Model, Ok};
run([Op | Rest], Ix, S0, Model0, Pids, Ok0) ->
    {S1, Model1} = step(Op, Ix, S0, Model0, Pids),
    Ok1 = Ok0 andalso invariants_hold(S1, Model1),
    run(Rest, Ix + 1, S1, Model1, Pids, Ok1).

step({grant, L}, Ix, S0, M0, Pids) ->
    {_, S1} = apply_cmd({grant_lease, L, ?BIG_TTL, {o, L}, maps:get(L, Pids)},
                        Ix, S0),
    {S1, M0#{leases := maps:put(L, true, maps:get(leases, M0))}};
step({renew, L}, Ix, S0, M0, _Pids) ->
    {_, S1} = apply_cmd({renew, [L]}, Ix, S0),
    {S1, M0};
step({revoke, L}, Ix, S0, M0, _Pids) ->
    {_, S1} = apply_cmd({revoke_lease, L}, Ix, S0),
    {S1, forget_lease(L, M0)};
step({down, L}, Ix, S0, M0, Pids) ->
    {_, S1} = apply_cmd({down, maps:get(L, Pids), normal}, Ix, S0),
    {S1, forget_lease(L, M0)};
%% Unreachable, not dead: the lease and its locks must survive.
step({down_noconn, L}, Ix, S0, M0, Pids) ->
    {_, S1} = apply_cmd({down, maps:get(L, Pids), noconnection}, Ix, S0),
    {S1, M0};
step({acquire, L, K}, Ix, S0, M0, _Pids) ->
    {Reply, S1} = apply_cmd({acquire, L, K, {o, L}, undefined, nowait}, Ix, S0),
    {S1, model_acquire(Reply, L, K, M0)};
step({release, L, K}, Ix, S0, M0, _Pids) ->
    %% Release with the model's token for the key, so only the actual holder
    %% can free it; a stale token must be rejected by the machine.
    Token = case maps:get(K, maps:get(locks, M0), undefined) of
                {L, T} -> T;
                _ -> -1
            end,
    {Reply, S1} = apply_cmd({release, K, Token}, Ix, S0),
    M1 = case Reply of
             ok -> M0#{locks := maps:remove(K, maps:get(locks, M0))};
             _ -> M0
         end,
    {S1, M1}.

forget_lease(L, M0) ->
    Leases = maps:remove(L, maps:get(leases, M0)),
    Locks = maps:filter(fun(_K, {Owner, _T}) -> Owner =/= L end,
                        maps:get(locks, M0)),
    M0#{leases := Leases, locks := Locks}.

model_acquire({ok, Token}, L, K, M0) ->
    Locks0 = maps:get(locks, M0),
    case maps:get(K, Locks0, undefined) of
        {L, _} ->
            M0;
        undefined ->
            Max0 = maps:get(max, M0),
            M0#{locks := maps:put(K, {L, Token}, Locks0),
                max := maps:put(K, max(Token, maps:get(K, Max0, -1)), Max0)};
        {_Other, _} ->
            M0
    end;
model_acquire(_Reply, _L, _K, M0) ->
    M0.

apply_cmd(Cmd, Ix, State) ->
    Meta = portunus_test_helpers:meta(Ix),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S};
        {S, Reply, _Effects} -> {Reply, S}
    end.

%% The machine's owner for every key matches the model, and the token sits at
%% the per-key historical maximum (so it never regressed).
invariants_hold(S, M) ->
    Locks = maps:get(locks, M),
    Max = maps:get(max, M),
    lists:all(
      fun(K) ->
              case {portunus_machine:query_owner(K, S),
                    maps:get(K, Locks, undefined)} of
                  {{error, not_held}, undefined} ->
                      true;
                  {{ok, #{lease := L, token := T}}, {L, T}} ->
                      T =:= maps:get(K, Max);
                  _ ->
                      false
              end
      end, ?KEYS).
