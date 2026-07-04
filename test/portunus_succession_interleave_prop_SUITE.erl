%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_succession_interleave_prop_SUITE).

%% Where `portunus_succession_prop` checks that releasing a holder promotes the
%% right waiter, this interleaves queued `wait` acquires with the other ways a
%% holder leaves (`revoke`, a monitored `down`, and lease expiry) and asserts
%% the safety net after every step: at most one owner per key, an owner whose
%% lease is still live, fencing tokens that never regress, and the property that
%% catches a dropped promotion, namely a key held exactly when some live lease
%% still wants it. Exact succession order is left to `portunus_succession_prop`;
%% this run needs only single-owner and that a freed key with live waiters is
%% never stranded.

-include_lib("proper/include/proper.hrl").

-export([all/0, interleaving_preserves_safety/1]).

-define(KEYS, [k1, k2]).
-define(LEASES, [la, lb, lc]).

all() ->
    [interleaving_preserves_safety].

interleaving_preserves_safety(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_interleaving_preserves_safety/0, 500).

prop_interleaving_preserves_safety() ->
    ?FORALL(Ops, list(op()),
            begin
                %% One distinct pid per lease so a `down` targets exactly that
                %% lease; killed at the end so a run leaks nothing.
                Pids = maps:from_list(
                         [{L, spawn(fun() -> timer:sleep(infinity) end)}
                          || L <- ?LEASES]),
                S0 = portunus_machine:init(#{cluster => prop}),
                M0 = #{live => #{}, claims => #{}, max => #{}},
                {_, _, Ok} = run(Ops, 1, S0, M0, Pids, true),
                _ = [exit(P, kill) || P <- maps:values(Pids)],
                Ok
            end).

%% Short ttls so an `expire` removes some leases while the long ones survive in
%% the same run.
op() ->
    oneof([{grant, oneof(?LEASES), oneof([2, 4, 1000000])},
           {acquire_nowait, oneof(?LEASES), oneof(?KEYS)},
           {acquire_wait, oneof(?LEASES), oneof(?KEYS), choose(-2, 3)},
           {revoke, oneof(?LEASES)},
           {down, oneof(?LEASES)},
           {down_noconn, oneof(?LEASES)},
           {expire}]).

run([], _Ix, S, M, _Pids, Ok) ->
    {S, M, Ok};
run([Op | Rest], Ix, S0, M0, Pids, Ok0) ->
    {S1, M1} = step(Op, Ix, S0, M0, Pids),
    {Ok, M2} = check(S1, M1),
    run(Rest, Ix + 1, S1, M2, Pids, Ok0 andalso Ok).

step({grant, L, Ttl}, Ix, S0, M0, Pids) ->
    {_, S1} = apply_cmd({grant_lease, L, Ttl, {o, L}, maps:get(L, Pids)}, Ix, S0),
    Live = maps:put(L, Ix + Ttl, maps:get(live, M0)),
    {S1, M0#{live := Live}};
step({acquire_nowait, L, K}, Ix, S0, M0, _Pids) ->
    {_, S1} = apply_cmd({acquire, L, K, {o, L}, undefined, nowait}, Ix, S0),
    {S1, model_acquire(nowait, L, K, M0)};
step({acquire_wait, L, K, Score}, Ix, S0, M0, _Pids) ->
    {_, S1} = apply_cmd({acquire, L, K, {o, L}, undefined, wait, Score}, Ix, S0),
    {S1, model_acquire(wait, L, K, M0)};
step({revoke, L}, Ix, S0, M0, _Pids) ->
    {_, S1} = apply_cmd({revoke_lease, L}, Ix, S0),
    {S1, drop_lease(L, M0)};
step({down, L}, Ix, S0, M0, Pids) ->
    {_, S1} = apply_cmd({down, maps:get(L, Pids), normal}, Ix, S0),
    {S1, drop_lease(L, M0)};
step({down_noconn, L}, Ix, S0, M0, Pids) ->
    %% The lease and its claims must survive a netsplit blip.
    {_, S1} = apply_cmd({down, maps:get(L, Pids), noconnection}, Ix, S0),
    {S1, M0};
step({expire}, Ix, S0, M0, _Pids) ->
    {_, S1} = apply_cmd({timeout, expire}, Ix, S0),
    Expired = [L || {L, D} <- maps:to_list(maps:get(live, M0)), D =< Ix],
    {S1, lists:foldl(fun drop_lease/2, M0, Expired)}.

%% A live lease claims a key. `nowait` claims only a free key (a busy key is a
%% conflict, not a queue); `wait` always claims. An acquire by a lease that is
%% not live is a no-op, mirroring the machine rejecting an unknown lease.
model_acquire(Mode, L, K, M0) ->
    case maps:is_key(L, maps:get(live, M0)) of
        false ->
            M0;
        true ->
            Cur = maps:get(K, maps:get(claims, M0), []),
            case {Mode, Cur, lists:member(L, Cur)} of
                {_, _, true}        -> M0;
                {nowait, [], false} -> set_claims(K, [L], M0);
                {nowait, _,  false} -> M0;
                {wait, _,    false} -> set_claims(K, Cur ++ [L], M0)
            end
    end.

drop_lease(L, M0) ->
    Live = maps:remove(L, maps:get(live, M0)),
    Claims = maps:map(fun(_K, Ls) -> [X || X <- Ls, X =/= L] end,
                      maps:get(claims, M0)),
    M0#{live := Live, claims := Claims}.

set_claims(K, Ls, M0) ->
    M0#{claims := maps:put(K, Ls, maps:get(claims, M0))}.

apply_cmd(Cmd, Ix, State) ->
    Meta = portunus_test_helpers:meta(Ix),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S};
        {S, Reply, _Effects} -> {Reply, S}
    end.

%% For every key: held exactly when a live lease still wants it; the owner is
%% one of those live claimants; and the fencing token never went backwards
%% across the run.
check(S, M) ->
    Claims = maps:get(claims, M),
    Live = maps:get(live, M),
    {Ok, Max} =
        lists:foldl(
          fun(K, {Acc, Max0}) ->
                  Want = maps:get(K, Claims, []),
                  case {portunus_machine:query_owner(K, S), Want} of
                      {{error, not_held}, []} ->
                          {Acc, Max0};
                      {{ok, #{lease := L, token := T}}, [_ | _]} ->
                          PrevMax = maps:get(K, Max0, -1),
                          Held = lists:member(L, Want)
                              andalso maps:is_key(L, Live)
                              andalso T >= PrevMax,
                          {Acc andalso Held, maps:put(K, T, Max0)};
                      _ ->
                          {false, Max0}
                  end
          end, {true, maps:get(max, M)}, ?KEYS),
    {Ok, M#{max := Max}}.
