%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_rescore_prop_SUITE).

-include_lib("proper/include/proper.hrl").

-export([all/0, rebids_preserve_ranked_promotion/1]).

-define(KEYS, [k1, k2]).
-define(LEASES, [la, lb, lc]).

all() ->
    [rebids_preserve_ranked_promotion].

rebids_preserve_ranked_promotion(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_rebids_preserve_ranked_promotion/0, 500).

prop_rebids_preserve_ranked_promotion() ->
    ?FORALL(Ops, list(op()),
            begin
                Pids = maps:from_list(
                         [{L, spawn(fun() -> timer:sleep(infinity) end)}
                          || L <- ?LEASES]),
                S0 = portunus_machine:init(#{cluster => rescore_prop}),
                M0 = #{live => #{}, holders => #{}, waiters => #{},
                       max => #{}},
                {_, _, Ok} = run(Ops, 1, S0, M0, Pids, true),
                _ = [exit(P, kill) || P <- maps:values(Pids)],
                Ok
            end).

%% `acquire_wait` doubles as the re-bid: an already-queued lease submitting
%% again refreshes its score. Short TTLs make `expire` bite mid-run.
op() ->
    oneof([{grant, oneof(?LEASES), oneof([2, 4, 1000000])},
           {acquire_nowait, oneof(?LEASES), oneof(?KEYS)},
           {acquire_wait, oneof(?LEASES), oneof(?KEYS), choose(-2, 3)},
           {acquire_wait, oneof(?LEASES), oneof(?KEYS), choose(-2, 3)},
           {release, oneof(?KEYS)},
           {revoke, oneof(?LEASES)},
           {down, oneof(?LEASES)},
           {down_noconn, oneof(?LEASES)},
           {leave, oneof(?LEASES), oneof(?KEYS)},
           {expire}]).

run([], _Ix, S, M, _Pids, Ok) ->
    {S, M, Ok};
run([Op | Rest], Ix, S0, M0, Pids, Ok0) ->
    {S1, M1} = step(Op, Ix, S0, M0, Pids),
    {Ok, M2} = check(S1, M1),
    run(Rest, Ix + 1, S1, M2, Pids, Ok0 andalso Ok).

step({grant, L, Ttl}, Ix, S0, M0, Pids) ->
    {_, S1} = apply_cmd({grant_lease, L, Ttl, {o, L}, maps:get(L, Pids)},
                        Ix, S0),
    {S1, M0#{live := maps:put(L, Ix + Ttl, maps:get(live, M0))}};
step({acquire_nowait, L, K}, Ix, S0, M0, _Pids) ->
    {_, S1} = apply_cmd({acquire, L, K, {o, L}, undefined, nowait}, Ix, S0),
    {S1, model_acquire(nowait, L, K, 0, Ix, M0)};
step({acquire_wait, L, K, Score}, Ix, S0, M0, _Pids) ->
    {_, S1} = apply_cmd({acquire, L, K, {o, L}, undefined, wait, Score},
                        Ix, S0),
    {S1, model_acquire(wait, L, K, Score, Ix, M0)};
step({release, K}, Ix, S0, M0, _Pids) ->
    %% Release with the current token; a key nobody holds is a no-op in
    %% both machine and model.
    case portunus_machine:query_owner(K, S0) of
        {ok, #{token := T}} ->
            {_, S1} = apply_cmd({release, K, T}, Ix, S0),
            {S1, model_free(K, M0)};
        {error, not_held} ->
            {S0, M0}
    end;
step({revoke, L}, Ix, S0, M0, _Pids) ->
    {_, S1} = apply_cmd({revoke_lease, L}, Ix, S0),
    {S1, model_drop([L], M0)};
step({down, L}, Ix, S0, M0, Pids) ->
    {_, S1} = apply_cmd({down, maps:get(L, Pids), normal}, Ix, S0),
    {S1, model_drop([L], M0)};
step({down_noconn, L}, Ix, S0, M0, Pids) ->
    %% A netsplit blip: the lease and its claims survive.
    {_, S1} = apply_cmd({down, maps:get(L, Pids), noconnection}, Ix, S0),
    {S1, M0};
step({leave, L, K}, Ix, S0, M0, _Pids) ->
    {_, S1} = apply_cmd({leave_queue, K, L}, Ix, S0),
    Ws = maps:remove(L, key_waiters(K, M0)),
    {S1, set_waiters(K, Ws, M0)};
step({expire}, Ix, S0, M0, _Pids) ->
    %% The sweep is aux-side; the pairs are built as the leader would,
    %% fenced with each lease's current `refreshed` index.
    View = portunus_machine:lease_view(S0),
    Expired = [L || {L, D} <- maps:to_list(maps:get(live, M0)), D =< Ix],
    Pairs = [{L, Fence} || L <- Expired,
                           {_Ttl, Fence} <- [maps:get(L, View, undefined)]],
    {_, S1} = apply_cmd({expire_leases, Pairs}, Ix, S0),
    {S1, model_drop(Expired, M0)}.

%%----------------------------------------------------------------------
%% The model: holders and per-key waiting bids
%%----------------------------------------------------------------------

%% An acquire by a dead lease has no effect.
%% A live lease acquiring its own held key is idempotent.
%% A free key is granted in either mode.
%% A held key conflicts in `nowait` mode; in the `wait` mode,
%% the first bid enqueues at this index and a re-bid refreshes the score
%% but keeps the original `seq`.
model_acquire(Mode, L, K, Score, Ix, M0) ->
    Holder = maps:get(K, maps:get(holders, M0), undefined),
    case {maps:is_key(L, maps:get(live, M0)), Holder, Mode} of
        {false, _, _} ->
            M0;
        {true, L, _} ->
            M0;
        {true, undefined, _} ->
            M0#{holders := maps:put(K, L, maps:get(holders, M0))};
        {true, _, nowait} ->
            M0;
        {true, _, wait} ->
            Ws0 = key_waiters(K, M0),
            Ws = case maps:find(L, Ws0) of
                     {ok, {_Old, Seq}} -> maps:put(L, {Score, Seq}, Ws0);
                     error -> maps:put(L, {Score, Ix}, Ws0)
                 end,
            set_waiters(K, Ws, M0)
    end.

%% Free `K` and promote the best surviving waiter, exactly as the machine
%% ranks: maximum `{Score, -Seq}`.
model_free(K, M0) ->
    Holders0 = maps:remove(K, maps:get(holders, M0)),
    case best_waiter(key_waiters(K, M0)) of
        undefined ->
            set_waiters(K, #{}, M0#{holders := Holders0});
        L ->
            Ws = maps:remove(L, key_waiters(K, M0)),
            set_waiters(K, Ws, M0#{holders := maps:put(K, L, Holders0)})
    end.

%% Leases leaving (a revoke, a `down`, an expiry batch): purge their bids
%% everywhere, then free each key they held.
model_drop(Leases, M0) ->
    Dead = maps:from_keys(Leases, true),
    Live = maps:without(Leases, maps:get(live, M0)),
    Waiters = maps:map(fun(_K, Ws) -> maps:without(Leases, Ws) end,
                       maps:get(waiters, M0)),
    M1 = M0#{live := Live, waiters := Waiters},
    Freed = [K || {K, L} <- maps:to_list(maps:get(holders, M1)),
                  is_map_key(L, Dead)],
    lists:foldl(fun model_free/2, M1, Freed).

best_waiter(Ws) when map_size(Ws) =:= 0 ->
    undefined;
best_waiter(Ws) ->
    {L, _} = maps:fold(
               fun(L, {Score, Seq}, undefined) ->
                       {L, {Score, -Seq}};
                  (L, {Score, Seq}, {_BestL, BestRank} = Best) ->
                       case {Score, -Seq} > BestRank of
                           true -> {L, {Score, -Seq}};
                           false -> Best
                       end
               end, undefined, Ws),
    L.

key_waiters(K, M) ->
    maps:get(K, maps:get(waiters, M), #{}).

set_waiters(K, Ws, M) ->
    M#{waiters := maps:put(K, Ws, maps:get(waiters, M))}.

apply_cmd(Cmd, Ix, State) ->
    Meta = portunus_test_helpers:meta(Ix),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S};
        {S, Reply, _Effects} -> {Reply, S}
    end.

%% The machine's owner must be exactly the model's predicted holder, and
%% the fencing token must never regress.
check(S, M) ->
    Holders = maps:get(holders, M),
    {Ok, Max} =
        lists:foldl(
          fun(K, {Acc, Max0}) ->
                  Want = maps:get(K, Holders, undefined),
                  case {portunus_machine:query_owner(K, S), Want} of
                      {{error, not_held}, undefined} ->
                          {Acc, Max0};
                      {{ok, #{lease := L, token := T}}, L} ->
                          PrevMax = maps:get(K, Max0, -1),
                          {Acc andalso T >= PrevMax, maps:put(K, T, Max0)};
                      _ ->
                          {false, Max0}
                  end
          end, {true, maps:get(max, M)}, ?KEYS),
    {Ok, M#{max := Max}}.
