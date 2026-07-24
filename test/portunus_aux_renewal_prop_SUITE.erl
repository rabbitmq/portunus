%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_aux_renewal_prop_SUITE).

%% The central property of the off-log renewal design, next to
%% at-most-one-owner: no early expiry. Random interleavings of grant, aux
%% renew, sweep ticks, term changes, and, above all, delayed application of
%% in-flight expiry proposals run against the pure aux core and the real
%% `apply/3`, on a modeled clock. A lease must never be revoked while it is
%% within its TTL of the last acknowledged promise (a grant `ok` or a renew
%% `ok`); the `refreshed` fence and the pending set exist to close exactly
%% the races the delayed-apply operations create.

-include_lib("proper/include/proper.hrl").

-export([all/0, no_early_expiry/1]).

-define(LEASES, [la, lb, lc]).

all() ->
    [no_early_expiry].

no_early_expiry(_Config) ->
    true = portunus_test_helpers:quickcheck(fun prop_no_early_expiry/0, 500).

op() ->
    oneof([{grant, oneof(?LEASES), oneof([50, 200, 1000])},
           {renew_aux, oneof(?LEASES)},
           {revoke, oneof(?LEASES)},
           {tick, choose(1, 400)},
           %% A sweep whose proposal stays in flight: the command is
           %% appended but not yet applied.
           {tick_hold, choose(1, 400)},
           %% Apply the oldest in-flight proposal, possibly long after
           %% grants and renewals overtook it.
           apply_inflight,
           %% A leadership change. Ra queues `consistent_aux` calls until the
           %% new term's noop commits, which first commits and applies every
           %% surviving prior-term entry, so an in-flight proposal either
           %% applies before any new-term renewal is acknowledged
           %% (`term_change`) or was discarded with the divergent log
           %% (`term_change_drop`). A new-term renewal racing an unapplied
           %% old-term proposal is not a reachable ordering.
           term_change,
           term_change_drop]).

prop_no_early_expiry() ->
    ?FORALL(Ops, list(op()),
            begin
                S0 = portunus_machine:init(#{cluster => prop_aux}),
                M0 = #{aux => portunus_machine_aux:new(),
                       term => 1, now => 0, ix => 1,
                       promises => #{}, inflight => [], ok => true},
                {_S, MFinal} = lists:foldl(fun step/2, {S0, M0}, Ops),
                maps:get(ok, MFinal)
            end).

step(_Op, {S, #{ok := false} = M}) ->
    {S, M};
step({grant, L, Ttl}, {S0, #{ix := Ix, now := Now} = M0}) ->
    {S1, Reply, _} = apply_at({grant_lease, L, Ttl, {o, L}, undefined}, S0, M0),
    M1 = M0#{ix := Ix + 1},
    case Reply of
        {ok, L} ->
            %% The grant acknowledgment promises a full TTL; run the
            %% `{aux, {refreshed, ...}}` effect the leader would.
            Aux = portunus_machine_aux:refreshed(
                    maps:get(aux, M1), portunus_machine:lease_view(S1),
                    maps:get(term, M1), Now, [L]),
            {S1, M1#{aux := Aux,
                     promises := (maps:get(promises, M1))#{L => Now + Ttl}}};
        _ ->
            {S1, M1}
    end;
step({renew_aux, L}, {S, #{now := Now} = M0}) ->
    View = portunus_machine:lease_view(S),
    {Aux, Results} = portunus_machine_aux:renew(
                       maps:get(aux, M0), View, maps:get(term, M0), Now, [L]),
    M1 = M0#{aux := Aux},
    case Results of
        [{L, ok}] ->
            {Ttl, _} = maps:get(L, View),
            {S, M1#{promises := (maps:get(promises, M1))#{L => Now + Ttl}}};
        _ ->
            {S, M1}
    end;
step({revoke, L}, {S0, #{ix := Ix} = M0}) ->
    {S1, _, _} = apply_at({revoke_lease, L}, S0, M0),
    {S1, M0#{ix := Ix + 1,
             promises := maps:remove(L, maps:get(promises, M0))}};
step({tick, Dt}, {S, M0}) ->
    {M1, Pairs} = sweep(S, M0, Dt),
    do_apply_pairs(Pairs, {S, M1});
step({tick_hold, Dt}, {S, M0}) ->
    {M1, Pairs} = sweep(S, M0, Dt),
    {S, M1#{inflight := maps:get(inflight, M1) ++ [Pairs]}};
step(apply_inflight, {S, M0}) ->
    case maps:get(inflight, M0) of
        [] -> {S, M0};
        [Pairs | Rest] -> do_apply_pairs(Pairs, {S, M0#{inflight := Rest}})
    end;
step(term_change, {S0, M0}) ->
    {S1, M1} = lists:foldl(fun do_apply_pairs/2, {S0, M0#{inflight := []}},
                           maps:get(inflight, M0)),
    {S1, M1#{term := maps:get(term, M1) + 1}};
step(term_change_drop, {S, M0}) ->
    {S, M0#{term := maps:get(term, M0) + 1, inflight := []}}.

sweep(S, M0, Dt) ->
    Now = maps:get(now, M0) + Dt,
    {Aux, Pairs} = portunus_machine_aux:leader_tick(
                     maps:get(aux, M0), portunus_machine:lease_view(S),
                     maps:get(term, M0), Now),
    {M0#{now := Now, aux := Aux}, Pairs}.

%% The check: applying a proposal must never revoke a lease that is still
%% inside an acknowledged promise window.
do_apply_pairs(Pairs, {S0, #{ix := Ix, now := Now} = M0}) ->
    View = portunus_machine:lease_view(S0),
    Revoked = [Id || {Id, Fence} <- Pairs,
                     case maps:get(Id, View, undefined) of
                         {_Ttl, Fence} -> true;
                         _ -> false
                     end],
    Early = [Id || Id <- Revoked,
                   maps:get(Id, maps:get(promises, M0), 0) > Now],
    {S1, ok, _} = apply_at({expire_leases, Pairs}, S0, M0),
    Ok = maps:get(ok, M0) andalso Early =:= [],
    {S1, M0#{ix := Ix + 1, ok := Ok,
             promises := maps:without(Revoked, maps:get(promises, M0))}}.

apply_at(Cmd, S, #{ix := Ix, now := Now}) ->
    portunus_machine:apply(portunus_test_helpers:meta(Ix, Now), Cmd, S).
