%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_expire_leases_unit_SUITE).

%% The `apply/3` surface behind off-log expiry: fenced `{expire_leases, ...}`
%% revoking through the ordinary revoke path, grants stamping `refreshed`
%% and emitting the `{aux, {refreshed, ...}}` deadline extension, and no
%% expiry timers anywhere.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([expire_with_matching_fence_revokes_and_promotes/1,
         expire_notifies_holder_and_watchers/1,
         stale_fence_pair_is_skipped_rest_applies/1,
         grants_emit_refreshed_effect/1,
         grant_arms_no_timer/1]).

all() ->
    [expire_with_matching_fence_revokes_and_promotes,
     expire_notifies_holder_and_watchers,
     stale_fence_pair_is_skipped_rest_applies,
     grants_emit_refreshed_effect,
     grant_arms_no_timer].

new() ->
    portunus_machine:init(#{cluster => expire_test}).

at(Cmd, Ix, S) ->
    portunus_machine:apply(portunus_test_helpers:meta(Ix), Cmd, S).

fence(LeaseId, S) ->
    {_Ttl, Fence} = maps:get(LeaseId, portunus_machine:lease_view(S)),
    Fence.

expire_with_matching_fence_revokes_and_promotes(_Config) ->
    P2 = spawn(fun() -> receive stop -> ok end end),
    S0 = new(),
    {S1, {ok, l1}, _} = at({grant_lease, l1, 1000, o1, undefined}, 1, S0),
    {S2, {ok, _T}, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, S1),
    {S3, {ok, l2}, _} = at({grant_lease, l2, 1000, o2, P2}, 3, S2),
    {S4, {queued, 1}, _} = at({acquire, l2, k, o2, undefined, wait}, 4, S3),
    {S5, ok, Effs} = at({expire_leases, [{l1, fence(l1, S4)}]}, 5, S4),
    ?assertMatch({ok, #{owner := o2}}, portunus_machine:query_owner(k, S5)),
    ?assertNot(maps:is_key(l1, portunus_machine:lease_view(S5))),
    ?assert(lists:any(fun({send_msg, Pid, {portunus, granted, k, _, l2}}) ->
                              Pid =:= P2;
                         (_) -> false
                      end, Effs)),
    P2 ! stop.

expire_notifies_holder_and_watchers(_Config) ->
    S0 = new(),
    {S1, {ok, l1}, _} = at({grant_lease, l1, 1000, o1, self()}, 1, S0),
    {S2, {ok, _T}, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, S1),
    {S3, {ok, Ref}, _} = at({watch, k, self()}, 3, S2),
    {_S4, ok, Effs} = at({expire_leases, [{l1, fence(l1, S3)}]}, 4, S3),
    ?assert(lists:member({send_msg, self(),
                          {portunus, lease_lost, l1}, [local]}, Effs)),
    ?assert(lists:member({send_msg, self(),
                          {portunus, watch, Ref, released}}, Effs)).

stale_fence_pair_is_skipped_rest_applies(_Config) ->
    S0 = new(),
    {S1, {ok, l1}, _} = at({grant_lease, l1, 1000, o1, undefined}, 1, S0),
    {S2, {ok, l2}, _} = at({grant_lease, l2, 1000, o2, undefined}, 2, S1),
    StaleFence = fence(l1, S2),
    %% A re-grant outran the proposal: l1's fence no longer matches.
    {S3, {ok, l1}, _} = at({grant_lease, l1, 1000, o1, undefined}, 3, S2),
    {S4, ok, _} = at({expire_leases,
                      [{l1, StaleFence}, {l2, fence(l2, S3)}]}, 4, S3),
    View = portunus_machine:lease_view(S4),
    ?assert(maps:is_key(l1, View)),
    ?assertNot(maps:is_key(l2, View)).

grants_emit_refreshed_effect(_Config) ->
    S0 = new(),
    {S1, {ok, l1}, E1} = at({grant_lease, l1, 1000, o1, undefined}, 1, S0),
    ?assert(lists:member({aux, {refreshed, [l1]}}, E1)),
    ?assertEqual(1, fence(l1, S1)),
    %% The idempotent same-owner re-grant is a logged refresh too.
    {S2, {ok, l1}, E2} = at({grant_lease, l1, 2000, o1, undefined}, 2, S1),
    ?assert(lists:member({aux, {refreshed, [l1]}}, E2)),
    ?assertEqual(2, fence(l1, S2)).

grant_arms_no_timer(_Config) ->
    S0 = new(),
    {S1, {ok, l1}, Effs} = at({grant_lease, l1, 1000, o1, undefined}, 1, S0),
    ?assertNot(lists:any(fun({timer, _, _}) -> true;
                            (_) -> false
                         end, Effs)),
    ?assertNot(lists:any(fun({timer, _, _}) -> true;
                            (_) -> false
                         end, portunus_machine:state_enter(leader, S1))).
