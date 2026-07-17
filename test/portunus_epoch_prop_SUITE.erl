%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_epoch_prop_SUITE).

%% Property tests use PropEr, so this module includes only proper.hrl:
%% mixing it with eunit/ct headers redefines macros such as LET.
%%
%% The cross-incarnation guarantee the epoch adds, driven through real
%% `apply/3` calls: every client-facing identifier minted by a later
%% incarnation (a fresh machine whose first stamp is higher) exceeds every
%% identifier minted by an earlier one, whatever either incarnation did.

-include_lib("proper/include/proper.hrl").

-export([all/0, epoch_monotonic_across_incarnations/1]).

all() ->
    [epoch_monotonic_across_incarnations].

epoch_monotonic_across_incarnations(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_epoch_monotonic_across_incarnations/0, 300).

%% Run a random command sequence against incarnation A (first stamp `E1`)
%% and another against a fresh incarnation B stamped strictly later; every
%% token, auto-assigned lease id and watch reference of B must exceed every
%% one of A. The index part alone would order many of these the wrong way:
%% only the epoch carries the guarantee.
prop_epoch_monotonic_across_incarnations() ->
    ?FORALL({OpsA, OpsB, E1, Gap},
            {non_empty(list(op())), non_empty(list(op())),
             choose(1, 1 bsl 42), choose(1, 1 bsl 42)},
            begin
                IdsA = minted(OpsA, E1),
                IdsB = minted(OpsB, E1 + Gap),
                IdsA =:= [] orelse IdsB =:= [] orelse
                    lists:max(IdsA) < lists:min(IdsB)
            end).

-define(LEASES, [la, lb, lc]).
-define(KEYS, [k1, k2, k3]).

op() ->
    oneof([{grant, oneof(?LEASES)},
           {acquire, oneof(?LEASES), oneof(?KEYS)},
           {release, oneof(?KEYS)},
           {watch, oneof(?KEYS)}]).

%% Fold the ops over a fresh incarnation whose commands all carry `Epoch`
%% (only the first stamps it), collecting every identifier returned to a
%% client. Lease grants use auto-assigned ids, so grants mint too; an alias
%% map tracks them for the acquires.
minted(Ops, Epoch) ->
    S0 = portunus_machine:init(#{cluster => prop}),
    {_S, _Aliases, _Ix, Ids} =
        lists:foldl(fun(Op, Acc) -> step(Op, Epoch, Acc) end,
                    {S0, #{}, 1, []}, Ops),
    Ids.

step({grant, Alias}, Epoch, {S0, Aliases, Ix, Ids}) ->
    {Reply, S1} = apply_cmd({grant_lease, undefined, 100000, Alias, undefined},
                            Ix, Epoch, S0),
    {ok, LeaseId} = Reply,
    {S1, Aliases#{Alias => LeaseId}, Ix + 1, [LeaseId | Ids]};
step({acquire, Alias, Key}, Epoch, {S0, Aliases, Ix, Ids}) ->
    LeaseId = maps:get(Alias, Aliases, no_such_lease),
    {Reply, S1} = apply_cmd({acquire, LeaseId, Key, Alias, undefined, nowait},
                            Ix, Epoch, S0),
    Ids1 = case Reply of
               {ok, Token} -> [Token | Ids];
               {error, _} -> Ids
           end,
    {S1, Aliases, Ix + 1, Ids1};
step({release, Key}, Epoch, {S0, Aliases, Ix, Ids}) ->
    %% Mint-free, so the sequences interleave held and free keys.
    Token = case portunus_machine:query_owner(Key, S0) of
                {ok, #{token := T}} -> T;
                {error, not_held} -> 0
            end,
    {_Reply, S1} = apply_cmd({release, Key, Token}, Ix, Epoch, S0),
    {S1, Aliases, Ix + 1, Ids};
step({watch, Key}, Epoch, {S0, Aliases, Ix, Ids}) ->
    {{ok, Ref}, S1} = apply_cmd({watch, Key, self()}, Ix, Epoch, S0),
    {S1, Aliases, Ix + 1, [Ref | Ids]}.

apply_cmd(Cmd, Ix, Epoch, State) ->
    Meta = portunus_test_helpers:meta(Ix, Epoch),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S};
        {S, Reply, _Effects} -> {Reply, S}
    end.
