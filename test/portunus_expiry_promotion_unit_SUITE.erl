%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_expiry_promotion_unit_SUITE).

%% One command can revoke a chain: a key's owner and its queued successor
%% both expire in one tick, or one pid's death revokes both leases. Every
%% token in a command is the same Raft index, so promoting a lease that the
%% same command is about to revoke would re-free the key and mint a second,
%% equal token: two owners the fence cannot tell apart. Promotion must skip
%% the leases the command is revoking: one grant, one mint, and the
%% watcher's last event names the remaining owner.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([chained_expiry_grants_once/1,
         holder_death_chain_grants_once/1,
         chained_expiry_watch_order/1]).

all() ->
    [chained_expiry_grants_once,
     holder_death_chain_grants_once,
     chained_expiry_watch_order].

chained_expiry_grants_once(_Config) ->
    {S, Effects} = expire_chain(),
    [{portunus, granted, k, Token, l3}] = grants_of(k, Effects),
    {ok, #{owner := o3, token := Token}} = portunus_machine:query_owner(k, S).

holder_death_chain_grants_once(_Config) ->
    P = spawn(fun() -> receive stop -> ok end end),
    S0 = portunus_machine:init(#{}),
    %% One pid holds l1 (owns k) and l2 (waits on k); l3 waits behind.
    {_, S1, _} = step({grant_lease, l1, 100000, o1, P}, 1, 0, S0),
    {_, S2, _} = step({grant_lease, l2, 100000, o2, P}, 2, 0, S1),
    {_, S3, _} = step({grant_lease, l3, 100000, o3, self()}, 3, 0, S2),
    {{ok, _}, S4, _} = step({acquire, l1, k, o1, undefined, nowait}, 4, 0, S3),
    {{queued, _}, S5, _} = step({acquire, l2, k, o2, undefined, wait, 0}, 5, 0, S4),
    {{queued, _}, S6, _} = step({acquire, l3, k, o3, undefined, wait, 0}, 6, 0, S5),
    {ok, S7, Effects} = step({down, P, killed}, 7, 100, S6),
    [{portunus, granted, k, Token, l3}] = grants_of(k, Effects),
    {ok, #{owner := o3, token := Token}} = portunus_machine:query_owner(k, S7),
    P ! stop.

chained_expiry_watch_order(_Config) ->
    {_S, Effects} = expire_chain(),
    Events = [E || {send_msg, _, {portunus, watch, _, E}} <- Effects],
    ?assert(lists:member(released, Events)),
    ?assertEqual({acquired, o3}, lists:last(Events)).

%%----------------------------------------------------------------------
%% Fixtures and `apply/3` helpers
%%----------------------------------------------------------------------

%% l1 holds k and l2 waits on it, both expiring in one tick; l3 waits with
%% a live lease and a watcher observes k.
expire_chain() ->
    S0 = portunus_machine:init(#{}),
    {_, S1, _} = step({grant_lease, l1, 1000, o1, undefined}, 1, 0, S0),
    {_, S2, _} = step({grant_lease, l2, 1000, o2, undefined}, 2, 0, S1),
    {_, S3, _} = step({grant_lease, l3, 100000, o3, self()}, 3, 0, S2),
    {{ok, _}, S4, _} = step({acquire, l1, k, o1, undefined, nowait}, 4, 0, S3),
    {{queued, _}, S5, _} = step({acquire, l2, k, o2, undefined, wait, 0}, 5, 0, S4),
    {{queued, _}, S6, _} = step({acquire, l3, k, o3, undefined, wait, 0}, 6, 0, S5),
    {{ok, _}, S7, _} = step({watch, k, self()}, 7, 0, S6),
    {ok, S8, Effects} = step({expire_leases,
                          portunus_test_helpers:expire_pairs(S7, 0, 5000)},
                         8, 5000, S7),
    {S8, Effects}.

grants_of(Key, Effects) ->
    [Msg || {send_msg, _, {portunus, granted, K, _, _} = Msg} <- Effects,
            K =:= Key].

step(Cmd, Ix, Time, S) ->
    Meta = portunus_test_helpers:meta(Ix, Time),
    case portunus_machine:apply(Meta, Cmd, S) of
        {S1, Reply} -> {Reply, S1, []};
        {S1, Reply, Effects} -> {Reply, S1, Effects}
    end.
