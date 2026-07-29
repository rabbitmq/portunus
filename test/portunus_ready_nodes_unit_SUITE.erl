%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_ready_nodes_unit_SUITE).

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([waiters_collapse_to_unique_owners/1,
         dead_lease_waiter_is_excluded/1,
         non_atom_owner_is_dropped/1]).

all() ->
    [waiters_collapse_to_unique_owners,
     dead_lease_waiter_is_excluded,
     non_atom_owner_is_dropped].

%% n2 bids on both held keys and appears once.
waiters_collapse_to_unique_owners(_Config) ->
    S0 = held_by(l1, n1),
    {{ok, _}, S1, _} = step({acquire, l1, kb, n1, undefined, nowait}, 9, S0),
    {_, S2, _} = step({grant_lease, l2, 100000, n2, self()}, 10, S1),
    {_, S3, _} = step({grant_lease, l3, 100000, n3, self()}, 11, S2),
    {{queued, _}, S4, _} = step({acquire, l2, ka, n2, undefined, wait, 0}, 12, S3),
    {{queued, _}, S5, _} = step({acquire, l2, kb, n2, undefined, wait, 0}, 13, S4),
    {{queued, _}, S6, _} = step({acquire, l3, ka, n3, undefined, wait, 0}, 14, S5),
    ?assertEqual([n2, n3], portunus_machine:query_ready_nodes(S6)).

dead_lease_waiter_is_excluded(_Config) ->
    S0 = held_by(l1, n1),
    {_, S1, _} = step({grant_lease, l2, 100000, n2, self()}, 10, S0),
    {{queued, _}, S2, _} = step({acquire, l2, ka, n2, undefined, wait, 0}, 11, S1),
    ?assertEqual([n2], portunus_machine:query_ready_nodes(S2)),
    %% The holder remains and is rightly absent from the empty set: a node
    %% with no queued bid can accept no key by transfer.
    {_, S3, _} = step({revoke_lease, l2}, 12, S2),
    ?assertEqual([], portunus_machine:query_ready_nodes(S3)).

non_atom_owner_is_dropped(_Config) ->
    S0 = held_by(l1, n1),
    {_, S1, _} = step({grant_lease, l2, 100000, {pid, x}, self()}, 10, S0),
    {_, S2, _} = step({grant_lease, l3, 100000, n3, self()}, 11, S1),
    {{queued, _}, S3, _} = step({acquire, l2, ka, {pid, x}, undefined, wait, 0},
                                12, S2),
    {{queued, _}, S4, _} = step({acquire, l3, ka, n3, undefined, wait, 0}, 13, S3),
    ?assertEqual([n3], portunus_machine:query_ready_nodes(S4)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% `Owner` holds `ka` under `LeaseId`.
held_by(LeaseId, Owner) ->
    S0 = portunus_machine:init(#{cluster => test}),
    {_, S1, _} = step({grant_lease, LeaseId, 100000, Owner, self()}, 1, S0),
    {{ok, _}, S2, _} = step({acquire, LeaseId, ka, Owner, undefined, nowait},
                            2, S1),
    S2.

step(Cmd, Ix, State) ->
    Meta = portunus_test_helpers:meta(Ix),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.
