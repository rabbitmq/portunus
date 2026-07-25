%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_transfer_batch_unit_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([batch_moves_every_key_at_one_index/1,
         mixed_batch_returns_ordered_per_item_results/1,
         later_item_sees_earlier_promotion/1,
         malformed_items_are_not_owner_and_change_nothing/1,
         empty_batch_is_empty_result/1]).

all() ->
    [batch_moves_every_key_at_one_index,
     mixed_batch_returns_ordered_per_item_results,
     later_item_sees_earlier_promotion,
     malformed_items_are_not_owner_and_change_nothing,
     empty_batch_is_empty_result].

%% One `apply/3` call carries the whole batch, so N moves are one log entry
%% and every new token is that entry's index.
batch_moves_every_key_at_one_index(_Config) ->
    N = 5,
    {Keys, Tokens, S0} = held_keys(N),
    Items = [{K, T, o2} || {K, T} <- lists:zip(Keys, Tokens)],
    {Results, S1, _} = at({transfer_batch, Items}, 100, 0, S0),
    ?assertEqual([{K, ok} || K <- Keys], Results),
    NewTokens = [begin
                     {ok, #{owner := o2, token := T}} =
                         portunus_machine:query_owner(K, S1),
                     T
                 end || K <- Keys],
    [OneToken] = lists:usort(NewTokens),
    ?assert(OneToken > lists:max(Tokens)).

mixed_batch_returns_ordered_per_item_results(_Config) ->
    S0 = portunus_machine:init(#{cluster => test}),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, l2}, S2, _} = at({grant_lease, l2, 100000, o2, dummy_pid()}, 2, 0, S1),
    {{ok, Ta}, S3, _} = at({acquire, l1, ka, o1, undefined, nowait}, 3, 0, S2),
    {{ok, Tb}, S4, _} = at({acquire, l1, kb, o1, undefined, nowait}, 4, 0, S3),
    {{ok, Tc}, S5, _} = at({acquire, l1, kc, o1, undefined, nowait}, 5, 0, S4),
    {{queued, 1}, S6, _} = at({acquire, l2, ka, o2, undefined, wait}, 6, 0, S5),
    Items = [{ka, Ta, o2},
             {kb, Tb + 999, o2},
             {kfree, 1, o2},
             {kc, Tc, o9}],
    {Results, S7, _} = at({transfer_batch, Items}, 100, 0, S6),
    [{ka, ok},
     {kb, {error, not_owner}},
     {kfree, {error, not_owner}},
     {kc, {error, {no_contender, o9}}}] = Results,
    {ok, #{owner := o2}} = portunus_machine:query_owner(ka, S7),
    {ok, #{owner := o1, token := Tb}} = portunus_machine:query_owner(kb, S7),
    {ok, #{owner := o1, token := Tc}} = portunus_machine:query_owner(kc, S7).

%% Both items carry the key's pre-batch token; the fold threads state, so the
%% first item's promotion fences the second out.
later_item_sees_earlier_promotion(_Config) ->
    S0 = portunus_machine:init(#{cluster => test}),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, l2}, S2, _} = at({grant_lease, l2, 100000, o2, dummy_pid()}, 2, 0, S1),
    {{ok, l3}, S3, _} = at({grant_lease, l3, 100000, o3, dummy_pid()}, 3, 0, S2),
    {{ok, Ta}, S4, _} = at({acquire, l1, a, o1, undefined, nowait}, 4, 0, S3),
    {{queued, 1}, S5, _} = at({acquire, l2, a, o2, undefined, wait}, 5, 0, S4),
    {{queued, 2}, S6, _} = at({acquire, l3, a, o3, undefined, wait}, 6, 0, S5),
    {Results, S7, _} = at({transfer_batch, [{a, Ta, o2}, {a, Ta, o3}]}, 100, 0, S6),
    [{a, ok}, {a, {error, not_owner}}] = Results,
    {ok, #{owner := o2}} = portunus_machine:query_owner(a, S7).

%% The fold matches item shapes itself, so a malformed element is answered
%% with `not_owner`, never a crash.
malformed_items_are_not_owner_and_change_nothing(_Config) ->
    {[K1 | _], _Tokens, S0} = held_keys(2),
    Items = [not_a_tuple,
             {K1},
             {K1, tok},
             {K1, tok, owner, extra},
             42],
    {Results, S1, _} = at({transfer_batch, Items}, 100, 0, S0),
    [{not_a_tuple, {error, not_owner}},
     {{K1}, {error, not_owner}},
     {{K1, tok}, {error, not_owner}},
     {{K1, tok, owner, extra}, {error, not_owner}},
     {42, {error, not_owner}}] = Results,
    ?assertEqual(S0, S1).

empty_batch_is_empty_result(_Config) ->
    {_Keys, _Tokens, S0} = held_keys(1),
    {[], S1, Effs} = at({transfer_batch, []}, 100, 0, S0),
    ?assertEqual(S0, S1),
    [] = Effs.

%% Helpers

%% Holder o1 (lease l1) holds keys k1..kN; contender o2 (lease l2) waits on
%% each. Returns the keys, their acquire tokens (in key order), and the state.
held_keys(N) ->
    S0 = portunus_machine:init(#{cluster => test}),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, l2}, S2, _} = at({grant_lease, l2, 100000, o2, dummy_pid()}, 2, 0, S1),
    Keys = [{k, I} || I <- lists:seq(1, N)],
    {TokensRev, S3} =
        lists:foldl(
          fun({K, I}, {Toks, S}) ->
                  {{ok, T}, Sa, _} =
                      at({acquire, l1, K, o1, undefined, nowait}, 10 + I, 0, S),
                  {{queued, 1}, Sb, _} =
                      at({acquire, l2, K, o2, undefined, wait}, 100 + I, 0, Sa),
                  {[T | Toks], Sb}
          end, {[], S2}, lists:zip(Keys, lists:seq(1, N))),
    {Keys, lists:reverse(TokensRev), S3}.

at(Cmd, Ix, Now, State) ->
    Meta = portunus_test_helpers:meta(Ix, Now),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.

dummy_pid() ->
    spawn(fun() -> timer:sleep(infinity) end).
