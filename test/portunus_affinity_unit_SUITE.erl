%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_affinity_unit_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([default_scores_zero/1,
         fifo_scores_zero/1,
         pinned_favours_target/1,
         preferred_orders_takeover/1,
         hash_scores_local_node/1,
         hash_balances_across_members/1,
         metric_bids_local_value/1,
         random_bids_vary/1,
         fun_spec_is_dynamic/1,
         strategies_tolerate_empty_members/1]).

all() ->
    [default_scores_zero,
     fifo_scores_zero,
     pinned_favours_target,
     preferred_orders_takeover,
     hash_scores_local_node,
     hash_balances_across_members,
     metric_bids_local_value,
     random_bids_vary,
     fun_spec_is_dynamic,
     strategies_tolerate_empty_members].

default_scores_zero(_Config) ->
    ?assertEqual(0, portunus_affinity:score(default, key, [node()])),
    ?assertEqual(deterministic, portunus_affinity:kind(default)).

fifo_scores_zero(_Config) ->
    Spec = {fifo, []},
    ?assertEqual(0, portunus_affinity:score(Spec, key, [node()])),
    ?assertEqual(deterministic, portunus_affinity:kind(Spec)).

pinned_favours_target(_Config) ->
    Here = node(),
    ?assertEqual(1, portunus_affinity:score({pinned, Here},
                                             key, [Here])),
    ?assertEqual(0, portunus_affinity:score(
                      {pinned, 'somewhere@else'},
                      key, [Here])).

preferred_orders_takeover(_Config) ->
    Here = node(),
    Other = 'other@host',
    %% First in the order outscores later, and both outscore a node that is
    %% not listed at all.
    First = portunus_affinity:score(
              {preferred, [Here, Other]}, key, []),
    Second = portunus_affinity:score(
               {preferred, [Other, Here]}, key, []),
    Absent = portunus_affinity:score(
               {preferred, [Other]}, key, []),
    ?assert(First > Second),
    ?assert(Second > Absent),
    ?assertEqual(0, Absent).

hash_scores_local_node(_Config) ->
    Spec = {hash, []},
    %% Each node bids its own weight for the key, keyed on its node name,
    %% ignoring Members and Args. This identity is what makes `hash_winner/2`
    %% below (and in the property suite) a faithful model of the strategy:
    %% the machine takes the max over each node's bid.
    [?assertEqual(erlang:phash2({Key, node()}),
                  portunus_affinity:score(Spec, Key, Members))
     || Key <- [{res, 1}, {res, 2}, other],
        Members <- [[node()], [node(), a@h], []]].

hash_balances_across_members(_Config) ->
    Members = [a@h, b@h, c@h, d@h],
    Keys = [{k, N} || N <- lists:seq(1, 400)],
    %% `hash_winner/2` models the cluster: each member M bids
    %% `phash2({Key, M})` (its own `score/3` result), and the machine grants
    %% to the top bid.
    Counts = lists:foldl(fun(Key, Acc) ->
                                 W = hash_winner(Key, Members),
                                 maps:update_with(W, fun(C) -> C + 1 end, 1, Acc)
                         end, #{}, Keys),
    %% Every member wins a meaningful share, well above zero.
    [?assert(maps:get(M, Counts, 0) > 40) || M <- Members].

metric_bids_local_value(_Config) ->
    Spec = {metric, fun () -> 42 end},
    ?assertEqual(42, portunus_affinity:score(Spec, key, [node()])),
    ?assertEqual(dynamic, portunus_affinity:kind(Spec)).

random_bids_vary(_Config) ->
    Spec = {random, []},
    ?assertEqual(dynamic, portunus_affinity:kind(Spec)),
    Scores = [portunus_affinity:score(Spec, key, [node()])
              || _ <- lists:seq(1, 100)],
    [?assert(is_integer(S)) || S <- Scores],
    %% Independent rolls, so a hundred of them are not all identical.
    ?assert(length(lists:usort(Scores)) > 1).

fun_spec_is_dynamic(_Config) ->
    Fun = fun (_Key, Members) -> length(Members) end,
    ?assertEqual(3, portunus_affinity:score(Fun, key, [x, y, z])),
    ?assertEqual(dynamic, portunus_affinity:kind(Fun)).

strategies_tolerate_empty_members(_Config) ->
    %% Every built-in scores the local node and ignores Members, so an empty
    %% member list must never crash scoring.
    Specs = [default,
             {fifo, []},
             {pinned, node()},
             {preferred, [node()]},
             {hash, []},
             {metric, fun () -> 1 end},
             {random, []}],
    [?assert(is_integer(portunus_affinity:score(S, key, []))) || S <- Specs].

hash_winner(Key, Members) ->
    {_, W} = lists:max([{erlang:phash2({Key, M}), M} || M <- Members]),
    W.
