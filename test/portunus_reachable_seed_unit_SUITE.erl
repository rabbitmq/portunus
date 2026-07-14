%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_reachable_seed_unit_SUITE).

%% Seed selection, driven by an injected predicate so no node control is needed.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([lowest_reachable_is_seed/1,
         skips_unreachable_lowest/1,
         skips_leading_unreachable_run/1,
         only_local_reachable/1,
         input_order_does_not_matter/1]).

all() ->
    [lowest_reachable_is_seed,
     skips_unreachable_lowest,
     skips_leading_unreachable_run,
     only_local_reachable,
     input_order_does_not_matter].

reachable(Set) ->
    fun(N) -> lists:member(N, Set) end.

%% All reachable: lowest seeds, unchanged from before the fallback.
lowest_reachable_is_seed(_Config) ->
    ?assertEqual(a, portunus:effective_seed([a, b, c], reachable([a, b, c]))).

skips_unreachable_lowest(_Config) ->
    ?assertEqual(b, portunus:effective_seed([a, b, c], reachable([b, c]))).

skips_leading_unreachable_run(_Config) ->
    ?assertEqual(c, portunus:effective_seed([a, b, c, d], reachable([c, d]))).

only_local_reachable(_Config) ->
    ?assertEqual(c, portunus:effective_seed([a, b, c], reachable([c]))).

%% Selection sorts first, so input order does not matter.
input_order_does_not_matter(_Config) ->
    ?assertEqual(b, portunus:effective_seed([c, a, b], reachable([b, c]))).
