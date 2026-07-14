%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_reachable_seed_prop_SUITE).

%% Property tests use PropEr, so this module includes only proper.hrl:
%% mixing it with eunit/ct headers redefines macros such as LET.
-include_lib("proper/include/proper.hrl").

-export([all/0, lowest_reachable_is_seed/1]).

all() ->
    [lowest_reachable_is_seed].

lowest_reachable_is_seed(_Config) ->
    true = portunus_test_helpers:quickcheck(fun prop_lowest_reachable_is_seed/0, 300).

%% Lowest reachable member, so nodes with the same view agree on the seed: the
%% one-agreed-seed precondition formation relies on.
prop_lowest_reachable_is_seed() ->
    ?FORALL(Members, non_empty(list(node_name())),
            ?FORALL(Reachable, non_empty_subset(Members),
                    lists:min(Reachable) =:=
                        portunus:effective_seed(
                          Members, fun(N) -> lists:member(N, Reachable) end))).

%% A non-empty, deduplicated subset of `Members`.
non_empty_subset(Members) ->
    Distinct = lists:usort(Members),
    ?LET(Keep, vector(length(Distinct), boolean()),
         case [M || {M, true} <- lists:zip(Distinct, Keep)] of
             [] -> [hd(Distinct)];
             Subset -> Subset
         end).

node_name() ->
    ?LET(N, choose(1, 8), list_to_atom("n" ++ integer_to_list(N) ++ "@h")).
