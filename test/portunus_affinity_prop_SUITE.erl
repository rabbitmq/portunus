%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_affinity_prop_SUITE).

%% Property tests use PropEr, so this module includes only proper.hrl:
%% mixing it with eunit/ct headers redefines macros such as LET.
-include_lib("proper/include/proper.hrl").

-export([all/0, hash_minimal_movement/1]).

all() ->
    [hash_minimal_movement].

hash_minimal_movement(_Config) ->
    true = portunus_test_helpers:quickcheck(fun prop_hash_minimal_movement/0, 300).

%% For a random cluster and key set, dropping one member only changes the
%% owner of keys that the dropped member owned; every other key keeps its
%% owner. By symmetry this also bounds movement when a node is added. This
%% is the minimal-movement guarantee of rendezvous hashing.
%%
%% `hash_winner/2` models the cluster: member M bids `phash2({Key, M})`,
%% which is exactly `portunus_affinity_hash:score/3` evaluated on node M
%% (see the `hash_scores_local_node` test), and the machine grants the key
%% to the top bid.
prop_hash_minimal_movement() ->
    ?FORALL(Members, members(),
            ?FORALL({Gone, Keys}, {elements(Members), non_empty(list(key_term()))},
                    begin
                        Rest = Members -- [Gone],
                        lists:all(
                          fun(Key) ->
                                  case hash_winner(Key, Members) of
                                      Gone -> true;
                                      W -> W =:= hash_winner(Key, Rest)
                                  end
                          end, Keys)
                    end)).

hash_winner(Key, Members) ->
    {_, W} = lists:max([{erlang:phash2({Key, M}), M} || M <- Members]),
    W.

%% A cluster of at least two distinct nodes, so dropping one always leaves
%% a cluster behind.
members() ->
    ?LET(Extra, list(node_name()), lists:usort([n1@h, n2@h | Extra])).

node_name() ->
    ?LET(N, choose(1, 8), list_to_atom("n" ++ integer_to_list(N) ++ "@h")).

key_term() ->
    ?LET(N, choose(1, 100), {key, N}).
