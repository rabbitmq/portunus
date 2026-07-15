%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_empty_log_recovery_prop_SUITE).

%% Property tests use PropEr, so this module includes only proper.hrl:
%% mixing it with eunit/ct headers redefines macros such as LET.
-include_lib("proper/include/proper.hrl").

-export([all/0, reports_the_reachable_peers/1, cluster_free_exactly_when_no_peer_has_one/1]).

all() ->
    [reports_the_reachable_peers,
     cluster_free_exactly_when_no_peer_has_one].

reports_the_reachable_peers(_Config) ->
    true = portunus_test_helpers:quickcheck(fun prop_reports_the_reachable_peers/0, 300).

cluster_free_exactly_when_no_peer_has_one(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_cluster_free_exactly_when_no_peer_has_one/0, 300).

%% Exactly the reachable candidates other than this node, each carrying the view
%% it was given, in sorted order: the election decision is read off this list, so
%% a peer dropped or an answer altered here is a term minted or refused wrongly.
prop_reports_the_reachable_peers() ->
    ?FORALL(Members, non_empty(list(node_name())),
            ?FORALL({Reachable, Views}, {subset(Members), views(Members)},
                    begin
                        Got = portunus:peer_views(Members, reachable(Reachable),
                                                  view_fun(Views)),
                        Expected = [{N, view_of(N, Views)}
                                    || N <- lists:usort(Members),
                                       N =/= node(), lists:member(N, Reachable)],
                        Got =:= Expected
                    end)).

%% No `{cluster, _}` in the views exactly when no reachable peer runs a
%% multi-member cluster. That emptiness is the only state that permits an
%% election, so `solo` and `none` must never be mistaken for a cluster and a
%% cluster must never be dropped.
prop_cluster_free_exactly_when_no_peer_has_one() ->
    ?FORALL(Members, non_empty(list(node_name())),
            ?FORALL({Reachable, Views}, {subset(Members), views(Members)},
                    begin
                        Got = portunus:peer_views(Members, reachable(Reachable),
                                                  view_fun(Views)),
                        NoCluster = [] =:= [N || {N, {cluster, _}} <- Got],
                        NoneReachableHasOne =
                            [] =:= [N || N <- lists:usort(Members),
                                         N =/= node(), lists:member(N, Reachable),
                                         is_cluster(view_of(N, Views))],
                        NoCluster =:= NoneReachableHasOne
                    end)).

is_cluster({cluster, _}) -> true;
is_cluster(_) -> false.

reachable(Set) ->
    fun(N) -> lists:member(N, Set) end.

view_fun(Views) ->
    fun(N) -> view_of(N, Views) end.

view_of(N, Views) ->
    proplists:get_value(N, Views, none).

%% One view per distinct candidate.
views(Members) ->
    Distinct = lists:usort(Members),
    ?LET(Vs, vector(length(Distinct), view()), lists:zip(Distinct, Vs)).

view() ->
    oneof([none, solo, {cluster, [{portunus, n1@h}, {portunus, n2@h}]}]).

subset(Members) ->
    Distinct = lists:usort(Members),
    ?LET(Keep, vector(length(Distinct), boolean()),
         [M || {M, true} <- lists:zip(Distinct, Keep)]).

node_name() ->
    ?LET(N, choose(1, 8), list_to_atom("n" ++ integer_to_list(N) ++ "@h")).
