%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_empty_log_recovery_unit_SUITE).

%% The peers' views a configless seed reads before it elects, driven by injected
%% predicates so no cluster is needed. A `{cluster, _}` in the result is what
%% stops the election, so which peers are asked and what their answers become
%% decides whether a term can be minted.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([no_peer_has_a_server/1,
         solo_peers_do_not_block_an_election/1,
         a_multi_member_peer_is_reported/1,
         skips_unreachable_peers/1,
         never_probes_this_node/1,
         views_are_sorted/1,
         unreachable_peer_with_a_cluster_is_not_reported/1]).

-define(NAME, portunus_empty_log_recovery_unit_test).

all() ->
    [no_peer_has_a_server,
     solo_peers_do_not_block_an_election,
     a_multi_member_peer_is_reported,
     skips_unreachable_peers,
     never_probes_this_node,
     views_are_sorted,
     unreachable_peer_with_a_cluster_is_not_reported].

reachable(Set) ->
    fun(N) -> lists:member(N, Set) end.

views(Map) ->
    fun(N) -> maps:get(N, Map, none) end.

all_reachable() ->
    fun(_) -> true end.

%% The clusters reported by a set of views: empty means an election is permitted.
clusters(Views) ->
    [{N, Ms} || {N, {cluster, Ms}} <- Views].

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

%% Genesis: nobody has a server yet, so nothing blocks the seed's election.
no_peer_has_a_server(_Config) ->
    Views = portunus:peer_views([a, b, node()], all_reachable(), views(#{})),
    ?assertEqual([{a, none}, {b, none}], Views),
    ?assertEqual([], clusters(Views)).

%% Every peer alone is either a genesis solo cluster or a configless replica.
%% Both are one observation and both want this node to form, so neither reports a
%% cluster. This is the case that separates the design from a tri-state probe,
%% which would refuse to elect here and wedge forever.
solo_peers_do_not_block_an_election(_Config) ->
    Views = portunus:peer_views([a, b, node()], all_reachable(),
                                views(#{a => solo, b => solo})),
    ?assertEqual([{a, solo}, {b, solo}], Views),
    ?assertEqual([], clusters(Views)).

%% A real cluster reports its members, so the caller decides "wait" against
%% "join" without a second query.
a_multi_member_peer_is_reported(_Config) ->
    Ms = [{?NAME, a}, {?NAME, b}],
    Views = portunus:peer_views([a, b, node()], all_reachable(),
                                views(#{a => {cluster, Ms}, b => solo})),
    ?assertEqual([{a, Ms}], clusters(Views)).

skips_unreachable_peers(_Config) ->
    Probed = ets:new(probed, [public]),
    PeerView = fun(N) -> ets:insert(Probed, {N}), none end,
    Views = portunus:peer_views([a, b, c, node()], reachable([b]), PeerView),
    ?assertEqual([{b, none}], Views),
    ?assertEqual([{b}], ets:tab2list(Probed)),
    ets:delete(Probed).

never_probes_this_node(_Config) ->
    PeerView = fun(N) -> ?assertNotEqual(node(), N), none end,
    ?assertEqual([{a, none}], portunus:peer_views([a, node()], all_reachable(), PeerView)).

%% Sorted, so `elect_or_join/5` and `form_or_join_existing/3` pick the same peer.
views_are_sorted(_Config) ->
    Views = portunus:peer_views([c, a, b, node()], all_reachable(), views(#{})),
    ?assertEqual([a, b, c], [N || {N, _} <- Views]).

%% Reachability is checked first, so a partitioned peer's cluster cannot block
%% the election: it is not asked at all. The bound the design states rather than
%% removes.
unreachable_peer_with_a_cluster_is_not_reported(_Config) ->
    Ms = [{?NAME, a}, {?NAME, b}],
    Views = portunus:peer_views([a, b, node()], reachable([b]),
                                views(#{a => {cluster, Ms}, b => none})),
    ?assertEqual([{b, none}], Views),
    ?assertEqual([], clusters(Views)).
