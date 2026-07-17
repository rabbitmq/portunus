%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_seed_gate_multinode_SUITE).

%% `portunus:is_seed_cluster_member/2`, the reconcile gate: it must ask about the
%% node `join_or_form/3` picks (the effective seed), so the gate keeps
%% answering while the lowest candidate is down, and every error reads as
%% `false` rather than opening the gate.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([in_seed_cluster_follows_the_effective_seed/1,
         in_seed_cluster_is_false_before_joining/1]).

-define(SYS, portunus).
-define(NAME, portunus_seed_gate_multinode_test).
-define(SIZE, 3).

all() ->
    [in_seed_cluster_follows_the_effective_seed,
     in_seed_cluster_is_false_before_joining].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    Peers = [portunus_ct_cluster:start_node(Config, #{})
             || _ <- lists:seq(1, ?SIZE)],
    Nodes = [Node || {_, Node} <- Peers],
    portunus_ct_cluster:mesh(Nodes),
    [{cluster, #{peers => Peers, nodes => Nodes}} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

%% The lowest candidate never starts, so the effective seed is the lowest
%% reachable member. Red if the helper asks the lowest candidate rather than
%% the effective seed (the `022` failure mode): the down node answers
%% nothing and every member reads `false` forever.
in_seed_cluster_follows_the_effective_seed(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Down = unreachable_below(hd(Nodes), "aaaa_down_lowest"),
    Candidates = [Down | Nodes],
    ok = portunus_ct_cluster:converge_all(Candidates, Nodes, ?NAME),
    _ = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    [?assert(rpc:call(N, portunus, is_seed_cluster_member, [?NAME, Candidates]))
     || N <- Nodes].

in_seed_cluster_is_false_before_joining(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    [A, B, C] = lists:sort(Nodes),
    %% No cluster exists anywhere: the seed's members query errors, and the
    %% error reads as `false`.
    ?assertEqual(false, rpc:call(C, portunus, is_seed_cluster_member,
                                 [?NAME, Nodes])),
    %% A cluster forms on the two lower nodes; the third has not joined.
    {ok, _, _} = rpc:call(A, portunus, start_cluster, [?SYS, ?NAME, [A, B]]),
    _ = portunus_ct_cluster:wait_leader([A, B], ?NAME),
    ?assertEqual(false, rpc:call(C, portunus, is_seed_cluster_member,
                                 [?NAME, Nodes])),
    ?assert(rpc:call(B, portunus, is_seed_cluster_member, [?NAME, Nodes])),
    %% Once joined, the gate opens.
    ok = rpc:call(C, portunus, join_or_form, [?SYS, ?NAME, Nodes]),
    ok = portunus_ct_cluster:wait_until(
           fun() ->
                   rpc:call(C, portunus, is_seed_cluster_member, [?NAME, Nodes])
                       =:= true
           end).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% A never-started node whose prefix sorts below the peer names.
unreachable_below(Node, Prefix) ->
    [_, Host] = string:split(atom_to_list(Node), "@"),
    list_to_atom(Prefix ++ "@" ++ Host).
