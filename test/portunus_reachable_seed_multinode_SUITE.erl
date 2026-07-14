%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_reachable_seed_multinode_SUITE).

%% Multi-node coverage of the reachable-seed fallback in `join_or_form/3`: a down
%% lowest member does not block formation, a returning lowest member joins rather
%% than forms a rival, and a reachable seed whose Ra server blips resets no one.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([skips_down_lowest_member_and_forms/1,
         returning_seed_joins_existing_cluster/1,
         blip_of_reachable_seed_does_not_reset_member/1,
         isolated_effective_seed_forms_solo/1]).

-define(SYS, portunus).
-define(NAME, portunus_reachable_seed_multinode_test).
-define(TTL, 60000).
-define(SIZE, 3).
-define(RETRIES, 100).

all() ->
    [skips_down_lowest_member_and_forms,
     returning_seed_joins_existing_cluster,
     blip_of_reachable_seed_does_not_reset_member,
     isolated_effective_seed_forms_solo].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    Peers = [portunus_ct_cluster:start_node(Config, #{}) || _ <- lists:seq(1, ?SIZE)],
    Nodes = [Node || {_, Node} <- Peers],
    portunus_ct_cluster:mesh(Nodes),
    [{cluster, #{peers => Peers, nodes => Nodes}} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

%% The lowest member never starts, so the reachable members skip it and form.
%% Regression for formation blocked by a down seed.
skips_down_lowest_member_and_forms(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Members = [unreachable_below(hd(Nodes), "aaaa_absent_seed") | Nodes],
    ok = converge_all(Members, Nodes, ?RETRIES),
    ?assertEqual(?SIZE, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    {?NAME, Leader} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    ?assert(lists:member(Leader, Nodes)),
    Token = place_lock(Leader, {res, hold}),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 rpc:call(Leader, portunus, owner, [?NAME, {res, hold}])).

%% The lowest member returns as the effective seed to a cluster the two higher
%% members already formed, so it joins rather than forms a rival; the earlier
%% lock survives.
returning_seed_joins_existing_cluster(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    [Low | Highs] = lists:sort(Nodes),
    {ok, _, _} = rpc:call(hd(Highs), portunus, start_cluster, [?SYS, ?NAME, Highs]),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Highs, ?NAME),
    Token = place_lock(hd(Highs), {res, hold}),
    ok = rpc:call(Low, portunus, join_or_form, [?SYS, ?NAME, Nodes]),
    ok = await_member_count(Nodes, ?SIZE, ?RETRIES),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 rpc:call(Low, portunus, owner, [?NAME, {res, hold}])).

%% The seed's Ra server stops but its node stays up, so it stays reachable and the
%% effective seed. Convergence must not reset an established member.
blip_of_reachable_seed_does_not_reset_member(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Seed = hd(lists:sort(Nodes)),
    [Member | _] = lists:sort(Nodes) -- [Seed],
    {ok, _, _} = rpc:call(Seed, portunus, start_cluster, [?SYS, ?NAME, Nodes]),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    Token = place_lock(Member, {res, hold}),
    ok = portunus_ct_cluster:stop_ra_server(Seed, ?NAME),
    ?assertEqual(ok, rpc:call(Member, portunus, join_or_form, [?SYS, ?NAME, Nodes])),
    ?assertEqual(?SIZE, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 rpc:call(Member, portunus, owner, [?NAME, {res, hold}])).

%% Only the local node is reachable, so the effective seed has no cluster to join
%% and forms solo.
isolated_effective_seed_forms_solo(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Node = hd(Nodes),
    Members = [Node,
               unreachable_below(Node, "aaaa_absent_a"),
               unreachable_below(Node, "aaab_absent_b")],
    ok = rpc:call(Node, portunus, join_or_form, [?SYS, ?NAME, Members]),
    ok = await_member_count([Node], 1, ?RETRIES),
    {?NAME, Node} = portunus_ct_cluster:wait_leader([Node], ?NAME),
    Token = place_lock(Node, {res, hold}),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 rpc:call(Node, portunus, owner, [?NAME, {res, hold}])).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% A never-started node whose prefix sorts below the peer names: the unreachable
%% lowest member the fallback must skip.
unreachable_below(Node, Prefix) ->
    [_, Host] = string:split(atom_to_list(Node), "@"),
    list_to_atom(Prefix ++ "@" ++ Host).

%% `join_or_form/3` on every live node each round until all are one cluster.
converge_all(_Members, LiveNodes, 0) ->
    ct:fail({converge_timed_out, LiveNodes});
converge_all(Members, LiveNodes, Retries) ->
    _ = [rpc:call(N, portunus, join_or_form, [?SYS, ?NAME, Members]) || N <- LiveNodes],
    case portunus_ct_cluster:member_count(LiveNodes, ?NAME) =:= length(LiveNodes) of
        true -> ok;
        false -> timer:sleep(100), converge_all(Members, LiveNodes, Retries - 1)
    end.

await_member_count(_Nodes, _N, 0) ->
    ct:fail(member_count_timed_out);
await_member_count(Nodes, N, Retries) ->
    case portunus_ct_cluster:member_count(Nodes, ?NAME) of
        N -> ok;
        _ -> timer:sleep(100), await_member_count(Nodes, N, Retries - 1)
    end.

%% A lock held by a long-lived client on `Node`.
place_lock(Node, Key) ->
    Client = portunus_ct_cluster:start_client(Node),
    {ok, Lease} = portunus_ct_cluster:until_quorum(Client, grant_lease, [?NAME, ?TTL]),
    {ok, Token} = portunus_ct_cluster:until_quorum(Client, acquire, [?NAME, Key, Lease, owner_a]),
    portunus_ct_cluster:await_owner(Node, ?NAME, Key, owner_a, Token),
    Token.
