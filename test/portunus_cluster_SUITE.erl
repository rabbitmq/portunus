%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_cluster_SUITE).

%% Multi-node tests over a real, distributed Ra cluster. Each test forms a
%% fresh cluster of peer nodes (one Erlang node per member) through the shared
%% `portunus_ct_cluster` harness, drives the public `portunus` API from a
%% long-lived client process on a member, and injects faults by stopping and
%% restarting Ra servers. The target is the core safety invariant, at most one
%% owner per key, across Ra leader changes, quorum loss, and membership
%% changes.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([form_cluster_elects_leader/1,
         leader_change_preserves_single_owner/1,
         holder_death_after_leader_change_releases_lock/1,
         node_crash_holds_lock_until_lease_expiry/1,
         monotonic_tokens_across_leader_change/1,
         quorum_loss_blocks_writes_until_healed/1,
         five_node_cluster_tolerates_two_failures/1,
         membership_add_remove/1]).

-define(SYS, portunus).
-define(NAME, portunus_cluster_test).
-define(TICK_MS, 200).

all() ->
    [form_cluster_elects_leader,
     leader_change_preserves_single_owner,
     holder_death_after_leader_change_releases_lock,
     node_crash_holds_lock_until_lease_expiry,
     monotonic_tokens_across_leader_change,
     quorum_loss_blocks_writes_until_healed,
     five_node_cluster_tolerates_two_failures,
     membership_add_remove].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Size = case TC of
               five_node_cluster_tolerates_two_failures -> 5;
               _ -> 3
           end,
    [{cluster, portunus_ct_cluster:start(Config, ?NAME, Size)} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

form_cluster_elects_leader(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    {Members, {?NAME, LeaderNode}} = portunus_ct_cluster:cluster_info(Nodes, ?NAME),
    ?assertEqual(3, length(Members)),
    ?assert(lists:member(LeaderNode, Nodes)),
    %% Every member agrees on the one leader.
    ?assertEqual({?NAME, LeaderNode}, portunus_ct_cluster:wait_leader(Nodes, ?NAME)),
    %% Every node reports itself a member, answered from its local replica.
    [?assert(rpc:call(N, portunus, is_member, [?NAME])) || N <- Nodes].

leader_change_preserves_single_owner(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Key = {res, leader_change},
    {?NAME, LeaderNode} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    Survivors = Nodes -- [LeaderNode],
    %% The lease holder lives on a surviving follower, so the lease itself
    %% is never at risk; only the leader goes away.
    Holder = portunus_ct_cluster:start_client(hd(Survivors)),
    {ok, Lease} = grant(Holder, 60000),
    {ok, Token} = acquire(Holder, Key, Lease, owner_a),
    portunus_ct_cluster:await_owner(hd(Survivors), ?NAME, Key, owner_a, Token),
    ok = portunus_ct_cluster:stop_ra_server(LeaderNode, ?NAME),
    {?NAME, NewLeader} = portunus_ct_cluster:wait_leader(Survivors, ?NAME),
    ?assert(lists:member(NewLeader, Survivors)),
    %% The owner and its fencing token outlive the leader change unchanged.
    portunus_ct_cluster:await_owner(hd(Survivors), ?NAME, Key, owner_a, Token),
    %% At-most-one-owner still holds: a contender on another node is refused.
    Contender = portunus_ct_cluster:start_client(lists:last(Survivors)),
    {ok, L2} = grant(Contender, 60000),
    ?assertEqual({error, {held_by, owner_a}},
                 portunus_ct_cluster:ccall(Contender, acquire,
                                           [?NAME, Key, L2, owner_b])).

holder_death_after_leader_change_releases_lock(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Key = {res, monitor_leader_change},
    {?NAME, LeaderNode} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    Survivors = Nodes -- [LeaderNode],
    Holder = portunus_ct_cluster:start_client(hd(Survivors)),
    {ok, Lease} = grant(Holder, 60000),
    {ok, _Token} = acquire(Holder, Key, Lease, owner_a),
    %% The new leader inherits the holder and must re-arm its monitor in
    %% `state_enter/2`, or the death below would go unnoticed.
    ok = portunus_ct_cluster:stop_ra_server(LeaderNode, ?NAME),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Survivors, ?NAME),
    portunus_ct_cluster:await_owner(hd(Survivors), ?NAME, Key, owner_a),
    %% A genuine death (not a partition) must release the lock.
    true = exit(Holder, kill),
    portunus_ct_cluster:await_released(hd(Survivors), ?NAME, Key).

node_crash_holds_lock_until_lease_expiry(Config) ->
    #{nodes := Nodes, peers := Peers} = ?config(cluster, Config),
    Key = {res, node_crash},
    {?NAME, LeaderNode} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    %% Hold from a follower and crash only that follower, so the leader
    %% stays put and the lease lifecycle is what we are observing.
    [HolderNode | _] = Nodes -- [LeaderNode],
    Holder = portunus_ct_cluster:start_client(HolderNode),
    {ok, Lease} = grant(Holder, 5000),
    {ok, _Token} = acquire(Holder, Key, Lease, owner_a),
    portunus_ct_cluster:await_owner(LeaderNode, ?NAME, Key, owner_a),
    {HolderPeer, HolderNode} = lists:keyfind(HolderNode, 2, Peers),
    ok = peer:stop(HolderPeer),
    Survivors = Nodes -- [HolderNode],
    {?NAME, _} = portunus_ct_cluster:wait_leader(Survivors, ?NAME),
    %% An unreachable holder is not a dead one: the lock is still held a
    %% second on, rather than fast-released by the monitor going down.
    timer:sleep(1000),
    ?assertMatch({ok, #{owner := owner_a}},
                 portunus_ct_cluster:papi(hd(Survivors), owner, [?NAME, Key])),
    %% With nothing left to renew it, though, the lease expires and the
    %% lock is reclaimed.
    portunus_ct_cluster:await_released(hd(Survivors), ?NAME, Key),
    New = portunus_ct_cluster:start_client(hd(Survivors)),
    {ok, L2} = grant(New, 60000),
    ?assertMatch({ok, _}, acquire(New, Key, L2, owner_b)).

monotonic_tokens_across_leader_change(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Key = {res, tokens},
    {?NAME, LeaderNode} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    Survivors = Nodes -- [LeaderNode],
    Holder1 = portunus_ct_cluster:start_client(hd(Survivors)),
    {ok, L1} = grant(Holder1, 60000),
    {ok, T1} = acquire(Holder1, Key, L1, owner_a),
    ok = portunus_ct_cluster:ccall(Holder1, release, [?NAME, Key, T1]),
    ok = portunus_ct_cluster:stop_ra_server(LeaderNode, ?NAME),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Survivors, ?NAME),
    Holder2 = portunus_ct_cluster:start_client(lists:last(Survivors)),
    {ok, L2} = grant(Holder2, 60000),
    {ok, T2} = acquire(Holder2, Key, L2, owner_b),
    %% Tokens are the Raft index: strictly increasing across the term
    %% change, so a stale token can never fence out a newer holder.
    ?assert(T2 > T1).

quorum_loss_blocks_writes_until_healed(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Key = {res, quorum},
    [Survivor, Down1, Down2] = Nodes,
    Holder = portunus_ct_cluster:start_client(Survivor),
    {ok, Lease} = grant(Holder, 60000),
    {ok, _} = acquire(Holder, Key, Lease, owner_a),
    %% Stop two of three servers: the lone survivor loses its majority and
    %% a write can no longer commit.
    ok = portunus_ct_cluster:stop_ra_server(Down1, ?NAME),
    ok = portunus_ct_cluster:stop_ra_server(Down2, ?NAME),
    %% Without a majority the lone survivor cannot commit, so the client
    %% sees no_quorum (whether its routing target is the survivor itself,
    %% now unable to elect, or a stopped node).
    ?assertEqual({error, no_quorum},
                 portunus_ct_cluster:ccall(Holder, grant_lease, [?NAME, 60000])),
    %% Heal: bring one back and quorum (and writes) return.
    ok = portunus_ct_cluster:restart_ra_server(Down1, ?NAME),
    {?NAME, _} = portunus_ct_cluster:wait_leader([Survivor, Down1], ?NAME),
    ?assertMatch({ok, _}, grant(Holder, 60000)).

five_node_cluster_tolerates_two_failures(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Key = {res, five_node},
    {?NAME, LeaderNode} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    Followers = Nodes -- [LeaderNode],
    Holder = portunus_ct_cluster:start_client(hd(Followers)),
    {ok, Lease} = grant(Holder, 60000),
    {ok, Token} = acquire(Holder, Key, Lease, owner_a),
    %% Drop the leader and a follower at once: 3 of 5 remain, still a
    %% majority.
    DownFollower = lists:last(Followers),
    ok = portunus_ct_cluster:stop_ra_server(LeaderNode, ?NAME),
    ok = portunus_ct_cluster:stop_ra_server(DownFollower, ?NAME),
    Survivors = Nodes -- [LeaderNode, DownFollower],
    {?NAME, _} = portunus_ct_cluster:wait_leader(Survivors, ?NAME),
    %% The lock and its fencing token survived two failures, and the
    %% cluster still refuses a second owner.
    portunus_ct_cluster:await_owner(hd(Survivors), ?NAME, Key, owner_a, Token),
    Contender = portunus_ct_cluster:start_client(lists:last(Survivors)),
    {ok, L2} = grant(Contender, 60000),
    ?assertEqual({error, {held_by, owner_a}},
                 portunus_ct_cluster:ccall(Contender, acquire,
                                           [?NAME, Key, L2, owner_b])).

membership_add_remove(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    {[_, _, _], _Leader} = portunus_ct_cluster:cluster_info(Nodes, ?NAME),
    %% Start a fresh node and a Ra server on it that already knows the
    %% cluster, then add it through the public API.
    {ExtraPeer, ExtraNode} = portunus_ct_cluster:start_node(Config, #{}),
    try
        Existing = [{?NAME, N} || N <- Nodes],
        ok = rpc:call(ExtraNode, ra, start_server,
                      [?SYS, ?NAME, {?NAME, ExtraNode}, machine(), Existing]),
        Member = hd(Nodes),
        ok = rpc:call(Member, portunus, add_member, [?NAME, ExtraNode]),
        ok = portunus_ct_cluster:wait_until(
               fun() ->
                       portunus_ct_cluster:member_count([ExtraNode | Nodes], ?NAME)
                           =:= 4
               end),
        %% Counted is not enough: the new node's local replica must itself be a
        %% member, the signal that it serves reads rather than just appearing in
        %% the count.
        ?assert(rpc:call(ExtraNode, portunus, is_member, [?NAME])),
        %% Removing it returns the cluster to its original size.
        ok = rpc:call(Member, portunus, remove_member, [?NAME, ExtraNode]),
        ok = portunus_ct_cluster:wait_until(
               fun() -> portunus_ct_cluster:member_count(Nodes, ?NAME) =:= 3 end)
    after
        %% Stop the ad-hoc node even if an assertion above aborts the test.
        catch peer:stop(ExtraPeer)
    end.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

machine() ->
    {module, portunus_machine, #{cluster => ?NAME, tick_interval_ms => ?TICK_MS}}.

grant(Holder, TtlMs) ->
    portunus_ct_cluster:until_quorum(Holder, grant_lease, [?NAME, TtlMs]).

acquire(Holder, Key, Lease, Owner) ->
    portunus_ct_cluster:until_quorum(Holder, acquire, [?NAME, Key, Lease, Owner]).
