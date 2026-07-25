%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_election_transfer_batch_unit_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
         end_per_testcase/2]).
-export([settled_ok_recontends_with_one_stepdown/1,
         settled_no_contender_restores_on_same_token/1,
         lease_lost_between_prepare_and_settle_drops_late_settle/1,
         orphaned_prepare_uncommitted_restores_via_reconcile/1,
         orphaned_prepare_committed_recontends_via_reconcile/1,
         prepared_election_outlasts_the_short_reconcile/1]).

-define(SYS, portunus_election_transfer_batch_unit_sys).
-define(NAME, portunus_election_transfer_batch_test).
-define(TTL, 3000).
-define(PEER, 'peer@nohost').

all() ->
    [settled_ok_recontends_with_one_stepdown,
     settled_no_contender_restores_on_same_token,
     lease_lost_between_prepare_and_settle_drops_late_settle,
     orphaned_prepare_uncommitted_restores_via_reconcile,
     orphaned_prepare_committed_recontends_via_reconcile,
     prepared_election_outlasts_the_short_reconcile].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

init_per_testcase(Case, Config)
  when Case =:= orphaned_prepare_uncommitted_restores_via_reconcile;
       Case =:= orphaned_prepare_committed_recontends_via_reconcile ->
    %% The orphan cases wait out the settle deadline (30 s by default); a
    %% short one keeps them fast without changing the path they exercise.
    application:set_env(portunus, transfer_settle_deadline_ms, 2000),
    process_flag(trap_exit, true),
    Config;
init_per_testcase(_Case, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_Case, _Config) ->
    application:unset_env(portunus, transfer_settle_deadline_ms),
    ok.

settled_ok_recontends_with_one_stepdown(_Config) ->
    Key = {election, batch_ok},
    {ok, E} = start(Key),
    T1 = receive {elected, Key, Tok, E} -> Tok after 30000 -> ct:fail(no_leader) end,
    PeerLease = enqueue_peer(Key),
    {ok, Key, T1} = portunus_election:prepare_transfer(E, ?PEER),
    receive {stepped_down, Key, E} -> ok after 5000 -> ct:fail(no_stepdown) end,
    [{Key, ok}] = portunus:transfer_many(?NAME, [{Key, T1, ?PEER}]),
    receive {portunus, granted, Key, T2, PeerLease} -> ?assert(T2 > T1)
    after 5000 -> ct:fail(peer_not_granted)
    end,
    ok = portunus_election:settle_transfer(E, ok),
    false = portunus_election:is_leader(E),
    ok = portunus:revoke_lease(?NAME, PeerLease),
    T3 = receive {elected, Key, Tok3, E} -> Tok3
         after 30000 -> ct:fail(no_win_back)
         end,
    ?assert(T3 > T1),
    receive {stepped_down, Key, E} -> ct:fail(second_stepdown) after 200 -> ok end,
    ok = portunus_election:stop(E).

%% `{no_contender, _}` means the key never moved, so the work restores on the
%% token prepare returned, with no teardown.
settled_no_contender_restores_on_same_token(_Config) ->
    Key = {election, batch_no_contender},
    {ok, E} = start(Key),
    T1 = receive {elected, Key, Tok, E} -> Tok after 30000 -> ct:fail(no_leader) end,
    PeerLease = enqueue_peer(Key),
    {ok, Key, T1} = portunus_election:prepare_transfer(E, ?PEER),
    receive {stepped_down, Key, E} -> ok after 5000 -> ct:fail(no_stepdown) end,
    ok = portunus_election:settle_transfer(E, {error, {no_contender, ?PEER}}),
    receive {elected, Key, T1, E} -> ok after 5000 -> ct:fail(not_restored) end,
    true = portunus_election:is_leader(E),
    {ok, #{token := T1}} = portunus:owner(?NAME, Key),
    ok = portunus:revoke_lease(?NAME, PeerLease),
    ok = portunus_election:stop(E).

lease_lost_between_prepare_and_settle_drops_late_settle(_Config) ->
    Key = {election, batch_stale_loss},
    {ok, E} = start(Key),
    receive {elected, Key, _T, E} -> ok after 30000 -> ct:fail(no_leader) end,
    {ok, #{lease := OldLease}} = portunus:owner(?NAME, Key),
    PeerLease = enqueue_peer(Key),
    {ok, Key, _T1} = portunus_election:prepare_transfer(E, ?PEER),
    receive {stepped_down, Key, E} -> ok after 5000 -> ct:fail(no_stepdown) end,
    E ! {portunus, lease_lost, OldLease},
    receive {stepped_down, Key, E} -> ct:fail(second_stepdown) after 500 -> ok end,
    %% The revoke behind `lease_lost` frees the key, promoting the peer.
    receive {portunus, granted, Key, _T2, PeerLease} -> ok
    after 5000 -> ct:fail(peer_not_granted)
    end,
    ok = portunus_election:settle_transfer(E, ok),
    receive {stepped_down, Key, E} -> ct:fail(stepdown_after_settle) after 500 -> ok end,
    ok = portunus:revoke_lease(?NAME, PeerLease),
    receive {elected, Key, _T3, E} -> ok after 30000 -> ct:fail(no_win_back) end,
    ok = portunus_election:stop(E).

orphaned_prepare_uncommitted_restores_via_reconcile(_Config) ->
    Key = {election, batch_orphan_uncommitted},
    {ok, E} = start(Key),
    T1 = receive {elected, Key, Tok, E} -> Tok after 30000 -> ct:fail(no_leader) end,
    PeerLease = enqueue_peer(Key),
    {ok, Key, T1} = portunus_election:prepare_transfer(E, ?PEER),
    receive {stepped_down, Key, E} -> ok after 5000 -> ct:fail(no_stepdown) end,
    %% No command, no settle: the reconcile backstop (ttl/3) restores.
    receive {elected, Key, T1, E} -> ok after 30000 -> ct:fail(not_restored) end,
    true = portunus_election:is_leader(E),
    receive {stepped_down, Key, E} -> ct:fail(second_stepdown) after 200 -> ok end,
    ok = portunus:revoke_lease(?NAME, PeerLease),
    ok = portunus_election:stop(E).

%% The reconcile read finds the key moved to the peer, so the election must
%% re-contend as a standby, never restore work on the stale token.
orphaned_prepare_committed_recontends_via_reconcile(_Config) ->
    Key = {election, batch_orphan_committed},
    {ok, E} = start(Key),
    T1 = receive {elected, Key, Tok, E} -> Tok after 30000 -> ct:fail(no_leader) end,
    PeerLease = enqueue_peer(Key),
    {ok, Key, T1} = portunus_election:prepare_transfer(E, ?PEER),
    receive {stepped_down, Key, E} -> ok after 5000 -> ct:fail(no_stepdown) end,
    [{Key, ok}] = portunus:transfer_many(?NAME, [{Key, T1, ?PEER}]),
    receive {portunus, granted, Key, T2, PeerLease} -> ?assert(T2 > T1)
    after 5000 -> ct:fail(peer_not_granted)
    end,
    %% No settle: the reconcile read finds the peer owning and re-contends,
    %% never restoring the work on the stale token.
    receive {elected, Key, _, E} -> ct:fail(dual_run) after ?TTL -> ok end,
    {ok, #{lease := PeerLease}} = portunus:owner(?NAME, Key),
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   {ok, Owners} = portunus:contenders(?NAME, Key),
                   lists:member(node(), Owners)
           end, 30000),
    ok = portunus:revoke_lease(?NAME, PeerLease),
    receive {elected, Key, T3, E} -> ?assert(T3 > T1)
    after 30000 -> ct:fail(no_win_back)
    end,
    ok = portunus_election:stop(E).

%% A prepared election must not restore at the ordinary reconcile interval
%% (`ttl_ms div 3`): the batch command may still commit and move the key,
%% and restored work would then run on two nodes.
prepared_election_outlasts_the_short_reconcile(_Config) ->
    Key = {election, batch_deadline},
    {ok, E} = start(Key),
    T1 = receive {elected, Key, Tok, E} -> Tok after 30000 -> ct:fail(no_leader) end,
    PeerLease = enqueue_peer(Key),
    {ok, Key, T1} = portunus_election:prepare_transfer(E, ?PEER),
    receive {stepped_down, Key, E} -> ok after 5000 -> ct:fail(no_stepdown) end,
    %% Several ttl/3 intervals pass with no settle: no restore fires.
    receive {elected, Key, _, E} -> ct:fail(early_restore) after 4000 -> ok end,
    false = portunus_election:is_leader(E),
    [{Key, ok}] = portunus:transfer_many(?NAME, [{Key, T1, ?PEER}]),
    ok = portunus_election:settle_transfer(E, ok),
    ok = portunus:revoke_lease(?NAME, PeerLease),
    receive {elected, Key, T3, E} -> ?assert(T3 > T1)
    after 30000 -> ct:fail(no_win_back)
    end,
    ok = portunus_election:stop(E).

%% Helpers

start(Key) ->
    portunus_election:start_link(?NAME, Key, portunus_demo_election,
                                 self(), #{ttl_ms => ?TTL}).

%% A live contender for `Key` with owner `?PEER`, created through the raw API:
%% its lease belongs to this process and outlives the test case.
enqueue_peer(Key) ->
    {ok, Lease} = portunus:grant_lease(?NAME, 60000),
    {queued, _} = portunus:acquire_or_join_succession_queue(?NAME, Key, Lease,
                                                            ?PEER),
    Lease.
