%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([grant_acquire_release/1,
         acquire_conflict/1,
         reacquire_idempotent/1,
         release_token_fenced/1,
         revoke_releases/1,
         lease_expiry/1,
         renew_keeps_alive/1,
         proposed_lease_id_idempotent/1,
         with_lock/1,
         lock_conflict_releases/1,
         session/1,
         election/1,
         election_start_failure_recovers/1,
         election_bad_affinity_degrades/1,
         registry/1,
         registry_affinity_opt/1,
         delayed_restart/1,
         delayed_forget/1,
         affinity_score/1,
         affinity_rebid_and_negative/1,
         has_quorum_and_status/1,
         property_core_invariant/1,
         property_fifo_succession/1]).

%% Started on the local node by the registry test's child spec.
-export([start_registry_worker/1, noop_start/0]).

-define(SYS, portunus_sys).
-define(NAME, portunus_test).

all() ->
    [grant_acquire_release,
     acquire_conflict,
     reacquire_idempotent,
     release_token_fenced,
     revoke_releases,
     lease_expiry,
     renew_keeps_alive,
     proposed_lease_id_idempotent,
     with_lock,
     lock_conflict_releases,
     session,
     election,
     election_start_failure_recovers,
     election_bad_affinity_degrades,
     registry,
     registry_affinity_opt,
     delayed_restart,
     delayed_forget,
     affinity_score,
     affinity_rebid_and_negative,
     has_quorum_and_status,
     property_core_invariant,
     property_fifo_succession].

init_per_suite(Config) ->
    %% A short tick so lease expiry happens promptly in tests.
    application:set_env(portunus, tick_interval_ms, 200),
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = wait_leader(?NAME, 100),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

grant_acquire_release(_Config) ->
    K = {res, key1},
    {ok, Lease} = portunus:grant_lease(?NAME, 60000),
    {ok, Token} = portunus:acquire(?NAME, K, Lease, owner1),
    ?assert(is_integer(Token)),
    {ok, #{owner := owner1, token := Token}} = portunus:owner(?NAME, K),
    ok = portunus:release(?NAME, K, Token),
    {error, not_held} = portunus:owner(?NAME, K),
    ok = portunus:revoke_lease(?NAME, Lease).

acquire_conflict(_Config) ->
    K = {res, key2},
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {ok, _T} = portunus:acquire(?NAME, K, L1, owner_a),
    {error, {held_by, owner_a}} = portunus:acquire(?NAME, K, L2, owner_b),
    ok = portunus:revoke_lease(?NAME, L1),
    ok = portunus:revoke_lease(?NAME, L2).

reacquire_idempotent(_Config) ->
    K = {res, key3},
    {ok, L} = portunus:grant_lease(?NAME, 60000),
    {ok, T1} = portunus:acquire(?NAME, K, L, owner_a),
    %% Re-acquiring the same key under the same lease returns the same token.
    {ok, T2} = portunus:acquire(?NAME, K, L, owner_a),
    ?assertEqual(T1, T2),
    ok = portunus:revoke_lease(?NAME, L).

release_token_fenced(_Config) ->
    K = {res, key4},
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {ok, T1} = portunus:acquire(?NAME, K, L1, owner_a),
    ok = portunus:revoke_lease(?NAME, L1),
    %% A second owner takes it; the stale token must not release the new holder.
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {ok, T2} = portunus:acquire(?NAME, K, L2, owner_b),
    ?assert(T2 > T1),
    {error, not_owner} = portunus:release(?NAME, K, T1),
    {ok, #{owner := owner_b}} = portunus:owner(?NAME, K),
    ok = portunus:revoke_lease(?NAME, L2).

revoke_releases(_Config) ->
    K1 = {res, k5a}, K2 = {res, k5b},
    {ok, L} = portunus:grant_lease(?NAME, 60000),
    {ok, _} = portunus:acquire(?NAME, K1, L, o),
    {ok, _} = portunus:acquire(?NAME, K2, L, o),
    ok = portunus:revoke_lease(?NAME, L),
    {error, not_held} = portunus:owner(?NAME, K1),
    {error, not_held} = portunus:owner(?NAME, K2).

lease_expiry(_Config) ->
    K = {res, key6},
    {ok, L} = portunus:grant_lease(?NAME, 300),
    {ok, _} = portunus:acquire(?NAME, K, L, owner_a),
    %% No renewal: the lease expires and the lock is reclaimed.
    ok = wait_until(fun() ->
                            portunus:owner(?NAME, K) =:= {error, not_held}
                    end, 50),
    ?assertEqual({error, not_held}, portunus:owner(?NAME, K)).

renew_keeps_alive(_Config) ->
    K = {res, key7},
    {ok, L} = portunus:grant_lease(?NAME, 1000),
    {ok, _} = portunus:acquire(?NAME, K, L, owner_a),
    %% Renew every 200 ms, well inside the 1000 ms TTL, for longer than the
    %% TTL: the lease stays held only because of the renewals, and the wide
    %% per-renew margin keeps a slow CI scheduler from letting it expire.
    [begin [{L, ok}] = portunus:renew_leases(?NAME, [L]), timer:sleep(200) end
     || _ <- lists:seq(1, 6)],
    {ok, #{owner := owner_a}} = portunus:owner(?NAME, K),
    ok = portunus:revoke_lease(?NAME, L).

proposed_lease_id_idempotent(_Config) ->
    Id = {my, stable, lease},
    {ok, Id} = portunus:grant_lease(?NAME, 60000, #{proposed_id => Id}),
    %% Re-granting the same id by the same owner is idempotent.
    {ok, Id} = portunus:grant_lease(?NAME, 60000, #{proposed_id => Id}),
    ok = portunus:revoke_lease(?NAME, Id).

with_lock(_Config) ->
    K = {res, key8},
    Result = portunus:with_lock(?NAME, K, 60000,
                                fun() ->
                                        {ok, #{owner := _}} =
                                            portunus:owner(?NAME, K),
                                        worked
                                end),
    ?assertEqual(worked, Result),
    %% Released after the fun returns.
    ok = wait_until(fun() ->
                            portunus:owner(?NAME, K) =:= {error, not_held}
                    end, 50).

lock_conflict_releases(_Config) ->
    K = {res, key9},
    {ok, L} = portunus:grant_lease(?NAME, 60000),
    {ok, _T} = portunus:acquire(?NAME, K, L, holder),
    Before = maps:get(leases, portunus:status(?NAME)),
    %% `lock/3` must fail on a held key without leaking its lease.
    ?assertMatch({error, {held_by, holder}}, portunus:lock(?NAME, K, 60000)),
    ok = wait_until(fun() ->
                            maps:get(leases, portunus:status(?NAME)) =:= Before
                    end, 50),
    ok = portunus:revoke_lease(?NAME, L).

session(_Config) ->
    {ok, S} = portunus_session:open(?NAME, #{ttl_ms => 60000}),
    {ok, _T1} = portunus_session:claim(S, {vhost, a}),
    {ok, _T2} = portunus_session:claim(S, {vhost, b}),
    ?assertEqual(lists:sort([{vhost, a}, {vhost, b}]),
                 lists:sort(portunus_session:keys(S))),
    {ok, #{owner := Owner}} = portunus:owner(?NAME, {vhost, a}),
    Owner = node(),
    ok = portunus_session:release(S, {vhost, a}),
    {error, not_held} = portunus:owner(?NAME, {vhost, a}),
    %% Closing the session drops the remaining keys.
    ok = portunus_session:close(S),
    ok = wait_until(fun() ->
                            portunus:owner(?NAME, {vhost, b}) =:=
                                {error, not_held}
                    end, 50).

election(_Config) ->
    Key = {election, sched},
    Self = self(),
    {ok, E1} = portunus_election:start_link(?NAME, Key,
                                            portunus_demo_election, Self),
    %% The first participant is elected.
    Leader1 = receive {elected, Key, _T, Pid1} -> Pid1
              after 30000 -> ct:fail(no_election) end,
    ?assertEqual(E1, Leader1),
    ?assert(portunus_election:is_leader(E1)),
    %% A second participant contends but does not become leader.
    {ok, E2} = portunus_election:start_link(?NAME, Key,
                                            portunus_demo_election, Self),
    timer:sleep(300),
    ?assertNot(portunus_election:is_leader(E2)),
    %% When the leader steps down, the standby is elected.
    ok = portunus_election:stop(E1),
    Leader2 = receive {elected, Key, _T2, Pid2} -> Pid2
              after 30000 -> ct:fail(no_promotion) end,
    ?assertEqual(E2, Leader2),
    ok = portunus_election:stop(E2).

election_start_failure_recovers(_Config) ->
    Key = {election, crashy},
    %% The callback's start always crashes. On this single node the election
    %% wins, the start fails, and it must release and re-contend rather than
    %% crash with the lock held.
    {ok, E} = portunus_election:start_link(?NAME, Key,
                                           portunus_failing_election, undefined),
    %% It survives the failed start (and the re-contend that follows) instead
    %% of dying, and never reports itself leader.
    timer:sleep(500),
    ?assert(is_process_alive(E)),
    ?assertNot(portunus_election:is_leader(E)),
    ok = portunus_election:stop(E),
    %% Stopping releases the lock cleanly.
    ok = wait_until(fun() -> portunus:owner(?NAME, Key) =:= {error, not_held} end,
                    100).

election_bad_affinity_degrades(_Config) ->
    Key = {election, badplace},
    Self = self(),
    %% A misconfigured affinity (the metric arg is not a fun) must not crash
    %% the election: scoring degrades to FIFO and the node still wins.
    Bad = {metric, not_a_fun},
    {ok, E} = portunus_election:start_link(?NAME, Key,
                                           portunus_demo_election, Self,
                                           #{affinity => Bad}),
    Leader = receive {elected, Key, _T, Pid} -> Pid
             after 30000 -> ct:fail(no_election) end,
    ?assertEqual(E, Leader),
    ?assert(portunus_election:is_leader(E)),
    ok = portunus_election:stop(E).

registry(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => 60000}),
    Key = {svc, reg_a},
    WName = registry_test_worker_a,
    Spec = #{id => Key,
             start => {?MODULE, start_registry_worker, [WName]},
             restart => transient, shutdown => 5000,
             type => worker, modules => [?MODULE]},
    ok = portunus_registry:add(Reg, Key, Spec),
    %% A single node always wins its own election, so the child runs here.
    ok = wait_until(fun() -> whereis(WName) =/= undefined end, 100),
    ?assertEqual([Key], portunus_registry:keys(Reg)),
    ok = wait_until(fun() -> portunus_registry:owned_keys(Reg) =:= [Key] end, 100),
    %% add is idempotent.
    ok = portunus_registry:add(Reg, Key, Spec),
    ?assertEqual([Key], portunus_registry:keys(Reg)),
    %% Removing it stops the child.
    ok = portunus_registry:remove(Reg, Key),
    ok = wait_until(fun() -> whereis(WName) =:= undefined end, 100),
    ?assertEqual([], portunus_registry:keys(Reg)),
    ok = portunus_registry:stop(Reg).

registry_affinity_opt(_Config) ->
    %% The registry threads a affinity option down to each election. On one
    %% node the pinned owner is this node, so the child still starts; this
    %% exercises the option wiring rather than the steering.
    {ok, Reg} = portunus_registry:start_link(
                  ?NAME, #{ttl_ms => 60000,
                           affinity => {pinned, node()}}),
    Key = {svc, placed},
    WName = registry_placed_worker,
    Spec = #{id => Key,
             start => {?MODULE, start_registry_worker, [WName]},
             restart => transient, shutdown => 5000,
             type => worker, modules => [?MODULE]},
    ok = portunus_registry:add(Reg, Key, Spec),
    ok = wait_until(fun() -> whereis(WName) =/= undefined end, 100),
    ok = portunus_registry:stop(Reg).

delayed_restart(_Config) ->
    %% `child_spec/1` rewrites a supervisor2-style {permanent, Delay} into a
    %% standard restart type plus a delayed start wrapper.
    Spec0 = {dr, {?MODULE, start_registry_worker, [dr_w]}, {permanent, 1},
             5000, worker, [?MODULE]},
    ?assertMatch({dr, {portunus_delayed_restart, start_link, [dr, 1, _]},
                  permanent, 5000, worker, [?MODULE]},
                 portunus_delayed_restart:child_spec(Spec0)),
    %% A standard restart type passes through untouched.
    Plain = #{id => p, start => {m, f, []}, restart => transient},
    ?assertEqual(Plain, portunus_delayed_restart:child_spec(Plain)),
    %% Functional: a delayed-restart child comes back after a crash.
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => 60000}),
    Key = {svc, dr},
    Spec = #{id => Key, start => {?MODULE, start_registry_worker, [dr_w2]},
             restart => {permanent, 1}, shutdown => 5000,
             type => worker, modules => [?MODULE]},
    ok = portunus_registry:add(Reg, Key, Spec),
    ok = wait_until(fun() -> is_pid(whereis(dr_w2)) end, 100),
    P1 = whereis(dr_w2),
    exit(P1, kill),
    %% The restart is delayed ~1s, then the worker returns with a fresh pid.
    ok = wait_until(fun() ->
                            case whereis(dr_w2) of
                                P when is_pid(P), P =/= P1 -> true;
                                _ -> false
                            end
                    end, 100),
    ok = portunus_registry:stop(Reg).

delayed_forget(_Config) ->
    Id = forget_demo,
    MFA = {?MODULE, noop_start, []},
    %% First start is immediate; a restart (marker set) waits the 1s delay.
    {T1, {ok, _}} = timer:tc(portunus_delayed_restart, start_link, [Id, 1, MFA]),
    {T2, {ok, _}} = timer:tc(portunus_delayed_restart, start_link, [Id, 1, MFA]),
    %% After a stop clears the marker, the next first start is immediate.
    ok = portunus_delayed_restart:forget(self(), Id),
    {T3, {ok, _}} = timer:tc(portunus_delayed_restart, start_link, [Id, 1, MFA]),
    ok = portunus_delayed_restart:forget(self(), Id),
    ?assert(T1 < 500000),
    ?assert(T2 >= 1000000),
    ?assert(T3 < 500000).

affinity_score(_Config) ->
    K = {res, affinity},
    {ok, LA} = portunus:grant_lease(?NAME, 60000),
    {ok, LB} = portunus:grant_lease(?NAME, 60000),
    {ok, LC} = portunus:grant_lease(?NAME, 60000),
    {ok, TA} = portunus:acquire(?NAME, K, LA, owner_a),
    %% Two waiters queue behind the holder; owner_c bids a higher score
    %% even though it queues last.
    {queued, _} = portunus:acquire_or_join_succession_queue(
                    ?NAME, K, LB, owner_b, #{score => 0}),
    {queued, _} = portunus:acquire_or_join_succession_queue(
                    ?NAME, K, LC, owner_c, #{score => 5}),
    ok = portunus:release(?NAME, K, TA),
    %% The higher score wins succession over the earlier-queued waiter.
    ok = wait_until(fun() ->
                            case portunus:owner(?NAME, K) of
                                {ok, #{owner := owner_c}} -> true;
                                _ -> false
                            end
                    end, 100),
    ok = portunus:revoke_lease(?NAME, LA),
    ok = portunus:revoke_lease(?NAME, LB),
    ok = portunus:revoke_lease(?NAME, LC).

affinity_rebid_and_negative(_Config) ->
    K = {res, affinity_rebid},
    {ok, LA} = portunus:grant_lease(?NAME, 60000),
    {ok, LB} = portunus:grant_lease(?NAME, 60000),
    {ok, LC} = portunus:grant_lease(?NAME, 60000),
    {ok, TA} = portunus:acquire(?NAME, K, LA, owner_a),
    %% owner_b starts below the default-zero waiter with a negative bid.
    {queued, _} = portunus:acquire_or_join_succession_queue(
                    ?NAME, K, LB, owner_b, #{score => -1}),
    {queued, _} = portunus:acquire_or_join_succession_queue(
                    ?NAME, K, LC, owner_c, #{score => 0}),
    %% It re-acquires with a winning bid; the public API refreshes its one
    %% waiter rather than queuing a duplicate.
    {queued, _} = portunus:acquire_or_join_succession_queue(
                    ?NAME, K, LB, owner_b, #{score => 5}),
    ok = portunus:release(?NAME, K, TA),
    %% The refreshed bid (5) wins over owner_c (0).
    ok = wait_until(fun() ->
                            case portunus:owner(?NAME, K) of
                                {ok, #{owner := owner_b}} -> true;
                                _ -> false
                            end
                    end, 100),
    ok = portunus:revoke_lease(?NAME, LA),
    ok = portunus:revoke_lease(?NAME, LB),
    ok = portunus:revoke_lease(?NAME, LC).

has_quorum_and_status(_Config) ->
    ?assert(portunus:has_quorum(?NAME)),
    Status = portunus:status(?NAME),
    ?assertEqual(true, maps:get(quorum, Status)),
    ?assertMatch([_ | _], maps:get(members, Status)),
    ?assert(is_integer(maps:get(locks, Status))).

property_core_invariant(_Config) ->
    %% The core safety property, driven against the machine's `apply/3`.
    ?assert(portunus_test_helpers:quickcheck(
              fun portunus_machine_prop:prop_single_owner_and_monotonic_tokens/0,
              300)).

property_fifo_succession(_Config) ->
    ?assert(portunus_test_helpers:quickcheck(
              fun portunus_machine_prop:prop_fifo_succession/0, 100)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% A trivial supervised worker that registers a name, so a test can see
%% whether the registry's elected owner started it.
start_registry_worker(Name) ->
    Pid = spawn_link(fun() ->
                             register(Name, self()),
                             receive stop -> ok end
                     end),
    {ok, Pid}.

%% A throwaway startable child for the delayed-restart timing test.
noop_start() ->
    {ok, spawn(fun() -> ok end)}.

%% Thin wrappers over the shared helpers, so a timeout fails with a clear
%% message instead of a badmatch on `{error, timeout}`.
wait_leader(Name, N) ->
    portunus_test_helpers:await_leader(Name, N * 50).

wait_until(Fun, N) ->
    portunus_test_helpers:await_condition(Fun, N * 50).
