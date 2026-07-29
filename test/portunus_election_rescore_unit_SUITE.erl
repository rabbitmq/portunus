%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_election_rescore_unit_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([changed_score_rebids_with_the_new_score/1,
         unchanged_score_reads_and_does_not_rebid/1,
         rebid_grant_installs_leadership_and_drops_late_granted/1,
         pending_transfer_never_rebids/1,
         lease_expired_rebid_recontends_at_once/1,
         failed_rebid_is_retried_next_round/1]).

-define(NAME, portunus_election_rescore_test).
-define(KEY, {election, rescore}).
%% The TTL floor, so the reconcile fires at its 1000 ms minimum and each
%% test waits for at most a round or two.
-define(TTL, 2000).
-define(LEASE, rescore_lease).
-define(TAB, portunus_election_rescore_scores).

all() ->
    [changed_score_rebids_with_the_new_score,
     unchanged_score_reads_and_does_not_rebid,
     rebid_grant_installs_leadership_and_drops_late_granted,
     pending_transfer_never_rebids,
     lease_expired_rebid_recontends_at_once,
     failed_rebid_is_retried_next_round].

init_per_testcase(_TC, Config) ->
    process_flag(trap_exit, true),
    %% A registered mock for the shared renewer. Used so that the election's
    %% monitor on the name holds.
    %% This trick keeps the suite independent of application start order.
    KA = ensure_keepalive(),
    ?TAB = ets:new(?TAB, [named_table, public]),
    ets:insert(?TAB, {score, 1}),
    ok = meck:new(portunus, [passthrough, no_link]),
    ok = meck:new(portunus_batch_keepalive, [passthrough, no_link]),
    meck:expect(portunus_batch_keepalive, attach, fun(_N, _L, _T) -> ok end),
    meck:expect(portunus_batch_keepalive, detach, fun(_N, _L) -> ok end),
    meck:expect(portunus, grant_lease, fun(_N, _T) -> {ok, ?LEASE} end),
    meck:expect(portunus, revoke_lease, fun(_N, _L) -> ok end),
    meck:expect(portunus, owner, fun(_N, _K) -> {error, not_held} end),
    %% An explicit `score` resolves as in production; anything else reads
    %% the mutable table, standing in for a `dynamic` strategy.
    meck:expect(portunus, succession_score,
                fun(_N, _K, #{score := S}) -> S;
                   (_N, _K, _Opts) -> ets:lookup_element(?TAB, score, 2)
                end),
    meck:expect(portunus, acquire_or_join_succession_queue,
                fun(_N, _K, _L, _O, _Opts) -> {queued, 1} end),
    [{keepalive, KA} | Config].

end_per_testcase(_TC, Config) ->
    catch meck:unload(portunus_batch_keepalive),
    catch meck:unload(portunus),
    catch ets:delete(?TAB),
    stop_keepalive(?config(keepalive, Config)),
    ok.

%% Reuses an already-registered renewer (a full run leaves the real one
%% up); spawns an idle stand-in only when the name is free.
ensure_keepalive() ->
    case whereis(portunus_batch_keepalive) of
        undefined ->
            KA = spawn(fun idle/0),
            register(portunus_batch_keepalive, KA),
            KA;
        _Existing ->
            existing
    end.

stop_keepalive(KA) when is_pid(KA) ->
    catch exit(KA, kill),
    ok;
stop_keepalive(_) ->
    ok.

%% After the score changes, the next reconciliation re-submits the bid carrying
%% the new score. Rounds after the first one do not issue any bids.
changed_score_rebids_with_the_new_score(_Config) ->
    E = start_queued(),
    ets:insert(?TAB, {score, 7}),
    ok = meck:wait(2, portunus, acquire_or_join_succession_queue, '_', 3000),
    ?assertMatch(#{score := 7},
                 meck:capture(2, portunus, acquire_or_join_succession_queue,
                              '_', 5)),
    %% The stored score now matches the table, so a later round reads.
    Reads = meck:num_calls(portunus, owner, '_'),
    ok = meck:wait(Reads + 1, portunus, owner, '_', 3000),
    ?assertEqual(2, meck:num_calls(portunus, acquire_or_join_succession_queue,
                                   '_')),
    stop(E).

%% A stable score issues no command: every reconcile round is the read.
unchanged_score_reads_and_does_not_rebid(_Config) ->
    E = start_queued(),
    ok = meck:wait(2, portunus, owner, '_', 4000),
    ?assertEqual(1, meck:num_calls(portunus, acquire_or_join_succession_queue,
                                   '_')),
    stop(E).

rebid_grant_installs_leadership_and_drops_late_granted(_Config) ->
    meck:expect(portunus, acquire_or_join_succession_queue, 5,
                meck:seq([{queued, 1}, {ok, 42}])),
    E = start_queued(),
    ets:insert(?TAB, {score, 7}),
    expect_elected(42),
    E ! {portunus, granted, ?KEY, 43, ?LEASE},
    refute_callback(),
    ?assert(portunus_election:is_leader(E)),
    stop(E).

pending_transfer_never_rebids(_Config) ->
    meck:expect(portunus, acquire_or_join_succession_queue, 5,
                meck:seq([{ok, 42}])),
    meck:expect(portunus, contenders, fun(_N, _K) -> {ok, [other@node]} end),
    meck:expect(portunus, transfer, fun(_N, _K, _T, _O) ->
                                            {error, no_quorum}
                                    end),
    %% The read must stay inconclusive, or the reconcile would resolve the
    %% pending transfer and re-contend, which also calls acquire.
    meck:expect(portunus, owner, fun(_N, _K) -> {error, no_quorum} end),
    E = start_elected(42),
    {error, no_quorum} = portunus_election:transfer_to(E, other@node),
    ets:insert(?TAB, {score, 7}),
    Reads = meck:num_calls(portunus, owner, '_'),
    ok = meck:wait(Reads + 2, portunus, owner, '_', 4000),
    ?assertEqual(1, meck:num_calls(portunus, acquire_or_join_succession_queue,
                                   '_')),
    stop(E).

lease_expired_rebid_recontends_at_once(_Config) ->
    meck:expect(portunus, acquire_or_join_succession_queue, 5,
                meck:seq([{queued, 1}, {error, lease_expired}, {queued, 1}])),
    E = start_queued(),
    ets:insert(?TAB, {score, 7}),
    %% The third acquire is the fresh contend after the teardown.
    ok = meck:wait(3, portunus, acquire_or_join_succession_queue, '_', 4000),
    ?assertEqual(2, meck:num_calls(portunus, grant_lease, '_')),
    ?assert(meck:num_calls(portunus, revoke_lease, '_') >= 1),
    refute_callback(),
    ?assertNot(portunus_election:is_leader(E)),
    stop(E).

%% Any other re-bid error leaves the stored score alone, so the next round
%% retries the re-bid. The lease ownership is retained.
failed_rebid_is_retried_next_round(_Config) ->
    meck:expect(portunus, acquire_or_join_succession_queue, 5,
                meck:seq([{queued, 1}, {error, no_quorum}, {queued, 1}])),
    E = start_queued(),
    ets:insert(?TAB, {score, 7}),
    ok = meck:wait(3, portunus, acquire_or_join_succession_queue, '_', 4000),
    ?assertEqual(1, meck:num_calls(portunus, grant_lease, '_')),
    ?assertNot(portunus_election:is_leader(E)),
    stop(E).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

start_queued() ->
    {ok, E} = portunus_election:start_link(?NAME, ?KEY,
                                           portunus_demo_election, self(),
                                           #{ttl_ms => ?TTL}),
    ?assertNot(portunus_election:is_leader(E)),
    E.

start_elected(Token) ->
    {ok, E} = portunus_election:start_link(?NAME, ?KEY,
                                           portunus_demo_election, self(),
                                           #{ttl_ms => ?TTL}),
    expect_elected(Token),
    E.

expect_elected(Token) ->
    receive
        {elected, ?KEY, Token, _Pid} -> ok
    after 5000 -> ct:fail(not_elected)
    end.

refute_callback() ->
    receive
        {elected, _, _, _} -> ct:fail(unexpected_elected);
        {stepped_down, _, _} -> ct:fail(unexpected_stepped_down)
    after 300 -> ok
    end.

stop(E) ->
    _ = catch portunus_election:stop(E),
    ok.

idle() ->
    receive _ -> idle() end.
