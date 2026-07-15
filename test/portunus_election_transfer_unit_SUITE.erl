%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_election_transfer_unit_SUITE).

%% `portunus_election:transfer_to/2` beyond the refusal cases in
%% `portunus_election_unit_SUITE`: the success ordering (one `stepped_down`,
%% then a re-contend as a standby), a stale `lease_lost` delivered around the
%% command, and the two outcomes of a timed-out (`no_quorum`) transfer
%% command: not committed (the owner is restored on its own token) and
%% committed after all (the old owner re-contends, never restarting the work).
%%
%% The target is a contender created through the raw API with owner
%% `peer@nohost`, so the non-self transfer path runs on one node; the timeout
%% is injected by stubbing `portunus:transfer/4` with meck. The mock is
%% installed (and unloaded) only while no election or keepalive is running,
%% because loading or unloading a mock purges the module and kills any
%% process caught executing it, such as a keepalive mid-renew; in between,
%% the stub is added and removed with `meck:expect` and `meck:delete`,
%% which do not purge.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
         end_per_testcase/2]).
-export([transfer_recontends_with_one_stepdown/1,
         stale_lease_lost_after_transfer_is_dropped/1,
         uncommitted_no_quorum_restores_owner_on_same_token/1,
         committed_no_quorum_recontends_without_restart/1]).

-define(SYS, portunus_election_transfer_unit_sys).
-define(NAME, portunus_election_transfer_test).
-define(TTL, 3000).
-define(PEER, 'peer@nohost').

all() ->
    [transfer_recontends_with_one_stepdown,
     stale_lease_lost_after_transfer_is_dropped,
     uncommitted_no_quorum_restores_owner_on_same_token,
     committed_no_quorum_recontends_without_restart].

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

init_per_testcase(_Case, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_Case, _Config) ->
    catch meck:unload(portunus),
    ok.

%% A successful transfer runs `stepped_down` exactly once and leaves the
%% former owner a re-contending standby: when the new owner's lease is
%% revoked it wins the key back at a higher token.
transfer_recontends_with_one_stepdown(_Config) ->
    Key = {election, xfer_ok},
    {ok, E} = start(Key),
    T1 = receive {elected, Key, Tok, E} -> Tok after 30000 -> ct:fail(no_leader) end,
    PeerLease = enqueue_peer(Key),
    ok = portunus_election:transfer_to(E, ?PEER),
    receive {stepped_down, Key, E} -> ok after 5000 -> ct:fail(no_stepdown) end,
    %% The peer's grant goes to this process, which holds the peer's lease.
    receive {portunus, granted, Key, T2, PeerLease} -> ?assert(T2 > T1)
    after 5000 -> ct:fail(peer_not_granted)
    end,
    false = portunus_election:is_leader(E),
    ok = portunus:revoke_lease(?NAME, PeerLease),
    T3 = receive {elected, Key, Tok3, E} -> Tok3
         after 30000 -> ct:fail(no_win_back)
         end,
    ?assert(T3 > T1),
    receive {stepped_down, Key, E} -> ct:fail(second_stepdown) after 200 -> ok end,
    ok = portunus_election:stop(E).

%% A `lease_lost` for the pre-transfer lease delivered after the transfer is
%% dropped: the election has already reset, so no second `stepped_down` fires
%% and the election still re-contends.
stale_lease_lost_after_transfer_is_dropped(_Config) ->
    Key = {election, xfer_stale_loss},
    {ok, E} = start(Key),
    receive {elected, Key, _T, E} -> ok after 30000 -> ct:fail(no_leader) end,
    {ok, #{lease := OldLease}} = portunus:owner(?NAME, Key),
    PeerLease = enqueue_peer(Key),
    ok = portunus_election:transfer_to(E, ?PEER),
    receive {stepped_down, Key, E} -> ok after 5000 -> ct:fail(no_stepdown) end,
    E ! {portunus, lease_lost, OldLease},
    receive {stepped_down, Key, E} -> ct:fail(second_stepdown) after 500 -> ok end,
    ok = portunus:revoke_lease(?NAME, PeerLease),
    receive {elected, Key, _T2, E} -> ok after 30000 -> ct:fail(no_win_back) end,
    ok = portunus_election:stop(E).

%% The transfer command times out without committing: the owner still holds
%% the key, so the work stays stopped only until the reconciliation read confirms
%% ownership and restores it on the unchanged token. Exactly one
%% `stepped_down` and one restoring `elected` fire.
uncommitted_no_quorum_restores_owner_on_same_token(_Config) ->
    Key = {election, xfer_timeout_uncommitted},
    meck:new(portunus, [passthrough]),
    {ok, E} = start(Key),
    T1 = receive {elected, Key, Tok, E} -> Tok after 30000 -> ct:fail(no_leader) end,
    _PeerLease = enqueue_peer(Key),
    meck:expect(portunus, transfer, fun(_, _, _, _) -> {error, no_quorum} end),
    {error, no_quorum} = portunus_election:transfer_to(E, ?PEER),
    receive {stepped_down, Key, E} -> ok after 5000 -> ct:fail(no_stepdown) end,
    false = portunus_election:is_leader(E),
    meck:delete(portunus, transfer, 4),
    receive
        {elected, Key, T1, E} -> ok
    after 30000 ->
        ct:fail(not_restored)
    end,
    true = portunus_election:is_leader(E),
    receive {stepped_down, Key, E} -> ct:fail(second_stepdown) after 200 -> ok end,
    ok = portunus_election:stop(E).

%% The transfer command times out but committed: the target owns the key, so
%% the old owner must not restart the work on its stale token (that would run
%% it on two nodes with no correction). It re-contends instead, and the
%% target stays the owner.
committed_no_quorum_recontends_without_restart(_Config) ->
    Key = {election, xfer_timeout_committed},
    meck:new(portunus, [passthrough]),
    {ok, E} = start(Key),
    T1 = receive {elected, Key, Tok, E} -> Tok after 30000 -> ct:fail(no_leader) end,
    PeerLease = enqueue_peer(Key),
    meck:expect(portunus, transfer,
                fun(N, K, T, Tgt) ->
                        ok = meck:passthrough([N, K, T, Tgt]),
                        {error, no_quorum}
                end),
    {error, no_quorum} = portunus_election:transfer_to(E, ?PEER),
    receive {stepped_down, Key, E} -> ok after 5000 -> ct:fail(no_stepdown) end,
    receive {portunus, granted, Key, T2, PeerLease} -> ?assert(T2 > T1)
    after 5000 -> ct:fail(peer_not_granted)
    end,
    meck:delete(portunus, transfer, 4),
    %% The old owner never restores on its stale token; it settles as a
    %% queued standby behind the peer, which remains the owner.
    receive {elected, Key, _, E} -> ct:fail(dual_run) after 4000 -> ok end,
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

%% Helpers

start(Key) ->
    portunus_election:start_link(?NAME, Key, portunus_demo_election,
                                 self(), #{ttl_ms => ?TTL}).

%% A live contender for `Key` with owner `?PEER`, created through the raw
%% API: its lease belongs to this process and outlives the test case.
enqueue_peer(Key) ->
    {ok, Lease} = portunus:grant_lease(?NAME, 60000),
    {queued, _} = portunus:acquire_or_join_succession_queue(?NAME, Key, Lease,
                                                            ?PEER),
    Lease.
