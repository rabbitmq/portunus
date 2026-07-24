%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_aux_renewal_integration_SUITE).

%% Off-log renewal against a real cluster: a renewed lease outlives many
%% TTLs while the Raft log does not grow, an abandoned lease still expires
%% through the aux sweep (promoting the next waiter and notifying the
%% holder), a restart recovers leases that then expire unless renewed, and
%% a leader that lost its quorum cannot acknowledge a renewal (the property
%% that forced `ra:consistent_aux/3` over a plain aux command).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([renewal_writes_nothing_to_the_log/1,
         abandoned_lease_expires_and_promotes/1,
         restart_recovers_then_expires_unrenewed/1,
         leader_transfer_keeps_renewed_lease/1,
         quorum_loss_renewal_times_out/1]).

-define(SYS, portunus_aux_renewal_sys).
-define(NAME, portunus_aux_renewal_test).
-define(TTL, 3000).

all() ->
    [renewal_writes_nothing_to_the_log,
     abandoned_lease_expires_and_promotes,
     restart_recovers_then_expires_unrenewed,
     leader_transfer_keeps_renewed_lease,
     quorum_loss_renewal_times_out].

init_per_suite(Config) ->
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

renewal_writes_nothing_to_the_log(_Config) ->
    Key = {res, no_log_growth},
    {ok, Lease} = portunus:grant_lease(?NAME, ?TTL),
    {ok, KA} = portunus:keep_alive(?NAME, Lease, ?TTL),
    {ok, _} = portunus:acquire(?NAME, Key, Lease, holder),
    [{Lease, ok}] = portunus:renew_leases(?NAME, [Lease]),
    Before = last_index(),
    %% Two full TTLs of renewals: with logged renewals this was several
    %% appends per round (plus expiry-timer re-arms); now it is zero.
    timer:sleep(2 * ?TTL),
    ?assertEqual(Before, last_index()),
    ?assertMatch({ok, #{owner := holder}}, portunus:owner(?NAME, Key)),
    ok = portunus_keepalive:stop(KA),
    ok = portunus:revoke_lease(?NAME, Lease).

abandoned_lease_expires_and_promotes(_Config) ->
    Key = {res, promote},
    {ok, L1} = portunus:grant_lease(?NAME, ?TTL),
    {ok, L2} = portunus:grant_lease(?NAME, ?TTL),
    {ok, KA} = portunus:keep_alive(?NAME, L2, ?TTL),
    {ok, _} = portunus:acquire(?NAME, Key, L1, owner_a),
    {queued, 1} = portunus:acquire_or_join_succession_queue(?NAME, Key, L2,
                                                            owner_b),
    %% Nothing renews L1: the sweep expires it within TTL + tick + slack,
    %% promotes the waiter, and notifies the abandoning holder.
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   case portunus:owner(?NAME, Key) of
                       {ok, #{owner := owner_b, lease := L2}} -> true;
                       _ -> false
                   end
           end),
    receive
        {portunus, lease_lost, L1} -> ok
    after 5000 ->
            ct:fail("no lease_lost notice for the expired lease")
    end,
    ok = portunus_keepalive:stop(KA),
    ok = portunus:revoke_lease(?NAME, L2).

restart_recovers_then_expires_unrenewed(_Config) ->
    Key = {res, restart},
    {ok, Lease} = portunus:grant_lease(?NAME, ?TTL),
    {ok, _} = portunus:acquire(?NAME, Key, Lease, holder),
    ok = ra:stop_server(?SYS, {?NAME, node()}),
    ok = portunus:restart_server(?SYS, ?NAME),
    ok = portunus_test_helpers:await_leader(?NAME),
    %% Recovered, not expired: the new leader seeds the full TTL (leases die
    %% late, never early).
    ?assertMatch({ok, #{owner := holder}}, portunus:owner(?NAME, Key)),
    %% Unrenewed, it then expires within TTL + tick + slack.
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:owner(?NAME, Key) =:= {error, not_held} end).

leader_transfer_keeps_renewed_lease(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> leader_transfer_keeps_renewed_lease1(Config);
        {skip, _} = Skip -> Skip
    end.

leader_transfer_keeps_renewed_lease1(Config) ->
    Name = portunus_aux_transfer_test,
    Cluster = portunus_ct_cluster:start(Config, Name, 3),
    #{nodes := Nodes} = Cluster,
    try
        {Name, LeaderNode} = portunus_ct_cluster:wait_leader(Nodes, Name),
        %% A long-lived holder process: the machine monitors the granting
        %% pid, and a transient `rpc` process would drop the lease at once.
        Client = portunus_ct_cluster:start_client(hd(Nodes)),
        Key = {res, transfer},
        {ok, L1} = portunus_ct_cluster:ccall(Client, grant_lease,
                                             [Name, ?TTL]),
        {ok, _} = portunus_ct_cluster:ccall(Client, acquire,
                                            [Name, Key, L1, holder]),
        [{L1, ok}] = portunus_ct_cluster:ccall(Client, renew_leases,
                                               [Name, [L1]]),
        [Target | _] = [N || N <- Nodes, N =/= LeaderNode],
        ok = rpc:call(LeaderNode, ra, transfer_leadership,
                      [{Name, LeaderNode}, {Name, Target}]),
        ok = portunus_test_helpers:await_condition(
               fun() ->
                       case rpc:call(Target, ra_leaderboard, lookup_leader,
                                     [Name]) of
                           {Name, N} when N =/= LeaderNode -> true;
                           _ -> false
                       end
               end),
        %% The new leader has none of the old leader's deadlines: the
        %% lease is seeded at its full TTL and a renewal succeeds.
        ok = portunus_test_helpers:await_condition(
               fun() ->
                       portunus_ct_cluster:ccall(Client, renew_leases,
                                                 [Name, [L1], 2000])
                           =:= [{L1, ok}]
               end),
        %% Abandoned after the transfer (the holder stays alive but stops
        %% renewing), the lease still expires: within one extra TTL of the
        %% ordinary bound, late but never held forever.
        ok = portunus_test_helpers:await_condition(
               fun() ->
                       portunus_ct_cluster:ccall(Client, owner, [Name, Key])
                           =:= {error, not_held}
               end, 3 * ?TTL + 5000),
        Client ! stop
    after
        portunus_ct_cluster:stop(Cluster)
    end.

quorum_loss_renewal_times_out(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> quorum_loss_renewal_times_out1(Config);
        {skip, _} = Skip -> Skip
    end.

quorum_loss_renewal_times_out1(Config) ->
    Name = portunus_aux_quorum_test,
    Cluster = portunus_ct_cluster:start(Config, Name, 3),
    #{nodes := Nodes} = Cluster,
    try
        {Name, LeaderNode} = portunus_ct_cluster:wait_leader(Nodes, Name),
        Client = portunus_ct_cluster:start_client(LeaderNode),
        {ok, Lease} = portunus_ct_cluster:ccall(Client, grant_lease,
                                                [Name, ?TTL]),
        [{Lease, ok}] = portunus_ct_cluster:ccall(Client, renew_leases,
                                                  [Name, [Lease]]),
        Others = [N || N <- Nodes, N =/= LeaderNode],
        [ok = portunus_ct_cluster:stop_ra_server(N, Name) || N <- Others],
        %% The heartbeat round cannot complete without a quorum, so the
        %% deposed-or-isolated leader must not acknowledge: the reply is a
        %% transient failure, never `ok`.
        ?assertEqual([{Lease, {error, no_quorum}}],
                     portunus_ct_cluster:ccall(Client, renew_leases,
                                               [Name, [Lease], 2000])),
        Client ! stop
    after
        portunus_ct_cluster:stop(Cluster)
    end.

last_index() ->
    maps:get(last_index, ra:key_metrics({?NAME, node()}), 0).
