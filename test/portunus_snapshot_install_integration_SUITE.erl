%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_snapshot_install_integration_SUITE).

%% A node joining after the log was truncated receives the state as a
%% snapshot install, not a replay. Leadership then moves to the joiner and
%% its installed state must serve reads and mint above the old watermark.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([joiner_serves_from_installed_snapshot/1]).
%% Runs on the seed peer over rpc.
-export([spin_commands/3]).

-define(SYS, portunus_snapshot_install_integration_sys).
-define(NAME, portunus_snap_install_test).

all() ->
    [joiner_serves_from_installed_snapshot].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok ->
            DataDir = filename:join(?config(priv_dir, Config), "ra_local"),
            ok = filelib:ensure_dir(filename:join(DataDir, "x")),
            ok = portunus:start_system(?SYS, DataDir),
            Config;
        Skip ->
            Skip
    end.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    catch ra:force_delete_server(?SYS, {?NAME, node()}),
    ok.

%% Peers are linked to the starting process, so they must start here, in
%% the test case's own process, not in init_per_suite.
init_per_testcase(_TC, Config) ->
    [{cluster, portunus_ct_cluster:start(Config, ?NAME, 1,
                  #{env => [{snapshot_interval, 64}]})} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

joiner_serves_from_installed_snapshot(Config) ->
    #{nodes := [Seed]} = ?config(cluster, Config),
    Key = {res, install},
    Holder = portunus_ct_cluster:start_client(Seed),
    {ok, L} = portunus_ct_cluster:until_quorum(
                Holder, grant_lease, [?NAME, 60000, #{proposed_id => snap_l}]),
    {ok, T1} = portunus_ct_cluster:until_quorum(
                 Holder, acquire, [?NAME, Key, L, owner_a]),
    %% Past ra's 4096-entry floor, so the seed snapshots and truncates.
    ok = rpc:call(Seed, ?MODULE, spin_commands, [?NAME, L, 4500]),
    ct:log("commands spun"),
    ok = portunus_ct_cluster:wait_until(
           fun() ->
                   KM = rpc:call(Seed, ra, key_metrics, [{?NAME, Seed}]),
                   is_map(KM) andalso maps:get(snapshot_index, KM, 0) > 0
           end, 300),
    ct:log("seed snapshotted"),
    ok = portunus:join_cluster(?SYS, ?NAME, Seed),
    ok = portunus_ct_cluster:wait_until(fun() -> portunus:is_member(?NAME) end),
    ct:log("joined"),
    %% The joiner's state arrived as a snapshot, not a replay from index 0.
    ok = portunus_ct_cluster:wait_until(
           fun() ->
                   KM = ra:key_metrics({?NAME, node()}),
                   maps:get(snapshot_index, KM, 0) > 0
           end),
    ct:log("joiner snapshotted"),
    ok = rpc:call(Seed, ra, transfer_leadership,
                  [{?NAME, Seed}, {?NAME, node()}]),
    ok = portunus_ct_cluster:wait_until(
           fun() ->
                   ra_leaderboard:lookup_leader(?NAME) =:= {?NAME, node()}
           end),
    ct:log("leadership transferred"),
    {ok, #{owner := owner_a, token := T1}} = portunus:owner(?NAME, Key),
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {ok, T2} = portunus:acquire(?NAME, {res, install_2}, L2, owner_b),
    ?assert(T2 > T1),
    ok = portunus:revoke_lease(?NAME, L2),
    Holder ! stop.

%% Renewals no longer append (they travel over the aux transport), so
%% grant-and-revoke pairs drive the log instead.
spin_commands(Name, _Lease, N) ->
    [begin
         {ok, Ln} = portunus:grant_lease(Name, 60000),
         ok = portunus:revoke_lease(Name, Ln)
     end || _ <- lists:seq(1, N div 2 + 1)],
    ok.
