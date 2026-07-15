%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_snapshot_integration_SUITE).

%% With release cursors the log is snapshotted and truncated, so a restart
%% recovers from a snapshot rather than a full replay: held locks, tokens
%% and the mint watermark must all survive that path.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([state_survives_snapshot_recovery/1]).

-define(SYS, portunus_snapshot_integration_sys).
-define(NAME, portunus_snapshot_test).

all() ->
    [state_survives_snapshot_recovery].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    %% Ra only snapshots when the machine suggests a release cursor, and
    %% never before its own 4096-entry floor. Suggest often, then drive
    %% past the floor.
    application:set_env(portunus, snapshot_interval, 64),
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_suite(_Config) ->
    application:unset_env(portunus, snapshot_interval),
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

state_survives_snapshot_recovery(_Config) ->
    Key = {res, snapshot},
    {ok, L} = portunus:grant_lease(?NAME, 60000, #{proposed_id => snap_lease}),
    {ok, T1} = portunus:acquire(?NAME, Key, L, owner_a),
    %% Enough commands to cross ra's 4096-entry snapshot floor.
    [[{L, ok}] = portunus:renew_leases(?NAME, [L]) || _ <- lists:seq(1, 4500)],
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   KM = ra:key_metrics({?NAME, node()}),
                   maps:get(snapshot_index, KM, 0) > 0
           end, 30000),
    ok = ra:stop_server(?SYS, {?NAME, node()}),
    ok = ra:restart_server(?SYS, {?NAME, node()}),
    _ = catch ra:trigger_election({?NAME, node()}),
    ok = portunus_test_helpers:await_leader(?NAME),
    %% The lock, its token and the lease all came back from the snapshot.
    {ok, #{owner := owner_a, token := T1}} = portunus:owner(?NAME, Key),
    [{L, ok}] = portunus:renew_leases(?NAME, [L]),
    %% Minting continues above the pre-restart watermark.
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {ok, T2} = portunus:acquire(?NAME, {res, snapshot_2}, L2, owner_b),
    ?assert(T2 > T1),
    ok = portunus:revoke_lease(?NAME, L2),
    ok = portunus:revoke_lease(?NAME, L).
