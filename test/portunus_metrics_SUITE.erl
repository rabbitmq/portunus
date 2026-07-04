%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_metrics_SUITE).

%% The wired gauges are populated on the leader: `is_leader` and
%% `leader_changes_total` from the machine's `state_enter`, and the rest from
%% `handle_aux` after a lock is granted.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([gauges_are_populated/1]).

-define(SYS, portunus).
-define(NAME, portunus_metrics_test).

all() ->
    [gauges_are_populated].

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

gauges_are_populated(_Config) ->
    {ok, L} = portunus:grant_lease(?NAME, 60000),
    {ok, _T} = portunus:acquire(?NAME, {res, metric}, L, owner_a),
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   O = portunus_counters:overview(?NAME),
                   maps:get(is_leader, O, 0) =:= 1
                       andalso maps:get(has_quorum, O, 0) =:= 1
                       andalso maps:get(cluster_members, O, 0) =:= 1
                       andalso maps:get(raft_term, O, 0) >= 1
                       andalso maps:get(locks_held, O, 0) =:= 1
                       andalso maps:get(fencing_token, O, 0) > 0
                       andalso maps:get(leader_changes_total, O, 0) >= 1
           end),
    ok = portunus:revoke_lease(?NAME, L).
