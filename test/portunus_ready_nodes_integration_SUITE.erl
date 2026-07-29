%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_ready_nodes_integration_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([ready_nodes_track_live_bids/1]).

-define(SYS, portunus_ready_nodes_int_sys).
-define(NAME, portunus_ready_nodes_int_test).

all() ->
    [ready_nodes_track_live_bids].

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

ready_nodes_track_live_bids(_Config) ->
    Key = {ready, k},
    ?assertEqual({ok, []}, portunus:ready_nodes(?NAME)),
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {ok, _} = portunus:acquire(?NAME, Key, L1, holder),
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {queued, _} = portunus:acquire_or_join_succession_queue(?NAME, Key, L2, n2),
    {ok, L3} = portunus:grant_lease(?NAME, 60000),
    {queued, _} = portunus:acquire_or_join_succession_queue(?NAME, Key, L3, n3),
    %% The local query can lag the commits it follows.
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:ready_nodes(?NAME) =:= {ok, [n2, n3]} end),
    ok = portunus:revoke_lease(?NAME, L2),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:ready_nodes(?NAME) =:= {ok, [n3]} end).
