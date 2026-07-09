%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_leave_queue_integration_SUITE).

%% `portunus:leave_succession_queue/3` through a live cluster: a contender
%% that left is skipped on the next promotion, its lease and its other
%% claims survive, and the withdrawal is counted.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([promotion_skips_a_contender_that_left/1,
         leave_without_a_bid_is_not_queued/1]).

-define(SYS, portunus).
-define(NAME, portunus_leave_queue_test).

all() ->
    [promotion_skips_a_contender_that_left,
     leave_without_a_bid_is_not_queued].

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

%% o2 outranks o3 but leaves before the release: o3 is promoted, o2's lease
%% still holds its other key, and `queue_leaves_total` moved.
promotion_skips_a_contender_that_left(_Config) ->
    Key = {dq, skip},
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {ok, T1} = portunus:acquire(?NAME, Key, L1, o1),
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {ok, _} = portunus:acquire(?NAME, {dq, other}, L2, o2),
    {ok, L3} = portunus:grant_lease(?NAME, 60000),
    {queued, 1} = portunus:acquire_or_join_succession_queue(
                    ?NAME, Key, L2, o2, #{score => 5}),
    {queued, 2} = portunus:acquire_or_join_succession_queue(?NAME, Key, L3, o3),
    Before = maps:get(queue_leaves_total, portunus_counters:overview(?NAME), 0),
    ok = portunus:leave_succession_queue(?NAME, Key, L2),
    ok = portunus:release(?NAME, Key, T1),
    {ok, #{owner := o3}} = portunus:owner(?NAME, Key),
    {ok, #{owner := o2}} = portunus:owner(?NAME, {dq, other}),
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   maps:get(queue_leaves_total,
                            portunus_counters:overview(?NAME), 0) =:= Before + 1
           end).

leave_without_a_bid_is_not_queued(_Config) ->
    Key = {dq, none},
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {error, not_queued} = portunus:leave_succession_queue(?NAME, Key, L1).
