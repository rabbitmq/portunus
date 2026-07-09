%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_acquire_with_timeout_integration_SUITE).

%% `portunus:acquire_with_timeout/5` through a live cluster: a free key returns at
%% once, a released key is granted within the wait, a timeout withdraws the
%% bid so the caller is never granted later, and both outcomes of the
%% promoted-at-the-deadline race are settled correctly
%% (`settle_timed_out_bid/3`, exported for tests: the race itself cannot be
%% timed deterministically).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([free_key_returns_at_once/1,
         release_within_the_wait_grants/1,
         timeout_withdraws_the_bid/1,
         settle_returns_a_key_the_race_granted/1,
         settle_without_the_key_is_timeout/1]).

-define(SYS, portunus).
-define(NAME, portunus_acquire_with_timeout_test).

all() ->
    [free_key_returns_at_once,
     release_within_the_wait_grants,
     timeout_withdraws_the_bid,
     settle_returns_a_key_the_race_granted,
     settle_without_the_key_is_timeout].

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

free_key_returns_at_once(_Config) ->
    Key = {aa, free},
    {ok, L} = portunus:grant_lease(?NAME, 60000),
    {ok, Token} = portunus:acquire_with_timeout(?NAME, Key, L, o1, 5000),
    {ok, #{owner := o1, token := Token}} = portunus:owner(?NAME, Key).

%% The holder releases mid-wait; the waiting caller is granted the key
%% before its deadline.
release_within_the_wait_grants(_Config) ->
    Key = {aa, granted},
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {ok, T1} = portunus:acquire(?NAME, Key, L1, o1),
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    _ = spawn_link(fun() ->
                           timer:sleep(300),
                           ok = portunus:release(?NAME, Key, T1)
                   end),
    {ok, T2} = portunus:acquire_with_timeout(?NAME, Key, L2, o2, 30000),
    ?assert(T2 > T1),
    {ok, #{owner := o2}} = portunus:owner(?NAME, Key).

%% On timeout the bid is withdrawn: the caller is not a contender any
%% longer, and the holder's later release frees the key instead of
%% granting it to the caller.
timeout_withdraws_the_bid(_Config) ->
    Key = {aa, timed_out},
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {ok, T1} = portunus:acquire(?NAME, Key, L1, o1),
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {error, timeout} = portunus:acquire_with_timeout(?NAME, Key, L2, o2, 200),
    {ok, []} = portunus:contenders(?NAME, Key),
    ok = portunus:release(?NAME, Key, T1),
    {error, not_held} = portunus:owner(?NAME, Key),
    receive {portunus, granted, Key, _, _} -> ct:fail(granted_after_timeout)
    after 500 -> ok
    end.

%% The race arm where the grant committed just before the withdrawal: the
%% settlement finds the key owned by the caller's lease and returns it.
settle_returns_a_key_the_race_granted(_Config) ->
    Key = {aa, race_won},
    {ok, L} = portunus:grant_lease(?NAME, 60000),
    {ok, Token} = portunus:acquire(?NAME, Key, L, o1),
    {ok, Token} = portunus:settle_timed_out_bid(?NAME, Key, L).

%% The race arm where the bid vanished without a grant (the lease died and
%% the queue dropped it): settlement reports the timeout.
settle_without_the_key_is_timeout(_Config) ->
    Key = {aa, race_lost},
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {ok, _} = portunus:acquire(?NAME, Key, L1, o1),
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {error, timeout} = portunus:settle_timed_out_bid(?NAME, Key, L2),
    {error, timeout} = portunus:settle_timed_out_bid(?NAME, {aa, nothere}, L2).
