%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_transfer_integration_SUITE).

%% `portunus:transfer/4` through a live cluster: the machine unit suite
%% covers the token fence in isolation; here a stale token is refused
%% through the full command path after a real handoff, and the transfer
%% counters are visible in `portunus_counters:overview/1`.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([stale_token_is_refused_after_handoff/1,
         transfer_counters_are_published/1]).

-define(SYS, portunus_transfer_integration_sys).
-define(NAME, portunus_transfer_integration_test).

all() ->
    [stale_token_is_refused_after_handoff,
     transfer_counters_are_published].

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

%% After a committed handoff the old owner's token is fenced out: replaying
%% the transfer with it is `not_owner` and the new owner keeps the key.
stale_token_is_refused_after_handoff(_Config) ->
    Key = {api, stale_after_handoff},
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {ok, T1} = portunus:acquire(?NAME, Key, L1, o1),
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {queued, 1} = portunus:acquire_or_join_succession_queue(?NAME, Key, L2, o2),
    ok = portunus:transfer(?NAME, Key, T1, o2),
    {ok, #{owner := o2, token := T2}} = portunus:owner(?NAME, Key),
    ?assert(T2 > T1),
    {error, not_owner} = portunus:transfer(?NAME, Key, T1, o1),
    {ok, #{owner := o2, token := T2}} = portunus:owner(?NAME, Key).

%% Both transfer counters reach the node's seshat set: the success through
%% the machine's leader-side `mod_call` effect, the committed refusal
%% likewise.
transfer_counters_are_published(_Config) ->
    Key = {api, counters},
    Before = portunus_counters:overview(?NAME),
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {ok, T1} = portunus:acquire(?NAME, Key, L1, o1),
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {queued, 1} = portunus:acquire_or_join_succession_queue(?NAME, Key, L2, o2),
    {error, {no_contender, o9}} = portunus:transfer(?NAME, Key, T1, o9),
    ok = portunus:transfer(?NAME, Key, T1, o2),
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   After = portunus_counters:overview(?NAME),
                   count_of(transfers_total, After) =:=
                       count_of(transfers_total, Before) + 1
                       andalso count_of(transfer_no_contender_total, After) =:=
                           count_of(transfer_no_contender_total, Before) + 1
           end).

count_of(Field, Overview) ->
    maps:get(Field, Overview, 0).
