%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_service_SUITE).

%% `portunus_service` coverage: one owner per key, `stop/2` on hand-off, group
%% namespacing, and crashed-election restart. The suite is its own callback
%% module, routing start/stop notifications to the testcase.

-behaviour(portunus_service).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([starts_one_owner_per_key/1,
         stop_runs_the_stop_callback/1,
         group_isolates_services/1,
         crashed_election_is_restarted/1,
         transfer_routes_to_the_keys_election/1,
         duplicate_keys_start_one_election/1]).
-export([keys/1, start/3, stop/2]).

-define(SYS, portunus).
-define(NAME, portunus_service_test).
-define(TTL, 2000).

all() ->
    [starts_one_owner_per_key, stop_runs_the_stop_callback,
     group_isolates_services, crashed_election_is_restarted,
     transfer_routes_to_the_keys_election, duplicate_keys_start_one_election].

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
    ok.

%% `portunus_service` callbacks. Args carries the keys and a collector pid that
%% `start/3` and `stop/2` report to.
keys(#{keys := Keys}) -> Keys.

start(Key, Token, #{collector := C}) ->
    C ! {started, Key, Token, self()},
    {ok, {Key, C}}.

stop(Key, {Key, C}) ->
    C ! {stopped, Key},
    ok.

starts_one_owner_per_key(_Config) ->
    {ok, Svc} = start_service([k1, k2], #{}),
    {T1, _} = await_started(k1),
    {T2, _} = await_started(k2),
    ?assert(is_integer(T1) andalso is_integer(T2)),
    ok = portunus_service:stop(Svc).

stop_runs_the_stop_callback(_Config) ->
    {ok, Svc} = start_service([k1, k2], #{}),
    _ = await_started(k1),
    _ = await_started(k2),
    ok = portunus_service:stop(Svc),
    await_stopped(k1),
    await_stopped(k2).

group_isolates_services(_Config) ->
    {ok, SvcA} = start_service([k], #{group => svc_grp_a}),
    {ok, SvcB} = start_service([k], #{group => svc_grp_b}),
    %% Same key, different groups, so both services own and start it.
    await_n_started(k, 2),
    ok = portunus_service:stop(SvcA),
    ok = portunus_service:stop(SvcB).

crashed_election_is_restarted(_Config) ->
    {ok, Svc} = start_service([k], #{}),
    {_, Election1} = await_started(k),
    exit(Election1, kill),
    %% The restarted election wins again once the old lease expires, so `start/3`
    %% runs a second time under a fresh election pid.
    {_, Election2} = await_started(k),
    ?assert(Election2 =/= Election1),
    ok = portunus_service:stop(Svc).

%% `transfer/3` looks the election up by the bare key (the lock key is the
%% namespaced `{Group, Key}`): a self-transfer on the owned key is `ok`,
%% an unknown key is `not_owner`, and the owner keeps running throughout.
transfer_routes_to_the_keys_election(_Config) ->
    {ok, Svc} = start_service([k1], #{}),
    _ = await_started(k1),
    ok = portunus_service:transfer(Svc, k1, node()),
    {error, not_owner} = portunus_service:transfer(Svc, no_such_key, node()),
    receive {stopped, k1} -> ct:fail(owner_stopped) after 200 -> ok end,
    ok = portunus_service:stop(Svc).

%% A duplicate key from `keys/1` starts one tracked election, not a second,
%% untracked one that would win invisibly.
duplicate_keys_start_one_election(_Config) ->
    {ok, Svc} = start_service([k1, k1], #{}),
    {_, Pid} = await_started(k1),
    receive {started, k1, _, Other} -> ct:fail({second_election, Other})
    after 1000 -> ok
    end,
    ?assert(is_pid(Pid)),
    ok = portunus_service:stop(Svc).

start_service(Keys, Opts) ->
    portunus_service:start_link(?NAME, ?MODULE,
                                #{keys => Keys, collector => self()},
                                Opts#{ttl_ms => ?TTL}).

await_started(Key) ->
    receive {started, Key, Token, Pid} -> {Token, Pid}
    after 15000 -> ct:fail({no_start, Key}) end.

await_stopped(Key) ->
    receive {stopped, Key} -> ok
    after 5000 -> ct:fail({no_stop, Key}) end.

await_n_started(_Key, 0) -> ok;
await_n_started(Key, N) ->
    receive {started, Key, _, _} -> await_n_started(Key, N - 1)
    after 15000 -> ct:fail({missing_starts, Key, N}) end.
