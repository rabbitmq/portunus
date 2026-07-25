%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_service_transfer_batch_SUITE).

-behaviour(portunus_service).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([transfer_many_routes_over_the_election_map/1]).
-export([keys/1, start/3, stop/2]).

-define(SYS, portunus_service_transfer_batch_sys).
-define(NAME, portunus_service_transfer_batch_test).
-define(TTL, 2000).

all() ->
    [transfer_many_routes_over_the_election_map].

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

transfer_many_routes_over_the_election_map(_Config) ->
    {ok, Svc} = start_service([k1, k2]),
    _ = await_started(k1),
    _ = await_started(k2),
    Results = portunus_service:transfer_many(
                Svc, [{k1, node()}, {k2, node()}, {k2, node()}, {ghost, node()}]),
    ?assertEqual([{k1, ok}, {k2, ok}, {ghost, {error, not_owner}}], Results),
    receive {stopped, _} -> ct:fail(child_stopped) after 200 -> ok end,
    ok = portunus_service:stop(Svc).

%% `portunus_service` callbacks.
keys(#{keys := Keys}) -> Keys.

start(Key, Token, #{collector := C}) ->
    C ! {started, Key, Token, self()},
    {ok, {Key, C}}.

stop(Key, {Key, C}) ->
    C ! {stopped, Key},
    ok.

%% Helpers

start_service(Keys) ->
    portunus_service:start_link(?NAME, ?MODULE,
                                #{keys => Keys, collector => self()},
                                #{ttl_ms => ?TTL}).

await_started(Key) ->
    receive {started, Key, Token, Pid} -> {Token, Pid}
    after 15000 -> ct:fail({no_start, Key}) end.
