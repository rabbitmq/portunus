%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_lock_unit_SUITE).

%% `with_lock/4` releases the lock even when the wrapped function raises, so a
%% crash in the critical section cannot strand the key.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([with_lock_releases_on_exception/1]).

-define(SYS, portunus_lock_unit_sys).
-define(NAME, portunus_lock_test).

all() ->
    [with_lock_releases_on_exception].

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

with_lock_releases_on_exception(_Config) ->
    Key = {res, with_lock_raise},
    ?assertError(boom,
                 portunus:with_lock(?NAME, Key, 60000, fun() -> error(boom) end)),
    ok = portunus_test_helpers:await_condition(fun() -> portunus:owner(?NAME, Key) =:= {error, not_held} end).
