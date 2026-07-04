%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_namespacing_SUITE).

%% Two registries on one cluster with the same child id each
%% run their own child, because lock keys are namespaced by group.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([same_id_in_two_registries_both_run/1]).
-export([start_worker/1]).

-define(SYS, portunus).
-define(NAME, portunus_namespacing_test).

all() ->
    [same_id_in_two_registries_both_run].

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

same_id_in_two_registries_both_run(_Config) ->
    {ok, RegA} = portunus_registry:start_link({local, ns_reg_a}, ?NAME, #{ttl_ms => 60000}),
    {ok, RegB} = portunus_registry:start_link({local, ns_reg_b}, ?NAME, #{ttl_ms => 60000}),
    Id = shared_child,
    ok = portunus_registry:add(RegA, Id, worker_spec(Id, ns_worker_a)),
    ok = portunus_registry:add(RegB, Id, worker_spec(Id, ns_worker_b)),
    ok = portunus_test_helpers:await_condition(
           fun() -> is_pid(whereis(ns_worker_a)) andalso is_pid(whereis(ns_worker_b)) end),
    ok = portunus_registry:stop(RegA),
    ok = portunus_registry:stop(RegB).

worker_spec(Id, RegName) ->
    #{id => Id, start => {?MODULE, start_worker, [RegName]},
      restart => transient, shutdown => 5000, type => worker, modules => [?MODULE]}.

start_worker(RegName) ->
    {ok, spawn_link(fun() -> register(RegName, self()), receive stop -> ok end end)}.
