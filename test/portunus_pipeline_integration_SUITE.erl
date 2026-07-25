%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_pipeline_integration_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([contend_burst_resolves_every_key/1]).

-define(SYS, portunus_pipeline_sys).
-define(NAME, portunus_pipeline_test).
-define(TTL, 60000).
-define(KEYS, 300).

all() ->
    [contend_burst_resolves_every_key].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_suite(Config) ->
    Config.

contend_burst_resolves_every_key(_Config) ->
    Failures0 = quorum_failures(),
    Parent = self(),
    Pids = [spawn_link(
              fun() ->
                      {ok, LeaseId} = portunus:grant_lease(?NAME, ?TTL),
                      Result = portunus:acquire(?NAME, {burst_key, I}, LeaseId, node()),
                      Parent ! {contended, I, LeaseId, Result},
                      %% Stay alive so the machine's monitor does not release
                      %% the key before the assertions below run.
                      receive stop -> ok end
              end) || I <- lists:seq(1, ?KEYS)],
    Results = [receive {contended, I, L, R} -> {I, L, R}
               after 30000 -> ct:fail("contender ~b never answered", [I])
               end || I <- lists:seq(1, ?KEYS)],
    [?assertMatch({_, _, {ok, _}}, Res) || Res <- Results],
    %% Every key is owned by the lease that acquired it.
    [begin
         {ok, #{lease := Owner}} = portunus:owner(?NAME, {burst_key, I}),
         ?assertEqual(LeaseId, Owner)
     end || {I, LeaseId, _} <- Results],
    ?assertEqual(Failures0, quorum_failures()),
    [P ! stop || P <- Pids],
    ok.

quorum_failures() ->
    maps:get(failures_due_to_lack_of_online_quorum_total,
             portunus_counters:overview(?NAME), 0).
