%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_registry_recovery_SUITE).

%% A registry restarts an election that crashed while still registered, does not
%% restart one removed via `remove/2`, and stops (so its own supervisor can
%% restart it) when its local supervisor dies.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([crashed_election_is_restarted/1,
         removed_key_is_not_restarted/1,
         local_sup_death_stops_registry/1]).
-export([start_worker/1]).

-define(SYS, portunus).
-define(NAME, portunus_registry_recovery_test).
-define(TTL, 2000).

all() ->
    [crashed_election_is_restarted,
     removed_key_is_not_restarted,
     local_sup_death_stops_registry].

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

%% A registry links to its caller; trapping exits lets the test observe an
%% abnormal stop as a message rather than dying with it.
init_per_testcase(_Case, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

crashed_election_is_restarted(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    Key = {svc, crash},
    ok = portunus_registry:add(Reg, Key, worker_spec(Key, rr_w1)),
    ok = portunus_test_helpers:await_condition(
           fun() -> is_pid(whereis(rr_w1)) andalso portunus_registry:owned_keys(Reg) =:= [Key] end),
    Killed = election_pid(Reg, Key),
    exit(Killed, kill),
    %% The crashed election's orphan child is stopped, not left as a rogue owner.
    ok = portunus_test_helpers:await_condition(fun() -> whereis(rr_w1) =:= undefined end),
    %% After the old (un-revoked) lease expires, the restarted election wins
    %% again under a fresh pid and starts a fresh child.
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   case election_pid(Reg, Key) of
                       undefined -> false;
                       P -> P =/= Killed andalso is_pid(whereis(rr_w1))
                            andalso portunus_registry:owned_keys(Reg) =:= [Key]
                   end
           end, 15000),
    ok = portunus_registry:stop(Reg).

removed_key_is_not_restarted(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    Key = {svc, remove},
    ok = portunus_registry:add(Reg, Key, worker_spec(Key, rr_w2)),
    ok = portunus_test_helpers:await_condition(fun() -> is_pid(whereis(rr_w2)) end),
    ok = portunus_registry:remove(Reg, Key),
    ok = portunus_test_helpers:await_condition(fun() -> whereis(rr_w2) =:= undefined end),
    %% It stays gone: no key, no election, after a window longer than the backoff.
    timer:sleep(1500),
    ?assertEqual([], portunus_registry:keys(Reg)),
    ?assertEqual(undefined, whereis(rr_w2)),
    ok = portunus_registry:stop(Reg).

local_sup_death_stops_registry(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    Ref = monitor(process, Reg),
    exit(local_sup(Reg), kill),
    receive
        {'DOWN', Ref, process, Reg, Reason} ->
            ?assertMatch({local_sup_down, _}, Reason)
    after 5000 ->
        ct:fail(registry_survived_local_sup_death)
    end.

worker_spec(Key, RegName) ->
    #{id => Key, start => {?MODULE, start_worker, [RegName]},
      restart => transient, shutdown => 5000, type => worker, modules => [?MODULE]}.

start_worker(RegName) ->
    {ok, spawn_link(fun() -> register(RegName, self()), receive stop -> ok end end)}.

%% White-box access to the registry's internal state. #state is
%% {state, name, group, ttl_ms, affinity, local_sup, elections}.
election_pid(Reg, Key) ->
    Elections = element(7, sys:get_state(Reg)),
    case maps:find(Key, Elections) of
        {ok, {Pid, _Spec}} -> Pid;
        error -> undefined
    end.

local_sup(Reg) ->
    element(6, sys:get_state(Reg)).
