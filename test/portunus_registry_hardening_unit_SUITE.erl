%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_registry_hardening_unit_SUITE).

%% Registration-time validation, the `remove/2`-inside-backoff race, and
%% `owned_keys/1` staying safe while elections are blocked.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([add_validates_specs/1,
         re_add_semantics/1,
         remove_in_backoff_cancels_restart/1,
         owned_keys_survives_blocked_elections/1,
         named_registries_default_group_to_name/1,
         unknown_calls_get_an_error/1,
         supervisor_propagates_spec_errors/1,
         supervisor_passes_ignore_through/1]).
%% portunus_supervisor callback (this suite doubles as the callback module)
-export([init/1]).
%% worker start for child specs
-export([start_worker/1]).

-define(SYS, portunus).
-define(NAME, portunus_reg_hardening_test).
-define(TTL, 2000).

all() ->
    [add_validates_specs,
     re_add_semantics,
     remove_in_backoff_cancels_restart,
     owned_keys_survives_blocked_elections,
     named_registries_default_group_to_name,
     unknown_calls_get_an_error,
     supervisor_propagates_spec_errors,
     supervisor_passes_ignore_through].

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

add_validates_specs(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    %% Not a child spec at all.
    ?assertMatch({error, {invalid_child_spec, _}},
                 portunus_registry:add(Reg, k1, garbage)),
    ?assertMatch({error, {invalid_child_spec, _}},
                 portunus_registry:add(Reg, garbage)),
    %% A supervisor2 restart type the rewriter does not accept.
    Intrinsic = {w1, {?MODULE, start_worker, [rh_w0]},
                 {intrinsic, 5}, 5000, worker, [?MODULE]},
    ?assertMatch({error, {invalid_child_spec, _}},
                 portunus_registry:add(Reg, k2, Intrinsic)),
    %% One child id under two keys would share one local child.
    ok = portunus_registry:add(Reg, k3, worker_spec(shared_id, rh_w1)),
    ?assertMatch({error, {duplicate_child_id, shared_id}},
                 portunus_registry:add(Reg, k4, worker_spec(shared_id, rh_w2))),
    ok = portunus_registry:stop(Reg).

re_add_semantics(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    Spec = worker_spec(re_add_id, rh_w3),
    ok = portunus_registry:add(Reg, k1, Spec),
    %% Identical spec: idempotent. A different one: an error, not a silent
    %% keep-the-old-spec.
    ?assertEqual(ok, portunus_registry:add(Reg, k1, Spec)),
    ?assertMatch({error, {already_added, k1}},
                 portunus_registry:add(Reg, k1, worker_spec(re_add_id, rh_w4))),
    ok = portunus_registry:stop(Reg).

remove_in_backoff_cancels_restart(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    Key = {svc, backoff_remove},
    ok = portunus_registry:add(Reg, Key, worker_spec(Key, rh_w5)),
    ok = portunus_test_helpers:await_condition(
           fun() -> is_pid(whereis(rh_w5)) end),
    exit(election_pid(Reg, Key), kill),
    %% Only remove once the registry has marked the entry restarting, so the
    %% test pins the backoff-cancel path, not the live-pid removal.
    ok = portunus_test_helpers:await_condition(
           fun() -> election_pid(Reg, Key) =:= restarting end),
    ok = portunus_registry:remove(Reg, Key),
    timer:sleep(1500),
    ?assertEqual([], portunus_registry:keys(Reg)),
    ?assertEqual(undefined, whereis(rh_w5)),
    ok = portunus_registry:stop(Reg).

owned_keys_survives_blocked_elections(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    Key = {svc, blocked},
    ok = portunus_registry:add(Reg, Key, worker_spec(Key, rh_w6)),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus_registry:owned_keys(Reg) =:= [Key] end),
    %% A suspended election stands in for one blocked in a Ra command
    %% during a quorum loss: `is_leader` times out. Before the off-process probes that
    %% timeout crashed the registry and every child with it.
    Election = election_pid(Reg, Key),
    ok = sys:suspend(Election),
    ?assertEqual([], portunus_registry:owned_keys(Reg)),
    ?assert(is_process_alive(Reg)),
    ok = sys:resume(Election),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus_registry:owned_keys(Reg) =:= [Key] end),
    ok = portunus_registry:stop(Reg).

%% Every registered-name form derives the default group from the name, so
%% two named registries never silently share a namespace.
named_registries_default_group_to_name(_Config) ->
    {ok, R1} = portunus_registry:start_link({local, rh_reg_local}, ?NAME, #{}),
    ?assertEqual(rh_reg_local, group_of(R1)),
    {ok, R2} = portunus_registry:start_link({global, rh_reg_global}, ?NAME, #{}),
    ?assertEqual(rh_reg_global, group_of(R2)),
    ok = portunus_registry:stop(R1),
    ok = portunus_registry:stop(R2).

unknown_calls_get_an_error(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{}),
    ?assertEqual({error, unknown_call}, gen_server:call(Reg, nonsense)),
    ok = portunus_registry:stop(Reg).

supervisor_propagates_spec_errors(_Config) ->
    ?assertMatch({error, {invalid_child_spec, _}},
                 portunus_supervisor:start_link(?NAME, ?MODULE, bad_spec)),
    ok.

supervisor_passes_ignore_through(_Config) ->
    ?assertEqual(ignore,
                 portunus_supervisor:start_link(?NAME, ?MODULE, ignore)),
    ok.

%%----------------------------------------------------------------------
%% portunus_supervisor callback
%%----------------------------------------------------------------------

init(bad_spec) ->
    {ok, {#{strategy => one_for_one, intensity => 1, period => 5},
          [not_a_child_spec]}};
init(ignore) ->
    ignore.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

worker_spec(Id, RegName) ->
    #{id => Id, start => {?MODULE, start_worker, [RegName]},
      restart => transient, shutdown => 5000, type => worker,
      modules => [?MODULE]}.

start_worker(RegName) ->
    {ok, spawn_link(fun() ->
                            register(RegName, self()),
                            receive stop -> ok end
                    end)}.

%% White-box access to the registry's internal state. #state is
%% {state, name, group, ttl_ms, affinity, local_sup, elections}.
election_pid(Reg, Key) ->
    Elections = element(7, sys:get_state(Reg)),
    {ok, {Pid, _Spec}} = maps:find(Key, Elections),
    Pid.

group_of(Reg) ->
    element(3, sys:get_state(Reg)).
