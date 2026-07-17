%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_registry_sync_integration_SUITE).

%% `portunus_registry:sync/2` on one node: it reconciles the registration
%% set (add the missing, remove the stale, remove-then-add a changed spec),
%% a repeated sync causes no churn, and duplicate or invalid specs are
%% refused before anything changes.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([sync_reconciles_the_set/1,
         sync_is_idempotent/1,
         sync_applies_a_changed_spec/1,
         sync_refuses_duplicates_and_invalid_specs/1]).
-export([start_worker/1]).

-define(SYS, portunus_registry_sync_int_sys).
-define(NAME, portunus_registry_sync_int_test).
-define(TTL, 2000).

all() ->
    [sync_reconciles_the_set,
     sync_is_idempotent,
     sync_applies_a_changed_spec,
     sync_refuses_duplicates_and_invalid_specs].

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

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

sync_reconciles_the_set(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    ok = portunus_registry:sync(Reg, [spec(set_a, rs_w_a), spec(set_b, rs_w_b)]),
    ok = portunus_test_helpers:await_condition(
           fun() -> is_pid(whereis(rs_w_a)) andalso is_pid(whereis(rs_w_b)) end),
    BPid = whereis(rs_w_b),
    ok = portunus_registry:sync(Reg, [spec(set_b, rs_w_b), spec(set_c, rs_w_c)]),
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   whereis(rs_w_a) =:= undefined andalso is_pid(whereis(rs_w_c))
           end),
    ?assertEqual([set_b, set_c], lists:sort(portunus_registry:keys(Reg))),
    %% The unchanged child was never touched.
    ?assertEqual(BPid, whereis(rs_w_b)),
    ok = portunus_registry:stop(Reg).

sync_is_idempotent(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    Set = [spec(idem_a, rs_w_ia), spec(idem_b, rs_w_ib)],
    ok = portunus_registry:sync(Reg, Set),
    ok = portunus_test_helpers:await_condition(
           fun() -> is_pid(whereis(rs_w_ia)) andalso is_pid(whereis(rs_w_ib)) end),
    Pids = {whereis(rs_w_ia), whereis(rs_w_ib)},
    ok = portunus_registry:sync(Reg, Set),
    %% Unchanged specs are untouched synchronously: no election restarted,
    %% no worker replaced, no ownership churn.
    ?assertEqual(Pids, {whereis(rs_w_ia), whereis(rs_w_ib)}),
    ?assertEqual([idem_a, idem_b], lists:sort(portunus_registry:keys(Reg))),
    ok = portunus_registry:stop(Reg).

sync_applies_a_changed_spec(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    ok = portunus_registry:sync(Reg, [spec(ch, rs_w_ch)]),
    ok = portunus_test_helpers:await_condition(
           fun() -> is_pid(whereis(rs_w_ch)) end),
    P1 = whereis(rs_w_ch),
    Changed = (spec(ch, rs_w_ch))#{shutdown => 6000},
    ok = portunus_registry:sync(Reg, [Changed]),
    %% Remove then add: the child restarts under the new spec.
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   case whereis(rs_w_ch) of
                       undefined -> false;
                       P -> P =/= P1
                   end
           end),
    ?assertEqual([ch], portunus_registry:keys(Reg)),
    ok = portunus_registry:stop(Reg).

sync_refuses_duplicates_and_invalid_specs(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    ok = portunus_registry:sync(Reg, [spec(keep, rs_w_keep)]),
    ok = portunus_test_helpers:await_condition(
           fun() -> is_pid(whereis(rs_w_keep)) end),
    Kept = whereis(rs_w_keep),
    ?assertEqual({error, {duplicate_child_id, dup}},
                 portunus_registry:sync(Reg, [spec(dup, rs_w_d1),
                                              spec(dup, rs_w_d2)])),
    ?assertMatch({error, {invalid_child_spec, _}},
                 portunus_registry:sync(Reg, [#{id => broken}])),
    %% Refused before any change: the existing registration is untouched.
    ?assertEqual([keep], portunus_registry:keys(Reg)),
    ?assertEqual(Kept, whereis(rs_w_keep)),
    ok = portunus_registry:stop(Reg).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

spec(Id, RegName) ->
    #{id => Id, start => {?MODULE, start_worker, [RegName]},
      restart => transient, shutdown => 5000, type => worker,
      modules => [?MODULE]}.

start_worker(RegName) ->
    {ok, spawn_link(fun() ->
                            register(RegName, self()),
                            receive stop -> ok end
                    end)}.
