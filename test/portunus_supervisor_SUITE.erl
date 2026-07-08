%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_supervisor_SUITE).

%% Tests `portunus_supervisor`: it starts the children returned by `init/1`,
%% keeps two supervisors that share a child id apart by giving each its own
%% group, and stops its children when the process it returns is killed (it owns
%% them, the caller does not).

-behaviour(portunus_supervisor).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([starts_declared_children/1,
         group_isolates_supervisors/1,
         children_die_with_owner/1,
         transfer_and_which_children_delegate/1]).
-export([init/1, start_worker/1]).

-define(SYS, portunus).
-define(NAME, portunus_supervisor_test).

all() ->
    [starts_declared_children, group_isolates_supervisors, children_die_with_owner,
     transfer_and_which_children_delegate].

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

%% A supervisor links to its caller; trapping exits lets a test kill it and
%% observe the consequence rather than dying with it.
init_per_testcase(_Case, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%% portunus_supervisor callback: one idle worker per id.
init(Ids) ->
    Children = [child(Id) || Id <- Ids],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 10}, Children}}.

starts_declared_children(_Config) ->
    {ok, Sup} = portunus_supervisor:start_link(?NAME, ?MODULE, [sw1, sw2]),
    ok = portunus_test_helpers:await_condition(
           fun() -> running_ids(Sup) =:= [sw1, sw2] end),
    ok = portunus_registry:stop(Sup).

group_isolates_supervisors(_Config) ->
    {ok, SupA} = portunus_supervisor:start_link(?NAME, ?MODULE, [dup], #{group => grp_a}),
    {ok, SupB} = portunus_supervisor:start_link(?NAME, ?MODULE, [dup], #{group => grp_b}),
    ok = portunus_test_helpers:await_condition(
           fun() -> has_running(SupA, dup) andalso has_running(SupB, dup) end),
    ok = portunus_registry:stop(SupA),
    ok = portunus_registry:stop(SupB).

%% Killing the returned process takes its children with it: the local
%% supervisor is owned by it, not leaked to the caller.
children_die_with_owner(_Config) ->
    {ok, Sup} = portunus_supervisor:start_link(?NAME, ?MODULE, [ow1]),
    ok = portunus_test_helpers:await_condition(fun() -> has_running(Sup, ow1) end),
    [{ow1, Pid, _, _}] = portunus_registry:which_children(Sup),
    Ref = monitor(process, Pid),
    exit(Sup, kill),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 5000 ->
        ct:fail(child_outlived_owner)
    end.

%% `transfer/3` and `which_children/1` act on the returned handle. A transfer
%% to a node with no contender is refused and the child stays.
transfer_and_which_children_delegate(_Config) ->
    {ok, Sup} = portunus_supervisor:start_link(?NAME, ?MODULE, [tw1]),
    ok = portunus_test_helpers:await_condition(
           fun() -> running_ids(Sup) =:= [tw1] end),
    [{tw1, P, worker, _}] = portunus_supervisor:which_children(Sup),
    true = is_pid(P),
    {error, {no_contender, 'ghost@nohost'}} =
        portunus_supervisor:transfer(Sup, tw1, 'ghost@nohost'),
    true = has_running(Sup, tw1),
    ok = portunus_registry:stop(Sup).

child(Id) ->
    #{id => Id, start => {?MODULE, start_worker, [Id]},
      restart => transient, shutdown => 5000, type => worker, modules => [?MODULE]}.

start_worker(_Id) ->
    {ok, spawn_link(fun() -> receive stop -> ok end end)}.

running_ids(Sup) ->
    lists:sort([Id || {Id, P, _, _} <- portunus_registry:which_children(Sup), is_pid(P)]).

has_running(Sup, Id) ->
    lists:member(Id, running_ids(Sup)).
