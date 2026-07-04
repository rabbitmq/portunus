%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_election_unit_SUITE).

%% Ownership transfer on a crash: when the leader crashes, the standby that
%% queued behind it is promoted and runs `elected/1`. The crash path (not a clean stop)
%% exercises the machine's monitor-driven release feeding the succession
%% queue, which the reconcile backstop also guards.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2]).
-export([crash_promotes_standby/1,
         lease_loss_steps_down_and_recontends/1]).

-define(SYS, portunus).
-define(NAME, portunus_election_test).
-define(TTL, 3000).

all() ->
    [crash_promotes_standby,
     lease_loss_steps_down_and_recontends].

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

%% The elections are linked to the test process, so it must trap exits to
%% survive killing them.
init_per_testcase(_Case, Config) ->
    process_flag(trap_exit, true),
    Config.

crash_promotes_standby(_Config) ->
    Key = {election, crash},
    {ok, E1} = start(Key),
    T1 = receive {elected, Key, Tok, E1} -> Tok after 30000 -> ct:fail(no_leader) end,
    {ok, E2} = start(Key),
    %% Let E2 establish its lease and join the queue behind E1.
    timer:sleep(1000),
    true = exit(E1, kill),
    receive
        {elected, Key, T2, E2} -> ?assert(T2 > T1)
    after 30000 ->
        ct:fail(no_promotion)
    end,
    true = exit(E2, kill).

%% Killing the renewer loses the lease: the leader steps down, re-contends,
%% and (alone here) wins again once its orphaned lease expires.
lease_loss_steps_down_and_recontends(_Config) ->
    Key = {election, lease_loss},
    Ttl = 2000,
    {ok, E} = start(Key, Ttl),
    receive {elected, Key, _T1, E} -> ok after 30000 -> ct:fail(no_leader) end,
    true = exit(keepalive_of(E), kill),
    receive {stepped_down, Key, E} -> ok after 30000 -> ct:fail(no_stepdown) end,
    receive
        {elected, Key, _T2, E} -> ok
    after Ttl + 10000 ->
        ct:fail(no_recontend)
    end,
    true = exit(E, kill).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

start(Key) ->
    start(Key, ?TTL).

start(Key, Ttl) ->
    portunus_election:start_link(?NAME, Key, portunus_demo_election,
                                 self(), #{ttl_ms => Ttl}).

%% The election's links are the test process and its keepalive; the latter is
%% the one to kill to simulate lease loss.
keepalive_of(E) ->
    {links, Links} = process_info(E, links),
    case Links -- [self()] of
        [KA] -> KA;
        Other -> ct:fail({unexpected_links, Other})
    end.
