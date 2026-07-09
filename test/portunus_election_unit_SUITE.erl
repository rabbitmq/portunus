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
         lease_loss_steps_down_and_recontends/1,
         transfer_to_self_is_noop/1,
         transfer_to_unready_target_refuses_without_stepdown/1,
         transfer_to_non_owner_is_not_owner/1]).

-define(SYS, portunus).
-define(NAME, portunus_election_test).
-define(TTL, 3000).

all() ->
    [crash_promotes_standby,
     lease_loss_steps_down_and_recontends,
     transfer_to_self_is_noop,
     transfer_to_unready_target_refuses_without_stepdown,
     transfer_to_non_owner_is_not_owner].

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
    ok = await_contender(Key),
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

%% Transferring to this node returns `ok`: no step-down, still the owner.
transfer_to_self_is_noop(_Config) ->
    Key = {election, xfer_self},
    {ok, E} = start(Key),
    receive {elected, Key, _T, E} -> ok after 30000 -> ct:fail(no_leader) end,
    ok = portunus_election:transfer_to(E, node()),
    receive {stepped_down, Key, E} -> ct:fail(unexpected_stepdown) after 500 -> ok end,
    true = portunus_election:is_leader(E),
    true = exit(E, kill).

%% A target that is not a ready contender is refused by the pre-check before
%% any local work stops: no step-down, the healthy owner stays.
transfer_to_unready_target_refuses_without_stepdown(_Config) ->
    Key = {election, xfer_unready},
    {ok, E} = start(Key),
    receive {elected, Key, _T, E} -> ok after 30000 -> ct:fail(no_leader) end,
    Ghost = 'ghost@nohost',
    {error, {no_contender, Ghost}} = portunus_election:transfer_to(E, Ghost),
    receive {stepped_down, Key, E} -> ct:fail(unexpected_stepdown) after 500 -> ok end,
    true = portunus_election:is_leader(E),
    true = exit(E, kill).

%% Only the owner can transfer: a standby returns not_owner.
transfer_to_non_owner_is_not_owner(_Config) ->
    Key = {election, xfer_follower},
    {ok, E1} = start(Key),
    receive {elected, Key, _T, E1} -> ok after 30000 -> ct:fail(no_leader) end,
    {ok, E2} = start(Key),
    ok = await_contender(Key),
    false = portunus_election:is_leader(E2),
    {error, not_owner} = portunus_election:transfer_to(E2, node()),
    true = exit(E1, kill),
    true = exit(E2, kill).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

start(Key) ->
    start(Key, ?TTL).

start(Key, Ttl) ->
    portunus_election:start_link(?NAME, Key, portunus_demo_election,
                                 self(), #{ttl_ms => Ttl}).

%% E2 is a ready contender once its owner term appears in the succession
%% queue; a fixed sleep can miss that under load and silently test the
%% free-key acquire path instead.
await_contender(Key) ->
    portunus_test_helpers:await_condition(
      fun() ->
              case portunus:contenders(?NAME, Key) of
                  {ok, Owners} -> lists:member(node(), Owners);
                  _ -> false
              end
      end).

%% The election's links are the test process and its keepalive; the latter is
%% the one to kill to simulate lease loss.
keepalive_of(E) ->
    {links, Links} = process_info(E, links),
    case Links -- [self()] of
        [KA] -> KA;
        Other -> ct:fail({unexpected_links, Other})
    end.
