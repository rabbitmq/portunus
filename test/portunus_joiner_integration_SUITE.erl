%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_joiner_integration_SUITE).

%% `portunus_joiner` against a real single-node cluster: the boot race
%% (`ensure_system` failing until the host is ready), the locally stopped
%% server the seed's view alone cannot catch, and `recheck/1` as the
%% host's membership signal.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([joined_waits_for_ensure_system/1,
         stopped_server_is_caught_by_the_backstop/1,
         recheck_runs_a_pass_without_the_backstop/1,
         healthy_member_is_not_churned/1]).

-define(SYS, portunus_joiner_int_sys).

all() ->
    [joined_waits_for_ensure_system,
     stopped_server_is_caught_by_the_backstop,
     recheck_runs_a_pass_without_the_backstop,
     healthy_member_is_not_churned].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    catch meck:unload(portunus),
    ok.

%% The federation boot race: the host's system is not ready when the
%% joiner starts, so `ensure_system` fails and is retried with the join.
joined_waits_for_ensure_system(_Config) ->
    Name = portunus_joiner_int_boot,
    Ready = atomics:new(1, []),
    {ok, Joiner} =
        portunus_joiner:start_link(
          #{system => ?SYS, name => Name,
            candidates => fun() -> [node()] end,
            ensure_system => fun() ->
                                     case atomics:get(Ready, 1) of
                                         0 -> {error, host_not_ready};
                                         _ -> ok
                                     end
                             end}),
    receive
        {portunus_joiner, Name, joined} -> ct:fail(joined_before_ready)
    after 600 -> ok
    end,
    ?assertNot(portunus:is_member(Name)),
    ok = atomics:put(Ready, 1, 1),
    joined = await_notification(Name),
    ?assert(portunus:is_member(Name)),
    cleanup(Joiner, Name).

%% A locally stopped server stays on the seed's member list, so only the
%% `is_member/1` half of the check sees it. The periodic re-check alone
%% (no `recheck/1` call) must restart it through the rejoin.
stopped_server_is_caught_by_the_backstop(_Config) ->
    Name = portunus_joiner_int_backstop,
    {ok, Joiner} = start_joiner(Name, #{recheck_interval_ms => 200}),
    joined = await_notification(Name),
    ok = ra:stop_server(?SYS, {Name, node()}),
    rejoining = await_notification(Name),
    joined = await_notification(Name),
    ?assert(portunus:is_member(Name)),
    cleanup(Joiner, Name).

recheck_runs_a_pass_without_the_backstop(_Config) ->
    Name = portunus_joiner_int_recheck,
    %% An interval far beyond the test's timeout, so only `recheck/1`
    %% can have triggered the pass.
    {ok, Joiner} = start_joiner(Name, #{recheck_interval_ms => 300000}),
    joined = await_notification(Name),
    ok = ra:stop_server(?SYS, {Name, node()}),
    ok = portunus_joiner:recheck(Joiner),
    rejoining = await_notification(Name),
    joined = await_notification(Name),
    ?assert(portunus:is_member(Name)),
    cleanup(Joiner, Name).

%% Passes on a healthy member run the membership check and nothing else:
%% re-running `join_or_form/3` would needlessly restart the local
%% server's supervision path.
healthy_member_is_not_churned(_Config) ->
    Name = portunus_joiner_int_no_churn,
    {ok, Joiner} = start_joiner(Name, #{recheck_interval_ms => 100}),
    joined = await_notification(Name),
    ok = meck:new(portunus, [passthrough, no_link]),
    ok = portunus_joiner:recheck(Joiner),
    timer:sleep(500),
    ?assertEqual(0, meck:num_calls(portunus, join_or_form, '_')),
    ?assert(portunus:is_member(Name)),
    meck:unload(portunus),
    cleanup(Joiner, Name).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

start_joiner(Name, Extra) ->
    portunus_joiner:start_link(
      maps:merge(#{system => ?SYS, name => Name,
                   candidates => fun() -> [node()] end},
                 Extra)).

await_notification(Name) ->
    receive
        {portunus_joiner, Name, Status} -> Status
    after 15000 ->
            ct:fail({no_notification, Name})
    end.

cleanup(Joiner, Name) ->
    unlink(Joiner),
    exit(Joiner, shutdown),
    catch ra:stop_server(?SYS, {Name, node()}),
    catch ra:force_delete_server(?SYS, {Name, node()}),
    ok.
