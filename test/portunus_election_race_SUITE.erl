%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_election_race_SUITE).

%% The election is a gen_server, so it processes one message at a time: its
%% "races" are just the order in which `granted`, `lease_lost`, and `'EXIT'` arrive. With the
%% Ra boundary mocked, the real election is driven through each ordering and
%% must keep `elected/1` and `stepped_down/1` correctly paired and never act on a lease it
%% has abandoned. No Concuerror: a single gen_server has no internal
%% concurrency to explore, and the orderings that matter are few enough to list.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([granted_elects/1,
         lease_lost_while_queued_recontends_quietly/1,
         leader_then_lease_lost_steps_down/1,
         stale_granted_is_ignored/1,
         keepalive_exit_recontends_quietly/1]).

-define(NAME, portunus_election_race_test).
-define(KEY, {election, race}).
-define(TTL, 60000).
-define(LEASE, lease_a).

all() ->
    [granted_elects,
     lease_lost_while_queued_recontends_quietly,
     leader_then_lease_lost_steps_down,
     stale_granted_is_ignored,
     keepalive_exit_recontends_quietly].

init_per_testcase(_TC, Config) ->
    process_flag(trap_exit, true),
    %% A stand-in for the keepalive pid the election holds; never linked, so
    %% the test injects its `EXIT` by hand to drive that branch.
    KA = spawn(fun idle/0),
    ok = meck:new(portunus, [passthrough, no_link]),
    ok = meck:new(portunus_keepalive, [passthrough, no_link]),
    %% acquire always queues, so the election only ever becomes leader through
    %% an injected `granted` whose ordering the test controls.
    meck:expect(portunus, grant_lease, fun(_Name, _Ttl) -> {ok, ?LEASE} end),
    meck:expect(portunus, acquire_or_join_succession_queue,
                fun(_N, _K, _L, _O, _Opts) -> {queued, 1} end),
    meck:expect(portunus, owner, fun(_N, _K) -> {error, not_held} end),
    meck:expect(portunus, revoke_lease, fun(_N, _L) -> ok end),
    meck:expect(portunus_keepalive, start_link, fun(_N, _L, _T) -> {ok, KA} end),
    meck:expect(portunus_keepalive, stop, fun(_P) -> ok end),
    [{keepalive, KA} | Config].

end_per_testcase(_TC, Config) ->
    catch meck:unload(portunus_keepalive),
    catch meck:unload(portunus),
    case ?config(keepalive, Config) of
        KA when is_pid(KA) -> exit(KA, kill);
        _ -> ok
    end,
    ok.

%% A granted for our lease promotes us.
granted_elects(_Config) ->
    E = start_queued(),
    E ! {portunus, granted, ?KEY, 42, ?LEASE},
    expect_elected(42),
    stop(E).

%% Losing the lease while merely queued re-contends without electing or
%% stepping down.
lease_lost_while_queued_recontends_quietly(_Config) ->
    E = start_queued(),
    E ! {portunus, lease_lost, ?LEASE},
    refute_callback(),
    ?assertNot(portunus_election:is_leader(E)),
    stop(E).

%% A leader that loses its lease steps down.
leader_then_lease_lost_steps_down(_Config) ->
    E = start_queued(),
    E ! {portunus, granted, ?KEY, 42, ?LEASE},
    expect_elected(42),
    E ! {portunus, lease_lost, ?LEASE},
    expect_stepped_down(),
    stop(E).

%% A granted minted for an earlier, abandoned lease is ignored, so a revoked
%% token never installs us as leader.
stale_granted_is_ignored(_Config) ->
    E = start_queued(),
    E ! {portunus, granted, ?KEY, 99, some_other_lease},
    refute_callback(),
    ?assertNot(portunus_election:is_leader(E)),
    stop(E).

%% The renewer dying is a lease loss; while merely queued it re-contends
%% quietly.
keepalive_exit_recontends_quietly(Config) ->
    KA = ?config(keepalive, Config),
    E = start_queued(),
    E ! {'EXIT', KA, killed},
    refute_callback(),
    ?assertNot(portunus_election:is_leader(E)),
    stop(E).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% Start the election and wait until it has contended and queued. The
%% `is_leader` call is processed after the `contend` the election posts itself
%% in init, so a `false` reply means contention is complete.
start_queued() ->
    {ok, E} = portunus_election:start_link(?NAME, ?KEY,
                                           portunus_demo_election, self(),
                                           #{ttl_ms => ?TTL}),
    ?assertNot(portunus_election:is_leader(E)),
    E.

expect_elected(Token) ->
    receive
        {elected, ?KEY, Token, _Pid} -> ok
    after 5000 -> ct:fail(not_elected)
    end.

expect_stepped_down() ->
    receive
        {stepped_down, ?KEY, _Pid} -> ok
    after 5000 -> ct:fail(not_stepped_down)
    end.

refute_callback() ->
    receive
        {elected, _, _, _} -> ct:fail(unexpected_elected);
        {stepped_down, _, _} -> ct:fail(unexpected_stepped_down)
    after 300 -> ok
    end.

stop(E) ->
    _ = catch portunus_election:stop(E),
    ok.

idle() ->
    receive _ -> idle() end.
