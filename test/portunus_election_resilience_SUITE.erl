%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_election_resilience_SUITE).

%% A transient no_quorum makes the election re-contend (not exit), a lost lease
%% is revoked before re-contending, and the reconcile backstop recovers a dropped
%% grant. portunus is mocked, so no cluster is needed.

-behaviour(portunus_election).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([transient_no_quorum_does_not_stop_election/1,
         lease_loss_revokes_orphan_lease/1,
         reconcile_recovers_a_dropped_grant/1]).
-export([elected/1, stepped_down/1]).

-define(NAME, portunus_resilience_test).
-define(LEASE, resil_lease).

all() ->
    [transient_no_quorum_does_not_stop_election,
     lease_loss_revokes_orphan_lease,
     reconcile_recovers_a_dropped_grant].

init_per_testcase(_TC, Config) ->
    process_flag(trap_exit, true),
    KA = spawn(fun idle/0),
    ok = meck:new(portunus, [passthrough, no_link]),
    ok = meck:new(portunus_keepalive, [passthrough, no_link]),
    meck:expect(portunus, grant_lease, fun(_N, _T) -> {ok, ?LEASE} end),
    meck:expect(portunus, revoke_lease, fun(_N, _L) -> ok end),
    meck:expect(portunus_keepalive, start_link, fun(_N, _L, _T) -> {ok, KA} end),
    meck:expect(portunus_keepalive, stop, fun(_P) -> ok end),
    meck:expect(portunus, acquire_or_join_succession_queue,
                fun(_N, _K, _L, _O, _Opts) -> {error, no_quorum} end),
    [{keepalive, KA} | Config].

end_per_testcase(_TC, Config) ->
    catch meck:unload(portunus_keepalive),
    catch meck:unload(portunus),
    catch exit(?config(keepalive, Config), kill),
    ok.

transient_no_quorum_does_not_stop_election(_Config) ->
    {ok, E} = portunus_election:start_link(?NAME, k, ?MODULE, no_args,
                                           #{ttl_ms => 2000}),
    %% acquire keeps failing transiently, so the election re-contends on a
    %% backoff and must stay alive without ever becoming leader.
    timer:sleep(1500),
    ?assert(is_process_alive(E)),
    ?assertNot(portunus_election:is_leader(E)),
    ?assert(meck:num_calls(portunus, acquire_or_join_succession_queue, '_') > 1),
    %% Once acquire succeeds, the next contend makes it leader.
    meck:expect(portunus, acquire_or_join_succession_queue,
                fun(_N, _K, _L, _O, _Opts) -> {ok, 42} end),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus_election:is_leader(E) end),
    portunus_election:stop(E).

%% On lease loss the election revokes its now-orphaned lease before
%% re-contending, so the re-grant does not queue behind its own still-held
%% lock until the lease expires.
lease_loss_revokes_orphan_lease(_Config) ->
    meck:expect(portunus, acquire_or_join_succession_queue,
                fun(_N, _K, _L, _O, _Opts) -> {ok, 7} end),
    {ok, E} = portunus_election:start_link(?NAME, k, ?MODULE, no_args,
                                           #{ttl_ms => 2000}),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus_election:is_leader(E) end),
    E ! {portunus, lease_lost, ?LEASE},
    ok = portunus_test_helpers:await_condition(
           fun() -> meck:called(portunus, revoke_lease, [?NAME, ?LEASE]) end),
    portunus_election:stop(E).

%% The grant message is dropped, but the reconcile backstop reads ownership
%% and promotes us.
reconcile_recovers_a_dropped_grant(_Config) ->
    meck:expect(portunus, acquire_or_join_succession_queue,
                fun(_N, _K, _L, _O, _Opts) -> {queued, 1} end),
    meck:expect(portunus, owner,
                fun(_N, _K) -> {ok, #{lease => ?LEASE, token => 99}} end),
    {ok, E} = portunus_election:start_link(?NAME, k, ?MODULE, no_args,
                                           #{ttl_ms => 2000}),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus_election:is_leader(E) end),
    portunus_election:stop(E).

elected(#{token := Token}) -> {ok, Token}.
stepped_down(_State) -> ok.

idle() -> receive _ -> idle() end.
