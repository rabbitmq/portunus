%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_keepalive_unit_SUITE).

%% The renewer's failure handling: a lease the machine reports as `lease_expired`
%% is terminal and notifies the holder; a healthy lease keeps being renewed past
%% its TTL.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([expired_lease_notifies_holder/1,
         healthy_lease_is_renewed_past_ttl/1,
         transient_failure_does_not_lose_lease/1]).

-define(SYS, portunus).
-define(NAME, portunus_keepalive_test).
-define(TTL, 3000).

all() ->
    [expired_lease_notifies_holder,
     healthy_lease_is_renewed_past_ttl,
     transient_failure_does_not_lose_lease].

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

%% Revoking the lease out from under the renewer makes the next renewal
%% report `expired`, which is terminal: the holder is told and the renewer
%% stops.
expired_lease_notifies_holder(_Config) ->
    {Holder, Lease} = start_holder(),
    ok = portunus:revoke_lease(?NAME, Lease),
    receive
        {lost, Holder, Lease} -> ok
    after ?TTL + 2000 ->
        ct:fail(no_lease_lost)
    end.

%% A lease that is renewed normally outlives its TTL and the holder is never
%% told it was lost.
healthy_lease_is_renewed_past_ttl(_Config) ->
    {Holder, Lease} = start_holder(),
    timer:sleep(?TTL + 1000),
    ?assertEqual([{Lease, ok}], portunus:renew_leases(?NAME, [Lease])),
    receive
        {lost, Holder, Lease} -> ct:fail(unexpected_lease_lost)
    after 0 -> ok
    end,
    true = exit(Holder, kill).

%% A renewal that only fails transiently (here, the whole server stopped)
%% must not be read as lease loss: the renewer retries, and the lease
%% survives once the server is back, well within its TTL.
transient_failure_does_not_lose_lease(_Config) ->
    Ttl = 10000,
    {Holder, Lease} = start_holder(Ttl),
    ok = ra:stop_server(?SYS, {?NAME, node()}),
    timer:sleep(2500),
    refute_lost(Holder, Lease),
    _ = ra:restart_server(?SYS, {?NAME, node()}),
    ok = portunus_test_helpers:await_leader(?NAME),
    timer:sleep(2000),
    refute_lost(Holder, Lease),
    ?assertEqual([{Lease, ok}], portunus:renew_leases(?NAME, [Lease])),
    true = exit(Holder, kill).

%%----------------------------------------------------------------------
%% Resource owner process and polling helpers
%%----------------------------------------------------------------------

start_holder() ->
    start_holder(?TTL).

%% A holder grants a lease, runs a renewer linked to itself, and forwards a
%% `lease_lost` notification back to the test.
start_holder(Ttl) ->
    Ctrl = self(),
    Holder = spawn(fun() ->
                           {ok, Lease} = portunus:grant_lease(?NAME, Ttl),
                           {ok, _KA} = portunus_keepalive:start_link(?NAME, Lease,
                                                                     Ttl),
                           Ctrl ! {ready, self(), Lease},
                           receive
                               {portunus, lease_lost, Lease} ->
                                   Ctrl ! {lost, self(), Lease}
                           end
                   end),
    receive
        {ready, Holder, Lease} -> {Holder, Lease}
    after 30000 ->
        ct:fail(holder_start_timeout)
    end.

refute_lost(Holder, Lease) ->
    receive
        {lost, Holder, Lease} -> ct:fail(unexpected_lease_lost)
    after 0 -> ok
    end.
