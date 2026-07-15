%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_lease_renewal_SUITE).

%% Automatic lease renewal: a lease granted with `auto_renew`,
%% or kept alive with `keep_alive/3`, stays held past its TTL via a
%% holder-linked renewer, and frees when the holder dies or revokes.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([auto_renew_outlives_ttl/1,
         keep_alive_outlives_ttl/1,
         stopping_renewer_lets_lease_expire/1,
         explicit_revoke_ends_auto_renew/1,
         auto_renew_honours_proposed_id/1]).

-define(SYS, portunus_lease_renewal_sys).
-define(NAME, portunus_renewal_test).
%% A TTL short enough for a test to wait past quickly, yet wide enough that
%% the renewer (every TTL/3, floored at 1s) keeps a comfortable margin.
-define(TTL, 3000).

all() ->
    [auto_renew_outlives_ttl,
     keep_alive_outlives_ttl,
     stopping_renewer_lets_lease_expire,
     explicit_revoke_ends_auto_renew,
     auto_renew_honours_proposed_id].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = wait_leader(?NAME, 100),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

auto_renew_outlives_ttl(_Config) ->
    Key = {res, auto_renew},
    {Holder, _Lease} = start_holder(auto_renew, Key),
    %% Well past the TTL: without renewal the lease would have expired.
    timer:sleep(?TTL + 1000),
    {ok, #{owner := holder}} = portunus:owner(?NAME, Key),
    %% Death stops the renewer (link) and frees the lock (monitor).
    true = exit(Holder, kill),
    ok = wait_until(fun() -> portunus:owner(?NAME, Key) =:= {error, not_held} end).

keep_alive_outlives_ttl(_Config) ->
    Key = {res, keep_alive},
    {Holder, _Lease} = start_holder(keep_alive, Key),
    timer:sleep(?TTL + 1000),
    {ok, #{owner := holder}} = portunus:owner(?NAME, Key),
    true = exit(Holder, kill),
    ok = wait_until(fun() -> portunus:owner(?NAME, Key) =:= {error, not_held} end).

stopping_renewer_lets_lease_expire(_Config) ->
    Key = {res, stop_renewer},
    Ctrl = self(),
    Holder = spawn(fun() ->
                           {ok, Lease} = portunus:grant_lease(?NAME, ?TTL),
                           {ok, Renewer} = portunus:keep_alive(?NAME, Lease, ?TTL),
                           {ok, _} = portunus:acquire(?NAME, Key, Lease, holder),
                           Ctrl ! {held, self(), Renewer},
                           receive stop -> ok end
                   end),
    Renewer = receive {held, Holder, R} -> R after 30000 -> ct:fail(timeout) end,
    %% Stop only the renewer; the holder lives on, but with nothing renewing
    %% it the lease expires and the lock is reclaimed.
    ok = portunus_keepalive:stop(Renewer),
    ok = wait_until(fun() -> portunus:owner(?NAME, Key) =:= {error, not_held} end),
    true = exit(Holder, kill).

explicit_revoke_ends_auto_renew(_Config) ->
    Key = {res, revoke_auto},
    {Holder, Lease} = start_holder(auto_renew, Key),
    ok = portunus:revoke_lease(?NAME, Lease),
    ok = wait_until(fun() -> portunus:owner(?NAME, Key) =:= {error, not_held} end),
    %% Past a renew interval: the renewer cannot bring the lock back.
    timer:sleep(1500),
    {error, not_held} = portunus:owner(?NAME, Key),
    true = exit(Holder, kill).

auto_renew_honours_proposed_id(_Config) ->
    Id = {stable, auto, lease},
    Ctrl = self(),
    %% A spawned holder, so the auto-renew renewer is linked to it and dies
    %% when it is killed, rather than outliving this test case.
    Holder = spawn(fun() ->
                           R = portunus:grant_lease(?NAME, ?TTL,
                                   #{proposed_id => Id, auto_renew => true}),
                           Ctrl ! {granted, self(), R},
                           receive stop -> ok end
                   end),
    receive
        {granted, Holder, R} -> ?assertEqual({ok, Id}, R)
    after 30000 -> ct:fail(grant_timeout)
    end,
    true = exit(Holder, kill).

%%----------------------------------------------------------------------
%% Holder process and polling helpers
%%----------------------------------------------------------------------

start_holder(Mode, Key) ->
    Ctrl = self(),
    Holder = spawn(fun() -> hold(Mode, Key, Ctrl) end),
    receive
        {held, Holder, Lease} -> {Holder, Lease}
    after 30000 ->
        ct:fail(holder_start_timeout)
    end.

hold(Mode, Key, Ctrl) ->
    {ok, Lease} = grant(Mode),
    {ok, _Token} = portunus:acquire(?NAME, Key, Lease, holder),
    Ctrl ! {held, self(), Lease},
    receive stop -> ok end.

grant(auto_renew) ->
    portunus:grant_lease(?NAME, ?TTL, #{auto_renew => true});
grant(keep_alive) ->
    {ok, Lease} = portunus:grant_lease(?NAME, ?TTL),
    {ok, _Renewer} = portunus:keep_alive(?NAME, Lease, ?TTL),
    {ok, Lease}.

%% Thin wrappers over the shared helpers, so a timeout fails with a clear
%% message instead of a badmatch on `{error, timeout}`.
wait_leader(Name, N) ->
    portunus_test_helpers:await_leader(Name, N * 50).

wait_until(Fun) ->
    portunus_test_helpers:await_condition(Fun, 10000).
