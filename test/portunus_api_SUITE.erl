%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_api_SUITE).

%% The finalized public API: `acquire/4` tries once and never
%% queues, `acquire_or_join_succession_queue` queues, `owner/2` reports a
%% `remaining_ms`, a missing renew is `lease_expired`, and a watch ref
%% round-trips.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([acquire_tries_once/1,
         join_queue_waits/1,
         owner_reports_remaining_ttl/1,
         context_round_trips_through_owner/1,
         renew_missing_is_lease_expired/1,
         watch_ref_round_trips/1,
         is_member_reports_membership/1]).

-define(SYS, portunus).
-define(NAME, portunus_api_test).
-define(TTL, 60000).

all() ->
    [acquire_tries_once, join_queue_waits, owner_reports_remaining_ttl,
     context_round_trips_through_owner, renew_missing_is_lease_expired,
     watch_ref_round_trips, is_member_reports_membership].

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

acquire_tries_once(_Config) ->
    K = {res, once},
    {ok, L1} = portunus:grant_lease(?NAME, ?TTL),
    {ok, L2} = portunus:grant_lease(?NAME, ?TTL),
    {ok, _} = portunus:acquire(?NAME, K, L1, owner_a),
    ?assertEqual({error, {held_by, owner_a}},
                 portunus:acquire(?NAME, K, L2, owner_b)),
    ok = portunus:revoke_lease(?NAME, L1),
    ok = portunus:revoke_lease(?NAME, L2).

join_queue_waits(_Config) ->
    K = {res, wait},
    {ok, L1} = portunus:grant_lease(?NAME, ?TTL),
    {ok, L2} = portunus:grant_lease(?NAME, ?TTL),
    {ok, _} = portunus:acquire(?NAME, K, L1, owner_a),
    ?assertEqual({queued, 1},
                 portunus:acquire_or_join_succession_queue(?NAME, K, L2, owner_b)),
    ok = portunus:revoke_lease(?NAME, L1),
    ok = portunus:revoke_lease(?NAME, L2).

is_member_reports_membership(_Config) ->
    ?assert(portunus:is_member(?NAME)),
    %% No local replica exists for a cluster that was never started here.
    ?assertNot(portunus:is_member(portunus_api_no_such_cluster)).

owner_reports_remaining_ttl(_Config) ->
    K = {res, ttl},
    {ok, L} = portunus:grant_lease(?NAME, ?TTL),
    {ok, _} = portunus:acquire(?NAME, K, L, owner_a),
    {ok, Info} = portunus:owner(?NAME, K),
    Remaining = maps:get(remaining_ms, Info),
    ?assert(is_integer(Remaining) andalso Remaining > 0 andalso Remaining =< ?TTL),
    ok = portunus:revoke_lease(?NAME, L).

context_round_trips_through_owner(_Config) ->
    K = {res, ctx},
    Ctx = #{shovel => sh1},
    {ok, L} = portunus:grant_lease(?NAME, ?TTL),
    {ok, _} = portunus:acquire(?NAME, K, L, owner_a, Ctx),
    {ok, Info} = portunus:owner(?NAME, K),
    ?assertEqual(Ctx, maps:get(context, Info)),
    ok = portunus:revoke_lease(?NAME, L).

renew_missing_is_lease_expired(_Config) ->
    ?assertEqual([{no_such_lease, {error, lease_expired}}],
                 portunus:renew_leases(?NAME, [no_such_lease])).

watch_ref_round_trips(_Config) ->
    {ok, Ref} = portunus:watch(?NAME, {res, watch}),
    ?assert(is_integer(Ref)),
    ?assertEqual(ok, portunus:unwatch(?NAME, Ref)).
