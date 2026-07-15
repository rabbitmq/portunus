%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_lifecycle_SUITE).

%% Cluster-lifecycle calls reached only indirectly elsewhere: `join_cluster/3`
%% idempotency, `reset_server/2` refusing to wipe a live member, and
%% `ensure_started/1` forming a local cluster from an env map.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([join_cluster_is_idempotent_for_a_member/1,
         reset_server_refuses_a_live_member/1,
         reset_server_refuses_when_membership_unknown/1,
         ensure_started_forms_a_local_cluster/1]).

-define(SYS, portunus_lifecycle_sys).
-define(ENSURE_SYS, portunus_lifecycle_ensure_sys).
-define(NAME, portunus_lifecycle_test).

all() ->
    [join_cluster_is_idempotent_for_a_member,
     reset_server_refuses_a_live_member,
     reset_server_refuses_when_membership_unknown,
     ensure_started_forms_a_local_cluster].

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
    catch ra:stop_server(?ENSURE_SYS, {portunus_lifecycle_ensure, node()}),
    catch ra:stop_server(?SYS, {portunus_lifecycle_reset, node()}),
    catch ra_system:stop(?ENSURE_SYS),
    ok.

join_cluster_is_idempotent_for_a_member(_Config) ->
    ?assertEqual(ok, portunus:join_cluster(?SYS, ?NAME, node())).

reset_server_refuses_a_live_member(_Config) ->
    ?assertEqual({error, still_member},
                 portunus:reset_server(?SYS, {?NAME, node()})).

%% A failed membership query is not proof of non-membership, so reset_server
%% refuses to wipe rather than risk deleting a live member's data during a
%% transient partition. The data survives, proven by the server restarting.
reset_server_refuses_when_membership_unknown(_Config) ->
    Name = portunus_lifecycle_reset,
    {ok, _, _} = portunus:start_cluster(?SYS, Name, [node()]),
    ok = portunus_test_helpers:await_leader(Name),
    ok = ra:stop_server(?SYS, {Name, node()}),
    ?assertEqual({error, no_quorum},
                 portunus:reset_server(?SYS, {Name, node()})),
    ok = portunus:restart_server(?SYS, Name),
    ok = portunus_test_helpers:await_leader(Name).

%% Its own system, since it asks for its own directory: naming `?SYS`, which
%% `init_per_suite` already started elsewhere, is asking for a directory it would
%% not get.
ensure_started_forms_a_local_cluster(Config) ->
    Name = portunus_lifecycle_ensure,
    DataDir = filename:join(?config(priv_dir, Config), "ensure"),
    {ok, [_ | _], _} = portunus:ensure_started(#{ra_system => ?ENSURE_SYS,
                                                 name => Name,
                                                 data_dir => DataDir,
                                                 membership => local}),
    ok = portunus_test_helpers:await_leader(Name),
    ?assert(portunus:has_quorum(Name)).
