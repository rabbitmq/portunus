%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_bootstrap_SUITE).

%% The `join_or_form/3` bootstrap helper and the name-collision guard on
%% `start_cluster/3` and `join_cluster/3`. Ra registers a server locally under
%% the cluster name, so a collision with another registered process is reported
%% as `{error, {name_registered, Pid}}` rather than left to surface as an opaque
%% `cluster_not_formed`. `join_or_form/3` recovers an on-disk replica if there is
%% one, else the lowest node forms a single-node cluster and the rest join.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([start_cluster_rejects_foreign_name/1,
         join_cluster_rejects_foreign_name/1,
         guard_allows_own_running_server/1,
         join_or_form_seed_forms_single_node/1,
         join_or_form_is_idempotent/1,
         join_or_form_recovers_after_ra_restart/1]).

-define(SYS, portunus_bootstrap_sys).
-define(NAME, portunus_bootstrap_test).
-define(TTL, 60000).

all() ->
    [start_cluster_rejects_foreign_name,
     join_cluster_rejects_foreign_name,
     guard_allows_own_running_server,
     join_or_form_seed_forms_single_node,
     join_or_form_is_idempotent,
     join_or_form_recovers_after_ra_restart].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Dir = filename:join([?config(priv_dir, Config), atom_to_list(TC), "ra"]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ok = portunus:start_system(?SYS, Dir),
    [{ra_dir, Dir} | Config].

end_per_testcase(_TC, _Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    catch ra_system:stop(?SYS),
    ok.

%% A foreign process registered under the cluster name makes `start_cluster/3`
%% fail with a clear `{name_registered, Pid}` rather than `cluster_not_formed`.
start_cluster_rejects_foreign_name(_Config) ->
    Pid = spawn_registered(?NAME),
    try
        ?assertEqual({error, {name_registered, Pid}},
                     portunus:start_cluster(?SYS, ?NAME, [node()]))
    after
        stop_registered(Pid)
    end.

%% The same guard protects `join_cluster/3`, before it reaches the seed.
join_cluster_rejects_foreign_name(_Config) ->
    Pid = spawn_registered(?NAME),
    try
        ?assertEqual({error, {name_registered, Pid}},
                     portunus:join_cluster(?SYS, ?NAME, node()))
    after
        stop_registered(Pid)
    end.

%% The guard must not trip on the cluster's own running Ra server, so an
%% idempotent re-`start_cluster/3` is never misreported as a name collision.
guard_allows_own_running_server(_Config) ->
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertNotMatch({error, {name_registered, _}},
                    portunus:start_cluster(?SYS, ?NAME, [node()])).

%% The lowest node in the membership forms a single-node cluster of itself.
join_or_form_seed_forms_single_node(_Config) ->
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(?NAME) end),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertMatch({ok, _}, portunus:grant_lease(?NAME, ?TTL)).

%% Repeated calls on a healthy member stay `ok` and never disturb membership.
join_or_form_is_idempotent(_Config) ->
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ?assert(portunus:is_member(?NAME)).

%% After a `ra` restart, `join_or_form/3` recovers this node's on-disk replica
%% rather than forming afresh, and a lock it held survives with its token.
join_or_form_recovers_after_ra_restart(Config) ->
    Dir = ?config(ra_dir, Config),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, L} = portunus:grant_lease(?NAME, ?TTL),
    {ok, T} = portunus:acquire(?NAME, {res, survive}, L, owner_a),
    ok = application:stop(ra),
    {ok, _} = application:ensure_all_started(ra),
    ok = portunus:start_system(?SYS, Dir),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(?NAME) end),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertMatch({ok, #{owner := owner_a, token := T}},
                 portunus:owner(?NAME, {res, survive})).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

spawn_registered(Name) ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    true = register(Name, Pid),
    Pid.

stop_registered(Pid) ->
    catch unregister(?NAME),
    Pid ! stop,
    ok.
