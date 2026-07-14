%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_recovery_multinode_SUITE).

%% A restarted member rejoins through any live quorum, not a fixed seed: each
%% case keeps the lowest-sorted node (the bootstrap seed) down during recovery.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([recovers_with_seed_down/1,
         recovers_with_non_seed_down/1,
         registered_recovery_rejoins_after_system_restart/1]).

-define(NAME, portunus_recovery_test).

all() ->
    [recovers_with_seed_down,
     recovers_with_non_seed_down,
     registered_recovery_rejoins_after_system_restart].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    [{cluster, portunus_ct_cluster:start(Config, ?NAME, 3)} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

recovers_with_seed_down(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Seed = hd(lists:sort(Nodes)),
    Key = {res, seed_down},
    [Survivor | _] = Nodes -- [Seed],
    Other = hd((Nodes -- [Seed]) -- [Survivor]),
    Token = place_lock(Survivor, Key),
    stop_two(Survivor, [Seed, Other]),
    ok = portunus_ct_cluster:restart_ra_server(Other, ?NAME),
    {?NAME, _} = portunus_ct_cluster:wait_leader([Survivor, Other], ?NAME),
    portunus_ct_cluster:await_owner(Survivor, ?NAME, Key, owner_a, Token).

recovers_with_non_seed_down(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Seed = hd(lists:sort(Nodes)),
    Key = {res, non_seed_down},
    [Survivor, Down] = Nodes -- [Seed],
    Token = place_lock(Survivor, Key),
    stop_two(Survivor, [Seed, Down]),
    ok = portunus_ct_cluster:restart_ra_server(Seed, ?NAME),
    {?NAME, _} = portunus_ct_cluster:wait_leader([Survivor, Seed], ?NAME),
    portunus_ct_cluster:await_owner(Survivor, ?NAME, Key, owner_a, Token).

registered_recovery_rejoins_after_system_restart(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Seed = hd(lists:sort(Nodes)),
    Key = {res, sys_restart},
    [Survivor | _] = Nodes -- [Seed],
    Token = place_lock(Survivor, Key),
    ok = portunus_ct_cluster:restart_ra_system(Config, Seed),
    ok = portunus_ct_cluster:wait_until(
           fun() -> rpc:call(Seed, portunus, is_member, [?NAME]) =:= true end),
    {?NAME, _} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    portunus_ct_cluster:await_owner(Seed, ?NAME, Key, owner_a, Token).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% A 60s TTL so the lease outlives the no-quorum window.
place_lock(Node, Key) ->
    Client = portunus_ct_cluster:start_client(Node),
    {ok, Lease} = portunus_ct_cluster:until_quorum(Client, grant_lease, [?NAME, 60000]),
    {ok, Token} = portunus_ct_cluster:until_quorum(
                    Client, acquire, [?NAME, Key, Lease, owner_a]),
    portunus_ct_cluster:await_owner(Node, ?NAME, Key, owner_a),
    Token.

stop_two(Survivor, [A, B]) ->
    ok = portunus_ct_cluster:stop_ra_server(A, ?NAME),
    ok = portunus_ct_cluster:stop_ra_server(B, ?NAME),
    Client = portunus_ct_cluster:start_client(Survivor),
    ?assertEqual({error, no_quorum},
                 portunus_ct_cluster:ccall(Client, grant_lease, [?NAME, 60000])).
