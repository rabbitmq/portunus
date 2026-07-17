%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_orphan_integration_SUITE).

%% Orphaned replica directories on a hosted system: the registration-loss
%% re-formation leaves the previous replica's directory behind, and only
%% `orphaned_replicas/1` can enumerate it (a `reset_server/2` needs the
%% registration the orphan lacks). The data directory is shared with other
%% tenants there, so the enumeration and the deletion must answer only for
%% this node's `portunus` machines.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([orphan_is_listed_and_deletable/1,
         foreign_replicas_are_not_portunus_business/1,
         orphan_api_requires_a_running_system/1]).

-define(SYS, portunus_orphan_sys).
-define(NAME, portunus_orphan_test).

all() ->
    [orphan_is_listed_and_deletable,
     foreign_replicas_are_not_portunus_business,
     orphan_api_requires_a_running_system].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Dir = filename:join([?config(priv_dir, Config), atom_to_list(TC), "ra"]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    [{ra_dir, Dir} | Config].

end_per_testcase(_TC, _Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    catch ra_system:stop(?SYS),
    ok.

dir(Config) ->
    ?config(ra_dir, Config).

form(Config) ->
    ok = portunus_ct_cluster:start_host_system(?SYS, dir(Config)),
    ok = portunus:use_system(?SYS),
    ok = portunus:join_or_form(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

orphan_is_listed_and_deletable(Config) ->
    Dir = dir(Config),
    form(Config),
    OldUId = ra_directory:uid_of(?SYS, ?NAME),
    ?assert(is_binary(OldUId)),
    ?assertEqual({ok, []}, portunus:orphaned_replicas(?SYS)),
    %% The registration-loss re-formation: the old replica's directory
    %% stays, unregistered.
    ok = ra_system:stop(?SYS),
    ok = file:delete(filename:join(Dir, "names.dets")),
    form(Config),
    NewUId = ra_directory:uid_of(?SYS, ?NAME),
    ?assertNotEqual(OldUId, NewUId),
    {ok, Orphans} = portunus:orphaned_replicas(?SYS),
    ?assertMatch([#{name := ?NAME, uid := OldUId, dir := _}], Orphans),
    %% The live replica is never listed and its deletion is refused.
    ?assertEqual({error, registered},
                 portunus:delete_orphaned_replica(?SYS, NewUId)),
    [#{dir := OrphanDir}] = Orphans,
    ?assertEqual(ok, portunus:delete_orphaned_replica(?SYS, OldUId)),
    ?assertNot(filelib:is_dir(OrphanDir)),
    ?assertEqual({ok, []}, portunus:orphaned_replicas(?SYS)),
    ?assertEqual({error, not_found},
                 portunus:delete_orphaned_replica(?SYS, OldUId)),
    %% The live replica survived the cleanup.
    ?assertMatch({ok, _}, portunus:grant_lease(?NAME, 60000)).

%% Another tenant's replica and another node's replica stay out of the
%% enumeration and read as `not_found` to deletion: neither is this node's
%% `portunus` to name, let alone delete.
foreign_replicas_are_not_portunus_business(Config) ->
    Dir = dir(Config),
    form(Config),
    TenantDir = fake_replica(Dir, "foreign_tenant_uid", other_cluster, node(),
                             {module, some_other_machine, #{}}),
    NodeDir = fake_replica(Dir, "other_node_uid", ?NAME, 'other@nowhere',
                           {module, portunus_machine, #{}}),
    ?assertEqual({ok, []}, portunus:orphaned_replicas(?SYS)),
    ?assertEqual({error, not_found},
                 portunus:delete_orphaned_replica(?SYS, <<"foreign_tenant_uid">>)),
    ?assertEqual({error, not_found},
                 portunus:delete_orphaned_replica(?SYS, <<"other_node_uid">>)),
    ?assert(filelib:is_dir(TenantDir)),
    ?assert(filelib:is_dir(NodeDir)).

%% The registration lookup reads the directory DETS table, so both calls
%% need the system running; the error is `use_system/1`'s retryable tag.
orphan_api_requires_a_running_system(_Config) ->
    ?assertEqual({error, {ra_system_not_running, ?SYS}},
                 portunus:orphaned_replicas(?SYS)),
    ?assertEqual({error, {ra_system_not_running, ?SYS}},
                 portunus:delete_orphaned_replica(?SYS, <<"any">>)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% A replica directory as another server would leave it: a `config` file in
%% `ra_log:write_config/2`'s format.
fake_replica(DataDir, UId, Name, Node, Machine) ->
    Sub = filename:join(DataDir, UId),
    ok = filelib:ensure_dir(filename:join(Sub, "x")),
    C = #{id => {Name, Node}, uid => list_to_binary(UId), machine => Machine},
    ok = file:write_file(filename:join(Sub, "config"),
                         io_lib:format("~p.~n", [C])),
    Sub.
