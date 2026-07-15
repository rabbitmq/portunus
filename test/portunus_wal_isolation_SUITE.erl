%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_wal_isolation_SUITE).

%% `ra_system:default_config/0` sets `data_dir` and `wal_data_dir`, so overriding
%% only the first leaves the WAL in `ra_env:data_dir/0`: under a host such as
%% RabbitMQ, another Ra system's directory. Ra recovers every `*.wal` in a
%% directory whoever wrote it, hands the entries to its own segment writer, finds
%% the UIDs unregistered, drops them and deletes the file. These tests point
%% `ra.data_dir` at a foreign directory and run a second Ra system there, which
%% is the whole experiment: misplacement alone is survivable, contention is what
%% destroys.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([wal_dir_is_the_system_data_dir/1,
         wal_files_stay_in_the_system_data_dir/1,
         restart_recovers_the_log_with_a_foreign_ra_data_dir/1,
         foreign_system_does_not_delete_our_wal/1,
         reusing_a_matching_system_is_idempotent/1,
         reusing_a_foreign_system_errors/1,
         reusing_a_foreign_system_does_not_rewrite_its_config/1,
         an_ra_restart_still_recovers/1]).

-export([start_system_named/3]).

-define(SYS, portunus_wal_isolation_sys).
-define(PEER_NAME, portunus_wal_isolation_peer).
-define(FOREIGN_SYS, portunus_wal_isolation_foreign_sys).
-define(NAME, portunus_wal_isolation_test).
-define(TTL, 60000).

all() ->
    [wal_dir_is_the_system_data_dir,
     wal_files_stay_in_the_system_data_dir,
     restart_recovers_the_log_with_a_foreign_ra_data_dir,
     foreign_system_does_not_delete_our_wal,
     reusing_a_matching_system_is_idempotent,
     reusing_a_foreign_system_errors,
     reusing_a_foreign_system_does_not_rewrite_its_config,
     an_ra_restart_still_recovers].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Base = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    Ours = filename:join(Base, "portunus"),
    Foreign = filename:join(Base, "foreign"),
    ok = filelib:ensure_dir(filename:join(Ours, "x")),
    ok = filelib:ensure_dir(filename:join(Foreign, "x")),
    %% What a host such as RabbitMQ sets for its own Ra system, and what
    %% `default_config/0` would hand portunus as `wal_data_dir`.
    Prev = application:get_env(ra, data_dir),
    ok = application:set_env(ra, data_dir, Foreign),
    [{ours, Ours}, {foreign_base, Foreign}, {prev_ra_data_dir, Prev} | Config].

end_per_testcase(_TC, Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    catch ra_system:stop(?SYS),
    catch ra_system:stop(?FOREIGN_SYS),
    case ?config(prev_ra_data_dir, Config) of
        {ok, Dir} -> application:set_env(ra, data_dir, Dir);
        _ -> application:unset_env(ra, data_dir)
    end,
    ok.

ours(Config) -> ?config(ours, Config).

%% Where an unfixed portunus would put its WAL, and so where the other system
%% has to be for the two to contend: `ra_env:data_dir/0` appends the node name to
%% `ra.data_dir`, exactly as a host such as RabbitMQ leaves it.
foreign(_Config) -> ra_env:data_dir().

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

%% The direct regression guard: both path keys must name the given directory,
%% not just `data_dir`.
wal_dir_is_the_system_data_dir(Config) ->
    Dir = ours(Config),
    ok = portunus:start_system(?SYS, Dir),
    #{data_dir := DataDir, wal_data_dir := WalDir} = ra_system:fetch(?SYS),
    ?assertEqual(absdir(Dir), absdir(DataDir)),
    ?assertEqual(absdir(Dir), absdir(WalDir)).

%% The entries must land where the system will read them back.
wal_files_stay_in_the_system_data_dir(Config) ->
    Dir = ours(Config),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, _} = portunus:grant_lease(?NAME, ?TTL),
    ?assertNotEqual([], wals(Dir)),
    ?assertEqual([], wals(foreign(Config))).

%% The reported failure's shape end to end, on a node that dies rather than
%% stops: the entries are still in the WAL, the foreign system recovers the
%% shared directory first and deletes them as an unregistered UID, and the
%% replica comes back with an empty log while its metadata carries the term.
%%
%% The node must be halted, not stopped. Any in-VM stop, clean or killed, ends
%% with this system's own segment writer flushing the mem tables to `data_dir`,
%% which is ours either way, so the entries would survive the deletion and the
%% case would pass with the bug present. A small log never rolls over, so
%% `erlang:halt/1` leaves them in the WAL and nowhere else, which is why the
%% reported failure had no segments at all.
restart_recovers_the_log_with_a_foreign_ra_data_dir(Config) ->
    Dir = ours(Config),
    Base = ?config(foreign_base, Config),
    {Peer, Node} = start_peer(),
    ok = rpc:call(Node, application, set_env, [ra, data_dir, Base]),
    %% Resolved on the peer: the node name in it is the peer's.
    Foreign = rpc:call(Node, ra_env, data_dir, []),
    ok = rpc:call(Node, portunus, start_system, [?SYS, Dir]),
    {ok, _, _} = rpc:call(Node, portunus, start_cluster, [?SYS, ?NAME, [Node]]),
    ok = await_peer_leader(Node),
    %% The grant commits entries; the lease itself is not the point and its holder
    %% dies with the node.
    {ok, _} = rpc:call(Node, portunus, grant_lease, [?NAME, ?TTL]),
    #{last_index := Committed} = rpc:call(Node, ra, key_metrics, [{?NAME, Node}]),
    ?assert(Committed > 0),
    %% Nothing rolled over, so the entries are in the WAL and not in a segment.
    ?assertEqual([], segments(Dir)),
    ok = sync_registration(Node),
    ok = halt_peer(Peer, Node),
    {_Peer2, Node} = start_peer(),
    ok = rpc:call(Node, application, set_env, [ra, data_dir, Base]),
    {ok, _} = rpc:call(Node, application, ensure_all_started, [ra]),
    %% The foreign system recovers the shared directory first: it is the one that
    %% deletes what it does not recognise.
    ok = rpc:call(Node, ?MODULE, start_system_named, [?FOREIGN_SYS, Foreign, Foreign]),
    ok = rpc:call(Node, portunus, start_system, [?SYS, Dir]),
    ok = rpc:call(Node, portunus, restart_server, [?SYS, ?NAME]),
    ok = await_peer_leader(Node),
    ?assertMatch(#{last_index := I} when I >= Committed,
                 rpc:call(Node, ra, key_metrics, [{?NAME, Node}])),
    ?assertEqual({ok, [{?NAME, Node}], {?NAME, Node}},
                 rpc:call(Node, ra, members, [{?NAME, Node}, 5000])).

%% The mechanism one level down, asserted directly rather than through its
%% outcome: the foreign system's WAL recovery must not touch our files.
foreign_system_does_not_delete_our_wal(Config) ->
    Dir = ours(Config),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, _} = portunus:grant_lease(?NAME, ?TTL),
    Before = wals(Dir),
    ?assertNotEqual([], Before),
    ok = start_foreign_system(Config),
    ok = restart_foreign_system(Config),
    ?assertEqual(Before, wals(Dir)).

%% The property `ensure_started/1` and the recovery strategy rely on.
reusing_a_matching_system_is_idempotent(Config) ->
    Dir = ours(Config),
    ok = portunus:start_system(?SYS, Dir),
    ok = portunus:start_system(?SYS, Dir),
    %% The same directory spelled differently is the same request.
    ok = portunus:start_system(?SYS, Dir ++ "/"),
    ok = portunus:start_system(?SYS, list_to_binary(Dir)),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertMatch({ok, _}, portunus:grant_lease(?NAME, ?TTL)).

%% Naming a system someone else started under a foreign directory is the
%% configuration that caused the reported failure: it now refuses rather than
%% returning ok and writing where it will not read back.
reusing_a_foreign_system_errors(Config) ->
    ok = start_system_named(?SYS, foreign(Config), foreign(Config)),
    ?assertMatch({error, {ra_system_mismatch, ?SYS, #{wal_data_dir := _}}},
                 portunus:start_system(?SYS, ours(Config))).

%% `ra_systems_sup:start_system/1` stores the config before it checks the child,
%% so a check hung off `{already_started, _}` would both compare against itself
%% and leave the running system's config overwritten.
reusing_a_foreign_system_does_not_rewrite_its_config(Config) ->
    Theirs = foreign(Config),
    ok = start_system_named(?SYS, Theirs, Theirs),
    Before = ra_system:fetch(?SYS),
    ?assertMatch({error, {ra_system_mismatch, _, _}},
                 portunus:start_system(?SYS, ours(Config))),
    ?assertEqual(Before, ra_system:fetch(?SYS)).

%% The distinction `running_config/1` exists for: after an `ra` restart the
%% config outlives the processes, so `start_system/2` must still reach
%% `ra_system:start/1` and recover the replica rather than compare and return.
an_ra_restart_still_recovers(Config) ->
    Dir = ours(Config),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, Lease} = portunus:grant_lease(?NAME, ?TTL),
    {ok, Token} = portunus:acquire(?NAME, {res, hold}, Lease, owner_a),
    ok = application:stop(ra),
    {ok, _} = application:ensure_all_started(ra),
    ?assertNotEqual(undefined, ra_system:fetch(?SYS)),
    ok = portunus:start_system(?SYS, Dir),
    ok = portunus:restart_server(?SYS, ?NAME),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(?NAME) end),
    ok = portunus_test_helpers:await_leader(?NAME),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 portunus:owner(?NAME, {res, hold})).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

absdir(Dir) -> filename:absname(filename:join([Dir])).

wals(Dir) ->
    lists:sort(filelib:wildcard(filename:join(Dir, "*.wal"))).

segments(Dir) ->
    lists:sort(filelib:wildcard(filename:join([Dir, "*", "*.segment"]))).

%% A peer under a fixed name, so the node restarted after the halt recovers the
%% first one's data directory.
start_peer() ->
    {ok, Peer, Node} = ?CT_PEER(#{name => ?PEER_NAME,
                                  wait_boot => 60000,
                                  args => ["-pa" | code:get_path()]}),
    {Peer, Node}.

%% `ra_directory` writes the registration to DETS and syncs it only on a clean
%% deinit, so a halted node loses it and comes back with nothing to restart. That
%% is a separate defect; sync it here so this case is about the log alone.
sync_registration(Node) ->
    #{directory_rev := DirRev} = rpc:call(Node, ra_system, derive_names, [?SYS]),
    rpc:call(Node, dets, sync, [DirRev]).

%% `erlang:halt/1` rather than `peer:stop/1`: a graceful stop flushes the WAL to
%% this system's segments, and those survive the foreign deletion.
halt_peer(Peer, Node) ->
    _ = rpc:call(Node, erlang, halt, [0]),
    _ = catch peer:stop(Peer),
    portunus_test_helpers:await_condition(
      fun() -> net_adm:ping(Node) =:= pang end).

%% A second Ra system in the directory `ra.data_dir` points at, standing in for
%% the quorum-queue system a RabbitMQ node runs there.
start_foreign_system(Config) ->
    Dir = foreign(Config),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    start_system_named(?FOREIGN_SYS, Dir, Dir).

restart_foreign_system(Config) ->
    ok = ra_system:stop(?FOREIGN_SYS),
    start_foreign_system(Config).

await_peer_leader(Node) ->
    portunus_test_helpers:await_condition(
      fun() -> rpc:call(Node, ra_leaderboard, lookup_leader, [?NAME]) =:= {?NAME, Node} end).

start_system_named(System, DataDir, WalDataDir) ->
    Config = (ra_system:default_config())#{
               name => System,
               data_dir => DataDir,
               wal_data_dir => WalDataDir,
               names => ra_system:derive_names(System)},
    case ra_system:start(Config) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        Err -> Err
    end.
