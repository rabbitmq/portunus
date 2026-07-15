%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_registration_recovery_SUITE).

%% A server's registration is a DETS write that a hard kill can lose while the
%% replica's directory, config and log survive. `start_system/2` must rebuild it
%% from the replicas' own `config` files before the Ra system starts: WAL
%% recovery deletes the entries of any writer whose UID is not registered, so
%% the repair cannot wait for `restart_server/2`. The loss is simulated by
%% deleting `names.dets` between a system stop and a system start, which is the
%% state a kill leaves: an empty registration table next to an intact replica
%% directory.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([lost_registration_is_recovered_from_disk/1,
         a_new_node_is_still_not_registered/1,
         ambiguous_directories_are_not_guessed/1,
         foreign_configs_are_ignored/1]).

-define(SYS, portunus_registration_recovery_sys).
-define(NAME, portunus_registration_recovery_test).
-define(TTL, 60000).

all() ->
    [lost_registration_is_recovered_from_disk,
     a_new_node_is_still_not_registered,
     ambiguous_directories_are_not_guessed,
     foreign_configs_are_ignored].

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

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

lost_registration_is_recovered_from_disk(Config) ->
    Dir = dir(Config),
    Token = form_and_lock(Dir),
    ok = wipe_registration(Dir),
    %% The precondition the case is about: no registration, replica on disk.
    ?assertNot(filelib:is_regular(filename:join(Dir, "names.dets"))),
    ?assertMatch([_], server_dirs(Dir)),
    ok = portunus:start_system(?SYS, Dir),
    %% The repair itself, observed directly: the directory knows the server
    %% again before anything is asked to converge.
    ok = portunus_test_helpers:await_registered(?SYS, ?NAME),
    ok = portunus:restart_server(?SYS, ?NAME),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(?NAME) end),
    ok = portunus_test_helpers:await_leader(?NAME),
    %% The log itself must come back, not just the registration: a repair that
    %% runs after WAL recovery leaves an empty log behind a live server.
    ?assertMatch(#{last_index := I} when I > 0, ra:key_metrics({?NAME, node()})),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 portunus:owner(?NAME, {res, hold})).

%% A node with no replica directory still routes to formation.
a_new_node_is_still_not_registered(Config) ->
    ok = portunus:start_system(?SYS, dir(Config)),
    ?assertEqual({error, name_not_registered},
                 portunus:restart_server(?SYS, ?NAME)).

%% Two directories claiming the id cannot be told apart without the
%% registration, so the scan must refuse rather than guess.
ambiguous_directories_are_not_guessed(Config) ->
    Dir = dir(Config),
    _ = form_and_lock(Dir),
    [ServerDir] = server_dirs(Dir),
    ok = wipe_registration(Dir),
    Fake = filename:join(Dir, "TANZU_FAKESTALEUID0"),
    ok = filelib:ensure_dir(filename:join(Fake, "x")),
    {ok, _} = file:copy(filename:join(ServerDir, "config"),
                        filename:join(Fake, "config")),
    ok = portunus:start_system(?SYS, Dir),
    ?assertEqual({error, name_not_registered},
                 portunus:restart_server(?SYS, ?NAME)),
    ?assertEqual(undefined, whereis(?NAME)).

%% A config naming another node's server, as a copied data directory would
%% carry, is not registered here.
foreign_configs_are_ignored(Config) ->
    Dir = dir(Config),
    Foreign = filename:join(Dir, "TANZU_FOREIGNUID00"),
    ok = filelib:ensure_dir(filename:join(Foreign, "x")),
    FConfig = #{id => {?NAME, 'other_node@other_host'},
                uid => <<"TANZU_FOREIGNUID00">>,
                cluster_name => ?NAME},
    ok = file:write_file(filename:join(Foreign, "config"),
                         io_lib:format("~p.", [FConfig])),
    ok = portunus:start_system(?SYS, Dir),
    ?assertEqual({error, name_not_registered},
                 portunus:restart_server(?SYS, ?NAME)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

form_and_lock(Dir) ->
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, Lease} = portunus:grant_lease(?NAME, ?TTL),
    {ok, Token} = portunus:acquire(?NAME, {res, hold}, Lease, owner_a),
    Token.

%% Stop cleanly, then delete the registration table: what survives is exactly
%% what survives a kill that lost the buffered write.
wipe_registration(Dir) ->
    ok = ra_system:stop(?SYS),
    ok = file:delete(filename:join(Dir, "names.dets")),
    ok.

server_dirs(Dir) ->
    [S || S <- filelib:wildcard(filename:join(Dir, "*")),
          filelib:is_dir(S),
          filelib:is_regular(filename:join(S, "config"))].
