%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_system_mismatch_unit_SUITE).

%% The Ra system config comparison behind `start_system/2`'s refusal to reuse a
%% system that is not the one asked for. The comparison is pure, so none of this
%% needs a running system. A false refusal is the only way the check can hurt a
%% caller doing nothing wrong, so most of these pin the directory spellings that
%% must still agree.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([matching_system_agrees/1,
         omitted_wal_data_dir_is_not_a_mismatch/1,
         representation_is_not_a_mismatch/1,
         relative_and_absolute_are_not_a_mismatch/1,
         trailing_separator_is_not_a_mismatch/1,
         an_unreadable_path_does_not_crash/1,
         foreign_wal_dir_is_a_mismatch/1,
         foreign_data_dir_is_a_mismatch/1,
         mismatch_names_both_directories/1,
         both_directories_foreign_reports_both/1,
         missing_recovery_strategy_is_a_mismatch/1,
         a_different_recovery_strategy_is_a_mismatch/1,
         matching_recovery_strategy_agrees/1]).

all() ->
    [matching_system_agrees,
     omitted_wal_data_dir_is_not_a_mismatch,
     representation_is_not_a_mismatch,
     relative_and_absolute_are_not_a_mismatch,
     trailing_separator_is_not_a_mismatch,
     an_unreadable_path_does_not_crash,
     foreign_wal_dir_is_a_mismatch,
     foreign_data_dir_is_a_mismatch,
     mismatch_names_both_directories,
     both_directories_foreign_reports_both,
     missing_recovery_strategy_is_a_mismatch,
     a_different_recovery_strategy_is_a_mismatch,
     matching_recovery_strategy_agrees].

config(Dir) ->
    #{data_dir => Dir, wal_data_dir => Dir}.

%%----------------------------------------------------------------------
%% Agreement: a mismatch reported here is a refusal to boot
%%----------------------------------------------------------------------

matching_system_agrees(_Config) ->
    C = config("/var/lib/portunus/n1"),
    ?assertEqual(#{}, portunus:config_mismatch(C, C)).

%% Ra's own fallback puts the WAL in `data_dir` when the key is absent, so a
%% config that omits it asks for exactly what we want.
omitted_wal_data_dir_is_not_a_mismatch(_Config) ->
    Dir = "/var/lib/portunus/n1",
    Running = #{data_dir => Dir},
    ?assertEqual(#{}, portunus:config_mismatch(config(Dir), Running)),
    ?assertEqual(#{}, portunus:config_mismatch(Running, config(Dir))).

%% `data_dir` is a `file:filename_all()`, so both spellings really do arrive.
representation_is_not_a_mismatch(_Config) ->
    Dir = "/var/lib/portunus/n1",
    ?assertEqual(#{}, portunus:config_mismatch(config(Dir), config(<<"/var/lib/portunus/n1">>))).

%% Ra resolves a relative directory against the node's working directory, so the
%% two name one directory.
relative_and_absolute_are_not_a_mismatch(_Config) ->
    Rel = "data/portunus",
    Abs = filename:absname(Rel),
    ?assertEqual(#{}, portunus:config_mismatch(config(Rel), config(Abs))).

trailing_separator_is_not_a_mismatch(_Config) ->
    ?assertEqual(#{}, portunus:config_mismatch(config("/var/lib/portunus/n1/"),
                                               config("/var/lib/portunus/n1"))).

%% The running system's config is the one input portunus does not control, so a
%% value that is not a filename must compare unequal rather than raise inside
%% `start_system/2`.
an_unreadable_path_does_not_crash(_Config) ->
    Running = config({not_a, filename}),
    ?assertMatch(#{data_dir := _, wal_data_dir := _},
                 portunus:config_mismatch(config("/var/lib/portunus/n1"), Running)).

%%----------------------------------------------------------------------
%% Refusal
%%----------------------------------------------------------------------

%% The reported failure's shape: our `data_dir`, another system's WAL.
foreign_wal_dir_is_a_mismatch(_Config) ->
    Ours = "/var/lib/rabbitmq/locks_and_registry/n1",
    Want = config(Ours),
    Running = #{data_dir => Ours, wal_data_dir => "/var/lib/rabbitmq/quorum/n1"},
    ?assertMatch(#{wal_data_dir := _}, portunus:config_mismatch(Want, Running)),
    ?assertNot(maps:is_key(data_dir, portunus:config_mismatch(Want, Running))).

foreign_data_dir_is_a_mismatch(_Config) ->
    Want = config("/var/lib/portunus/n1"),
    Running = #{data_dir => "/somewhere/else", wal_data_dir => "/var/lib/portunus/n1"},
    ?assertMatch(#{data_dir := _}, portunus:config_mismatch(Want, Running)),
    ?assertNot(maps:is_key(wal_data_dir, portunus:config_mismatch(Want, Running))).

%% The error is what the check produces, so its content is worth pinning: both
%% directories, wanted first.
mismatch_names_both_directories(_Config) ->
    Ours = "/var/lib/portunus/n1",
    Theirs = "/var/lib/rabbitmq/quorum/n1",
    Mismatch = portunus:config_mismatch(config(Ours),
                                        #{data_dir => Ours, wal_data_dir => Theirs}),
    ?assertEqual(#{wal_data_dir => {Ours, Theirs}}, Mismatch).

both_directories_foreign_reports_both(_Config) ->
    Mismatch = portunus:config_mismatch(config("/ours"), config("/theirs")),
    ?assertEqual(#{data_dir => {"/ours", "/theirs"},
                   wal_data_dir => {"/ours", "/theirs"}}, Mismatch).

%%----------------------------------------------------------------------
%% The recovery strategy: a system without it does not bring this node's
%% replicas back, and one portunus did not start never gets the registration
%% repair either. `ra_system:default_config/0` never sets the key, so every
%% system portunus did not build reads `undefined`.
%%----------------------------------------------------------------------

missing_recovery_strategy_is_a_mismatch(_Config) ->
    Dir = "/var/lib/portunus/n1",
    Want = (config(Dir))#{server_recovery_strategy => registered},
    ?assertEqual(#{server_recovery_strategy => {registered, undefined}},
                 portunus:config_mismatch(Want, config(Dir))).

%% A host's own system carries its own strategy: RabbitMQ's quorum system uses an
%% MFA. Reusing it is the shape that dropped portunus's config silently.
a_different_recovery_strategy_is_a_mismatch(_Config) ->
    Dir = "/var/lib/portunus/n1",
    Want = (config(Dir))#{server_recovery_strategy => registered},
    Theirs = (config(Dir))#{server_recovery_strategy =>
                                {rabbit_quorum_queue, recover, []}},
    ?assertEqual(#{server_recovery_strategy =>
                       {registered, {rabbit_quorum_queue, recover, []}}},
                 portunus:config_mismatch(Want, Theirs)).

matching_recovery_strategy_agrees(_Config) ->
    C = (config("/var/lib/portunus/n1"))#{server_recovery_strategy => registered},
    ?assertEqual(#{}, portunus:config_mismatch(C, C)).
