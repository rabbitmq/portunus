%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_custom_names_integration_SUITE).

%% A running host system whose process names are not derived from the system
%% name. `start_system/2` must see it running (the WAL name comes from the
%% fetched config, not `derive_names/1`) and refuse with `ra_system_mismatch`:
%% `ra_systems_sup:start_system/1` stores the new config before it checks the
%% child, so proceeding would silently overwrite the host's stored config.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([custom_names_host_is_refused/1]).

-define(SYS, portunus_custom_names_sys).

all() ->
    [custom_names_host_is_refused].

init_per_testcase(TC, Config) ->
    Dir = filename:join([?config(priv_dir, Config), atom_to_list(TC), "ra"]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    [{ra_dir, Dir} | Config].

end_per_testcase(_TC, _Config) ->
    catch ra_system:stop(?SYS),
    ok.

custom_names_host_is_refused(Config) ->
    Dir = ?config(ra_dir, Config),
    {ok, _} = application:ensure_all_started(ra),
    %% Underived names: registered process names a host would choose freely.
    Names = ra_system:derive_names(portunus_custom_names_other_source),
    HostConfig0 = maps:remove(server_recovery_strategy,
                              ra_system:default_config()),
    HostConfig = HostConfig0#{name => ?SYS,
                              data_dir => Dir,
                              wal_data_dir => Dir,
                              names => Names},
    {ok, _} = ra_system:start(HostConfig),
    %% Red before the fetched-names check: `running_config/1` derived the
    %% WAL name, saw no such process, read the system as not running, and
    %% `start_system/2` returned `ok` with the host's config clobbered.
    ?assertMatch({error, {ra_system_mismatch, ?SYS, _}},
                 portunus:start_system(?SYS, Dir)),
    ?assertEqual(Names, maps:get(names, ra_system:fetch(?SYS))).
