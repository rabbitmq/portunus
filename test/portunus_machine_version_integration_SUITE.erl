%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_machine_version_integration_SUITE).

%% May seem simplistic without several machine versions but this suite
%% was inspired by its quorum queue and Khepri counterparts.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([formed_cluster_runs_at_the_declared_version/1]).

-define(SYS, portunus_machine_version_int_sys).
-define(NAME, portunus_machine_version_int_test).

all() ->
    [formed_cluster_runs_at_the_declared_version].

init_per_suite(Config) ->
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

formed_cluster_runs_at_the_declared_version(_Config) ->
    {ok, Overview, _} = ra:member_overview({?NAME, node()}),
    ?assertEqual(portunus_machine:version(),
                 maps:get(effective_machine_version, Overview)),
    %% The cluster still answers commands at that version.
    {ok, Lease} = portunus:grant_lease(?NAME, 60000),
    ok = portunus:revoke_lease(?NAME, Lease).
