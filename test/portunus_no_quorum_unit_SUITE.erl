%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_no_quorum_unit_SUITE).

%% A command that cannot be committed returns a stable `no_quorum` to the
%% caller and increments the `failures_due_to_lack_of_online_quorum_total`
%% counter.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([no_quorum_returns_error_and_counts/1]).

-define(SYS, portunus).
-define(NAME, portunus_no_quorum_test).

all() ->
    [no_quorum_returns_error_and_counts].

init_per_suite(Config) ->
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

no_quorum_returns_error_and_counts(_Config) ->
    Before = count(failures_due_to_lack_of_online_quorum_total),
    %% With the only server stopped, no command can commit.
    ok = ra:stop_server(?SYS, {?NAME, node()}),
    ?assertEqual({error, no_quorum}, portunus:grant_lease(?NAME, 1000)),
    ?assert(count(failures_due_to_lack_of_online_quorum_total) > Before).

count(Field) ->
    maps:get(Field, portunus_counters:overview(?NAME), 0).
