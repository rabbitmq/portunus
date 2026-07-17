%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_epoch_integration_SUITE).

%% One real cluster case for the epoch: every other epoch test drives
%% `apply/3` directly, and a wiring mistake (Ra not stamping `system_time`
%% the way the machine expects) would otherwise surface only in the hosted
%% re-formation regression.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([live_cluster_mints_packed_tokens/1]).

-define(SYS, portunus_epoch_int_sys).
-define(NAME, portunus_epoch_int_test).

all() ->
    [live_cluster_mints_packed_tokens].

init_per_testcase(TC, Config) ->
    Dir = filename:join([?config(priv_dir, Config), atom_to_list(TC), "ra"]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    [{ra_dir, Dir} | Config].

end_per_testcase(_TC, _Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    catch ra_system:stop(?SYS),
    ok.

live_cluster_mints_packed_tokens(Config) ->
    ok = portunus:start_system(?SYS, ?config(ra_dir, Config)),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    {ok, Lease} = portunus:grant_lease(?NAME, 60000),
    {ok, Token} = portunus:acquire(?NAME, {res, epoch}, Lease, owner_a),
    #{epoch := Epoch, index := Index} = portunus:token_info(Token),
    ?assert(Epoch > 0),
    ?assert(Index > 0),
    %% Ra stamps `system_time` in milliseconds; a wildly different unit
    %% would still pass a bare positivity check.
    Now = erlang:system_time(millisecond),
    ?assert(Epoch > Now - 3600000 andalso Epoch =< Now),
    %% The auto-assigned lease id packs the same epoch.
    ?assertEqual(Epoch, maps:get(epoch, portunus:token_info(Lease))),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 portunus:owner(?NAME, {res, epoch})),
    %% The aux tick publishes the token's parts: the packed value does not
    %% fit a 64-bit gauge, so `fencing_token` carries the index and the new
    %% `fencing_epoch` gauge the epoch.
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   Gauges = portunus_counters:overview(?NAME),
                   maps:get(fencing_token, Gauges, 0) =:= Index
                       andalso maps:get(fencing_epoch, Gauges, 0) =:= Epoch
           end).
