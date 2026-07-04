%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_succession_context_unit_SUITE).

%% A queued acquirer can attach a context, exactly as the try-once path
%% does: the machine stores it on the waiter and attaches it to the grant
%% on promotion.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([queued_context_survives_promotion/1]).

-define(SYS, portunus).
-define(NAME, portunus_succession_ctx_test).

all() ->
    [queued_context_survives_promotion].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

queued_context_survives_promotion(_Config) ->
    Key = {res, queued_ctx},
    Ctx = #{shovel => <<"a">>},
    {ok, L1} = portunus:grant_lease(?NAME, 60000),
    {ok, L2} = portunus:grant_lease(?NAME, 60000),
    {ok, T1} = portunus:acquire(?NAME, Key, L1, owner_1),
    {queued, 1} = portunus:acquire_or_join_succession_queue(
                    ?NAME, Key, L2, owner_2, #{context => Ctx}),
    ok = portunus:release(?NAME, Key, T1),
    receive
        {portunus, granted, Key, _T2, L2} -> ok
    after 10000 ->
            ct:fail(no_promotion)
    end,
    {ok, Info} = portunus:owner(?NAME, Key),
    ?assertEqual(Ctx, maps:get(context, Info)),
    ok = portunus:revoke_lease(?NAME, L2),
    ok = portunus:revoke_lease(?NAME, L1).
