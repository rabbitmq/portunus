%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_session_release_SUITE).

%% A release that cannot reach quorum surfaces the error and
%% keeps the key, rather than reporting ok and dropping it.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([release_under_no_quorum_keeps_key/1]).

-define(SYS, portunus).
-define(NAME, portunus_session_release_test).
-define(TTL, 60000).

all() ->
    [release_under_no_quorum_keeps_key].

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

init_per_testcase(_TC, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TC, _Config) ->
    catch meck:unload(portunus),
    ok.

release_under_no_quorum_keeps_key(_Config) ->
    {ok, S} = portunus_session:open(?NAME, #{ttl_ms => ?TTL}),
    Key = {res, sess_rel},
    {ok, _T} = portunus_session:claim(S, Key),
    ok = meck:new(portunus, [passthrough, no_link]),
    meck:expect(portunus, release, fun(_N, _K, _Tok) -> {error, no_quorum} end),
    ?assertEqual({error, no_quorum}, portunus_session:release(S, Key)),
    ?assertEqual([Key], portunus_session:keys(S)),
    meck:unload(portunus),
    %% A real release now frees the still-held key.
    ?assertEqual(ok, portunus_session:release(S, Key)),
    ?assertEqual([], portunus_session:keys(S)),
    portunus_session:close(S).
