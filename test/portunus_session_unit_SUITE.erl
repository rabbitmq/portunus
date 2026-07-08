%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_session_unit_SUITE).

%% Losing the underlying lease stops the session and frees its keys. The
%% lease is opened with a known proposed id so the test can revoke it
%% directly, driving the renewer's `expired` path into the session.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2]).
-export([lease_loss_stops_session_and_frees_keys/1]).

-define(SYS, portunus).
-define(NAME, portunus_session_test).
-define(TTL, 3000).

all() ->
    [lease_loss_stops_session_and_frees_keys].

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

%% The session links to the test process, so trapping exits lets the test
%% observe its stop rather than die with it.
init_per_testcase(_Case, Config) ->
    process_flag(trap_exit, true),
    Config.

lease_loss_stops_session_and_frees_keys(_Config) ->
    Id = {stable, session, lease},
    {ok, Session} = portunus_session:open(?NAME, #{proposed_id => Id, ttl_ms => ?TTL}),
    Key = {res, session_key},
    {ok, _Token} = portunus_session:claim(Session, Key),
    {ok, #{owner := Owner}} = portunus:owner(?NAME, Key),
    Owner = node(),
    %% Revoking the lease frees the key at once; the renewer then reports
    %% `expired` and the session stops.
    ok = portunus:revoke_lease(?NAME, Id),
    ok = portunus_test_helpers:await_condition(fun() -> portunus:owner(?NAME, Key) =:= {error, not_held} end),
    receive
        {'EXIT', Session, lease_lost} -> ok
    after ?TTL + 2000 ->
        ct:fail(session_did_not_stop)
    end.
