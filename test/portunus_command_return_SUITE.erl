%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_command_return_SUITE).

%% `cmd/3` and `query/2` must be exhaustive against Ra's real return surface. A
%% server mid-election with no leader can answer a command or query with a bare
%% `ok`, outside the documented `{ok, _, _} | {timeout, _} | {error, _}`, which a
%% non-exhaustive `case` crashes on with `{case_clause, ok}`. A single-node
%% cluster never reaches it, since its local server is always the leader, so
%% these tests force the reply through `meck` and assert the wrappers degrade to
%% a transient `{error, no_quorum}` the caller retries, never a crash.

-include_lib("proper/include/proper.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([bare_ok_command_is_no_quorum/1,
         bare_ok_query_is_no_quorum/1,
         any_unexpected_command_reply_is_no_quorum/1,
         any_unexpected_query_reply_is_no_quorum/1]).

-define(SYS, portunus_cmd_return_sys).
-define(NAME, portunus_cmd_return_test).
-define(KEY, {res, k}).
-define(TTL, 60000).
-define(REPLY_KEY, {?MODULE, stubbed_reply}).

all() ->
    [bare_ok_command_is_no_quorum,
     bare_ok_query_is_no_quorum,
     any_unexpected_command_reply_is_no_quorum,
     any_unexpected_query_reply_is_no_quorum].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    Config.

end_per_suite(_Config) ->
    persistent_term:erase(?REPLY_KEY),
    ok.

init_per_testcase(TC, Config) ->
    Dir = filename:join([proplists:get_value(priv_dir, Config),
                         atom_to_list(TC), "ra"]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_testcase(_TC, _Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    catch ra_system:stop(?SYS),
    ok.

%% A bare `ok` from `ra:process_command/3` becomes `{error, no_quorum}`, the
%% transient signal a caller retries, rather than crashing `grant_lease/2`.
bare_ok_command_is_no_quorum(_Config) ->
    with_command_reply(ok,
        fun() ->
                {error, no_quorum} = portunus:grant_lease(?NAME, ?TTL),
                ok
        end).

%% Same for a query: a bare `ok` from `ra:consistent_query/3` becomes
%% `{error, no_quorum}` rather than a crash in `owner/2`.
bare_ok_query_is_no_quorum(_Config) ->
    with_query_reply(ok,
        fun() ->
                {error, no_quorum} = portunus:owner(?NAME, ?KEY),
                ok
        end).

%% Whatever unexpected term Ra returns for a command, the wrapper degrades to
%% `{error, no_quorum}` and never crashes.
any_unexpected_command_reply_is_no_quorum(_Config) ->
    meck:new(ra, [passthrough, no_link, unstick]),
    meck:expect(ra, process_command, fun(_, _, _) -> stubbed_reply() end),
    try
        true = portunus_test_helpers:quickcheck(
                 fun prop_command_no_quorum/0, 100)
    after
        meck:unload(ra)
    end.

any_unexpected_query_reply_is_no_quorum(_Config) ->
    meck:new(ra, [passthrough, no_link, unstick]),
    meck:expect(ra, consistent_query, fun(_, _, _) -> stubbed_reply() end),
    try
        true = portunus_test_helpers:quickcheck(
                 fun prop_query_no_quorum/0, 100)
    after
        meck:unload(ra)
    end.

prop_command_no_quorum() ->
    ?FORALL(Reply, unexpected_reply(),
            begin
                persistent_term:put(?REPLY_KEY, Reply),
                {error, no_quorum} =:= portunus:grant_lease(?NAME, ?TTL)
            end).

prop_query_no_quorum() ->
    ?FORALL(Reply, unexpected_reply(),
            begin
                persistent_term:put(?REPLY_KEY, Reply),
                {error, no_quorum} =:= portunus:owner(?NAME, ?KEY)
            end).

%% Read in the meck fun, which may run in any process, so a process-independent
%% store is required.
stubbed_reply() ->
    persistent_term:get(?REPLY_KEY, ok).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% Terms outside Ra's documented `{ok, _, _} | {timeout, _} | {error, _}` that a
%% server in a transient state can nonetheless return; none must reach a caller
%% as anything but `{error, no_quorum}`.
unexpected_reply() ->
    proper_types:oneof([ok,
                        {ok},
                        {pending},
                        {redirect, node()},
                        unexpected_atom,
                        {1, 2},
                        []]).

with_command_reply(Reply, Fun) ->
    meck:new(ra, [passthrough, no_link, unstick]),
    meck:expect(ra, process_command, fun(_, _, _) -> Reply end),
    try Fun() after meck:unload(ra) end.

with_query_reply(Reply, Fun) ->
    meck:new(ra, [passthrough, no_link, unstick]),
    meck:expect(ra, consistent_query, fun(_, _, _) -> Reply end),
    try Fun() after meck:unload(ra) end.
