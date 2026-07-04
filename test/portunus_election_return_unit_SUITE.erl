%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_election_return_unit_SUITE).

%% A callback that returns a bad value from `elected/1`, instead of
%% raising, takes the same release-and-recontend path as an exception. In
%% a `try ... of` the non-matching return raises `try_clause` outside the
%% protected section, so before the explicit `Other ->` clause it crashed the election.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([bad_return_defers_and_recovers/1]).
%% portunus_election callbacks
-export([elected/1, stepped_down/1]).

-define(SYS, portunus).
-define(NAME, portunus_election_return_test).

all() ->
    [bad_return_defers_and_recovers].

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

bad_return_defers_and_recovers(_Config) ->
    Counter = counters:new(1, []),
    {ok, E} = portunus_election:start_link(?NAME, {key, bad_return},
                                           ?MODULE, {Counter, self()},
                                           #{ttl_ms => 5000}),
    %% First win returns garbage: the election must survive, release, and
    %% win again on the retry, where the callback succeeds.
    receive
        {elected_ok, {key, bad_return}} -> ok
    after 15000 ->
            ct:fail(no_recovery_after_bad_return)
    end,
    ?assert(is_process_alive(E)),
    ?assert(portunus_election:is_leader(E)),
    ok = portunus_election:stop(E).

%%----------------------------------------------------------------------
%% portunus_election callbacks
%%----------------------------------------------------------------------

elected(#{key := Key, args := {Counter, Ctrl}}) ->
    case counters:get(Counter, 1) of
        0 ->
            ok = counters:add(Counter, 1, 1),
            {error, not_what_the_contract_says};
        _ ->
            Ctrl ! {elected_ok, Key},
            {ok, no_state}
    end.

stepped_down(_State) ->
    ok.
