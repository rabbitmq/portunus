%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_stop_all_unit_SUITE).

%% `stop_all/2` bounds a registry or service shutdown to one deadline: the
%% stops run concurrently, and a member that cannot terminate in time is
%% killed rather than holding the tree open.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([stops_run_concurrently/1,
         stragglers_are_killed_at_the_deadline/1]).
%% Minimal gen_server callbacks: the suite doubles as a slow-stopping server.
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

all() ->
    [stops_run_concurrently,
     stragglers_are_killed_at_the_deadline].

stops_run_concurrently(_Config) ->
    %% Each terminate sleeps 700 ms: serial stops would need ~2100 ms, so
    %% finishing under 2000 ms proves the stops overlap.
    Pids = [begin
                {ok, P} = gen_server:start(?MODULE, stoppable, []),
                P
            end || _ <- [1, 2, 3]],
    Elapsed = elapsed_ms(fun() -> portunus_election:stop_all(Pids, 5000) end),
    ?assert(Elapsed >= 700 andalso Elapsed < 2000),
    [?assertNot(is_process_alive(P)) || P <- Pids].

stragglers_are_killed_at_the_deadline(_Config) ->
    %% A process that ignores system messages models an election stuck in
    %% user `stepped_down` code.
    Stuck = spawn(fun() -> receive never -> ok end end),
    Elapsed = elapsed_ms(fun() ->
                                 portunus_election:stop_all([Stuck], 500)
                         end),
    ?assert(Elapsed >= 500 andalso Elapsed < 3000),
    ok = portunus_test_helpers:await_condition(
           fun() -> not is_process_alive(Stuck) end).

init(stoppable) ->
    {ok, no_state}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    timer:sleep(700).

elapsed_ms(Fun) ->
    T0 = erlang:monotonic_time(millisecond),
    _ = Fun(),
    erlang:monotonic_time(millisecond) - T0.
