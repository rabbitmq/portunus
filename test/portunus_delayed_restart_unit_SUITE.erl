%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_delayed_restart_unit_SUITE).

%% Rate-limited restarts: the first start and any isolated restart run at
%% once; only a restart within `Delay` of the previous attempt waits out the
%% remainder. A failed start is paced too, and `forget/2` makes the next
%% start immediate. `start_link/3` keys its marker on the calling process,
%% so the suite process stands in for the local supervisor.

-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([first_start_is_immediate/1,
         rapid_restart_waits_out_the_delay/1,
         isolated_restart_is_immediate/1,
         forget_makes_next_start_immediate/1,
         child_spec_rewrites/1]).

all() ->
    [first_start_is_immediate,
     rapid_restart_waits_out_the_delay,
     isolated_restart_is_immediate,
     forget_makes_next_start_immediate,
     child_spec_rewrites].

init_per_suite(Config) ->
    %% The app supervisor owns the marker table, as in production; a table
    %% created here would die with this transient process.
    {ok, _} = application:ensure_all_started(portunus),
    Config.

end_per_suite(_Config) ->
    ok.

first_start_is_immediate(_Config) ->
    ?assert(elapsed_ms(fun() -> start(id_first, 1) end) < 500).

rapid_restart_waits_out_the_delay(_Config) ->
    %% Both starts together: the second waits out whatever remains of the
    %% delay, so the sum is at least one delay regardless of scheduling.
    Elapsed = elapsed_ms(fun() ->
                                 _ = start(id_rapid, 1),
                                 _ = start(id_rapid, 1)
                         end),
    ?assert(Elapsed >= 900).

isolated_restart_is_immediate(_Config) ->
    _ = start(id_isolated, 1),
    timer:sleep(1100),
    ?assert(elapsed_ms(fun() -> start(id_isolated, 1) end) < 500).

forget_makes_next_start_immediate(_Config) ->
    _ = start(id_forget, 1),
    ok = portunus_delayed_restart:forget(self(), id_forget),
    ?assert(elapsed_ms(fun() -> start(id_forget, 1) end) < 500).

child_spec_rewrites(_Config) ->
    MFA = {mod, fun_name, [a]},
    %% Delay 0 is a plain restart type, as in supervisor2.
    ?assertEqual(#{id => a, start => MFA, restart => permanent},
                 portunus_delayed_restart:child_spec(
                   #{id => a, start => MFA, restart => {permanent, 0}})),
    %% A positive delay wraps the start MFA.
    #{start := {portunus_delayed_restart, start_link, [b, 5, MFA]},
      restart := transient} =
        portunus_delayed_restart:child_spec(
          #{id => b, start => MFA, restart => {transient, 5}}),
    %% Float delays are supervisor2-legal.
    #{start := {portunus_delayed_restart, start_link, [c, 0.5, MFA]}} =
        portunus_delayed_restart:child_spec(
          #{id => c, start => MFA, restart => {permanent, 0.5}}),
    %% Tuple form, and a plain spec passes through untouched.
    {d, {portunus_delayed_restart, start_link, [d, 3, MFA]},
     permanent, 5000, worker, [mod]} =
        portunus_delayed_restart:child_spec(
          {d, MFA, {permanent, 3}, 5000, worker, [mod]}),
    Plain = #{id => e, start => MFA, restart => permanent},
    ?assertEqual(Plain, portunus_delayed_restart:child_spec(Plain)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% The wrapped start returns whatever the MFA returns; pacing is by attempt
%% time, so the result does not matter here.
start(Id, DelaySeconds) ->
    portunus_delayed_restart:start_link(Id, DelaySeconds, {erlang, self, []}).

elapsed_ms(Fun) ->
    T0 = erlang:monotonic_time(millisecond),
    _ = Fun(),
    erlang:monotonic_time(millisecond) - T0.
