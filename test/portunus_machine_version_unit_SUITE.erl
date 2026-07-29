%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_machine_version_unit_SUITE).

%% May seem simplistic without several machine versions but this suite
%% was inspired by its quorum queue and Khepri counterparts.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([the_baseline_callbacks/1,
         version_commands_are_identity_conversions/1,
         pre_versioning_command_shapes_still_apply/1,
         replay_with_conversion_is_deterministic/1]).

all() ->
    [the_baseline_callbacks,
     version_commands_are_identity_conversions,
     pre_versioning_command_shapes_still_apply,
     replay_with_conversion_is_deterministic].

the_baseline_callbacks(_Config) ->
    ?assertEqual(1, portunus_machine:version()),
    ?assertEqual(portunus_machine, portunus_machine:which_module(0)),
    ?assertEqual(portunus_machine, portunus_machine:which_module(1)).

%% The version-raise command, exactly as Ra delivers it (the meta carries
%% the new version), leaves state built at the prior version unchanged.
%% So does a client-forged one with arbitrary fields: `016` §1.1's
%% poison-pill discipline, a no-op, never a crash.
version_commands_are_identity_conversions(_Config) ->
    S0 = populated_state(),
    {ok, S1, _} = step_at_version({machine_version, 0, 1}, 20, 1, S0),
    ?assertEqual(S0, S1),
    {ok, S2, _} = step({machine_version, <<"garbage">>, [-1]}, 21, S1),
    ?assertEqual(S0, S2).

%% A log written before this version carries the score-less wait acquire;
%% it still queues (at the FIFO score) and its waiter still promotes.
pre_versioning_command_shapes_still_apply(_Config) ->
    S0 = populated_state(),
    {{queued, 2}, S1, _} = step({acquire, l3, k, o3, undefined, wait}, 20, S0),
    {ok, #{token := T}} = portunus_machine:query_owner(k, S1),
    {ok, S2, _} = step({release, k, T}, 21, S1),
    %% l2 queued first at score 1; l3's default score 0 ranks below it.
    {ok, #{owner := o2}} = portunus_machine:query_owner(k, S2).

replay_with_conversion_is_deterministic(_Config) ->
    Log = [{grant_lease, l1, 1000, o1, dummy_pid()},
           {machine_version, 0, 1},
           {acquire, l1, k, o1, undefined, nowait},
           {grant_lease, l2, 1000, o2, dummy_pid()},
           {acquire, l2, k, o2, undefined, wait, 3},
           {release, k, 3}],
    ?assertEqual(replay(Log), replay(Log)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% A holder (l1, token minted) and a waiter (l2 at score 1), so the
%% conversion is checked against non-trivial state.
populated_state() ->
    S0 = portunus_machine:init(#{cluster => test}),
    {_, S1, _} = step({grant_lease, l1, 100000, o1, self()}, 1, S0),
    {_, S2, _} = step({grant_lease, l2, 100000, o2, self()}, 2, S1),
    {_, S3, _} = step({grant_lease, l3, 100000, o3, self()}, 3, S2),
    {{ok, _}, S4, _} = step({acquire, l1, k, o1, undefined, nowait}, 4, S3),
    {{queued, _}, S5, _} = step({acquire, l2, k, o2, undefined, wait, 1}, 5, S4),
    S5.

step(Cmd, Ix, State) ->
    do_step(portunus_test_helpers:meta(Ix), Cmd, State).

step_at_version(Cmd, Ix, Version, State) ->
    Meta = maps:put(machine_version, Version, portunus_test_helpers:meta(Ix)),
    do_step(Meta, Cmd, State).

do_step(Meta, Cmd, State) ->
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.

replay(Log) ->
    lists:foldl(
      fun({Cmd, Ix}, S0) ->
              {_, S, _} = step(Cmd, Ix, S0),
              S
      end,
      portunus_machine:init(#{cluster => replay}),
      lists:zip(Log, lists:seq(1, length(Log)))).

dummy_pid() ->
    spawn(fun() -> ok end).
