%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_monitor_prop_SUITE).

%% Property tests use PropEr, so this module includes only proper.hrl:
%% mixing it with eunit/ct headers redefines macros such as LET.
-include_lib("proper/include/proper.hrl").

-export([all/0, monitor_balance/1]).

all() ->
    [monitor_balance].

monitor_balance(_Config) ->
    true = portunus_test_helpers:quickcheck(fun prop_monitor_balance/0, 500).

%% For one holder under any sequence of grants and revokes, the machine emits a
%% `monitor` exactly when its live-lease count goes 0 -> 1 and a `demonitor`
%% exactly when it goes 1 -> 0, and never the other effect. Monitoring tracks
%% "the holder has at least one lease" with no leak.
prop_monitor_balance() ->
    ?FORALL(Ops, list(op()),
            check(Ops, self())).

op() ->
    {oneof([grant, revoke]), integer(1, 4)}.

check(Ops, Pid) ->
    S0 = portunus_machine:init(#{cluster => test}),
    {Ok, _S, _Live, _Ix} =
        lists:foldl(fun(Op, {Acc, S, Live, Ix}) ->
                            {Ok, S1, Live1} = step_check(Op, S, Live, Ix, Pid),
                            {Acc andalso Ok, S1, Live1, Ix + 1}
                    end, {true, S0, sets:new([{version, 2}]), 1}, Ops),
    Ok.

step_check({grant, Id}, S, Live, Ix, Pid) ->
    WasEmpty = sets:size(Live) =:= 0,
    {_, S1, E} = step({grant_lease, Id, 1000, o, Pid}, Ix, 0, S),
    Ok = (has(monitor, Pid, E) =:= WasEmpty) andalso (not has(demonitor, Pid, E)),
    {Ok, S1, sets:add_element(Id, Live)};
step_check({revoke, Id}, S, Live, Ix, Pid) ->
    BecomesEmpty = sets:is_element(Id, Live) andalso sets:size(Live) =:= 1,
    {_, S1, E} = step({revoke_lease, Id}, Ix, 0, S),
    Ok = (has(demonitor, Pid, E) =:= BecomesEmpty) andalso (not has(monitor, Pid, E)),
    {Ok, S1, sets:del_element(Id, Live)}.

has(Kind, Pid, Effects) ->
    lists:member({Kind, process, Pid}, Effects).

step(Cmd, Index, Time, State) ->
    case portunus_machine:apply(portunus_test_helpers:meta(Index, Time), Cmd, State) of
        {S, R} -> {R, S, []};
        {S, R, E} -> {R, S, E}
    end.
