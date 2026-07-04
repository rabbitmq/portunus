%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_expiry_notice_unit_SUITE).

%% Expiry is the one lease loss nobody initiated, so the machine's expiry
%% sweep tells the holder directly with the same `lease_lost` message the
%% renewer would deliver a renew interval later. A deliberate revoke sends
%% nothing: its initiator already knows.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([expiry_sweep_notifies_the_holder/1,
         revoke_sends_no_notice/1,
         expired_holder_receives_lease_lost/1]).

-define(SYS, portunus).
-define(NAME, portunus_expiry_notice_test).

all() ->
    [expiry_sweep_notifies_the_holder,
     revoke_sends_no_notice,
     expired_holder_receives_lease_lost].

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

expiry_sweep_notifies_the_holder(_Config) ->
    S0 = portunus_machine:init(#{}),
    {S1, {ok, l1}, _} = apply_at({grant_lease, l1, 1000, o, self()}, 1, 0, S0),
    {_S2, ok, Effects} = apply_at({timeout, expire}, 2, 5000, S1),
    ?assert(lists:member({send_msg, self(),
                          {portunus, lease_lost, l1}, [local]}, Effects)).

revoke_sends_no_notice(_Config) ->
    S0 = portunus_machine:init(#{}),
    {S1, {ok, l1}, _} = apply_at({grant_lease, l1, 1000, o, self()}, 1, 0, S0),
    {_S2, ok, Effects} = apply_at({revoke_lease, l1}, 2, 100, S1),
    ?assertNot(lists:any(fun({send_msg, _, {portunus, lease_lost, _}, _}) -> true;
                            ({send_msg, _, {portunus, lease_lost, _}}) -> true;
                            (_) -> false
                         end, Effects)).

%% End to end: a holder that stops renewing hears about the expiry from the
%% sweep, well before a renewer would have noticed.
expired_holder_receives_lease_lost(_Config) ->
    {ok, L} = portunus:grant_lease(?NAME, 2000),
    receive
        {portunus, lease_lost, L} -> ok
    after 10000 ->
            ct:fail(no_expiry_notice)
    end.

apply_at(Cmd, Ix, Time, S) ->
    portunus_machine:apply(portunus_test_helpers:meta(Ix, Time), Cmd, S).
