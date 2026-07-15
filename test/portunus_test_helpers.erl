%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_test_helpers).

%% Polling helpers shared by the suites, mirroring
%% `rabbit_ct_helpers:await_condition/2`: poll a predicate and fail with a
%% clear message on timeout, rather than assert once against async state (a
%% common flake) or crash with an opaque badmatch when a hand-rolled loop runs
%% out of retries.

-export([await_condition/1, await_condition/2,
         await_leader/1, await_leader/2,
         await_registered/2,
         meta/1, meta/2, quickcheck/2]).

%% A valid `ra_machine:command_meta_data()` for driving `portunus_machine:apply/3`
%% directly in a test. The contract requires `term` even though the machine
%% reads only `index` and `system_time`.
-spec meta(non_neg_integer()) -> ra_machine:command_meta_data().
meta(Index) ->
    meta(Index, Index).

-spec meta(non_neg_integer(), non_neg_integer()) -> ra_machine:command_meta_data().
meta(Index, SystemTime) ->
    #{index => Index, system_time => SystemTime, term => 1}.

%% Run a PropEr property given as a nullary fun. Going through `erlang:apply/2`
%% hands `proper:quickcheck/2` a plain term rather than the opaque
%% `proper:outer_test()` the `?FORALL` macro builds, which keeps dialyzer quiet
%% over the property suites (the same trick `ra` uses).
-spec quickcheck(fun(), pos_integer()) -> boolean().
quickcheck(MakeProp, NumTests) ->
    proper:quickcheck(erlang:apply(MakeProp, []),
                      [{numtests, NumTests}, {to_file, user}]).

-spec await_condition(fun(() -> boolean())) -> ok.
await_condition(Fun) ->
    await_condition(Fun, 15000).

-spec await_condition(fun(() -> boolean()), pos_integer()) -> ok.
await_condition(Fun, Timeout) ->
    await_retries(Fun, ceil(Timeout / 50)).

await_retries(_Fun, 0) ->
    ct:fail("condition did not hold within the timeout");
await_retries(Fun, Retries) ->
    case Fun() of
        true -> ok;
        _ -> timer:sleep(50), await_retries(Fun, Retries - 1)
    end.

%% The registration is what recovery finds a server by, so "the directory knows
%% this server" is the readiness signal to wait on after a system start.
-spec await_registered(portunus:system(), portunus:name()) -> ok.
await_registered(System, Name) ->
    await_condition(fun() -> is_binary(ra_directory:uid_of(System, Name)) end).

-spec await_leader(portunus:name()) -> ok.
await_leader(Name) ->
    await_leader(Name, 5000).

-spec await_leader(portunus:name(), pos_integer()) -> ok.
await_leader(Name, Timeout) ->
    await_condition(fun() -> ra_leaderboard:lookup_leader(Name) =/= undefined end,
                    Timeout).
