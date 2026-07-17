%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_epoch_unit_SUITE).

%% The fencing epoch, driving `apply/3` directly (no Ra): the first applied
%% command with a positive leader-stamped `system_time` sets it, every
%% client-facing identifier packs it, and `token_info/1` decomposes it.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([epoch_is_stamped_by_the_first_command/1,
         all_client_identifiers_pack/1,
         token_info_decomposes/1,
         gauge_components_fit_atomics/1,
         epoch_stamping_is_deterministic_on_replay/1]).

%% A plausible millisecond wall clock.
-define(EPOCH, 1750000000000).

all() ->
    [epoch_is_stamped_by_the_first_command,
     all_client_identifiers_pack,
     token_info_decomposes,
     gauge_components_fit_atomics,
     epoch_stamping_is_deterministic_on_replay].

%% Commands without a positive stamp leave the epoch unset (raw indices);
%% the first positively-stamped command sets it, and a later, higher stamp
%% does not move it.
epoch_is_stamped_by_the_first_command(_Config) ->
    S0 = new(),
    {{ok, R0}, S1, _} = at({watch, k0, dummy_pid()}, 1, 0, S0),
    ?assertEqual(#{epoch => 0, index => 1}, portunus_machine:token_info(R0)),
    {{ok, L}, S2, _} =
        at({grant_lease, undefined, 100000, o1, dummy_pid()}, 2, ?EPOCH, S1),
    ?assertEqual(#{epoch => ?EPOCH, index => 2},
                 portunus_machine:token_info(L)),
    {{ok, T}, _S3, _} =
        at({acquire, L, k, o1, undefined, nowait}, 3, ?EPOCH + 5000, S2),
    ?assertEqual(#{epoch => ?EPOCH, index => 3},
                 portunus_machine:token_info(T)).

%% The stamp comes from the incarnation's first command, whatever it is (here
%% a membership no-op), and a token, an auto-assigned lease id and a watch
%% reference minted afterwards all decompose to that epoch and their
%% command's index.
all_client_identifiers_pack(_Config) ->
    S0 = new(),
    {ok, S1, _} = at({nodeup, 'n@host'}, 1, ?EPOCH, S0),
    {{ok, LeaseId}, S2, _} =
        at({grant_lease, undefined, 100000, o1, dummy_pid()}, 2, ?EPOCH + 1, S1),
    {{ok, Token}, S3, _} =
        at({acquire, LeaseId, k, o1, undefined, nowait}, 3, ?EPOCH + 2, S2),
    {{ok, Ref}, _S4, _} = at({watch, k, dummy_pid()}, 4, ?EPOCH + 3, S3),
    [?assertEqual(#{epoch => ?EPOCH, index => Ix},
                  portunus_machine:token_info(Id))
     || {Id, Ix} <- [{LeaseId, 2}, {Token, 3}, {Ref, 4}]].

%% `portunus:token_info/1` (a delegation) round-trips packed and epoch-zero
%% identifiers.
token_info_decomposes(_Config) ->
    ?assertEqual(#{epoch => 0, index => 0}, portunus:token_info(0)),
    ?assertEqual(#{epoch => 0, index => 42}, portunus:token_info(42)),
    ?assertEqual(#{epoch => ?EPOCH, index => 42},
                 portunus:token_info((?EPOCH bsl 64) + 42)).

%% Seshat gauges are 64-bit atomics: the packed token exceeds them, so
%% `node_gauges/2` publishes `token_info/1`'s parts, and each part must fit.
gauge_components_fit_atomics(_Config) ->
    S0 = new(),
    {{ok, L}, S1, _} =
        at({grant_lease, undefined, 100000, o1, dummy_pid()}, 1, ?EPOCH, S0),
    {{ok, Token}, _S2, _} =
        at({acquire, L, k, o1, undefined, nowait}, 2, ?EPOCH, S1),
    ?assert(Token >= (1 bsl 64)),
    #{epoch := Epoch, index := Index} = portunus_machine:token_info(Token),
    ?assert(Epoch < (1 bsl 63)),
    ?assert(Index < (1 bsl 63)).

%% The stamp is read from logged command metadata, so replaying the same log
%% twice reaches identical state: the epoch cannot diverge across replicas.
epoch_stamping_is_deterministic_on_replay(_Config) ->
    Log = [{{nodeup, 'n@host'}, 1, ?EPOCH},
           {{grant_lease, undefined, 100000, o1, undefined}, 2, ?EPOCH + 1},
           {{acquire, 2 + (?EPOCH bsl 64), k, o1, undefined, nowait}, 3,
            ?EPOCH + 2}],
    ?assertEqual(replay(Log), replay(Log)).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

new() ->
    portunus_machine:init(#{cluster => test}).

at(Cmd, Ix, Now, State) ->
    Meta = portunus_test_helpers:meta(Ix, Now),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.

replay(Log) ->
    lists:foldl(fun({Cmd, Ix, Now}, S) ->
                        {_, S1, _} = at(Cmd, Ix, Now, S),
                        S1
                end, new(), Log).

dummy_pid() ->
    spawn(fun() -> timer:sleep(infinity) end).
