%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_command_guard_prop_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([apply_is_total/1]).

all() ->
    [apply_is_total].

%% `apply/3` must return an error rather than raise on any term: a command
%% that crashes it would crash every replica that replays the log. Hence the
%% commands here are arbitrary terms, not just the expected ones.
apply_is_total(_Config) ->
    true = portunus_test_helpers:quickcheck(fun prop_apply_is_total/0, 500).

prop_apply_is_total() ->
    ?FORALL(Cmd, command_gen(),
            begin
                S0 = seeded_state(),
                try portunus_machine:apply(
                      portunus_test_helpers:meta(100), Cmd, S0) of
                    {_, _} -> true;
                    {_, _, _} -> true
                catch
                    _:_ -> false
                end
            end).

%% Known command shapes with arbitrary fields, plus entirely arbitrary terms.
command_gen() ->
    Field = field_gen(),
    oneof([{grant_lease, Field, Field, Field, Field},
           {renew, list(Field)},
           {revoke_lease, Field},
           {acquire, Field, Field, Field, Field, oneof([wait, nowait, Field])},
           {acquire, Field, Field, Field, Field, oneof([wait, nowait, Field]),
            Field},
           {release, Field, Field},
           {transfer, Field, Field, Field},
           {transfer_batch, list({Field, Field, Field})},
           {transfer_batch, list(Field)},
           {transfer_batch, Field},
           {leave_queue, Field, Field},
           {watch, Field, Field},
           {unwatch, Field},
           {expire_leases, list({Field, Field})},
           {expire_leases, Field},
           {timeout, expire},
           {down, Field, Field},
           {nodeup, Field},
           {nodedown, Field},
           Field]).

field_gen() ->
    oneof([integer(), atom(), binary(), list(integer()),
           {atom(), integer()}, exactly(self()), exactly(undefined),
           float()]).

%% A state with a lease, a held key, a waiter and a watch, so field-level
%% garbage meets non-empty maps rather than only the empty state.
seeded_state() ->
    S0 = portunus_machine:init(#{}),
    Steps = [{{grant_lease, l1, 5000, o1, self()}, 1},
             {{grant_lease, l2, 5000, o2, undefined}, 2},
             {{acquire, l1, k1, o1, ctx, nowait}, 3},
             {{acquire, l2, k1, o2, ctx, wait, 7}, 4},
             {{watch, k1, self()}, 5}],
    lists:foldl(fun({Cmd, Ix}, S) ->
                        case portunus_machine:apply(
                               portunus_test_helpers:meta(Ix), Cmd, S) of
                            {S1, _} -> S1;
                            {S1, _, _} -> S1
                        end
                end, S0, Steps).
