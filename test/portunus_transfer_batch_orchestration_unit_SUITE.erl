%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_transfer_batch_orchestration_unit_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
         end_per_testcase/2]).
-export([self_target_and_unknown_key_skip_the_command/1,
         duplicate_key_is_deduplicated/1,
         batch_no_quorum_reports_per_key_and_restores/1,
         large_batch_is_split_into_bounded_commands/1]).

-define(SYS, portunus_transfer_batch_orchestration_sys).
-define(NAME, portunus_transfer_batch_orchestration_test).
-define(TTL, 3000).
-define(PEER, 'peer@nohost').

all() ->
    [self_target_and_unknown_key_skip_the_command,
     duplicate_key_is_deduplicated,
     batch_no_quorum_reports_per_key_and_restores,
     large_batch_is_split_into_bounded_commands].

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

init_per_testcase(_Case, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_Case, _Config) ->
    catch meck:unload(portunus),
    application:unset_env(portunus, transfer_batch_chunk),
    ok.

self_target_and_unknown_key_skip_the_command(_Config) ->
    Key = {orch, self_unknown},
    {ok, E} = start(Key),
    receive {elected, Key, _T, E} -> ok after 30000 -> ct:fail(no_leader) end,
    Results = portunus_election:transfer_many(
                ?NAME, [{Key, node()}, {{orch, ghost}, ?PEER}], #{Key => E}),
    [{Key, ok}, {{orch, ghost}, {error, not_owner}}] = Results,
    true = portunus_election:is_leader(E),
    ok = portunus_election:stop(E).

%% A key named twice collapses to its first target.
duplicate_key_is_deduplicated(_Config) ->
    Key = {orch, dup},
    {ok, E} = start(Key),
    receive {elected, Key, _T, E} -> ok after 30000 -> ct:fail(no_leader) end,
    PeerLease = enqueue_peer(Key),
    Results = portunus_election:transfer_many(
                ?NAME, [{Key, ?PEER}, {Key, ?PEER}], #{Key => E}),
    [{Key, ok}] = Results,
    ok = portunus_test_helpers:await_condition(
           fun() -> owner_is_peer(Key, PeerLease) end, 30000),
    ok = portunus:revoke_lease(?NAME, PeerLease),
    ok = portunus_election:stop(E).

%% A timed-out command sends no settle: the prepared election resolves
%% ownership itself and, since nothing committed, restores its work.
batch_no_quorum_reports_per_key_and_restores(_Config) ->
    Key = {orch, no_quorum},
    meck:new(portunus, [passthrough]),
    {ok, E} = start(Key),
    T1 = receive {elected, Key, Tok, E} -> Tok after 30000 -> ct:fail(no_leader) end,
    _PeerLease = enqueue_peer(Key),
    meck:expect(portunus, transfer_many, fun(_Name, _Batch) -> {error, no_quorum} end),
    [{Key, {error, no_quorum}}] =
        portunus_election:transfer_many(?NAME, [{Key, ?PEER}], #{Key => E}),
    meck:delete(portunus, transfer_many, 2),
    %% The command never committed, so the reconcile backstop finds this node
    %% still owning and restores the work on the same token.
    receive {elected, Key, T1, E} -> ok after 30000 -> ct:fail(not_restored) end,
    true = portunus_election:is_leader(E),
    ok = portunus_election:stop(E).

large_batch_is_split_into_bounded_commands(_Config) ->
    application:set_env(portunus, transfer_batch_chunk, 2),
    meck:new(portunus, [passthrough]),
    Keys = [{orch, chunk, I} || I <- lists:seq(1, 3)],
    Elections = [begin
                     {ok, Ei} = start(K),
                     receive {elected, K, _, Ei} -> ok
                     after 30000 -> ct:fail({no_leader, K})
                     end,
                     _ = enqueue_peer(K),
                     {K, Ei}
                 end || K <- Keys],
    Sizes = ets:new(sizes, [public, bag]),
    meck:expect(portunus, transfer_many,
                fun(_Name, Batch) ->
                        ets:insert(Sizes, {size, length(Batch)}),
                        [{LockKey, ok} || {LockKey, _, _} <- Batch]
                end),
    Live = maps:from_list(Elections),
    Results = portunus_election:transfer_many(
                ?NAME, [{K, ?PEER} || K <- Keys], Live),
    ?assertEqual([{K, ok} || K <- Keys], Results),
    %% Three keys at a bound of two: two commands, neither over the bound.
    2 = meck:num_calls(portunus, transfer_many, 2),
    [?assert(N =< 2) || {size, N} <- ets:tab2list(Sizes)],
    ets:delete(Sizes),
    meck:delete(portunus, transfer_many, 2),
    [ok = portunus_election:stop(Ei) || {_, Ei} <- Elections].

%% Helpers

start(Key) ->
    portunus_election:start_link(?NAME, Key, portunus_demo_election,
                                 self(), #{ttl_ms => ?TTL}).

enqueue_peer(Key) ->
    {ok, Lease} = portunus:grant_lease(?NAME, 60000),
    {queued, _} = portunus:acquire_or_join_succession_queue(?NAME, Key, Lease,
                                                            ?PEER),
    Lease.

owner_is_peer(Key, PeerLease) ->
    case portunus:owner(?NAME, Key) of
        {ok, #{lease := PeerLease}} -> true;
        _ -> false
    end.
