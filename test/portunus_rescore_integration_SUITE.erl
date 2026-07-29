%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_rescore_integration_SUITE).

%% Succession re-scoring using a real cluster.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([promotion_follows_current_scores/1,
         stable_scores_append_nothing/1]).

-define(SYS, portunus_rescore_integration_sys).
-define(NAME, portunus_rescore_integration_test).
%% The TTL floor keeps the reconcile at its 1000 ms minimum, so a score
%% change is re-bid within a second or two.
-define(TTL, 2000).
-define(TAB, portunus_rescore_integration_scores).

all() ->
    [promotion_follows_current_scores,
     stable_scores_append_nothing].

init_per_suite(Config) ->
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

init_per_testcase(_TC, Config) ->
    %% Elections are linked; killing the owner must not kill the test.
    process_flag(trap_exit, true),
    ?TAB = ets:new(?TAB, [named_table, public]),
    Config.

end_per_testcase(_TC, _Config) ->
    catch ets:delete(?TAB),
    ok.

%% The three elections score themselves from slots a, b, and c.
%% Whichever contends first owns the key.
%% The other two queue up based on their enqueue-time
%% scores.
%%
%% The scores then move in a way that inverts the ranking.
%% Killing the owner election whose current (at the time of process termination)
%% score is highest.
promotion_follows_current_scores(_Config) ->
    Key = {rescore, follows_current},
    ets:insert(?TAB, [{a, 0}, {b, 0}, {c, 0}]),
    Elections = [start_election(Key, Slot) || Slot <- [a, b, c]],
    Owner = expect_elected(Key),
    [Standby1, Standby2] = [E || E <- Elections, E =/= Owner],
    ok = await_queue_depth(Key, 2),
    %% Rank the standbys by slot so the flip below inverts them precisely.
    #{Standby1 := Slot1, Standby2 := Slot2} =
        maps:from_list(lists:zip(Elections, [a, b, c])),
    %% First make Slot1 the stored best, so the final ranking cannot win by
    %% an arrival-order tie; then invert it. Without re-bids the queue would
    %% still rank Slot1 first. Each move gets two reconcile intervals to be
    %% re-bid.
    ets:insert(?TAB, [{Slot1, 10}, {Slot2, 1}]),
    timer:sleep(2500),
    ets:insert(?TAB, [{Slot1, 1}, {Slot2, 10}]),
    timer:sleep(2500),
    exit(Owner, kill),
    ?assertEqual(Standby2, expect_elected(Key)),
    portunus_election:stop_all([E || E <- Elections, E =/= Owner]).

%% With every score stable, reconciliation rounds are reads and renewals stay
%% off the Raft log. We detect that by watching the applied index during
%% a quiet ("actionless" interval).
stable_scores_append_nothing(_Config) ->
    Key = {rescore, stable},
    ets:insert(?TAB, [{a, 3}, {b, 2}, {c, 1}]),
    Elections = [start_election(Key, Slot) || Slot <- [a, b, c]],
    _Owner = expect_elected(Key),
    ok = await_queue_depth(Key, 2),
    %% Let in-flight commands from the contends drain before sampling.
    timer:sleep(500),
    Before = last_index(),
    timer:sleep(2500),
    ?assertEqual(Before, last_index()),
    portunus_election:stop_all(Elections).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

start_election(Key, Slot) ->
    Affinity = fun(_Key, _Members) -> ets:lookup_element(?TAB, Slot, 2) end,
    {ok, E} = portunus_election:start_link(?NAME, Key, portunus_demo_election,
                                           self(),
                                           #{ttl_ms => ?TTL,
                                             affinity => Affinity}),
    E.

expect_elected(Key) ->
    receive
        {elected, Key, _Token, Pid} -> Pid
    after 10000 -> ct:fail(not_elected)
    end.

await_queue_depth(Key, Depth) ->
    portunus_test_helpers:await_condition(
      fun() ->
              case portunus:contenders(?NAME, Key) of
                  {ok, Owners} -> length(Owners) =:= Depth;
                  _ -> false
              end
      end).

last_index() ->
    maps:get(last_index, ra:key_metrics({?NAME, node()}), 0).
