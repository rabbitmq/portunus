%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_watch_unit_SUITE).

%% End-to-end watch delivery through the public API: a watcher is sent an
%% `acquired` event when the key is taken and a `released` event when it is
%% freed, tagged with the ref from `watch/2`.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([watch_delivers_acquire_and_release/1,
         watch_handoff_orders_released_before_acquired/1,
         re_watch_supersedes_the_old_ref/1,
         watcher_death_drops_the_watch/1]).

-define(SYS, portunus).
-define(NAME, portunus_watch_test).

all() ->
    [watch_delivers_acquire_and_release,
     watch_handoff_orders_released_before_acquired,
     re_watch_supersedes_the_old_ref,
     watcher_death_drops_the_watch].

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

watch_delivers_acquire_and_release(_Config) ->
    Key = {res, watched},
    {ok, Ref} = portunus:watch(?NAME, Key),
    Owner = start_owner(Key),
    expect_event(Ref, {acquired, watched_owner}),
    Owner ! release,
    expect_event(Ref, released),
    true = exit(Owner, kill).

%% On a handoff to a queued waiter the watcher must see released before the
%% successor's acquired, so its last event names the real owner.
watch_handoff_orders_released_before_acquired(_Config) ->
    Key = {res, handoff},
    {ok, Ref} = portunus:watch(?NAME, Key),
    OwnerA = start_owner(Key, owner_a),
    ?assertEqual({acquired, owner_a}, expect_next(Ref)),
    WaiterB = start_waiter(Key, owner_b),
    OwnerA ! release,
    %% Asserted in arrival order: released before the successor's acquired.
    ?assertEqual(released, expect_next(Ref)),
    ?assertEqual({acquired, owner_b}, expect_next(Ref)),
    true = exit(WaiterB, kill),
    true = exit(OwnerA, kill).

%% Re-watching a key from the same process returns a new ref and supersedes the
%% old one, so only the new ref fires.
re_watch_supersedes_the_old_ref(_Config) ->
    Key = {res, rewatch},
    {ok, Ref1} = portunus:watch(?NAME, Key),
    {ok, Ref2} = portunus:watch(?NAME, Key),
    ?assertNotEqual(Ref1, Ref2),
    Owner = start_owner(Key, rewatch_owner),
    expect_event(Ref2, {acquired, rewatch_owner}),
    refute_event(Ref1),
    true = exit(Owner, kill).

%% A watcher's death drops it from the replicated watch set, so the watched
%% key count returns to its starting point. Counts are compared relative to a
%% baseline, since other tests may leave watches mid-cleanup.
watcher_death_drops_the_watch(_Config) ->
    Key = {res, watched_death},
    Before = watched_keys(),
    Ctrl = self(),
    Watcher = spawn(fun() ->
                            {ok, _Ref} = portunus:watch(?NAME, Key),
                            Ctrl ! watching,
                            receive stop -> ok end
                    end),
    receive
        watching -> ok
    after 30000 ->
        ct:fail(watcher_start_timeout)
    end,
    ok = portunus_test_helpers:await_condition(fun() -> watched_keys() > Before end),
    true = exit(Watcher, kill),
    ok = portunus_test_helpers:await_condition(fun() -> watched_keys() =< Before end).

%%----------------------------------------------------------------------
%% Owner process and event helpers
%%----------------------------------------------------------------------

watched_keys() ->
    maps:get(watchers, portunus:status(?NAME), 0).

%% A separate process takes the key, then frees it on request.
start_owner(Key) ->
    start_owner(Key, watched_owner).

start_owner(Key, OwnerTag) ->
    Ctrl = self(),
    Pid = spawn(fun() ->
                        {ok, Lease} = portunus:grant_lease(?NAME, 60000),
                        {ok, Token} = portunus:acquire(?NAME, Key, Lease, OwnerTag),
                        Ctrl ! ready,
                        receive release -> ok end,
                        ok = portunus:release(?NAME, Key, Token),
                        receive stop -> ok end
                end),
    receive
        ready -> Pid
    after 30000 ->
        ct:fail(owner_start_timeout)
    end.

%% A separate process that queues behind the current holder and stays alive to
%% keep its lease, so a release promotes it.
start_waiter(Key, OwnerTag) ->
    Ctrl = self(),
    Pid = spawn(fun() ->
                        {ok, Lease} = portunus:grant_lease(?NAME, 60000),
                        {queued, _} = portunus:acquire_or_join_succession_queue(
                                        ?NAME, Key, Lease, OwnerTag),
                        Ctrl ! queued,
                        receive stop -> ok end
                end),
    receive
        queued -> Pid
    after 30000 ->
        ct:fail(waiter_start_timeout)
    end.

expect_event(Ref, Event) ->
    receive
        {portunus, watch, Ref, Event} -> ok
    after 30000 ->
        ct:fail({missing_event, Event})
    end.

refute_event(Ref) ->
    receive
        {portunus, watch, Ref, Event} -> ct:fail({unexpected_event, Ref, Event})
    after 500 ->
        ok
    end.

%% The next event for `Ref` in arrival order, so a test can assert ordering, not
%% just presence.
expect_next(Ref) ->
    receive
        {portunus, watch, Ref, Event} -> Event
    after 30000 ->
        ct:fail(missing_event)
    end.
