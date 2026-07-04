%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_integration_SUITE).

%% End-to-end affinity over a real, distributed Ra cluster of peer nodes (from
%% the shared `portunus_ct_cluster` harness): an election threads its affinity
%% score through acquire, so a pinned node wins succession over its peers, and
%% succession moves on when it leaves.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([pinned_affinity_wins_succession/1,
         hash_affinity_selects_rendezvous_winner/1,
         metric_affinity_highest_bid_wins/1]).

%% Spawned on the peer nodes.
-export([election_holder/4]).

-define(NAME, portunus_affinity_test).

all() ->
    [pinned_affinity_wins_succession,
     hash_affinity_selects_rendezvous_winner,
     metric_affinity_highest_bid_wins].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    [{cluster, portunus_ct_cluster:start(Config, ?NAME, 3)} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

pinned_affinity_wins_succession(Config) ->
    #{nodes := [N1, N2, N3]} = ?config(cluster, Config),
    Key = {election, sched},
    %% Every contender pins the child to N2.
    Pin = {pinned, N2},
    %% N1 starts first and, unopposed, takes the lock.
    H1 = start_election(N1, Key, Pin),
    await_owner(N1, Key, {election, N1}),
    %% N3 (score 0) queues first, then N2 (score 1). Waiting for each to
    %% enqueue before starting the next fixes the arrival order, so N2
    %% winning below can only be the score, not FIFO.
    _H3 = start_election(N3, Key, Pin),
    await_queued(N1, Key, 1),
    H2 = start_election(N2, Key, Pin),
    await_queued(N1, Key, 2),
    %% Dropping N1 promotes N2 ahead of the earlier-queued N3, because its
    %% pinned score outranks FIFO order.
    stop_election(H1),
    await_owner(N1, Key, {election, N2}),
    %% When the pinned node leaves too, succession falls to N3.
    stop_election(H2),
    await_owner(N1, Key, {election, N3}).

hash_affinity_selects_rendezvous_winner(Config) ->
    #{nodes := [N1, N2, N3]} = ?config(cluster, Config),
    Key = {election, hashed},
    Hash = {hash, []},
    %% N1 holds first; N2 and N3 queue behind it.
    H1 = start_election(N1, Key, Hash),
    await_owner(N1, Key, {election, N1}),
    _H3 = start_election(N3, Key, Hash),
    await_queued(N1, Key, 1),
    H2 = start_election(N2, Key, Hash),
    await_queued(N1, Key, 2),
    %% Dropping N1, rendezvous hashing promotes the higher-weight node of
    %% the two waiters, regardless of arrival order.
    stop_election(H1),
    await_owner(N1, Key, {election, rendezvous_winner(Key, [N2, N3])}),
    stop_election(H2).

metric_affinity_highest_bid_wins(Config) ->
    #{nodes := [N1, N2, N3]} = ?config(cluster, Config),
    Key = {election, metered},
    %% A dynamic bid: N2 reports a high local metric, every other node a low
    %% one. The fun runs on each node, so `node/0` names the bidder.
    Bid = {metric, fun () -> bid(node(), N2) end},
    H1 = start_election(N1, Key, Bid),
    await_owner(N1, Key, {election, N1}),
    _H3 = start_election(N3, Key, Bid),
    await_queued(N1, Key, 1),
    H2 = start_election(N2, Key, Bid),
    await_queued(N1, Key, 2),
    %% Dropping N1, the highest bidder wins succession over earlier-queued N3.
    stop_election(H1),
    await_owner(N1, Key, {election, N2}),
    stop_election(H2).

%% The rendezvous (highest-random-weight) winner: each node bids
%% `phash2({Key, node()})`, so the controller can predict the top bid.
rendezvous_winner(Key, Nodes) ->
    {_, Winner} = lists:max([{erlang:phash2({Key, N}), N} || N <- Nodes]),
    Winner.

bid(Node, Node) -> 5;
bid(_Node, _High) -> 1.

%%----------------------------------------------------------------------
%% Election holders on peer nodes
%%----------------------------------------------------------------------

start_election(Node, Key, Affinity) ->
    Ctrl = self(),
    Holder = spawn(Node, ?MODULE, election_holder, [?NAME, Key, Affinity, Ctrl]),
    receive
        {election_ready, Holder} -> Holder
    after 30000 ->
        error({election_start_timeout, Node})
    end.

election_holder(Name, Key, Affinity, Ctrl) ->
    {ok, E} = portunus_election:start_link(Name, Key,
                                           portunus_demo_election, self(),
                                           #{ttl_ms => 30000,
                                             affinity => Affinity}),
    Ctrl ! {election_ready, self()},
    receive stop -> _ = catch portunus_election:stop(E), ok end.

stop_election(Holder) ->
    Holder ! stop,
    ok.

%%----------------------------------------------------------------------
%% Waiting
%%----------------------------------------------------------------------

await_owner(QueryNode, Key, Owner) ->
    portunus_ct_cluster:await_owner(QueryNode, ?NAME, Key, Owner).

%% Only one key is contended per test, so the system-wide waiter count is
%% the count for `Key`.
await_queued(QueryNode, _Key, Count) ->
    portunus_ct_cluster:wait_until(
      fun() ->
              case rpc:call(QueryNode, portunus, status, [?NAME]) of
                  #{waiters := W} -> W >= Count;
                  _ -> false
              end
      end).
