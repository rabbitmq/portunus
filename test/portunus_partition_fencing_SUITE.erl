%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_partition_fencing_SUITE).

%% The fencing story under real network partitions, extending
%% `portunus_partition_SUITE` (same `inet_tcp_proxy` harness: peers whose
%% distribution can be cut and healed at runtime, controlled over stdio): a
%% holder isolated past its TTL is fenced out by the token the majority
%% mints for its successor, and the token sequence observed by surviving
%% clients never decreases through a leader loss.
%%
%% The proxy is part of the RabbitMQ source tree, not a hex dependency, so
%% the suite finds its ebin via the sibling checkout (or the
%% INET_TCP_PROXY_EBIN environment variable) and skips when it is absent.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([expiry_during_partition_fences_stale_holder/1,
         tokens_observed_monotonic_through_leader_loss/1]).

%% Run on the peer nodes.
-export([holder_loop/0, holder_call/1]).

-define(SYS, portunus).
-define(NAME, portunus_partition_fencing_test).
-define(TICK_MS, 200).
-define(HOLDER, portunus_partition_fencing_holder).
-define(COOKIE, "portunus_proxy_test").
-define(RETRIES, 200).

all() ->
    [expiry_during_partition_fences_stale_holder,
     tokens_observed_monotonic_through_leader_loss].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok ->
            case proxy_ebin() of
                {ok, Ebin} ->
                    [{proxy_ebin, Ebin} | Config];
                error ->
                    {skip, "inet_tcp_proxy ebin not found; set INET_TCP_PROXY_EBIN"}
            end;
        Skip ->
            Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    [{cluster, setup_cluster(Config, 3)} | Config].

end_per_testcase(_TC, Config) ->
    case ?config(cluster, Config) of
        #{peers := Peers} ->
            _ = [catch peer:stop(P) || {P, _} <- Peers],
            ok;
        _ ->
            ok
    end.

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

%% The fencing token doing its one job, under a real partition: the holder
%% is isolated into the minority past its TTL, the majority expires the
%% lease and promotes a waiting contender with a higher token, and after
%% healing the old holder's token opens nothing. The holder renews manually
%% (that is, not at all), never through `auto_renew`: a renewer would exit
%% the holder on the lease loss, and the holder must survive to attempt the
%% fenced calls.
expiry_during_partition_fences_stale_holder(Config) ->
    #{peers := Peers, nodes := Nodes} = ?config(cluster, Config),
    Key = {res, fence},
    {?NAME, LeaderNode} = wait_leader(Peers),
    [HolderNode | _] = Nodes -- [LeaderNode],
    Majority = Nodes -- [HolderNode],
    MajPeers = pairs(Peers, Majority),
    HolderPeer = peer_of(Peers, HolderNode),
    ok = start_holder(HolderPeer),
    {ok, Lease1} = until_quorum(HolderPeer, grant_lease, [?NAME, 3000]),
    {ok, Token1} = until_quorum(HolderPeer, acquire,
                                [?NAME, Key, Lease1, owner_a]),
    %% A contender waits from the majority side.
    ContenderPeer = peer_of(Peers, hd(Majority)),
    ok = start_holder(ContenderPeer),
    {ok, Lease2} = until_quorum(ContenderPeer, grant_lease, [?NAME, 60000]),
    {queued, 1} = until_quorum(ContenderPeer,
                               acquire_or_join_succession_queue,
                               [?NAME, Key, Lease2, owner_b]),
    partition(Peers, [HolderNode], Majority),
    {?NAME, _} = wait_leader(MajPeers),
    %% The majority expires the unrenewed lease and promotes the contender.
    Token2 = await_new_owner(MajPeers, Key, owner_b),
    ?assert(Token2 > Token1),
    heal(Peers, [HolderNode], Majority),
    {?NAME, _} = wait_leader(Peers),
    %% The old holder is fenced: its token releases nothing and its lease
    %% is gone.
    ?assertEqual({error, not_owner},
                 until_quorum(HolderPeer, release, [?NAME, Key, Token1])),
    ok = wait_until(
           fun() ->
                   hcall(HolderPeer, renew_leases, [?NAME, [Lease1]])
                       =:= [{Lease1, {error, lease_expired}}]
           end),
    %% The successor still holds at its higher token.
    await_owner(Peers, Key, owner_b, Token2).

%% Acquire-release cycles from a surviving client while the leader is
%% isolated and a new one elected: the observed token sequence never
%% decreases, and keeps making progress on both sides of the healing.
tokens_observed_monotonic_through_leader_loss(Config) ->
    #{peers := Peers, nodes := Nodes} = ?config(cluster, Config),
    Key = {res, mono},
    {?NAME, OldLeader} = wait_leader(Peers),
    Majority = Nodes -- [OldLeader],
    MajPeers = pairs(Peers, Majority),
    ClientPeer = peer_of(Peers, hd(Majority)),
    ok = start_holder(ClientPeer),
    {ok, Lease} = until_quorum(ClientPeer, grant_lease, [?NAME, 60000]),
    Before = cycle_tokens(ClientPeer, Key, Lease, 5),
    partition(Peers, [OldLeader], Majority),
    {?NAME, NewLeader} = wait_leader(MajPeers),
    ?assertNotEqual(OldLeader, NewLeader),
    During = cycle_tokens(ClientPeer, Key, Lease, 5),
    heal(Peers, [OldLeader], Majority),
    {?NAME, _} = wait_leader(Peers),
    After = cycle_tokens(ClientPeer, Key, Lease, 5),
    Observed = Before ++ During ++ After,
    ?assertEqual(lists:sort(Observed), Observed),
    %% Idempotent retries may repeat a token, but the sequence must have
    %% made real progress through the transition.
    ?assert(length(lists:usort(Observed)) >= 12).

%%----------------------------------------------------------------------
%% Token observation
%%----------------------------------------------------------------------

%% One acquire-release cycle per element: the acquire's token is recorded,
%% the release tolerates `not_held` (a retried release whose first attempt
%% committed).
cycle_tokens(Peer, Key, Lease, N) ->
    lists:map(
      fun(_) ->
              {ok, T} = until_quorum(Peer, acquire, [?NAME, Key, Lease, owner_a]),
              case until_quorum(Peer, release, [?NAME, Key, T]) of
                  ok -> ok;
                  {error, not_held} -> ok
              end,
              T
      end, lists:seq(1, N)).

await_new_owner(Pairs, Key, Owner) ->
    {Peer, _} = hd(Pairs),
    ok = wait_until(
           fun() ->
                   case peer:call(Peer, portunus, owner, [?NAME, Key], 30000) of
                       {ok, #{owner := Owner}} -> true;
                       _ -> false
                   end
           end),
    {ok, #{owner := Owner, token := Token}} =
        peer:call(Peer, portunus, owner, [?NAME, Key], 30000),
    Token.

%%----------------------------------------------------------------------
%% Cluster setup (proxied peers, controlled over stdio), partition control
%% and the holder process: the `portunus_partition_SUITE` harness.
%%----------------------------------------------------------------------

setup_cluster(Config, N) ->
    Ebin = ?config(proxy_ebin, Config),
    PrivDir = ?config(priv_dir, Config),
    Args = ["-proto_dist", "inet_tcp_proxy",
            "-setcookie", ?COOKIE,
            "-kernel", "net_ticktime", "6",
            "-pa", Ebin | code:get_path()],
    Peers = [begin
                 {ok, Peer, Node} =
                     peer:start_link(#{name => peer:random_name("portunus_fence"),
                                       connection => standard_io,
                                       wait_boot => 60000,
                                       args => Args}),
                 {Peer, Node}
             end || _ <- lists:seq(1, N)],
    Nodes = [Node || {_, Node} <- Peers],
    lists:foreach(
      fun({Peer, Node}) ->
              {ok, _} = peer:call(Peer, application, ensure_all_started,
                                  [inet_tcp_proxy_dist]),
              _ = peer:call(Peer, application, load, [portunus]),
              ok = peer:call(Peer, application, set_env,
                             [portunus, tick_interval_ms, ?TICK_MS]),
              DataDir = filename:join([PrivDir, atom_to_list(Node)]),
              ok = peer:call(Peer, portunus, start_system, [?SYS, DataDir])
      end, Peers),
    _ = [pong = peer:call(P, net_adm, ping, [Other])
         || {P, Self} <- Peers, Other <- Nodes, Other =/= Self],
    {P1, _} = hd(Peers),
    {ok, _, _} = peer:call(P1, portunus, start_cluster, [?SYS, ?NAME, Nodes]),
    {?NAME, _} = wait_leader(Peers),
    #{peers => Peers, nodes => Nodes}.

partition(Peers, GroupA, GroupB) ->
    each_cross(Peers, GroupA, GroupB,
               fun(Pa, A, Pb, B) ->
                       ok = peer:call(Pa, inet_tcp_proxy_dist, block, [B]),
                       ok = peer:call(Pb, inet_tcp_proxy_dist, block, [A])
               end).

heal(Peers, GroupA, GroupB) ->
    each_cross(Peers, GroupA, GroupB,
               fun(Pa, A, Pb, B) ->
                       ok = peer:call(Pa, inet_tcp_proxy_dist, allow, [B]),
                       ok = peer:call(Pb, inet_tcp_proxy_dist, allow, [A]),
                       _ = peer:call(Pa, net_kernel, connect_node, [B], 10000)
               end).

each_cross(Peers, GroupA, GroupB, Fun) ->
    _ = [Fun(peer_of(Peers, A), A, peer_of(Peers, B), B)
         || A <- GroupA, B <- GroupB],
    ok.

start_holder(Peer) ->
    ok = peer:call(Peer, ?MODULE, holder_call, [start]).

holder_call(start) ->
    case whereis(?HOLDER) of
        undefined ->
            register(?HOLDER, spawn(fun holder_loop/0)),
            ok;
        _ ->
            ok
    end;
holder_call({F, A}) ->
    Pid = whereis(?HOLDER),
    Pid ! {call, self(), F, A},
    receive
        {Pid, Reply} -> Reply
    after 25000 ->
        {error, holder_timeout}
    end.

holder_loop() ->
    receive
        {call, From, F, A} ->
            From ! {self(), apply(portunus, F, A)},
            holder_loop();
        stop ->
            ok
    end.

until_quorum(Peer, F, A) ->
    until_quorum(Peer, F, A, ?RETRIES).

until_quorum(Peer, F, A, 0) ->
    hcall(Peer, F, A);
until_quorum(Peer, F, A, N) ->
    case hcall(Peer, F, A) of
        {error, no_quorum} -> timer:sleep(100), until_quorum(Peer, F, A, N - 1);
        Reply -> Reply
    end.

hcall(Peer, F, A) ->
    peer:call(Peer, ?MODULE, holder_call, [{F, A}], 30000).

peer_of(Peers, Node) ->
    {Peer, Node} = lists:keyfind(Node, 2, Peers),
    Peer.

pairs(Peers, Nodes) ->
    [lists:keyfind(N, 2, Peers) || N <- Nodes].

wait_leader(Pairs) ->
    wait_leader(Pairs, ?RETRIES).

wait_leader(Pairs, 0) ->
    ct:fail({no_leader, [N || {_, N} <- Pairs]});
wait_leader(Pairs, N) ->
    Nodes = [Node || {_, Node} <- Pairs],
    Views = [peer:call(P, ra_leaderboard, lookup_leader, [?NAME], 10000)
             || {P, _} <- Pairs],
    case lists:usort(Views) of
        [{?NAME, LeaderNode} = Leader] ->
            case lists:member(LeaderNode, Nodes) of
                true -> Leader;
                false -> timer:sleep(100), wait_leader(Pairs, N - 1)
            end;
        _ ->
            timer:sleep(100),
            wait_leader(Pairs, N - 1)
    end.

await_owner(Pairs, Key, Owner, Token) ->
    {Peer, _} = hd(Pairs),
    ok = wait_until(
           fun() ->
                   case peer:call(Peer, portunus, owner, [?NAME, Key], 30000) of
                       {ok, #{owner := Owner, token := Token}} -> true;
                       _ -> false
                   end
           end).

wait_until(Fun) ->
    wait_until(Fun, ?RETRIES).

wait_until(_Fun, 0) ->
    ct:fail(timeout);
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        _ -> timer:sleep(100), wait_until(Fun, N - 1)
    end.

%%----------------------------------------------------------------------
%% Locating the inet_tcp_proxy ebin
%%----------------------------------------------------------------------

proxy_ebin() ->
    Candidates = [os:getenv("INET_TCP_PROXY_EBIN") | sibling_guesses()],
    case [D || D <- Candidates, is_list(D), filelib:is_dir(D)] of
        [Dir | _] -> {ok, Dir};
        [] -> error
    end.

sibling_guesses() ->
    Tail = ["main.git", "deps", "inet_tcp_proxy", "ebin"],
    Bases = [B || B <- [safe_lib_dir(), safe_cwd()], B =/= undefined],
    [filename:join([Ancestor | Tail])
     || Base <- Bases, Ancestor <- ancestors(Base, 8)].

safe_lib_dir() ->
    case code:lib_dir(portunus) of
        {error, _} -> undefined;
        Dir -> Dir
    end.

safe_cwd() ->
    case file:get_cwd() of
        {ok, Dir} -> Dir;
        _ -> undefined
    end.

ancestors(Dir, 0) ->
    [Dir];
ancestors(Dir, N) ->
    case filename:dirname(Dir) of
        Dir -> [Dir];
        Parent -> [Dir | ancestors(Parent, N - 1)]
    end.
