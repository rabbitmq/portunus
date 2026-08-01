%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_joiner_multinode_SUITE).

%% Joiners across real peer nodes: independent nodes converge to one
%% cluster with no host trigger at all, a stopped replica is repaired, and
%% the periodic re-check alone merges solo clusters after the candidate
%% set widens over existing connections (the shape a lost host trigger
%% takes in a real host).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([independent_joiners_converge_and_repair/1,
         backstop_alone_merges_widened_candidates/1]).
%% Runs on the peer nodes, so it must be exported.
-export([joiner_holder/1]).

-define(SYS, portunus).
-define(RECHECK_MS, 500).

all() ->
    [independent_joiners_converge_and_repair,
     backstop_alone_merges_widened_candidates].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

independent_joiners_converge_and_repair(Config) ->
    Name = portunus_joiner_mn_converge,
    {Peers, Nodes} = start_peers(Config, 3),
    [put_candidates(N, Name, Nodes) || N <- Nodes],
    _ = [start_joiner(N, Name) || N <- Nodes],
    ok = portunus_ct_cluster:wait_until(
           fun() -> portunus_ct_cluster:member_count(Nodes, Name) =:= 3 end),
    %% A stopped replica stays on the seed's member list, so only the
    %% joiner's local `is_member/1` check can notice. The backstop
    %% repairs it with no trigger.
    Victim = lists:last(Nodes),
    ok = portunus_ct_cluster:stop_ra_server(Victim, Name),
    ok = portunus_ct_cluster:wait_until(
           fun() -> rpc:call(Victim, portunus, is_member, [Name]) =:= true end),
    ?assertEqual(3, portunus_ct_cluster:member_count(Nodes, Name)),
    portunus_ct_cluster:stop(#{peers => Peers}).

backstop_alone_merges_widened_candidates(Config) ->
    Name = portunus_joiner_mn_backstop,
    {Peers, Nodes} = start_peers(Config, 3),
    %% Each node first sees only itself and forms a single-member cluster.
    [put_candidates(N, Name, [N]) || N <- Nodes],
    _ = [start_joiner(N, Name) || N <- Nodes],
    ok = portunus_ct_cluster:wait_until(
           fun() ->
                   lists:all(fun(N) ->
                                     rpc:call(N, portunus, is_member, [Name])
                                         =:= true
                             end, Nodes)
           end),
    %% The candidate set widens over the existing connections, with no
    %% `recheck/1` call anywhere: the periodic re-check alone must merge
    %% the three solo clusters.
    [put_candidates(N, Name, Nodes) || N <- Nodes],
    ok = portunus_ct_cluster:wait_until(
           fun() -> portunus_ct_cluster:member_count(Nodes, Name) =:= 3 end),
    portunus_ct_cluster:stop(#{peers => Peers}).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

start_peers(Config, N) ->
    Peers = [portunus_ct_cluster:start_node(Config, #{})
             || _ <- lists:seq(1, N)],
    Nodes = [Node || {_, Node} <- Peers],
    portunus_ct_cluster:mesh(Nodes),
    {Peers, Nodes}.

put_candidates(Node, Name, Candidates) ->
    ok = rpc:call(Node, persistent_term, put,
                  [{?MODULE, Name}, Candidates]).

%% The joiner is linked to its starter, so it needs a process on the peer
%% that outlives this call. The holder is that process.
start_joiner(Node, Name) ->
    erlang:spawn(Node, ?MODULE, joiner_holder,
                 [#{system => ?SYS, name => Name,
                    candidates => fun() ->
                                          persistent_term:get({?MODULE, Name})
                                  end,
                    recheck_interval_ms => ?RECHECK_MS}]).

joiner_holder(Opts) ->
    {ok, _} = portunus_joiner:start_link(Opts),
    receive stop -> ok end.
