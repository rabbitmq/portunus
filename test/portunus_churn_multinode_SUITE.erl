%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_churn_multinode_SUITE).

%% Seeded random churn over a three-node cluster on a host-style Ra system
%% (the RabbitMQ shape: no recovery strategy, caller-paced recovery, every
%% rejoin path reachable). One designated client node holds a lock under an
%% auto-renewed lease and is never disturbed; each round injects one fault
%% on one of the other two nodes, re-runs every node's bootstrap until the
%% cluster reports three members, and asserts the invariants: exactly one
%% owner for the tracked key, a token that never decreases, and a status
%% answered with quorum. One fault per round keeps quorum by construction,
%% so a round that does not converge is a real bug, not test noise.
%%
%% Deterministic under a fixed seed. Overridable through the environment:
%% `PORTUNUS_CHURN_ROUNDS` (default 15) and `PORTUNUS_CHURN_SEED` (default
%% random); the seed and the action trace are logged for reproduction.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([seeded_churn_preserves_the_invariants/1]).

-define(SYS, portunus_churn_sys).
-define(NAME, portunus_churn_test).
-define(KEY, {res, churn}).
-define(TTL, 6000).

all() ->
    [seeded_churn_preserves_the_invariants].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    Peers = [portunus_ct_cluster:start_node(Config, #{hosted => ?SYS})
             || _ <- lists:seq(1, 3)],
    Nodes = [Node || {_, Node} <- Peers],
    ok = portunus_ct_cluster:mesh(Nodes),
    ok = portunus_ct_cluster:converge_all(?SYS, Nodes, Nodes, ?NAME),
    _ = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    [{cluster, #{peers => Peers, nodes => Nodes}} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

%%----------------------------------------------------------------------
%% The test case
%%----------------------------------------------------------------------

seeded_churn_preserves_the_invariants(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    [ClientNode | Victims] = lists:sort(Nodes),
    Client = portunus_ct_cluster:start_client(ClientNode),
    {ok, Lease} = portunus_ct_cluster:until_quorum(
                    Client, grant_lease, [?NAME, ?TTL, #{auto_renew => true}]),
    {ok, Token0} = portunus_ct_cluster:until_quorum(
                     Client, acquire, [?NAME, ?KEY, Lease, owner_a]),
    Rounds = env_int("PORTUNUS_CHURN_ROUNDS", 15),
    Seed = env_int("PORTUNUS_CHURN_SEED",
                   erlang:phash2({erlang:monotonic_time(), self()},
                                 1 bsl 32)),
    ct:log("churn: seed ~b, ~b rounds, client node ~p, victims ~p",
           [Seed, Rounds, ClientNode, Victims]),
    Rng = rand:seed_s(exsss, Seed),
    churn(Rounds, Rng, Token0, [], Seed,
          #{nodes => Nodes, client => ClientNode, victims => Victims,
            config => Config}).

churn(0, _Rng, _LastToken, Trace, Seed, _Env) ->
    ct:log("churn: seed ~b completed; trace (newest first): ~p",
           [Seed, Trace]),
    ok;
churn(N, Rng0, LastToken, Trace, Seed, Env) ->
    #{nodes := Nodes, client := ClientNode, victims := Victims,
      config := Config} = Env,
    {Victim, Rng1} = pick(Victims, Rng0),
    {Action0, Rng2} = pick([kill_server, restart_system, wipe_registration,
                            delete_replica_dir, kill_leader], Rng1),
    Action = settle_action(Action0, Nodes, ClientNode, Victim),
    ct:log("churn round (remaining ~b): ~p", [N, Action]),
    ok = inject(Action, Config),
    ok = portunus_ct_cluster:converge_all(?SYS, Nodes, Nodes, ?NAME),
    Token = assert_invariants(ClientNode, LastToken),
    churn(N - 1, Rng2, Token, [Action | Trace], Seed, Env).

pick(List, Rng0) ->
    {Ix, Rng1} = rand:uniform_s(length(List), Rng0),
    {lists:nth(Ix, List), Rng1}.

%% The leader kill re-rolls to a plain victim kill when the leader sits on
%% the undisturbed client node.
settle_action(kill_leader, Nodes, ClientNode, Victim) ->
    case portunus_ct_cluster:wait_leader(Nodes, ?NAME) of
        {?NAME, ClientNode} -> {kill_server, Victim};
        {?NAME, LeaderNode} -> {kill_server, LeaderNode}
    end;
settle_action(Action, _Nodes, _ClientNode, Victim) ->
    {Action, Victim}.

%%----------------------------------------------------------------------
%% Fault injection
%%----------------------------------------------------------------------

inject({kill_server, Node}, _Config) ->
    case rpc:call(Node, erlang, whereis, [?NAME]) of
        Pid when is_pid(Pid) ->
            true = rpc:call(Node, erlang, exit, [Pid, kill]),
            ok;
        %% Already down (a prior round's fault landed close): nothing to kill.
        _ ->
            ok
    end;
inject({restart_system, Node}, Config) ->
    portunus_ct_cluster:restart_host_system(Config, Node, ?SYS);
inject({wipe_registration, Node}, Config) ->
    Dir = portunus_ct_cluster:data_dir(Config, Node),
    ok = rpc:call(Node, ra_system, stop, [?SYS]),
    ok = file:delete(filename:join(Dir, "names.dets")),
    ok = rpc:call(Node, portunus_ct_cluster, start_host_system, [?SYS, Dir]),
    ok = rpc:call(Node, portunus, use_system, [?SYS]);
inject({delete_replica_dir, Node}, Config) ->
    %% The stale-registration state: the directory goes, the registration
    %% stays. The rejoin must read it as "no local identity" and evict.
    Dir = portunus_ct_cluster:data_dir(Config, Node),
    UId = rpc:call(Node, ra_directory, uid_of, [?SYS, ?NAME]),
    ok = rpc:call(Node, ra_system, stop, [?SYS]),
    case is_binary(UId) of
        true -> ok = file:del_dir_r(filename:join(Dir, UId));
        false -> ok
    end,
    ok = rpc:call(Node, portunus_ct_cluster, start_host_system, [?SYS, Dir]),
    ok = rpc:call(Node, portunus, use_system, [?SYS]).

%%----------------------------------------------------------------------
%% Invariants
%%----------------------------------------------------------------------

%% The tracked key has exactly its one undisturbed owner, at a token that
%% never decreases (and could only increase if ownership had moved), and
%% the cluster answers a status query with quorum.
assert_invariants(ClientNode, LastToken) ->
    ok = portunus_ct_cluster:await_owner(ClientNode, ?NAME, ?KEY, owner_a),
    {ok, #{owner := owner_a, token := Token}} =
        rpc:call(ClientNode, portunus, owner, [?NAME, ?KEY]),
    true = Token >= LastToken,
    ok = portunus_ct_cluster:wait_until(
           fun() ->
                   case rpc:call(ClientNode, portunus, status, [?NAME]) of
                       #{quorum := true} -> true;
                       _ -> false
                   end
           end),
    Token.

env_int(Var, Default) ->
    case os:getenv(Var) of
        false -> Default;
        Value -> list_to_integer(Value)
    end.
