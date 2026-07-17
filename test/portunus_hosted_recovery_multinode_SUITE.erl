%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_hosted_recovery_multinode_SUITE).

%% Node recovery on a host-owned Ra system (no `server_recovery_strategy`, so
%% every replica comes back through the caller's bootstrap), and the rejoin of
%% a member whose local identity was lost: the cluster still lists it, the
%% node must evict the remembered member and rejoin as a new one, and it must
%% touch nothing local while the cluster cannot answer. Each case builds its
%% own cluster: a case that stops systems and servers would leak that state
%% into a shared one.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([node_restart_rejoins_on_hosted_system/1,
         registered_but_stopped_member_is_restarted/1,
         remembered_member_with_lost_registration_rejoins/1,
         no_quorum_leaves_the_lost_node_untouched/1]).

-define(SYS, portunus_hosted_multi_sys).
-define(NAME, portunus_hosted_multi_test).
-define(KEY, {res, hosted_multi}).

all() ->
    [node_restart_rejoins_on_hosted_system,
     registered_but_stopped_member_is_restarted,
     remembered_member_with_lost_registration_rejoins,
     no_quorum_leaves_the_lost_node_untouched].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        {skip, _} = Skip -> Skip
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

nodes_sorted(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    lists:sort(Nodes).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

node_restart_rejoins_on_hosted_system(Config) ->
    [Seed, _, Victim] = Nodes = nodes_sorted(Config),
    Token = portunus_ct_cluster:place_lock(Seed, ?NAME, ?KEY),
    ok = portunus_ct_cluster:restart_host_system(Config, Victim, ?SYS),
    %% No recovery strategy: the replica stays down until the bootstrap runs.
    ?assertEqual(undefined, rpc:call(Victim, erlang, whereis, [?NAME])),
    ok = bootstrap(Victim, Nodes),
    ?assertEqual(3, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    portunus_ct_cluster:await_owner(Seed, ?NAME, ?KEY, owner_a, Token).

registered_but_stopped_member_is_restarted(Config) ->
    [Seed, _, Victim] = nodes_sorted(Config),
    Token = portunus_ct_cluster:place_lock(Seed, ?NAME, ?KEY),
    ok = portunus_ct_cluster:stop_ra_server(Victim, ?SYS, ?NAME),
    %% Red before 025: a listed member got a bare `ok` and stayed down.
    ok = rpc:call(Victim, portunus, join_cluster, [?SYS, ?NAME, Seed]),
    Pid = rpc:call(Victim, erlang, whereis, [?NAME]),
    ?assert(is_pid(Pid)),
    %% A listed member whose server already runs is left alone.
    ok = rpc:call(Victim, portunus, join_cluster, [?SYS, ?NAME, Seed]),
    ?assertEqual(Pid, rpc:call(Victim, erlang, whereis, [?NAME])),
    portunus_ct_cluster:wait_until(
      fun() -> rpc:call(Victim, portunus, is_member, [?NAME]) end),
    portunus_ct_cluster:await_owner(Seed, ?NAME, ?KEY, owner_a, Token).

remembered_member_with_lost_registration_rejoins(Config) ->
    [Seed, _, Victim] = Nodes = nodes_sorted(Config),
    Token = portunus_ct_cluster:place_lock(Seed, ?NAME, ?KEY),
    ok = wipe_registration(Config, Victim),
    %% Red without the evict: the seed keeps listing the victim, join keeps
    %% answering `ok`, and no server ever starts.
    ok = bootstrap(Victim, Nodes),
    ?assertEqual(3, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    portunus_ct_cluster:await_owner(Seed, ?NAME, ?KEY, owner_a, Token),
    %% The rejoin's own registration was flushed, not left to the auto-save.
    ?assertMatch([{?NAME, _}], cold_copy_lookup(Config, Victim)).

no_quorum_leaves_the_lost_node_untouched(Config) ->
    [Seed, Mid, Victim] = Nodes = nodes_sorted(Config),
    Token = portunus_ct_cluster:place_lock(Seed, ?NAME, ?KEY),
    ok = wipe_registration(Config, Victim),
    ok = portunus_ct_cluster:stop_ra_server(Seed, ?SYS, ?NAME),
    ok = portunus_ct_cluster:stop_ra_server(Mid, ?SYS, ?NAME),
    Digest = replica_digest(Config, Victim),
    %% One of three replicas left, on the node that cannot start it: the seed
    %% read fails before any destructive step and the pass errors, retryable.
    ?assertMatch({error, _},
                 rpc:call(Victim, portunus, join_or_form,
                          [?SYS, ?NAME, Nodes])),
    ?assertEqual(Digest, replica_digest(Config, Victim)),
    ok = rpc:call(Seed, ra, restart_server, [?SYS, {?NAME, Seed}]),
    ok = rpc:call(Mid, ra, restart_server, [?SYS, {?NAME, Mid}]),
    _ = portunus_ct_cluster:wait_leader([Seed, Mid], ?NAME),
    %% No removal was appended while the cluster could not answer: the
    %% survivors still list the victim's remembered identity.
    {Members, _} = portunus_ct_cluster:cluster_info([Seed, Mid], ?NAME),
    ?assert(lists:member({?NAME, Victim}, Members)),
    ok = bootstrap(Victim, Nodes),
    ?assertEqual(3, portunus_ct_cluster:member_count(Nodes, ?NAME)),
    portunus_ct_cluster:await_owner(Seed, ?NAME, ?KEY, owner_a, Token).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% The consumer's bootstrap loop: `join_or_form/3` until this node is a member.
bootstrap(Node, Members) ->
    portunus_ct_cluster:wait_until(
      fun() ->
              _ = rpc:call(Node, portunus, join_or_form, [?SYS, ?NAME, Members]),
              rpc:call(Node, portunus, is_member, [?NAME]) =:= true
      end).

%% What a hard kill inside the DETS auto-save window leaves: an intact replica
%% directory next to no registration.
wipe_registration(Config, Node) ->
    ok = rpc:call(Node, ra_system, stop, [?SYS]),
    ok = file:delete(filename:join(data_dir(Config, Node), "names.dets")),
    ok = rpc:call(Node, portunus_ct_cluster, start_host_system,
                  [?SYS, data_dir(Config, Node)]),
    ok = rpc:call(Node, portunus, use_system, [?SYS]).

data_dir(Config, Node) ->
    portunus_ct_cluster:data_dir(Config, Node).

cold_copy_lookup(Config, Node) ->
    portunus_ct_cluster:cold_registration_lookup(data_dir(Config, Node), ?NAME).

%% Content digest of the node's replica directories (not the whole data dir:
%% the running system rewrites `names.dets` on its own).
replica_digest(Config, Node) ->
    Dir = data_dir(Config, Node),
    Subs = [S || S <- filelib:wildcard(filename:join(Dir, "*")),
                 filelib:is_dir(S),
                 filelib:is_regular(filename:join(S, "config"))],
    lists:sort([{F, file_md5(F)}
                || S <- Subs,
                   F <- filelib:wildcard(filename:join(S, "**")),
                   filelib:is_regular(F)]).

file_md5(F) ->
    {ok, B} = file:read_file(F),
    erlang:md5(B).
