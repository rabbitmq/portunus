%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_registration_recovery_multinode_SUITE).

%% The deadlock a lost registration causes under quorum loss: the restarted
%% node believes it was never a member, its fallback asks a survivor that has
%% no quorum, and quorum needs exactly the replica that will not start. The
%% repair at system start recovers the replica from disk, which itself
%% restores quorum.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([quorum_returns_after_registration_loss/1]).

-define(SYS, portunus).
-define(NAME, portunus_registration_recovery_multinode_test).
-define(TTL, 60000).
-define(SIZE, 3).

all() ->
    [quorum_returns_after_registration_loss].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    Peers = [portunus_ct_cluster:start_node(Config, #{}) || _ <- lists:seq(1, ?SIZE)],
    Nodes = [Node || {_, Node} <- Peers],
    portunus_ct_cluster:mesh(Nodes),
    [{cluster, #{peers => Peers, nodes => Nodes}} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

%% One non-seed loses its registration, the other non-seed's replica is down,
%% so the seed survives with one replica of three: no quorum. The node with the
%% lost registration must recover its replica from its own disk, restoring
%% quorum, rather than ask the quorumless survivor whether it is a member.
quorum_returns_after_registration_loss(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    [Seed, Lost, Down] = lists:sort(Nodes),
    ok = portunus_ct_cluster:converge_all(Nodes, Nodes, ?NAME),
    {?NAME, Leader} = portunus_ct_cluster:wait_leader(Nodes, ?NAME),
    Token = portunus_ct_cluster:place_lock(Leader, ?NAME, {res, hold}),
    ok = wipe_registration(Config, Lost),
    ok = portunus_ct_cluster:stop_ra_server(Down, ?NAME),
    %% Start the system on the wiped node: the repair runs here, and the
    %% `registered` recovery strategy restarts the replica it re-registered.
    ok = rpc:call(Lost, portunus, start_system,
                  [?SYS, portunus_ct_cluster:data_dir(Config, Lost)]),
    %% The repair itself, observed directly on the wiped node.
    ok = portunus_ct_cluster:await_registered(Lost, ?SYS, ?NAME),
    ok = rpc:call(Lost, portunus, join_or_form, [?SYS, ?NAME, Nodes]),
    %% Two of three replicas are up again: a leader must emerge and the
    %% committed lock survive.
    {?NAME, _} = portunus_ct_cluster:wait_leader([Seed, Lost], ?NAME),
    ?assertMatch({ok, #{owner := owner_a, token := Token}},
                 rpc:call(Lost, portunus, owner, [?NAME, {res, hold}])).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% Stop the system and delete the registration table: what a hard kill inside
%% the DETS auto-save window leaves behind.
wipe_registration(Config, Node) ->
    Dir = portunus_ct_cluster:data_dir(Config, Node),
    ok = rpc:call(Node, ra_system, stop, [?SYS]),
    ok = rpc:call(Node, file, delete, [filename:join(Dir, "names.dets")]),
    ok.

