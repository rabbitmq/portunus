%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_formation_flush_multinode_SUITE).

%% `ra:start_cluster/4` writes a registration on every node it starts a
%% server on, and the sync is a local DETS flush, so a coordinated formation
%% must flush on every member, not only where `start_cluster/3` was called.
%% The cold-copy lookup reads exactly what a hard kill at that instant would
%% have left on disk.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([coordinated_formation_flushes_every_member/1]).

-define(SYS, portunus).
-define(NAME, portunus_formation_flush_multinode_test).
-define(SIZE, 3).

all() ->
    [coordinated_formation_flushes_every_member].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    Peers = [portunus_ct_cluster:start_node(Config, #{})
             || _ <- lists:seq(1, ?SIZE)],
    Nodes = [Node || {_, Node} <- Peers],
    portunus_ct_cluster:mesh(Nodes),
    [{cluster, #{peers => Peers, nodes => Nodes}} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

%% Red without the remote flush: the two nodes that did not run
%% `start_cluster/3` keep their registration in the 500 ms auto-save buffer,
%% and the cold copies taken right after the call come back empty there.
coordinated_formation_flushes_every_member(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    {ok, _, _} = rpc:call(hd(Nodes), portunus, start_cluster,
                          [?SYS, ?NAME, Nodes]),
    [?assertMatch([{?NAME, _}],
                  portunus_ct_cluster:cold_registration_lookup(
                    portunus_ct_cluster:data_dir(Config, Node), ?NAME))
     || Node <- Nodes].
