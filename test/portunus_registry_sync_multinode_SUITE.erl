%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_registry_sync_multinode_SUITE).

%% The generic reproduction of the add-only reconcile defect (`019 §12`, the
%% first consumer's bug): a node that missed a delete keeps contending for
%% the deleted child, and once it is the sole contender it runs a worker for
%% something that no longer exists. `sync/2` closes it: the node's next
%% reconcile pass carries the current set, and the absent id is removed.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([deleted_child_is_not_resurrected/1]).
%% Run on the peer nodes.
-export([registry_holder/2, start_worker/0]).

-define(NAME, portunus_registry_sync_multinode_test).
-define(REG, portunus_registry_sync_multinode_reg).
-define(WORKER, portunus_registry_sync_multinode_worker).
-define(TTL, 3000).

all() ->
    [deleted_child_is_not_resurrected].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok -> Config;
        Skip -> Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    [{cluster, portunus_ct_cluster:start(Config, ?NAME, 2)} | Config].

end_per_testcase(_TC, Config) ->
    portunus_ct_cluster:stop(?config(cluster, Config)).

deleted_child_is_not_resurrected(Config) ->
    #{nodes := [A, B]} = ?config(cluster, Config),
    [start_registry(N) || N <- [A, B]],
    Spec = worker_spec(),
    ok = rpc:call(A, portunus_registry, sync, [?REG, [Spec]]),
    ok = rpc:call(B, portunus_registry, sync, [?REG, [Spec]]),
    ok = portunus_ct_cluster:wait_until(
           fun() -> length(worker_nodes([A, B])) =:= 1 end),
    %% The child is deleted, but the fan-out reaches only A: B keeps its
    %% registration, exactly what a node offline at delete time does.
    ok = rpc:call(A, portunus_registry, sync, [?REG, []]),
    %% B is now the sole contender and (still or soon) runs the worker: the
    %% resurrection state. With an add-only reconcile it would stay forever.
    ok = portunus_ct_cluster:wait_until(
           fun() -> worker_nodes([A, B]) =:= [B] end),
    %% B's next reconcile pass carries the current truth: no such child.
    ok = rpc:call(B, portunus_registry, sync, [?REG, []]),
    ok = portunus_ct_cluster:wait_until(
           fun() -> worker_nodes([A, B]) =:= [] end),
    ?assertEqual([], rpc:call(A, portunus_registry, keys, [?REG])),
    ?assertEqual([], rpc:call(B, portunus_registry, keys, [?REG])),
    %% It stays gone past the election-restart backoff window.
    timer:sleep(1500),
    ?assertEqual([], worker_nodes([A, B])).

%%----------------------------------------------------------------------
%% Registry holders and the worker on the peer nodes
%%----------------------------------------------------------------------

worker_spec() ->
    #{id => x, start => {?MODULE, start_worker, []},
      restart => transient, shutdown => 5000, type => worker,
      modules => [?MODULE]}.

start_worker() ->
    {ok, spawn_link(fun() ->
                            register(?WORKER, self()),
                            receive stop -> ok end
                    end)}.

worker_nodes(Nodes) ->
    [N || N <- Nodes, is_pid(rpc:call(N, erlang, whereis, [?WORKER]))].

start_registry(Node) ->
    Ctrl = self(),
    _ = spawn(Node, ?MODULE, registry_holder, [?NAME, Ctrl]),
    receive
        {registry_ready, Node} -> ok
    after 30000 ->
        error({registry_start_timeout, Node})
    end.

registry_holder(Name, Ctrl) ->
    {ok, Reg} = portunus_registry:start_link(Name, #{ttl_ms => ?TTL}),
    register(?REG, Reg),
    Ctrl ! {registry_ready, node()},
    receive stop -> ok end.
