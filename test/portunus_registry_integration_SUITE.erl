%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_registry_integration_SUITE).

%% A registry child over a real cluster of peer nodes: the same key is
%% registered on every node, exactly one node runs the child, and when that
%% node dies the child moves to a survivor once the dead owner's lease expires.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([registry_child_moves_to_survivor/1,
         registry_transfer_hands_child_to_named_peer/1]).
%% Run on the peer nodes.
-export([registry_holder/3, member_keys/0, start_worker/0]).

-define(NAME, portunus_registry_int_test).
-define(REG, portunus_registry_int_reg).
-define(TTL, 3000).

all() ->
    [registry_child_moves_to_survivor,
     registry_transfer_hands_child_to_named_peer].

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

registry_child_moves_to_survivor(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Key = {svc, owner_death},
    [start_registry(N, Key) || N <- Nodes],
    %% Exactly one node owns the child.
    ok = portunus_test_helpers:await_condition(
           fun() -> length(owners(Nodes, Key)) =:= 1 end, 30000),
    [Owner] = owners(Nodes, Key),
    Survivors = Nodes -- [Owner],
    %% Killing the owner node moves the child to a survivor once the dead
    %% owner's lease expires (a lost node is unreachable, so release waits for
    %% the TTL rather than a clean down).
    stop_node(Owner, Config),
    ok = portunus_test_helpers:await_condition(
           fun() -> length(owners(Survivors, Key)) =:= 1 end, ?TTL + 30000).

%% The current owner hands the child to a named peer with a targeted transfer:
%% the child moves there and stops on the former owner, and a transfer from the
%% former owner (now a standby) is refused.
registry_transfer_hands_child_to_named_peer(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Key = {svc, transfer},
    [start_registry(N, Key) || N <- Nodes],
    ok = portunus_test_helpers:await_condition(
           fun() -> length(owners(Nodes, Key)) =:= 1 end, 30000),
    [Owner] = owners(Nodes, Key),
    Target = hd(Nodes -- [Owner]),
    %% Retry tolerates a lagging local replica not yet showing the target as a
    %% contender; the transfer itself moves ownership on the first `ok`.
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   rpc:call(Owner, portunus_registry, transfer,
                            [?REG, Key, Target]) =:= ok
           end, 30000),
    ok = portunus_test_helpers:await_condition(
           fun() -> owners(Nodes, Key) =:= [Target] end, 30000),
    %% The former owner is a standby now, so a transfer from it is refused.
    {error, not_owner} =
        rpc:call(Owner, portunus_registry, transfer, [?REG, Key, Owner]).

%%----------------------------------------------------------------------
%% Registry holders and child on the peer nodes
%%----------------------------------------------------------------------

start_registry(Node, Key) ->
    Ctrl = self(),
    _ = spawn(Node, ?MODULE, registry_holder, [Key, ?NAME, Ctrl]),
    receive
        {registry_ready, Node} -> ok
    after 30000 ->
        error({registry_start_timeout, Node})
    end.

registry_holder(Key, Name, Ctrl) ->
    {ok, Reg} = portunus_registry:start_link(Name, #{ttl_ms => ?TTL}),
    register(?REG, Reg),
    Spec = #{id => Key, start => {?MODULE, start_worker, []},
             restart => transient, shutdown => 5000, type => worker,
             modules => [?MODULE]},
    ok = portunus_registry:add(Reg, Key, Spec),
    Ctrl ! {registry_ready, node()},
    receive stop -> ok end.

%% Reported by each node so the controller can see where the child runs.
member_keys() ->
    case whereis(?REG) of
        undefined -> [];
        Reg -> portunus_registry:owned_keys(Reg)
    end.

start_worker() ->
    {ok, spawn_link(fun() -> timer:sleep(infinity) end)}.

%%----------------------------------------------------------------------
%% Controller-side helpers
%%----------------------------------------------------------------------

owners(Nodes, Key) ->
    [N || N <- Nodes, lists:member(Key, peer_member_keys(N))].

peer_member_keys(Node) ->
    case rpc:call(Node, ?MODULE, member_keys, []) of
        Keys when is_list(Keys) -> Keys;
        _ -> []
    end.

stop_node(Node, Config) ->
    #{peers := Peers} = ?config(cluster, Config),
    {Peer, Node} = lists:keyfind(Node, 2, Peers),
    ok = peer:stop(Peer).
