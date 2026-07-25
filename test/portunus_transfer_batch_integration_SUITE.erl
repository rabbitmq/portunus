%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_transfer_batch_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([batch_moves_all_keys_without_dual_ownership/1,
         batch_with_unreachable_target_moves_the_rest/1]).
%% Run on the peer nodes.
-export([registry_holder/3, member_keys/0, start_worker/0]).

-define(NAME, portunus_transfer_batch_int_test).
-define(REG, portunus_transfer_batch_int_reg).
-define(TTL, 3000).

all() ->
    [batch_moves_all_keys_without_dual_ownership,
     batch_with_unreachable_target_moves_the_rest].

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

%% While the owner moves every key in one `transfer_many/2`, a sampler
%% polling every node must never see a key owned by two nodes at once.
batch_moves_all_keys_without_dual_ownership(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Keys = [{batch, I} || I <- lists:seq(1, 8)],
    [Owner | Rest] = Nodes,
    start_registry(Owner, Keys),
    ok = portunus_test_helpers:await_condition(
           fun() -> owners([Owner], Keys) =:= {Owner, Keys} end, 30000),
    [start_registry(N, Keys) || N <- Rest],
    Target = hd(Rest),
    ok = await_contender(Owner, Keys, Target),
    Sampler = spawn_sampler(Nodes, Keys),
    Results = rpc:call(Owner, portunus_registry, transfer_many,
                       [?REG, [{K, Target} || K <- Keys]]),
    ?assertEqual(lists:sort([{K, ok} || K <- Keys]), lists:sort(Results)),
    ok = portunus_test_helpers:await_condition(
           fun() -> node_owns(Target, Keys) end, 30000),
    [] = stop_sampler(Sampler).

batch_with_unreachable_target_moves_the_rest(Config) ->
    #{nodes := Nodes} = ?config(cluster, Config),
    Keys = [{part, I} || I <- lists:seq(1, 4)],
    [Owner, Target, Bystander] = Nodes,
    %% No registry on `Bystander`, so it never contends for any key.
    start_registry(Owner, Keys),
    ok = portunus_test_helpers:await_condition(
           fun() -> owners([Owner], Keys) =:= {Owner, Keys} end, 30000),
    start_registry(Target, Keys),
    ok = await_contender(Owner, Keys, Target),
    [Stay | Move] = Keys,
    KeyTargets = [{Stay, Bystander} | [{K, Target} || K <- Move]],
    Results = rpc:call(Owner, portunus_registry, transfer_many, [?REG, KeyTargets]),
    {error, {no_contender, Bystander}} = proplists:get_value(Stay, Results),
    [ok = proplists:get_value(K, Results) || K <- Move],
    ok = portunus_test_helpers:await_condition(
           fun() -> node_owns(Target, Move) end, 30000),
    %% The unmoved key stays with the original owner, its child still running.
    true = node_owns(Owner, [Stay]).

%%----------------------------------------------------------------------
%% Registry holders and child on the peer nodes
%%----------------------------------------------------------------------

start_registry(Node, Keys) ->
    Ctrl = self(),
    _ = spawn(Node, ?MODULE, registry_holder, [Keys, ?NAME, Ctrl]),
    receive
        {registry_ready, Node} -> ok
    after 30000 ->
        error({registry_start_timeout, Node})
    end.

registry_holder(Keys, Name, Ctrl) ->
    {ok, Reg} = portunus_registry:start_link(Name, #{ttl_ms => ?TTL}),
    register(?REG, Reg),
    [ok = portunus_registry:add(Reg, K, spec(K)) || K <- Keys],
    Ctrl ! {registry_ready, node()},
    receive stop -> ok end.

spec(Key) ->
    #{id => Key, start => {?MODULE, start_worker, []},
      restart => transient, shutdown => 5000, type => worker,
      modules => [?MODULE]}.

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

%% The keys `Node` currently owns, among `Keys`.
owned(Node, Keys) ->
    [K || K <- Keys, lists:member(K, peer_member_keys(Node))].

node_owns(Node, Keys) ->
    lists:sort(owned(Node, Keys)) =:= lists:sort(Keys).

%% Every key mapped to the single node that owns it, restricted to `Nodes`;
%% used only where each key is expected to have exactly one owner.
owners([Node], Keys) ->
    {Node, lists:sort(owned(Node, Keys))}.

peer_member_keys(Node) ->
    case rpc:call(Node, ?MODULE, member_keys, []) of
        Keys when is_list(Keys) -> Keys;
        _ -> []
    end.

%% Wait until `Target` is a live contender for every key as seen from
%% `ReadNode`'s replica, which is the node that runs the transfer and so the
%% one whose (possibly lagging) local read gates prepare's pre-check.
await_contender(ReadNode, Keys, Target) ->
    portunus_test_helpers:await_condition(
      fun() ->
              lists:all(
                fun(K) ->
                        case rpc:call(ReadNode, portunus, contenders,
                                      [?NAME, {?NAME, K}]) of
                            {ok, Owners} -> lists:member(Target, Owners);
                            _ -> false
                        end
                end, Keys)
      end, 30000).

%% A background poller recording any key it ever sees owned by more than one
%% node.
spawn_sampler(Nodes, Keys) ->
    Ctrl = self(),
    spawn(fun() -> sample_loop(Nodes, Keys, Ctrl, []) end).

sample_loop(Nodes, Keys, Ctrl, Seen) ->
    receive
        {stop, From} -> From ! {violations, self(), lists:usort(Seen)}
    after 15 ->
        Dual = [K || K <- Keys,
                     length([N || N <- Nodes, lists:member(K, peer_member_keys(N))]) > 1],
        sample_loop(Nodes, Keys, Ctrl, Dual ++ Seen)
    end.

stop_sampler(Sampler) ->
    Sampler ! {stop, self()},
    receive {violations, Sampler, Seen} -> Seen
    after 30000 -> error(sampler_timeout)
    end.
