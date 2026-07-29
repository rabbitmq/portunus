%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_identity_prop_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([sync_converges_on_identities/1]).
-export([start_worker/1]).

-define(SYS, portunus_identity_prop_sys).
-define(NAME, portunus_identity_prop_test).
-define(TTL, 2000).
-define(KEYS, [pk1, pk2, pk3]).

all() ->
    [sync_converges_on_identities].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

sync_converges_on_identities(_Config) ->
    true = portunus_test_helpers:quickcheck(fun prop_sync_converges/0, 15).

%% Each step is a desired set: a subset of the keys, each at an identity
%% version the generator sometimes bumps.
prop_sync_converges() ->
    ?FORALL(Steps, non_empty(proper_types:list(step())),
            begin
                {ok, Reg} = portunus_registry:start_link(?NAME,
                                                         #{ttl_ms => ?TTL}),
                try
                    run(Reg, Steps, #{})
                after
                    catch portunus_registry:stop(Reg)
                end
            end).

step() ->
    ?LET(Keys, sublist_of(?KEYS),
         [{K, proper_types:range(1, 3)} || K <- Keys]).

sublist_of(Keys) ->
    ?LET(Flags, proper_types:vector(length(Keys), proper_types:boolean()),
         [K || {K, true} <- lists:zip(Keys, Flags)]).

run(_Reg, [], _Prev) ->
    true;
run(Reg, [Desired | Rest], Prev) ->
    ok = portunus_registry:sync(Reg, [spec(K, V) || {K, V} <- Desired]),
    Wanted = lists:sort([K || {K, _} <- Desired]),
    %% A bumped key replaces its child, so settling means the wanted names
    %% are registered and none of them still runs a to-be-replaced pid.
    Stale = [{K, Pid} || {K, V} <- Desired,
                         {PrevV, Pid} <- [maps:get(K, Prev, {V, none})],
                         PrevV =/= V],
    ok = portunus_test_helpers:await_condition(
           fun() ->
                   registered_workers() =:= Wanted
                       andalso lists:all(
                                 fun({K, Pid}) ->
                                         whereis(worker_name(K)) =/= Pid
                                 end, Stale)
           end, 15000),
    KeysOk = lists:sort(portunus_registry:keys(Reg)) =:= Wanted,
    Pids = maps:from_list([{K, whereis(worker_name(K))} || K <- Wanted]),
    Ok = KeysOk andalso lists:all(
           fun({K, V}) ->
                   case maps:get(K, Prev, undefined) of
                       {V, Pid} ->
                           %% Same identity version: the child survived.
                           maps:get(K, Pids) =:= Pid;
                       {_Bumped, _Pid} ->
                           %% The stale-pid await above proved replacement.
                           true;
                       undefined ->
                           true
                   end
           end, Desired),
    Now = maps:from_list([{K, {V, maps:get(K, Pids)}} || {K, V} <- Desired]),
    Ok andalso run(Reg, Rest, Now).

registered_workers() ->
    lists:sort([K || K <- ?KEYS, is_pid(whereis(worker_name(K)))]).

worker_name(K) ->
    list_to_atom("idp_w_" ++ atom_to_list(K)).

spec(Key, Vsn) ->
    #{id => Key,
      start => {?MODULE, start_worker, [{worker_name(Key), make_ref()}]},
      restart => transient, shutdown => 5000, type => worker,
      modules => [?MODULE],
      identity => {Key, Vsn}}.

start_worker({RegName, _Noise}) ->
    {ok, spawn_link(fun() ->
                            register(RegName, self()),
                            receive stop -> ok end
                    end)}.
