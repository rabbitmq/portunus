%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_system_restart_prop_SUITE).

%% A host's bootstrap retry loop calls `start_system/2` and `restart_server/2`
%% repeatedly on a node that is already a healthy member. Those calls must be
%% idempotent: they never disturb a held lock or its fencing token.

-include_lib("proper/include/proper.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         redundant_bootstrap_is_idempotent/1]).

-define(SYS, portunus_restart_prop_sys).
-define(NAME, portunus_restart_prop_test).
-define(KEY, {res, prop}).

all() ->
    [redundant_bootstrap_is_idempotent].

init_per_suite(Config) ->
    application:set_env(portunus, tick_interval_ms, 200),
    Dir = filename:join(proplists:get_value(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ok = portunus:start_system(?SYS, Dir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    %% A long-lived holder keeps the lease alive, since the lease is tied to its
    %% pid and a dead holder would release the lock.
    Ctrl = self(),
    Holder = spawn(fun() -> hold_lock(Ctrl) end),
    Token = receive {token, T} -> T after 10000 -> error(no_token) end,
    [{ra_dir, Dir}, {token, Token}, {holder, Holder} | Config].

end_per_suite(Config) ->
    case proplists:get_value(holder, Config) of
        Holder when is_pid(Holder) -> Holder ! stop;
        _ -> ok
    end,
    catch ra:stop_server(?SYS, {?NAME, node()}),
    catch ra_system:stop(?SYS),
    ok.

hold_lock(Ctrl) ->
    %% A long TTL so the lock cannot expire during the run.
    {ok, Lease} = portunus:grant_lease(?NAME, 600000),
    {ok, Token} = portunus:acquire(?NAME, ?KEY, Lease, owner_a),
    Ctrl ! {token, Token},
    receive stop -> ok end.

redundant_bootstrap_is_idempotent(Config) ->
    Dir = proplists:get_value(ra_dir, Config),
    Token = proplists:get_value(token, Config),
    true = portunus_test_helpers:quickcheck(
             fun() -> prop_redundant_bootstrap_preserves_lock(Dir, Token) end, 100).

%% However many times the bootstrap calls run, the lock stays held by the same
%% owner with the same token.
prop_redundant_bootstrap_preserves_lock(Dir, Token) ->
    ?FORALL(N, choose(0, 6),
            begin
                lists:foreach(
                  fun(_) ->
                          ok = portunus:start_system(?SYS, Dir),
                          ok = portunus:restart_server(?SYS, ?NAME)
                  end, lists:seq(1, N)),
                case portunus:owner(?NAME, ?KEY) of
                    {ok, #{owner := owner_a, token := Token}} -> true;
                    _ -> false
                end
            end).
