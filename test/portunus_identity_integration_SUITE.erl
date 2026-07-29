%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_identity_integration_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([add_comparison_matrix/1,
         idempotent_add_keeps_the_running_child/1]).
-export([start_worker/1]).

-define(SYS, portunus_identity_int_sys).
-define(NAME, portunus_identity_int_test).
-define(TTL, 2000).

%% The `sync/2` half (churn-free on matching identities, replacement on a
%% bump) is covered by `portunus_identity_prop_SUITE`, which generalizes
%% the two cases this suite once carried.
all() ->
    [add_comparison_matrix,
     idempotent_add_keeps_the_running_child].

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

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

add_comparison_matrix(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    %% No identities: equal specs are idempotent, different specs are not.
    Bare = bare_spec(mx_bare, id_w_bare),
    ok = portunus_registry:add(Reg, mx_bare, Bare),
    ?assertEqual(ok, portunus_registry:add(Reg, mx_bare, Bare)),
    ?assertEqual({error, {already_added, mx_bare}},
                 portunus_registry:add(Reg, mx_bare,
                                       Bare#{shutdown => 6000})),
    %% Equal identities: idempotent although the args never compare equal.
    ok = portunus_registry:add(Reg, mx_id, spec(mx_id, id_w_id, {v, 1})),
    ?assertEqual(ok,
                 portunus_registry:add(Reg, mx_id, spec(mx_id, id_w_id, {v, 1}))),
    %% Different identities: a changed registration.
    ?assertEqual({error, {already_added, mx_id}},
                 portunus_registry:add(Reg, mx_id, spec(mx_id, id_w_id, {v, 2}))),
    %% Identity on one side only: the structural comparison decides, and
    %% fresh args make it a mismatch.
    ?assertEqual({error, {already_added, mx_id}},
                 portunus_registry:add(Reg, mx_id, bare_spec(mx_id, id_w_id))),
    ok = portunus_registry:stop(Reg).

idempotent_add_keeps_the_running_child(_Config) ->
    {ok, Reg} = portunus_registry:start_link(?NAME, #{ttl_ms => ?TTL}),
    ok = portunus_registry:add(Reg, keep, spec(keep, id_w_keep, {v, 1})),
    ok = portunus_test_helpers:await_condition(
           fun() -> is_pid(whereis(id_w_keep)) end),
    Pid = whereis(id_w_keep),
    ok = portunus_registry:add(Reg, keep, spec(keep, id_w_keep, {v, 1})),
    ?assertEqual(Pid, whereis(id_w_keep)),
    ok = portunus_registry:stop(Reg).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

%% `make_ref/0` is volatile. We intentionally use it in the args guarantees
%% that the structure will hash in an unstable way,
%% imitating a set of parameters with encrypted credentials or
%% something naturally unstable like that.
spec(Id, RegName, Identity) ->
    (bare_spec(Id, RegName))#{identity => Identity}.

bare_spec(Id, RegName) ->
    #{id => Id, start => {?MODULE, start_worker, [{RegName, make_ref()}]},
      restart => transient, shutdown => 5000, type => worker,
      modules => [?MODULE]}.

start_worker({RegName, _Noise}) ->
    {ok, spawn_link(fun() ->
                            register(RegName, self()),
                            receive stop -> ok end
                    end)}.
