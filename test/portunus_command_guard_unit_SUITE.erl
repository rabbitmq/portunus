%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_command_guard_unit_SUITE).

%% An ill-typed command must never crash `apply/3`: a crash there is a
%% poison pill that re-crashes every replica on log replay. Guarded clauses
%% fall through to the catch-all and reply `{error, unknown_command}` with
%% the state untouched. The client-side guards reject the same inputs at
%% the call site.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([bad_grant_fields_are_rejected/1,
         bad_watch_pid_is_rejected/1,
         bad_acquire_score_is_rejected/1,
         client_guards_reject_bad_ttls/1]).

all() ->
    [bad_grant_fields_are_rejected,
     bad_watch_pid_is_rejected,
     bad_acquire_score_is_rejected,
     client_guards_reject_bad_ttls].

bad_grant_fields_are_rejected(_Config) ->
    S0 = portunus_machine:init(#{}),
    [begin
         {S1, Reply} = apply_cmd(Cmd, 1, S0),
         ?assertEqual({error, unknown_command}, Reply),
         ?assertEqual(S0, S1)
     end || Cmd <- [{grant_lease, undefined, "5000", o, undefined},
                    {grant_lease, undefined, 0, o, undefined},
                    {grant_lease, undefined, -5, o, undefined},
                    {grant_lease, undefined, 5000.0, o, undefined},
                    {grant_lease, undefined, 5000, o, not_a_pid}]],
    ok.

bad_watch_pid_is_rejected(_Config) ->
    S0 = portunus_machine:init(#{}),
    {S1, Reply} = apply_cmd({watch, key, not_a_pid}, 1, S0),
    ?assertEqual({error, unknown_command}, Reply),
    ?assertEqual(S0, S1).

bad_acquire_score_is_rejected(_Config) ->
    S0 = portunus_machine:init(#{}),
    {{ok, l1}, S1} = ok_apply({grant_lease, l1, 5000, o, undefined}, 1, S0),
    {S2, Reply} = apply_cmd({acquire, l1, key, o, undefined, wait, "high"}, 2, S1),
    ?assertEqual({error, unknown_command}, Reply),
    ?assertEqual(S1, S2).

client_guards_reject_bad_ttls(_Config) ->
    ?assertError(function_clause, portunus:grant_lease(name, "5000")),
    ?assertError(function_clause, portunus:grant_lease(name, 0)),
    %% `auto_renew` requires the renewer's floor; a plain grant does not.
    ?assertError(function_clause,
                 portunus:grant_lease(name, 1000, #{auto_renew => true})),
    ?assertError(function_clause, portunus:lock(name, key, 1000)),
    ?assertError(function_clause,
                 portunus:with_lock(name, key, 1000, fun() -> ok end)),
    ?assertError(function_clause,
                 portunus_keepalive:start_link(name, lease, 1000)),
    ?assertError(function_clause,
                 portunus_election:start_link(name, key, mod, args,
                                              #{ttl_ms => 1000})),
    ?assertError(function_clause,
                 portunus_session:open(name, #{ttl_ms => 1000})),
    ?assertError(function_clause,
                 portunus_service:start_link(name, mod, args, #{ttl_ms => 1000})),
    ?assertError(function_clause,
                 portunus_registry:start_link(name, #{ttl_ms => 1000})).

apply_cmd(Cmd, Ix, S) ->
    case portunus_machine:apply(portunus_test_helpers:meta(Ix), Cmd, S) of
        {S1, Reply} -> {S1, Reply};
        {S1, Reply, _Effs} -> {S1, Reply}
    end.

ok_apply(Cmd, Ix, S) ->
    {S1, Reply} = apply_cmd(Cmd, Ix, S),
    {Reply, S1}.
