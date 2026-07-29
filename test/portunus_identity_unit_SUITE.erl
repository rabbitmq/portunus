%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_identity_unit_SUITE).

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([identity_is_removed_from_a_plain_spec/1,
         identity_is_removed_from_a_delayed_restart_spec/1,
         a_spec_without_identity_is_untouched/1]).

all() ->
    [identity_is_removed_from_a_plain_spec,
     identity_is_removed_from_a_delayed_restart_spec,
     a_spec_without_identity_is_untouched].

identity_is_removed_from_a_plain_spec(_Config) ->
    Out = portunus_delayed_restart:child_spec(spec(#{identity => {k, 1}})),
    ?assertEqual(spec(#{}), Out),
    ?assertEqual(ok, supervisor:check_childspecs([Out])).

identity_is_removed_from_a_delayed_restart_spec(_Config) ->
    Out = portunus_delayed_restart:child_spec(
            spec(#{identity => {k, 1}, restart => {transient, 5}})),
    ?assertNot(is_map_key(identity, Out)),
    %% The restart rewrite still applies: the start moves to the wrapper.
    ?assertMatch(#{restart := transient,
                   start := {portunus_delayed_restart, start_link, _}}, Out).

a_spec_without_identity_is_untouched(_Config) ->
    ?assertEqual(spec(#{}), portunus_delayed_restart:child_spec(spec(#{}))).

spec(Extra) ->
    maps:merge(#{id => w, start => {erlang, apply, [fun() -> ok end, []]},
                 restart => transient, shutdown => 5000, type => worker,
                 modules => []},
               Extra).
