%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_pipeline_unit_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([applied_reply_is_returned/1,
         stale_notifications_are_skipped/1,
         rejections_are_followed_to_the_named_leader/1,
         rejection_without_a_leader_is_no_quorum/1,
         rejection_ping_pong_is_bounded_by_the_deadline/1,
         silence_runs_into_the_deadline/1]).

-define(NAME, pipeline_unit_test).

all() ->
    [applied_reply_is_returned,
     stale_notifications_are_skipped,
     rejections_are_followed_to_the_named_leader,
     rejection_without_a_leader_is_no_quorum,
     rejection_ping_pong_is_bounded_by_the_deadline,
     silence_runs_into_the_deadline].

init_per_testcase(_Testcase, Config) ->
    meck:new(ra, [passthrough, no_link]),
    meck:new(ra_leaderboard, [passthrough, no_link]),
    meck:expect(ra_leaderboard, lookup_leader, fun(_) -> undefined end),
    Config.

end_per_testcase(_Testcase, Config) ->
    application:unset_env(portunus, pipeline_command_timeout),
    meck:unload(ra_leaderboard),
    meck:unload(ra),
    Config.

%%
%% Test cases
%%

applied_reply_is_returned(_Config) ->
    meck:expect(ra, pipeline_command,
                fun(Server, _Cmd, Corr, low) ->
                        self() ! {ra_event, Server, {applied, [{Corr, {ok, tok1}}]}},
                        ok
                end),
    ?assertEqual({ok, tok1}, portunus:acquire(?NAME, key1, lease1, node())).

%% Notifications correlated to an earlier, abandoned wait must not be taken
%% for this command's reply.
stale_notifications_are_skipped(_Config) ->
    Stale = make_ref(),
    meck:expect(ra, pipeline_command,
                fun(Server, _Cmd, Corr, low) ->
                        self() ! {ra_event, Server, {applied, [{Stale, {ok, wrong}}]}},
                        self() ! {ra_event, Server, {rejected, {not_leader, undefined, Stale}}},
                        self() ! {ra_event, Server, {applied, [{Corr, {ok, tok2}}]}},
                        ok
                end),
    ?assertEqual({ok, tok2}, portunus:acquire(?NAME, key2, lease2, node())).

%% Leadership can move more than once during a rolling restart, so each
%% rejection is followed to the leader it names.
rejections_are_followed_to_the_named_leader(_Config) ->
    Local = {?NAME, node()},
    L2 = {?NAME, 'second@nohost'},
    L3 = {?NAME, 'third@nohost'},
    meck:expect(ra, pipeline_command,
                fun(S, _Cmd, Corr, low) when S =:= Local ->
                        self() ! {ra_event, S, {rejected, {not_leader, L2, Corr}}},
                        ok;
                   (S, _Cmd, Corr, low) when S =:= L2 ->
                        self() ! {ra_event, S, {rejected, {not_leader, L3, Corr}}},
                        ok;
                   (S, _Cmd, Corr, low) when S =:= L3 ->
                        self() ! {ra_event, S, {applied, [{Corr, {ok, tok3}}]}},
                        ok
                end),
    ?assertEqual({ok, tok3}, portunus:acquire(?NAME, key3, lease3, node())),
    ?assertEqual(3, meck:num_calls(ra, pipeline_command, '_')).

rejection_without_a_leader_is_no_quorum(_Config) ->
    meck:expect(ra, pipeline_command,
                fun(Server, _Cmd, Corr, low) ->
                        self() ! {ra_event, Server, {rejected, {not_leader, undefined, Corr}}},
                        ok
                end),
    ?assertEqual({error, no_quorum}, portunus:acquire(?NAME, key4, lease4, node())).

%% Two servers each naming the other as leader must not chase forever.
rejection_ping_pong_is_bounded_by_the_deadline(_Config) ->
    application:set_env(portunus, pipeline_command_timeout, 100),
    A = {?NAME, node()},
    B = {?NAME, 'other@nohost'},
    meck:expect(ra, pipeline_command,
                fun(S, _Cmd, Corr, low) ->
                        Other = case S of A -> B; _ -> A end,
                        self() ! {ra_event, S, {rejected, {not_leader, Other, Corr}}},
                        ok
                end),
    ?assertEqual({error, no_quorum}, portunus:acquire(?NAME, key5, lease5, node())).

silence_runs_into_the_deadline(_Config) ->
    application:set_env(portunus, pipeline_command_timeout, 100),
    meck:expect(ra, pipeline_command, fun(_, _, _, low) -> ok end),
    T0 = erlang:monotonic_time(millisecond),
    ?assertEqual({error, no_quorum}, portunus:acquire(?NAME, key6, lease6, node())),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed >= 100 andalso Elapsed < 3000).
