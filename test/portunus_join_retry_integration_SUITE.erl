%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_join_retry_integration_SUITE).

%% A join is two steps: start the local server, then `ra:add_member` on the
%% leader. When the second step fails (`cluster_change_not_permitted` from
%% a concurrent join, or a timeout), the node has a started non-member
%% server. Before `ensure_local_server` no retry could complete: `join_cluster/3` wedged
%% on `already_started` and `join_or_form/3` reported success through
%% `restart_server/2` while `is_member/1` stayed false forever.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([join_cluster_retry_completes_a_half_join/1,
         join_or_form_finishes_a_half_join/1]).

-define(SYS, portunus_join_retry_integration_sys).

all() ->
    [join_cluster_retry_completes_a_half_join,
     join_or_form_finishes_a_half_join].

init_per_suite(Config) ->
    case portunus_ct_cluster:ensure_distribution() of
        ok ->
            DataDir = filename:join(?config(priv_dir, Config), "ra_local"),
            ok = filelib:ensure_dir(filename:join(DataDir, "x")),
            ok = portunus:start_system(?SYS, DataDir),
            Config;
        Skip ->
            Skip
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    catch meck:unload(ra),
    ok.

join_cluster_retry_completes_a_half_join(Config) ->
    Name = portunus_join_retry_a,
    {Cluster, Seed} = seed_cluster(Config, Name),
    ok = half_join(Name, Seed),
    %% The retry now proceeds past the already-started local server to the
    %% idempotent add_member.
    ?assertEqual(ok, portunus:join_cluster(?SYS, Name, Seed)),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(Name) end),
    cleanup(Cluster, Name).

join_or_form_finishes_a_half_join(Config) ->
    Name = portunus_join_retry_b,
    {Cluster, Seed} = seed_cluster(Config, Name),
    ok = half_join(Name, Seed),
    %% `restart_server` alone would report ok for the recovered non-member
    %% server; the membership check must route back to the join.
    ?assertEqual(ok, portunus:join_or_form(?SYS, Name,
                                           lists:sort([node(), Seed]))),
    ok = portunus_test_helpers:await_condition(
           fun() -> portunus:is_member(Name) end),
    cleanup(Cluster, Name).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

seed_cluster(Config, Name) ->
    %% The seed's name must sort below this node's: `join_or_form/3` treats
    %% the lowest member as the one that forms, and here the peer did.
    Cluster = portunus_ct_cluster:start(Config, Name, 1,
                                        #{name_prefix => "aseed"}),
    #{nodes := [Seed]} = Cluster,
    true = Seed < node(),
    {Cluster, Seed}.

%% Start the local server, then fail the add_member the way a concurrent
%% join does, leaving the wedge state a retry must recover from.
half_join(Name, Seed) ->
    ok = meck:new(ra, [passthrough, no_link]),
    meck:expect(ra, add_member,
                fun(_Leader, _ServerId) ->
                        {error, cluster_change_not_permitted}
                end),
    {error, cluster_change_not_permitted} =
        portunus:join_cluster(?SYS, Name, Seed),
    meck:unload(ra),
    false = portunus:is_member(Name),
    ok.

cleanup(Cluster, Name) ->
    catch ra:stop_server(?SYS, {Name, node()}),
    catch ra:force_delete_server(?SYS, {Name, node()}),
    portunus_ct_cluster:stop(Cluster).
