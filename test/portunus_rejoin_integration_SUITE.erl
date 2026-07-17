%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_rejoin_integration_SUITE).

%% The evict-then-rejoin arms `ra` faults drive: the removal answered
%% `not_member` by a concurrent pass, a timeout, and the one-evict-per-pass
%% budget when the membership read keeps listing the node. Ra is mecked
%% because these answers cannot be timed against a real cluster.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([one_evict_per_pass/1,
         concurrent_removal_continues_the_pass/1,
         removal_timeout_propagates/1,
         removal_error_propagates/1,
         unreadable_local_directory_retries/1]).

-define(SYS, portunus_rejoin_int_sys).
-define(NAME, portunus_rejoin_int_test).

all() ->
    [one_evict_per_pass,
     concurrent_removal_continues_the_pass,
     removal_timeout_propagates,
     removal_error_propagates,
     unreadable_local_directory_retries].

init_per_testcase(TC, Config) ->
    Dir = filename:join([?config(priv_dir, Config), atom_to_list(TC), "ra"]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    %% The system must run, or `locally_known/2` reads as unavailable and the
    %% evict path is never reached.
    ok = portunus:start_system(?SYS, Dir),
    meck:new(ra, [passthrough]),
    Config.

end_per_testcase(_TC, _Config) ->
    catch meck:unload(ra),
    catch ra_system:stop(?SYS),
    ok.

%% The seed keeps listing this node in every clause below: `?NAME` has no
%% local server, so `rejoin_action(false, true)` routes to the evict.
listed() ->
    Self = {?NAME, node()},
    meck:expect(ra, members, fun({?NAME, _}, _) -> {ok, [Self], Self} end).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

%% A removal that "succeeds" while the re-read still lists the node (stale
%% leadership) must not loop inside one call: one evict per pass, then a
%% retryable error.
one_evict_per_pass(_Config) ->
    listed(),
    meck:expect(ra, remove_member, fun(_, _) -> {ok, ok, {?NAME, node()}} end),
    ?assertEqual({error, membership_change_pending},
                 portunus:join_cluster(?SYS, ?NAME, node())),
    ?assertEqual(1, meck:num_calls(ra, remove_member, '_')).

concurrent_removal_continues_the_pass(_Config) ->
    %% `not_member` (a concurrent pass already removed it) continues the pass
    %% instead of propagating; the static listing then exhausts the budget.
    listed(),
    meck:expect(ra, remove_member, fun(_, _) -> {error, not_member} end),
    ?assertEqual({error, membership_change_pending},
                 portunus:join_cluster(?SYS, ?NAME, node())),
    ?assertEqual(1, meck:num_calls(ra, remove_member, '_')).

removal_timeout_propagates(_Config) ->
    listed(),
    Self = {?NAME, node()},
    meck:expect(ra, remove_member, fun(_, _) -> {timeout, Self} end),
    ?assertEqual({error, {timeout, Self}},
                 portunus:join_cluster(?SYS, ?NAME, node())).

removal_error_propagates(_Config) ->
    listed(),
    meck:expect(ra, remove_member,
                fun(_, _) -> {error, cluster_change_not_permitted} end),
    ?assertEqual({error, cluster_change_not_permitted},
                 portunus:join_cluster(?SYS, ?NAME, node())).

%% A stopped system makes `ra_directory:uid_of/2` raise: that must read as
%% "retry", never as "not registered" (which would evict an intact identity).
unreadable_local_directory_retries(_Config) ->
    listed(),
    meck:expect(ra, remove_member, fun(_, _) -> error(unreachable) end),
    ok = ra_system:stop(?SYS),
    ?assertEqual({error, local_view_unavailable},
                 portunus:join_cluster(?SYS, ?NAME, node())),
    ?assertEqual(0, meck:num_calls(ra, remove_member, '_')).
