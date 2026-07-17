%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_rejoin_directory_integration_SUITE).

%% The stale-registration state `ra:force_delete_server/2` can leave behind:
%% it deletes the replica directory before the buffered unregistration, so a
%% kill inside the call leaves a name pointing at a directory that is gone.
%% The rejoin decision must read that as "no local identity" and route to
%% the evict; an intact identity must still restart, and an unreadable local
%% view must still only retry. Ra is mecked for the seed's member list, as
%% in `portunus_rejoin_integration_SUITE`.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([stale_registration_with_missing_directory_evicts/1,
         registration_with_config_file_still_restarts/1,
         unreadable_local_view_still_retries/1]).

-define(SYS, portunus_rejoin_dir_sys).
-define(NAME, portunus_rejoin_dir_test).

all() ->
    [stale_registration_with_missing_directory_evicts,
     registration_with_config_file_still_restarts,
     unreadable_local_view_still_retries].

init_per_testcase(TC, Config) ->
    Dir = filename:join([?config(priv_dir, Config), atom_to_list(TC), "ra"]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ok = portunus:start_system(?SYS, Dir),
    meck:new(ra, [passthrough]),
    Config.

end_per_testcase(_TC, _Config) ->
    catch meck:unload(ra),
    catch ra:stop_server(?SYS, {?NAME, node()}),
    catch ra_system:stop(?SYS),
    ok.

%% The seed keeps listing this node in every clause below.
listed() ->
    Self = {?NAME, node()},
    meck:expect(ra, members, fun({?NAME, _}, _) -> {ok, [Self], Self} end).

%% A name-to-UID mapping with no directory behind it: the kill state.
register_stale_name() ->
    #{names := #{directory_rev := Rev}} = ra_system:fetch(?SYS),
    ok = dets:insert(Rev, {?NAME, <<"stale_uid_with_no_directory">>}).

%%----------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------

%% Red before the directory check: the registration resolved, the pass
%% routed to a restart that failed reading the missing directory, and every
%% later pass returned the same error with the member never removed.
stale_registration_with_missing_directory_evicts(_Config) ->
    register_stale_name(),
    listed(),
    meck:expect(ra, remove_member, fun(_, _) -> {ok, ok, {?NAME, node()}} end),
    %% The static member list keeps listing the node after the one evict, so
    %% the pass ends with the retryable budget error, exactly like a
    %% no-registration evict.
    ?assertEqual({error, membership_change_pending},
                 portunus:join_cluster(?SYS, ?NAME, node())),
    ?assertEqual(1, meck:num_calls(ra, remove_member, '_')).

%% The anti-eviction guard: an intact identity (registration and readable
%% `config`) is restarted, never evicted.
registration_with_config_file_still_restarts(_Config) ->
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    ok = ra:stop_server(?SYS, {?NAME, node()}),
    listed(),
    meck:expect(ra, remove_member, fun(_, _) -> error(unreachable) end),
    ?assertEqual(ok, portunus:join_cluster(?SYS, ?NAME, node())),
    ?assert(is_pid(whereis(?NAME))),
    ?assertEqual(0, meck:num_calls(ra, remove_member, '_')).

%% A stopped system makes the local read raise: that must read as "retry",
%% never as "not known" (which would evict an identity it cannot see), even
%% with the stale registration present.
unreadable_local_view_still_retries(_Config) ->
    register_stale_name(),
    listed(),
    meck:expect(ra, remove_member, fun(_, _) -> error(unreachable) end),
    ok = ra_system:stop(?SYS),
    ?assertEqual({error, local_view_unavailable},
                 portunus:join_cluster(?SYS, ?NAME, node())),
    ?assertEqual(0, meck:num_calls(ra, remove_member, '_')).
