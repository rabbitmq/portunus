%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_keepalive_budget_SUITE).

%% Each renew is bounded to a fraction of the TTL, so an
%% unreachable leader cannot burn the whole TTL budget in one blocked attempt.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([renew_call_is_bounded_to_a_fraction_of_ttl/1,
         sustained_no_quorum_loses_after_a_ttl/1]).

-define(NAME, portunus_kab_test).

all() ->
    [renew_call_is_bounded_to_a_fraction_of_ttl,
     sustained_no_quorum_loses_after_a_ttl].

init_per_testcase(_TC, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TC, _Config) ->
    catch meck:unload(portunus),
    ok.

renew_call_is_bounded_to_a_fraction_of_ttl(_Config) ->
    Ctrl = self(),
    ok = meck:new(portunus, [passthrough, no_link]),
    %% Key on our own lease id: when several suites share one VM a keepalive
    %% from another can renew through the mocked `portunus` too, and an
    %% unconditional clause would report its timeout as ours.
    meck:expect(portunus, renew_leases,
                fun(_N, [lease], Timeout) ->
                        Ctrl ! {renew_timeout, Timeout},
                        [{lease, ok}];
                   (_N, Ls, _Timeout) ->
                        [{L, ok} || L <- Ls]
                end),
    Ttl = 3000,
    {ok, KA} = portunus_keepalive:start_link(?NAME, lease, Ttl),
    Timeout = receive {renew_timeout, T} -> T after 5000 -> ct:fail(no_renew) end,
    %% renew_timeout(3000) = max(3000 div 5, 500) = 600, well under the 5000ms
    %% default command timeout.
    ?assertEqual(600, Timeout),
    portunus_keepalive:stop(KA).

%% A renew that keeps failing transiently is ridden out, but once a whole TTL
%% has passed without a confirmed renewal the holder is told the lease is lost.
sustained_no_quorum_loses_after_a_ttl(_Config) ->
    ok = meck:new(portunus, [passthrough, no_link]),
    meck:expect(portunus, renew_leases, fun(_N, [L], _T) -> [{L, {error, no_quorum}}] end),
    {ok, _KA} = portunus_keepalive:start_link(?NAME, lease, 2000),
    receive
        {portunus, lease_lost, lease} -> ok
    after 5000 -> ct:fail(no_lease_lost)
    end.
