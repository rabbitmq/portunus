%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_batch_keepalive_unit_SUITE).

%% Leases attached with the same `{ClusterName, TTL}` renew in one Ra command per
%% round. Loss rules are `portunus_keepalive`'s. A renewer restart is
%% survived by re-attaching within the TTL.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([healthy_lease_is_renewed_past_ttl/1,
         group_renews_in_one_command_per_round/1,
         expired_lease_notifies_only_its_holder/1,
         holder_death_detaches/1,
         detach_stops_renewal/1,
         reattach_after_hub_restart_keeps_lease/1,
         transient_failure_does_not_lose_lease/1]).

-define(SYS, portunus_batch_keepalive_unit_sys).
-define(NAME, portunus_batch_keepalive_test).
-define(TTL, 3000).

all() ->
    [healthy_lease_is_renewed_past_ttl,
     group_renews_in_one_command_per_round,
     expired_lease_notifies_only_its_holder,
     holder_death_detaches,
     detach_stops_renewal,
     reattach_after_hub_restart_keeps_lease,
     transient_failure_does_not_lose_lease].

init_per_suite(Config) ->
    %% 1 s, not the usual 200 ms: the machine's expiry tick is a Ra command
    %% too, and the command-count case needs it out of the noise floor.
    application:set_env(portunus, tick_interval_ms, 1000),
    DataDir = filename:join(?config(priv_dir, Config), "ra"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ok = portunus:start_system(?SYS, DataDir),
    {ok, _, _} = portunus:start_cluster(?SYS, ?NAME, [node()]),
    ok = portunus_test_helpers:await_leader(?NAME),
    Config.

end_per_suite(_Config) ->
    catch ra:stop_server(?SYS, {?NAME, node()}),
    ok.

healthy_lease_is_renewed_past_ttl(_Config) ->
    {Holder, Lease} = start_holder(),
    timer:sleep(?TTL + 1000),
    ?assertEqual([{Lease, ok}], portunus:renew_leases(?NAME, [Lease])),
    refute_lost(Holder, Lease),
    stop_holder(Holder).

%% Over a bit more than one TTL, at most 4 renewal rounds fire and at most 4
%% expiry ticks apply, so batched renewal adds about 8 commands. A per-lease
%% renewer would add 40 renewal commands.
group_renews_in_one_command_per_round(_Config) ->
    Holders = [start_holder() || _ <- lists:seq(1, 10)],
    Before = ra_commands_count(),
    timer:sleep(?TTL + 500),
    Delta = ra_commands_count() - Before,
    ?assert(Delta =< 12, {too_many_renew_commands, Delta}),
    [begin refute_lost(H, L), stop_holder(H) end || {H, L} <- Holders].

%% A revoked lease is lost for its owner only; the other lease in the group
%% keeps renewing.
expired_lease_notifies_only_its_holder(_Config) ->
    {Holder1, Lease1} = start_holder(),
    {Holder2, Lease2} = start_holder(),
    ok = portunus:revoke_lease(?NAME, Lease1),
    receive
        {lost, Holder1, Lease1} -> ok
    after ?TTL + 2000 ->
        ct:fail(no_lease_lost)
    end,
    refute_lost(Holder2, Lease2),
    ?assertEqual([{Lease2, ok}], portunus:renew_leases(?NAME, [Lease2])),
    ?assertNot(is_attached(Lease1)),
    ?assert(is_attached(Lease2)),
    stop_holder(Holder2).

holder_death_detaches(_Config) ->
    {Holder, Lease} = start_holder(),
    true = exit(Holder, kill),
    ok = wait_until(fun() -> not is_attached(Lease) end, 2000),
    %% The machine's own lock owner monitor releases the lease itself.
    ok = wait_until(fun() ->
                            [{Lease, {error, lease_expired}}] =:=
                                portunus:renew_leases(?NAME, [Lease])
                    end, 2000).

detach_stops_renewal(_Config) ->
    {Holder, Lease} = start_holder(),
    ok = portunus_batch_keepalive:detach(?NAME, Lease),
    ?assertNot(is_attached(Lease)),
    %% Polling with a renew would keep the lease alive, so sleep past the
    %% worst case (a round just before the detach, plus expiry landing on a
    %% 1 s tick) and check once that it expired.
    timer:sleep(?TTL + 3000),
    ?assertEqual([{Lease, {error, lease_expired}}],
                 portunus:renew_leases(?NAME, [Lease])),
    stop_holder(Holder).

%% A killed renewer restarts empty under `portunus_sup`. A lock owner that
%% re-attaches within the TTL keeps its lease.
reattach_after_hub_restart_keeps_lease(_Config) ->
    {Holder, Lease} = start_holder(),
    Hub = whereis(portunus_batch_keepalive),
    Mon = erlang:monitor(process, Hub),
    exit(Hub, kill),
    receive {'DOWN', Mon, process, Hub, _} -> ok
    after 5000 -> ct:fail(hub_not_killed)
    end,
    ok = wait_until(fun() -> is_pid(whereis(portunus_batch_keepalive)) end, 5000),
    Holder ! reattach,
    ok = wait_until(fun() -> is_attached(Lease) end, 5000),
    timer:sleep(?TTL + 1000),
    ?assertEqual([{Lease, ok}], portunus:renew_leases(?NAME, [Lease])),
    refute_lost(Holder, Lease),
    stop_holder(Holder).

%% With the Ra server stopped, renewals fail transiently. The renewer
%% retries and the lease survives once the server is back.
transient_failure_does_not_lose_lease(_Config) ->
    Ttl = 10000,
    {Holder, Lease} = start_holder(Ttl),
    ok = ra:stop_server(?SYS, {?NAME, node()}),
    timer:sleep(2500),
    refute_lost(Holder, Lease),
    _ = ra:restart_server(?SYS, {?NAME, node()}),
    ok = portunus_test_helpers:await_leader(?NAME),
    timer:sleep(2000),
    refute_lost(Holder, Lease),
    ?assertEqual([{Lease, ok}], portunus:renew_leases(?NAME, [Lease])),
    stop_holder(Holder).

%%----------------------------------------------------------------------
%% Lock owner process and polling helpers
%%----------------------------------------------------------------------

start_holder() ->
    start_holder(?TTL).

%% `reattach` re-attaches after a renewer restart, like a real owner's
%% 'DOWN' handler.
start_holder(Ttl) ->
    Ctrl = self(),
    Holder = spawn(fun() ->
                           {ok, Lease} = portunus:grant_lease(?NAME, Ttl),
                           ok = portunus_batch_keepalive:attach(?NAME, Lease,
                                                                Ttl),
                           Ctrl ! {ready, self(), Lease},
                           holder_loop(Ctrl, Lease, Ttl)
                   end),
    receive
        {ready, Holder, Lease} -> {Holder, Lease}
    after 30000 ->
        ct:fail(holder_start_timeout)
    end.

holder_loop(Ctrl, Lease, Ttl) ->
    receive
        {portunus, lease_lost, Lease} ->
            Ctrl ! {lost, self(), Lease};
        reattach ->
            ok = portunus_batch_keepalive:attach(?NAME, Lease, Ttl),
            holder_loop(Ctrl, Lease, Ttl);
        stop ->
            ok
    end.

stop_holder(Holder) ->
    Holder ! stop,
    ok.

refute_lost(Holder, Lease) ->
    receive
        {lost, Holder, Lease} -> ct:fail(unexpected_lease_lost)
    after 0 -> ok
    end.

is_attached(Lease) ->
    lists:member(Lease,
                 lists:append(maps:values(portunus_batch_keepalive:overview()))).

ra_commands_count() ->
    Counters = maps:get({?NAME, node()}, ra_counters:overview()),
    maps:get(commands, Counters).

wait_until(_Fun, Left) when Left =< 0 ->
    ct:fail(wait_until_timeout);
wait_until(Fun, Left) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Fun, Left - 50)
    end.
