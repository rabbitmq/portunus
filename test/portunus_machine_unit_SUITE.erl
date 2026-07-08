%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_machine_unit_SUITE).

%% More `apply/3` unit tests (no Ra, deterministic), beyond `portunus_machine_SUITE`.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([expiry_releases_keys_and_promotes/1,
         multi_lease_expiry_releases_all/1,
         fencing_after_expiry_and_regrant/1,
         re_grant_refreshes_the_deadline/1,
         double_release_is_not_held/1,
         replay_is_deterministic/1,
         membership_commands_are_noops/1,
         unknown_command_is_rejected/1,
         overview_counts_state/1,
         transfer_promotes_named_contender/1,
         transfer_no_matching_contender_is_noop/1,
         transfer_stale_token_is_not_owner/1,
         transfer_free_key_is_not_owner/1,
         transfer_to_self_is_ok/1,
         transfer_promotes_pidless_contender/1,
         transfer_prefers_higher_rank_same_owner/1,
         transfer_watch_event_is_clean_handover/1,
         query_contenders_lists_live_contenders/1]).

%% This case feeds `apply/3` a command outside `command()` on purpose, to
%% assert the catch-all rejects it; dialyzer correctly sees the call as
%% never-returning, so the warning is suppressed here rather than worked around.
-dialyzer({nowarn_function, [unknown_command_is_rejected/1]}).

all() ->
    [expiry_releases_keys_and_promotes,
     multi_lease_expiry_releases_all,
     fencing_after_expiry_and_regrant,
     re_grant_refreshes_the_deadline,
     double_release_is_not_held,
     replay_is_deterministic,
     membership_commands_are_noops,
     unknown_command_is_rejected,
     overview_counts_state,
     transfer_promotes_named_contender,
     transfer_no_matching_contender_is_noop,
     transfer_stale_token_is_not_owner,
     transfer_free_key_is_not_owner,
     transfer_to_self_is_ok,
     transfer_promotes_pidless_contender,
     transfer_prefers_higher_rank_same_owner,
     transfer_watch_event_is_clean_handover,
     query_contenders_lists_live_contenders].

%% One lease holding two keys expires: both keys are released, and a key with
%% a surviving waiter is promoted to it with a fresh grant.
expiry_releases_keys_and_promotes(_Config) ->
    P2 = dummy_pid(),
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100, o1, dummy_pid()}, 1, 0, S0),
    {{ok, _}, S2, _} = at({acquire, l1, k1, o1, undefined, nowait}, 2, 0, S1),
    {{ok, _}, S3, _} = at({acquire, l1, k2, o1, undefined, nowait}, 3, 0, S2),
    {{ok, l2}, S4, _} = at({grant_lease, l2, 100000, o2, P2}, 4, 0, S3),
    {{queued, 1}, S5, _} = at({acquire, l2, k1, o2, undefined, wait}, 5, 0, S4),
    {ok, S6, Effs} = at({timeout, expire}, 6, 200, S5),
    {ok, #{owner := o2}} = portunus_machine:query_owner(k1, S6),
    {error, not_held} = portunus_machine:query_owner(k2, S6),
    ?assertNotEqual(false, grant_token(Effs, P2, k1)).

%% Several independent leases expire in one sweep: every key is released.
multi_lease_expiry_releases_all(_Config) ->
    Grants = [{l1, k1, o1}, {l2, k2, o2}, {l3, k3, o3}],
    Granted = lists:foldl(fun({L, _K, O}, S) ->
                                  {{ok, L}, Next, _} =
                                      at({grant_lease, L, 100, O, dummy_pid()},
                                         ix(L), 0, S),
                                  Next
                          end, new(), Grants),
    Held = lists:foldl(fun({L, K, O}, S) ->
                               {{ok, _}, Next, _} =
                                   at({acquire, L, K, O, undefined, nowait},
                                      ix({a, L}), 0, S),
                               Next
                       end, Granted, Grants),
    {ok, Swept, _} = at({timeout, expire}, 99, 200, Held),
    [?assertEqual({error, not_held}, portunus_machine:query_owner(K, Swept))
     || {_L, K, _O} <- Grants].

%% The fencing guarantee: a holder whose lease expired and whose key was
%% re-granted cannot release the new owner, and cannot re-acquire under its
%% dead lease. The new token strictly exceeds the old.
fencing_after_expiry_and_regrant(_Config) ->
    S0 = new(),
    {{ok, la}, S1, _} = at({grant_lease, la, 100, oa, dummy_pid()}, 1, 0, S0),
    {{ok, T1}, S2, _} = at({acquire, la, k, oa, undefined, nowait}, 2, 0, S1),
    {ok, S3, _} = at({timeout, expire}, 3, 200, S2),
    {{ok, lb}, S4, _} = at({grant_lease, lb, 100000, ob, dummy_pid()}, 4, 300, S3),
    {{ok, T2}, S5, _} = at({acquire, lb, k, ob, undefined, nowait}, 5, 300, S4),
    ?assert(T2 > T1),
    {{error, not_owner}, S6, _} = at({release, k, T1}, 6, 300, S5),
    {ok, #{owner := ob}} = portunus_machine:query_owner(k, S6),
    {{error, lease_expired}, _, _} =
        at({acquire, la, k, oa, undefined, nowait}, 7, 300, S6).

double_release_is_not_held(_Config) ->
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 1000, o, dummy_pid()}, 1, 0, S0),
    {{ok, T}, S2, _} = at({acquire, l1, k, o, undefined, nowait}, 2, 0, S1),
    {ok, S3, _} = at({release, k, T}, 3, 0, S2),
    {{error, not_held}, _, _} = at({release, k, T}, 4, 0, S3).

%% The same command log folded twice yields identical state: the contract
%% that every replica reaches the same state from the same log.
replay_is_deterministic(_Config) ->
    Log = [{{grant_lease, l1, 1000, o1, dummy_pid()}, 1, 0},
           {{grant_lease, l2, 1000, o2, dummy_pid()}, 2, 0},
           {{acquire, l1, k1, o1, undefined, nowait}, 3, 0},
           {{acquire, l2, k1, o2, undefined, wait}, 4, 0},
           {{acquire, l1, k2, o1, undefined, nowait}, 5, 0},
           {{release, k1, 3}, 6, 0},
           {{timeout, expire}, 7, 5000}],
    ?assertEqual(replay(Log), replay(Log)).

%% An idempotent re-grant by the same owner moves the deadline out, so a tick
%% past the original deadline but before the new one does not expire the lease.
re_grant_refreshes_the_deadline(_Config) ->
    S0 = new(),
    {{ok, l}, S1, _} = at({grant_lease, l, 1000, o, dummy_pid()}, 1, 0, S0),
    {{ok, _}, S2, _} = at({acquire, l, k, o, undefined, nowait}, 2, 0, S1),
    {{ok, l}, S3, _} = at({grant_lease, l, 5000, o, dummy_pid()}, 3, 100, S2),
    {_, S4, _} = at({timeout, expire}, 4, 2000, S3),
    ?assertMatch({ok, #{owner := o}}, portunus_machine:query_owner(k, S4)).

%% Membership signals are observational: they never change lock state.
membership_commands_are_noops(_Config) ->
    S0 = new(),
    {ok, S1, _} = at({nodeup, 'n@host'}, 1, 0, S0),
    {ok, S2, _} = at({nodedown, 'n@host'}, 2, 0, S1),
    ?assertEqual(S0, S2).

unknown_command_is_rejected(_Config) ->
    S0 = new(),
    {{error, unknown_command}, _, _} = at({bogus, command}, 1, 0, S0).

%% The gauges the leader publishes each tick reflect the replicated state:
%% three leases, one held key, one waiter behind it, one watched key.
overview_counts_state(_Config) ->
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 1000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, l2}, S2, _} = at({grant_lease, l2, 1000, o2, dummy_pid()}, 2, 0, S1),
    {{ok, l3}, S3, _} = at({grant_lease, l3, 1000, o3, dummy_pid()}, 3, 0, S2),
    {{ok, _}, S4, _} = at({acquire, l1, k1, o1, undefined, nowait}, 4, 0, S3),
    {{queued, 1}, S5, _} = at({acquire, l2, k1, o2, undefined, wait}, 5, 0, S4),
    {{ok, _}, S6, _} = at({watch, k2, dummy_pid()}, 6, 0, S5),
    ?assertMatch(#{leases := 3, locks := 1, waiters := 1, watchers := 1},
                 portunus_machine:query_status(S6)).

%%----------------------------------------------------------------------
%% A transfer names a later, non-best-ranked waiter: that exact contender is
%% promoted, the token strictly increases, its lease pid gets the grant, and
%% the other waiter stays queued.
transfer_promotes_named_contender(_Config) ->
    P3 = dummy_pid(),
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, Tok}, S2, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, 0, S1),
    {{ok, l2}, S3, _} = at({grant_lease, l2, 100000, o2, dummy_pid()}, 3, 0, S2),
    {{ok, l3}, S4, _} = at({grant_lease, l3, 100000, o3, P3}, 4, 0, S3),
    {{queued, 1}, S5, _} = at({acquire, l2, k, o2, undefined, wait}, 5, 0, S4),
    {{queued, 2}, S6, _} = at({acquire, l3, k, o3, undefined, wait}, 6, 0, S5),
    {ok, S7, Effs} = at({transfer, k, Tok, o3}, 7, 0, S6),
    {ok, #{owner := o3, token := T7}} = portunus_machine:query_owner(k, S7),
    ?assert(T7 > Tok),
    ?assertNotEqual(false, grant_token(Effs, P3, k)),
    [o2] = portunus_machine:query_contenders(k, S7).

%% A transfer to an owner with no waiter changes nothing: the holder keeps the
%% key at its current token.
transfer_no_matching_contender_is_noop(_Config) ->
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, Tok}, S2, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, 0, S1),
    {{error, {no_contender, o2}}, S3, _} = at({transfer, k, Tok, o2}, 3, 0, S2),
    {ok, #{owner := o1, token := Tok}} = portunus_machine:query_owner(k, S3).

%% A transfer carrying a stale token is not_owner and changes nothing.
transfer_stale_token_is_not_owner(_Config) ->
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, Tok}, S2, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, 0, S1),
    {{ok, l2}, S3, _} = at({grant_lease, l2, 100000, o2, dummy_pid()}, 3, 0, S2),
    {{queued, 1}, S4, _} = at({acquire, l2, k, o2, undefined, wait}, 4, 0, S3),
    {{error, not_owner}, S5, _} = at({transfer, k, Tok + 999, o2}, 5, 0, S4),
    {ok, #{owner := o1, token := Tok}} = portunus_machine:query_owner(k, S5).

%% A transfer of a free key is not_owner (the caller is not the holder).
transfer_free_key_is_not_owner(_Config) ->
    {{error, not_owner}, _, _} = at({transfer, k, 1, o2}, 1, 0, new()).

%% A transfer to the holder's own owner returns ok and changes nothing.
transfer_to_self_is_ok(_Config) ->
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, Tok}, S2, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, 0, S1),
    {ok, S3, _} = at({transfer, k, Tok, o1}, 3, 0, S2),
    {ok, #{owner := o1, token := Tok}} = portunus_machine:query_owner(k, S3).

%% A target whose lease has no monitored pid is still promoted; the match is on
%% the owner term, so only the grant effect (which needs a pid) is absent.
transfer_promotes_pidless_contender(_Config) ->
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, Tok}, S2, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, 0, S1),
    {{ok, l2}, S3, _} = at({grant_lease, l2, 100000, o2, undefined}, 3, 0, S2),
    {{queued, 1}, S4, _} = at({acquire, l2, k, o2, undefined, wait}, 4, 0, S3),
    {ok, S5, Effs} = at({transfer, k, Tok, o2}, 5, 0, S4),
    {ok, #{owner := o2}} = portunus_machine:query_owner(k, S5),
    [] = [M || {send_msg, _, {portunus, granted, _, _, _}} = M <- Effs].

%% Two waiters share one owner term: the highest-ranked (higher score) is
%% promoted, keeping the transition deterministic.
transfer_prefers_higher_rank_same_owner(_Config) ->
    P3 = dummy_pid(),
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, Tok}, S2, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, 0, S1),
    {{ok, l2}, S3, _} = at({grant_lease, l2, 100000, shared, dummy_pid()}, 3, 0, S2),
    {{ok, l3}, S4, _} = at({grant_lease, l3, 100000, shared, P3}, 4, 0, S3),
    {{queued, 1}, S5, _} = at({acquire, l2, k, shared, undefined, wait, 0}, 5, 0, S4),
    {{queued, 2}, S6, _} = at({acquire, l3, k, shared, undefined, wait, 5}, 6, 0, S5),
    {ok, S7, Effs} = at({transfer, k, Tok, shared}, 7, 0, S6),
    ?assertNotEqual(false, grant_token(Effs, P3, k)),
    {ok, #{owner := shared}} = portunus_machine:query_owner(k, S7).

%% A watcher of a transferred key sees one clean ownership change to the new
%% owner, never a transient release (the key is never free during a transfer).
transfer_watch_event_is_clean_handover(_Config) ->
    W = dummy_pid(),
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, Tok}, S2, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, 0, S1),
    {{ok, _Ref}, S3, _} = at({watch, k, W}, 3, 0, S2),
    {{ok, l2}, S4, _} = at({grant_lease, l2, 100000, o2, dummy_pid()}, 4, 0, S3),
    {{queued, 1}, S5, _} = at({acquire, l2, k, o2, undefined, wait}, 5, 0, S4),
    {ok, _S6, Effs} = at({transfer, k, Tok, o2}, 6, 0, S5),
    [{acquired, o2}] = [E || {send_msg, P, {portunus, watch, _, E}} <- Effs,
                             P =:= W].

%% `query_contenders/2` lists the owner terms of the live waiters on a key.
query_contenders_lists_live_contenders(_Config) ->
    S0 = new(),
    {{ok, l1}, S1, _} = at({grant_lease, l1, 100000, o1, dummy_pid()}, 1, 0, S0),
    {{ok, _}, S2, _} = at({acquire, l1, k, o1, undefined, nowait}, 2, 0, S1),
    [] = portunus_machine:query_contenders(k, S2),
    {{ok, l2}, S3, _} = at({grant_lease, l2, 100000, o2, dummy_pid()}, 3, 0, S2),
    {{queued, 1}, S4, _} = at({acquire, l2, k, o2, undefined, wait}, 4, 0, S3),
    [o2] = portunus_machine:query_contenders(k, S4).

%% Helpers
%%----------------------------------------------------------------------

new() ->
    portunus_machine:init(#{cluster => test}).

at(Cmd, Ix, Now, State) ->
    Meta = portunus_test_helpers:meta(Ix, Now),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.

replay(Log) ->
    lists:foldl(fun({Cmd, Ix, Now}, S) ->
                        {_, S1, _} = at(Cmd, Ix, Now, S),
                        S1
                end, new(), Log).

%% A stable per-term index, so test command order does not depend on the
%% caller passing distinct integers by hand.
ix(Term) ->
    erlang:phash2(Term).

dummy_pid() ->
    spawn(fun() -> timer:sleep(infinity) end).

grant_token(Effects, Pid, Key) ->
    case [{T, L} || {send_msg, P, {portunus, granted, K, T, L}} <- Effects,
                    P =:= Pid, K =:= Key] of
        [TL | _] -> TL;
        [] -> false
    end.
