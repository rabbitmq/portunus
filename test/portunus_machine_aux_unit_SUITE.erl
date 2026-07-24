%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_machine_aux_unit_SUITE).

%% The pure decision core of the aux renewal and expiry sweep
%% (`portunus_machine_aux`), driven with hand-built lease views, terms, and
%% a modeled clock: no Ra cluster, no machine state. The aux record is
%% opaque, so every rule is asserted through the observable outputs (renew
%% results and expire pairs).

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([seeded_lease_expires_only_after_full_ttl/1,
         gone_lease_is_dropped/1,
         expired_id_is_proposed_once/1,
         renewal_of_proposed_id_is_refused/1,
         logged_refresh_voids_proposal_and_renewals_resume/1,
         regrant_after_expiry_is_not_treated_as_pending/1,
         renew_moves_the_deadline/1,
         renew_of_unknown_id_is_lease_expired/1,
         refreshed_extends_the_deadline/1,
         term_change_clears_state/1,
         non_leader_tick_clears_state/1]).

all() ->
    [seeded_lease_expires_only_after_full_ttl,
     gone_lease_is_dropped,
     expired_id_is_proposed_once,
     renewal_of_proposed_id_is_refused,
     logged_refresh_voids_proposal_and_renewals_resume,
     regrant_after_expiry_is_not_treated_as_pending,
     renew_moves_the_deadline,
     renew_of_unknown_id_is_lease_expired,
     refreshed_extends_the_deadline,
     term_change_clears_state,
     non_leader_tick_clears_state].

-define(T, 1).

new() ->
    portunus_machine_aux:new().

tick(Aux, View, Now) ->
    portunus_machine_aux:leader_tick(Aux, View, ?T, Now).

renew(Aux, View, Now, Ids) ->
    portunus_machine_aux:renew(Aux, View, ?T, Now, Ids).

%% A lease first seen by the sweep is seeded at its full TTL: proposed at
%% `Seed + Ttl`, never before.
seeded_lease_expires_only_after_full_ttl(_Config) ->
    View = #{l1 => {100, 7}},
    {A1, []} = tick(new(), View, 0),
    {A2, []} = tick(A1, View, 99),
    {_A3, Pairs} = tick(A2, View, 100),
    ?assertEqual([{l1, 7}], Pairs).

gone_lease_is_dropped(_Config) ->
    View = #{l1 => {100, 7}},
    {A1, []} = tick(new(), View, 0),
    %% The lease left machine state (revoked or expired): the sweep drops it,
    %% and a lease with the same id re-granted later is seeded afresh.
    {A2, []} = tick(A1, #{}, 500),
    {_A3, []} = tick(A2, #{l1 => {100, 42}}, 501),
    ok.

expired_id_is_proposed_once(_Config) ->
    View = #{l1 => {100, 7}},
    {A1, []} = tick(new(), View, 0),
    {A2, Pairs} = tick(A1, View, 200),
    ?assertEqual([{l1, 7}], Pairs),
    %% Still pending (the proposal has not applied): not re-proposed.
    {_A3, []} = tick(A2, View, 300),
    ok.

renewal_of_proposed_id_is_refused(_Config) ->
    View = #{l1 => {100, 7}},
    {A1, []} = tick(new(), View, 0),
    {A2, [{l1, 7}]} = tick(A1, View, 200),
    %% Acknowledging would extend a deadline that the appended expiry
    %% command is about to void.
    {_A3, Results} = renew(A2, View, 210, [l1]),
    ?assertEqual([{l1, {error, lease_expired}}], Results).

logged_refresh_voids_proposal_and_renewals_resume(_Config) ->
    View0 = #{l1 => {100, 7}},
    {A1, []} = tick(new(), View0, 0),
    {A2, [{l1, 7}]} = tick(A1, View0, 200),
    %% A re-grant committed after the proposal: `refreshed`
    %% changed, the entry is void, and renewals resume.
    View1 = #{l1 => {100, 9}},
    {A3, [{l1, ok}]} = renew(A2, View1, 210, [l1]),
    %% The void entry is dropped on the tick and the renewed deadline holds.
    {_A4, []} = tick(A3, View1, 300),
    ok.

regrant_after_expiry_is_not_treated_as_pending(_Config) ->
    View0 = #{l1 => {100, 7}},
    {A1, []} = tick(new(), View0, 0),
    {A2, [{l1, 7}]} = tick(A1, View0, 200),
    %% The expiry applied (lease gone), then the id was re-granted with a new
    %% `refreshed`: a fresh lease, renewable at once.
    View1 = #{l1 => {100, 11}},
    {_A3, [{l1, ok}]} = renew(A2, View1, 250, [l1]),
    ok.

renew_moves_the_deadline(_Config) ->
    View = #{l1 => {100, 7}},
    {A1, []} = tick(new(), View, 0),
    {A2, [{l1, ok}]} = renew(A1, View, 90, [l1]),
    %% Seeded deadline was 100; the renewal moved it to 190.
    {A3, []} = tick(A2, View, 150),
    {_A4, [{l1, 7}]} = tick(A3, View, 190),
    ok.

renew_of_unknown_id_is_lease_expired(_Config) ->
    {_A, Results} = renew(new(), #{}, 0, [nope]),
    ?assertEqual([{nope, {error, lease_expired}}], Results).

refreshed_extends_the_deadline(_Config) ->
    View0 = #{l1 => {100, 7}},
    {A1, []} = tick(new(), View0, 0),
    %% Time passed the seeded deadline while the holder re-granted: the
    %% `{refreshed, ...}` effect extends the aux deadline, so the re-granted
    %% lease is not proposed right after a successful grant.
    View1 = #{l1 => {100, 9}},
    A2 = portunus_machine_aux:refreshed(A1, View1, ?T, 150, [l1]),
    {A3, []} = tick(A2, View1, 200),
    {_A4, [{l1, 9}]} = tick(A3, View1, 250),
    ok.

term_change_clears_state(_Config) ->
    View = #{l1 => {100, 7}},
    {A1, []} = portunus_machine_aux:leader_tick(new(), View, 1, 0),
    %% Deposed and re-elected: deadlines from the previous leadership are
    %% stale (the holders renewed with the interim leader), so the
    %% lease is re-seeded at the full TTL instead of swept at 200.
    {A2, []} = portunus_machine_aux:leader_tick(A1, View, 2, 200),
    {_A3, Pairs} = portunus_machine_aux:leader_tick(A2, View, 2, 300),
    ?assertEqual([{l1, 7}], Pairs).

non_leader_tick_clears_state(_Config) ->
    View = #{l1 => {100, 7}},
    {A1, []} = tick(new(), View, 0),
    A2 = portunus_machine_aux:non_leader_tick(A1),
    %% Re-elected in the same term: cleared state means re-seeding, so the
    %% first leader tick after demotion proposes nothing.
    {_A3, []} = tick(A2, View, 200),
    ok.
