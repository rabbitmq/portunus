%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_counters).
-moduledoc """
Operational metrics for `portunus`, backed by `seshat` (one counter set
per node, per cluster).

Gauges are published per node by the machine's `handle_aux/5` on each tick
(`set_gauges/2`), so every member reports its own view, not the leader's.
`is_leader` and `leader_changes_total` come from the machine's `state_enter/2`,
which fires on every member. Counters are bumped via `incr/2` from the leader's
apply effects, never inside `apply/3`, which runs on every replica and would
N-fold over-count. The one exception is
`failures_due_to_lack_of_online_quorum_total`, bumped on the client side when
a command or query cannot be served because the Raft cluster has no online
majority, and so never reaches the machine.
""".

-export([init/0,
         ensure/1,
         incr/2,
         set_gauge/3,
         set_gauges/2,
         overview/1]).

-define(GROUP, portunus).
-define(FIELDS_KEY, portunus_seshat_fields).

%% {Name, Index, seshat:metric_type(), Help}
%% Gauges are published per node by the machine's `handle_aux/5`; counters are
%% bumped by the leader's apply effects. `members_reachable` is left out: only
%% the leader tracks peer liveness, so a follower has no honest value for it.
-define(FIELDS,
        [{is_leader,                1, gauge,   "1 if this node is the Ra leader"},
         {has_quorum,               2, gauge,   "1 if this node currently sees a leader"},
         {cluster_members,          3, gauge,   "Configured cluster members"},
         {raft_term,                4, gauge,   "Current Raft term (this node's view)"},
         {apply_lag,                5, gauge,   "commit_index - last_applied (this node)"},
         {log_entries,              6, gauge,   "Log entries since the last snapshot (this node)"},
         {locks_held,               7, gauge,   "Locks currently held"},
         {leases_active,            8, gauge,   "Active leases"},
         {waiters,                  9, gauge,   "Queued succession candidates (succession depth)"},
         {fencing_token,           10, gauge,   "Index part of the highest fencing token issued"},
         {leader_changes_total,    11, counter, "Times this node became leader"},
         {acquires_total,          12, counter, "Successful lock acquisitions"},
         {acquire_conflicts_total, 13, counter, "Acquires rejected (already held)"},
         {releases_total,          14, counter, "Lock releases"},
         {renewals_total,          15, counter, "Lease renewals"},
         {lease_expiries_total,    16, counter, "Leases expired without renewal"},
         {failures_due_to_lack_of_online_quorum_total, 17, counter, "Commands and queries that failed because there was no online quorum (majority)"},
         {transfers_total,         18, counter, "Targeted ownership transfers that handed a key to a named node"},
         {transfer_no_contender_total, 19, counter, "Targeted transfers refused because the target was not a ready contender"},
         {queue_leaves_total,      20, counter, "Succession bids withdrawn with leave_succession_queue"},
         %% Tokens are epoch-packed and exceed the gauge's 64-bit atomic, so
         %% the two parts are published separately (`portunus:token_info/1`
         %% decomposes a token the same way).
         {fencing_epoch,           21, gauge,   "Epoch part of the highest fencing token issued"}]).

-doc "Create the seshat group and register the field spec. Idempotent.".
-spec init() -> ok.
init() ->
    {ok, _} = application:ensure_all_started(seshat),
    _ = seshat:new_group(?GROUP),
    persistent_term:put(?FIELDS_KEY, ?FIELDS),
    ok.

-doc "Ensure a counter set exists for `Cluster` on this node. Returns its ref.".
-spec ensure(atom()) -> counters:counters_ref().
ensure(Cluster) ->
    case seshat:fetch(?GROUP, {Cluster, node()}) of
        undefined ->
            seshat:new(?GROUP, {Cluster, node()},
                       {persistent_term, ?FIELDS_KEY},
                       #{cluster => Cluster, node => node()});
        Ref ->
            Ref
    end.

-doc "Bump a counter by one. Safe to call as a leader-only `mod_call` effect.".
%% Ra applies `mod_call` effects unprotected, so counting must never crash
%% the caller: a counters fault here would take down the Ra leader.
-spec incr(atom(), atom()) -> ok.
incr(Cluster, Field) ->
    try counters:add(ensure(Cluster), index_of(Field), 1)
    catch _:_ -> ok
    end,
    ok.

-doc "Set a single gauge to `Value`.".
-spec set_gauge(atom(), atom(), integer()) -> ok.
set_gauge(Cluster, Field, Value) ->
    try counters:put(ensure(Cluster), index_of(Field), Value)
    catch _:_ -> ok
    end,
    ok.

-doc "Set gauges from a map of gauge field name to value.".
-spec set_gauges(atom(), #{atom() => integer()}) -> ok.
set_gauges(Cluster, Gauges) ->
    try
        Ref = ensure(Cluster),
        maps:foreach(fun(Field, Value) ->
                             counters:put(Ref, index_of(Field), Value)
                     end, Gauges)
    catch _:_ -> ok
    end,
    ok.

-doc "Snapshot all counters for `Cluster` on this node.".
-spec overview(atom()) -> #{atom() => integer()}.
overview(Cluster) ->
    case seshat:fetch(?GROUP, {Cluster, node()}) of
        undefined -> #{};
        _Ref -> seshat:counters(?GROUP, {Cluster, node()})
    end.

index_of(Field) ->
    case field_index(Field) of
        undefined -> error({unknown_metric, Field});
        Ix -> Ix
    end.

field_index(Field) ->
    case lists:keyfind(Field, 1, ?FIELDS) of
        {Field, Ix, _Type, _Help} -> Ix;
        false -> undefined
    end.
