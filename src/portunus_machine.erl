%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_machine).
-moduledoc """
The `portunus` Ra state machine. It manages leases, locks, fencing
tokens, a score-ordered succession queue (FIFO among equal scores), and
tick-based lease expiry.

Determinism rules (see AGENTS.md): `apply/3` never reads node-local
time (only the leader-stamped `system_time` in the command metadata),
never calls `make_ref/0`/`self/0`, and mints tokens/ids from the Raft
`index`. Timers and monitors are effects that only *trigger* commands;
the decision is always re-derived from replicated state.
""".

-behaviour(ra_machine).

-export([init/1,
         apply/3,
         state_enter/2,
         init_aux/1,
         handle_aux/5,
         overview/1,
         version/0,
         which_module/1]).

%% Exported for queries run via ra:consistent_query/local_query.
-export([query_owner/2,
         query_contenders/2,
         query_status/1]).

-define(DEFAULT_TICK_MS, 1000).
-define(DEFAULT_SNAPSHOT_INTERVAL, 4096).

-type lock_key() :: term().
-type lease_id() :: term().
-type token() :: non_neg_integer().
%% A watch registration handle: a Raft index.
-type watch_ref() :: non_neg_integer().
-type owner() :: term().
-type owner_info() :: #{owner := owner(), lease := lease_id(),
                        token := token(), context := term(),
                        remaining_ms := non_neg_integer()}.

%% Fencing tokens are *not* opaque by design: a client fences an external
%% write by comparing them.
-export_type([lock_key/0, lease_id/0, token/0, watch_ref/0, owner/0,
              owner_info/0]).

-type command() ::
        {grant_lease, portunus:option(lease_id()), pos_integer(), owner(),
         portunus:option(pid())} |
        {renew, [lease_id()]} |
        {revoke_lease, lease_id()} |
        {acquire, lease_id(), lock_key(), owner(), term(), wait | nowait} |
        {acquire, lease_id(), lock_key(), owner(), term(), wait | nowait,
         integer()} |
        {release, lock_key(), token()} |
        {transfer, lock_key(), token(), owner()} |
        {watch, lock_key(), pid()} |
        {unwatch, watch_ref()} |
        {timeout, expire} |
        {down, pid(), term()} |
        {nodeup | nodedown, node()}.

%% One held lock, kept under its lease.
-record(held_lock, {token :: token(),
                    owner :: owner(),
                    context :: term(),
                    since :: integer()}).

%% A succession candidate in a key's queue. It references the contender's
%% lease, so promotion attaches the key to that lease. `score` defines
%% promotion order (the highest value wins).
%% `seq` is the enqueue Raft index and breaks
%% score ties deterministically in candidate arrival order (FIFO).
-record(waiter, {lease_id :: lease_id(),
                 owner :: owner(),
                 context :: term(),
                 score = 0 :: integer(),
                 seq = 0 :: non_neg_integer()}).

-record(lease, {id :: lease_id(),
                ttl_ms :: pos_integer(),
                deadline :: integer(),
                owner :: owner(),
                pid :: portunus:option(pid()),
                keys = #{} :: #{lock_key() => #held_lock{}}}).

-record(?MODULE, {cluster :: atom(),
                  tick_ms = ?DEFAULT_TICK_MS :: pos_integer(),
                  snapshot_interval = ?DEFAULT_SNAPSHOT_INTERVAL :: pos_integer(),
                  %% the Raft index of the last release cursor
                  last_release = 0 :: non_neg_integer(),
                  leases = #{} :: #{lease_id() => #lease{}},
                  %% by_lock index: at most one owner per key
                  locks = #{} :: #{lock_key() => lease_id()},
                  %% by_pid index for fast monitor-driven cleanup
                  lease_pids = #{} :: #{pid() => #{lease_id() => true}},
                  %% per-key succession queues, stored in arrival order;
                  %% promotion ranks by `{score, -seq}`
                  leader_succession_queue = #{} :: #{lock_key() => [#waiter{}]},
                  %% watch registry, currently kept in replicated state
                  watchers = #{} :: #{lock_key() => #{pid() => token()}},
                  %% pids currently monitored, to avoid duplicate effects
                  monitored = #{} :: #{pid() => true},
                  %% the highest fencing token produced so far, exposed as a metric (a gauge)
                  max_token = 0 :: token()}).

-opaque state() :: #?MODULE{}.
-export_type([state/0]).

%%
%% ra_machine callbacks
%%

%% Ra merges `name` and `machine_version` into the init args, so the
%% callback must accept the wider machine-init map (it carries `cluster`
%% and `tick_interval_ms` from the cluster's machine config).
-spec init(map()) -> state().
init(Config) ->
    #?MODULE{cluster = maps:get(cluster, Config, portunus),
             tick_ms = maps:get(tick_interval_ms, Config, ?DEFAULT_TICK_MS),
             snapshot_interval = maps:get(snapshot_interval, Config,
                                          ?DEFAULT_SNAPSHOT_INTERVAL)}.

-spec apply(ra_machine:command_meta_data(), command(), state()) ->
    {state(), term(), ra_machine:effects()}.
apply(Meta, Cmd, State0) ->
    maybe_release_cursor(Meta, do_apply(Meta, Cmd, State0)).

%% A release cursor every `snapshot_interval` entries lets Ra snapshot
%% and truncate the log.
maybe_release_cursor(#{index := Index}, Ret) ->
    {State, Reply, Effs} = case Ret of
                               {S, R} -> {S, R, []};
                               {S, R, E} -> {S, R, E}
                           end,
    #?MODULE{snapshot_interval = Interval, last_release = Last} = State,
    case Index - Last >= Interval of
        true ->
            State1 = State#?MODULE{last_release = Index},
            {State1, Reply, [{release_cursor, Index, State1} | Effs]};
        false ->
            {State, Reply, Effs}
    end.

%% Guards on command fields keep an ill-typed command out of `do_apply/3`: a
%% crash here is a poison pill, re-crashing every replica on log replay. An
%% unmatched command falls through to the catch-all and returns
%% `{error, unknown_command}`.
do_apply(Meta, {grant_lease, ProposedId, TtlMs, Owner, Pid}, State0)
  when is_integer(TtlMs), TtlMs > 0,
       (Pid =:= undefined orelse is_pid(Pid)) ->
    Now = now_ms(Meta),
    LeaseId = case ProposedId of
                  undefined -> index(Meta);
                  _ -> ProposedId
              end,
    case maps:find(LeaseId, State0#?MODULE.leases) of
        {ok, #lease{owner = Owner} = L} ->
            %% Idempotent re-grant by the same owner: refresh the deadline,
            %% re-arming the monitor lost to a `noconnection`.
            L1 = L#lease{deadline = Now + TtlMs, ttl_ms = TtlMs},
            {State1, Effs} =
                ensure_monitor(L#lease.pid,
                               set_lease(LeaseId, L1, State0), []),
            {State1, {ok, LeaseId}, Effs};
        {ok, #lease{}} ->
            {State0, {error, id_in_use}};
        error ->
            L = #lease{id = LeaseId, ttl_ms = TtlMs, deadline = Now + TtlMs,
                       owner = Owner, pid = Pid},
            State1 = add_lease_pid(Pid, LeaseId, set_lease(LeaseId, L, State0)),
            {State2, Effs} = ensure_monitor(Pid, State1, []),
            %% The expiry timer runs only while leases exist (an idle cluster
            %% must not log one tick per second forever); the first lease
            %% arms it. A same-name timer effect resets a pending one.
            Timer = case map_size(State0#?MODULE.leases) of
                        0 -> [{timer, expire, State2#?MODULE.tick_ms}];
                        _ -> []
                    end,
            {State2, {ok, LeaseId}, Timer ++ Effs}
    end;
do_apply(Meta, {renew, LeaseIds}, State0) when is_list(LeaseIds) ->
    Now = now_ms(Meta),
    %% Renewal re-arms the monitor: `{down, noconnection}` drops the
    %% `monitored` entry, and a holder that keeps renewing after the
    %% reconnect is otherwise never re-monitored until a leader change.
    {State1, Results, MonEffs} =
        lists:foldl(
          fun(LeaseId, {S, Acc, Effs}) ->
                  case maps:find(LeaseId, S#?MODULE.leases) of
                      {ok, L} ->
                          L1 = L#lease{deadline = Now + L#lease.ttl_ms},
                          {S1, Effs1} =
                              ensure_monitor(L#lease.pid,
                                             set_lease(LeaseId, L1, S), Effs),
                          {S1, [{LeaseId, ok} | Acc], Effs1};
                      error ->
                          {S, [{LeaseId, {error, lease_expired}} | Acc], Effs}
                  end
          end, {State0, [], []}, LeaseIds),
    Effs = case lists:keymember(ok, 2, Results) of
               true -> [incr(renewals_total, State1) | MonEffs];
               false -> MonEffs
           end,
    {State1, lists:reverse(Results), Effs};
do_apply(Meta, {revoke_lease, LeaseId}, State0) ->
    {State1, Effs} = revoke_lease(Meta, LeaseId, #{LeaseId => true}, State0),
    {State1, ok, Effs};
do_apply(Meta, {acquire, LeaseId, LockKey, Owner, Context, Wait}, State0) ->
    do_acquire(Meta, LeaseId, LockKey, Owner, Context, Wait, 0, State0);
do_apply(Meta, {acquire, LeaseId, LockKey, Owner, Context, Wait, Score}, State0)
  when is_integer(Score) ->
    do_acquire(Meta, LeaseId, LockKey, Owner, Context, Wait, Score, State0);
do_apply(Meta, {release, LockKey, Token}, State0) ->
    %% Token-fenced: a stale token (the lock was reclaimed and re-granted)
    %% returns not_owner and does not release the current owner.
    case maps:find(LockKey, State0#?MODULE.locks) of
        {ok, LeaseId} ->
            Lease = maps:get(LeaseId, State0#?MODULE.leases),
            case maps:get(LockKey, Lease#lease.keys) of
                #held_lock{token = Token} ->
                    {State1, Effs} =
                        release_key(Meta, LockKey, LeaseId, #{}, State0),
                    {State1, ok, [incr(releases_total, State1) | Effs]};
                #held_lock{} ->
                    {State0, {error, not_owner}}
            end;
        error ->
            {State0, {error, not_held}}
    end;
%% Token-fenced like release: only the current holder transfers, and only to a
%% named contender. A free key or a stale token returns not_owner, a target
%% equal to the holder returns ok, and a target with no live contender is
%% refused.
do_apply(Meta, {transfer, LockKey, Token, TargetOwner}, State0) ->
    case maps:find(LockKey, State0#?MODULE.locks) of
        {ok, LeaseId} ->
            Lease = maps:get(LeaseId, State0#?MODULE.leases),
            case maps:get(LockKey, Lease#lease.keys) of
                #held_lock{token = Token, owner = TargetOwner} ->
                    {State0, ok};
                #held_lock{token = Token} ->
                    do_transfer(Meta, LockKey, LeaseId, TargetOwner, State0);
                #held_lock{} ->
                    {State0, {error, not_owner}}
            end;
        error ->
            {State0, {error, not_owner}}
    end;
%% A non-pid here would crash every successive leader through the monitor
%% effect and `state_enter/2`'s re-derivation, hence the guard.
do_apply(Meta, {watch, LockKey, Pid}, State0) when is_pid(Pid) ->
    Ref = index(Meta),
    Ws0 = maps:get(LockKey, State0#?MODULE.watchers, #{}),
    Ws1 = maps:put(Pid, Ref, Ws0),
    State1 = State0#?MODULE{watchers = maps:put(LockKey, Ws1,
                                                State0#?MODULE.watchers)},
    {State2, Effs} = ensure_monitor(Pid, State1, []),
    {State2, {ok, Ref}, Effs};
do_apply(_Meta, {unwatch, Ref}, State0) ->
    %% Drop the ref, then demonitor the pid if it now has no lease and no watch,
    %% so unwatch does not leak a monitor.
    {Watchers, Dropped} =
        maps:fold(
          fun(K, Ws, {Acc, Drop}) ->
                  Removed = [P || P := R <- Ws, R =:= Ref],
                  case maps:filter(fun(_P, R) -> R =/= Ref end, Ws) of
                      Kept when map_size(Kept) =:= 0 -> {Acc, Removed ++ Drop};
                      Kept -> {maps:put(K, Kept, Acc), Removed ++ Drop}
                  end
          end, {#{}, []}, State0#?MODULE.watchers),
    State1 = State0#?MODULE{watchers = Watchers},
    {State2, Effs} =
        lists:foldl(fun(P, {S, E}) ->
                            {S1, E1} = maybe_demonitor(P, S),
                            {S1, E1 ++ E}
                    end, {State1, []}, lists:usort(Dropped)),
    {State2, ok, Effs};
do_apply(Meta, {timeout, expire}, State0) ->
    Now = now_ms(Meta),
    %% Sorted so the revocation order is a pure function of replicated
    %% state, never of map iteration order, which can vary across versions.
    Expired = lists:sort(
                [LeaseId || {LeaseId, #lease{deadline = D}} <-
                                maps:to_list(State0#?MODULE.leases), D =< Now]),
    %% Expiry is the one loss nobody initiated, so the deposed holder is
    %% told directly instead of waiting out a renew interval. `local`
    %% delivers through the member on the holder's node, which may be the
    %% only path when the holder expired because it cannot reach the leader.
    Notices = lease_lost_msgs(Expired, State0),
    {State1, Effs} = revoke_leases(Meta, Expired, State0),
    ExpEff = case Expired of
                 [] -> [];
                 _ -> [incr(lease_expiries_total, State1)]
             end,
    %% Re-arm only while leases remain; the next grant re-arms an idle
    %% machine.
    Timer = case map_size(State1#?MODULE.leases) of
                0 -> [];
                _ -> [{timer, expire, State1#?MODULE.tick_ms}]
            end,
    {State1, ok, Notices ++ Effs ++ ExpEff ++ Timer};
%% Unreachable, not dead: the lease still decides. The cross-node monitor
%% auto-cleared with the connection, so drop the stale monitored entry; a later
%% grant, renew or watch re-arms it. A watch-only pid has no renewal path, so
%% its monitor comes back only at the next leader change — acceptable for
%% best-effort watches.
do_apply(_Meta, {down, Pid, noconnection}, State0) ->
    {State0#?MODULE{monitored = maps:remove(Pid, State0#?MODULE.monitored)}, ok};
%% A genuine local death: release the pid's leases (the monitor fast-path).
do_apply(Meta, {down, Pid, _Reason}, State0) ->
    {State1, Effs} = release_pid(Meta, Pid, State0),
    {State1, ok, Effs};
do_apply(_Meta, {nodeup, _Node}, State0) ->
    {State0, ok};
do_apply(_Meta, {nodedown, _Node}, State0) ->
    %% Unreachable, not dead. Leases expire via the tick if not renewed.
    {State0, ok};
do_apply(_Meta, _Unknown, State0) ->
    {State0, {error, unknown_command}}.

-spec state_enter(ra_server:ra_state() | eol, state()) ->
    ra_machine:effects().
state_enter(leader, State) ->
    %% A new leader re-derives its monitors from replicated state, and arms
    %% the expiry timer only if there are leases to expire.
    Mons = [{monitor, process, P} || P <- known_pids(State)],
    Timer = case map_size(State#?MODULE.leases) of
                0 -> [];
                _ -> [{timer, expire, State#?MODULE.tick_ms}]
            end,
    Cluster = State#?MODULE.cluster,
    Mons ++ Timer ++ [incr(leader_changes_total, State),
                      set_gauge(Cluster, is_leader, 1)];
state_enter(_, _State) ->
    %% No demotion gauge here: Ra runs `mod_call` effects only on the leader,
    %% so it would be dropped. The aux tick publishes `is_leader` per node
    %% and clears a deposed leader's within a tick.
    [].

-spec init_aux(atom()) -> undefined.
init_aux(_Name) ->
    undefined.

%% Runs on every member each tick (the per-member `{aux, tick}`), so each node
%% publishes its own gauges from its own replica rather than reading zero.
%% This cadence is Ra's `tick_timeout` (1000 ms), not the machine's
%% `tick_interval_ms`, which drives only the lease expiry sweep.
-spec handle_aux(ra_server:ra_state(), term(), term(), undefined,
                 ra_aux:internal_state()) ->
    {no_reply, undefined, ra_aux:internal_state()}.
handle_aux(_RaState, _Type, tick, Aux, RaAux) ->
    %% Publishing metrics must never crash the Ra server, so it is best-effort.
    _ = try
            Mac = ra_aux:machine_state(RaAux),
            portunus_counters:set_gauges(Mac#?MODULE.cluster, node_gauges(Mac, RaAux))
        catch _:_ -> ok
        end,
    {no_reply, Aux, RaAux};
handle_aux(_RaState, _Type, _Cmd, Aux, RaAux) ->
    {no_reply, Aux, RaAux}.

%% This node's gauge values: machine counts from its replica, Raft figures from
%% the local server, membership from the cluster configuration.
-spec node_gauges(state(), ra_aux:internal_state()) -> #{atom() => integer()}.
node_gauges(#?MODULE{cluster = Cluster} = Mac, RaAux) ->
    #{locks := Locks, leases := Leases, waiters := Waiters,
      fencing_token := Token} = overview(Mac),
    KM = ra:key_metrics({Cluster, node()}),
    Commit = maps:get(commit_index, KM, 0),
    Applied = maps:get(last_applied, KM, 0),
    Snapshot = maps:get(snapshot_index, KM, 0),
    Last = maps:get(last_index, KM, 0),
    LeaderId = ra_aux:leader_id(RaAux),
    HasQuorum = case LeaderId of
                    undefined -> 0;
                    _ -> 1
                end,
    IsLeader = case LeaderId of
                   {Cluster, N} when N =:= node() -> 1;
                   _ -> 0
               end,
    #{locks_held => Locks,
      leases_active => Leases,
      waiters => Waiters,
      fencing_token => Token,
      raft_term => maps:get(term, KM, 0),
      apply_lag => max(0, Commit - Applied),
      log_entries => max(0, Last - Snapshot),
      cluster_members => map_size(ra_aux:members_info(RaAux)),
      has_quorum => HasQuorum,
      is_leader => IsLeader}.

-spec overview(state()) -> map().
overview(State) ->
    Waiters = maps:fold(fun(_K, Ws, Acc) -> Acc + length(Ws) end, 0,
                        State#?MODULE.leader_succession_queue),
    #{leases => maps:size(State#?MODULE.leases),
      locks => maps:size(State#?MODULE.locks),
      waiters => Waiters,
      watchers => maps:size(State#?MODULE.watchers),
      fencing_token => State#?MODULE.max_token}.

-spec version() -> ra_machine:version().
version() -> 0.

-spec which_module(ra_machine:version()) -> module().
which_module(0) -> ?MODULE.

%%----------------------------------------------------------------------
%% Query funs (run on a replica via ra:consistent_query/local_query)
%%----------------------------------------------------------------------

-spec query_owner(lock_key(), state()) ->
    {ok, owner_info()} | {error, not_held}.
query_owner(LockKey, State) ->
    case maps:find(LockKey, State#?MODULE.locks) of
        {ok, LeaseId} ->
            Lease = maps:get(LeaseId, State#?MODULE.leases),
            #held_lock{token = T, owner = O, context = C} =
                maps:get(LockKey, Lease#lease.keys),
            %% Remaining TTL is liveness, so a node-local clock read in a query
            %% (never in `apply/3`) is acceptable; it is approximate.
            Remaining = max(0, Lease#lease.deadline -
                                erlang:system_time(millisecond)),
            {ok, #{owner => O, lease => LeaseId, token => T, context => C,
                   remaining_ms => Remaining}};
        error ->
            {error, not_held}
    end.

%% Live contenders (their owner terms) for a key, read by the transfer
%% pre-check (`portunus_election:transfer_to/2`). A waiter whose lease is gone
%% is not a viable target, so it is left out.
-spec query_contenders(lock_key(), state()) -> [owner()].
query_contenders(LockKey, State) ->
    Ws = maps:get(LockKey, State#?MODULE.leader_succession_queue, []),
    [W#waiter.owner || W <- Ws,
                       maps:is_key(W#waiter.lease_id, State#?MODULE.leases)].

-spec query_status(state()) -> map().
query_status(State) ->
    overview(State).

%%----------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------

now_ms(Meta) -> maps:get(system_time, Meta).

index(Meta) -> maps:get(index, Meta).

set_lease(LeaseId, Lease, State) ->
    State#?MODULE{leases = maps:put(LeaseId, Lease, State#?MODULE.leases)}.

set_lock(LockKey, LeaseId, State) ->
    State#?MODULE{locks = maps:put(LockKey, LeaseId, State#?MODULE.locks)}.

held_owner(LeaseId, LockKey, State) ->
    Lease = maps:get(LeaseId, State#?MODULE.leases),
    (maps:get(LockKey, Lease#lease.keys))#held_lock.owner.

add_lease_pid(undefined, _LeaseId, State) ->
    State;
add_lease_pid(Pid, LeaseId, State) ->
    Inner = maps:get(Pid, State#?MODULE.lease_pids, #{}),
    State#?MODULE{lease_pids = maps:put(Pid, maps:put(LeaseId, true, Inner),
                                        State#?MODULE.lease_pids)}.

del_lease_pid(Pid, LeaseId, #?MODULE{lease_pids = LeasePids} = State) ->
    case LeasePids of
        #{Pid := #{LeaseId := true} = Inner} ->
            LP = case maps:remove(LeaseId, Inner) of
                     Empty when map_size(Empty) =:= 0 -> maps:remove(Pid, LeasePids);
                     Inner1 -> LeasePids#{Pid := Inner1}
                 end,
            State#?MODULE{lease_pids = LP};
        _ ->
            State
    end.

%% Avoid emitting a duplicate monitor effect for an already-monitored pid.
ensure_monitor(undefined, State, Effs) ->
    {State, Effs};
ensure_monitor(Pid, State, Effs) ->
    case maps:is_key(Pid, State#?MODULE.monitored) of
        true ->
            {State, Effs};
        false ->
            {State#?MODULE{monitored = maps:put(Pid, true,
                                                State#?MODULE.monitored)},
             [{monitor, process, Pid} | Effs]}
    end.

do_acquire(Meta, LeaseId, LockKey, Owner, Context, Wait, Score, State0) ->
    case maps:find(LeaseId, State0#?MODULE.leases) of
        error ->
            {State0, {error, lease_expired}};
        {ok, Lease} ->
            case maps:find(LockKey, State0#?MODULE.locks) of
                {ok, LeaseId} ->
                    %% The same lease re-acquiring its own key is
                    %% idempotent: it returns the existing token and does
                    %% not change the stored context.
                    #held_lock{token = T} = maps:get(LockKey, Lease#lease.keys),
                    {State0, {ok, T}};
                {ok, OtherId} ->
                    case Wait of
                        wait ->
                            W = #waiter{lease_id = LeaseId, owner = Owner,
                                        context = Context, score = Score,
                                        seq = index(Meta)},
                            State1 = enqueue_waiter(LockKey, W, State0),
                            %% The number of succession candidates on the key, not this
                            %% caller's place in line: score decides order.
                            Q = State1#?MODULE.leader_succession_queue,
                            Depth = length(maps:get(LockKey, Q)),
                            {State1, {queued, Depth}};
                        _ ->
                            HeldBy = held_owner(OtherId, LockKey, State0),
                            {State0, {error, {held_by, HeldBy}},
                             [incr(acquire_conflicts_total, State0)]}
                    end;
                error ->
                    Token = index(Meta),
                    Held = #held_lock{token = Token, owner = Owner,
                                 context = Context, since = now_ms(Meta)},
                    Lease1 = Lease#lease{keys = maps:put(LockKey, Held,
                                                         Lease#lease.keys)},
                    State1 = bump_token(Token,
                                        set_lock(LockKey, LeaseId,
                                                 set_lease(LeaseId, Lease1,
                                                           State0))),
                    Effs = notify_watchers(LockKey, {acquired, Owner}, State1),
                    {State1, {ok, Token}, [incr(acquires_total, State1) | Effs]}
            end
    end.

%% A lease has at most one succession candidate per key: a re-acquire while queued
%% refreshes its bid (score and seq) rather than adding a duplicate.
enqueue_waiter(LockKey, #waiter{lease_id = LeaseId} = Waiter, State) ->
    Q = State#?MODULE.leader_succession_queue,
    Ws0 = maps:get(LockKey, Q, []),
    Ws1 = [W || W <- Ws0, W#waiter.lease_id =/= LeaseId],
    State#?MODULE{leader_succession_queue =
                      maps:put(LockKey, [Waiter | Ws1], Q)}.

%% Release a single held key, then promote the next live succession
%% candidate (if any).
release_key(Meta, LockKey, LeaseId, Revoking, State0) ->
    Lease = maps:get(LeaseId, State0#?MODULE.leases),
    Lease1 = Lease#lease{keys = maps:remove(LockKey, Lease#lease.keys)},
    State1 = set_lease(LeaseId, Lease1, State0),
    State2 = State1#?MODULE{locks = maps:remove(LockKey,
                                                State1#?MODULE.locks)},
    Effs0 = notify_watchers(LockKey, released, State2),
    promote_waiter(Meta, LockKey, Revoking, State2, Effs0).

%% Grant the freed lock to the highest-scoring live succession candidate
%% (ties break to the earliest arrival). Skipped: candidates whose lease is
%% gone, and candidates
%% whose lease is in `Revoking` — being revoked by this same command. Such a
%% promotion would be revoked again a fold step later, and its re-promotion
%% would mint a second token for the key at the same Raft index, breaking
%% per-key token monotonicity.
promote_waiter(Meta, LockKey, Revoking, State0, Effs) ->
    Ws0 = maps:get(LockKey, State0#?MODULE.leader_succession_queue, []),
    Live = [W || W <- Ws0,
                 maps:is_key(W#waiter.lease_id, State0#?MODULE.leases),
                 not is_map_key(W#waiter.lease_id, Revoking)],
    case Live of
        [] ->
            {set_waiters(LockKey, [], State0), Effs};
        _ ->
            #waiter{lease_id = LeaseId, seq = Seq} = Best = best_waiter(Live),
            Rest = [W || W <- Live, W#waiter.seq =/= Seq],
            State1 = set_waiters(LockKey, Rest, State0),
            Lease = maps:get(LeaseId, State1#?MODULE.leases),
            Token = index(Meta),
            Held = #held_lock{token = Token, owner = Best#waiter.owner,
                         context = Best#waiter.context, since = now_ms(Meta)},
            Lease1 = Lease#lease{keys = maps:put(LockKey, Held,
                                                 Lease#lease.keys)},
            State2 = bump_token(Token,
                                set_lock(LockKey, LeaseId,
                                         set_lease(LeaseId, Lease1, State1))),
            Effs1 = notify_watchers(LockKey, {acquired, Best#waiter.owner},
                                    State2),
            GrantEffs = grant_msg(Lease#lease.pid, LockKey, Token, LeaseId),
            %% Released before acquired, so a watcher's last event on a handoff
            %% reflects the new owner, not a key that looks free.
            {State2, [incr(acquires_total, State2) | GrantEffs] ++ Effs ++ Effs1}
    end.

%% Targeted transfer: promote the highest-ranked live waiter whose owner is
%% `TargetOwner`, removing the current holder in the same transition. The key
%% is never free and never doubly held, and the new token comes from this
%% command's index, so per-key monotonicity holds exactly as for a lapse.
do_transfer(Meta, LockKey, OldLeaseId, TargetOwner, State0) ->
    Ws0 = maps:get(LockKey, State0#?MODULE.leader_succession_queue, []),
    Matching = [W || W <- Ws0,
                     W#waiter.owner =:= TargetOwner,
                     maps:is_key(W#waiter.lease_id, State0#?MODULE.leases)],
    case Matching of
        [] ->
            {State0, {error, {no_contender, TargetOwner}},
             [incr(transfer_no_contender_total, State0)]};
        _ ->
            #waiter{lease_id = NewLeaseId, seq = Seq} = Best = best_waiter(Matching),
            %% Drop the key from the current holder; keep the other live
            %% waiters, pruning dead ones as a lapse promotion does.
            OldLease = maps:get(OldLeaseId, State0#?MODULE.leases),
            OldLease1 = OldLease#lease{keys = maps:remove(LockKey,
                                                          OldLease#lease.keys)},
            Rest = [W || W <- Ws0, W#waiter.seq =/= Seq,
                         maps:is_key(W#waiter.lease_id, State0#?MODULE.leases)],
            State1 = set_waiters(LockKey, Rest,
                                 set_lease(OldLeaseId, OldLease1, State0)),
            %% Install the target as the new holder with a fresh token.
            NewLease = maps:get(NewLeaseId, State1#?MODULE.leases),
            Token = index(Meta),
            Held = #held_lock{token = Token, owner = Best#waiter.owner,
                              context = Best#waiter.context, since = now_ms(Meta)},
            NewLease1 = NewLease#lease{keys = maps:put(LockKey, Held,
                                                       NewLease#lease.keys)},
            State2 = bump_token(Token,
                                set_lock(LockKey, NewLeaseId,
                                         set_lease(NewLeaseId, NewLease1, State1))),
            Effs = notify_watchers(LockKey, {acquired, Best#waiter.owner}, State2),
            GrantEffs = grant_msg(NewLease#lease.pid, LockKey, Token, NewLeaseId),
            {State2, ok, [incr(transfers_total, State2) | GrantEffs] ++ Effs}
    end.

best_waiter([W | Ws]) ->
    lists:foldl(fun(C, Best) ->
                        case rank(C) > rank(Best) of
                            true -> C;
                            false -> Best
                        end
                end, W, Ws).

%% Highest score first; for equal scores, the lowest seq (earliest) wins.
rank(#waiter{score = Score, seq = Seq}) -> {Score, -Seq}.

set_waiters(LockKey, [], State) ->
    Q = maps:remove(LockKey, State#?MODULE.leader_succession_queue),
    State#?MODULE{leader_succession_queue = Q};
set_waiters(LockKey, Ws, State) ->
    Q = maps:put(LockKey, Ws, State#?MODULE.leader_succession_queue),
    State#?MODULE{leader_succession_queue = Q}.

%% The lease id lets a succession candidate tell a fresh grant from one
%% minted for an
%% earlier, since-revoked attempt that is still in flight.
grant_msg(undefined, _LockKey, _Token, _LeaseId) ->
    [];
grant_msg(Pid, LockKey, Token, LeaseId) ->
    [{send_msg, Pid, {portunus, granted, LockKey, Token, LeaseId}}].

%% The same message the renewer delivers on a failed renew; receivers
%% already treat the duplicate as a no-op.
lease_lost_msgs(LeaseIds, State) ->
    lists:filtermap(
      fun(LeaseId) ->
              case maps:get(LeaseId, State#?MODULE.leases) of
                  #lease{pid = Pid} when is_pid(Pid) ->
                      {true, {send_msg, Pid,
                              {portunus, lease_lost, LeaseId}, [local]}};
                  #lease{} ->
                      false
              end
      end, LeaseIds).

%% Revoke each lease in turn. Effect groups are collected reversed and
%% appended once, so the emitted order follows the revocation order and the
%% accumulation stays linear: a watcher's last event on a chained handoff
%% must name the final owner.
revoke_leases(Meta, LeaseIds, State0) ->
    Revoking = maps:from_keys(LeaseIds, true),
    {State, Groups} =
        lists:foldl(fun(LeaseId, {S, Acc}) ->
                            {S1, E1} = revoke_lease(Meta, LeaseId, Revoking, S),
                            {S1, [E1 | Acc]}
                    end, {State0, []}, LeaseIds),
    {State, lists:append(lists:reverse(Groups))}.

%% Release each of one lease's keys, promoting each key's next candidate.
release_keys(Meta, LockKeys, LeaseId, Revoking, State0) ->
    {State, Groups} =
        lists:foldl(fun(LockKey, {S, Acc}) ->
                            {S1, E1} = release_key(Meta, LockKey, LeaseId,
                                                   Revoking, S),
                            {S1, [E1 | Acc]}
                    end, {State0, []}, LockKeys),
    {State, lists:append(lists:reverse(Groups))}.

%% Revoke a whole lease: release every key it holds (promoting
%% candidates),
%% drop its pid index entry, and remove the lease.
revoke_lease(Meta, LeaseId, Revoking, State0) ->
    case maps:find(LeaseId, State0#?MODULE.leases) of
        error ->
            {State0, []};
        {ok, #lease{pid = Pid, keys = Keys}} ->
            %% Sorted so the effect order is a pure function of state, not of
            %% map iteration order.
            {State1, Effs} = release_keys(Meta, lists:sort(maps:keys(Keys)),
                                          LeaseId, Revoking, State0),
            State2 = State1#?MODULE{leases = maps:remove(LeaseId,
                                                         State1#?MODULE.leases)},
            State3 = drop_waiters_of(LeaseId,
                                     del_lease_pid(Pid, LeaseId, State2)),
            {State4, DemonEffs} = maybe_demonitor(Pid, State3),
            {State4, Effs ++ DemonEffs}
    end.

%% Drop a gone lease's queue entries on keys it never acquired, so a lease
%% that gave up waiting and was revoked does not linger until the holder
%% next releases.
drop_waiters_of(LeaseId, State) ->
    maps:fold(fun(LockKey, Ws, S) ->
                      case [W || W <- Ws, W#waiter.lease_id =/= LeaseId] of
                          Ws -> S;
                          Kept -> set_waiters(LockKey, Kept, S)
                      end
              end, State, State#?MODULE.leader_succession_queue).

%% Release all leases held by a dead pid and drop it from watch sets.
release_pid(Meta, Pid, State0) ->
    %% Sorted for an iteration-order-independent revocation order.
    LeaseIds = lists:sort(maps:keys(maps:get(Pid, State0#?MODULE.lease_pids, #{}))),
    {State1, Effs} = revoke_leases(Meta, LeaseIds, State0),
    State2 = drop_watcher(Pid, State1),
    State3 = State2#?MODULE{monitored = maps:remove(Pid,
                                                    State2#?MODULE.monitored)},
    {State3, Effs}.

drop_watcher(Pid, State) ->
    Watchers = maps:fold(
                 fun(K, Ws, Acc) ->
                         case maps:remove(Pid, Ws) of
                             Empty when map_size(Empty) =:= 0 -> Acc;
                             Ws1 -> maps:put(K, Ws1, Acc)
                         end
                 end, #{}, State#?MODULE.watchers),
    State#?MODULE{watchers = Watchers}.

notify_watchers(LockKey, Event, State) ->
    Ws = maps:get(LockKey, State#?MODULE.watchers, #{}),
    maps:fold(fun(Pid, Ref, Acc) ->
                      [{send_msg, Pid, {portunus, watch, Ref, Event}} | Acc]
              end, [], Ws).

known_pids(State) ->
    LeasePids = maps:keys(State#?MODULE.lease_pids),
    WatchPids = maps:fold(fun(_K, Ws, Acc) -> maps:keys(Ws) ++ Acc end, [],
                          State#?MODULE.watchers),
    lists:usort(LeasePids ++ WatchPids).

%% A leader-only effect that bumps a seshat counter once per event.
incr(Field, State) ->
    {mod_call, portunus_counters, incr, [State#?MODULE.cluster, Field]}.

%% A leader-only effect that sets a gauge to a value.
set_gauge(Cluster, Field, Value) ->
    {mod_call, portunus_counters, set_gauge, [Cluster, Field, Value]}.

bump_token(Token, State) ->
    State#?MODULE{max_token = max(Token, State#?MODULE.max_token)}.

%% Demonitor a pid once it holds no lease and no watch, so an expired-but-alive
%% holder leaks no monitor and is re-monitored after a later leader change.
maybe_demonitor(undefined, State) ->
    {State, []};
maybe_demonitor(Pid, State) ->
    case still_referenced(Pid, State) of
        true ->
            {State, []};
        false ->
            {State#?MODULE{monitored = maps:remove(Pid, State#?MODULE.monitored)},
             [{demonitor, process, Pid}]}
    end.

still_referenced(Pid, State) ->
    maps:is_key(Pid, State#?MODULE.lease_pids) orelse watches_any(Pid, State).

watches_any(Pid, State) ->
    maps:fold(fun(_K, Ws, Acc) -> Acc orelse maps:is_key(Pid, Ws) end,
              false, State#?MODULE.watchers).
