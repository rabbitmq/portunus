%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_machine).
-moduledoc """
The `portunus` Ra state machine. It manages leases, locks, fencing
tokens, and a score-ordered succession queue (FIFO among equal scores).

Key decisions:

1. `apply/3` never reads node-local time, only the leader-stamped `system_time` in the command metadata.
2. Never uses `make_ref/0`, `self/0`, and derives tokens and IDs from the Raft
   log `index`, packed with a per-incarnation (think restarts) epoch (see `token_info/1`)

This main goal of having per-incarnation epochs is to make sure
that higher epochs result in higher fencing tokens produced.

Lease renewal and expiry timing live off the Raft log, in per-server aux
state (see `portunus_machine_aux`): renewals arrive over
`ra:consistent_aux/3` and move an in-memory deadline on the leader, and
the leader's aux tick proposes `{expire_leases, ...}` commands for leases
whose deadline passed. Replicated state keeps, per lease, only the
`refreshed` index of the last logged command that refreshed it (its
grant, initial or idempotent); an expiry proposal is fenced with that
index, so a proposal outrun by a re-grant is skipped by `apply/3`.
Steady-state renewal therefore appends nothing and triggers no `fsync(2)`
on any member. The trade-off, the same one etcd makes: a leader change can
extend a lease by up to one full TTL (late expiry only, never early).
""".

-behaviour(ra_machine).

-export([init/1,
         apply/3,
         state_enter/2,
         init_aux/1,
         handle_aux/5,
         overview/1]).

%% Exported for queries run via `ra:consistent_query/3` or `ra:local_query/3`.
-export([query_owner/2,
         query_contenders/2,
         query_status/1]).

-export([token_info/1]).

%% For testing of
%% `portunus_machine_aux` without a Ra cluster.
-export([lease_view/1]).

-define(DEFAULT_SNAPSHOT_INTERVAL, 4096).
%% Client-facing identifiers are `(Epoch bsl ?EPOCH_SHIFT) + Index`; 64 bits
%% holds any real log's index.
-define(EPOCH_SHIFT, 64).

-type lock_key() :: term().
-type lease_id() :: term().
-type token() :: non_neg_integer().
%% A watch registration handle: an epoch-packed Raft index.
-type watch_ref() :: non_neg_integer().
-type owner() :: term().
-type owner_info() :: #{owner := owner(), lease := lease_id(),
                        token := token(), context := term()}.

%% Fencing tokens are *not* opaque by design: a client fences an external
%% write by comparing them.
-export_type([lock_key/0, lease_id/0, token/0, watch_ref/0, owner/0,
              owner_info/0]).

-type command() ::
        {grant_lease, portunus:option(lease_id()), pos_integer(), owner(),
         portunus:option(pid())} |
        {revoke_lease, lease_id()} |
        {acquire, lease_id(), lock_key(), owner(), term(), wait | nowait} |
        {acquire, lease_id(), lock_key(), owner(), term(), wait | nowait,
         integer()} |
        {release, lock_key(), token()} |
        {transfer, lock_key(), token(), owner()} |
        {leave_queue, lock_key(), lease_id()} |
        {watch, lock_key(), pid()} |
        {unwatch, watch_ref()} |
        {expire_leases, [portunus_machine_aux:expire_pair()]} |
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

%% `refreshed` is the Raft index of the last logged command that refreshed
%% the lease (its grant, initial or idempotent); the aux sweep fences its
%% expiry proposals with it. The operative deadline lives in leader aux
%% state.
-record(lease, {id :: lease_id(),
                ttl_ms :: pos_integer(),
                refreshed = 0 :: ra:index(),
                owner :: owner(),
                pid :: portunus:option(pid()),
                keys = #{} :: #{lock_key() => #held_lock{}}}).

-record(?MODULE, {cluster :: atom(),
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
                  %% the fencing epoch: the leader-stamped `system_time` of this
                  %% incarnation's first applied command, packed into every
                  %% client-facing identifier so a re-formed cluster mints above
                  %% the dead incarnation's fences
                  epoch = 0 :: non_neg_integer(),
                  %% the highest fencing token produced so far, exposed as a metric (a gauge)
                  max_token = 0 :: token()}).

-opaque state() :: #?MODULE{}.
-export_type([state/0]).

%%
%% ra_machine callbacks
%%

%% Ra merges `name` and `machine_version` into the init args, so the
%% callback must accept the wider machine-init map (it carries `cluster`
%% from the cluster's machine config).
-spec init(map()) -> state().
init(Config) ->
    #?MODULE{cluster = maps:get(cluster, Config, portunus),
             snapshot_interval = maps:get(snapshot_interval, Config,
                                          ?DEFAULT_SNAPSHOT_INTERVAL)}.

-spec apply(ra_machine:command_meta_data(), command(), state()) ->
    {state(), term(), ra_machine:effects()}.
apply(Meta, Cmd, State0) ->
    maybe_release_cursor(Meta, do_apply(Meta, Cmd, ensure_epoch(Meta, State0))).

%% The first applied command with a positive leader-stamped `system_time`
%% sets the epoch. The stamp is in the log, so every replica, replay and
%% snapshot recovery derives the same value; the positive guard means a
%% pathological clock degrades to raw-index identifiers instead of minting
%% negative ones.
ensure_epoch(Meta, #?MODULE{epoch = 0} = State) ->
    case maps:get(system_time, Meta, 0) of
        T when T > 0 -> State#?MODULE{epoch = T};
        _ -> State
    end;
ensure_epoch(_Meta, State) ->
    State.

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
    LeaseId = case ProposedId of
                  undefined -> packed_index(Meta, State0);
                  _ -> ProposedId
              end,
    %% Both grant outcomes are logged refreshes: they stamp `refreshed` and
    %% extend the leader's aux deadline. Without the extension, a re-granted
    %% lease whose old aux deadline had passed would be proposed for expiry
    %% on the next tick, seconds after a successful grant.
    Refreshed = refreshed_effect([LeaseId]),
    case maps:find(LeaseId, State0#?MODULE.leases) of
        {ok, #lease{owner = Owner} = L} ->
            %% Idempotent re-grant by the same owner: refresh the lease,
            %% re-arming the monitor lost to a `noconnection`. The command's
            %% pid is ignored: through the public API the owner is the pid,
            %% so a same-owner re-grant always carries the same pid.
            L1 = L#lease{refreshed = index(Meta), ttl_ms = TtlMs},
            {State1, Effs} =
                ensure_monitor(L#lease.pid,
                               set_lease(LeaseId, L1, State0), []),
            {State1, {ok, LeaseId}, Refreshed ++ Effs};
        {ok, #lease{}} ->
            {State0, {error, id_in_use}};
        error ->
            L = #lease{id = LeaseId, ttl_ms = TtlMs,
                       refreshed = index(Meta), owner = Owner, pid = Pid},
            State1 = add_lease_pid(Pid, LeaseId, set_lease(LeaseId, L, State0)),
            {State2, Effs} = ensure_monitor(Pid, State1, []),
            {State2, {ok, LeaseId}, Refreshed ++ Effs}
    end;
%% The aux sweep's expiry proposal. Each pair is fenced with the `refreshed`
%% index the sweep read from applied state: a re-grant that committed after
%% the sweep changed that index, so a mismatching pair is skipped and the
%% live lease survives; the rest of the batch still applies.
do_apply(Meta, {expire_leases, Pairs}, State0) when is_list(Pairs) ->
    Expired = lists:usort(
                [Id || {Id, Fence} <- Pairs,
                       case maps:find(Id, State0#?MODULE.leases) of
                           {ok, #lease{refreshed = Fence}} -> true;
                           _ -> false
                       end]),
    Notices = lease_lost_msgs(Expired, State0),
    {State1, Effs} = revoke_leases(Meta, Expired, State0),
    ExpEff = case Expired of
                 [] -> [];
                 _ -> [incr(lease_expiries_total, State1)]
             end,
    {State1, ok, Notices ++ Effs ++ ExpEff};
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
    %% returns `not_owner` and does not release the current owner.
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
%% named contender. A free key or a stale token returns `not_owner`, a target
%% equal to the holder returns `ok`, and a target with no live contender is
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
%% Withdraw a lease's succession bid on one key: `release` for waiters. No
%% token moves and no promotion runs, so the key's holder is untouched. A
%% lease with no bid on the key (including the holder itself) gets
%% `{error, not_queued}` and nothing changes.
do_apply(_Meta, {leave_queue, LockKey, LeaseId}, State0) ->
    Ws0 = maps:get(LockKey, State0#?MODULE.leader_succession_queue, []),
    case [W || W <- Ws0, W#waiter.lease_id =:= LeaseId] of
        [] ->
            {State0, {error, not_queued}};
        _ ->
            Ws1 = [W || W <- Ws0, W#waiter.lease_id =/= LeaseId],
            State1 = set_waiters(LockKey, Ws1, State0),
            {State1, ok, [incr(queue_leaves_total, State1)]}
    end;
%% A non-pid here would crash every successive leader through the monitor
%% effect and `state_enter/2`'s re-derivation, hence the guard.
do_apply(Meta, {watch, LockKey, Pid}, State0) when is_pid(Pid) ->
    Ref = packed_index(Meta, State0),
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
                  case #{P => R || P := R <- Ws, R =/= Ref} of
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
%% Unreachable, not dead: the lease still decides. The cross-node monitor
%% auto-cleared with the connection, so drop the stale monitored entry; a
%% later grant or watch re-arms it, and everything else is re-monitored at
%% the next leader change. Renewals are aux-side and cannot re-arm; a holder
%% that dies inside that window is still bounded by lease expiry, since the
%% monitor is only the fast path.
do_apply(_Meta, {down, Pid, noconnection}, State0) ->
    {State0#?MODULE{monitored = maps:remove(Pid, State0#?MODULE.monitored)}, ok};
%% A genuine local death: release the pid's leases (the monitor fast-path).
do_apply(Meta, {down, Pid, _Reason}, State0) ->
    {State1, Effs} = release_pid(Meta, Pid, State0),
    {State1, ok, Effs};
do_apply(_Meta, {nodeup, _Node}, State0) ->
    {State0, ok};
do_apply(_Meta, {nodedown, _Node}, State0) ->
    %% Unreachable, not dead. Leases expire via the aux sweep if not
    %% renewed.
    {State0, ok};
do_apply(_Meta, _Unknown, State0) ->
    {State0, {error, unknown_command}}.

-spec state_enter(ra_server:ra_state() | eol, state()) ->
    ra_machine:effects().
state_enter(leader, State) ->
    %% A new leader re-derives its monitors from replicated state. No expiry
    %% timer: the aux sweep runs on Ra's tick and seeds its deadlines from
    %% this state.
    Mons = [{monitor, process, P} || P <- known_pids(State)],
    Cluster = State#?MODULE.cluster,
    Mons ++ [incr(leader_changes_total, State),
             set_gauge(Cluster, is_leader, 1)];
state_enter(_, _State) ->
    %% No demotion gauge here: Ra runs `mod_call` effects only on the leader,
    %% so it would be dropped. The aux tick publishes `is_leader` per node
    %% and clears a deposed leader's within a tick.
    [].

-spec init_aux(atom()) -> portunus_machine_aux:aux().
init_aux(_Name) ->
    portunus_machine_aux:new().

%% The per-member `{aux, tick}` runs on every member on Ra's `tick_timeout`
%% (1000 ms) with no log write: each node publishes its own gauges from its
%% own replica, and the leader also runs the expiry sweep. The renewal and
%% sweep decisions are pure functions in `portunus_machine_aux`; this
%% callback extracts their inputs and turns their outputs into effects.
-spec handle_aux(ra_server:ra_state(), term(), term(),
                 portunus_machine_aux:aux(),
                 ra_aux:internal_state()) ->
    {no_reply, portunus_machine_aux:aux(), ra_aux:internal_state()} |
    {no_reply, portunus_machine_aux:aux(), ra_aux:internal_state(),
     ra_machine:effects()} |
    {reply, term(), portunus_machine_aux:aux(), ra_aux:internal_state()}.
handle_aux(RaState, _Type, tick, Aux0, RaAux) ->
    %% Publishing metrics must never crash the Ra server, so it is best-effort.
    _ = try
            Mac0 = ra_aux:machine_state(RaAux),
            portunus_counters:set_gauges(Mac0#?MODULE.cluster,
                                         node_gauges(Mac0, RaAux))
        catch _:_ -> ok
        end,
    case RaState of
        leader ->
            Mac = ra_aux:machine_state(RaAux),
            {Aux, Pairs} =
                portunus_machine_aux:leader_tick(Aux0, lease_view(Mac),
                                                 ra_aux:current_term(RaAux),
                                                 mono_ms()),
            Effs = case Pairs of
                       [] -> [];
                       _ -> [{append, {expire_leases, Pairs}, noreply}]
                   end,
            {no_reply, Aux, RaAux, Effs};
        _ ->
            {no_reply, portunus_machine_aux:non_leader_tick(Aux0), RaAux}
    end;
%% The renewal transport (`ra:consistent_aux/3`). Ra runs it on the
%% leader after a heartbeat round confirmed a live quorum, so an `ok` here
%% is as trustworthy as a committed command's reply. Aux state is not
%% replicated, so monotonic time is fine here; the determinism rules apply
%% to `apply/3` only.
handle_aux(leader, _Type, {renew, LeaseIds}, Aux0, RaAux)
  when is_list(LeaseIds) ->
    Mac = ra_aux:machine_state(RaAux),
    {Aux, Results} =
        portunus_machine_aux:renew(Aux0, lease_view(Mac),
                                   ra_aux:current_term(RaAux),
                                   mono_ms(), LeaseIds),
    %% Bumped directly, not as an effect: aux runs only live, never in
    %% replay, and only on this node.
    case lists:keymember(ok, 2, Results) of
        true -> portunus_counters:incr(Mac#?MODULE.cluster, renewals_total);
        false -> ok
    end,
    {reply, Results, Aux, RaAux};
%% Not the leader: a plain aux command could still land here, and a
%% non-leader's aux state holds no operative deadlines. The client reads
%% any non-list reply as a transient quorum failure.
handle_aux(_RaState, _Type, {renew, LeaseIds}, Aux, RaAux)
  when is_list(LeaseIds) ->
    {reply, {error, not_leader}, Aux, RaAux};
%% A grant committed: extend the aux deadlines to the full TTL. Emitted by
%% `apply/3`, executed on the leader only, and never during replay.
handle_aux(leader, _Type, {refreshed, LeaseIds}, Aux0, RaAux)
  when is_list(LeaseIds) ->
    Mac = ra_aux:machine_state(RaAux),
    Aux = portunus_machine_aux:refreshed(Aux0, lease_view(Mac),
                                         ra_aux:current_term(RaAux),
                                         mono_ms(), LeaseIds),
    {no_reply, Aux, RaAux};
handle_aux(_RaState, _Type, _Cmd, Aux, RaAux) ->
    {no_reply, Aux, RaAux}.

%% The applied leases as the aux core's view.
-spec lease_view(state()) -> portunus_machine_aux:lease_view().
lease_view(#?MODULE{leases = Leases}) ->
    #{Id => {L#lease.ttl_ms, L#lease.refreshed} || Id := L <- Leases}.

mono_ms() ->
    erlang:monotonic_time(millisecond).

%% This node's gauge values: machine counts from its replica, Raft figures from
%% the local server, membership from the cluster configuration.
-spec node_gauges(state(), ra_aux:internal_state()) -> #{atom() => integer()}.
node_gauges(#?MODULE{cluster = Cluster} = Mac, RaAux) ->
    #{locks := Locks, leases := Leases, waiters := Waiters,
      fencing_token := Token} = overview(Mac),
    %% Seshat gauges are 64-bit atomics: a packed token does not fit and
    %% would wrap silently, so publish its components.
    #{epoch := TokenEpoch, index := TokenIndex} = token_info(Token),
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
      fencing_token => TokenIndex,
      fencing_epoch => TokenEpoch,
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

-doc """
Decompose an epoch-packed identifier (a fencing token, an auto-assigned
lease id, or a watch reference) for logging and debugging. An epoch of `0`
means the identifier was minted before the incarnation had a stamp.
""".
-spec token_info(token()) ->
    #{epoch := non_neg_integer(), index := non_neg_integer()}.
token_info(Token) when is_integer(Token), Token >= 0 ->
    #{epoch => Token bsr ?EPOCH_SHIFT,
      index => Token band ((1 bsl ?EPOCH_SHIFT) - 1)}.

%%----------------------------------------------------------------------
%% Query funs (run on a replica via `ra:consistent_query/3` or `ra:local_query/3`)
%%----------------------------------------------------------------------

-spec query_owner(lock_key(), state()) ->
    {ok, owner_info()} | {error, not_held}.
query_owner(LockKey, State) ->
    case maps:find(LockKey, State#?MODULE.locks) of
        {ok, LeaseId} ->
            Lease = maps:get(LeaseId, State#?MODULE.leases),
            #held_lock{token = T, owner = O, context = C} =
                maps:get(LockKey, Lease#lease.keys),
            {ok, #{owner => O, lease => LeaseId, token => T, context => C}};
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

%% Every identifier returned to a client packs the epoch: tokens, auto lease
%% ids and watch references are compared or matched across time, and a raw
%% index minted by a new incarnation aliases one minted by the old. Addition
%% rather than `bor`: identical while the index is below `2^?EPOCH_SHIFT`,
%% and still monotonic in both arguments if that ever fails to hold. The
%% waiter `seq` stays a raw index: it never leaves one incarnation's state.
packed_index(Meta, #?MODULE{epoch = Epoch}) ->
    (Epoch bsl ?EPOCH_SHIFT) + index(Meta).

%% Ra runs `{aux, ...}` effects on the leader only, and replay never runs
%% effects, so a recovering leader falls back to the sweep's seeding.
refreshed_effect(LeaseIds) ->
    [{aux, {refreshed, LeaseIds}}].

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
                    Token = packed_index(Meta, State0),
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
%% gone, and candidates whose lease is in `Revoking` (being revoked by this
%% same command). Such a promotion would be revoked again a fold step later,
%% and its re-promotion would mint a second token for the key at the same
%% Raft index, breaking per-key token monotonicity.
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
            Token = packed_index(Meta, State1),
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
            Token = packed_index(Meta, State1),
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
%% minted for an earlier, since-revoked attempt that is still in flight.
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

%% Revoke a whole lease: release every key it holds (promoting candidates),
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

%% Sorted so the effect order is a pure function of replicated state, like
%% every other effect-emitting fold in this module.
notify_watchers(LockKey, Event, State) ->
    Ws = maps:get(LockKey, State#?MODULE.watchers, #{}),
    [{send_msg, Pid, {portunus, watch, maps:get(Pid, Ws), Event}}
     || Pid <- lists:sort(maps:keys(Ws))].

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
