%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_election).
-moduledoc """
Helps implement a leader election with application-specific semantics.

A candidate (an election participant) runs on every node. At most one candidate
is elected at a time (at any committed Raft index).

When a candidate is elected, the `elected/1` callback is called.
Its opposite, `stepped_down/1`, is invoked when the participant
loses leadership.

```erlang
-callback elected(Ctx :: election_ctx()) -> {ok, State :: term()}.
-callback stepped_down(State :: term()) -> ok.
```

`Ctx` is an `election_ctx()` map carrying `name`, `key`, `token`, and
`args`, so the elected leader can use the fencing token for operations
on external resources.
""".

-behaviour(gen_server).

-include("portunus.hrl").

-export([start_link/4, start_link/5, is_leader/1, is_leader/2, transfer_to/2,
         transfer_many/3, prepare_transfer/2, settle_transfer/2,
         stop/1, stop_all/1, stop_all/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Passed to `elected/1`. `token` is the fencing token for
%% the leader to (optionally) use on operations against external resources.
-type election_ctx() :: #{name := portunus:name(),
                          key := portunus:lock_key(),
                          token := portunus:token(),
                          args := term()}.
-export_type([election_ctx/0]).

-callback elected(election_ctx()) -> {ok, State :: term()}.
-callback stepped_down(State :: term()) -> ok.

-record(state, {name :: portunus:name(),
                key :: portunus:lock_key(),
                ttl_ms :: pos_integer(),
                mod :: module(),
                args :: term(),
                affinity = default :: portunus_affinity:spec(),
                %% The score last submitted with our bid; a re-bid is issued
                %% only when a reconcile-time recomputation differs.
                score :: portunus:option(integer()),
                lease_id :: portunus:option(portunus:lease_id()),
                %% Monitor on the shared renewer (`portunus_batch_keepalive`).
                %% As long as owners can re-attach and monitor the renewer again
                %% after a 'DOWN' event within lease TTL, the lease is maintained (not lost).
                renewer_mon :: portunus:option(reference()),
                token :: portunus:option(portunus:token()),
                cb_state :: term(),
                role = follower :: follower | leader,
                %% A transfer command timed out: its outcome is unknown until
                %% the reconciliation read confirms who owns the key.
                pending_transfer = false :: boolean(),
                reconcile = 0 :: non_neg_integer()}).

%% A `dynamic` affinity is re-scored at the reconcile cadence while the
%% election waits, and a changed score refreshes its queued bid (see
%% `portunus_affinity`).
-type election_opts() :: #{ttl_ms => pos_integer(),
                           affinity => portunus_affinity:spec()}.
-export_type([election_opts/0]).

-spec start_link(portunus:name(), portunus:lock_key(), module(), term()) ->
    {ok, pid()} | {error, term()}.
start_link(Name, Key, Mod, Args) ->
    start_link(Name, Key, Mod, Args, #{}).

%% The `ttl_ms` floor is `?MIN_RENEWABLE_TTL_MS`, defined in `portunus.hrl`.
%% A lease has to be renewable well within its TTL, so the renewer requires
%% this minimum.
-spec start_link(portunus:name(), portunus:lock_key(), module(), term(),
                 election_opts()) ->
    {ok, pid()} | {error, term()}.
start_link(Name, Key, Mod, Args, Opts) when ?IS_RENEWABLE_TTL_OPT(Opts) ->
    TtlMs = maps:get(ttl_ms, Opts, 60000),
    gen_server:start_link(?MODULE, {Name, Key, TtlMs, Mod, Args, Opts}, []).

-spec is_leader(pid()) -> boolean().
is_leader(Pid) ->
    is_leader(Pid, 5000).

-doc """
Whether this participant is the elected owner. An election blocked in a
Ra command (a quorum loss, a slow `elected/1`) is not the owner, so a
caller that must not block treats a timeout as `false`.
""".
-spec is_leader(pid(), timeout()) -> boolean().
is_leader(Pid, Timeout) ->
    gen_server:call(Pid, is_leader, Timeout).

-doc """
Ask this node's election, if it is the current owner of its key, to hand
ownership to `TargetNode`. It pre-checks that `TargetNode` is a ready
contender, stops the local work, issues the token-fenced transfer, and on
success re-contends as a standby; if the target was not ready it restores the
local work and stays owner. Returns `{error, not_owner}` when this node is not
the owner, and `{error, {no_contender, TargetNode}}` when the target is not a
ready contender. `{error, no_quorum}` means the command timed out and its
outcome is unknown: the work stays stopped while the election settles
ownership itself (restoring it or re-contending), so the caller retries later
rather than treating it as a failed transfer. A retry made before that
settles also returns `{error, not_owner}`; it does not prove ownership moved.
""".
-spec transfer_to(pid(), node()) ->
    portunus:ok_or_error({no_contender, node()} | not_owner | no_quorum).
transfer_to(Pid, TargetNode) ->
    %% Bounds a `stepped_down`, a fenced command, and `elected`; a wedged
    %% election surfaces as a timeout the caller retries, not an endless block.
    gen_server:call(Pid, {transfer_to, TargetNode}, 15000).

-doc """
Move several keys owned by this node to their targets in one batched command.
This is `transfer_to/2`'s choreography run once for the whole batch instead of
per key: prepare stops each key's local work and gathers its token, a single
`portunus:transfer_many/2` command commits them all, and settle re-contends or
restores each key from its per-item result.

`KeyTargets` names, per key, the node it should move to. `LiveElections` maps
each of this node's keys to its election pid, so the caller (a registry or a
service) supplies only its own election map. Ahead of the command the pairs
are deduplicated by key, a pair whose target is this node is `ok` without
touching its election, and a key with no live election here is
`{error, not_owner}`; a very large batch is chunked into bounded commands.
Returns one `{Key, ok | {error, term()}}` per distinct key, in input order.
""".
%% `Key` is the caller's own key term (a registry or service key), not the
%% namespaced lock key; the lock key is gathered from each election in prepare.
-spec transfer_many(portunus:name(), [{Key, node()}], #{Key => pid()}) ->
    [{Key, ok | {error, term()}}] when Key :: term().
transfer_many(Name, KeyTargets, LiveElections) when is_list(KeyTargets) ->
    Deduped = dedup_by_key(KeyTargets),
    {Live, Immediate} = classify(Deduped, LiveElections),
    Results = maps:from_list(chunked_transfer(Name, Live) ++ Immediate),
    [{Key, maps:get(Key, Results)} || {Key, _Target} <- Deduped].

-doc """
Stop this election's local work for a planned transfer to `TargetNode` and
return `{ok, LockKey, Token}` for the caller to commit in a batch. It runs
`transfer_to/2`'s ready-contender pre-check first, so a not-ready target is
refused with `{error, {no_contender, TargetNode}}` before any work stops, and
a standby is `{error, not_owner}`.

The election then keeps its lease renewing but stays a follower until
`settle_transfer/2` (or, if none arrives, a reconciliation read) resolves who
owns the key, so between prepare and settle the key runs on no node.
""".
-spec prepare_transfer(pid(), node()) ->
    {ok, portunus:lock_key(), portunus:token()} |
    {error, {no_contender, node()} | not_owner | no_quorum}.
prepare_transfer(Pid, TargetNode) ->
    gen_server:call(Pid, {prepare_transfer, TargetNode}, 15000).

-doc """
Hand a prepared election its committed per-item result: `ok` re-contends as a
standby, `{error, {no_contender, _}}` restores the local work on the unchanged
token, and `{error, not_owner}` re-contends because the lease lapsed during
the command. A settle arriving after the election has already moved on (a
`lease_lost`, or the reconciliation backstop) is a no-op.
""".
-spec settle_transfer(pid(), ok | {error, term()}) -> ok.
settle_transfer(Pid, Result) ->
    gen_server:cast(Pid, {settle_transfer, Result}).

-spec stop(pid()) -> ok.
stop(Pid) ->
    %% An already-stopped election is this call's goal state, not an error.
    try gen_server:stop(Pid)
    catch exit:noproc -> ok
    end.

-doc """
Stop several elections concurrently against one deadline, killing
stragglers. Each election's terminate runs user `stepped_down` code plus a
revoke that blocks up to the command timeout under no quorum, so a serial
stop holds the caller for the sum. A killed election's revoke is lost; TTL
expiry covers it.
""".
-spec stop_all([pid()]) -> ok.
stop_all(Pids) ->
    stop_all(Pids, 15000).

-spec stop_all([pid()], pos_integer()) -> ok.
stop_all(Pids, TimeoutMs) ->
    Stoppers = [{spawn_monitor(fun() -> stop(P) end), P} || P <- Pids],
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    lists:foreach(
      fun({{_, Ref}, Pid}) ->
              Left = max(0, Deadline - erlang:monotonic_time(millisecond)),
              receive
                  {'DOWN', Ref, process, _, _} -> ok
              after Left ->
                      exit(Pid, kill),
                      %% The kill unblocks the stopper; reap its 'DOWN' so no
                      %% stray message is left in the caller's mailbox.
                      receive {'DOWN', Ref, process, _, _} -> ok end
              end
      end, Stoppers),
    ok.

init({Name, Key, TtlMs, Mod, Args, Opts}) ->
    process_flag(trap_exit, true),
    proc_lib:set_label({portunus_election, Name, Key}),
    self() ! contend,
    {ok, #state{name = Name, key = Key, ttl_ms = TtlMs, mod = Mod, args = Args,
                affinity = maps:get(affinity, Opts, default)}}.

handle_call(is_leader, _From, State) ->
    {reply, State#state.role =:= leader, State};
handle_call({transfer_to, TargetNode}, _From, #state{role = leader} = State) ->
    do_transfer_to(TargetNode, State);
handle_call({transfer_to, _TargetNode}, _From, State) ->
    %% Only the elected owner can transfer; a standby is not the owner.
    {reply, {error, not_owner}, State};
handle_call({prepare_transfer, TargetNode}, _From, #state{role = leader} = State) ->
    do_prepare_transfer(TargetNode, State);
handle_call({prepare_transfer, _TargetNode}, _From, State) ->
    %% A standby is not the owner, and a second prepare (already a follower
    %% awaiting settle) is refused the same way.
    {reply, {error, not_owner}, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({settle_transfer, Result},
            #state{pending_transfer = true} = State) ->
    {noreply, do_settle_transfer(Result, State)};
handle_cast({settle_transfer, _Result}, State) ->
    %% The election already resolved the transfer itself (a `lease_lost`, or
    %% the reconciliation backstop), so a late settle changes nothing.
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(contend, State0) ->
    %% Establish an auto-renewing lease, then enqueue. The renewal is live
    %% before elected runs.
    maybe
        {ok, LeaseId} ?= portunus:grant_lease(State0#state.name,
                                              State0#state.ttl_ms),
        {ok, Mon} ?= attach_renewal(State0#state.name, LeaseId,
                                    State0#state.ttl_ms),
        Score = portunus:succession_score(State0#state.name, State0#state.key,
                                          #{affinity => State0#state.affinity}),
        State1 = State0#state{lease_id = LeaseId, renewer_mon = Mon,
                              score = Score},
        Owner = node(),
        case portunus:acquire_or_join_succession_queue(
               State1#state.name, State1#state.key, LeaseId, Owner,
               #{score => Score}) of
            {ok, Token} ->
                {noreply, become_leader(Token, State1)};
            {queued, _Depth} ->
                {noreply, schedule_reconcile(State1)};
            {error, _} ->
                %% A transient no_quorum during acquire is routine on a leader
                %% change; re-contend rather than exit.
                {noreply, defer_contend(State1)}
        end
    else
        {error, _Reason} ->
            %% Could not even get a lease (e.g. no quorum); retry shortly.
            erlang:send_after(1000, self(), contend),
            {noreply, State0#state{role = follower}}
    end;
handle_info({portunus, granted, Key, Token, LeaseId},
            #state{key = Key, lease_id = LeaseId, role = follower,
                   pending_transfer = false} = State) ->
    %% Matching `lease_id` drops a grant minted for an earlier contend that
    %% we have since abandoned, which would otherwise install us as leader
    %% on a revoked token. While a transfer outcome is pending this clause
    %% does not match: a delayed grant from before the transfer would
    %% restart the work on a stale token, so only the reconciliation read may
    %% restore leadership until the flag clears.
    {noreply, become_leader(Token, State)};
handle_info({portunus, lease_lost, LeaseId},
            #state{lease_id = LeaseId} = State) ->
    {noreply, lose_and_recontend(State)};
handle_info({'DOWN', Mon, process, _Pid, _Reason},
            #state{renewer_mon = Mon} = State) ->
    %% The lease stays valid for one TTL after its last renewal.
    %% If the renewer fails, the owner process re-monitors, so resource
    %% ownership is not lost.
    %%
    %% If the lease expired before such re-attachment could take place,
    %% the next round delivers `lease_lost`.
    {noreply, reattach_renewal(State)};
handle_info(reattach,
            #state{lease_id = LeaseId, renewer_mon = undefined} = State)
  when LeaseId =/= undefined ->
    {noreply, reattach_renewal(State)};
handle_info({reconcile, Gen}, #state{reconcile = Gen, role = follower,
                                     lease_id = LeaseId} = State)
  when LeaseId =/= undefined ->
    {noreply, reconcile(State)};
handle_info({'EXIT', _Pid, _Reason}, State) ->
    %% Exits from the parent are handled by gen_server before dispatch
    %% (trap_exit is set for terminate/2). Links created by `elected/1`
    %% callback code deliver here and are deliberately ignored: `elected/1`
    %% runs in this process, and supervising whatever it links is the
    %% callback's job.
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{role = leader, mod = Mod, cb_state = CbState,
                          name = Name, lease_id = LeaseId}) ->
    _ = (catch Mod:stepped_down(CbState)),
    _ = portunus:revoke_lease(Name, LeaseId),
    ok;
terminate(_Reason, #state{name = Name, lease_id = LeaseId})
  when LeaseId =/= undefined ->
    _ = portunus:revoke_lease(Name, LeaseId),
    ok;
terminate(_Reason, _State) ->
    ok.

%% Each reconcile round while queued answers two questions: does this node
%% already own the key (a lost `granted`, a settled transfer), and is our
%% queued bid still current? While a transfer outcome is pending only the
%% ownership read runs: a re-bid's idempotent `{ok, Token}` would restart
%% work a committed transfer is about to move, running it on two nodes.
%% Otherwise the score is recomputed; when it changed, the re-bid replaces
%% the read, because its reply answers the same ownership question.
reconcile(#state{pending_transfer = true} = State) ->
    reconcile_read(State);
reconcile(#state{name = Name, key = Key, affinity = Affinity,
                 score = Score} = State) ->
    case portunus:succession_score(Name, Key, #{affinity => Affinity}) of
        Score -> reconcile_read(State);
        Changed -> rebid(Changed, State)
    end.

%% Backstop for a lost `granted` message. Promotion is committed in the
%% machine, but the notification is a best-effort `send_msg` that a leader
%% change can drop, leaving us queued forever while we already hold the
%% lock. A linearizable read settles it without touching the succession
%% queue.
reconcile_read(#state{name = Name, key = Key, lease_id = LeaseId} = State) ->
    case portunus:owner(Name, Key) of
        {ok, #{lease := LeaseId, token := Token}} ->
            become_leader(Token, State#state{pending_transfer = false});
        {error, no_quorum} ->
            schedule_reconcile(State);
        _ when State#state.pending_transfer ->
            %% The timed-out transfer committed (or the key has since moved
            %% on): this node no longer holds it, so drop the lease and
            %% re-contend. `stepped_down` already ran before the command.
            teardown_lease(State),
            self() ! contend,
            reset(State);
        _ ->
            schedule_reconcile(State)
    end.

%% Re-submit our bid with the changed score; the machine refreshes it in
%% place, keeping our arrival order. `{ok, Token}` means this node holds the
%% key: a fresh grant, or a promotion whose `granted` message was lost.
rebid(Score, #state{name = Name, key = Key, lease_id = LeaseId} = State) ->
    case portunus:acquire_or_join_succession_queue(Name, Key, LeaseId, node(),
                                                   #{score => Score}) of
        {ok, Token} ->
            become_leader(Token, State);
        {queued, _Depth} ->
            schedule_reconcile(State#state{score = Score});
        {error, lease_expired} ->
            %% A committed answer that the lease is gone: this election can
            %% never be promoted, so re-contend now instead of waiting for
            %% the renewal round's `lease_lost`.
            teardown_lease(State),
            self() ! contend,
            reset(State);
        {error, _} ->
            %% Unknown outcome (a quorum loss, a timeout). Re-arm without
            %% storing the score, so the next round retries the re-bid.
            schedule_reconcile(State)
    end.

%% `elected/1` failure reasons and bad return values routinely embed the
%% host's child arguments, and hosts put credentials there. The warning
%% names the key and the failure class only; the user-supplied detail is a
%% log-level change away at debug, not lost.
become_leader(Token, #state{mod = Mod, name = Name, key = Key} = State) ->
    Ctx = #{name => Name, key => Key, token => Token, args => State#state.args},
    try Mod:elected(Ctx) of
        {ok, CbState} ->
            State#state{role = leader, token = Token, cb_state = CbState};
        Other ->
            %% A bad return value raises `try_clause` outside this try's own
            %% protection, so it gets the same release-and-recontend path as
            %% an exception, not a crash.
            logger:warning("portunus election ~p: elected/1 returned an "
                           "unexpected value; releasing to re-contend", [Key]),
            logger:debug("portunus election ~p: elected/1 returned ~p",
                         [Key, Other]),
            defer_contend(State)
    catch
        Class:Reason:Stacktrace ->
            %% The user's `elected/1` code could not start. Release the lock
            %% so another node can win, rather than crash-looping with the
            %% lock held.
            logger:warning("portunus election ~p: elected/1 raised ~p; "
                           "releasing to re-contend", [Key, Class]),
            logger:debug("portunus election ~p: elected/1 raised ~p:~p at ~p",
                         [Key, Class, Reason, Stacktrace]),
            defer_contend(State)
    end.

%% A planned transfer. `stepped_down` runs before the command so a brief gap
%% is preferred to two overlapping owners, and the pre-check refuses a
%% not-ready target before any local work stops. A committed refusal
%% (`no_contender`) restores the owner; a lease that lapsed during the command
%% (`not_owner`) re-contends without a second `stepped_down`; a timed-out
%% command (`no_quorum`) has an unknown outcome and is resolved by the
%% reconciliation read before the work restarts anywhere.
do_transfer_to(TargetNode, State) when TargetNode =:= node() ->
    {reply, ok, State};
do_transfer_to(TargetNode, #state{name = Name, key = Key, token = Token,
                                  mod = Mod, cb_state = CbState} = State) ->
    case is_ready_contender(Name, Key, TargetNode) of
        false ->
            %% Count the refusal here: the pre-check refuses before the command,
            %% so the machine's counter never sees this common churn case.
            _ = portunus_counters:incr(Name, transfer_no_contender_total),
            {reply, {error, {no_contender, TargetNode}}, State};
        true ->
            _ = (catch Mod:stepped_down(CbState)),
            case portunus:transfer(Name, Key, Token, TargetNode) of
                ok ->
                    teardown_lease(State),
                    self() ! contend,
                    {reply, ok, reset(State)};
                {error, {no_contender, _}} = Err ->
                    {reply, Err, become_leader(Token, State)};
                {error, no_quorum} = Err ->
                    %% The command timed out, so it may still commit. Restarting
                    %% the work on the old token would run it on two nodes if it
                    %% did (the target is granted while this node keeps going),
                    %% and nothing would ever correct that: renewal keeps the
                    %% lease alive, so no `lease_lost` arrives. Keep the work
                    %% stopped and the lease renewing until the reconciliation read
                    %% answers: if this node still owns the key the work is
                    %% restored, otherwise the election re-contends.
                    {reply, Err,
                     schedule_reconcile(State#state{role = follower,
                                                    pending_transfer = true})};
                {error, not_owner} = Err ->
                    %% Lease lapsed during the transfer: already lost, and
                    %% `stepped_down` has run, so re-contend without repeating it.
                    teardown_lease(State),
                    self() ! contend,
                    {reply, Err, reset(State)}
            end
    end.

%% The prepare half of a batched transfer: `do_transfer_to/2` up to but not
%% including the command, which the caller now issues once for the whole batch.
%% The pre-check refuses a not-ready target before any work stops, then
%% `stepped_down` runs and the election becomes a follower that keeps its lease
%% renewing. The `pending_transfer` flag carries the timed-out transfer's
%% semantics (a stray grant is dropped, a `lease_lost` needs no second
%% `stepped_down`), and the scheduled reconcile is the settle deadline: an
%% election left prepared, by an orchestrator that died or a prepare reply that
%% timed out, resolves ownership from the read instead of waiting forever.
%%
%% The deadline must outlive the orchestrator's whole choreography (the
%% prepare fan-out bound plus the command timeout): the usual `ttl_ms div 3`
%% reconcile could fire between prepare and commit, read this node as still
%% owning, and restart the work the committing batch is about to move,
%% running it on two nodes with no correction.
do_prepare_transfer(TargetNode, #state{name = Name, key = Key, token = Token,
                                       mod = Mod, cb_state = CbState} = State) ->
    case is_ready_contender(Name, Key, TargetNode) of
        false ->
            _ = portunus_counters:incr(Name, transfer_no_contender_total),
            {reply, {error, {no_contender, TargetNode}}, State};
        true ->
            _ = (catch Mod:stepped_down(CbState)),
            {reply, {ok, Key, Token},
             schedule_reconcile_after(settle_deadline_ms(),
                                      State#state{role = follower,
                                                  pending_transfer = true})}
    end.

%% The settle half: the committed per-item result decides what a prepared
%% election does, mirroring `do_transfer_to/2`'s own arms. An unrecognised
%% result (a batch-wide failure surfaced per key) is left to the reconcile
%% backstop, which is why this stays total.
do_settle_transfer(ok, State) ->
    %% The key moved to the target: drop the lease and re-contend as a standby.
    teardown_lease(State),
    self() ! contend,
    reset(State);
do_settle_transfer({error, {no_contender, _}}, State) ->
    %% Refused, and this node still holds the key on the same token: restore
    %% the local work.
    become_leader(State#state.token, State#state{pending_transfer = false});
do_settle_transfer({error, not_owner}, State) ->
    %% The lease lapsed during the command, so the key is already lost;
    %% `stepped_down` ran in prepare, so re-contend without repeating it.
    teardown_lease(State),
    self() ! contend,
    reset(State);
do_settle_transfer({error, no_quorum}, State) ->
    %% The batch command timed out: an unknown outcome for this key too. Move
    %% to the state a timed-out `transfer_to/2` leaves (still pending, the
    %% short reconcile armed) instead of waiting out the long settle deadline.
    schedule_reconcile(State);
do_settle_transfer(_Other, State) ->
    State.

%% The transfer pre-check: is `TargetNode` a live contender for `Key`? A local,
%% possibly-stale read; a failed read counts as not ready, so the owner is
%% never stepped down for an unconfirmed target.
is_ready_contender(Name, Key, TargetNode) ->
    case portunus:contenders(Name, Key) of
        {ok, Owners} -> lists:member(TargetNode, Owners);
        {error, _} -> false
    end.

%% Step down if we held the lock, then re-contend at once: a lost lease should
%% be replaced promptly, and the contend handler backs off if quorum is gone.
lose_and_recontend(#state{role = Role, mod = Mod, cb_state = CbState} = State) ->
    case Role of
        leader -> _ = (catch Mod:stepped_down(CbState));
        _ -> ok
    end,
    teardown_lease(State),
    self() ! contend,
    reset(State).

%% Re-contend after a backoff following a transient acquire failure.
defer_contend(State) ->
    teardown_lease(State),
    erlang:send_after(1000, self(), contend),
    reset(State).

%% Unmonitor the renewer, detach the lease from it, and revoke the now-orphaned
%% lease so the next grant does not queue behind our own still-held lock; all
%% best-effort.
teardown_lease(#state{name = Name, lease_id = LeaseId, renewer_mon = Mon}) ->
    _ = case Mon of
            undefined -> ok;
            _ -> erlang:demonitor(Mon, [flush])
        end,
    _ = case LeaseId of
            undefined -> ok;
            _ ->
                catch portunus_batch_keepalive:detach(Name, LeaseId),
                catch portunus:revoke_lease(Name, LeaseId)
        end,
    ok.

%% The monitor is taken after the call, so a renewer that dies in between
%% delivers an immediate 'DOWN' and the re-attach path runs.
attach_renewal(Name, LeaseId, TtlMs) ->
    try portunus_batch_keepalive:attach(Name, LeaseId, TtlMs) of
        ok -> {ok, erlang:monitor(process, portunus_batch_keepalive)}
    catch
        exit:_ -> {error, renewer_down}
    end.

%% The renewer is supervised, so being down is a restart window: retry
%% shortly rather than churn ownership.
reattach_renewal(#state{name = Name, lease_id = LeaseId,
                        ttl_ms = TtlMs} = State) ->
    case attach_renewal(Name, LeaseId, TtlMs) of
        {ok, Mon} ->
            State#state{renewer_mon = Mon};
        {error, renewer_down} ->
            erlang:send_after(500, self(), reattach),
            State#state{renewer_mon = undefined}
    end.

reset(State) ->
    State#state{role = follower, lease_id = undefined, renewer_mon = undefined,
                token = undefined, cb_state = undefined, score = undefined,
                pending_transfer = false}.

%% Re-check ownership at the renewal cadence: often enough to recover a lost
%% promotion well within the lease, rare enough to be a cheap backstop. Each
%% timer carries a generation, so a re-contend supersedes any earlier pending
%% reconcile rather than letting them accumulate.
schedule_reconcile(#state{ttl_ms = TtlMs} = State) ->
    schedule_reconcile_after(max(TtlMs div 3, 1000), State).

schedule_reconcile_after(DelayMs, #state{reconcile = Gen} = State) ->
    Next = Gen + 1,
    _ = erlang:send_after(DelayMs, self(), {reconcile, Next}),
    State#state{reconcile = Next}.

%%----------------------------------------------------------------------
%% Batched transfer orchestration (used by `transfer_many/3`)
%%----------------------------------------------------------------------

%% A batch is chunked into commands of at most this many transfers, so one
%% reconciliation pass costs a handful of log entries without any single entry
%% growing without bound. Overridable via the app environment
%% (`transfer_batch_chunk`).
-define(BATCH_CHUNK, 500).

%% The prepare fan-out deadline, matching `transfer_to/2`'s call bound.
-define(PREPARE_TIMEOUT_MS, 15000).

%% A prepared election's settle deadline: the prepare fan-out bound plus the
%% command timeout, with margin, so the deadline cannot pass while the
%% orchestrator may still commit. Overridable via the app environment
%% (`transfer_settle_deadline_ms`).
-define(SETTLE_DEADLINE_MS, 30000).

settle_deadline_ms() ->
    application:get_env(portunus, transfer_settle_deadline_ms,
                        ?SETTLE_DEADLINE_MS).

%% Keep the first target named for each key: results are looked up by key, so
%% a duplicate key would alias another's result.
dedup_by_key(KeyTargets) ->
    {_, Rev} = lists:foldl(
                 fun({Key, _Target} = KT, {Seen, Acc}) ->
                         case is_map_key(Key, Seen) of
                             true -> {Seen, Acc};
                             false -> {Seen#{Key => true}, [KT | Acc]}
                         end
                 end, {#{}, []}, KeyTargets),
    lists:reverse(Rev).

%% Split the deduplicated pairs into elections to prepare and results already
%% known: a key with no live election here is not owned by this node (as the
%% single-transfer path answers it), and a managed key whose target is this
%% node needs no transfer.
classify(Deduped, LiveElections) ->
    lists:foldr(
      fun({Key, Target}, {Live, Immediate}) ->
              case LiveElections of
                  #{Key := _Pid} when Target =:= node() ->
                      {Live, [{Key, ok} | Immediate]};
                  #{Key := Pid} ->
                      {[{Key, Pid, Target} | Live], Immediate};
                  _ ->
                      {Live, [{Key, {error, not_owner}} | Immediate]}
              end
      end, {[], []}, Deduped).

chunked_transfer(_Name, []) ->
    [];
chunked_transfer(Name, Live) ->
    Chunk = application:get_env(portunus, transfer_batch_chunk, ?BATCH_CHUNK),
    lists:append([move_batch(Name, C) || C <- chunks(Chunk, Live)]).

%% One chunk's three phases: prepare each election concurrently, commit the
%% prepared transfers in one command, then settle each from its per-item
%% result. A chunk that prepares nothing issues no command.
move_batch(Name, Entries) ->
    Prepared = prepare_all(Entries, ?PREPARE_TIMEOUT_MS),
    Batch = [{LockKey, Token, Target}
             || {_Key, _Pid, Target, {ok, LockKey, Token}} <- Prepared],
    case Batch of
        [] ->
            [{Key, Res} || {Key, _Pid, _Target, {error, _} = Res} <- Prepared];
        _ ->
            settle_batch(Prepared, portunus:transfer_many(Name, Batch))
    end.

%% Run `prepare_transfer/2` on every election concurrently under one deadline,
%% the way `stop_all/2` bounds many stops. A prepare that does not answer in
%% time is treated as a refusal and its key drops from the batch; the election
%% it may have prepared resolves itself through the reconcile backstop.
%% Each prober carries its verdict as its exit reason, which dialyzer reads as
%% a fun that never returns normally; that is the point.
-dialyzer({nowarn_function, prepare_all/2}).
prepare_all(Entries, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    Probes = [{Entry, spawn_monitor(
                        fun() -> exit({prepared, prepare_call(Pid, Target)}) end)}
              || {_Key, Pid, Target} = Entry <- Entries],
    [await_prepare(Entry, Ref, Deadline) || {Entry, {_, Ref}} <- Probes].

prepare_call(Pid, Target) ->
    try prepare_transfer(Pid, Target)
    catch exit:_ -> {error, no_quorum}
    end.

await_prepare({Key, Pid, Target}, Ref, Deadline) ->
    Left = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {'DOWN', Ref, process, _, {prepared, Result}} ->
            {Key, Pid, Target, Result};
        {'DOWN', Ref, process, _, _} ->
            {Key, Pid, Target, {error, no_quorum}}
    after Left ->
        erlang:demonitor(Ref, [flush]),
        {Key, Pid, Target, {error, no_quorum}}
    end.

%% A batch-wide `no_quorum` is an unknown outcome, not a failure. Settling
%% each prepared election with it moves it to a timed-out `transfer_to/2`'s
%% state (still pending, the short reconcile armed) rather than leaving it
%% to wait out the long settle deadline.
settle_batch(Prepared, {error, no_quorum}) ->
    [begin
         case Res of
             {ok, _LockKey, _Token} -> settle_transfer(Pid, {error, no_quorum});
             {error, _} -> ok
         end,
         {Key, batch_wide_result(Res)}
     end || {Key, Pid, _Target, Res} <- Prepared];
settle_batch(Prepared, Committed) when is_list(Committed) ->
    Results = maps:from_list(Committed),
    [settle_one(Entry, Results) || Entry <- Prepared].

batch_wide_result({ok, _LockKey, _Token}) -> {error, no_quorum};
batch_wide_result({error, _} = Err) -> Err.

settle_one({Key, Pid, _Target, {ok, LockKey, _Token}}, Results) ->
    ItemResult = maps:get(LockKey, Results, {error, no_quorum}),
    settle_transfer(Pid, ItemResult),
    {Key, ItemResult};
settle_one({Key, _Pid, _Target, {error, _} = Err}, _Results) ->
    {Key, Err}.

%% Split a list into sublists of at most `N`, preserving order.
chunks(N, List) when length(List) =< N ->
    [List];
chunks(N, List) ->
    {Head, Tail} = lists:split(N, List),
    [Head | chunks(N, Tail)].
