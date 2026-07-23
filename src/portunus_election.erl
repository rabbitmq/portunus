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

-type election_opts() :: #{ttl_ms => pos_integer(),
                           affinity => portunus_affinity:spec()}.
-export_type([election_opts/0]).

-spec start_link(portunus:name(), portunus:lock_key(), module(), term()) ->
    {ok, pid()} | {error, term()}.
start_link(Name, Key, Mod, Args) ->
    start_link(Name, Key, Mod, Args, #{}).

%% The `ttl_ms` floor is the renewer's (see `portunus_keepalive`).
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
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

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
        State1 = State0#state{lease_id = LeaseId, renewer_mon = Mon},
        Owner = node(),
        case portunus:acquire_or_join_succession_queue(
               State1#state.name, State1#state.key, LeaseId, Owner,
               #{affinity => State1#state.affinity}) of
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
    %% The lease stays valid for TTL since (after) its last renewal.
    %% If the renewer fails, the owner process re-monitors. The resource ownership
    %% is not lost.
    %%
    %% If the lease expired before such re-attachment could take place,
    %% the next round delivers `lease_lost`.
    {noreply, reattach_renewal(State)};
handle_info(reattach,
            #state{lease_id = LeaseId, renewer_mon = undefined} = State)
  when LeaseId =/= undefined ->
    {noreply, reattach_renewal(State)};
handle_info({reconcile, Gen}, #state{reconcile = Gen, role = follower,
                                     name = Name, key = Key,
                                     lease_id = LeaseId} = State)
  when LeaseId =/= undefined ->
    %% Backstop for a lost `granted` message. Promotion is committed in the
    %% machine, but the notification is a best-effort `send_msg` that a leader
    %% change can drop, leaving us queued forever while we already hold the
    %% lock. A linearizable read settles it without touching the succession
    %% queue (a re-acquire would reset our arrival order).
    case portunus:owner(Name, Key) of
        {ok, #{lease := LeaseId, token := Token}} ->
            {noreply, become_leader(Token, State#state{pending_transfer = false})};
        {error, no_quorum} ->
            {noreply, schedule_reconcile(State)};
        _ when State#state.pending_transfer ->
            %% The timed-out transfer committed (or the key has since moved
            %% on): this node no longer holds it, so drop the lease and
            %% re-contend. `stepped_down` already ran before the command.
            teardown_lease(State),
            self() ! contend,
            {noreply, reset(State)};
        _ ->
            {noreply, schedule_reconcile(State)}
    end;
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

become_leader(Token, #state{mod = Mod, name = Name, key = Key} = State) ->
    Ctx = #{name => Name, key => Key, token => Token, args => State#state.args},
    try Mod:elected(Ctx) of
        {ok, CbState} ->
            State#state{role = leader, token = Token, cb_state = CbState};
        Other ->
            %% A bad return value raises `try_clause` outside this try's own
            %% protection, so it gets the same release-and-recontend path as
            %% an exception, not a crash.
            logger:warning("portunus election ~p returned ~p from elected/1; "
                           "releasing to re-contend", [Key, Other]),
            defer_contend(State)
    catch
        Class:Reason ->
            %% The user's `elected/1` code could not start. Release the lock so another node
            %% can win, rather than crash-looping with the lock held.
            logger:warning("portunus election ~p failed to start (~p:~p); "
                           "releasing to re-contend", [Key, Class, Reason]),
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
                token = undefined, cb_state = undefined,
                pending_transfer = false}.

%% Re-check ownership at the renewal cadence: often enough to recover a lost
%% promotion well within the lease, rare enough to be a cheap backstop. Each
%% timer carries a generation, so a re-contend supersedes any earlier pending
%% reconcile rather than letting them accumulate.
schedule_reconcile(#state{ttl_ms = TtlMs, reconcile = Gen} = State) ->
    Next = Gen + 1,
    _ = erlang:send_after(max(TtlMs div 3, 1000), self(), {reconcile, Next}),
    State#state{reconcile = Next}.
