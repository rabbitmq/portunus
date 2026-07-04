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

-export([start_link/4, start_link/5, is_leader/1, is_leader/2, stop/1,
         stop_all/1, stop_all/2]).
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
                keepalive :: portunus:option(pid()),
                token :: portunus:option(portunus:token()),
                cb_state :: term(),
                role = follower :: follower | leader,
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
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(contend, State0) ->
    %% Establish a holder-linked auto-renewing lease, then enqueue. The
    %% renewer is live before elected runs.
    maybe
        {ok, LeaseId} ?= portunus:grant_lease(State0#state.name,
                                              State0#state.ttl_ms),
        {ok, KA} ?= portunus_keepalive:start_link(State0#state.name, LeaseId,
                                                  State0#state.ttl_ms),
        State1 = State0#state{lease_id = LeaseId, keepalive = KA},
        Owner = {election, node()},
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
            #state{key = Key, lease_id = LeaseId, role = follower} = State) ->
    %% Matching lease_id drops a grant minted for an earlier contend that
    %% we have since abandoned, which would otherwise install us as leader
    %% on a revoked token.
    {noreply, become_leader(Token, State)};
handle_info({portunus, lease_lost, LeaseId},
            #state{lease_id = LeaseId} = State) ->
    {noreply, lose_and_recontend(State)};
handle_info({'EXIT', KA, _Reason}, #state{keepalive = KA} = State) ->
    {noreply, lose_and_recontend(State)};
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
            {noreply, become_leader(Token, State)};
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

%% Stop the renewer and revoke the now-orphaned lease so the next grant does not
%% queue behind our own still-held lock; both are best-effort under no_quorum.
teardown_lease(#state{name = Name, lease_id = LeaseId, keepalive = KA}) ->
    _ = case KA of undefined -> ok; _ -> catch portunus_keepalive:stop(KA) end,
    _ = case LeaseId of
            undefined -> ok;
            _ -> catch portunus:revoke_lease(Name, LeaseId)
        end,
    ok.

reset(State) ->
    State#state{role = follower, lease_id = undefined, keepalive = undefined,
                token = undefined, cb_state = undefined}.

%% Re-check ownership at the renewal cadence: often enough to recover a lost
%% promotion well within the lease, rare enough to be a cheap backstop. Each
%% timer carries a generation, so a re-contend supersedes any earlier pending
%% reconcile rather than letting them accumulate.
schedule_reconcile(#state{ttl_ms = TtlMs, reconcile = Gen} = State) ->
    Next = Gen + 1,
    _ = erlang:send_after(max(TtlMs div 3, 1000), self(), {reconcile, Next}),
    State#state{reconcile = Next}.
