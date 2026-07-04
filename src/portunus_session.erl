%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_session).
-moduledoc """
A per-node session: one lease, one renewer, many exclusive keys claimed
under it. The session process *is* the lease holder, so its death releases
all of its keys at once. Renewal cost is O(sessions), not O(keys).

On lease loss the session exits with reason `lease_lost`. A linked opener
that does not trap exits crashes with it (the fail-stop default, since
its claims are gone). Trap exits to handle the loss instead.
""".

-include("portunus.hrl").

-behaviour(gen_server).

-export([open/1, open/2, claim/2, release/2, keys/1, close/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {name :: portunus:name(),
                opts :: session_opts(),
                lease_id :: portunus:option(portunus:lease_id()),
                ttl_ms :: pos_integer(),
                keepalive :: portunus:option(pid()),
                keys = #{} :: #{portunus:lock_key() => portunus:token()}}).

-type session() :: pid().
-type session_opts() :: #{ttl_ms => pos_integer(),
                          proposed_id => portunus:lease_id()}.
-export_type([session/0, session_opts/0]).

-spec open(portunus:name()) -> {ok, session()} | {error, term()}.
open(Name) ->
    open(Name, #{}).

-doc """
Open a session. Opts: `ttl_ms`, `proposed_id` (a stable proposed lease id).
After a partition, reopening with a `proposed_id` can return
`{error, id_in_use}` until the previous incarnation's lease expires;
supervisor restart handles it.
""".
-spec open(portunus:name(), session_opts()) ->
    {ok, session()} | {error, term()}.
open(Name, Opts) when ?IS_RENEWABLE_TTL_OPT(Opts) ->
    %% Two-phase: `init/1` cannot fail, so a grant failure comes back as an
    %% error tuple instead of an exit signal that kills a non-trapping caller.
    {ok, Pid} = gen_server:start_link(?MODULE, {Name, Opts}, []),
    case gen_server:call(Pid, establish, infinity) of
        ok ->
            {ok, Pid};
        {error, _} = Err ->
            ok = gen_server:stop(Pid),
            Err
    end.

-doc "Claim an exclusive key under the session; returns its fencing token.".
-spec claim(session(), portunus:lock_key()) ->
    {ok, portunus:token()} | {error, term()}.
claim(Session, Key) ->
    %% `infinity`: the inner Ra command already bounds latency, so the outer
    %% call must not time out and crash the caller while the session is fine.
    gen_server:call(Session, {claim, Key}, infinity).

-spec release(session(), portunus:lock_key()) -> ok | {error, term()}.
release(Session, Key) ->
    gen_server:call(Session, {release, Key}, infinity).

-spec keys(session()) -> [portunus:lock_key()].
keys(Session) ->
    gen_server:call(Session, keys, infinity).

-spec close(session()) -> ok.
close(Session) ->
    %% An already-closed session (it stops itself on `lease_lost`) is this
    %% call's goal state, not an error.
    try gen_server:stop(Session)
    catch exit:noproc -> ok
    end.

init({Name, Opts}) ->
    process_flag(trap_exit, true),
    proc_lib:set_label({portunus_session, Name}),
    {ok, #state{name = Name, opts = Opts,
                ttl_ms = maps:get(ttl_ms, Opts, 60000)}}.

handle_call(establish, _From, #state{name = Name, opts = Opts,
                                     ttl_ms = TtlMs} = State) ->
    ProposedId = maps:get(proposed_id, Opts, undefined),
    maybe
        {ok, LeaseId} ?= grant_with_retry(Name, TtlMs,
                                          #{proposed_id => ProposedId}, 5),
        {ok, KA} ?= portunus_keepalive:start_link(Name, LeaseId, TtlMs),
        proc_lib:set_label({portunus_session, Name, LeaseId}),
        {reply, ok, State#state{lease_id = LeaseId, keepalive = KA}}
    else
        {error, _} = Err -> {reply, Err, State}
    end;
handle_call({claim, Key}, _From, State) ->
    case portunus:acquire(State#state.name, Key, State#state.lease_id,
                          {session, node()}) of
        {ok, Token} ->
            {reply, {ok, Token},
             State#state{keys = maps:put(Key, Token, State#state.keys)}};
        Err ->
            {reply, Err, State}
    end;
handle_call({release, Key}, _From, State) ->
    case maps:find(Key, State#state.keys) of
        {ok, Token} ->
            case portunus:release(State#state.name, Key, Token) of
                ok ->
                    {reply, ok,
                     State#state{keys = maps:remove(Key, State#state.keys)}};
                {error, _} = Err ->
                    %% Keep the key so `close/1` still frees it and a retry is possible.
                    {reply, Err, State}
            end;
        error ->
            {reply, ok, State}
    end;
handle_call(keys, _From, State) ->
    {reply, maps:keys(State#state.keys), State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Ra turns a machine monitor's `DOWN` into a low-priority command
%% (`ra_server:handle_down/5`), so a supervised restart's re-grant can reach
%% the log before the dead incarnation's revocation. That window is
%% milliseconds; a lease legitimately still live (a partition, up to a full
%% TTL) keeps returning `id_in_use` and stays an error.
grant_with_retry(Name, TtlMs, Opts, Attempts) ->
    case portunus:grant_lease(Name, TtlMs, Opts) of
        {error, id_in_use} when Attempts > 1 ->
            timer:sleep(100),
            grant_with_retry(Name, TtlMs, Opts, Attempts - 1);
        Other ->
            Other
    end.

%% The renewer lost the lease: the session is no longer valid.
handle_info({portunus, lease_lost, LeaseId},
            #state{lease_id = LeaseId} = State) ->
    {stop, lease_lost, State};
handle_info({'EXIT', KA, _}, #state{keepalive = KA} = State) ->
    {stop, lease_lost, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{lease_id = undefined}) ->
    ok;
terminate(_Reason, State) ->
    %% Best-effort clean revoke; if we cannot reach quorum the lease
    %% expires on its own.
    _ = portunus:revoke_lease(State#state.name, State#state.lease_id),
    ok.
