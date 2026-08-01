%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_joiner).
-moduledoc """
Runs the cluster join and convergence loop around
`portunus:join_or_form/3`: one process per cluster, supervised by the
consumer.

While joining, each pass runs the `ensure_system` fun and then
`join_or_form/3`, retrying with backoff. Once joined, each pass only
checks membership and rejoins when the check fails:
`portunus:is_member/1` catches a dead or reset local server,
`portunus:is_seed_cluster_member/2` catches a node the seed's cluster
dropped.

The subscriber (by default the process that called `start_link/1`)
receives `{portunus_joiner, Name, joined}` and
`{portunus_joiner, Name, rejoining}`. These are status reports, not
edges: a restarted joiner reports `joined` again with no `rejoining` in
between.

Wait for `joined` before registering children, and do not stop anything
on `rejoining`: it is often a false alarm, and leases protect running
children.

Removing departed members stays with the consumer: only the host knows
that a node is gone for good. See `portunus:remove_member/2`.
""".

-behaviour(gen_server).

-export([start_link/1, recheck/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
%% The pure decision core, exported for the unit and property suites.
-export([decide/2]).

-define(BACKOFF_MIN_MS, 250).
-define(BACKOFF_MAX_MS, 5_000).
-define(DEFAULT_RECHECK_INTERVAL_MS, 60_000).

-type opts() :: #{system     := portunus:system(),
                  name       := portunus:name(),
                  candidates := fun(() -> [node()]),
                  ensure_system       => fun(() -> portunus:ok_or_error(term())),
                  subscriber          => pid(),
                  recheck_interval_ms => pos_integer()}.

-type status() :: joining | member.
-type event() :: trigger | pass_ok | {pass_failed, term()}.
-type action() :: {notify, joined | rejoining} | {arm, non_neg_integer()}.
-type decide_state() :: #{status := status(),
                          backoff_ms := pos_integer(),
                          recheck_interval_ms := pos_integer()}.
-export_type([opts/0]).

-record(state, {system :: portunus:system(),
                name :: portunus:name(),
                candidates :: fun(() -> [node()]),
                ensure_system :: fun(() -> portunus:ok_or_error(term())),
                subscriber :: pid(),
                decide :: decide_state(),
                timer :: reference()}).

-doc """
Start a joiner for cluster `name` on Ra system `system`, linked to the
caller.

`candidates` returns the nodes that should form one cluster, typically
the host's membership view. `node()` is added and the list sorted. A
raising fun counts as a failed attempt and is retried.

`ensure_system` runs before every join attempt and defaults to a fun
returning `ok`. A tenant of a host-owned system passes
`fun() -> portunus:use_system(System) end`. The fun is retried with the
join because it can fail early in the host's boot.

`recheck_interval_ms` (default 60000) paces the periodic membership
re-check.
""".
-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{system := System, name := Name, candidates := Candidates} = Opts0)
  when is_atom(System), is_atom(Name), is_function(Candidates, 0) ->
    Opts = maps:merge(#{ensure_system => fun() -> ok end,
                        subscriber => self(),
                        recheck_interval_ms => ?DEFAULT_RECHECK_INTERVAL_MS},
                      Opts0),
    %% A bad interval would otherwise only crash at the first re-arm
    %% after a join, far from the caller.
    case invalid_options(Opts) of
        [] -> gen_server:start_link(?MODULE, Opts, []);
        Keys -> {error, {invalid_options, Keys}}
    end.

invalid_options(#{ensure_system := EnsureSystem, subscriber := Subscriber,
                  recheck_interval_ms := RecheckMs}) ->
    [K || {K, false} <-
              [{ensure_system, is_function(EnsureSystem, 0)},
               {subscriber, is_pid(Subscriber)},
               {recheck_interval_ms,
                is_integer(RecheckMs) andalso RecheckMs > 0}]].

-doc """
Re-evaluate membership now. Call this from the host's membership event
handler. It is asynchronous, and a burst of calls runs one pass.
""".
-spec recheck(pid()) -> ok.
recheck(Pid) ->
    gen_server:cast(Pid, recheck).

init(#{system := System, name := Name, candidates := Candidates,
       ensure_system := EnsureSystem, subscriber := Subscriber,
       recheck_interval_ms := RecheckMs}) ->
    proc_lib:set_label({?MODULE, Name}),
    {ok, #state{system = System,
                name = Name,
                candidates = Candidates,
                ensure_system = EnsureSystem,
                subscriber = Subscriber,
                decide = #{status => joining,
                           backoff_ms => ?BACKOFF_MIN_MS,
                           recheck_interval_ms => RecheckMs},
                timer = arm(0)}}.

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(recheck, State) ->
    {noreply, apply_event(trigger, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, Ref, pass}, #state{timer = Ref} = State) ->
    {noreply, apply_event(run_pass(State), State)};
handle_info(_Msg, State) ->
    %% Includes stale timer fires: a reference mismatch after a cancel.
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%----------------------------------------------------------------------
%% The decision core
%%----------------------------------------------------------------------

-doc """
The decision core: an event and the current state in, the actions and
the next state out. It is pure, so the unit and property suites drive it
without a cluster. The `gen_server` runs the passes and performs the
actions.

A trigger re-arms the single timer to fire immediately, so bursts
coalesce into one pass. A failed pass while joining doubles the backoff.
A failed membership check reports `rejoining` and rejoins on the next,
immediately armed pass.
""".
-spec decide(event(), decide_state()) -> {[action()], decide_state()}.
decide(trigger, DState) ->
    {[{arm, 0}], DState};
decide(pass_ok, #{status := joining, recheck_interval_ms := RecheckMs} = DState) ->
    {[{notify, joined}, {arm, RecheckMs}],
     DState#{status := member, backoff_ms := ?BACKOFF_MIN_MS}};
decide(pass_ok, #{status := member, recheck_interval_ms := RecheckMs} = DState) ->
    {[{arm, RecheckMs}], DState};
decide({pass_failed, _Reason}, #{status := joining, backoff_ms := Backoff} = DState) ->
    {[{arm, Backoff}],
     DState#{backoff_ms := min(Backoff * 2, ?BACKOFF_MAX_MS)}};
decide({pass_failed, _Reason}, #{status := member} = DState) ->
    {[{notify, rejoining}, {arm, 0}],
     DState#{status := joining, backoff_ms := ?BACKOFF_MIN_MS}}.

apply_event(Event, #state{decide = DState0} = State) ->
    {Actions, DState} = decide(Event, DState0),
    lists:foldl(fun perform/2, State#state{decide = DState}, Actions).

perform({notify, Status}, #state{name = Name, subscriber = Subscriber} = State) ->
    Subscriber ! {?MODULE, Name, Status},
    State;
perform({arm, Ms}, #state{timer = Ref} = State) ->
    _ = erlang:cancel_timer(Ref),
    State#state{timer = arm(Ms)}.

arm(Ms) ->
    erlang:start_timer(Ms, self(), pass).

%%----------------------------------------------------------------------
%% Passes
%%----------------------------------------------------------------------

%% A pass can block for several peer probe timeouts. Nothing a consumer
%% calls waits on one.
run_pass(#state{decide = #{status := joining}} = State) ->
    try
        join_pass(State)
    catch Class:Reason ->
            {pass_failed, {Class, Reason}}
    end;
run_pass(#state{decide = #{status := member},
                name = Name, candidates = Candidates}) ->
    try portunus:is_member(Name)
        andalso portunus:is_seed_cluster_member(Name, candidates(Candidates)) of
        true -> pass_ok;
        false -> {pass_failed, not_a_member}
    catch Class:Reason ->
            {pass_failed, {Class, Reason}}
    end.

join_pass(#state{system = System, name = Name,
                 candidates = Candidates, ensure_system = EnsureSystem}) ->
    maybe
        ok ?= EnsureSystem(),
        ok ?= portunus:join_or_form(System, Name, candidates(Candidates)),
        pass_ok
    else
        {error, Reason} -> {pass_failed, Reason};
        Other -> {pass_failed, {unexpected_return, Other}}
    end.

candidates(Fun) ->
    lists:usort([node() | Fun()]).
