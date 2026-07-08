%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_service).
-moduledoc """
A managed set of keys, each with exactly one owner in the cluster at any
given time. When an owner's lease is lost, ownership moves to another
node. Runs one `portunus_election` per key. The callback module supplies
the key set and the per-key work:

```erlang
-callback keys(Args :: term()) -> [Key :: term()].
-callback start(Key :: term(), Token :: portunus:token(), Args :: term()) ->
    {ok, State :: term()}.
-callback stop(Key :: term(), State :: term()) -> ok.
```

`start/3` must link whatever it starts to the calling process: a crashed
election loses the callback state, and an unlinked resource then keeps
running with no way to stop it.

Every node contends eagerly, and the election keeps a single winner per
key until that winner's lease is lost. Pass `#{affinity => Spec}` (see
`portunus_affinity`) to steer which
node wins, for example a preferred-owner or consistent-hash hint; the
default is FIFO in arrival (registration) order.
""".

-include("portunus.hrl").

-behaviour(gen_server).
%% This module is also the `portunus_election` callback that adapts
%% elections to the user's `start/3` and `stop/2`.
-behaviour(portunus_election).

-export([start_link/3, start_link/4, transfer/3, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
%% portunus_election callbacks
-export([elected/1, stepped_down/1]).

-callback keys(term()) -> [term()].
-callback start(term(), portunus:token(), term()) -> {ok, term()}.
-callback stop(term(), term()) -> ok.

-record(state, {name :: portunus:name(),
                %% Namespaces lock keys as {Group, Key}, so several services can
                %% share one cluster without colliding; defaults to the module.
                group :: term(),
                mod :: module(),
                args :: term(),
                ttl_ms :: pos_integer(),
                affinity = default :: portunus_affinity:spec(),
                elections = #{} :: #{term() => pid()}}).

-type service_opts() :: #{ttl_ms => pos_integer(),
                          affinity => portunus_affinity:spec(),
                          group => term()}.
-export_type([service_opts/0]).

-spec start_link(portunus:name(), module(), term()) ->
    {ok, pid()} | {error, term()}.
start_link(Name, Mod, Args) ->
    start_link(Name, Mod, Args, #{}).

-spec start_link(portunus:name(), module(), term(), service_opts()) ->
    {ok, pid()} | {error, term()}.
start_link(Name, Mod, Args, Opts) when ?IS_RENEWABLE_TTL_OPT(Opts) ->
    gen_server:start_link(?MODULE, {Name, Mod, Args, Opts}, []).

-doc """
If this node currently owns `Key`, hand ownership to `TargetNode`; if not,
return `{error, not_owner}`. Only the owner can transfer. If
`TargetNode` is not a ready contender the owner keeps running and the reply is
`{error, {no_contender, TargetNode}}`.
""".
-spec transfer(pid(), term(), node()) ->
    portunus:ok_or_error({no_contender, node()} | not_owner | no_quorum).
transfer(Server, Key, TargetNode) ->
    gen_server:call(Server, {transfer, Key, TargetNode}, infinity).

-spec stop(pid()) -> ok.
stop(Pid) ->
    %% An already-stopped service is this call's goal state, not an error.
    try gen_server:stop(Pid)
    catch exit:noproc -> ok
    end.

init({Name, Mod, Args, Opts}) ->
    process_flag(trap_exit, true),
    proc_lib:set_label({portunus_service, Name, Mod}),
    State0 = #state{name = Name, group = maps:get(group, Opts, Mod), mod = Mod,
                    args = Args, ttl_ms = maps:get(ttl_ms, Opts, 60000),
                    affinity = maps:get(affinity, Opts, default)},
    Elections = lists:foldl(
                  fun(Key, Acc) ->
                          {ok, Pid} = start_election(Key, State0),
                          maps:put(Key, Pid, Acc)
                  end, #{}, Mod:keys(Args)),
    {ok, State0#state{elections = Elections}}.

handle_call(elections, _From, State) ->
    {reply, State#state.elections, State};
handle_call({transfer, Key, TargetNode}, From,
            #state{elections = Elections} = State) ->
    case maps:find(Key, Elections) of
        {ok, Pid} when is_pid(Pid) ->
            %% Offloaded so the service stays responsive while one election
            %% runs its `stepped_down` and the fenced command.
            _ = spawn(fun() -> gen_server:reply(From, transfer_to(Pid, TargetNode)) end),
            {noreply, State};
        _ ->
            {reply, {error, not_owner}, State}
    end;
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, #state{elections = Elections} = State) ->
    case [K || {K, P} <- maps:to_list(Elections), P =:= Pid] of
        [Key] ->
            %% An election crashed: restart it after a backoff so the node keeps
            %% contending for the key.
            logger:warning("portunus service: election for ~p exited (~p); "
                           "restarting", [Key, Reason]),
            erlang:send_after(1000, self(), {restart_election, Key}),
            {noreply, State#state{elections = maps:remove(Key, Elections)}};
        [] ->
            {noreply, State}
    end;
handle_info({restart_election, Key}, #state{elections = Elections} = State) ->
    case maps:is_key(Key, Elections) of
        true ->
            {noreply, State};
        false ->
            {ok, Pid} = start_election(Key, State),
            {noreply, State#state{elections = Elections#{Key => Pid}}}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    portunus_election:stop_all(maps:values(State#state.elections)).

%%----------------------------------------------------------------------
%% `portunus_election` callbacks (adapter to the user's `start/3` and `stop/2`)
%%----------------------------------------------------------------------

-doc false.
elected(#{token := Token, args := {Mod, Key, Args}}) ->
    {ok, UserState} = Mod:start(Key, Token, Args),
    {ok, {Mod, Key, UserState}}.

-doc false.
stepped_down({Mod, Key, UserState}) ->
    Mod:stop(Key, UserState).

start_election(Key, #state{name = Name, group = Group, ttl_ms = TtlMs,
                           mod = Mod, args = Args, affinity = Affinity}) ->
    portunus_election:start_link(Name, {Group, Key}, ?MODULE, {Mod, Key, Args},
                                 #{ttl_ms => TtlMs, affinity => Affinity}).

%% A wedged or timed-out election surfaces as a transient `no_quorum` the
%% caller retries, rather than crashing the spawned reply process unreplied.
transfer_to(Pid, TargetNode) ->
    try portunus_election:transfer_to(Pid, TargetNode)
    catch exit:_ -> {error, no_quorum}
    end.
