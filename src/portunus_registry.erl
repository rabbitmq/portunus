%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_registry).
-moduledoc """
A dynamic cluster-wide supervisor. "Dynamic" isn't an Erlang/OTP supervisor
type here: it means children are added and removed at runtime with `add/3`
and `remove/2`, unlike `portunus_supervisor`, whose children are the fixed
set returned once by `init/1`.

`add/3` registers a child spec (a `supervisor:child_spec()`, or one carrying
an extended `{permanent, Delay}` restart option which `portunus_delayed_restart`
rewrites) under a key; `portunus` runs one election per key, and the elected
owner starts that child under a local Erlang/OTP supervisor.

`remove/2` stops the local election; the child is gone cluster-wide once every node that
added it calls `remove/2`, and a `remove/2` on the owner alone moves the
child to another node.

No new replicated state: the registry holds only local election pids and
a local supervisor. Restart is local and per-child, with one exception: a
child that crash-loops past the local supervisor's intensity takes the
registry down with `{local_sup_down, _}`, and the restarted registry is
empty — the host re-adds its children, as it does after a node restart.

The registry's cleanup releases cluster-wide locks, so a child spec
supervising it should give it a generous `shutdown` value.
""".

-include("portunus.hrl").

-behaviour(gen_server).
%% Also the `portunus_election` callback that adapts an election win to a
%% `supervisor:start_child/2` on the local supervisor.
-behaviour(portunus_election).

-export([start_link/2, start_link/3, add/2, add/3, remove/2, keys/1,
         owned_keys/1, which_children/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
%% portunus_election callbacks
-export([elected/1, stepped_down/1]).

-record(state, {name :: portunus:name(),
                %% Namespaces this registry's lock keys as {Group, Key}, so
                %% several registries can share one cluster without colliding.
                group :: term(),
                ttl_ms :: pos_integer(),
                affinity = default :: portunus_affinity:spec(),
                local_sup :: pid(),
                %% Key => {ElectionPid | restarting, ChildSpec}. `restarting`
                %% marks the backoff between an election crash and its
                %% restart, so `remove/2` can cancel the pending restart.
                elections = #{} ::
                    #{term() => {pid() | restarting,
                                 portunus_delayed_restart:child_spec_in()}}}).

%% A gen_server reference, so callers can address a name-registered
%% registry (see `start_link/3`) instead of threading a pid.
-type server() :: gen_server:server_ref().
-type registry_opts() :: #{ttl_ms => pos_integer(),
                           sup_flags => supervisor:sup_flags(),
                           affinity => portunus_affinity:spec(),
                           group => term()}.
-type add_error() :: {invalid_child_spec, term()} |
                     {already_added, term()} |
                     {duplicate_child_id, term()}.
-export_type([server/0, registry_opts/0, add_error/0]).

-spec start_link(portunus:name(), registry_opts()) ->
    {ok, pid()} | {error, term()}.
start_link(Name, Opts) when ?IS_RENEWABLE_TTL_OPT(Opts) ->
    gen_server:start_link(?MODULE, {Name, Opts}, []).

-doc """
Like `start_link/2`, but registers the registry process under `ServerName`
(e.g. `{local, shovel_registry}`) so `add/3`, `remove/2` and the rest can
address it by that name instead of a pid.
""".
-spec start_link(gen_server:server_name(), portunus:name(), registry_opts()) ->
    {ok, pid()} | {error, term()}.
start_link(ServerName, Name, Opts) when ?IS_RENEWABLE_TTL_OPT(Opts) ->
    gen_server:start_link(ServerName, ?MODULE,
                          {Name, with_group(ServerName, Opts)}, []).

%% A named registry defaults its group to the registered name; an anonymous one
%% defaults to the cluster name and must set `group` to run beside another.
with_group({local, RegName}, Opts) ->
    maps:merge(#{group => RegName}, Opts);
with_group({global, RegName}, Opts) ->
    maps:merge(#{group => RegName}, Opts);
with_group({via, _Mod, RegName}, Opts) ->
    maps:merge(#{group => RegName}, Opts);
with_group(_ServerName, Opts) ->
    Opts.

-doc """
Register `ChildSpec` under its child id. Only one instance of it can run
in the cluster at any given time.
""".
-spec add(server(), portunus_delayed_restart:child_spec_in()) ->
    ok | {error, add_error()}.
add(Server, ChildSpec) ->
    try child_id(ChildSpec) of
        Id -> add(Server, Id, ChildSpec)
    catch _:_ ->
        {error, {invalid_child_spec, ChildSpec}}
    end.

-doc """
Register `ChildSpec` under `Key`. Only one instance of it can run in the
cluster at any given time. Validated
at registration: an invalid spec, a re-add with a different spec, or a
child id already used under another key is an error, not a later
elect-and-fail loop. Re-adding the identical spec is idempotent.
""".
-spec add(server(), term(), portunus_delayed_restart:child_spec_in()) ->
    ok | {error, add_error()}.
add(Server, Key, ChildSpec) ->
    gen_server:call(Server, {add, Key, ChildSpec}, infinity).

-doc """
Stop and forget the child keyed by `Key` on this node. The key is only
gone cluster-wide once every node that added it calls `remove/2`; removing
it on the owner alone moves the child to another contender.
""".
-spec remove(server(), term()) -> ok.
remove(Server, Key) ->
    gen_server:call(Server, {remove, Key}, infinity).

-doc "The keys this node is contending for.".
-spec keys(server()) -> [term()].
keys(Server) ->
    gen_server:call(Server, keys, infinity).

-doc "Keys for which this node is currently the elected owner.".
-spec owned_keys(server()) -> [term()].
owned_keys(Server) ->
    %% Bounded: the reply comes from a plain spawned process, so a caller
    %% must not wait forever on the off chance it dies unreplied.
    gen_server:call(Server, owned_keys, 15000).

-doc """
The children this node currently runs (it was elected for), in
`supervisor:which_children/1` shape `[{Id, Child, Type, Modules}]`.
""".
-spec which_children(server()) ->
    [{term(), pid() | restarting | undefined, worker | supervisor, [module()] | dynamic}].
which_children(Server) ->
    gen_server:call(Server, which_children, infinity).

-spec stop(server()) -> ok.
stop(Server) ->
    %% An already-stopped registry is this call's goal state, not an error.
    try gen_server:stop(Server)
    catch exit:noproc -> ok
    end.

init({Name, Opts}) ->
    process_flag(trap_exit, true),
    proc_lib:set_label({portunus_registry, Name}),
    TtlMs = maps:get(ttl_ms, Opts, 60000),
    Group = maps:get(group, Opts, Name),
    SupFlags = maps:get(sup_flags, Opts,
                        #{strategy => one_for_one, intensity => 10, period => 10}),
    {ok, LocalSup} = supervisor:start_link(portunus_local_sup, {Group, SupFlags}),
    {ok, #state{name = Name, group = Group, ttl_ms = TtlMs,
                affinity = maps:get(affinity, Opts, default),
                local_sup = LocalSup}}.

handle_call({add, Key, ChildSpec}, _From,
            #state{elections = Elections} = State) ->
    case validate(Key, ChildSpec, Elections) of
        ok ->
            {ok, Pid} = start_election(Key, ChildSpec, State),
            {reply, ok,
             State#state{elections = Elections#{Key => {Pid, ChildSpec}}}};
        idempotent ->
            {reply, ok, State};
        {error, _} = Err ->
            {reply, Err, State}
    end;
handle_call({remove, Key}, _From, #state{elections = Elections,
                                         local_sup = LocalSup} = State) ->
    case maps:take(Key, Elections) of
        {{Pid, Spec}, Rest} when is_pid(Pid) ->
            %% Bounded: an election stuck in user callback code must not
            %% wedge the registry and every caller queued behind it.
            ok = portunus_election:stop_all([Pid], 5000),
            %% A live election stopped its child in `stepped_down`; one that
            %% just died could not, and its `'EXIT'` may still be queued
            %% behind this call. Stopping by id is idempotent either way.
            stop_local_child(LocalSup, child_id(Spec)),
            {reply, ok, State#state{elections = Rest}};
        {{restarting, Spec}, Rest} ->
            %% Taking the entry cancels the pending restart.
            stop_local_child(LocalSup, child_id(Spec)),
            {reply, ok, State#state{elections = Rest}};
        error ->
            {reply, ok, State}
    end;
handle_call(keys, _From, #state{elections = Elections} = State) ->
    {reply, maps:keys(Elections), State};
handle_call(owned_keys, From, #state{elections = Elections} = State) ->
    Entries = [{Key, Pid} || {Key, {Pid, _}} <- maps:to_list(Elections),
                             is_pid(Pid)],
    %% Answered off-process and probed concurrently: each `is_leader` can
    %% block up to its timeout while an election sits in a Ra command, and
    %% an introspection call must not stall the registry, or its caller,
    %% for the sum.
    _ = spawn(fun() -> gen_server:reply(From, probe_owned(Entries)) end),
    {noreply, State};
handle_call(which_children, _From, #state{local_sup = LocalSup} = State) ->
    {reply, supervisor:which_children(LocalSup), State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', LocalSup, Reason}, #state{local_sup = LocalSup} = State) ->
    %% The local supervisor died (a child crash-looped past intensity): fatal,
    %% so this registry's own supervisor restarts it cleanly.
    {stop, {local_sup_down, Reason}, State};
handle_info({'EXIT', Pid, Reason},
            #state{elections = Elections, local_sup = LocalSup} = State) ->
    case election_of(Pid, Elections) of
        {Key, ChildSpec} ->
            %% A crashed election (`remove/2` removes the entry first) ran no
            %% terminate, so stop its orphaned child before it becomes a second
            %% owner, then restart the election after a backoff. The entry
            %% stays, marked `restarting`, so a `remove/2` in the backoff
            %% window cancels the restart instead of resurrecting the child.
            stop_local_child(LocalSup, child_id(ChildSpec)),
            logger:warning("portunus registry: election for ~p exited (~p); "
                           "restarting", [Key, Reason]),
            erlang:send_after(1000, self(), {restart_election, Key}),
            {noreply, State#state{elections =
                                      Elections#{Key := {restarting, ChildSpec}}}};
        not_found ->
            {noreply, State}
    end;
handle_info({restart_election, Key}, #state{elections = Elections} = State) ->
    case Elections of
        #{Key := {restarting, ChildSpec}} ->
            {ok, Pid} = start_election(Key, ChildSpec, State),
            {noreply, State#state{elections =
                                      Elections#{Key := {Pid, ChildSpec}}}};
        _ ->
            %% Removed, or re-added and already running.
            {noreply, State}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{elections = Elections, local_sup = LocalSup}) ->
    ok = portunus_election:stop_all([P || {P, _} <- maps:values(Elections),
                                          is_pid(P)]),
    %% The local sup dies with this process; its restart markers would
    %% otherwise sit in the node-global table forever.
    portunus_delayed_restart:forget_all(LocalSup).

%%----------------------------------------------------------------------
%% portunus_election callbacks
%%----------------------------------------------------------------------

-doc false.
elected(#{args := {LocalSup, ChildSpec}}) ->
    Id = child_id(ChildSpec),
    ok = start_local_child(LocalSup, Id, portunus_delayed_restart:child_spec(ChildSpec)),
    {ok, {LocalSup, Id}}.

-doc false.
stepped_down({LocalSup, ChildId}) ->
    stop_local_child(LocalSup, ChildId).

%% Stop and forget an elected child. `forget/2` clears the delayed-restart
%% marker so the next owner (a deliberate hand-off) starts immediately.
stop_local_child(LocalSup, ChildId) ->
    _ = supervisor:terminate_child(LocalSup, ChildId),
    _ = supervisor:delete_child(LocalSup, ChildId),
    ok = portunus_delayed_restart:forget(LocalSup, ChildId).

child_id(#{id := Id}) -> Id;
child_id({Id, _, _, _, _, _}) -> Id.

lock_key(Group, Key) -> {Group, Key}.

%% Registration-time validation, so a bad spec is an error to the caller
%% rather than an endless elect-and-fail loop on whichever node wins.
validate(Key, ChildSpec, Elections) ->
    case maps:find(Key, Elections) of
        {ok, {_, ChildSpec}} ->
            idempotent;
        {ok, {_, _Other}} ->
            {error, {already_added, Key}};
        error ->
            try {child_id(ChildSpec),
                 portunus_delayed_restart:child_spec(ChildSpec)} of
                {Id, Rewritten} ->
                    case supervisor:check_childspecs([Rewritten]) of
                        ok -> unique_child_id(Id, Elections);
                        {error, Reason} ->
                            {error, {invalid_child_spec, Reason}}
                    end
            catch _:_ ->
                    {error, {invalid_child_spec, ChildSpec}}
            end
    end.

%% Two keys sharing one child id would share one local child: stepping one
%% down would stop the other's child.
unique_child_id(Id, Elections) ->
    Clash = [K || {K, {_, Spec}} <- maps:to_list(Elections),
                  child_id(Spec) =:= Id],
    case Clash of
        [] -> ok;
        _ -> {error, {duplicate_child_id, Id}}
    end.

start_election(Key, ChildSpec, #state{name = Name, group = Group, ttl_ms = TtlMs,
                                      affinity = Affinity, local_sup = LocalSup}) ->
    portunus_election:start_link(Name, lock_key(Group, Key), ?MODULE,
                                 {LocalSup, ChildSpec},
                                 #{ttl_ms => TtlMs, affinity => Affinity}).

election_of(Pid, Elections) ->
    case [{K, S} || {K, {P, S}} <- maps:to_list(Elections), P =:= Pid] of
        [{Key, Spec}] -> {Key, Spec};
        [] -> not_found
    end.

%% An election blocked in a Ra command (a quorum loss, a slow child start)
%% is not the elected owner; treating the timeout as `false` keeps an
%% introspection call from crashing the registry.
is_leader_or_false(Pid) ->
    try portunus_election:is_leader(Pid, 1000)
    catch exit:_ -> false
    end.

%% Each probe delivers its verdict as its exit reason, which dialyzer reads
%% as a fun that never returns normally; that is the point.
-dialyzer({nowarn_function, probe_owned/1}).
probe_owned(Entries) ->
    Probes = [{Key, spawn_monitor(fun() -> exit(is_leader_or_false(Pid)) end)}
              || {Key, Pid} <- Entries],
    [Key || {Key, {_, Ref}} <- Probes,
            receive {'DOWN', Ref, process, _, Verdict} -> Verdict =:= true end].

%% Start the elected child, tolerating one the previous owner left
%% terminating or a stale spec from a rapid lose-then-win on this node.
%% Other errors exit with a clear reason: `become_leader` logs it and
%% releases the lock to re-contend.
start_local_child(LocalSup, Id, Spec) ->
    case supervisor:start_child(LocalSup, Spec) of
        {ok, _Pid} -> ok;
        {ok, _Pid, _Info} -> ok;
        {error, {already_started, _Pid}} -> ok;
        {error, already_present} ->
            _ = supervisor:delete_child(LocalSup, Id),
            retry_start_local_child(LocalSup, Spec);
        {error, Reason} ->
            exit({child_start_failed, Reason})
    end.

retry_start_local_child(LocalSup, Spec) ->
    case supervisor:start_child(LocalSup, Spec) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, Reason} ->
            exit({child_start_failed, Reason})
    end.
