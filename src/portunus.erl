%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus).
-moduledoc """
The primary API of the `portunus` lock server. Its shape is inspired by
`ra`'s own API.

Lifecycle functions take a `name()` when they act on the local node
(`restart_server/2`, `join_or_form/3`) and a `server_id()` when they can
target any member's replica (`reset_server/2`).
""".

%% Cluster lifecycle.
-export([start_system/2,
         ensure_started/1,
         start_cluster/3,
         join_cluster/3,
         join_or_form/3,
         reset_and_join_cluster/3,
         restart_server/2,
         add_member/2,
         remove_member/2,
         reset_server/2,
         members/1]).

%% Leases.
-export([grant_lease/2, grant_lease/3,
         renew_leases/2, renew_leases/3,
         revoke_lease/2,
         keep_alive/3]).

%% Locks.
-export([acquire/4, acquire/5,
         acquire_or_join_succession_queue/4,
         acquire_or_join_succession_queue/5,
         acquire_with_timeout/5, acquire_with_timeout/6,
         leave_succession_queue/3,
         release/3,
         transfer/4,
         contenders/2,
         owner/2]).

%% Watch.
-export([watch/2, unwatch/2]).

%% Health and introspection.
-export([has_quorum/1, is_member/1, status/1]).

%% Conveniences.
-export([lock/3, unlock/1, with_lock/4]).

%% Exported (undocumented) so both settlement outcomes are testable directly:
%% the race that reaches it in production cannot be timed deterministically.
-export([settle_timed_out_bid/3]).

%% Exported (undocumented) so seed selection is testable with a supplied predicate.
-export([effective_seed/2]).

-include("portunus.hrl").

-define(CMD_TIMEOUT, 5000).

-type name() :: atom().
-type system() :: atom().
-type lock_key() :: portunus_machine:lock_key().
-type lease_id() :: portunus_machine:lease_id().
-type token() :: portunus_machine:token().
-type owner() :: portunus_machine:owner().
-type ttl() :: pos_integer().
-type server_id() :: ra:server_id().

%% The current holder of a lock, as reported by `owner/2`.
-type owner_info() :: portunus_machine:owner_info().

%% `acquire_or_join_succession_queue/5` options. `affinity` decides which
%% contender is promoted first (see `portunus_affinity`); the default is FIFO.
%% `context` is attached to the grant on promotion, exactly as `acquire/5`
%% attaches it to an "immediate" grant.
-type succession_opts() :: #{affinity => portunus_affinity:spec(),
                             context => term()}.

%% A watch registration handle from `watch/2`, passed to `unwatch/2` to stop watching.
%% Current Raft indices are used as watch references.
-type watch_ref() :: portunus_machine:watch_ref().

%% `grant_lease/3` options: `auto_renew` attaches a holder-linked renewer
%% so the lease lives as long as the caller.
%% See `portunus_keepalive`.
-type grant_opts() :: #{proposed_id => lease_id(), auto_renew => boolean()}.

%% `ensure_started/1` environment: every key is optional and defaulted.
-type env() :: #{ra_system => system(),
                 name => name(),
                 data_dir => file:filename_all(),
                 membership => [node()] | {module(), atom()} | local}.

%% `status/1`: leader, members and quorum are always present; the machine-derived
%% counts are absent if the status query could not be served.
-type status() :: #{leader := option(server_id()),
                    members := [server_id()],
                    quorum := boolean(),
                    leases => non_neg_integer(),
                    locks => non_neg_integer(),
                    waiters => non_neg_integer(),
                    watchers => non_neg_integer(),
                    fencing_token => non_neg_integer()}.

%% Errors: any command may yield `no_quorum`, meaning there was no online Raft quorum.
-type lease_error()   :: id_in_use | no_quorum.
-type acquire_error() :: {held_by, owner()} | lease_expired | no_quorum.
-type release_error() :: not_owner | not_held | no_quorum.
-type transfer_error() :: not_owner | {no_contender, owner()} | no_quorum.
-type leave_queue_error() :: not_queued | no_quorum.
-type acquisition_timeout_error() :: timeout | acquire_error().

%% The result shape of every command that carries no value.
-type ok_or_error(E) :: ok | {error, E}.

%% A value that may be absent.
-type option(T) :: T | undefined.

%% Returned by `lock/3`, consumed by `unlock/1`: opaque on purpose, so
%% callers treat it as a token rather than a map to destructure.
-opaque handle() :: #{name := name(), key := lock_key(), lease := lease_id(),
                      token := token(), renewer := pid()}.

-export_type([name/0, system/0, lock_key/0, lease_id/0, token/0, owner/0,
              ttl/0, server_id/0, owner_info/0, succession_opts/0, watch_ref/0,
              grant_opts/0, env/0, status/0, lease_error/0, acquire_error/0,
              release_error/0, transfer_error/0, leave_queue_error/0,
              acquisition_timeout_error/0, ok_or_error/1, option/1,
              handle/0]).

%%
%% Cluster lifecycle
%%

-doc """
Start a dedicated Ra system to use for `portunus`. Idempotent, and safe to call
again after the `ra` application has been restarted.
""".
-spec start_system(system(), file:filename_all()) -> ok_or_error(term()).
start_system(System, DataDir) ->
    maybe
        {ok, _} ?= application:ensure_all_started(ra),
        {ok, _} ?= application:ensure_all_started(seshat),
        %% The portunus app owns node-global state for the node's lifetime: it
        %% initialises the counters and, through `portunus_sup`, owns the
        %% delayed-restart marker table.
        {ok, _} ?= application:ensure_all_started(portunus),
        %% `server_recovery_strategy => registered` restarts this node's
        %% replicas from disk on system start, so a restart rejoins through any
        %% live quorum with no seed. It is inert when reusing a system portunus
        %% did not start, since `start_system/2` is then a no-op.
        Config = (ra_system:default_config())#{
                   name => System,
                   data_dir => DataDir,
                   server_recovery_strategy => registered,
                   names => ra_system:derive_names(System)},
        ensure_system_started(System, Config)
    else
        {error, _} = Err -> Err
    end.

%% Always go through `ra_system:start/1`: it re-creates the system, and
%% recovers its servers from disk, whenever the `ra` application has been
%% restarted. Guarding on `ra_system:fetch/1` instead would skip that, because
%% its config lives in a `persistent_term` that outlives the system's processes,
%% so a restarted system looks present while its ETS tables are gone.
%% `already_present` is a child spec left by a torn-down system: drop it and
%% start again.
ensure_system_started(System, Config) ->
    case ra_system:start(Config) of
        {error, already_present} ->
            _ = ra_system:stop(System),
            started(ra_system:start(Config));
        Result ->
            started(Result)
    end.

%% Map a `supervisor:startchild_ret()` to ok: a freshly started system, an
%% already-running one, or an error to propagate.
-spec started(supervisor:startchild_ret()) -> ok_or_error(term()).
started({ok, _}) -> ok;
started({ok, _, _}) -> ok;
started({error, {already_started, _}}) -> ok;
started({error, _} = Err) -> Err.

-doc """
Form (or join) a single cluster-wide portunus cluster from an `Env`
map. Every member runs a replica.

This forms across all `membership` nodes at once, for a coordinated start where
they come up together. For nodes that bootstrap independently, each running its
own retry loop, use `join_or_form/3` instead.
""".
-spec ensure_started(env()) ->
    {ok, [server_id()], [server_id()]} | {error, term()}.
ensure_started(Env) ->
    System = maps:get(ra_system, Env, portunus),
    Name = maps:get(name, Env, portunus),
    DataDir = maps:get(data_dir, Env, default_data_dir(System)),
    Nodes = resolve_membership(maps:get(membership, Env, [node()])),
    maybe
        ok ?= start_system(System, DataDir),
        start_cluster(System, Name, Nodes)
    else
        {error, _} = Err -> Err
    end.

-doc """
Start a portunus Ra cluster named `Name` across `Nodes`. Ra registers each
node's server locally under `Name`, so `Name` must not collide with another
registered process: a local collision is reported as
`{error, {name_registered, Pid}}` rather than left to surface as
`cluster_not_formed`.
""".
-spec start_cluster(system(), name(), [node()]) ->
    {ok, [server_id()], [server_id()]} | {error, term()}.
start_cluster(System, Name, Nodes) ->
    case ensure_name_unregistered(System, Name) of
        ok ->
            ServerIds = [{Name, N} || N <- Nodes],
            ra:start_cluster(System, Name, machine(Name), ServerIds);
        {error, _} = Err ->
            Err
    end.

-doc """
Start a local server and join the existing cluster that `SeedNode`
belongs to. The local Ra system must already be running (`start_system`).
Idempotent: succeeds if this node is already a member. As in `start_cluster/3`,
the cluster `Name` is registered locally, so a collision with another registered
process is reported as `{error, {name_registered, Pid}}`.
""".
-spec join_cluster(system(), name(), node()) -> ok_or_error(term()).
join_cluster(System, Name, SeedNode) ->
    case ensure_name_unregistered(System, Name) of
        ok ->
            join_cluster1(System, Name, SeedNode);
        {error, _} = Err ->
            Err
    end.

join_cluster1(System, Name, SeedNode) ->
    case ra:members({Name, SeedNode}, ?CMD_TIMEOUT) of
        {ok, Members, Leader} ->
            ServerId = {Name, node()},
            case lists:member(ServerId, Members) of
                true ->
                    ok;
                false ->
                    case ensure_local_server(System, Name, ServerId, Members) of
                        ok -> add_self(Leader, ServerId);
                        {error, _} = Err -> Err
                    end
            end;
        {timeout, _} = T ->
            {error, T};
        {error, _} = Err ->
            Err
    end.

%% A server left by an earlier join attempt whose `add_member` failed (a
%% concurrent join, a leader-change timeout) is fine: proceed to the
%% idempotent `add_member`, or every later retry wedges on `already_started`.
%% `ra_directory` decides "exists locally"; matching `ra:start_server`'s
%% error shapes would tie this to supervisor internals.
ensure_local_server(System, Name, ServerId, Members) ->
    case is_ra_server(System, Name) of
        true ->
            case whereis(Name) of
                undefined -> restart_server(System, Name);
                _ -> ok
            end;
        false ->
            ra:start_server(System, Name, ServerId, machine(Name), Members)
    end.

add_self(Leader, ServerId) ->
    case ra:add_member(Leader, ServerId) of
        {ok, _, _} -> ok;
        {error, already_member} -> ok;
        {timeout, _} = T -> {error, T};
        {error, _} = Err -> Err
    end.

-doc """
Reset this node's replica of cluster `Name` and join the cluster that `SeedNode`
belongs to. Unlike `join_cluster/3`, this first wipes any existing local replica,
so a node that formed its own single-node cluster during parallel boot can be merged into an
existing cluster rather than colliding with it.

The local replica's state is discarded, so call this
during cluster formation, before the node holds any leases worth keeping.
""".
-spec reset_and_join_cluster(system(), name(), node()) -> ok_or_error(term()).
reset_and_join_cluster(System, Name, SeedNode) ->
    ServerId = {Name, node()},
    _ = ra:stop_server(System, ServerId),
    _ = ra:force_delete_server(System, ServerId),
    join_cluster(System, Name, SeedNode).

-doc """
Restart this node's replica of cluster `Name`, recovering its state from disk.
Use this after `start_system/2`, which rebuilds the Ra system.

Returns `{error, name_not_registered}` if this node has no on-disk member data for
`Name`, in which case it was never a member and should use `start_cluster/3` or
`join_cluster/3` instead.
""".
-spec restart_server(system(), name()) -> ok_or_error(term()).
restart_server(System, Name) ->
    case ra:restart_server(System, {Name, node()}) of
        ok -> ok;
        {error, {already_started, _}} -> ok;
        Err -> Err
    end.

-doc """
Attempts to join an existing `portunus` Raft cluster or create one.

The algorithm is based on the concept of a seed member (cluster node)
that is computed as the lowest (in lexicographical order) reachable node
on the `Members` list. The `Members` list *MUST* include the current node:
when using the return value of `erlang:nodes/0`, don't forget to
prepend the value of `erlang:node/0` as well.

The seed node is asynchronously and independently agreed on by different
members, not pre-selected or pre-computed before deployment time.

When a new cluster is formed and a member computes that it itself is the seed,
it kicks off a Raft election. This is true for both single-node and multi-node
clusters.

Most designs that calculate a seed node suffer from that node not being
available. In order to avoid this, `portunus` picks the lowest (first)
reachable member and if that turns out to be a different member, (re)joins that
member's existing Raft cluster.

If the member (node) has existing on-disk member data and the computed seed responds
that the asking (calling) member is *not* on its known cluster member list,
such node will reset itself and join the seed's Raft cluster.

When the seed member `PS` is restarted and finds its pre-existing member data, it will also
compute the seed and should the seed node have changed during the `PS` absence,
join the new seed's cluster. If the existing on-disk data includes the new seed `NS`,
the rejoining `PS` will not reset itself and instead will catch up with the
current Raft leader.

However, it's important to explain when the reset happens. The newly joining
member will clarify its membership with the seed and only then perform
the reset and join the seed.

The companion function that initiates a coordinated start is `ensure_started/1`.

If the seed is unreachable, returns an error (such as `{error, seed_unreachable}`) and does not
reset the local member.
""".
-spec join_or_form(system(), name(), [node()]) -> ok_or_error(term()).
join_or_form(System, Name, Members) when is_list(Members), Members =/= [] ->
    Seed = effective_seed(Members),
    case restart_server(System, Name) of
        ok ->
            converge(System, Name, Seed);
        {error, name_not_registered} when node() =:= Seed ->
            form_or_join_existing(System, Name, Members);
        {error, name_not_registered} ->
            join_cluster(System, Name, Seed);
        {error, _} = Err ->
            Err
    end.

-spec effective_seed([node()]) -> node().
effective_seed(Members) ->
    effective_seed(Members, fun is_reachable/1).

%% Lowest-sorted reachable member; `node()` is always one, so the match never fails.
-spec effective_seed([node()], fun((node()) -> boolean())) -> node().
effective_seed(Members, IsReachable) ->
    {value, Seed} = lists:search(IsReachable, lists:sort(Members)),
    Seed.

is_reachable(Node) ->
    Node =:= node()
        orelse lists:member(Node, nodes())
        orelse net_adm:ping(Node) =:= pong.

%% Form only when no reachable member runs a cluster, so a returning lowest node
%% joins it rather than form a rival the merge would wipe.
form_or_join_existing(System, Name, Members) ->
    Others = [N || N <- lists:sort(Members), N =/= node(), is_reachable(N)],
    case lists:search(fun(N) -> has_cluster(Name, N) end, Others) of
        {value, Peer} ->
            join_cluster(System, Name, Peer);
        false ->
            case start_cluster(System, Name, [node()]) of
                {ok, _, _} -> ok;
                {error, _} = Err -> Err
            end
    end.

has_cluster(Name, Node) ->
    case ra:members({Name, Node}, ?CMD_TIMEOUT) of
        {ok, _, _} -> true;
        _ -> false
    end.

%% Forces "orphan" members to rejoin their seed; a member already with the seed is
%% left alone.
%%
%% This function is needed during initial cluster formation because members can be
%% started in parallel. It has no effect on members that have already rejoined
%% their cluster (seed).
converge(System, Name, Seed) ->
    ServerId = {Name, node()},
    case ra:members({local, ServerId}, ?CMD_TIMEOUT) of
        {ok, [ServerId], _Leader} when node() =:= Seed ->
            %% Sole-member seed recovered leaderless with an empty log (formed,
            %% then restarted before committing): elect it regardless of
            %% `has_log_entries/1`, which is false here and would misroute to
            %% `join_cluster/3` against a leaderless local replica.
            maybe_trigger_single_member_election(ServerId, [ServerId]),
            ok;
        {ok, Members, _Leader} ->
            case lists:member(ServerId, Members) andalso has_log_entries(ServerId) of
                true when node() =:= Seed ->
                    maybe_trigger_single_member_election(ServerId, Members),
                    ok;
                true ->
                    case lists:member({Name, Seed}, Members) of
                        true -> ok;
                        false -> merge_into_seed(System, Name, Seed, ServerId)
                    end;
                false ->
                    join_cluster(System, Name, Seed)
            end;
        _ ->
            %% Local query timed out. Retry rather than joining "as is", so a
            %% "seedless" member is not added without the reset `merge_into_seed/4`
            %% applies to it.
            {error, local_view_unavailable}
    end.

%% Asks the seed if given node is its cluster member. If not,
%% resets the node, then joins the seed.
merge_into_seed(System, Name, Seed, ServerId) ->
    case ra:members({Name, Seed}, ?CMD_TIMEOUT) of
        {ok, SeedMembers, _Leader} ->
            case lists:member(ServerId, SeedMembers) of
                true -> ok;
                false -> reset_and_join_cluster(System, Name, Seed)
            end;
        _ ->
            {error, seed_unreachable}
    end.

%% A single-member cluster recovered by `restart_server/2` is leaderless. Only
%% `start_cluster/3` triggers the initial election, so call it explicitly.
maybe_trigger_single_member_election(ServerId, [ServerId]) ->
    _ = catch ra:trigger_election(ServerId),
    ok;
maybe_trigger_single_member_election(_ServerId, _Members) ->
    ok.

%% Set both identically on every node: `init/1` stores them in replicated
%% state.
machine(Name) ->
    Tick = application:get_env(portunus, tick_interval_ms, 1000),
    SnapInterval = application:get_env(portunus, snapshot_interval, 4096),
    {module, portunus_machine, #{cluster => Name,
                                 tick_interval_ms => Tick,
                                 snapshot_interval => SnapInterval}}.

-doc """
Add a node as a member. Adding automatically, e.g. from a boot
sequence, is safe; removal is never automatic (see `remove_member/2`).
""".
-spec add_member(name(), node()) -> ok_or_error(term()).
add_member(Name, Node) ->
    case ra:add_member(leader_or_local(Name), {Name, Node}) of
        {ok, _, _} -> ok;
        {timeout, _} = T -> {error, T};
        {error, _} = Err -> Err
    end.

-doc "Remove a member. Removal is explicit, never automatic.".
-spec remove_member(name(), node()) -> ok_or_error(term()).
remove_member(Name, Node) ->
    case ra:remove_member(leader_or_local(Name), {Name, Node}) of
        {ok, _, _} -> ok;
        {timeout, _} = T -> {error, T};
        {error, _} = Err -> Err
    end.

-doc "Stop and wipe a server's data dir. Refuses if still a member.".
-spec reset_server(system(), server_id()) ->
    ok_or_error(still_member | no_quorum).
reset_server(System, {Name, _Node} = ServerId) ->
    case ra:members({Name, node()}, ?CMD_TIMEOUT) of
        {ok, Members, _} ->
            case lists:member(ServerId, Members) of
                true ->
                    {error, still_member};
                false ->
                    _ = ra:stop_server(System, ServerId),
                    _ = ra:force_delete_server(System, ServerId),
                    ok
            end;
        _ ->
            %% Refuse rather than risk wiping a live member: a failed membership
            %% query (a transient no_quorum) is not proof of non-membership.
            {error, no_quorum}
    end.

-doc "Current members and the leader.".
-spec members(name()) -> {ok, [server_id()], server_id()} | {error, term()}.
members(Name) ->
    case ra:members({Name, node()}, ?CMD_TIMEOUT) of
        {ok, _, _} = Ok -> Ok;
        {timeout, _} = T -> {error, T};
        {error, _} = Err -> Err
    end.

%%----------------------------------------------------------------------
%% Leases
%%----------------------------------------------------------------------

-doc "Grant a lease with a TTL in milliseconds. See `grant_lease/3` for options.".
-spec grant_lease(name(), ttl()) -> {ok, lease_id()} | {error, lease_error()}.
grant_lease(Name, TtlMs) when is_integer(TtlMs), TtlMs > 0 ->
    grant_lease(Name, TtlMs, #{}).

-doc """
Grant a lease with a TTL in milliseconds. With `#{auto_renew => true}` a
holder-linked renewer keeps it alive for as long as the calling process
lives; the lease (and any locks held under it) ends when the caller dies
or revokes. The returned id is used exactly like any other. A
`proposed_id` makes the grant idempotent under retry; without one the id
is auto-assigned. Auto-assigned ids are integers (Raft indices), so an
integer `proposed_id` can collide with them and draw a spurious
`id_in_use`; propose a tuple or any other non-integer term.

A holder that revokes an auto-renewed lease receives one final
`{portunus, lease_lost, LeaseId}` when the renewer discovers the
revocation; ignore it. For a renewer the holder can stop explicitly,
use `keep_alive/3`, which returns the renewer's pid.
""".
-spec grant_lease(name(), ttl(), grant_opts()) ->
    {ok, lease_id()} | {error, lease_error()}.
%% `auto_renew` requires the renewer's TTL floor (see `portunus_keepalive`);
%% failing the guard here rejects the call before a lease is granted.
grant_lease(Name, TtlMs, #{auto_renew := true} = Opts)
  when is_integer(TtlMs), TtlMs >= ?MIN_RENEWABLE_TTL_MS ->
    grant_lease1(Name, TtlMs, Opts);
grant_lease(Name, TtlMs, Opts)
  when is_integer(TtlMs), TtlMs > 0, is_map(Opts),
       not is_map_key(auto_renew, Opts) orelse
       map_get(auto_renew, Opts) =:= false ->
    grant_lease1(Name, TtlMs, Opts).

grant_lease1(Name, TtlMs, Opts) ->
    ProposedId = maps:get(proposed_id, Opts, undefined),
    case cmd(Name, {grant_lease, ProposedId, TtlMs, self(), self()}) of
        {ok, LeaseId} = Ok ->
            case maps:get(auto_renew, Opts, false) of
                true ->
                    {ok, _Renewer} = portunus_keepalive:start_link(Name, LeaseId, TtlMs),
                    Ok;
                false ->
                    Ok
            end;
        {error, _} = Err ->
            Err
    end.

-doc "Renew one or more leases in a single command (batch renew).".
-spec renew_leases(name(), [lease_id()]) ->
    [{lease_id(), ok | {error, lease_expired | no_quorum}}].
renew_leases(Name, LeaseIds) ->
    renew_leases(Name, LeaseIds, ?CMD_TIMEOUT).

-doc """
Renew with an explicit command timeout. A renewer bounds this to a fraction
of the TTL so several attempts fit within one TTL across a leader change.
""".
-spec renew_leases(name(), [lease_id()], timeout()) ->
    [{lease_id(), ok | {error, lease_expired | no_quorum}}].
renew_leases(Name, LeaseIds, Timeout) when is_list(LeaseIds) ->
    case cmd(Name, {renew, LeaseIds}, Timeout) of
        {error, no_quorum} -> [{L, {error, no_quorum}} || L <- LeaseIds];
        Results -> Results
    end.

-doc "Revoke a lease and release every lock held under it.".
-spec revoke_lease(name(), lease_id()) -> ok_or_error(no_quorum).
revoke_lease(Name, LeaseId) ->
    cmd(Name, {revoke_lease, LeaseId}).

-doc """
Start a renewer that keeps `LeaseId` alive and is linked to the caller, so
it stops when the caller does. Pass the TTL the lease was granted with; a
renewer cannot sustain a TTL below 2000 ms (see `portunus_keepalive`).
`grant_lease/3` with `#{auto_renew => true}` does the same in one step.
""".
-spec keep_alive(name(), lease_id(), ttl()) -> {ok, pid()}.
keep_alive(Name, LeaseId, TtlMs)
  when is_integer(TtlMs), TtlMs >= ?MIN_RENEWABLE_TTL_MS ->
    portunus_keepalive:start_link(Name, LeaseId, TtlMs).

%%----------------------------------------------------------------------
%% Locks
%%----------------------------------------------------------------------

-doc """
Acquire `LockKey` under `LeaseId`, trying once. Returns `{ok, Token}`, the
fencing token, or `{error, {held_by, Owner}}` if the key is held. It never
queues: use `acquire_or_join_succession_queue/4` to wait for the key.
""".
-spec acquire(name(), lock_key(), lease_id(), owner()) ->
    {ok, token()} | {error, acquire_error()}.
acquire(Name, LockKey, LeaseId, Owner) ->
    acquire(Name, LockKey, LeaseId, Owner, undefined).

-doc """
Like `acquire/4`, attaching `Context` to the grant (returned by `owner/2`).
Context is set-once and should be a pointer, not a payload: it lives in every
replica's state, every snapshot and every `owner/2` reply.
""".
-spec acquire(name(), lock_key(), lease_id(), owner(), term()) ->
    {ok, token()} | {error, acquire_error()}.
acquire(Name, LockKey, LeaseId, Owner, Context) ->
    cmd(Name, {acquire, LeaseId, LockKey, Owner, Context, nowait}).

-doc """
Acquire `LockKey`, or join its succession queue if it is held. Returns
`{ok, Token}` if the key was free, otherwise `{queued, Depth}` (the number of
waiters on the key, not a place in line), and later sends
`{portunus, granted, Key, Token, LeaseId}` to the lease holder once it is
promoted.
""".
-spec acquire_or_join_succession_queue(name(), lock_key(), lease_id(), owner()) ->
    {ok, token()} | {queued, pos_integer()} | {error, acquire_error()}.
acquire_or_join_succession_queue(Name, LockKey, LeaseId, Owner) ->
    acquire_or_join_succession_queue(Name, LockKey, LeaseId, Owner, #{}).

-doc """
Like `acquire_or_join_succession_queue/4`, with `#{affinity => Spec}` to bias
which contender is promoted first (see `portunus_affinity`; the default is
FIFO in arrival order) and `#{context => Term}` to attach a context to the
grant on promotion, as `acquire/5` does for an immediate grant.
""".
-spec acquire_or_join_succession_queue(name(), lock_key(), lease_id(), owner(),
                                       succession_opts()) ->
    {ok, token()} | {queued, pos_integer()} | {error, acquire_error()}.
acquire_or_join_succession_queue(Name, LockKey, LeaseId, Owner, Opts) ->
    Score = succession_score(Name, LockKey, Opts),
    Context = maps:get(context, Opts, undefined),
    cmd(Name, {acquire, LeaseId, LockKey, Owner, Context, wait, Score}).

%% The succession score: an affinity spec resolved over the current members,
%% defaulting to 0 (FIFO). A faulty strategy degrades to FIFO; affinity is a
%% hint, not a correctness requirement. The raw integer `score` key is an
%% internal escape hatch, deliberately absent from `succession_opts()`.
succession_score(_Name, _Key, #{score := Score}) when is_integer(Score) ->
    Score;
succession_score(_Name, _Key, #{affinity := default}) ->
    0;
succession_score(Name, Key, #{affinity := Spec}) ->
    %% A local membership view: affinity is a hint, and a leader query here
    %% would block the caller for the command timeout during an election.
    Members = case ra:members({local, {Name, node()}}, 1000) of
                  {ok, ServerIds, _} -> [N || {_, N} <- ServerIds];
                  _ -> [node()]
              end,
    try portunus_affinity:score(Spec, Key, Members)
    catch Class:Reason ->
        logger:warning("portunus affinity ~p failed (~p:~p) for key ~p; "
                       "using FIFO succession", [Spec, Class, Reason, Key]),
        0
    end;
succession_score(_Name, _Key, _Opts) ->
    0.

-doc """
Acquire `LockKey`, waiting up to `TimeoutMs` for the current owner to
release, be revoked, or expire.

Must be called by the lease holder process, which receives the
`{portunus, granted, ...}` message.

On timeout the bid is withdrawn, so the caller is never granted a key it
gave up on. A grant that lands at the same instant is detected and the
key is returned.

`{error, no_quorum}` on the timeout path means the withdrawal is
unconfirmed: the bid may still be queued, so retry
`leave_succession_queue/3` or revoke the lease.
""".
-spec acquire_with_timeout(name(), lock_key(), lease_id(), owner(),
                    non_neg_integer()) ->
    {ok, token()} | {error, acquisition_timeout_error()}.
acquire_with_timeout(Name, LockKey, LeaseId, Owner, TimeoutMs) ->
    acquire_with_timeout(Name, LockKey, LeaseId, Owner, TimeoutMs, #{}).

-doc """
Like `acquire_with_timeout/5`, with the options of
`acquire_or_join_succession_queue/5`.
""".
-spec acquire_with_timeout(name(), lock_key(), lease_id(), owner(),
                    non_neg_integer(), succession_opts()) ->
    {ok, token()} | {error, acquisition_timeout_error()}.
acquire_with_timeout(Name, LockKey, LeaseId, Owner, TimeoutMs, Opts)
  when is_integer(TimeoutMs), TimeoutMs >= 0 ->
    case acquire_or_join_succession_queue(Name, LockKey, LeaseId, Owner,
                                          Opts) of
        {ok, Token} ->
            {ok, Token};
        {queued, _Depth} ->
            receive
                {portunus, granted, LockKey, Token, LeaseId} ->
                    {ok, Token}
            after TimeoutMs ->
                case leave_succession_queue(Name, LockKey, LeaseId) of
                    ok ->
                        %% A committed withdrawal proves the bid was never
                        %% promoted, so no grant message is in flight.
                        {error, timeout};
                    {error, not_queued} ->
                        settle_timed_out_bid(Name, LockKey, LeaseId);
                    {error, no_quorum} = Err ->
                        Err
                end
            end;
        {error, _} = Err ->
            Err
    end.

%% The bid was gone by the time the withdrawal committed: either the grant
%% won the race or the lease died and the queue dropped it. The grant's
%% message may still be in flight, so only a linearizable read can tell;
%% a late-arriving grant message for an owned key is then harmless.
-spec settle_timed_out_bid(name(), lock_key(), lease_id()) ->
    {ok, token()} | {error, timeout | no_quorum}.
settle_timed_out_bid(Name, LockKey, LeaseId) ->
    case owner(Name, LockKey) of
        {ok, #{lease := LeaseId, token := Token}} ->
            {ok, Token};
        {ok, #{}} ->
            {error, timeout};
        {error, not_held} ->
            {error, timeout};
        {error, no_quorum} = Err ->
            Err
    end.

-doc """
Withdraw the lease's succession bid on `LockKey`: the opposite of joining
the queue with `acquire_or_join_succession_queue/4,5`, for a contender that
no longer wants the key. The current holder is untouched, and the lease's
other keys and bids are unaffected. A lease with no bid on the key returns
`{error, not_queued}`; a holder releases with `release/3` instead.
""".
-spec leave_succession_queue(name(), lock_key(), lease_id()) ->
    ok_or_error(leave_queue_error()).
leave_succession_queue(Name, LockKey, LeaseId) ->
    cmd(Name, {leave_queue, LockKey, LeaseId}).

-doc "Release a held lock. Token-fenced: a stale token cannot release a re-granted lock.".
-spec release(name(), lock_key(), token()) -> ok_or_error(release_error()).
release(Name, LockKey, Token) ->
    cmd(Name, {release, LockKey, Token}).

-doc """
Hand a held lock to a chosen contender in one machine transition, keeping the
key held by exactly one owner throughout. `TargetOwner` names the contender by
its `owner` term; for the node-based batteries that owner is the node, so
`portunus_election:transfer_to/2` and `portunus_registry:transfer/3` take a
`node()`.

Token-fenced like `release/3`: only the current holder can transfer,
and a stale token or a free key returns `{error, not_owner}`. A `TargetOwner`
equal to the holder's own owner returns `ok`. If no live contender carries
`TargetOwner` the holder keeps the key and the reply is
`{error, {no_contender, TargetOwner}}`, never a release to some other node.
""".
-spec transfer(name(), lock_key(), token(), owner()) ->
    ok_or_error(transfer_error()).
transfer(Name, LockKey, Token, TargetOwner) ->
    cmd(Name, {transfer, LockKey, Token, TargetOwner}).

-doc """
The live contenders queued for `LockKey`, as their `owner` terms, read from the
local replica. Non-blocking and approximate under replica lag, which suits the
advisory transfer pre-check. Returns `{error, no_quorum}` if the local replica
cannot answer.
""".
-spec contenders(name(), lock_key()) -> {ok, [owner()]} | {error, no_quorum}.
contenders(Name, LockKey) ->
    case ra:local_query({Name, node()},
                        {portunus_machine, query_contenders, [LockKey]},
                        ?CMD_TIMEOUT) of
        {ok, {_IdxTerm, Owners}, _Leader} -> {ok, Owners};
        _ -> {error, no_quorum}
    end.

-doc "Query the owner of a lock (linearizable).".
-spec owner(name(), lock_key()) ->
    {ok, owner_info()} | {error, not_held | no_quorum}.
owner(Name, LockKey) ->
    query(Name, {portunus_machine, query_owner, [LockKey]}).

%%----------------------------------------------------------------------
%% Watch
%%----------------------------------------------------------------------

-doc """
Watch a key; the caller receives `{portunus, watch, Ref, Event}` messages,
where `Event` is `{acquired, Owner}` or `released`. One watch per process per
key: re-watching returns a new ref and supersedes the old. Pass `Ref` to
`unwatch/2`.
""".
-spec watch(name(), lock_key()) -> {ok, watch_ref()} | {error, no_quorum}.
watch(Name, LockKey) ->
    cmd(Name, {watch, LockKey, self()}).

-doc "Stop the watch registered under `Ref`.".
-spec unwatch(name(), watch_ref()) -> ok_or_error(no_quorum).
unwatch(Name, Ref) ->
    cmd(Name, {unwatch, Ref}).

%%----------------------------------------------------------------------
%% Health and introspection
%%----------------------------------------------------------------------

-doc "Whether the cluster currently has an online majority (a quorum-confirming read).".
-spec has_quorum(name()) -> boolean().
has_quorum(Name) ->
    case ra:consistent_query(leader_or_local(Name),
                             {portunus_machine, query_status, []},
                             ?CMD_TIMEOUT) of
        {ok, M, _} when is_map(M) -> true;
        _ -> false
    end.

-doc """
Whether this node is a member of cluster `Name`. Answered from the local
replica without contacting the leader, so it holds during an election or a
quorum loss, and is false when no local replica is running. Useful for a host
that bootstraps the cluster and needs to know when this node has joined.

A local view alone cannot decide this: Ra always includes a server in its
own view, joined or not. A genuine member also holds at least the log entry
that added it, so an empty log means the join never committed.
""".
-spec is_member(name()) -> boolean().
is_member(Name) ->
    ServerId = {Name, node()},
    case ra:members({local, ServerId}, ?CMD_TIMEOUT) of
        {ok, Members, _Leader} ->
            lists:member(ServerId, Members) andalso has_log_entries(ServerId);
        _ ->
            false
    end.

has_log_entries(ServerId) ->
    KM = try ra:key_metrics(ServerId) catch _:_ -> #{} end,
    maps:get(last_index, KM, 0) > 0.

-doc "A snapshot of cluster health and the machine-derived counts.".
-spec status(name()) -> status().
status(Name) ->
    %% A successful consistent_query is itself the quorum signal, so derive
    %% quorum from it rather than issuing a second identical query.
    {Base, Quorum} = case query(Name, {portunus_machine, query_status, []}) of
                         M when is_map(M) -> {M, true};
                         _ -> {#{}, false}
                     end,
    {Members, Leader} = case members(Name) of
                            {ok, Ms, L} -> {Ms, L};
                            _ -> {[], undefined}
                        end,
    Base#{leader => Leader,
          members => Members,
          quorum => Quorum}.

%%----------------------------------------------------------------------
%% Conveniences
%%----------------------------------------------------------------------

-doc """
One-shot exclusive lock with auto-renewal: grant a lease, acquire the
key, and start a holder-linked renewer. Returns a handle for `unlock/1`.
""".
-spec lock(name(), lock_key(), ttl()) ->
    {ok, handle()} | {error, acquire_error() | lease_error()}.
lock(Name, LockKey, TtlMs)
  when is_integer(TtlMs), TtlMs >= ?MIN_RENEWABLE_TTL_MS ->
    case grant_lease(Name, TtlMs) of
        {ok, LeaseId} ->
            {ok, Renewer} = portunus_keepalive:start_link(Name, LeaseId, TtlMs),
            case acquire(Name, LockKey, LeaseId, self()) of
                {ok, Token} ->
                    {ok, #{name => Name, key => LockKey, lease => LeaseId,
                           token => Token, renewer => Renewer}};
                Err ->
                    %% Acquire failed (e.g. held_by): do not leak the lease
                    %% or the renewer.
                    _ = portunus_keepalive:stop(Renewer),
                    _ = revoke_lease(Name, LeaseId),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

-doc "Release a lock taken with `lock/3`: stop the renewer and revoke the lease.".
-spec unlock(handle()) -> ok.
unlock(#{name := Name, lease := LeaseId, renewer := Renewer}) ->
    _ = portunus_keepalive:stop(Renewer),
    _ = revoke_lease(Name, LeaseId),
    ok.

-doc "Run `Fun` while holding `LockKey`, releasing on return or exception.".
-spec with_lock(name(), lock_key(), ttl(), fun(() -> Result)) ->
    Result | {error, acquire_error() | lease_error()}.
with_lock(Name, LockKey, TtlMs, Fun)
  when is_integer(TtlMs), TtlMs >= ?MIN_RENEWABLE_TTL_MS, is_function(Fun, 0) ->
    case lock(Name, LockKey, TtlMs) of
        {ok, Handle} ->
            try Fun()
            after
                unlock(Handle)
            end;
        {error, _} = Err ->
            Err
    end.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

leader_or_local(Name) ->
    case ra_leaderboard:lookup_leader(Name) of
        undefined -> {Name, node()};
        Leader -> Leader
    end.

%% Ra registers the local server under the cluster `Name`. If another process
%% already holds that registered name, Ra cannot start the server and the
%% failure surfaces as `cluster_not_formed` with nothing pointing at the cause.
%% Catch the local collision early. A name held by this cluster's own Ra server,
%% an idempotent re-call, is not a collision: `ra_directory` knows that name.
-spec ensure_name_unregistered(system(), name()) ->
    ok | {error, {name_registered, pid()}}.
ensure_name_unregistered(System, Name) ->
    case whereis(Name) of
        undefined ->
            ok;
        Pid ->
            case is_ra_server(System, Name) of
                true -> ok;
                false -> {error, {name_registered, Pid}}
            end
    end.

%% True only if `Name` is registered as a Ra server in `System`, identified by a
%% uid in `ra_directory`. portunus only registers Ra servers under a cluster
%% name, so a hit is this cluster's own server and a miss is a foreign process. A
%% lookup that raises (the system is not started) counts as a miss, so a foreign
%% registration still gets the clear error.
-spec is_ra_server(system(), name()) -> boolean().
is_ra_server(System, Name) ->
    try ra_directory:uid_of(System, Name) of
        UId when is_binary(UId) -> true;
        _ -> false
    catch
        _:_ -> false
    end.

%% Ra's `process_command/3` and `consistent_query/3` specs declare only
%% `{ok, _, _}`, `{timeout, _}` and `{error, _}`, but a server mid-election with
%% no leader can answer a bare `ok`, so the catch-all clauses in `cmd/3` and
%% `query/2` are reachable in practice. Dialyzer trusts the understated upstream
%% spec and reads them as dead, a false positive silenced here.
-dialyzer({no_match, [{cmd, 3}, {query, 2}]}).

cmd(Name, Command) ->
    cmd(Name, Command, ?CMD_TIMEOUT).

cmd(Name, Command, Timeout) ->
    case ra:process_command(leader_or_local(Name), Command, Timeout) of
        {ok, Reply, _Leader} -> Reply;
        {timeout, _} -> no_online_quorum(Name, timeout);
        {error, Reason} -> no_online_quorum(Name, Reason);
        %% `ra:process_command/3` can answer with a bare `ok` for a command it
        %% cannot turn into a committed reply, e.g. when the target server is a
        %% follower mid-election with no leader to forward to. There is no
        %% confirmation the command applied, so treat it (and any other
        %% unexpected term) as a transient quorum failure the caller retries,
        %% rather than crashing on a non-exhaustive `case`.
        Other -> no_online_quorum(Name, {unexpected_reply, Other})
    end.

%% A command could not be committed. The reason (timeout, the local server
%% down, a lost majority) is logged for diagnosis while callers still see a
%% single stable `no_quorum`, and the rejection is counted.
no_online_quorum(Name, Reason) ->
    ok = portunus_counters:incr(Name, failures_due_to_lack_of_online_quorum_total),
    logger:debug("portunus command on ~p rejected: ~p", [Name, Reason]),
    {error, no_quorum}.

query(Name, QueryMFA) ->
    case ra:consistent_query(leader_or_local(Name), QueryMFA, ?CMD_TIMEOUT) of
        {ok, Result, _Leader} -> Result;
        {timeout, _} -> no_online_quorum(Name, timeout);
        {error, Reason} -> no_online_quorum(Name, Reason);
        %% As in `cmd/3`: a bare `ok` or other unexpected term from a server in
        %% a transient state is a quorum failure, not a result.
        Other -> no_online_quorum(Name, {unexpected_reply, Other})
    end.

resolve_membership(Nodes) when is_list(Nodes) ->
    Nodes;
resolve_membership({M, F}) ->
    M:F();
resolve_membership(local) ->
    [node()].

default_data_dir(System) ->
    Base = case application:get_env(ra, data_dir) of
               {ok, D} -> D;
               undefined ->
                   Tmp = filename:join(["/tmp", "portunus"]),
                   logger:warning(
                     "portunus: no data_dir configured and ra's data_dir is "
                     "unset, falling back to ~ts. This is volatile storage: a "
                     "reboot can wipe the Raft log, and fencing tokens then "
                     "restart low. Configure a persistent data_dir for "
                     "anything but throwaway use.", [Tmp]),
                   Tmp
           end,
    filename:join(Base, atom_to_list(System)).
