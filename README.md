# Portunus

[![Test](https://github.com/rabbitmq/portunus/actions/workflows/tests.yml/badge.svg)](https://github.com/rabbitmq/portunus/actions/workflows/tests.yml)

`portunus` is a small, generic lock server for the Erlang ecosystem, built on
[Ra](https://github.com/rabbitmq/ra), RabbitMQ's Raft implementation.
It provides cluster-wide mutual exclusion, TTL leases with renewal,
leader election, and a succession queue with pluggable placement
affinity. It is a CP (consistency over availability) service in the
style of etcd and ZooKeeper, but embedded as a library rather than
run as an external service.

Portunus implements a Ra state machine. It does not depend on or use
Khepri. It is named after the Roman god of keys, doors, and gates.

## Project Maturity

This project is young: breaking changes are likely.

## Why

The standard library's `global` module has limits: no clear partition
handling semantics, no fencing tokens, and its maturity rules out major
changes or rapid iteration. Hand-rolling a distributed locking library
comparable in core features to ZooKeeper, `etcd`, or Consul is hard and
error-prone.

At the same time, the field of Raft-based distributed locking services
is mature and well understood, and Team RabbitMQ already maintains a
Raft implementation: Ra.

## Core Ideas

 * Safety and liveness are separated. At most one owner per key, and
   monotonically increasing fencing tokens, are guaranteed by Raft,
   independent of clocks. Lease expiry (TTL) is liveness: approximate
   and clock-dependent. A client must treat a renewal failure as
   "lease possibly lost" and stop acting
 * Fencing tokens. Every grant returns a token (the Raft index). The
   guarded resource (usually another component) records the highest
   token it has seen and rejects stale ones. This is what makes a lock
   safe across a paused or partitioned holder
 * Leases. A lock is held under a TTL lease and stays held for as long
   as the lease is renewed. Many exclusive keys can share one lease (a
   session)
 * Succession and affinity. A held key keeps a queue of succession
   candidates; release, revocation, and expiry promote the best-ranked
   one. Ranking is FIFO by default; an affinity strategy (`pinned`,
   `preferred`, `hash`, `metric`, `random`, or a custom
   `portunus_affinity` module) biases which node wins. Affinity is a
   hint, never a correctness requirement

## Requirements

Portunus requires Erlang/OTP 27 and should work equally well on
Erlang/OTP 28 and 29, including mixed-version clusters during upgrades.
It targets `ra` `3.1.8` or later.

## Dependency

There is no Hex release yet; depend on the git repository. With
`rebar3`:

```erlang
{deps, [{portunus, {git, "https://github.com/rabbitmq/portunus", {branch, "main"}}}]}.
```

With `erlang.mk`:

```makefile
DEPS = portunus
dep_portunus = git https://github.com/rabbitmq/portunus main
```

## Getting Started

Portunus requires modern Erlang/OTP (see Requirements above). Every
node starts a Ra system, which keeps its Raft state under the given
directory, and the nodes then form a named cluster:

```erlang
%% on every node, once
ok = portunus:start_system(portunus, "/var/lib/portunus"),

%% on the first node: form the cluster
{ok, _Started, _Failed} = portunus:start_cluster(portunus, my_locks, [node()]),

%% on the other nodes: join the existing members (idempotent)
ok = portunus:join_or_form(portunus, my_locks, ['first@host']).
```

For nodes that boot independently and retry until the cluster is up,
call `join_or_form/3` with the same full member list on every node from
a retry loop: every node picks the same seed, so two nodes can never
each form their own cluster.

## Locks and Leases

The core API grants a lease, then acquires keys under it. Every grant
returns a fencing token:

```erlang
{ok, Lease} = portunus:grant_lease(my_locks, 30000),
{ok, Token} = portunus:acquire(my_locks, {resource, 42}, Lease, self()),
%% carry Token into every external write made under this lock
ok = portunus:release(my_locks, {resource, 42}, Token).
```

A lease granted this way must be renewed by the caller
(`renew_leases/2`), or it expires after its TTL and its locks are
released. With `auto_renew`, a renewer process linked to the caller
keeps the lease alive for as long as the caller lives:

```erlang
{ok, Lease} = portunus:grant_lease(my_locks, 30000, #{auto_renew => true}).
```

The one-shot conveniences bundle the lease, its renewal, and the
acquire into a single auto-renewing handle:

```erlang
{ok, Handle} = portunus:lock(my_locks, {resource, 42}, 30000),
ok = portunus:unlock(Handle),

%% or scoped to a function, released on return or exception
Result = portunus:with_lock(my_locks, {resource, 42}, 30000,
                            fun() -> do_exclusive_work() end).
```

## Waiting for a Held Key

`acquire/4` never queues: if the key is held, it returns
`{error, {held_by, Owner}}`. To wait for the key instead, join its
succession queue; the caller is promoted when the current owner
releases, is revoked, or expires:

```erlang
case portunus:acquire_or_join_succession_queue(my_locks, Key, Lease, self()) of
    {ok, Token} ->
        %% the key was free: we own it now
        owned(Token);
    {queued, _Depth} ->
        %% promoted later: the lease holder receives this message
        receive
            {portunus, granted, Key, Token, Lease} -> owned(Token)
        end
end.
```

## Watches

`watch/2` subscribes the calling process to a key's acquire and release
events, and `owner/2` reads the current owner directly:

```erlang
{ok, Ref} = portunus:watch(my_locks, Key),
receive
    {portunus, watch, Ref, {acquired, Owner}} -> track(Owner);
    {portunus, watch, Ref, released} -> untrack()
end,
ok = portunus:unwatch(my_locks, Ref).
```

Watches are best-effort notifications; a decision that must be safe
should be fenced with the token, not made from a watch event.

## Sessions

A session is one lease with many exclusive keys claimed under it:
renewal cost stays per session, not per key, and the session process is
the lease holder, so its death releases all of its keys at once. On
lease loss the session exits with reason `lease_lost`, taking a linked,
non-trapping opener with it (the fail-stop default, since its claims
are gone):

```erlang
{ok, Session} = portunus_session:open(my_locks, #{ttl_ms => 30000}),
{ok, _Token1} = portunus_session:claim(Session, {vhost, <<"a">>}),
{ok, _Token2} = portunus_session:claim(Session, {vhost, <<"b">>}),
ok = portunus_session:release(Session, {vhost, <<"a">>}),
ok = portunus_session:close(Session).
```

## Health and Introspection

`status/1` returns the leader, members, quorum, and machine-derived
counts; `has_quorum/1` is a quorum-confirming read; `is_member/1`
answers from the local replica, so it holds during an election and is
the right check for a bootstrap retry loop. Operational metrics are
exposed as [seshat](https://github.com/rabbitmq/seshat) counters and
gauges per node (see `portunus_counters`).

The sections below are the "batteries": higher-level components built
on top of the lock and lease core.

## Leader Election

`portunus_election` keeps exactly one instance of a component running
in the cluster. A participant runs on every node; the elected one runs
`elected/1` (its context carries the fencing token) and `stepped_down/1`
when the lease is lost, at which point ownership moves to another node:

```erlang
-module(my_scheduler).
-behaviour(portunus_election).
-export([elected/1, stepped_down/1]).

elected(#{token := Token}) ->
    {ok, Pid} = my_scheduler_worker:start_link(Token),
    {ok, Pid}.

stepped_down(Pid) ->
    my_scheduler_worker:stop(Pid).
```

```erlang
{ok, Pid} = portunus_election:start_link(my_locks, scheduler_key,
                                         my_scheduler, [],
                                         #{ttl_ms => 30000}).
```

The owner can hand the key to a chosen node with `transfer_to/2`: it
stops the local work, performs the token-fenced transfer, and
re-contends as a standby. A target that is not a ready contender
(`contenders/2` lists them) is refused and the owner keeps running:

```erlang
ok = portunus_election:transfer_to(Pid, 'b@host').
```

## A Managed Set of Keys

`portunus_service` runs one election per key from a fixed key set, with
`start/3` and `stop/2` invoked per key. Every node runs the same
service; each key ends up with exactly one owner in the cluster:

```erlang
-module(my_partition_owner).
-behaviour(portunus_service).
-export([keys/1, start/3, stop/2]).

keys(NumPartitions) ->
    lists:seq(1, NumPartitions).

start(Partition, Token, _Args) ->
    my_partition_worker:start_link(Partition, Token).

stop(_Partition, Pid) ->
    my_partition_worker:stop(Pid).
```

```erlang
{ok, _Pid} = portunus_service:start_link(my_locks, my_partition_owner, 8,
                                         #{ttl_ms => 30000}).
```

## Declarative Supervision

`portunus_supervisor` looks like an Erlang/OTP `supervisor`, except
only one instance of each child spec returned by `init/1` can exist in
the cluster at any given time. The elected owner runs it under a local
supervisor, and Portunus drives the cross-node ownership transfer:

```erlang
-module(example_supervisor_mod).
-behaviour(portunus_supervisor).
-export([start_link/0, init/1]).

start_link() ->
    portunus_supervisor:start_link(my_locks, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    {ok, {SupFlags,
          [#{id => stats_collector,
             start => {my_stats_collector, start_link, []},
             restart => permanent}]}}.
```

Child specs may carry the extended `{permanent, Delay}` and
`{transient, Delay}` restart types (as `supervisor2` accepts), rewritten
by `portunus_delayed_restart` into a rate-limited standard spec.

## The Dynamic Registry

`portunus_registry` is the same idea with children added and removed at
runtime instead of being returned by `init/1`. This is the
`mirrored_supervisor` replacement for runtime-managed children such as
dynamic shovels and federation links: every node registers the same
child specs (driven by replicated parameter or policy events), Portunus
runs one election per key, and the elected node runs the child under a
local supervisor:

```erlang
{ok, Reg} = portunus_registry:start_link(my_locks, #{}),
ok = portunus_registry:add(Reg, #{id => shovel_a,
                                  start => {my_shovel, start_link, [a]},
                                  restart => permanent}).
```

`remove/2` stops contending on this node; removing a key on its current
owner moves the child to another node, and the key is gone cluster-wide
once every node that added it removes it. `owned_keys/1` and
`which_children/1` report what this node currently runs.

`transfer/3` hands a key this node owns to a named node in one
token-fenced machine transition:

```erlang
ok = portunus_registry:transfer(Reg, shovel_a, 'b@host').
```

A target that is not a ready contender is refused with
`{error, {no_contender, Node}}` and the current owner keeps running, so
a rebalancer can retry without ever leaving the key ownerless or run in
two places. `portunus_supervisor:transfer/3` and `portunus:transfer/4`
(by opaque owner term) are the same operation at the other layers.

## Placement Affinity

Elections, services, and registries accept `#{affinity => Spec}` to
bias which node wins a key. A spec names a built-in strategy with its
argument, a custom `portunus_affinity` behaviour module, or a scoring
fun:

```erlang
%% prefer these nodes, earliest first, over any others
#{affinity => {preferred, ['a@host', 'b@host']}}

%% spread keys evenly across members (rendezvous hashing)
#{affinity => {hash, undefined}}

%% the least-loaded node wins, by a locally read metric
#{affinity => {metric, fun() -> spare_capacity() end}}
```

## Node Restarts and Membership Changes

Membership is managed with `add_member/2`, `remove_member/2`, and
`members/1`:

```erlang
ok = portunus:add_member(my_locks, 'new@host'),
ok = portunus:remove_member(my_locks, 'departed@host'),
{ok, Members, Leader} = portunus:members(my_locks).
```

A few recommendations for applications that manage the cluster
lifecycle themselves.

Run `portunus` on every node of the host system (for example, on every
node of a RabbitMQ cluster). A node without a running member can never
own a key, and no cluster can form if such a node is chosen as the seed.

Compute the member list the same way on every node, from configuration
or from the host system's own membership, never from which nodes are
reachable: nodes with different views of "who is up" can each form
their own cluster on first boot.

On a restart, let `join_or_form/3` recover the node's on-disk state.
`reset_and_join_cluster/3` wipes it and is only for a node that joined
the wrong cluster.

Only remove a member that is gone for good. A disconnected node does
not block the surviving majority, and lease expiry already moves its
keys, so a disconnect needs no membership change.

Re-check membership periodically rather than from events alone: a
periodic pass retries a missed join, removes members that left quietly,
and heals a first-boot split.

## Layout

 * `portunus_machine`: the Ra state machine (leases, locks, fencing
   tokens, score-ordered succession, tick-based expiry, periodic log
   snapshots)
 * `portunus`: the public client API
 * `portunus_session`: one process's lease with many keys claimed under
   it
 * `portunus_election`: keeps one elected instance of a component per
   key, cluster-wide
 * `portunus_service`: a managed set of keys driven by a callback module
 * `portunus_supervisor`: a declarative, `supervisor`-shaped layer built
   on top of the registry
 * `portunus_registry`: a dynamic cluster-wide supervisor with children
   added and removed at runtime
 * `portunus_affinity`: placement strategies for succession, built-in
   and custom
 * `portunus_counters`: seshat metrics

## Building and Testing

Both `rebar3` (primary) and `erlang.mk` are supported, like Ra.

```
rebar3 compile
rebar3 ct
rebar3 dialyzer
rebar3 ex_doc      # API documentation under doc/
gmake              # the erlang.mk equivalent
```

## License

Dual-licensed under the Apache License 2.0 and the Mozilla Public
License 2.0, the same as Ra. See `LICENSE`, `LICENSE-APACHE2`, and
`LICENSE-MPL-RabbitMQ`.

© Team RabbitMQ &lt;teamrabbitmq@gmail.com&gt;.
