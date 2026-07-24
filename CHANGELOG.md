## Changes in `0.11.0` (in development)

### Breaking or Potentially Breaking Changes

 * A leader change can extend every outstanding lease by up to one full TTL:
   the new leader does not know the old leader's renewal bookkeeping, so it
   errs toward late expiry (the same trade `etcd` makes). At-most-one-owner
   is unaffected; only reclaim latency after a holder's ungraceful death
   grows across a leader change

 * `owner_info()` (returned by `portunus:owner/2`) no longer carries
   `remaining_ms`. Its source of truth was the replicated deadline, which no
   longer exists; a decaying lower bound would mislead

 * The `tick_interval_ms` application setting was removed. Lease expiry is now noticed
   within one Ra `tick_timeout` (1 s by default) of the lease's deadline

### Enhancements

 * Lease renewal moved off the Raft log. Renewals now travel over
   `ra:consistent_aux/3`: the leader confirms a live quorum with a
   heartbeat round and moves the lease's deadline in its in-memory (aux)
   state, so steady-state renewal appends nothing to the log and triggers no
   `fsync(2)` on any member. The periodic `{timeout, expire}` sweep command
   is gone too; the leader's aux tick proposes an `{expire_leases, ...}`
   command only when a lease actually expired, so a healthy cluster holding
   leases has a zero background write rate


## Changes in `0.10.0` (Jul 22, 2026)

### Enhancements

 * `portunus_batch_keepalive`: a node-wide lease renewer that renews all leases
   of one `portunus` instance (Ra cluster) with the same TTL in one Ra command
   per `TTL/3` round, instead of one command per lease.

   Every renewal command is a Raft log append which is `fsync(2)`ed by
   every member, so with hundreds or thousands of leases per node the per-lease
   renewers produce enough `fsync(2)`s to degrade I/O throughput.

   Per-lease semantics match `portunus_keepalive`: an expired lease notifies its owner
   process with `{portunus, lease_lost, LeaseId}`, transient failures are
   retried, and a dead owner's lease is dropped

 * `portunus_election` renews its lease through `portunus_batch_keepalive`, so
   every consumer built on elections (`portunus_registry`, `portunus_service`,
   `portunus_supervisor`) benefits from its reduced Raft log commit rate.
   
   
   Note that `portunus:lock/3`, `grant_lease/3` with `auto_renew`, `keep_alive/3` options, plus
   `portunus_session` keep using the original per-lease renewer


## Changes in `0.9.0` (Jul 17, 2026)

### Enhancements

 * Fencing tokens (and auto-assigned lease IDs, watch references) now include a
   per-Ra cluster incarnation epoch, so a re-formed cluster produces fencing tokens
   that compare greater than the "previous" version of the cluster.
   This is similar to Raft leader terms but serves a somewhat different purpose
   higher up in the abstraction layer

 * `portunus:use_system/1` is a helper for running `portunus` on a Ra system the embedding
   application owns, such as RabbitMQ's `coordination` system, as a tenant.
   When `portunus` is initialized this way, it does not start, stop or
   reconfigure such an externally owned system, does not need `server_recovery_strategy` to be
   enabled on it, and flushes its Ra server registrations DETS table immediately
   without waiting for the delayed DETS write in `ra`

 * `portunus_registry:sync/2`: reconcile this node's registrations to a given set of child
   specs in one call, adding the missing, removing the surplus and leaving the rest
   untouched. The building block for consumers that mirror children from an external source
   of truth, where a hand-rolled add-only reconcile resurrects a child that was deleted
   while this node was offline

 * `portunus:is_seed_cluster_member/2`: a predicate that returns `true` when this node is a
   member of the cluster the effective seed belongs to, reducing work for the embedding
   application's cluster formation loop

 * `portunus:orphaned_replicas/1` and `portunus:delete_orphaned_replica/2`: enumerate
   and remove this node's replica directories that lost their registration (think the
   evict-then-rejoin cluster formation path), scoped to `portunus` machines to avoid
   messing with other `ra` system tenants there might be

### Bug Fixes

 * Multiple changes around Ra cluster formation, node restart, intentional (or not) `ra`
   system reuse

 * `join_cluster/3` against a cluster that already lists this node as a member returned
   `ok` without starting a local server. It now restarts the server when the local Ra
   directory knows it, and when it does not (a lost registration or disk), removes the
   remembered member and rejoins as a new one

 * A registration pointing at a deleted replica directory (a kill inside
   `ra:force_delete_server/2`) wedged the hosted bootstrap: every pass retried a restart
   that could never succeed. The rejoin decision now requires a readable replica `config`
   file and routes such a node to the evict-then-rejoin path

 * `start_cluster/3` flushed only the local registration; it now flushes on every node a
   server started on

 * `start_system/2` silently overwrote the stored config of a running system whose
   process names were not derived from the system name; it now refuses with
   `ra_system_mismatch`


## Changes in `0.8.0` (Jul 14, 2026)

### Enhancements

 * A new cluster formation mechanism that does not suffer from the computed seed node
   being unreachable.
 
   `portunus` members now recompute the seed as the lowest (lexicographically) reachable node,
   and other nodes either join their existing cluster or ask the leader whether they are
   already in its cluster, and if not, reset themselves and join the seed's cluster.
 
   This design significantly increases the chances of only one cluster being formed
   at the cost of a little bit of latency during initial cluster formation.

### Bug Fixes

 * Multiple small bug fixes


## Changes in `0.7.0` (never released to hex.pm)

Initial commit.
