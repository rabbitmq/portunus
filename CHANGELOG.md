## Changes in `0.10.0` (in development)

No changes yet.


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
