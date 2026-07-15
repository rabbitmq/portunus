## Changes in `0.9.0` (in development)

### Bug Fixes

 * The Ra system now gets its own write-ahead log directory.

   It only overrode `data_dir`, leaving the WAL in Ra's default directory, which
   under a host such as RabbitMQ belongs to another Ra system. That system deletes
   any log it finds there and does not recognise, so a restarted member came back
   with an empty log and lost committed entries.

   Nothing to migrate: those files were already being deleted. Takes effect on a
   node restart, not an in-place upgrade.

 * A member that recovers with an empty log no longer elects itself.

   With no entries there is no cluster configuration, so the member believed it was
   alone and won an election with a quorum of one, forming a rival cluster against
   a live leader. It now asks the other members first.

 * A cluster holding an election is no longer mistaken for an absent one.

   Members were asked a question only a leader can answer, so a cluster between
   leaders read as no cluster at all, and a returning node formed a rival against
   it. They are now asked about their own replica, which answers either way.

 * Reusing a Ra system that is already running with a different configuration is
   now refused rather than silently accepted, since the configuration passed to
   it was dropped: the system kept writing where it already wrote, and without
   the recovery strategy `portunus` asks for, it does not bring this node's
   replicas back at all. The directories and that strategy are compared; nothing
   else is, and passing the same ones remains idempotent.


## Changes in `0.8.0` (Jul 14, 2026)

### Enhancements

 * A new cluster formation mechanism that does not suffer from the computed seed node
   being computed.
 
   `portunus` members now recompute the seed as the lowest (lexicographically) reachable node,
   and other nodes either join their existing cluster or ask the leader whether they are
   already in its cluster, and if not, reset themselves and join the seed's cluster.
 
   This design significantly increases the chances of only one cluster being formed
   at the cost of a little bit of latency during initial cluster formation.

### Bug Fixes

 * Multiple small bug fixes


## Changes in `0.7.0` (never released to hex.pm)

Initial commit.
