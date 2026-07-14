## Changes in `0.9.0` (in development)

No changes yet.


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
