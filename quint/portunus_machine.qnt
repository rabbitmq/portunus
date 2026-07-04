// A small, exhaustively-checkable model of the portunus lock machine's core
// safety. Leases hold keys; every grant carries a per-key strictly
// increasing fencing token; a lease's expiry frees its keys onto the
// succession queue. Promotion is score-ranked with ties broken towards the
// earlier candidate (lease id stands in for arrival order), matching the
// machine's `{score, seq}` ranking; a re-enqueue refreshes the candidate's
// bid. Mutual exclusion and fencing monotonicity must hold across every
// interleaving of grant, acquire, enqueue, release, and batch expiry.
// Release is token-fenced, as in the implementation: a release carrying a
// stale token replies `{error, not_owner}` and changes nothing
// (`staleRelease`). Strict per-key token increase is asserted at mint time
// through the `lastMinted` history map, so a mutant that re-mints an old
// token is caught even when the watermark checks still pass.
//
// `expireBatch` models one command revoking several leases at once — a tick
// catching several expiries, or one pid's death revoking every lease it
// held. Every token minted by one command carries that command's single
// Raft index, and a queued successor can itself be among the revoked: a
// fixed machine bug promoted such a successor and then re-promoted the
// freed key, minting two equal tokens for successive owners of one key.
// The fixed semantics modelled here skip the leases the command is
// revoking, so it grants and mints at most once per key; the pre-fix
// chain fails `mintStrictlyIncreases` (see the mutation note at
// `grantCount`).
//
// This is the abstract counterpart to portunus_machine_model_prop_SUITE: PropEr
// samples random command sequences against the real apply/3, while this checks
// the same invariants exhaustively over small bounds.
//
//   quint typecheck quint/portunus_machine.qnt
//   quint run       quint/portunus_machine.qnt --invariants mutualExclusion fencingMonotonic mintStrictlyIncreases
//   quint run       quint/portunus_machine.qnt --witnesses succession handover
//   quint verify    quint/portunus_machine.qnt --invariant=safety
//
// Widen Keys/Leases/Scores below to check larger instances.

module portunus_machine {
  pure val Keys = Set(1, 2)
  pure val Leases = Set(1, 2, 3)
  pure val Scores = Set(0, 1, 2)

  // Leases currently granted.
  var alive: Set[int]
  // (key, lease) pairs currently held. A set, not a map, so a buggy spec could
  // place two leases on one key and the mutual-exclusion check would catch it.
  var held: Set[(int, int)]
  // (key, lease, score) succession candidates. Promotion ranks by score, then by the
  // smaller lease id (standing in for earlier arrival).
  var candidates: Set[(int, int, int)]
  // Per key, the token of its current owner (0 when free).
  var tokenOf: int -> int
  // Per key, the highest token ever issued for it.
  var maxToken: int -> int
  // Monotonic source of fencing tokens: the Raft index, bumped once per
  // command that mints — every mint in one command shares its value.
  var index: int
  // History: per key, the most recent token minted for it, and whether any
  // mint ever failed to strictly increase past it. Only mints record these;
  // `mintStrictlyIncreases` reads them. Per key, not global: one command
  // may legitimately mint its one index for several *different* keys.
  var lastMinted: int -> int
  var mintRegressed: bool

  def ownersOf(k) = held.filter(p => p._1 == k)
  def free(k) = ownersOf(k).size() == 0
  // A key's candidates as (lease, score) bids.
  def bidsOf(k) = candidates.filter(w => w._1 == k).map(w => (w._2, w._3))

  // (lease, score) `a` is promoted before `b`: higher score, earlier
  // arrival on a tie — the machine's `{score, seq}` rank.
  pure def outranks(a: (int, int), b: (int, int)): bool =
    a._2 > b._2 or (a._2 == b._2 and a._1 < b._1)

  // The best bid's lease id, 0 when there are none.
  pure def bestOr0(bids: Set[(int, int)]): int =
    bids.fold((0, 0), (best, b) =>
      if (best._1 == 0 or outranks(b, best)) b else best)._1

  // The candidate a command promotes when it frees key `k`: the best-ranked
  // bid among leases the command is not revoking (0 when none).
  def promoteeOf(k, revoking) =
    bestOr0(bidsOf(k).filter(b => not(revoking.contains(b._1))))

  // Candidates from `revoking` ranked ahead of that promotee: the grants the
  // pre-fix machine performed and then revoked within the same command,
  // each minting the command's index once more.
  def revokingAhead(k, revoking) =
    bidsOf(k).filter(b => revoking.contains(b._1)).filter(b =>
      promoteeOf(k, revoking) == 0 or
      bidsOf(k).exists(s => s._1 == promoteeOf(k, revoking) and outranks(b, s)))

  // Mints a command performs for `k` when it frees it. Fixed semantics: one
  // for the promotee, none when every candidate is being revoked.
  // MUTATION for the pre-fix check (expect `safety` to fail):
  //   revokingAhead(k, revoking).size() + (if (promoteeOf(k, revoking) != 0) 1 else 0)
  def grantCount(k, revoking) = if (promoteeOf(k, revoking) != 0) 1 else 0

  action init = all {
    alive' = Set(),
    held' = Set(),
    candidates' = Set(),
    tokenOf' = Keys.mapBy(_ => 0),
    maxToken' = Keys.mapBy(_ => 0),
    index' = 0,
    lastMinted' = Keys.mapBy(_ => 0),
    mintRegressed' = false,
  }

  action grant(l) = all {
    not(alive.contains(l)),
    alive' = alive.union(Set(l)),
    held' = held,
    candidates' = candidates,
    tokenOf' = tokenOf,
    maxToken' = maxToken,
    index' = index,
    lastMinted' = lastMinted,
    mintRegressed' = mintRegressed,
  }

  action acquire(l, k) = all {
    alive.contains(l),
    free(k),
    index' = index + 1,
    held' = held.union(Set((k, l))),
    tokenOf' = tokenOf.set(k, index + 1),
    maxToken' = maxToken.set(k, index + 1),
    alive' = alive,
    candidates' = candidates,
    lastMinted' = lastMinted.set(k, index + 1),
    // Parenthesized: `x' = a or b` would parse as `(x' = a) or b`.
    mintRegressed' = (mintRegressed or index + 1 <= lastMinted.get(k)),
  }

  // A lease queues on a held key with a score; queueing again replaces its
  // bid, as a re-acquire refreshes `{score, seq}` in the machine.
  action enqueue(l, k, s) = all {
    alive.contains(l),
    not(free(k)),
    not(held.contains((k, l))),
    candidates' = candidates.filter(w => not(w._1 == k and w._2 == l))
      .union(Set((k, l, s))),
    alive' = alive,
    held' = held,
    tokenOf' = tokenOf,
    maxToken' = maxToken,
    index' = index,
    lastMinted' = lastMinted,
    mintRegressed' = mintRegressed,
  }

  // `release/2` carrying the owner's current token: frees the key and
  // promotes the best-ranked candidate, if any, minting this command's index.
  action release(k, t) = all {
    not(free(k)),
    t == tokenOf.get(k),
    val promotee = promoteeOf(k, Set())
    all {
      held' = held.filter(p => p._1 != k)
        .union(if (promotee != 0) Set((k, promotee)) else Set()),
      candidates' = candidates.filter(w => not(w._1 == k and w._2 == promotee)),
      index' = if (promotee != 0) index + 1 else index,
      tokenOf' = tokenOf.set(k, if (promotee != 0) index + 1 else 0),
      maxToken' = if (promotee != 0) maxToken.set(k, index + 1) else maxToken,
      lastMinted' = if (promotee != 0) lastMinted.set(k, index + 1) else lastMinted,
      mintRegressed' = (mintRegressed or (promotee != 0 and index + 1 <= lastMinted.get(k))),
    },
    alive' = alive,
  }

  // A release carrying a stale token: the machine replies `{error,
  // not_owner}` and changes nothing — the fence that keeps an ex-owner's
  // belated release from freeing its successor's key.
  action staleRelease(k, t) = all {
    t != tokenOf.get(k),
    held' = held,
    candidates' = candidates,
    tokenOf' = tokenOf,
    alive' = alive,
    maxToken' = maxToken,
    index' = index,
    lastMinted' = lastMinted,
    mintRegressed' = mintRegressed,
  }

  // One command revokes the whole set `ls` — a tick catching several
  // expiries, or one pid's death revoking every lease it held. Freed keys
  // promote the best-ranked candidate, skipping the command's own revoking
  // leases; every mint carries the command's one index, so a
  // second grant for the same key would re-mint an equal token — exactly
  // what `mintStrictlyIncreases` rejects.
  action expireBatch(ls) = all {
    ls != Set(),
    ls.subseteq(alive),
    val freed = Keys.filter(k => ownersOf(k).exists(p => ls.contains(p._2)))
    val minted = freed.filter(k => grantCount(k, ls) > 0)
    all {
      alive' = alive.exclude(ls),
      held' = held.filter(p => not(ls.contains(p._2)))
        .union(freed.filter(k => promoteeOf(k, ls) != 0)
          .map(k => (k, promoteeOf(k, ls)))),
      candidates' = candidates.filter(w => not(ls.contains(w._2))
        and not(freed.contains(w._1) and w._2 == promoteeOf(w._1, ls))),
      index' = if (minted != Set()) index + 1 else index,
      tokenOf' = Keys.mapBy(k =>
        if (freed.contains(k))
          (if (promoteeOf(k, ls) != 0) index + 1 else 0)
        else tokenOf.get(k)),
      maxToken' = Keys.mapBy(k =>
        if (freed.contains(k) and promoteeOf(k, ls) != 0) index + 1
        else maxToken.get(k)),
      lastMinted' = Keys.mapBy(k =>
        if (minted.contains(k)) index + 1 else lastMinted.get(k)),
      // The event-time strict-increase check, per mint: a first mint may
      // still undercut history; any second grant for the same key within
      // this command necessarily re-mints the index the first just
      // recorded.
      mintRegressed' = (mintRegressed or minted.exists(k =>
        index + 1 <= lastMinted.get(k) or grantCount(k, ls) >= 2)),
    },
  }

  action step = any {
    nondet l = Leases.oneOf()
    grant(l),
    nondet l = Leases.oneOf()
    nondet k = Keys.oneOf()
    acquire(l, k),
    nondet l = Leases.oneOf()
    nondet k = Keys.oneOf()
    nondet s = Scores.oneOf()
    enqueue(l, k, s),
    nondet k = Keys.oneOf()
    nondet t = 0.to(index).oneOf()
    release(k, t),
    nondet k = Keys.oneOf()
    nondet t = 0.to(index).oneOf()
    staleRelease(k, t),
    nondet ls = Leases.powerset().oneOf()
    expireBatch(ls),
  }

  // At most one lease holds any key.
  val mutualExclusion = Keys.forall(k => ownersOf(k).size() <= 1)

  // A held key's token is the highest ever issued for it, and no token exceeds
  // the global index.
  val fencingMonotonic =
    Keys.forall(k => tokenOf.get(k) <= maxToken.get(k) and maxToken.get(k) <= index)

  // Every mint for a key strictly increased past that key's previous one, so
  // a key's tokens never repeat. That's the half of the fencing claim the
  // watermark checks above cannot post (re-minting the current maximum, or
  // one command's chained double promotion).
  val mintStrictlyIncreases = not(mintRegressed)

  val safety = mutualExclusion and fencingMonotonic and mintStrictlyIncreases

  // Witnesses (`quint run --witnesses`): states the invariants are only
  // meaningful if the simulator actually reaches. `succession` is a key
  // with an owner and queued candidates; `handover` is a key freed after
  // ownership — the promotion paths the mint checks guard.
  val succession = Keys.exists(k => not(free(k)) and bidsOf(k).size() > 0)
  val handover = Keys.exists(k => tokenOf.get(k) == 0 and maxToken.get(k) > 0)
}
