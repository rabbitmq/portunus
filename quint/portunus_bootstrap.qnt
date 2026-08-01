// Bootstrap and convergence, exhaustively checkable at small bounds: the
// seed rule of `effective_seed/1`, `join_or_form/3` with `converge/4`
// (recover, join, form, reset-and-merge), and the `portunus_joiner` pass
// around them: membership check first, join only when it fails.
//
// Raft is abstracted to member views. A cluster is its members' local
// views. A join updates the joiner's and the seed's views (the committed
// config change), and `sync` brings a stale member up to date. Terms,
// votes, and the evict-before-rejoin identity rule live below this
// abstraction, and data safety is portunus_machine.qnt's job.
//
// Candidate views start at the node itself and only widen, so the model
// covers parallel boot under partial views: solo and rival clusters that
// must merge later. The environment stops servers, wipes disks, and
// prunes a downed node from the seed's view (`remove_member/2` as host
// policy).
//
// Two history flags record ground truth as the actions run, so a dropped
// guard is caught even though guard and truth agree in the honest model:
// `reset_when_included` (a reset the seed's view did not justify) and
// `elected_against_cluster` (a formation next to a visible live cluster).
//
// `no_stuck_when_stable` is the load-bearing invariant: with full
// candidate views, a state short of convergence must have a
// state-changing pass or sync enabled. Convergence then needs only the
// periodic re-check, never a lost trigger. Checking it found a real
// defect. See the note at `converge`.
//
// MUTATION M1 (expect `no_stuck_when_stable` to fail): drop the local
// half of `member_check`. A stopped server then passes every check and
// is never restarted.
// MUTATION M2 (expect `reset_healthy_spared` to fail): in `converge`,
// reset without asking the seed's view.
// MUTATION M3 (expect `election_respects_clusters` to fail): in
// `join_or_form`, form without consulting `peers_with_cluster`.
//
//   quint typecheck quint/portunus_bootstrap.qnt
//   quint run       quint/portunus_bootstrap.qnt --invariants safety
//   quint run       quint/portunus_bootstrap.qnt --witnesses converged_reached rivals_coexisted merged_after_reset rejoined_after_prune
//   quint verify    quint/portunus_bootstrap.qnt --invariant=safety
//
// Widen NODES below to check larger instances.

module portunus_bootstrap {
  pure val NODES = Set(1, 2, 3)

  type Replica = {
    // the identity is recoverable: a readable server config on disk
    known: bool,
    // the log carries entries: a join or formation was committed
    hasLog: bool,
    running: bool,
    // this replica's member view
    members: Set[int],
  }

  type Hist = {
    reset_when_included: bool,
    elected_against_cluster: bool,
    reset_ever: bool,
    removed_ever: bool,
  }

  // Replicas and history flags in one variable: an action assigns both in
  // a single expression, so `pass_result` is evaluated against the
  // pre-state only. A `val` in an action is re-evaluated per referencing
  // conjunct, and a conjunct evaluated after `nodes'` would see the
  // post-pass state and record nothing.
  var sys: { nodes: int -> Replica, hist: Hist }
  var cand: int -> Set[int]

  def nodes = sys.nodes
  def hist = sys.hist

  pure val blank: Replica =
    { known: false, hasLog: false, running: false, members: Set() }

  pure def min_node(s: Set[int]): int =
    s.fold(99, (best, n) => if (n < best) n else best)

  // `effective_seed/1`. Reachability is not modelled separately: a
  // stopped seed fails the ask the same way an unreachable one does.
  def seed_of(n: int): int = min_node(cand.get(n))

  // `peer_views/2` filtered to `{cluster, _}` answers.
  def peers_with_cluster(n: int): Set[int] =
    cand.get(n).filter(p =>
      p != n and nodes.get(p).running and nodes.get(p).members.size() > 1)

  //
  // The pass as a function from the current replicas to the next, so one
  // definition drives both the `pass` action and the progress question in
  // `no_stuck_when_stable`.
  //

  type PassResult = { ns: int -> Replica, bad_reset: bool, was_reset: bool }

  pure def unchanged(ns: int -> Replica): PassResult =
    { ns: ns, bad_reset: false, was_reset: false }

  // `join_cluster/3` to a running seed: the joiner adopts the seed's view
  // plus itself, the seed's view gains the joiner. Other members stay
  // stale until `sync`. A known joiner keeps its recovered state, an
  // unknown one starts fresh.
  pure def join_to(ns: int -> Replica, n: int, s: int): PassResult = {
    if (not(ns.get(s).running)) unchanged(ns) else {
      val sv = ns.get(s).members.union(Set(n))
      val ns1 = ns.setBy(s, r => { ...r, members: sv })
      val joined =
        if (ns1.get(n).known)
          ns1.setBy(n, r => { ...r, running: true })
        else
          ns1.set(n, { known: true, hasLog: true, running: true, members: sv })
      unchanged(joined)
    }
  }

  // `converge/4` for the recovered replica of `n`, seed `s`.
  //
  // The last branch deviates from `portunus.erl` as of 0.13.0 and is the
  // model's finding: the implementation first checks that the local view
  // carries the seed and answers `ok` if so, but that view is stale when
  // the seed re-formed on a wiped disk without this node, and trusting it
  // leaves this node outside the cluster forever. The seed must be asked
  // every time.
  def converge(ns: int -> Replica, n: int, s: int): PassResult = {
    val r1 = ns.get(n)
    if (not(r1.hasLog)) {
      // `elect_or_join`: an empty log is genesis or a lost log, and only
      // the peers can say which.
      if (n == s) {
        val peers = peers_with_cluster(n)
        if (peers.size() > 0)
          join_to(ns, n, min_node(peers))
        else if (r1.members == Set(n))
          unchanged(ns.setBy(n, r => { ...r, hasLog: true }))
        else
          unchanged(ns)
      } else join_to(ns, n, s)
    } else if (not(r1.members.contains(n))) {
      // committed knowledge of removal: rejoin as a new member
      join_to(ns, n, s)
    } else if (n == s) {
      unchanged(ns)
    } else {
      // `merge_into_seed/4`: ask the seed before the one destructive
      // step. MUTATION M2 lives here.
      if (not(ns.get(s).running)) unchanged(ns)
      else if (ns.get(s).members.contains(n)) unchanged(ns)
      else {
        val included = ns.get(s).members.contains(n)
        val rejoined = join_to(ns.set(n, blank), n, s)
        { ns: rejoined.ns, bad_reset: included, was_reset: true }
      }
    }
  }

  // `join_or_form/3` for node `n`.
  def join_or_form(n: int): PassResult = {
    val s = seed_of(n)
    if (nodes.get(n).known) {
      // recover first (`restart_server/2`), then converge
      val ns0 = nodes.setBy(n, r => { ...r, running: true })
      converge(ns0, n, s)
    } else if (n == s) {
      // `form_or_join_existing/3`: form only when no reachable peer runs
      // a cluster. MUTATION M3 lives here.
      val peers = peers_with_cluster(n)
      if (peers.size() > 0)
        join_to(nodes, n, min_node(peers))
      else
        unchanged(nodes.set(n,
          { known: true, hasLog: true, running: true, members: Set(n) }))
    } else join_to(nodes, n, s)
  }

  // The `portunus_joiner` member check. The local part catches a stopped
  // or reset server the seed still lists. The seed part catches a node
  // the seed's cluster no longer carries. MUTATION M1 drops the local
  // part.
  def member_check(n: int): bool = {
    val r = nodes.get(n)
    val s = seed_of(n)
    r.running and r.known and r.hasLog and r.members.contains(n)
      and nodes.get(s).running and nodes.get(s).members.contains(n)
  }

  def pass_result(n: int): PassResult =
    if (member_check(n)) unchanged(nodes) else join_or_form(n)

  // Whether this pass formed (started a fresh solo log) while a live
  // cluster was visible among the candidates: ground truth for M3. A
  // join is recognizable by its post view, which carries the seed too.
  def formed_against_cluster(n: int, res: PassResult): bool =
    res.ns.get(n).hasLog and not(nodes.get(n).hasLog)
      and res.ns.get(n).members == Set(n)
      and peers_with_cluster(n).size() > 0

  //
  // Actions
  //

  action pass(n: int): bool = {
    val res = pass_result(n)
    all {
      sys' = {
        nodes: res.ns,
        hist: {
          reset_when_included: hist.reset_when_included or res.bad_reset,
          elected_against_cluster:
            hist.elected_against_cluster or formed_against_cluster(n, res),
          reset_ever: hist.reset_ever or res.was_reset,
          removed_ever: hist.removed_ever,
        },
      },
      cand' = cand,
    }
  }

  def sync_ok(n: int, m: int): bool = {
    val rn = nodes.get(n)
    val rm = nodes.get(m)
    n != m and rn.running and rm.running and rn.hasLog and rm.hasLog
      and rm.members.contains(n)
      and rn.members.subseteq(rm.members) and rn.members != rm.members
  }

  // Config replication inside one cluster: a stale member adopts a newer
  // view that carries it. Views only grow, so superset is the log order.
  action sync(n: int, m: int): bool = all {
    sync_ok(n, m),
    sys' = {
      nodes: nodes.setBy(n, r => { ...r, members: nodes.get(m).members }),
      hist: hist,
    },
    cand' = cand,
  }

  action environment(ns: int -> Replica, cs: int -> Set[int], rm: bool): bool = all {
    sys' = { nodes: ns, hist: { ...hist, removed_ever: hist.removed_ever or rm } },
    cand' = cs,
  }

  // The host's membership view widens. It never shrinks here, since the
  // joiner ignores `nodedown`.
  action widen(n: int, extra: int): bool = all {
    not(cand.get(n).contains(extra)),
    environment(nodes, cand.setBy(n, c => c.union(Set(extra))), false),
  }

  action stop(n: int): bool = all {
    nodes.get(n).running,
    environment(nodes.setBy(n, r => { ...r, running: false }), cand, false),
  }

  // a lost data dir: identity, log, and view are gone
  action wipe(n: int): bool = all {
    nodes.get(n).known,
    environment(nodes.set(n, blank), cand, false),
  }

  // Host policy over `remove_member/2`: prune a downed node from the
  // seed's cluster. The removal is committed, so the pruned node's view
  // learns it too. Its next pass rejoins as a new member.
  action remove(n: int): bool = {
    val s = min_node(NODES)
    all {
      not(nodes.get(n).running),
      n != s,
      nodes.get(s).members.contains(n),
      environment(
        nodes.setBy(s, r => { ...r, members: r.members.exclude(Set(n)) })
             .setBy(n, r => { ...r, members: r.members.exclude(Set(n)) }),
        cand, true),
    }
  }

  action init = all {
    sys' = {
      nodes: NODES.mapBy(n => blank),
      hist: {
        reset_when_included: false,
        elected_against_cluster: false,
        reset_ever: false,
        removed_ever: false,
      },
    },
    cand' = NODES.mapBy(n => Set(n)),
  }

  action step = {
    nondet n = NODES.oneOf()
    nondet m = NODES.oneOf()
    any {
      pass(n),
      sync(n, m),
      widen(n, m),
      stop(n),
      wipe(n),
      remove(n),
    }
  }

  //
  // Invariants
  //

  // The one destructive step only ran against a node the seed's own view
  // had excluded, so a retry under transient doubt cannot wipe a healthy
  // replica.
  val reset_healthy_spared = not(hist.reset_when_included)

  // No node formed while a running multi-member cluster was visible among
  // its candidates: rivals arise only from disjoint candidate views.
  val election_respects_clusters = not(hist.elected_against_cluster)

  def stable = NODES.forall(n => cand.get(n) == NODES)

  def converged = NODES.forall(n => {
    val r = nodes.get(n)
    r.running and r.known and r.hasLog and r.members == NODES
  })

  // With full candidate views, any state short of convergence has an
  // enabled state-changing pass or sync.
  val no_stuck_when_stable =
    not(stable) or converged
      or NODES.exists(n => pass_result(n).ns != nodes)
      or NODES.exists(n => NODES.exists(m => sync_ok(n, m)))

  val safety = reset_healthy_spared and election_respects_clusters
    and no_stuck_when_stable

  //
  // Witnesses (`quint run --witnesses`): states the invariants are only
  // meaningful if the simulator actually reaches.
  //

  val converged_reached = converged

  // two independent clusters coexist, at least one multi-member
  val rivals_coexisted = NODES.exists(n => NODES.exists(m => {
    val rn = nodes.get(n)
    val rm = nodes.get(m)
    rn.running and rm.running and rn.hasLog and rm.hasLog
      and rn.members.size() > 1
      and rm.members.size() > 0
      and rn.members.intersect(rm.members) == Set()
  }))

  val merged_after_reset = hist.reset_ever and converged

  val rejoined_after_prune = hist.removed_ever and converged
}
