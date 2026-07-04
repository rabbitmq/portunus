// A Choreo-based model of the client protocol between `portunus_registry`
// and `portunus_election`: contend, grant, lease loss, election crash with
// delayed restart, and remove.
//
// Two registry nodes ("a", "b") each fuse the
// registry and its single election for one key; "m" is the lock machine —
// commands serialize through it, Raft itself is out of scope (the replicated
// core is covered by portunus_machine.qnt).
//
// Messages are a persistent soup (as in Choreo):
// nothing is consumed and duplicates redeliver, so every handler guards on
// state, as the real client code must against retries and stale sends.
//
// To run verification, use:
//
// quint typecheck quint/portunus_registry.qnt
// quint run       quint/portunus_registry.qnt --invariants no_orphan child_fenced fenced_single_owner tokens_bounded
// quint run       quint/portunus_registry.qnt --witnesses lease_window failover_complete

module portunus_registry {
  import basicSpells.* from "spells/basicSpells"
  import choreo(processes = NODES) as choreo from "./choreo"

  //
  // Types
  //

  type Role = Reg | Machine
  // The registry's map entry for the key. `Restarting` is the time window
  // between an unsuccessful election and its restart.
  // Mirrors this `portunus_registry` type spec: `{ElectionPid | restarting, ChildSpec}`
  type Entry = Present | Restarting | Absent

  type Node = str
  type Claim = { node: Node, lease: int }
  // a granted claim
  type Grant = { node: Node, lease: int, token: int }

  type Message =
    | MAcquire(Claim)
    | MRevoke(Claim)
    | Granted(Grant)
    | LeaseLost(Claim)

  type Event = | RestartTimer

  // Node state tagged with a role
  type StateFields = {
    role: Role,
    // Registry-node fields.
    entry: Entry,
    // local singleton child running
    child: bool,
    // current lease id, 0 means "none"
    lease: int,
    // fencing token held, 0 means "none"
    token: int,
    // source of unique lease ids: "a" produces 100+n, "b" 200+n
    lease_seq: int,
    // a `remove/2` call marker, only set once
    removed: bool,
    // Machine fields.
    // "" means there's no owner
    owner: str,
    owner_lease: int,
    next_token: int,
    // succession candidates
    queue: Set[Claim],
    // granted, unexpired leases
    live: Set[Claim],
    // revoked or expired leases, only set once
    dead: Set[Claim],
  }

  type CustomEffects = ()
  type Extensions = ()

  //
  // Type aliases
  //

  type LocalState = choreo::LocalState[Node, StateFields]
  type LocalContext = choreo::LocalContext[Node, StateFields, Message, Event, Extensions]
  type Transition = choreo::Transition[Node, StateFields, Message, Event, CustomEffects]
  type GlobalContext = choreo::GlobalContext[
    Node,
    StateFields,
    Message,
    Event,
    Extensions
  ]

  //
  // Helpers
  //

  pure def release(state: LocalState, c: Claim, notify: bool): Transition = {
    val state1 = { ...state, live: state.live.setRemove(c),
                   queue: state.queue.setRemove(c), dead: state.dead.setAdd(c) }
    val loss: Set[choreo::Effect[Node, Message, Event, CustomEffects]] =
      if (notify) Set(choreo::Send({ to: c.node, message: LeaseLost(c) })) else Set()
    if (state.owner != c.node or state.owner_lease != c.lease) {
      { effects: loss, post_state: state1 }
    } else {
      val eligible = state1.queue.filter(w => state1.live.contains(w))
      if (eligible == Set()) {
        { effects: loss, post_state: { ...state1, owner: "", owner_lease: 0 } }
      } else {
        val heir = eligible.fold({ node: "", lease: 0 }, (best, w) =>
          if (best.lease == 0 or w.lease < best.lease) w else best)
        val t = state.next_token + 1
        { effects: loss.union(Set(choreo::Send({
            to: heir.node,
            message: Granted({ node: heir.node, lease: heir.lease, token: t })
          }))),
          post_state: { ...state1, queue: state1.queue.setRemove(heir),
                        owner: heir.node, owner_lease: heir.lease, next_token: t } }
      }
    }
  }

  //
  // Node transitions
  //

  pure def contend(ctx: LocalContext): Set[Transition] = {
    if (ctx.state.role == Reg and ctx.state.entry == Present and ctx.state.lease == 0) {
      val minted = ctx.state.lease_seq + 1
      Set({
        effects: Set(choreo::Send({
          to: MACHINE,
          message: MAcquire({ node: ctx.state.process_id, lease: minted })
        })),
        post_state: { ...ctx.state, lease: minted, lease_seq: minted }
      })
    } else Set()
  }

  // A grant installs only when it names this node's *current* lease:
  // `portunus_election.erl` matches `lease_id` in its `{portunus, granted,
  // ...}` clause, so a grant minted for an abandoned contend cannot install
  // a leader on a revoked token.
  // MUTATION M1 (expect `fenced_single_owner` to fail): drop the lease
  // match — accept on `g.node == ctx.state.process_id` alone. The stale
  // grant also installs a child on a crashed or removed node, so
  // `no_orphan` and `child_fenced` fail too.
  pure def listen_grant(ctx: LocalContext): Set[Grant] = {
    if (ctx.state.role == Reg and ctx.state.token == 0) {
      ctx.messages.filterMap(m => match m {
        | Granted(g) =>
          if (g.node == ctx.state.process_id and g.lease == ctx.state.lease)
            Some(g)
          else
            None
        | _ => None
      })
    } else Set()
  }

  pure def handle_grant(ctx: LocalContext, g: Grant): Transition = {
    effects: Set(),
    post_state: { ...ctx.state, token: g.token, child: true }
  }

  pure def receive_grant(ctx: LocalContext): Set[Transition] =
    choreo::cue(ctx, listen_grant, handle_grant)

  // Lease expired at the machine: step down and free the slot; `contend`
  // re-fires later. A LeaseLost for an old lease never matches.
  pure def listen_lease_lost(ctx: LocalContext): Set[Claim] = {
    if (ctx.state.role == Reg) {
      ctx.messages.filterMap(m => match m {
        | LeaseLost(c) =>
          if (c.node == ctx.state.process_id and c.lease == ctx.state.lease)
            Some(c)
          else
            None
        | _ => None
      })
    } else Set()
  }

  pure def handle_lease_lost(ctx: LocalContext, c: Claim): Transition = {
    effects: Set(),
    post_state: { ...ctx.state, child: false, token: 0, lease: 0 }
  }

  pure def lease_lost(ctx: LocalContext): Set[Transition] =
    choreo::cue(ctx, listen_lease_lost, handle_lease_lost)

  // The election process crashes: no terminate ran, so its lease stays live
  // at the machine until expiry. The registry stops the orphaned child in
  // its EXIT handler (`portunus_registry.erl` `handle_info({'EXIT', ...})`)
  // and schedules a restart after a backoff.
  pure def election_crash(ctx: LocalContext): Set[Transition] = {
    if (ctx.state.role == Reg and ctx.state.entry == Present) {
      Set({
        effects: Set(choreo::TriggerEvent(RestartTimer)),
        post_state: { ...ctx.state, lease: 0, token: 0, child: false, entry: Restarting }
      })
    } else Set()
  }

  // The backoff timer fires. It acts only on a `Restarting` entry: a timer
  // surviving a remove must not resurrect the entry.
  // MUTATION M2 (expect `no_orphan` to fail): drop the
  // `entry == Restarting` guard — the persisted timer event then flips a
  // removed entry back to Present.
  pure def restart_timer(ctx: LocalContext): Set[Transition] = {
    if (ctx.state.role == Reg and ctx.events.contains(RestartTimer)
        and ctx.state.entry == Restarting) {
      Set({
        effects: Set(),
        post_state: { ...ctx.state, entry: Present }
      })
    } else Set()
  }

  // `remove/2`: stop the child regardless of election state — a crashed
  // election's EXIT may still be queued behind the call — revoke the lease
  // if any, and drop the entry.
  // MUTATION M3 (expect `no_orphan` and `child_fenced` to fail): keep
  // `child` unchanged in the post-state.
  pure def remove(ctx: LocalContext): Set[Transition] = {
    if (ctx.state.role == Reg and ctx.state.entry != Absent) {
      Set({
        effects:
          if (ctx.state.lease != 0)
            Set(choreo::Send({
              to: MACHINE,
              message: MRevoke({ node: ctx.state.process_id, lease: ctx.state.lease })
            }))
          else Set(),
        post_state: { ...ctx.state, entry: Absent, child: false, lease: 0, token: 0,
                      removed: true }
      })
    } else Set()
  }

  //
  // Machine transitions
  //

  // A new claim arrives: grant it the free lock, or queue it as a succession
  // candidate. `live` makes a redelivery a no-op; `dead` rejects a revoked
  // or expired claim, as the real machine rejects an acquire naming an
  // unknown lease id.
  pure def listen_acquire(ctx: LocalContext): Set[Claim] = {
    if (ctx.state.role == Machine) {
      ctx.messages.filterMap(m => match m {
        | MAcquire(c) =>
          if (ctx.state.live.contains(c) or ctx.state.dead.contains(c))
            None
          else
            Some(c)
        | _ => None
      })
    } else Set()
  }

  pure def handle_acquire(ctx: LocalContext, c: Claim): Transition = {
    if (ctx.state.owner == "") {
      val t = ctx.state.next_token + 1
      { effects: Set(choreo::Send({
          to: c.node,
          message: Granted({ node: c.node, lease: c.lease, token: t })
        })),
        post_state: { ...ctx.state, live: ctx.state.live.setAdd(c),
                      owner: c.node, owner_lease: c.lease, next_token: t } }
    } else {
      { effects: Set(),
        post_state: { ...ctx.state, live: ctx.state.live.setAdd(c),
                      queue: ctx.state.queue.setAdd(c) } }
    }
  }

  pure def on_acquire(ctx: LocalContext): Set[Transition] =
    choreo::cue(ctx, listen_acquire, handle_acquire)

  // TTL expiry of any live lease, owning or queued: the holder learns via
  // LeaseLost.
  pure def listen_expiry(ctx: LocalContext): Set[Claim] = {
    if (ctx.state.role == Machine) ctx.state.live else Set()
  }

  pure def handle_expiry(ctx: LocalContext, c: Claim): Transition =
    release(ctx.state, c, true)

  pure def expiry(ctx: LocalContext): Set[Transition] =
    choreo::cue(ctx, listen_expiry, handle_expiry)

  // Voluntary revoke: same as expiry minus the LeaseLost — the revoker asked.
  pure def listen_revoke(ctx: LocalContext): Set[Claim] = {
    if (ctx.state.role == Machine) {
      ctx.messages.filterMap(m => match m {
        | MRevoke(c) => if (ctx.state.live.contains(c)) Some(c) else None
        | _ => None
      })
    } else Set()
  }

  pure def handle_revoke(ctx: LocalContext, c: Claim): Transition =
    release(ctx.state, c, false)

  pure def on_revoke(ctx: LocalContext): Set[Transition] =
    choreo::cue(ctx, listen_revoke, handle_revoke)

  pure def main_listener(ctx: LocalContext): Set[Transition] = {
    Set(
      contend(ctx),
      receive_grant(ctx),
      lease_lost(ctx),
      election_crash(ctx),
      restart_timer(ctx),
      remove(ctx),
      on_acquire(ctx),
      expiry(ctx),
      on_revoke(ctx)
    ).flatten()
  }

  //
  // Protocol configuration
  //

  pure val MACHINE: Node = "m"
  pure val REGS: Set[Node] = Set("a", "b")
  pure val NODES = REGS.union(Set(MACHINE))

  pure def seq_base(n: Node): int = if (n == "a") 100 else 200

  pure def initialize(n: Node): LocalState = {
    {
      process_id: n,
      role: if (n == MACHINE) Machine else Reg,
      entry: Present,
      child: false,
      lease: 0,
      token: 0,
      lease_seq: seq_base(n),
      removed: false,
      owner: "",
      owner_lease: 0,
      next_token: 0,
      queue: Set(),
      live: Set(),
      dead: Set(),
    }
  }

  action init = choreo::init({
    system: NODES.mapBy(n => initialize(n)),
    messages: NODES.mapBy(n => Set()),
    events: NODES.mapBy(n => Set()),
    extensions: ()
  })

  action step = choreo::step(
    main_listener,
    (c, _) => c
  )

  //
  // Properties and invariants
  //

  def reg(n: Node): LocalState = s.system.get(n)
  def machine: LocalState = s.system.get(MACHINE)

  // A removed key stays removed and leaves nothing behind: no entry, no
  // child, no lease. The ghost `removed` bit stands in for "every node
  // called `remove/2`" — there is no re-add in this model, so in the
  // unmutated model `removed` and `entry == Absent` coincide.
  val no_orphan = REGS.forall(n =>
    reg(n).removed implies
      (reg(n).entry == Absent and not(reg(n).child) and reg(n).lease == 0))

  // A running child is always the fenced elected child: entry present, lease
  // and token held. Any path that leaves the singleton running after its
  // election is gone fails here.
  val child_fenced = REGS.forall(n =>
    reg(n).child implies
      (reg(n).entry == Present and reg(n).lease != 0 and reg(n).token != 0))

  // A node runs the child believing it is fenced, and no LeaseLost for its
  // current lease is pending: outside the documented lease-expiry dual-run
  // window, at most one such node exists and the machine agrees it owns.
  def holds_unexempted(n: Node): bool = and {
    reg(n).child,
    reg(n).token != 0,
    not(s.messages.get(n).contains(LeaseLost({ node: n, lease: reg(n).lease })))
  }

  val fenced_single_owner = {
    val holders = REGS.filter(n => holds_unexempted(n))
    holders.size() <= 1 and holders.forall(n => machine.owner == n)
  }

  // No held token exceeds what the machine has minted.
  val tokens_bounded = REGS.forall(n => reg(n).token <= machine.next_token)

  val safety = no_orphan and child_fenced and fenced_single_owner and tokens_bounded

  // Witness: the lease-expiry dual-run window — a node still runs the fenced
  // child, its LeaseLost sits undelivered, and the machine has already
  // granted past its lease. This is the documented gap TTL failover accepts;
  // the fencing token, not the notification, protects external writes.
  val lease_window = REGS.exists(n => and {
    reg(n).child,
    reg(n).token != 0,
    s.messages.get(n).contains(LeaseLost({ node: n, lease: reg(n).lease })),
    machine.owner != "",
    machine.owner != n or machine.owner_lease != reg(n).lease,
  })

  // Witness: a second-or-later grant is installed and running fenced — an
  // earlier owner was released and superseded (by another node, or by the
  // same node on a fresh lease).
  val failover_complete = REGS.exists(n => and {
    holds_unexempted(n),
    machine.owner == n,
    reg(n).token >= 2,
  })

  //
  // Scenario runs
  //

  // Grant to "a", queue "b", expire "a": "b" is promoted on a fresh token and
  // "a" steps down on the LeaseLost.
  run failover_test = init
    .then(step_with("a", contend))
    .then(step_with(MACHINE, on_acquire))
    .then(step_with("a", receive_grant))
    .then(step_with("b", contend))
    .then(step_with(MACHINE, on_acquire))
    .then(step_deterministic(MACHINE,
      ctx => release(ctx.state, { node: "a", lease: 101 }, true)))
    .then(step_with("b", receive_grant))
    .then(step_with("a", lease_lost))
    .expect(and {
      not(reg("a").child),
      reg("a").lease == 0,
      reg("b").child,
      reg("b").token == 2,
      machine.owner == "b",
    })

  //
  // Wiring
  //

  val s = choreo::s

  action step_with(v: Node, listener: LocalContext => Set[Transition]): bool =
    choreo::step_with(v, listener, (c, _) => c)

  action step_deterministic(v: Node, listener: LocalContext => Transition): bool =
    choreo::step_deterministic(v, listener, (c, _) => c)
}
