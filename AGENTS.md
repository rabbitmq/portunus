# Instructions for AI Agents

## What is This Codebase?

`portunus` is a small, generic, Ra/Raft-based lock server for the
Erlang ecosystem: cluster-wide mutual exclusion, TTL leases with
renewal, leader election, and a FIFO succession queue. It is
an `etcd`/ZooKeeper-style CP (consistency over availability) service.
Its first job is to replace RabbitMQ's ancient `mirrored_supervisor`,
but it does not depend on RabbitMQ.

It uses Ra and implements a Ra state machine. It does not depend on
or use Khepri.

## Target Erlang/OTP

Targets Erlang/OTP 27+ and uses it deliberately: `maybe` expressions,
`-moduledoc`/`-doc`, `proc_lib:set_label/1`, `~"…"` sigils, the built-in
`json` module. Unlike Ra/Seshat/Osiris (which are years to nearly two
decades old and target much older Erlang/OTP releases), this is a
greenfield codebase with no legacy compatibility burden: prefer the
modern construct.

## Build System

Both `rebar3` and `erlang.mk`, like Ra. `rebar3` is the primary tool;
`erlang.mk` (`make`/`gmake`) is kept working to be a good RabbitMQ-family
member. Look for `gmake` as well as `make`.

 * `rebar3 compile` — build
 * `rebar3 ct` — Common Test suites
 * `rebar3 eunit` — EUnit (inline `-ifdef(TEST)` and `test/*_tests.erl`)
 * `rebar3 dialyzer` — must be warning-free
 * `rebar3 xref`
 * `gmake` / `gmake tests` / `gmake dialyze` — the erlang.mk equivalents

Run `rebar3 compile` before changing code to confirm a clean baseline.

## Dependencies

 * `ra` (3.1.x) — the Raft engine and `ra_machine` behaviour
 * `seshat` (1.x) — counters/gauges registry, used for metrics

No Khepri. No third-party runtime deps. Transport, identity, and TLS are
the Erlang distribution's job.

## Module Layout

Core (the replicated state machine and its client API):

 * `portunus_machine` — the `ra_machine`: leases, locks, fencing tokens,
   score-ordered succession, tick-based lease expiry, monitor-driven
   release. The crown jewel; keep it deterministic (see below)
 * `portunus` — the public client API: leases, locks, queries, cluster
   lifecycle, and the convenience wrappers
 * `portunus_app` / `portunus_sup` — Erlang/OTP application and top
   supervisor
 * `portunus_counters` — seshat metrics (one counter set per node)

Batteries (client-side extras built on top of the core, no new
replicated state):

 * `portunus_keepalive` — the lease renewer process, linked to the holder
 * `portunus_session` — one lease per node holding many exclusive keys
 * `portunus_election` — keeps one elected instance of a component per
   key; ownership moves to another node on lease loss
 * `portunus_service` — a managed set of keys, each with exactly one
   owner (optional affinity)
 * `portunus_registry` — a dynamic cluster-wide supervisor: children are
   added and removed at runtime, one election per key
 * `portunus_supervisor` — a declarative, `supervisor`-shaped layer built
   on top of the registry
 * `portunus_affinity` and the `portunus_affinity_*` strategy modules —
   placement scoring for succession
 * `portunus_delayed_restart` — rewrites the extended `{permanent, Delay}`
   restart type into a rate-limited standard child spec
 * `portunus_local_sup` — the local supervisor an elected owner boots its
   children into

## State Machine Invariants (do not break)

 * **No node-local time in `apply/3`.** Use the `system_time` from the
   command metadata (leader-stamped, identical on every replica). Never
   `os:system_time/0`, `erlang:monotonic_time/0`, etc. inside the machine
 * **No non-determinism in `apply/3`.** No `make_ref/0`, `self/0`,
   `node/0`-dependent branching, `Math`-random, or map iteration order
   that escapes into state. Tokens and ids come from the Raft `index`
 * **Effects carry no authority.** Timers and monitors only *trigger*
   commands; the decision is re-derived from replicated state
 * **Safety vs. liveness.** At-most-one-owner and monotonic fencing
   tokens are safety (Raft + tokens, clock-independent); lease expiry is
   liveness (approximate). A client must treat renewal failure as
   "lease possibly lost" and stop acting

## Testing

 * Organise by type: `*_unit_SUITE`, `*_prop_SUITE`, `*_integration_SUITE`
 * Property-based tests use PropEr; property functions are named `prop_`,
   each with a brief comment, since properties are hard to read. Keep them
   in their own `*_prop_SUITE` module (proper.hrl clashes with eunit/ct)
 * Test modules do not carry `-moduledoc false` (or any moduledoc)
 * The core safety invariant, at most one owner per key, is the
   property worth checking hardest
 * Run a single CT suite: `rebar3 ct --suite test/portunus_machine_unit_SUITE`

## Comments, Writing Style and Voice

Only add very important comments to the tests and the implementation.

### Voice

Write like a senior engineer who values clarity and simplicity. This applies
to all prose: design docs, analyses, notes, and commit messages.

 * Plain and factual: state the why in one line, never narrate the what
 * Literal mechanism over metaphor: name the actual thing, not an image of it
 * Prefer the plainest word. No coined verbs, no jargon for its own sake
 * No flourish, no editorializing, no imagery. Real domain terms are fine
 * If a sentence needs a second clause to justify itself, it is probably too clever
 * Plain full sentences over compressed clever noun phrases: "convenience
   module", not "a `mirrored_supervisor`-shaped convenience"
 * State guarantees explicitly: "only one instance can run in the cluster
   at any given time", not "becomes a cluster-wide singleton"
 * Write "Erlang/OTP", never bare "OTP"
 * Spell jargon out: "ownership moves to another node" or "leadership
   (ownership) transfer", not "failover"
 * Avoid "singleton" where "the child", "the elected child", or an
   explicit one-instance phrasing reads naturally
 * No "term — explanation" em-dash glosses: use ": " or parentheses
 * These vocabulary rules apply to identifiers too: test case names,
   helper module names, and test lock-key atoms
   (`crash_promotes_standby`, not `crash_failover_promotes_standby`;
   `{election, X}` keys, not `{singleton, X}`)

### Writing and Markdown Style

 * Never add full stops to Markdown list items
 * Use "X and Y" in prose, not "X / Y" slash-shorthand. Exceptions: unit
   fractions (`bytes/edge`), single-concept abbreviations (`I/O`), and paths
   or code (`tests/unit/`, `m:f/a`, `queue.declare`)
 * Wrap code identifiers (types, functions, modules, file names, paths)
   in backticks in prose
 * Avoid robotic labels such as `**Thing / other:**`; write a plain
   sentence or a simple label
 * Match the existing conventions of the file and subdirectory you are
   editing: bullet character, heading depth, ID schemes, and table shape
   vary by project, and the local choice wins

## Git

 * Never add yourself to the list of commit co-authors
 * Never mention yourself in commit messages in any way
 * Do not commit changes automatically without explicit permission
