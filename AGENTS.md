# Instructions for AI Agents

## What This Is

`portunus` is a small `ra`/Raft-based lock server for the Erlang ecosystem:
cluster-wide mutual exclusion, TTL leases with renewal, leader election,
and a FIFO succession queue. It is a CP (consistency over availability) service like `etcd` or ZooKeeper.

`portunus` implements a [`ra`](http://github.com/rabbitmq/ra/) state machine.

While `portunus` was originally designed and developed for the needs of the RabbitMQ Core Team,
it is not specific to RabbitMQ.

## Target Erlang/OTP

Erlang/OTP 27+: use `maybe`, `-moduledoc`, `-doc`, `proc_lib:set_label/1`,
`~"…"` sigils, and the built-in `json` module as needed.

This is a young codebase that targets modern Erlang/OTP, so prefer modern constructs.

## Build

`rebar3` is the primary build tool. `erlang.mk` (GNU Make 4, `gmake`) is kept for
smoother integration with RabbitMQ and caters to the habits of the RabbitMQ Core Team.

 * `rebar3 compile` compiles the codebase. Run it before code changes to confirm a clean baseline
 * `rebar3 ct`: runs Common Test suites
 * `rebar3 eunit`: runs EUnit tests (see below)
 * `rebar3 dialyzer` runs must be warning-free
 * `rebar3 xref` must pass
 * `gmake`, `gmake tests` amd `gmake dialyze` are tje `erlang.mk` equivalents

## Dependencies

 * `ra` (3.1.x): Raft engine and the `ra_machine` behaviour
 * `seshat` (1.x): counters registry for metrics

No other runtime deps. Transport, identity, and TLS come from the Erlang
distribution.

## Module Layout

Core (the replicated state machine and its client API):

 * `portunus_machine`: the `ra_machine`: leases, locks, fencing tokens,
   score-ordered succession, monitor-driven release. Must stay
   deterministic (see below)
 * `portunus_machine_aux`: lease renewal and expiry sweep logic; renewals
   stay out of the Raft log and live in the leader's `aux` state
 * `portunus`: the public client API
 * `portunus_app`, `portunus_sup`: application and top supervisor
 * `portunus_counters`: seshat metrics, one counter set per node

Batteries (client-side extras, no new replicated state):

 * `portunus_keepalive`: lease renewer process, linked to the holder
 * `portunus_session`: one lease per node holding many exclusive keys
 * `portunus_election`: one elected instance per key; ownership moves to
   another node on lease loss
 * `portunus_service`: a managed set of keys, each with exactly one owner
 * `portunus_registry`: a dynamic cluster-wide supervisor, one election per key
 * `portunus_supervisor`: a declarative, `supervisor`-shaped layer on the registry
 * `portunus_affinity`, `portunus_affinity_*`: placement scoring for succession
 * `portunus_delayed_restart`: rewrites `{permanent, Delay}` into a
   rate-limited standard child spec
 * `portunus_local_sup`: the local supervisor an elected owner boots
   children into

## State Machine Invariants (do not break)

 * No node-local time in `apply/3`: use `system_time` from the command
   metadata, never `os:system_time/0` or `erlang:monotonic_time/0`
 * No non-determinism in `apply/3`: no `make_ref/0`, `self/0`,
   `node/0`-dependent branching, random values, or map iteration order
   stored into machine state
 * Tokens and ids come from the Raft `index`
 * Safety: at-most-one-owner and monotonic fencing tokens, both clock-independent
 * Liveness: a mandatory lease with expiry; approximation is acceptable
 * A client must treat renewal failure as "lease possibly lost" and stand down

## Testing

 * Organise test modules by type: `*_unit_SUITE`, `*_prop_SUITE`, `*_integration_SUITE`
 * PropEr properties are named `prop_`, each with a brief comment, and live
   in their own `*_prop_SUITE` (proper.hrl clashes with eunit/ct)
 * Test modules carry no moduledoc, not even `-moduledoc false`
 * The property worth checking hardest: at most one owner per key
 * To run a single CT suite, use `rebar3 ct --suite`: e.g.
   `rebar3 ct --suite test/portunus_machine_unit_SUITE`

## Comments, Writing Style and Voice

Only add very important comments to tests and implementation.

### Voice

Write like a senior engineer who values clarity and simplicity, in all
prose: design docs, analyses, notes, and commit messages.

 * Plain and factual: state the why in one line, never narrate the what
 * Name the actual mechanism, not an image of it
 * Prefer the plainest word. No coined verbs, no jargon for its own sake
 * No flourish, no editorializing, no imagery. Real domain terms are fine
 * If a sentence needs a second clause to justify itself, it is too clever
 * Full sentences over compressed noun phrases: "convenience module", not
   "a `mirrored_supervisor`-shaped convenience"
 * State guarantees explicitly: "only one instance can run in the cluster
   at any given time", not "becomes a cluster-wide singleton"
 * Write "Erlang/OTP", never bare "OTP"
 * Spell jargon out: "ownership moves to another node", not "failover";
   avoid "singleton" where "the child" or a one-instance phrasing works
 * No "term — explanation" em-dash glosses: use ": " or parentheses
 * These rules apply to identifiers too: `crash_promotes_standby`, not
   `crash_failover_promotes_standby`; `{election, X}` keys, not
   `{singleton, X}`

### Writing and Markdown Style

 * Never add full stops to Markdown list items
 * Use "X and Y" in prose, not "X / Y". Exceptions: unit fractions
   (`bytes/edge`), abbreviations (`I/O`), and paths or code (`tests/unit/`,
   `m:f/a`, `queue.declare`)
 * Wrap code identifiers (types, functions, modules, paths) in backticks
 * No robotic labels like `**Thing / other:**`; write a plain sentence
 * Match the conventions of the file you are editing: bullets, heading
   depth, ID schemes, and table shape vary, and the local choice wins

## Git

 * Never add yourself as a commit co-author
 * Never mention yourself in commit messages
 * Do not commit without explicit permission
