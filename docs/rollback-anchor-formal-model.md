# Static rollback-anchor safety model

Meldbase protocol v2 uses a fixed odd membership of `N = 2f+1` members and the
same `f+1` threshold for reads and writes. The model in this repository checks
the safety reason for that rule before any live-membership protocol is added.

## Checked properties

The independent executable model in
`integrations/anchorhttp/static_model_test.go` exhaustively enumerates one,
three and five-member systems over a bounded history containing both logical
commits and maintenance-only generations. It explores every delivery subset,
every exact read quorum and every recovery write quorum, including failed
writes and the database-ahead crash window. It checks that:

- every read quorum retains at least the last quorum-acknowledged floor;
- a database behind the observed floor is always rejected;
- a database ahead of the observed floor can repair any write quorum, after
  which every read quorum observes the repaired coordinate;
- crossed sequence/generation histories are rejected as incomparable;
- a failed logical write retained by a minority followed by recovery from the
  last acknowledged database and maintenance on that branch can create crossed
  coordinates; an exact response quorum without an observed maximum fails
  closed, while another clean majority or a later coordinate that advances both
  dimensions can make progress and converge them;
- reducing the read threshold below a majority produces a lost-floor
  counterexample; and
- endpoint aliases cannot count as distinct member identities.

This Go model runs in the ordinary test suite and under the race detector.

`integrations/anchorhttp/static_concurrency_model_test.go` independently
enumerates every delivery ordering for two concurrent writers on three- and
five-member systems. It covers crossed and comparable targets, persistence with
a delivered or lost response, client return before outstanding requests finish,
and both cancellation outcomes for those late requests: dropped before
execution or durably executed after return. It checks that crossed writers
never both acknowledge and every readable post-ack quorum remains at or above
the acknowledged floor. Both acknowledged and failed-but-quorum-durable
terminal outcomes must be reached so ambiguity coverage cannot become vacuous.
With response loss enabled, the current runs visit 1,845/948 states for the
three-member crossed/comparable scenarios and 180,571/54,786 states for five
members, including thousands of failed-but-quorum-durable terminal outcomes.

Real HTTP tests complement the scheduler model with repeated concurrent
writers and a connection-close fault injected after server-side persistence.
They prove the caller reports an ambiguous quorum failure when durable responses
are lost, while restart recovers the physically committed database and a later
advance converges the stale member.

## Bounded history checker

`internal/qualification.CheckAnchorHistory` is a separate bounded
linearizability checker for recorded operations. Each operation contains unique
controller-assigned invocation and return ordinals, so ordering does not depend
on synchronized process clocks. A response-before-invocation relation becomes
a mandatory real-time edge; overlapping operations may be ordered either way.

The checker searches monotonic-register histories of up to 24 operations.
Successful advances must take effect, successful loads must equal the state at
their chosen linearization point, and failed loads have no effect. Every failed
advance is explicitly ambiguous and is explored both as a no-op and, when
monotonic at that point, as a durable transition. A passing result includes one
concrete linearization and identifies the failed advances that had to be treated
as applied.

The HTTP integration suite records histories from two independent client
processes released through a controller gate against three real TCP servers.
It also starts two TLS 1.3/mTLS qualification-agent services, gives each an
independent Ed25519 key and durable request/result journal, and runs a real
`history-run` controller wave against the same quorum. The controller brackets
dispatch and verified response with conservative event ordinals. Each history
operation must exactly match one agent-signed request digest and outcome. The
schema-3 controller and schema-2 agent protocol also bind source/build revision,
dirty state and runtime identity for every execution host.
Crossed writers must elect exactly one successful operation. Injected response
loss exercises idempotent retrieval without executing the operation twice;
journal restart and corruption tests cover cached replay, incomplete durable
reservation recovery and fail-closed signature validation.

`anchor-qualification history-sign` adds stable full-member observations and a
final quorum load, then signs the controller log and checker result;
`history-verify` revalidates both layers offline. The repository test remains a
single-machine network topology. Production evidence still requires deploying
the same controller and agents across independently operated hosts and failure
domains and binding those infrastructure logs through the external-evidence
digest.

`specs/rollback_anchor_static.tla` is a separate bounded TLA+ state-machine
specification of the same static crash-fault protocol. It models logical and
maintenance publications, partial delivery, acknowledgement, crash, rollback
installation, rejection and database-ahead repair. Its invariants include
type safety, quorum retention of the acknowledged floor, rejection remaining
closed, and an opened database never being behind the acknowledged floor.

With the TLA+ tools installed, run:

```sh
java -cp /path/to/tla2tools.jar tlc2.TLC \
  -deadlock \
  -config specs/rollback_anchor_static.cfg \
  specs/rollback_anchor_static.tla
```

The checked-in configuration bounds the model to three members, sequence 2 and
generation 4. `-deadlock` is required because reaching either configured bound
is an intentional terminal state of this safety model. A local TLC 2.19 run
explored 89,561 distinct states (524,235 generated, depth 30) without an
invariant error. The repository does not bundle `tla2tools.jar`; therefore the
always-enforced gate is the exhaustive Go model, while TLC remains an explicit
external qualification step.

## Boundary of the result

These are safety models, not a claim of availability or a proof that a disk
truthfully implements `fsync`. The finite scheduler models bounded request
interleavings and response loss, while the TLA+ specification remains a static
single-writer state machine. Neither covers Byzantine members, compromised
keys, production TLS routing, unbounded histories, or live membership changes.
Filesystem and process failure behavior is covered by the separate destructive
harnesses; wire authentication and configuration binding are covered by
integration tests. Dynamic membership remains intentionally unsupported until
it has a joint-consensus specification and its own counterexample-driven model.
