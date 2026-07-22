# Core runtime evolution

This document records cross-cutting runtime direction. It is an engineering
map, not a public API or production-readiness claim: protocol, SDK, operations,
and qualification pages own their respective contracts.

## Current facts

Meldbase is a single-node copy-on-write engine. Each successful logical mutation
publishes one new durable generation, advances one monotonically increasing
commit sequence, appends one typed `CommitBatch`, and only then becomes visible
to query, reactive, RPC-idempotency and recovery code. The physical publication
currently has two persistence barriers: staged data/root pages are synced before
the inactive Meta slot is written and synced.

The Commit Log already contains canonical document/catalog changes, typed
before/after versions, an immutable changed-path set when the mutation compiler
can prove one, the post-commit Catalog root and a transaction ID. It is the
source for historical query replay. It is not a client query-delta protocol: a
query delta is a derived, authorized view of one or more commit records.

The database has one serialized write path. Shared reactive views run outside
that path from a bounded queue. By default, an acknowledged storage write includes
one physical publication and its rollback-anchor update when configured. An
explicitly enabled, bounded CommitCoordinator can group ordinary collection
mutations and completed public point-write transactions into one physical
final-Meta publication while retaining one ordered logical sequence per request.
When rollback protection is enabled, the group's final Meta is synchronously
anchored before any member succeeds.

## Invariants that must survive every optimization

1. A successful synchronous write is recoverable after process loss and is not
   acknowledged before its data and Meta publication are durable.
2. Each accepted logical mutation receives exactly one strictly increasing
  commit sequence. Sequence order is the order observed by the Commit Log,
   reactive replay, idempotency records and rollback anchors.
3. A failed write has no visible partial document/index/System-record state.
   An uncertain external rollback-anchor outcome remains fail-stop; it is never
   reclassified as a safe retry.
4. Reactive work, direct watchers and network clients never run user work on a
  storage writer. Backpressure must coalesce, resync or disconnect consumers;
   it must not grow memory without a bound or silently reorder commits.
   Each canonical reactive view additionally has immutable matching-member
   count and canonical-byte admission limits; a query window does not weaken
   that bound because the view may need off-window members for later ordering.
5. The raw Commit Log remains private to the engine. Authorization, projection,
   workspace scoping and opaque resume-token chaining are applied only above it.
6. Existing files, recovery receipts and protocol-v1 frames remain readable
   throughout this phase.

## Delivery order

### Phase A — evidence before a new queue

The checked-in benchmarks cover:

- concurrent independent commits, exposing the current single-writer and
  sync boundary;
- public two-insert sequential versus coordinator-grouped pairs, including
  caller admission/result delivery and physical-generation accounting;
- durable reactive fan-out over one shared canonical query;
- the existing selective/broad-view benchmarks, which distinguish shared-view
  work from per-subscriber delivery.

Run them on a dedicated target volume before and after a change. Record commit
throughput, p50/p95/p99 latency, fsync latency, CPU, allocation rate, resident
memory, write amplification and reactive queue pressure. CI may reject a
regression only against a stable dedicated-runner baseline; developer laptops
provide investigation data rather than universal thresholds.

### Phase B — CommitCoordinator

The coordinator is an opt-in optimization for independently accepted synchronous
writes. It may share a persistence barrier only when every logical request
retains its own durable sequence, recovery behavior, and terminal result.
`sync` remains the default acknowledgement mode; a future asynchronous mode
must be explicit and unable to masquerade as durable success.

The [CommitCoordinator design](commit-coordinator) is the authority for the
COW publication shape, recovery matrix, admission/cancellation behavior,
supported operations, and qualification evidence. It also records the current
implementation status. Do not restate that detail in this roadmap.

### Phase C — canonical change-stream boundary

Keep `CommitBatch` as the internal source of truth and add a bounded dispatcher
that fans it into replay, reactive views, backup/archive and eventually a
single-writer follower. Consumers must declare retention/acknowledgement needs;
they cannot pin history indefinitely. This phase does not add clustering,
multi-writer replication, a general-purpose CDC/replication protocol or a
Mongo-compatible oplog.

The first in-process slice is implemented: after durable publication the commit
path only admits an immutable `ChangeBatch` to a bounded ordered dispatcher.
Direct Go watchers are no longer cloned/enqueued under the database writer; a
new watcher starts strictly after the current commit token, and an overloaded
dispatcher closes non-resumable watchers with `ErrSlowConsumer`. Reactive views
use their existing snapshot-resync path on the same overload.

Meldbase now exposes the first public consumer boundary:
`CreateDurableCollectionChanges` and `OpenDurableCollectionChanges` create or
resume a named, collection-scoped Go document feed. Every global Commit Log
position is delivered as one batch and must be acknowledged explicitly; batches
for another collection are intentionally empty, so an application can advance
its checkpoint without pinning unrelated history. The feed excludes private
System records, collection lifecycle and raw storage bytes. It is consequently
safe as an application outbox/CDC primitive, but is deliberately not yet an
archive image format or follower-replication protocol.

`CreateDurableDatabaseChanges` is the corresponding database-level semantic
stream. It carries collection creation, completed index publication and document
changes in the same ordered token sequence, while private System records remain
non-observable. `BeginArchive` now joins that stream to a verified physical
backup without a snapshot/tail gap: it first pins a named checkpoint, then
creates the backup at `SnapshotToken`. A receiver durably stores and verifies
that artifact, drains and acknowledges tokens through `SnapshotToken` without
applying them, and applies only later tokens. The API deliberately stops there:
network framing, remote application, follower write exclusion, promotion and
failover are separate protocol decisions, not claims hidden in a local backup.

The local application state machine is now explicit too: `OpenFollower`
opens a bootstrap copy read-only, and `Follower.Apply` accepts only the exact
next `DurableDatabaseChangeBatch`. A duplicate, gap or local divergence returns
`ErrReplicaSequence`; ordinary CRUD and index-build starts return
`ErrReplicaReadOnly`. Catalog/document application is one target commit, so
queries and reactive readers never observe a half-applied source batch. A
publicly empty source position is represented by a private target marker solely
to preserve token contiguity; it does not pretend to replicate source-side RPC
or idempotency System records. `integrations/replicationhttp` and
`integrations/replicationws` supply the fixed HTTPS bootstrap and authenticated
WSS tail transports, while `primarylease` supplies a local write-fence/control
plane building block. Deployment still owns independent failure domains,
certificate issuance/rotation, member lifecycle, election/client routing and
the tested partition/failover procedure; promotion itself remains deliberately
closed without a separate history proof.

For deployments that already have a controller, `PrimaryWriteFence` now
provides the narrow local primary-side enforcement point. Its fast local
lease/epoch check runs before each business commit, including every logical
member in a group, and rejects a lost primary before storage mutation or token
advance. It intentionally does not renew a lease, call a controller while the
writer is held, select a leader or claim failover: those operations remain an
external control plane. Follower application bypasses the fence only while the
database is read-only and applies identity-bound source history; promotion then
requires that same configured fence to bind the controller-issued epoch before
returning ordinary writes to it.

`integrations/primarylease` supplies one concrete local implementation: a
controller signs a bounded-lived Ed25519 certificate for the database, owner,
epoch and current sequence, while the database process verifies it locally on
every write. It also supplies a durable per-member `FileStore`, exact-majority
`QuorumStore`, controller-side CAS `Authority`, promotion-readiness hook and a
narrow mTLS member RPC adapter. `integrations/authorityhttp` provides the
separate authenticated grant/renew/revoke boundary, deriving node owner and
operator authority from distinct verified mTLS identities; it intentionally has
no HTTP promotion operation. `primarylease.Renewer` can supervise one live
primary Guard outside the writer and fails closed when its supervisor stops,
but it is not election or replication confirmation. These are deliberately
composable control-plane building blocks, not a leader-election service:
deployment still owns authenticated controller routing, member lifecycle, clock
discipline, certificate rotation and the tested failover procedure. Promotion
stays closed without independent follower-history proof and makes no zero-RPO
claim; see [primary lease](primary-lease.md).

`docs/replication-protocol.md` fixes the transport-neutral v1 frame contract:
strict `hello`, `batch`, `ack` and `resync_required` envelopes bind a source
database identity, negotiate a bounded frame size and preserve the rule that an
acknowledgement follows durable target application. It is deliberately a codec
and protocol contract, not an unauthenticated listener or a claim of failover.

Under that API, a named `DurableCommitConsumer` persists a monotonic
acknowledgement in the private System tree, reopens from that exact position and
caps Commit Log pruning at the least acknowledged consumer. Its acknowledgement
changes only the physical COW generation, never creates a logical Commit Log
record, so it cannot form a self-consuming stream. Removing that retention pin
is explicit; closing the process-local stream alone does not discard its durable
checkpoint. The current historical replay source also bounds a full caller buffer by
`ReplayDeliveryTimeout`; it ends
with `ErrSlowConsumer` and releases its durable replay lease instead of pinning
the Commit Log indefinitely. Live views and historical replay share the
same `MaxReactiveViewDocuments`/`MaxReactiveViewBytes` resource admission; a
new or growing view that exceeds either terminates with `ErrResourceLimit`
rather than retaining an unbounded matching set.

The first view on an otherwise inactive collection scans a pinned storage
snapshot outside `db.mu`. Before it is registered, the hub reacquires the read
boundary and proves the database token is unchanged; a concurrent commit causes
a bounded rebuild and persistent contention uses the established lock-held
path. Existing view groups use the same bounded snapshot-and-token
handoff when a full recompute is needed: every view and the shared insertion
order are swapped while that token is still protected, then subscriber delivery
happens after the lock is released. A concurrent commit therefore retries the
scan rather than observing a partially rebuilt view. This removes normal cold
and warm scan write pauses without creating a snapshot/watch registration gap;
continuous contention retains the conservative lock-held path.

### Phase D — reactive influence routing

The current canonical-query sharing is retained. Optimize only with measured
evidence: index candidate views by collection and safe predicate/order
signatures, compute one immutable shared delta per affected view, then apply
connection-specific policy projection at the edge. Any uncertain routing result
must evaluate/rebuild the view rather than suppress a possible change.

## Explicit non-goals

This phase does not add sharding, distributed transactions, offline conflict
resolution, arbitrary JavaScript query execution, MongoDB wire compatibility or
unbounded subscriptions. It first makes the single-node write, change and
recovery path measurably strong enough to support those decisions later.
