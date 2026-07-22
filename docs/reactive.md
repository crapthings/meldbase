# Reactive queries

A live query is an initial deterministic query result plus later revisions driven
by committed change batches. Internally, each revision carries a monotonically
increasing commit token. The public compatibility API can emit full snapshots;
the delta API emits one initial snapshot and later ordered transformations.

Canonical queries share one Reactive View keyed by collection and effective
`QuerySpec`. Before/after document images update persistent ordered membership
incrementally; full recomputation is an observable recovery path for queue overflow or
explicit resynchronization. One shared immutable delta is generated per changed
view revision and then cloned only at public mutable-document boundaries.

Commit Log updates retain a sorted, deduplicated `ChangedPaths`
set whenever the mutation compiler can prove it; watcher and replay boundaries
copy that set with their document images. Missing metadata deliberately means
"whole document may be affected." It is not itself a query-delta routing rule:
this API returns whole documents, so changing an unfiltered field of a matching
row is still visible. Future projection or policy routing may use the metadata
only with an independently proven visibility contract.

Delivery is ordered per subscriber. Slow consumers are disconnected with a
resumable error rather than allowed to grow memory without bound. Commit
publication only appends to a bounded central queue (1,024 batches, 8,192
logical changes, and 64 MiB of canonical document-image payload) and never
waits for a client. The downstream shared reactive hub independently applies a
64 MiB canonical-image limit while it updates or rebuilds views. These byte
budgets are independent of configurable transaction limits, so an application
cannot accidentally turn realtime backlog into an unbounded memory sink.
Crossing either queue's bound fails direct watchers and rebuilds reactive views
from an atomic snapshot rather than retaining an unbounded backlog. Direct Go
change watchers use their own asynchronous queue: each watcher is capped at
64 MiB and all direct watchers together at 128 MiB of canonical document-image
payload. A watcher that crosses either direct-watcher limit ends with
`ErrSlowConsumer`; it never delays or rolls back a committed write.

Initial registration and snapshot publication share the database read boundary,
closing the subscribe/snapshot race window: a subscriber receives Snapshot N and
then only revisions newer than N. If the bounded reactive queue overflows, its
pending work is discarded and affected collections are explicitly rebuilt from a
new atomic snapshot instead of attempting an incomplete delta chain.

A delta transforms exactly `FromToken` into the greater `Token`. Its ordered
operations are remove, add-before, move-before, and change. Application validates
all IDs, documents and anchors and never mutates the input snapshot. This format
preserves sort, skip and limit window changes without sending the complete query
result after every commit.

## Durable collection changes

Meldbase provides a Go pull/acknowledge feed for work that must survive a process
restart: `CreateDurableCollectionChanges` creates a named consumer and
`OpenDurableCollectionChanges` resumes its stored checkpoint. A consumer receives
one `DurableChangeBatch` per global Commit Log token; a batch may contain no
documents when that token concerns another collection, and must still be `Ack`ed.
This makes retention and recovery explicit: acknowledge only after an external
side effect is durable, and handle `ErrHistoryLost` by resynchronizing rather
than assuming an unseen gap is safe.

The projection contains only document inserts, updates and deletes for the named
collection. It deliberately excludes private System records, raw page bytes and
collection lifecycle. It is an application outbox/CDC building block, not yet a
follower or physical-backup wire protocol.

For database archive or follower construction, use
`CreateDurableDatabaseChanges` instead. Its batches include `create_collection`,
`create_index` and document events in Commit Log order; it still omits private
System records. `BeginArchive` creates a durable database checkpoint before it
writes and verifies a physical backup. The returned `SnapshotToken` is the
handoff: persist and verify the backup, consume/ack all batches through that
token without reapplying them, then apply later batches in order. A network
transport and remote follower are intentionally outside this local API.

`OpenFollower` and `Follower.Apply` provide the local target state machine
for that later transport. The follower stays queryable/reactive but rejects
ordinary writes. It applies only the next durable database token, atomically,
and rejects duplicates or gaps with `ErrReplicaSequence`.

## Browser delivery

Browser subscriptions are a separate transport boundary. Resume tokens,
policy-lease revocation, snapshot and delta modes, and visibility overlays are
specified by the [client protocol](client-protocol). This runtime page defines
the committed-change source that those transport rules consume.
