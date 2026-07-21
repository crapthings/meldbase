# Architecture

## Product contract

Meldbase is an embedded, single-node document database whose distinguishing
feature is that a query can remain live after its initial result. It owns its
file format, query semantics, change tokens, and network protocol. Familiar
operator names are an ergonomic influence, not a compatibility promise.

The dependency direction is:

```text
server / SDK
    -> core reactive query coordinator
        -> planner and execution
            -> transactions and catalog
                -> indexes and record store
                    -> pager and Commit Log
```

Lower layers never import server, transport, or UI concepts. A committed change
is the single source for storage visibility, index maintenance, and live events.

Replication and failover remain above this dependency chain. The core can emit
durable, ordered change history and enforce a locally checked primary write
fence, but it does not elect leaders or make asynchronous replication lossless.
The optional primary-lease integrations provide fenced controller primitives;
they require deployment-owned quorum membership, renewal, identity, clock and
promotion-readiness policy. A promotion without independently proven follower
history is refused rather than presented as an HA success.

Durable RPC idempotency follows the same dependency rule. The transport owns
method authorization and wire envelopes, while the core owns a private COW system tree
and compare-and-set record transitions. Internal idempotency state is never a
user collection or a reactive query source. A durable claim is published before
application code starts; an interrupted pending claim becomes outcome-unknown
and is never automatically stolen. See
[`rpc-idempotency.md`](rpc-idempotency.md) for the crash contract.

Transactional RPC is a distinct opt-in registry. Its point-write view runs on an
immutable snapshot without holding the writer lock, then validates the base
token and publishes business roots plus the terminal System record in one COW
generation. Contention is a durable conflict, not an implicit method retry.
Handlers may not hide external side effects inside this contract.

Server JavaScript uses a separate trusted worker process and a language-neutral
WebSocket control protocol. A private worker hub dynamically resolves methods
and data-only query publications; it cannot bypass client RPC authorization or
publish documents directly. Go predeclares every worker-managed collection,
intersects Worker constraints with the local Authorizer and owns projection,
visibility overlays and disconnect revocation. Transaction point operations
execute against the Go-owned optimistic snapshot, preserving the same atomic
terminal contract as local Go methods.

Both public realtime and private Worker control protocols use an opt-in,
bounded compatibility descriptor before application frames begin. Versions and
capabilities are canonical fixed arrays: unknown capabilities are additive,
while a missing required capability fails before work is sent. Current clients
require the descriptor. Discovery runs only on authenticated control/ticket paths and never enters storage,
reactive publication, or commit hot paths.

Authorization semantics that depend on another collection use an explicit
commit-ordered invalidation, not a best-effort WebSocket event. The business
write, RPC terminal and random policy generation are published under one
Catalog/meta root. Only after durability does Go rotate the publication lease,
and it does so before publishing the business ChangeBatch. Existing subscribers
therefore resync before observing output under stale visibility; restart reloads
the generation from the same private System tree.

Observability follows the same dependency rule. The root database exposes only
a fixed-schema `DB.Stats()` snapshot and always-on bounded counters. The optional
`admin` package samples it, derives rates, retains a fixed history, serves a
separately authenticated SSE stream and can embed the dependency-free developer
dashboard. Admin consumers never enter the business reactive pipeline, and slow
consumers overwrite their own pending telemetry instead of backpressuring
commits.

The sampler also derives a fixed health state from engine-owned state and
adjacent counter deltas. Database fail-stop state and reactive queue capacities
come from the allocation-free core snapshot; transient overflow, slow-consumer,
durability, telemetry and transport signals are evaluated only across the same
process session. The result is a bounded enum/boolean contract, not an alerting
callback and not a source of dynamic metric labels.

The transport consumes a narrower allocation-free `OperationalState` contract
for orchestration probes. Liveness never consults the database; readiness
requires both readable and writable state. A durability fail-stop therefore
keeps diagnostic liveness and committed reads possible while removing the
instance from general traffic. Probe responses expose fixed booleans only and
never reuse detailed admin telemetry on the public listener.

Detailed diagnostics are a second, explicitly enabled layer. A small inlinable
fast path loads an atomic session pointer; when absent it does not read a clock.
An active session records only sanitized fixed-schema query/commit events into a
fixed ring. Admin readers paginate by diagnostic sequence and reset on a new
session identity; neither the ring nor its transport can retain application data.

Prometheus exposition is another cold consumer of the immutable admin sample. It
has a fixed `meldbase_` schema and only engine-owned enum labels. The separate
`integrations/otel` package consumes the same sample through caller-owned stable
Metrics API interfaces; it does not construct an SDK/exporter or describe the
embedded engine as a database client. The core database package and normal admin
sampler import no OpenTelemetry package.

Full runtime traces and CPU/heap profiles are a separate explicit admin-library
operation. Captures have hard time/byte limits, share a process-global exclusion
slot and write only to a caller-owned destination. They are not exposed by the
default admin HTTP handler and never run from storage or query hot paths.

Resource admission is also a core contract, not a transport-only defense. Every
memory and durable write applies the same immutable document-byte,
transaction-byte and transaction-change limits. Bytes mean the deterministic
canonical typed encoding, so accounting cannot drift with JSON spelling, Go heap
layout or compression. Rejection occurs before durable publication and is
reported through a fixed aggregate counter; it never exposes collection or
document identity.

Commit history is governed by two independent logical budgets: commit count and
canonical encoded bytes. The latter prevents a small number of unusually large
commits from defeating the count window. The writer evaluates both while the
business Commit Log tree is mutable, but replay leases cap the deletion
watermark. Byte accounting is derived from persisted inline/overflow descriptors
on open and then advanced only after a successful Meta publication.

Page allocation has a separate physical high-water quota. The allocator
consumes epoch-safe reusable pages first and checks the quota before increasing
`nextPage`; a rejection therefore performs no file write and is not a durability
failure. Real write/fsync/Meta errors retain fail-stop semantics. The quota does
not pretend that logical pruning immediately shrinks a COW file: reclamation
makes unreachable pages reusable, while compaction is the explicit shrinking
operation.

## Decisions that supersede the initial sketch

### A closed value model

The engine does not store `any` inside a tagged `Value`. Each kind has a checked
constructor and private representation. This makes invalid states unrepresentable
at the storage, comparison, and index-key boundaries. Loose Go values are accepted
only through an explicit conversion function.

Object field order is not semantically significant. Numeric values retain their
integer or floating-point kind. Cross-kind numeric comparison is specified by the
query layer rather than accidentally inherited from Go or JSON.

### Query language, not MongoDB emulation

Filters are compiled to an internal expression tree before execution. Unknown
operators and malformed operands are errors. We document every operator and do
not inherit undocumented edge cases from another product. This leaves room for
first-class live-query and local-first operators later.

### Commit and recovery

Each logical write publishes immutable tree pages, a DatabaseRoot and the
inactive Meta in one COW generation; the Commit Log is part of that same atomic
root. There is no sidecar WAL, deferred checkpoint or second storage engine.

Public `RunWriteTransaction` uses the same `DocumentTransaction` publication
primitive as transactional RPC. Its callback reads a pinned immutable root and
maintains a point overlay without holding the database writer mutex. Commit
validates an exact point read set inside the same storage write closure that
publishes the mutations. Every read or touched `(collection, document ID)` pins
expected existence and the hash of its canonical stored body. A related insert,
update or delete returns `ErrWriteConflict` without retrying callback code;
intervening commits to disjoint points may serialize before this transaction and
do not cause false conflicts. Effective mutations across collections,
Primary/Order/Secondary roots and one Commit Log batch share one Meta publication
and one reactive token. An index created after the callback snapshot but before
commit is included when the transaction builds its current Secondary mutations.

The point overlay is admitted while the callback runs, not only at commit:
tracked points are bounded by `MaxTransactionChanges`, and retained immutable
base plus current canonical document bytes share `MaxTransactionBytes` with
staged private System mutations. Replacing the same overlay value adjusts its
charge instead of accumulating it. `WriteTransaction.Find` scans one pinned
snapshot plus the callback overlay and records a persistent
`CollectionPrecondition` containing the collection identity and
`UpdatedSequence`. The storage transaction validates that fence atomically with
point reads and writes, so a concurrent document, collection creation or
published-index change cannot create a phantom. The initial collection-wide
fence is intentionally conservative; a later index-range fence may reduce
false conflicts but must preserve this serializability contract.

Open freezes a non-sensitive `RecoveryReport` after storage validation and
before the DB is returned. It describes selected Meta/root redundancy, removed
provable crash tails and optional allocator degradation. It never turns
ambiguous corruption into recovery. An in-process durability failure remains
fail-stop until close/restart; restart re-enters the complete format validation
and graph-validation path rather than resetting an error bit.

### Stable live-query boundary

Subscriptions consume ordered commit batches rather than observing collection
methods directly. This prevents events for failed transactions and allows the
same log position to support reconnect, audit, and future replication. The
server accepts a replay source that reconstructs Snapshot N and tails N+1; when
none can prove that contract it emits `resync_required` instead of presenting a
fresh snapshot as resumed history.

### Storage format boundary

`Open` creates or opens the sole current copy-on-write format. A missing/empty
path creates it; an unknown non-empty file fails closed. Historical alpha
formats require offline export/import and are never implicitly migrated.

The current copy-on-write page engine persists primary records, per-collection
secondary-index catalogs and the
logical Commit Log under one CatalogRoot publication, then provides exact
historical query replay to the server. Opening reads only collection and index
catalog metadata; it does not scan or decode Primary records. Index candidates
are checked against their resolved Primary document when read. Explicit
Reachability performs exhaustive Primary↔Order and Secondary-position checks.

Public queries pin one immutable DatabaseRoot and read Primary/Secondary
trees directly. `_id` lookup resolves one primary record; equality/range plans
stream a Secondary iterator and resolve matching primary records; unsorted
collection scans stream the persistent Order tree and resolve Primary records
under the same root. Secondary suffixes contain insertion position before
DocumentID, preserving the public tie breaker. A byte-and-entry-bounded decoded-document LRU
validates the current encoded record on every hit, while the immutable page cache
remains the lower-level IO cache. The engine does not rebuild process-local query
B+Trees or a decoded-document collection mirror on open. Update/Delete selection,
CreateIndex construction, SnapshotQuery and Reactive View initialization/resync
all consume pinned storage snapshots. Live CRUD retains only collection/index
definitions in the process-local catalog.

The compatibility `CreateIndex` path separates snapshot extraction from publication. Key
extraction runs against a pinned immutable root without holding the database
writer mutex; if a concurrent commit advances the source sequence, the build
discards that private result and retries a bounded number of times. The final
storage transaction revalidates Primary↔Order ownership and atomically attaches
the index catalog entry. Until that commit, queries and CRUD cannot observe the
candidate tree. A sorted bulk loader seals leaves page-by-page and retains only
the current leaf plus the branch frontier, avoiding the former second key copy
and complete mutable-node graph. This sorted-stream→private-tree→atomic-catalog
boundary is intentionally suitable for a later external merge source.

For write-heavy databases, `StartIndexBuild`/`ResumeIndexBuild` negotiate
`RequiredFeatureShadowIndexBuilds` and persist a separate BuildCatalog edge from
DatabaseRoot. Bounded maintenance generations advance the protected Primary
cursor and private Secondary root without advancing the logical sequence. The
build's applied watermark is also a persistent Commit Log retention lease.
After strict catch-up, finalization moves the already-built root into
IndexCatalog, removes the build record and appends its catalog event in one COW
commit. Cancellation preserves progress; explicit abort releases it. See
`docs/index-builds.md` for states and limits.

An optional per-database scheduler runs builds in deadline-bounded quanta with
fixed concurrency. It is outside the storage engine and default-off. Terminal
semantic failures are persisted as bounded enums; failed builds stop pinning
Commit Log history but retain their private graph until operator abort.

Physical B+Tree nodes split by exact encoded bytes rather than entry count.
Path-copy deletion merges fitting siblings and can propagate a new split upward
when a replacement separator grows beyond the page budget.

`DetectStorageFormat` distinguishes a missing/empty path from the current format
without mutating it; an unknown non-empty file fails closed.
`InspectStorageFormat` checksum-validates both Meta slots and reports the newest
revision/features/identity without acquiring the writer lock. Its compatibility
bit is a negotiation result, not a substitute for structural validation.
Normal open validates the selected Meta/root and required catalog metadata,
not every protected page. `OpenOptions.RequireGraphAudit` is the explicit startup
mode that walks both protected graphs before serving; it is structural only and
is intentionally opt-in because its cost grows with database size.
`VerifyFile` occupies the explicit offline layer between them: a shared-lock,
read-only audit reuses normal Meta selection, walks both protected business
graphs, proves published Secondary-to-Primary key correctness and
Primary-to-Secondary completeness from canonical stored documents, reports
optional FreeSpace degradation independently, and hashes the complete file
without repair or maintenance side effects. This semantic work is excluded from
normal open, read, commit and reclamation paths.
The current Meta magic, full-page checksum envelope and checksum position are a stable
negotiation boundary. A checksum-valid future revision or unknown required
feature fails explicitly as unsupported. When one Meta slot is torn or
checksum-invalid, recovery selects the other valid slot. Unknown optional bits
are carried without becoming recovery authority.
Format extensions use additive golden evidence. The applied shadow watermark
extension is pinned as a page-delta over the immutable business fixture: base,
patch and reconstructed-file digests are all checked, and the reader rebuilds
the artifact without running the current writer. This preserves historical
fixtures while avoiding duplicated full database blobs.
The nine B+Tree branch PageTypes are additionally frozen in a compact corpus
produced by the real sorted-tree builder. A coverage gate joins codec corpora
with openable database fixtures and rejects any PageType that has no revision-3
artifact. Codec evidence and reachable full-graph compatibility remain separate
claims: the former catches byte/type drift; the latter must also prove traversal,
recovery and future-writer advancement.
Unsorted COLLSCAN cursors are lazy and own a bounded snapshot pin until
exhaustion, limit completion, error, context cancellation or explicit `Close`.
Indexed and sorted plans retain the correct materializing execution path. Manual
compact-to-new-file and explicit epoch-safe page reuse are available. The
disk-full publication matrix and configurable large-database churn/reopen/reclaim
soak gate the default path; broader platform durability testing remains release
work. Page reuse protects both valid meta roots and every
pinned reader/replay root. Blocking reclamation remains available, while online
reclamation walks a duplicated read handle without holding the writer mutex and
installs candidates only if the Meta generation, physical high-water mark and
prior free pool remain unchanged. Concurrent commits cause bounded full-scan
retries instead of unsafe merging. An explicit default-off scheduler runs these
online audits serially with per-run deadlines and stops with the DB. The resulting candidate extents may be published
as optional immutable FreeSpace metadata without advancing the logical Commit
Log. Restart recovery filters the allocator's consumed high-ID suffix by page
generation; invalid acceleration resets to an empty pool, never to an
unverified allocation. Historical files are never silently reinterpreted.
The scheduler defaults to a memory-only O(1) pool installation; physical
FreeSpace publication is explicit because extent construction and fsync remain
a writer-lock maintenance step.

Physical backup is separate from logical compaction. `Backup` holds the source
writer boundary, copies and hashes the exact page-aligned file, performs physical
reachability plus public reopen verification, and publishes through a synced
no-overwrite destination link. It preserves database identity, Meta generation
and Commit Log history, so only one descendant may be writable in that identity
domain. `Compact` rebuilds one pinned logical snapshot under a new identity
when an independent fork is intended. It serializes with another compaction and
with close, but releases the database writer immediately after snapshot
admission; commits that follow that boundary remain in the source only. Both
operations expose only fixed aggregate observability fields.

## Concurrency model

The database transaction manager has one writer, snapshot readers and immutable
committed roots. User callbacks are never invoked while a storage lock is held.

## Package boundaries

- Root package `meldbase`: public embedded API and stable value types.
- `admin`: optional bounded sampler, secured admin HTTP/SSE adapter and embedded
  developer dashboard, plus fixed-schema Prometheus exposition.
- `internal/storage`: page IO, COW trees, checksums, allocation and record identifiers.
- `internal/index`: ordered key encoding and B+Tree implementation.
- `core`: public database API, query execution, committed batches and reactive views.
- `server`: public authenticated HTTP/WebSocket/RPC facade.
- `internal/server`: transport implementation and security machinery.

Packages are introduced when they have executable behavior; empty architecture
folders are deliberately avoided.

## Extension rules

Meldbase is intentionally opinionated about where a new capability belongs.
These rules preserve a small core surface as the product evolves:

- **Identity stays outside the engine.** The server verifies a actor with a
  subject and active workspace; account records, credentials, roles, membership
  changes and token issuance remain application concerns. A collection is not
  made special because it happens to contain users.
- **Generic data access stays finite and declarative.** The collection-access
  manifest has collaborative, owner and RPC-only surfaces. A proposed mode is
  valid only when it compiles to the existing server-enforced query/insert/
  update/delete policy algebra. Do not add modes for roles, approvals, billing,
  sharing or membership: use an application `Authorizer` or a named RPC.
- **Dynamic policy is a narrowing layer, never a second write engine.** Worker
  publications and `QueryPolicyResolver` may narrow reads and subscriptions.
  Role-dependent writes use a Go `Authorizer` or an explicitly authorized RPC;
  they must not be inferred from a read publication.
- **Transport does not create business semantics.** HTTP and realtime consume
  the same typed query, mutation, policy and RPC contracts. A transport-specific
  convenience API must not create a second authorization, retry or consistency
  rule.
- **One current storage contract during alpha.** A format or policy change may
  replace an internal alpha design directly, with new release guidance and
  evidence when needed. Do not retain dormant runtime fallbacks solely to
  preserve an unshipped implementation.

These rules are also the review bar for generated or agent-authored changes:
prefer extending an existing typed contract and its tests over adding a parallel
callback path, configuration dialect or client-only security check.
