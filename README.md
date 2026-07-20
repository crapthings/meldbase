# Meldbase

> Documents that stay live. Local by design.

Meldbase is an experimental, embedded reactive document database written in Go.
It combines a typed document model, local durable storage, query planning, and
live query subscriptions behind one coherent API. It is a new database—not a
MongoDB protocol or behavior clone.

The current implementation contains a typed Go engine, crash-recoverable
copy-on-write storage with a durable Commit Log, ordered compound B+Tree indexes, shared
incremental reactive views and deltas, a Go/TypeScript query contract, and a
secured HTTP/WebSocket reactive server. `Open` creates new databases with the
single-file V2 engine and detects existing V1/V2 files without migration.
`OpenV1` remains available for deliberate legacy-format creation.

It is still early-stage: the V2 Meta negotiation envelope fails closed on
unsupported revisions/features, and byte-exact cross-release fixtures now pin
all current revision-3 PageTypes in a reachable multi-level business graph.
Migration of existing V1 databases is explicit, and broader filesystem/platform
durability evidence remains incomplete. B+Tree deletion uses local borrow/merge, while V2
physical nodes split by encoded bytes and expose structural write counters. New
default databases use one main file; legacy V1 databases retain their `.wal`
sidecar. V1 bounds that WAL with a default 64 MiB/10,000-commit automatic
checkpoint policy; V2 checkpoints every COW commit directly.
V2 also retains a bounded 10,000-commit / 256 MiB logical replay window by
default, while active replay leases safely pin required history and expose
retention pressure.
All engines enforce canonical document/transaction resource limits before
publication. V2 point transactions additionally admit tracked entries and
retained base/current overlay bytes while the callback runs, preventing a
read-heavy callback from growing unbounded before commit.
V2 additionally has an 8 GiB default physical high-water quota; safe reusable
pages are consumed first and crossing the quota rejects before file I/O without
poisoning the database.

## Installation

The Go module is published from:

```sh
go get github.com/crapthings/meldbase@latest
```

Before placing production-like data on a target volume, run the non-destructive
capability probe in that directory:

```sh
go build -o /tmp/meldbase-qualification ./cmd/meld
/tmp/meldbase-qualification durability-check \
  --dir /path/to/database-volume \
  --source-revision "$(git rev-parse HEAD)" \
  --require-clean-source
```

It creates an isolated temporary directory, checks file and directory `fsync`,
exclusive advisory-lock conflict and close-release behavior, atomic
no-overwrite hard links, same-directory rename, and a real indexed V2
commit/reopen followed by offline full-graph verification. It prints schema-2
JSON and removes the probe directory. Passing proves those APIs work in the
current mounted environment; it does not prove controller behavior during power
loss. See [filesystem qualification](docs/filesystem-qualification.md).

Release candidates use an explicitly built, clean-revision binary for the
workload-independent probe and the concurrent storage soak:

```sh
go build -race -o /tmp/meldbase-qualification ./cmd/meld
/tmp/meldbase-qualification storage-soak \
  --dir /path/to/database-volume \
  --out storage-soak-receipt.json \
  --profile release --seconds 14400 --documents 10000 --reopens 12 \
  --source-revision "$(git rev-parse HEAD)" --require-clean-source
```

The schema-4 soak receipt binds the actual race-enabled binary revision and
dirty flag to the exact target volume, then proves concurrent writes, snapshots,
shadow-index catch-up, reclamation, repeated reopen and final offline semantics.
Its four-hour release floor applies to measured concurrent worker time; reopen
and verification overhead is reported separately and cannot satisfy the floor.
The release writer runs flat out only until it proves one real optimistic
reclamation conflict, then uses a hardware-independent cadence of one write
every ten seconds. This keeps the duration/recovery qualification inside the normal V2
physical safety quota; it is not a storage-throughput benchmark.
Shadow-index catch-up coalesces up to thirty seconds of ordered Commit Log work,
then drains any larger backlog in bounded batches. This keeps the soak focused
on realtime recovery behavior instead of synchronously mirroring every write
with another COW commit.
Sanitized 30-second stderr heartbeats expose phase progress and aggregate work
without changing the canonical receipt or revealing database paths and values.
`meld qualification-check` binds it to the schema-2 capability receipt. This is
Level 3 evidence; real ENOSPC and power-cut qualification remains separate.
The first Level 4 runner, `meld destructive-process-check`, now sends a real
`SIGKILL` to a separate writer and produces an append-only-oracle receipt with
old/new logical-state hashes, lock reacquisition and full offline verification.
The Linux-only `destructive-volume-check` and `destructive-enospc-check` pair
adds a token-gated, non-root, independently mounted disposable-volume runner
that reaches every V2 publication boundary, fills the real filesystem through
kernel `fallocate` until `ENOSPC`, kills the blocked writer and verifies recovery.
`destructive-qemu-reset` captures the QMP block inventory, exact hard-reset
command/response and host-originated RESET event. `destructive-power-prepare`
and `destructive-power-recover` bind that proof to the exact boundary marker,
Linux boot-ID change and untouched crash image. `destructive-manifest-build`
re-verifies all retained artifacts and
assembles the strict Level 4 record. The reference
[`meld-power-redfish-adapter`](docs/redfish-power-adapter.md) performs a
target-bound `ForceOff`/observed-Off/On/observed-On cycle and embeds its
redacted hardware log in the signed physical-controller proof. Level 4 storage qualification still
requires actually executing the complete external power-cut boundary matrix
described in [filesystem qualification](docs/filesystem-qualification.md), and
sets only `storageQualified=true`. A production claim requires the
Level 5 gate, which additionally verifies the complete signed rollback-anchor
phase chain and a separate multi-agent concurrent history, including clean
release provenance for every controller and execution agent.
Production Level 5 output is an independently signed, no-overwrite release
envelope. `qualification-packet-verify` requires all original evidence and
recomputes the qualification instead of trusting the archived boolean or its
signature alone. Destructive evidence is rooted in a machine-generated
content index for the exact secured artifact tree; added, removed, substituted
or symlinked files fail Level 4 and Level 5 verification. A strict Linux
environment capture additionally binds the target mount, kernel, block-device
and cache-policy chain, external controller method and indexed operator
authorization to the same campaign. Final verification uniquely locates every
original destructive receipt from that index, reruns its retained-artifact
checks and reconstructs the manifest rather than trusting its summary. The
complete secured tree can be relocated without changing receipt bytes; indexed
digests plus unique longest relative-path suffixes safely rebase old absolute
artifact references.
For the stronger VM-process loss case, `destructive-qemu-process-kill` verifies
direct-I/O target binding, terminates QEMU with a real host `SIGKILL`, captures
its wait status, and boots a new paused QEMU process before publishing evidence
and continuing recovery.
Power-matrix aggregation rejects repeated boot transitions and mixed controller
methods, so reset and whole-process-loss evidence cannot be combined to satisfy
coverage.
`destructive-corruption-check` complements the crash matrices with a
deterministic silent-bit-flip campaign over Meta, page header, payload, checksum
and page-tail offsets. Every successful verification must still be a complete
published generation with a full index semantic audit; the retained receipt can
be independently replayed against the exact source database.
The QEMU `blkdebug` EIO runner adds a distinct kernel-visible media-error case:
it maps the seeded database's real ext4 sectors, injects write-only `EIO`, binds
QMP `BLOCK_IO_ERROR` events to the exact config/image pair, and proves that the
first write fails, subsequent writes remain poisoned, reads remain available
and offline recovery preserves the previously published generation.
The separate flush-state runner uses a durable guest `ready` → host QMP `arm`
→ guest transaction handshake, then injects one `flush_to_disk`/flush `EIO` on
an ext4-backed virtio device. Its retained proof records both the exact injected
rule and QMP's actual operation classification, and binds old-generation
recovery to the seed and handshake receipts. This verifies handling of a
reported persistence-barrier failure; it does not claim that successful flushes
are truthful or cover stale reads.
An explicit volatile-overlay negative control covers that distinction: it
acknowledges a commit in a QEMU temporary layer, kills QEMU with `SIGKILL`,
proves the durable base image never changed, and reboots without the layer. The
harness must classify the resulting sequence rollback as unsafe. Public V2
opening supports a synchronous external rollback anchor; a stale but
checksum-valid generation is rejected before the database file is modified:

```go
anchor, err := meldbase.NewFileRollbackAnchorStore("/trusted/app.anchor")
if err != nil {
	return err
}
db, err := meldbase.OpenV2WithOptions("/data/app.meld", meldbase.V2Options{
	RollbackProtection: meldbase.V2RollbackProtection{
		AnchorStore:      anchor,
		InitializeAnchor: true, // audited first provisioning only
	},
})
```

Subsequent opens omit `InitializeAnchor`. Each logical commit or acknowledged
maintenance generation first makes the database durable, then atomically
advances and reads back the independent identity/sequence/generation anchor,
and only then reports success. An anchor failure or the default 10-second
anchor-operation timeout disables further writes. Failure is an ambiguous
outcome—the anchor may already have advanced before its response was lost—so
callers must not retry business logic as though the commit were absent. Custom authenticated remote
quorum or TPM-backed monotonic stores implement the context-aware
`RollbackAnchorStore` contract.
The anchor must be on independently trusted storage; placing it beside the
database cannot detect whole-device rollback.

[`integrations/anchorhttp`](integrations/anchorhttp) provides a reference
HTTPS/HMAC majority-read/majority-write store for one or an odd `2f+1` static
set of independent nodes. Its fault tests cover stale minority state, one-node
partition, duplicate advance, loss of quorum, authentication tampering, timeout,
rejoin and rejection of a rolled-back database. It is crash-fault tolerant, not
Byzantine-fault tolerant; safe dynamic membership remains separate work.
Protocol v2 signs an automatically derived full-membership configuration and
expected member identity, while every node directory is durably bound before
serving. [`meld anchor-serve`](docs/rollback-anchor-service.md) runs a standalone
TLS 1.3 + mTLS member with private-file enforcement and graceful shutdown.
The same guide documents the five-phase `meld anchor-qualification` workflow,
whose Ed25519-signed receipt chain binds full-member probes, offline database
verification and external network-controller evidence.
It also documents `history-agent`/`history-run`, which execute a concurrent plan
through independently signed, mTLS-authenticated and durably journaled agents,
plus `history-sign`/`history-verify`, which bind the resulting controller
ordinals, stable final member state and recomputed linearizability result into a
separate signed receipt.
The [static safety model](docs/rollback-anchor-formal-model.md) separately
enumerates quorum publication/recovery behavior and documents the bounded TLA+
specification, concurrent scheduler, multi-process history checker and their
fault-model boundaries.

Upgrade automation can inspect the newest checksum-valid Meta envelope without
opening or locking the database:

```sh
go run ./cmd/meld inspect --db app.meld --require-compatible
```

The JSON reports V1/V2, revision, generation, commit sequence, required and
optional feature bits, database identity, valid Meta slots and whether this
reader is compatible. A checksum-valid future V2 revision is reported rather
than misclassified as corruption; the compatibility gate exits with an error
after emitting the JSON.

Before restore or archival promotion, perform the more expensive offline audit:

```sh
go run ./cmd/meld verify --db app.meld2 --timeout 10m
```

`verify` takes a shared advisory lock, walks the complete business graph protected
by both valid Meta roots, and recomputes published secondary indexes against their
canonical primary documents in both directions. Durable shadow builds are checked
through their scan cursor or exact applied snapshot as appropriate. It also
validates persistent FreeSpace separately and hashes every file byte. It emits a
schema-versioned JSON receipt and never creates, truncates, repairs, reclaims, or
advances the database. An active writer makes it fail with `ErrDatabaseLocked`.

The TypeScript packages currently live in this pnpm workspace and are not yet
published to npm. Use the repository workspace for the developer preview; do not
assume `@meldbase/client` or `@meldbase/react` is available from the public npm
registry until an npm release is announced.

## Go quick start

```go
db, err := meldbase.Open("app.meld")
if err != nil { log.Fatal(err) }
defer db.Close()

users := db.Collection("users")
id, err := users.InsertOne(ctx, meldbase.Document{
  "name": meldbase.String("Ada"),
  "age":  meldbase.Int(30),
})

err = users.CreateIndex(ctx, "users_age", []meldbase.IndexField{
  {Field: "age", Order: 1},
}, meldbase.IndexOptions{})

// V2 and memory support one to four fields with independent directions.
err = users.CreateIndex(ctx, "users_name_age", []meldbase.IndexField{
  {Field: "name", Order: 1},
  {Field: "age", Order: -1},
}, meldbase.IndexOptions{Unique: true})

birthday, err := meldbase.CompileUpdate(meldbase.Update{
  "$inc": map[string]any{"age": 1},
})
if err != nil { log.Fatal(err) }

// Storage V2 atomically publishes point writes across collections. Meldbase
// never retries this callback when another commit wins first.
err = db.RunWriteTransaction(ctx, func(tx *meldbase.WriteTransaction) error {
  if err := tx.UpdateOne("users", id, birthday); err != nil {
    return err
  }
  _, err := tx.InsertOne("audit", meldbase.Document{
    "userId": meldbase.ID(id),
    "event":  meldbase.String("birthday"),
  })
  return err
})
```

Compound queries use a contiguous left prefix: equality fields followed by at
most one range field. A missing first field omits a document; a missing suffix
is represented internally so prefix queries remain complete without creating a
unique-key conflict. Uniqueness applies only to complete tuples. Legacy V1
deliberately rejects compound and descending definitions.

`RunWriteTransaction` reads one immutable V2 snapshot, supports `GetOne`,
`Find`, `InsertOne`, `ReplaceOne`, `UpdateOne` and `DeleteOne`, and publishes
all effective changes under one commit token. `Find` uses the same compiled
query syntax and sees earlier writes from its callback. It installs a durable
collection snapshot fence, so any concurrent document or published-index change
to that collection returns `ErrWriteConflict` rather than admitting a phantom.
The first implementation intentionally uses a collection-wide conflict domain;
it is serializable but may conflict for unrelated writes in the same
collection. Candidate count and bytes are bounded by the transaction resource
limits. A callback error or `ErrWriteConflict` publishes nothing; a no-op does
not advance the sequence. Keep callbacks short: do not retain the transaction,
call normal database methods, perform network I/O or create external side
effects. Legacy V1 and the in-memory engine return
`ErrWriteTransactionUnsupported`.

The TypeScript remote client also provides typed request/response RPC. HTTP is
the default for work that does not need a realtime connection:

```ts
const receipt = await client.call("orders.checkout", [orderId, 2n]);

// Optional: multiplex the same envelope over an existing realtime socket.
const liveReceipt = await client.call("orders.checkout", [orderId, 2n], {
  transport: "realtime",
  signal: controller.signal,
});

// Caller-owned retry identity; Meldbase never retries RPC automatically.
const durableReceipt = await client.call("orders.checkout", [orderId, 2n], {
  idempotencyKey: crypto.randomUUID(),
});
```

RPC reuses Meldbase wire values, so `bigint`, Date and binary values do not lose
type information. The public Go `server` package requires both explicit method
registration and a separate RPC authorizer; arbitrary server error text is never
sent to clients. Socket calls are never automatically retried after disconnect.
An explicit V2-backed server store can durably claim an idempotency key and
replay its terminal result; an interrupted claim returns outcome-unknown rather
than rerunning application code. Go methods registered through
`RPCTransactionalMethods` can additionally publish supported point writes and
the successful result in one V2 commit; exact point-read-set contention returns
a durable conflict without rerunning the method, while disjoint commits may
proceed. See
[`docs/client-protocol.md`](docs/client-protocol.md#rpc-calls).

Trusted Node.js business methods can run out of process through
`@meldbase/server`. A separately authenticated worker hub dynamically resolves
ordinary/transactional methods and data-only query publications while Go retains
authorization intersection, typed-value validation, transaction commit, field
projection and reactive publication. Worker credentials are
sent in control headers rather than URLs, and the control handler is intended
for a private listener. See
[`docs/server-js-sdk.md`](docs/server-js-sdk.md).

Run the worker example locally in two terminals:

```bash
export MELDBASE_WORKER_TOKEN=development-worker-token-0123456789abcdef
go run ./cmd/meld serve --db /tmp/meldbase-worker-demo.meld2 \
  --addr 127.0.0.1:8080 --dev-no-auth --worker-addr 127.0.0.1:9092 \
  --worker-publications orders
```

```bash
export MELDBASE_WORKER_TOKEN=development-worker-token-0123456789abcdef
pnpm --filter @meldbase/example-server-worker start
```

Then invoke its transactional method:

```bash
curl -sS http://127.0.0.1:8080/v1/rpc \
  -H 'content-type: application/json' \
  --data '{"v":1,"type":"call","requestId":"demo-1","idempotencyKey":"demo-order-000000000001","method":"orders.create","arguments":[{"t":"string","v":"first order"}]}'
```

The example also owns visibility for `orders`: it exposes only rows whose
`owner` matches the authenticated subject and only its declared result fields.
Go must predeclare `orders` through `--worker-publications`; if the worker is
offline, queries to that managed collection fail closed.

V1-to-V2 migration is explicit and never overwrites its destination:

```go
format, err := meldbase.DetectStorageFormat("app.meld")
if err != nil { log.Fatal(err) }
if format == meldbase.StorageFormatV1 {
  if err := db.MigrateToV2(ctx, "app-v2.meld"); err != nil {
    log.Fatal(err)
  }
}
```

Migration preserves empty collections, document insertion order and index
definitions, but intentionally assigns a new database identity. Existing V1
realtime resume tokens therefore resynchronize instead of crossing formats.

V2 can compact live state into a separately verified file without overwriting
the source. It pins one source snapshot briefly, then copies and verifies it
without blocking later commits; writes that finish after that snapshot are not
included in the compacted file:

```go
if err := db.CompactToV2(ctx, "app-compacted.meld2"); err != nil {
  log.Fatal(err)
}
```

The compacted file also has a new database identity. Lazy V2 COLLSCAN cursors
release their snapshot automatically on exhaustion, limit, error or context
cancellation; callers that stop early should call `cursor.Close()`.

V2 can also create an exact, checksummed physical restore artifact. Unlike
compaction, backup deliberately preserves the database identity, Meta
generation, Commit Log and physical history:

```go
result, err := db.BackupV2(ctx, "app-backup.meld2")
if err != nil {
  log.Fatal(err)
}
log.Printf("backup sequence=%d sha256=%s", result.CommitSequence, result.SHA256)
```

For an offline database, the CLI performs the same validation and emits a
schema-versioned JSON receipt:

```sh
go run ./cmd/meld backup --db app.meld2 --out app-backup.meld2 --timeout 10m
go run ./cmd/meld inspect --db app-backup.meld2 --require-compatible
go run ./cmd/meld verify --db app-backup.meld2 --timeout 10m
```

Restore requires the JSON receipt emitted by `backup` and always writes a new,
previously absent database file:

```sh
go run ./cmd/meld backup --db app.meld2 --out app-backup.meld2 --timeout 10m > app-backup.receipt.json
go run ./cmd/meld restore --in app-backup.meld2 --receipt app-backup.receipt.json --out app-restored.meld2 --timeout 10m
go run ./cmd/meld verify --db app-restored.meld2 --timeout 10m
```

For the complete single-node data-directory check, health probes, dashboard,
backup/restore drill and upgrade runbook, see
[single-node deployment and recovery](docs/single-node-deployment.md).

The destination must not exist. The library blocks source writes for the copy
duration while allowing readers; the CLI must acquire the database's exclusive
process lock, so it is intended for an offline source. A physical backup is a
restore artifact, not an independent writable clone: retire the original before
starting the restored file. Use `CompactToV2` when an independent database with
a new identity and history is required.

V2 reclamation can run as an explicit low-pause maintenance loop. It is off by
default; online scans do not hold the writer lock and discard their result if a
commit changes the audited generation:

```go
maintenance, err := db.StartV2Maintenance(ctx, meldbase.V2MaintenanceOptions{
  Interval:    5 * time.Minute,
  Timeout:     time.Minute,
  MaxAttempts: 2,
})
if err != nil {
  log.Fatal(err)
}
defer maintenance.Stop()
```

Runs are serial, deadline-bounded and stop automatically when the DB closes.
They default to memory-only pool installation so the final writer pause is O(1).
Set `PersistFreeSpace: true` only when restart acceleration is worth an explicit
physical maintenance/fsync step.

Deliberate legacy V1 deployments can tune or disable its synchronous automatic
checkpoint policy:

```go
db, err := meldbase.OpenV1WithOptions("legacy.meld", meldbase.V1Options{
  Checkpoint: meldbase.V1CheckpointPolicy{
    MaxWALBytes:   128 << 20,
    MaxWALCommits: 20_000,
  },
})
```

Either threshold triggers. The triggering business commit is already durable;
checkpoint maintenance does not advance the logical commit sequence.

Startup recovery is explicit and auditable. Normal `Open` performs only bounded
automatic recovery and freezes the result in `db.RecoveryReport()`. Deployments
that require an operator/offline verifier to approve every recovery can reject
before any crash tail is truncated or WAL is replayed:

```go
db, err := meldbase.OpenWithOptions("app.meld2", meldbase.OpenOptions{
  Recovery: meldbase.RecoveryRequireClean,
})
if errors.Is(err, meldbase.ErrRecoveryRequired) {
  // Keep the file untouched; run meld inspect/verify and follow site policy.
}
```

There is no online API for clearing durability fail-stop.

For a production process that prefers a slower startup to serving from a
structurally corrupt deep V2 page, opt into the protected-graph audit. It runs
before automatic crash-tail removal and does not replace the offline semantic
index verifier:

```go
db, err := meldbase.OpenV2WithOptions("app.meld2", meldbase.V2Options{
  RequireGraphAudit: true,
})
```

Write admission and V2 replay history are configured at open and remain
immutable for that handle. Zero fields select the production defaults:

```go
db, err := meldbase.OpenWithOptions("app.meld2", meldbase.OpenOptions{
  V2CommitRetention: meldbase.V2CommitRetentionPolicy{
    MaxCommits: 25_000,
    MaxBytes:   512 << 20,
  },
  // A full replay-consumer buffer is terminated after this interval so it
  // cannot indefinitely pin retained history. Zero selects five seconds.
  V2ReplayDeliveryTimeout: 5 * time.Second,
  V2StorageLimits: meldbase.V2StorageLimits{MaxFileBytes: 16 << 30},
  ResourceLimits: meldbase.ResourceLimits{
    MaxDocumentBytes:      8 << 20,
    MaxTransactionBytes:   32 << 20,
    MaxTransactionChanges: 5_000,
    MaxIndexBuildEntries:  500_000,
    MaxIndexBuildBytes:    128 << 20,
    MaxReactiveViewDocuments: 10_000,
    MaxReactiveViewBytes:     64 << 20,
  },
})
```

Resource bytes are deterministic typed binary sizes, not JSON or Go heap size.
Index-build bytes count each encoded scalar key plus its 8-byte insertion
position and 16-byte document ID. Oversized writes and index builds fail
atomically with `ErrResourceLimit`; the defaults cap one build at 1,000,000
entries and 256 MiB. Online index creation leaves the commit sequence and
durable bytes unchanged on rejection.
The compatibility `CreateIndex` path uses a pinned snapshot without holding the
database writer mutex and retries bounded snapshot conflicts. Storage V2 also
supports durable, crash-resumable construction for write-heavy collections:

```go
id, err := users.StartIndexBuild(ctx, "users_email_created",
  []meldbase.IndexField{{Field: "email", Order: 1}, {Field: "createdAt", Order: -1}},
  meldbase.IndexOptions{Unique: true})
err = db.ResumeIndexBuild(ctx, id) // safe after cancellation or reopen

scheduler, err := db.StartIndexBuildScheduler(ctx,
  meldbase.IndexBuildSchedulerOptions{
    PollInterval: time.Second, RunTimeout: 250 * time.Millisecond,
    MaxConcurrency: 1, RunImmediately: true,
  })
defer scheduler.Stop()
```

The private shadow catches up through retained commits and becomes visible only
through one atomic catalog publication. `db.IndexBuilds()` exposes progress and
`db.AbortIndexBuild(ctx, id)` releases it explicitly. Admin JSON, the embedded
dashboard, Prometheus and OpenTelemetry report aggregate phase/size metrics
without collection or index-name labels. Logical compaction refuses to discard
an unfinished durable build; finish or abort it first.
Each new catch-up generation also protects the exact applied CatalogRoot, so
offline verification can prove the private tree against its watermark even after
older Commit Log entries are pruned.
The scheduler is default-off, time-sliced, limited to one instance per database,
and persists terminal failure reasons rather than retrying them indefinitely.

An offline/local operator can manage the same durable records without writing a
Go program. Every successful command emits a schema-versioned JSON receipt and
acquires the normal exclusive database lock:

```sh
go run ./cmd/meld index-build start \
  --db app.meld2 --collection users --name users_email_created \
  --field email:1 --field createdAt:-1 --unique
go run ./cmd/meld index-build list --db app.meld2
go run ./cmd/meld index-build resume --db app.meld2 --id <build-id> --timeout 10m
go run ./cmd/meld index-build abort --db app.meld2 --id <build-id>
```

`resume` never hides a uniqueness or resource failure: it exits unsuccessfully,
and `list` can inspect the still-private task before an explicit abort. These are
local file-management commands, not unauthenticated HTTP administration routes.
Compaction inherits the source V2 quota. Use `CompactToV2WithOptions` or
`MigrateToV2WithOptions` with `V2DestinationOptions` when a rewritten file needs
a different quota or index-build budget; an undersized destination is never
published.

## TypeScript SDK

The same compiled, data-only query is used by the local cache and sent to the
server. Native JS applications can use a local reactive collection directly:

```ts
import { LocalCollection } from "@meldbase/client"

const todos = new LocalCollection([
  { _id: "one", title: "Learn Meldbase", done: false }
])

const open = todos.find({ done: false }, {
  sort: [{ path: "title", direction: 1 }]
})

const stop = open.subscribe(snapshot => render(snapshot))
```

Remote queries use HTTP for fetch and a ticket-authenticated WebSocket for their
ongoing state:

```ts
import { MeldbaseClient } from "@meldbase/client"

const db = new MeldbaseClient({
  baseUrl: "https://data.example.com",
  accessToken: () => auth.currentAccessToken(),
  // Enable after every server in a rolling deployment advertises its fixed
  // realtime/RPC capability descriptor.
  requireRealtimeProtocol: true
})

const query = db.collection("todos").find({ done: false })
const stop = query.subscribe(render, {
  onStatus: status => showSyncState(status.state)
})

const created = await db.collection("todos").insertOne({
  title: "Build something live",
  done: false
})

await db.collection("todos").updateOne(
  { _id: created._id },
  { $set: { done: true } }
)
```

React uses a thin `useSyncExternalStore` adapter over that same query object; it
does not introduce a second query language:

```tsx
import { useMemo } from "react"
import { useLiveQuery } from "@meldbase/react"

function OpenTodos({ db }: { db: MeldbaseClient }) {
  const query = useMemo(
    () => db.collection("todos").find({ done: false }),
    [db]
  )
  const { documents, status, error } = useLiveQuery(query)
  // Keep the query object stable; updates arrive over its WebSocket subscription.
  return <TodoList todos={documents} syncState={status} error={error} />
}
```

See
[`docs/client-protocol.md`](docs/client-protocol.md) for the realtime and security
model.

A complete browser example lives in
[`examples/realtime-todos`](examples/realtime-todos). Run the development server
above, then:

```sh
pnpm --filter @meldbase/example-realtime-todos dev
```

The example performs real HTTP mutations and WebSocket snapshots through the
React adapter. Open it twice to observe the same query update in both views.

## Run the end-to-end demo

The demo performs durable insert/update, creates and uses an index, observes a
reactive query, closes the database, and proves the data after reopen:

```sh
go run ./cmd/meld demo
```

Run the HTTP/WebSocket server locally only with the explicit development-auth
switch:

```sh
go run ./cmd/meld serve \
  --db ./app.meld \
  --addr :8080 \
  --dev-no-auth
```

`--dev-no-auth` grants every request full access and is intentionally required;
it is not a production authentication mode. A production embedding supplies the
server `Authenticator` and `Authorizer` implementations itself.

The development server can exercise the same fail-closed V2 anchor lifecycle.
On the first audited provisioning only, use `--rollback-anchor-init`; omit it on
every subsequent start:

```sh
go run ./cmd/meld serve \
  --db /data/app.meld \
  --rollback-anchor /trusted/app.anchor \
  --rollback-anchor-init \
  --rollback-anchor-timeout 10s \
  --dev-no-auth
```

`/trusted` must already exist and, for actual rollback protection, must not be
on the same failure/rollback domain as `/data`.

The CLI can also connect to the independently deployed TLS 1.3/mTLS quorum with
the complete `--rollback-anchor-cluster`, repeated
`--rollback-anchor-replica member=https://endpoint`, name, HMAC key-file, CA and
client-certificate flags. See the
[rollback-anchor service deployment guide](docs/rollback-anchor-service.md).

To experience the separately secured embedded observability panel:

```sh
export MELDBASE_ADMIN_TOKEN='replace-with-at-least-32-random-bytes'
go run ./cmd/meld serve \
  --db ./app.meld \
  --addr :8080 \
  --dev-no-auth \
  --admin-addr 127.0.0.1:9091 \
  --admin-diagnostics \
  --admin-metrics
```

Then open `http://127.0.0.1:9091/` and paste the token. The panel receives a
fixed-history snapshot followed by an isolated SSE stream; it is not backed by a
user collection or the business reactive pipeline. Its health strip separates
database, durability, storage, realtime, telemetry and optional transport state;
fixed explanations identify fail-stop writes, queue pressure and recent fallback
events without exposing business data. It also shows Commit Log retention
pressure and configured/rejected write resource budgets. Use
`--admin-diagnostics-all` only for short sessions that need every query/commit;
the default diagnostic mode retains bounded slow and failed operations.
The storage panel also exposes physical generation, rollback protection,
anchor sequence/generation lag, failures, maximum latency and configured
timeout.
Prometheus can scrape the separately authenticated `GET /metrics` endpoint when
`--admin-metrics` is enabled.

Applications already using OpenTelemetry can register the fixed aggregate
schema through `integrations/otel`. The adapter consumes the same sampler and
requires an application-owned `MeterProvider`; Meldbase does not construct an
OTel SDK or exporter. Short CPU profiles, heap profiles and Go runtime traces are
available through `admin.RuntimeProfiler` with explicit duration/byte limits and
no default HTTP endpoint. See [observability](docs/observability.md).

The implemented transport endpoints are:

```text
GET  /livez
GET  /readyz
GET  /health  (readiness-compatible alias)
POST /v1/collections/{collection}/query
POST /v1/collections/{collection}/documents
POST /v1/collections/{collection}/mutations
POST /v1/realtime/tickets
GET  /v1/realtime
```

`/livez` proves only that the Go handler can respond. `/readyz` and `/health`
require the database to be both readable and writable; a closed database or a
fail-stop durability error returns 503. Probe responses contain only a version,
fixed status and readable/writable booleans—never paths or error text. This keeps
liveness available during diagnosis without routing new application traffic to
a database that can no longer commit.

HTTP queries carry the same versioned, data-only AST used by the SDK. With the
development server above:

```sh
curl -X POST http://localhost:8080/v1/collections/todos/query \
  -H 'Content-Type: application/json' \
  --data '{
    "version": 1,
    "query": {
      "version": 1,
      "where": {"op":"compare","cmp":"eq","path":"done","value":{"t":"bool","v":false}},
      "sort": [{"path":"title","direction":1}]
    }
  }'
```

Browser realtime authentication is two-step: obtain a short-lived, single-use
ticket over authenticated HTTP, then send it in the first WebSocket message.
Credentials never appear in the WebSocket URL. The core V1 exchange is:

```json
{"v":1,"type":"authenticate","ticket":"<single-use-ticket>"}
{"v":1,"type":"subscribe","requestId":"open-todos","collection":"todos","query":{"version":1,"where":{"op":"compare","cmp":"eq","path":"done","value":{"t":"bool","v":false}}}}
{"v":1,"type":"snapshot","requestId":"open-todos","subscriptionId":"<id>","token":"<signed-resume-token>","documents":[]}
{"v":1,"type":"unsubscribe","subscriptionId":"<id>"}
```

See [`docs/client-protocol.md`](docs/client-protocol.md) for reconnect,
`resync_required`, limits, origin checks, and row/field authorization.

## Status

Early-stage and not suitable for production data. See
[`docs/architecture.md`](docs/architecture.md) and
[`docs/roadmap.md`](docs/roadmap.md). The low-cost metrics, bounded admin sampler
and secured realtime stats stream are in
[`docs/observability.md`](docs/observability.md). The first-stage
requirement-to-evidence map is in [`docs/mvp-audit.md`](docs/mvp-audit.md).

Supported query operators are `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`,
`$in`, `$nin`, `$exists`, `$and`, `$or`, and `$not`. Meldbase defines these
semantics itself and does not promise MongoDB compatibility.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
go run ./cmd/meld demo
pnpm check
pnpm test
pnpm build:example
```

Contributions should follow [`CONTRIBUTING.md`](CONTRIBUTING.md). Security
reports must use the private process described in [`SECURITY.md`](SECURITY.md),
not a public issue. Maintainer release gates are documented in
[`docs/releasing.md`](docs/releasing.md).

## License

Licensed under the [Apache License 2.0](LICENSE).
