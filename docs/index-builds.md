# Index construction architecture

The storage engine provides both the compatibility `CreateIndex` path and a negotiated,
crash-resumable shadow-build protocol. Both have one atomic visibility point;
only the latter persists private progress across cancellation and restart.

## Implemented path

`Collection.CreateIndex` currently has one synchronous result and one atomic
visibility point.

1. Reserve `(collection, index name)` in the process.
2. Pin commit sequence `S` and scan its immutable Primary tree without
   holding the database writer mutex.
3. Validate the complete canonical document encoding and `_id`, but materialize
   only the indexed scalar paths.
4. Enforce the configured entry and canonical Secondary-byte budgets.
5. If the current sequence is no longer `S`, discard the private entries and
   retry, at most three attempts.
6. Sort entries and feed complete Secondary keys into `SortedTreeBuilder`.
   Leaves are sealed incrementally; the builder retains one decoded leaf and a
   branch frontier rather than a complete mutable tree.
7. Revalidate every Primary/Order owner and publish the Secondary root,
   IndexCatalog metadata, Collection metadata and Commit Log event in one COW
   transaction.

Before step 7 the candidate index is unreachable from the authoritative
catalog. Cancellation, resource rejection, uniqueness failure and optimistic
conflict publish no index generation. Resource rejection does not enter
durability fail-stop. A concurrent writer can proceed during steps 2–5; final
page publication still occupies the single storage writer.

The admin schema exposes active/attempt/completed/failed builds,
optimistic retries/conflicts, last entry/byte size and last/max duration without
collection or index-name labels. It separately reports durable unfinished
builds by phase plus their aggregate entry/byte footprint. Build maintenance
refreshes an immutable aggregate snapshot after publication; `DB.Stats()` reads
that snapshot atomically in O(1), with no BuildCatalog traversal or allocation.

`BenchmarkStoredDocumentIndexProjection` isolates the scan codec. On the
development amd64 machine, decoding a representative 1 KiB document and then
looking up one nested scalar measures about 2.94 µs, 5,296 B and 44 allocations;
the validating path projection measures about 341 ns with zero allocations.
`BenchmarkCreateIndexTenThousandDocuments` is the end-to-end regression target
covering snapshot extraction, sorting, Primary/Order revalidation, bulk loading,
Commit Log publication and fsync. Benchmark allocation totals are cumulative
work, not a claim about peak resident memory.

## Persistent background protocol

The implementation negotiates `RequiredFeatureShadowIndexBuilds`. A compound
or descending build also atomically negotiates `RequiredFeatureCompoundIndexes`.
Older readers
reject the Meta envelope as unsupported before decoding the extended root
layout. A database does not enable the feature until its first persistent build.

With that feature enabled, `DatabaseRoot` has a protected
`IndexBuildCatalogRoot`. Reachability, verification, backup, compaction,
reclamation and golden fixtures must all understand it. Build-catalog records
are keyed by a random build ID and contain only bounded canonical fields:

- collection identity and index definition;
- source commit sequence and source CatalogRoot;
- phase: `scan`, `catch_up`, `ready`, `failed`;
- durable Primary/Order scan cursor;
- private Secondary root, entry count and canonical bytes;
- highest Commit Log sequence incorporated;
- creation/update timestamps and a fixed failure reason enum.

The source CatalogRoot is an authoritative reachability edge. This prevents the
scan snapshot from being reclaimed after later Meta generations. The build
record is also a persistent replay lease: Commit Log pruning must retain the
next sequence after the build's incorporated watermark. Count/byte retention
pressure remains observable and may exceed its normal budget while this lease
is active, exactly as with a live replay reader.

### State transitions

```text
absent
  -> scan(S, source root, cursor=begin, empty shadow root)
  -> scan(...cursor advanced, shadow root replaced per bounded batch)
  -> catch_up(applied=S)
  -> catch_up(applied=N, applied catalog root=N)
  -> ready(applied=current, applied catalog root=current)
  -> atomically published IndexCatalog entry + Commit Log catalog event
  -> absent
```

Each scan/catch-up batch is a physical maintenance generation: it advances Meta
generation but not logical commit sequence. The new DatabaseRoot protects the
latest private root and source snapshot before the older build pages can become
reclaimable. Scan batches are bounded to 4,096 source documents and 16 MiB of
canonical Secondary keys. Catch-up batches cover at most 1,024 commits and
10,000 relevant mutations. Database resource limits bound the complete shadow's
entry and canonical-byte totals.

Catch-up consumes commit images in strict sequence order. For compound indexes,
the scan and both commit images project the same ordered tuple codec. Inserts add one key;
deletes remove the Before key; updates remove Before and add After when the
indexed path changes. The durable insertion position remains part of the
Secondary suffix. Missing history is not silently ignored: the runner returns
`ErrHistoryLost`, after which the operator can abort and start from a new
snapshot.

Every catch-up batch also copies the final commit's immutable CatalogRoot into
the build record and negotiates a dedicated required-feature bit atomically with
that reachability edge. Consequently, pruning commits at or before the applied
watermark cannot erase the evidence needed to audit the shadow. Offline verify
checks scan builds exactly through `ScanAfter`; catch-up/ready builds are checked
bidirectionally against `AppliedCatalogRoot`, including entry/byte accounting,
document identity, insertion position and canonical compound key. Legacy
caught-up records remain readable but yield
`indexBuildContentsVerified: false` because their exact snapshot is unknowable.

The first applied-root encoding is frozen by an additive revision-3 page-delta
fixture rooted in the existing business fixture digest. Reader tests reconstruct
and audit the artifact independently of the writer; writer tests reproduce its
exact changed pages and compressed bytes. Older shadow and business fixtures are
never regenerated to make a newer extension appear historically present.

Finalization takes the storage writer lock and succeeds only when:

- the build watermark equals the current logical sequence;
- collection identity and source index absence still match;
- the shadow entry count and root are internally valid;
- a unique index has no equal scalar-key neighbors;
- no conflicting build or published index owns the name.

Finalization advances the logical sequence once and atomically moves the shadow
root into the normal IndexCatalog. It must not copy or rebuild the tree.

### Crash and cancellation semantics

- Crash before a maintenance Meta sync: reopen sees the previous cursor/root.
- Crash after maintenance Meta sync: reopen resumes the newly protected state.
- Crash during final data pages but before final Meta: index remains building.
- Crash after final Meta sync: index is fully published with its Commit Log
  event; no build record remains.
- Context cancellation stops the current runner and preserves durable progress.
  Explicit abort removes the build record in a maintenance generation; ordinary
  epoch-safe reclamation later recovers private pages.
- Corrupt build metadata fails normal graph verification. A semantically
  failed build is not database corruption and uses a fixed state/reason.
- Physical backup preserves the complete build graph and it can resume from the
  restored artifact. Logical compaction deliberately refuses to run while any
  build record exists, including failed records, so it cannot silently discard
  private progress or audit evidence.

### Go API

```go
id, err := collection.StartIndexBuild(ctx, "users_email_created",
    []meldbase.IndexField{
        {Field: "email", Order: 1},
        {Field: "createdAt", Order: -1},
    },
    meldbase.IndexOptions{Unique: true})

// Safe to call again after cancellation or process restart.
err = db.ResumeIndexBuild(ctx, id)

status, err := db.IndexBuild(id)
builds, err := db.IndexBuilds()
err = db.AbortIndexBuild(ctx, id)
```

`CreateIndexOnline` is the start-and-resume convenience form. These APIs are
Persistent storage only; memory returns `ErrIndexBuildUnsupported`. A unique-key
conflict rejects only final publication and leaves the private `ready` build
available for inspection or explicit abort.

For automatic progress, start the explicit default-off scheduler:

```go
scheduler, err := db.StartIndexBuildScheduler(ctx,
    meldbase.IndexBuildSchedulerOptions{
        PollInterval:   time.Second,
        RunTimeout:     250 * time.Millisecond,
        MaxConcurrency: 1,
        RunImmediately: true,
    })
defer scheduler.Stop()
```

Each run has a bounded time quantum and persists at batch boundaries. The
default concurrency is one and the maximum is eight; one database admits only
one scheduler. `Stop` waits for active runners to observe cancellation and does
not fail or abort their builds. The scheduler converts terminal unique,
resource, history-loss and invalid-index errors into fixed persistent failure
codes so they do not retry forever. Failed builds release their Commit Log
retention lease but keep source/shadow pages reachable for status inspection
until explicit abort. Direct `ResumeIndexBuild` still returns the error without
making this operator-policy decision; attempting to resume an already failed
record returns `ErrIndexBuildFailed`, with the fixed reason available on status.

The `meld index-build` CLI exposes `start`, `list`, `resume` and `abort` over the
same APIs for an offline/local operator. It performs read-only format negotiation
before opening, requires an existing compatible file, honors signal/deadline
cancellation and emits schema-versioned JSON only after a successful operation.
It deliberately does not expose an unauthenticated network control endpoint.
`start` accepts one to four ordered repeatable flags; an omitted suffix means
ascending, for example `--field email --field createdAt:-1`.

### API boundary

The storage layer exposes begin/append-scan/apply-catch-up/fail/finalize/abort
primitives over canonical data. The Go database layer owns scheduling,
deadlines, retry policy and public handles. JS/TS may later expose build status,
but names, paths and workspace identity remain outside default metrics.

`SortedTreeBuilder` is the leaf/page construction primitive for the synchronous
path and the initial empty shadow. Scan/catch-up batches incrementally update
the private COW Secondary tree. An external merge-sort source can later replace
the synchronous path's in-memory entry sort without changing publication.

The scheduled storage race soak keeps one shadow build scanning/catching up
while another goroutine commits indexed updates, readers pin snapshots, online
reclamation audits/reuses pages and the file repeatedly closes and reopens. Each
phase verifies the build record and full graph before continuing; the final
phase aborts it and re-audits reclamation reachability.
