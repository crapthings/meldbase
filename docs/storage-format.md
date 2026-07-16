# Storage formats

Both experimental formats use 16 KiB pages and alternating fixed meta pages.
Their complete page layouts are versioned but not frozen for compatibility.
`Open` detects an existing
V1 or V2 database without migration and creates missing/empty paths as V2.
`OpenV1` and `OpenV2` remain explicit format entry points.

If the main path is missing or empty but a legacy `.wal` sidecar exists, `Open`
fails closed instead of creating V2 and stranding possible V1 data. Unknown,
truncated and mixed-family main files also fail as corruption. Explicitly using
the wrong format opener is rejected without modifying the file.

`DetectStorageFormat` reads only the two fixed meta magic fields and never
mutates the path. Missing or zero-length paths report unknown; an unrecognized
non-empty, truncated, or mixed V1/V2 file is corruption. The chosen engine still
performs full checksum, root and graph validation.

`InspectStorageFormat` goes one step further without opening or locking the
database. It checksum-validates both Meta slots, selects the greatest valid
generation, and reports family, revision, logical sequence, required/optional
feature bits, identity, physical high-water mark and compatible-reader status.
Because V2 negotiation offsets are frozen, it can report a checksum-valid future
revision while refusing to interpret its page graph. The `meld inspect --db`
command with `--require-compatible` exposes the same versioned JSON contract for release
scripts.

The operational upgrade decision is fail closed:

- compatible V2 may be opened normally;
- V1 is opened by the legacy reader and copied through explicit
  `MigrateToV2` into a separately verified, no-overwrite destination;
- incompatible V2 must be handled by a binary that supports the reported
  revision/features—an older binary never falls back to and overwrites the
  other Meta slot;
- corruption or zero valid Meta slots is not treated as an upgrade request.

Inspection is negotiation evidence only. A successful `Open` still performs
current-revision Meta, root and graph validation.

`VerifyV2File` and `meld verify --db <path>` provide the explicit offline graph
audit between those two layers. Verification opens an existing V2 file read-only
under a non-blocking shared advisory lock, uses the same Meta/root selection code
as the normal opener, walks every business page protected by both valid Meta
roots, and semantically audits every published index. Secondary-to-Primary checks
that each stored key equals the key recomputed from the canonical document;
Primary-to-Secondary checks that every index-eligible document has its exact
entry. This covers compound direction, partial tuples and uniqueness in addition
to tree shape and counts. Verification validates persistent FreeSpace
acceleration separately and hashes every byte including an unaligned crash tail.
The schema-3 receipt includes separate `indexContentsVerified` and
`indexBuildContentsVerified` evidence, identity, generation, sequence,
physical/protected/reclaimable counts, FreeSpace status and SHA-256. A legacy
caught-up build without an applied CatalogRoot is still structurally verified,
but the second flag is false rather than overstating semantic proof.

Verification never creates, truncates, repairs, installs a free-page pool, or
publishes a generation. This distinction is tested by byte-comparing valid and
corrupt files before and after the audit. A normal V2 writer owns an exclusive
advisory lock, so offline verification fails as `ErrDatabaseLocked` while that
writer is active. Reclaimable counts are evidence only; `verify` is not a hidden
maintenance operation.

The semantic pass is intentionally offline and costs O(index entries + indexes ×
primary documents), including active shadow builds. Within each protected
collection generation it scans and decodes Primary documents once, projects all
published indexes from that pass, and compares exact Secondary keys and counts;
it does not decode every document once from each side of every index. Shadow
accounting and semantic structure share one Secondary scan before their Primary
comparison. Exact lookups reuse immutable decoded tree-page views within one
semantic verifier run. That verifier-local cache retains at most 1,024 pages
(16 MiB of page bytes) and rejects cross-TreeKind reuse; it is absent from normal
reachability and reclamation. Normal open, query, commit, stats and online
reclamation do not invoke the document projector or pay this scan/cache cost. A
future bounded external merge verifier may reduce remaining random exact-key
lookups, but verification will not replace exact bidirectional proof with a
probabilistic aggregate digest.

Successful `Open`, `OpenV1` and `OpenV2` calls expose an immutable
`DB.RecoveryReport()`. The report makes bounded automatic recovery auditable:
V1 checkpoint/WAL replay and partial-tail removal; V2 Meta/root selection,
older-root fallback, partial-page tail removal and optional FreeSpace
degradation. It is a receipt, not a repair command. Checksum ambiguity,
unsupported revisions/features, graph corruption, identity disagreement and
non-tail WAL corruption still reject Open. There is intentionally no API that
clears an in-process durability fail-stop or promotes an unverified root.

`OpenWithOptions`, `OpenV1WithOptions` and `OpenV2WithOptions` accept
`RecoveryRequireClean`. For an existing database this mode returns
`ErrRecoveryRequired` before truncating a main/WAL tail, replaying a complete V1
WAL record, accepting reduced Meta/root redundancy, selecting an older V2 root,
or discarding a corrupt optional FreeSpace snapshot. Byte-identity tests cover
all of those rejection paths. New database creation and a clean existing open
remain allowed. The default `RecoveryAutomatic` preserves the bounded recovery
behavior described above.

### V2 meta compatibility envelope

The V2 meta magic, 16 KiB envelope, SHA-256 coverage and checksum location are
stable compatibility fields. A meta page must pass that checksum before its
encoding revision or feature bitmap is interpreted. This distinction is part of
the recovery contract:

- a checksum-valid unsupported revision or unknown required feature returns
  `ErrUnsupportedFormat` and blocks fallback to an older meta generation;
- a torn or checksum-invalid meta page is corruption and may fall back to the
  other fully valid meta page;
- unknown optional feature bits are preserved and ignored. They may accelerate
  behavior but cannot be required to recover authoritative business state.

Both meta slots are inspected before selecting a generation. Consequently an
older binary cannot silently select and later overwrite an older generation when
the other slot was validly written by a newer binary. This freezes only the
negotiation envelope, not every V2 page or record layout.

The checked-in revision-3 Meta golden fixture independently pins its field
prefix, embedded checksum and complete 16 KiB page digest. Tests reconstruct a
two-slot database from that fixture, open it, publish the next generation and
reopen the result. A new encoding revision must add a new fixture rather than
rewriting historical release evidence. Full business-page golden files remain
part of the compatibility evidence below. The current revision-3 page and record
layouts exercised by those artifacts are pinned; future features must be
additive and separately negotiated or use a new revision.

A second revision-3 alpha fixture pins a complete deterministic business file,
both compressed and uncompressed SHA-256 digests, and its logical expectations.
It covers DatabaseRoot, Catalog, Primary, Order, IndexCatalog, Secondary, Commit
Log, Document/Commit/System overflow and persistent FreeSpace pages. CI requires
the current writer to reproduce its exact bytes and the current reader to open,
audit, query, advance and reopen it. This makes accidental layout drift explicit;
it does not by itself convert every covered alpha layout into a forever-stable
compatibility promise.

The optional persistent index-build extension is negotiated by the required
`RequiredFeatureShadowIndexBuilds` bit. When enabled, DatabaseRoot may reference
an IndexBuildCatalog B+Tree whose fixed records protect the source CatalogRoot,
private Secondary root, scan cursor and applied Commit Log watermark. Readers
that do not understand the bit reject the file before graph decoding. The
record codec has a deterministic SHA-256 golden and begin/finalize publication
fault matrices prove that feature bit, catalog root, shadow state, published
IndexCatalog entry and logical commit event never recover as a mixed generation.

Catch-up beyond the source sequence additionally negotiates
`RequiredFeatureIndexBuildAppliedRoot`. The build record stores the immutable
post-commit CatalogRoot represented by `AppliedSequence`, and reachability keeps
that snapshot alive independently of Commit Log retention. Its record-local
codec marker distinguishes new evidence from legacy zero-filled reserved bytes;
removing the required bit while such a root is reachable fails closed.

The additive `index-build-applied-root-revision-3` fixture pins this extension
without copying or rewriting the earlier business artifact. Its manifest names
the base fixture SHA-256 and stores a sorted, gzip-compressed set of changed
16 KiB pages plus patch and reconstructed-file hashes. CI reconstructs the file
without invoking the current writer, performs structural and shadow-semantic
verification, publishes the ready index, advances and reopens it. A separate
writer test must reproduce the exact page-ID list, compressed patch bytes and
full-file digest. This page-delta form keeps release evidence composable while
remaining a byte-exact reader artifact, not a logical export.

Revision 3 also has a compact `branch-pages-revision-3` corpus for the nine
B+Tree kinds. The production bulk builder creates one real branch root for each
Catalog, Primary, Secondary, CommitLog, IndexCatalog, Order, System, FreeSpace
and IndexBuildCatalog tree. The corpus pins page ID, PageType, generation,
sequence, full-page checksum and bytes; the reader decodes the page envelope and
branch node, validates child/count/separator structure, and proves the
TreeKind-to-PageType mapping. A coverage test combines this corpus with the
openable business/shadow/compound/applied-root artifacts and fails if any of the
22 current PageTypes lacks revision-3 byte evidence.

The branch corpus is intentionally not presented as an openable business
database: its child pages are not included. It freezes branch encoding and type
negotiation without pretending that isolated pages prove graph semantics.

The checked-in `multilevel-business-revision-3` artifact supplies that separate
graph evidence. It is a deterministic, independently decodable 10.9 MiB database
whose current DatabaseRoot reaches branch roots for all nine B+Tree kinds. Its
203 collections include 600 ordered and indexed documents, a wide IndexCatalog,
400 System records, a 400-change Commit, 64 durable shadow builds and a persistent
multi-leaf FreeSpace tree. The artifact stores compressed and uncompressed
SHA-256 digests; CI requires the production writer to reproduce the complete
file byte for byte. Reader tests verify the full reachable graph, published and
shadow index semantics, reusable-page safety, current queries, a subsequent
indexed commit, historical Snapshot 1 reconstruction and reopen. The exhaustive
coverage gate proves that every one of the 22 current PageTypes is present in
this openable artifact, while the smaller corpus remains useful for pinpointing
individual branch-codec drift.

The original revision-3 business fixture remains byte-for-byte unchanged. A
second checked-in `index-build-revision-3` fixture independently pins the later
extension: required feature bitmap, DatabaseRoot BuildCatalog edge, failed build
record, protected source CatalogRoot, private Secondary entries and fixed
failure reason. CI reproduces its exact bytes, performs the read-only verifier
and hash, opens/reaches the graph, aborts the build, advances one business commit
and reopens it. This preserves historical evidence instead of retroactively
rewriting the older alpha fixture.

Compound and descending Secondary definitions are independently negotiated by
`RequiredFeatureCompoundIndexes`. Existing single-ascending indexes retain
their scalar-key codec and exact IndexCatalog bytes. New definitions store one
to four ordered `(path, direction)` components and use tuple-key codec V3; the
same definition is carried by CreateIndex Commit Log images and durable shadow
build records. Publication of either an ordinary or private compound index sets
the required bit atomically with the first reachable V3 record.
V3 partial-prefix entries use a reserved marker plus document identity when a
non-leading component is missing. This preserves complete left-prefix query
membership while keeping partial documents outside complete-tuple uniqueness.

The additive `compound-index-revision-3` fixture pins this extension without
rewriting either earlier fixture. CI reproduces its exact bytes, checks both
compressed and full-file hashes, verifies and opens the graph, inspects the
ordered fields and codec, advances an indexed commit, and reopens it. Backup is
physical and therefore preserves the bytes; logical compaction reconstructs the
same ordered definition and required feature under its new database identity.

CI runs the format golden tests, exclusive-lock checks, V1/V2 publication fault
matrices and simulated disk-full recovery contracts on both Linux and macOS.
This establishes implementation portability across the currently targeted local
file APIs; it is not evidence of power-loss behavior for every filesystem,
kernel, storage controller or network-mounted volume.

`meld durability-check --dir <target-volume>` performs the same kind of
deployment-local capability qualification without touching an existing
database. Its versioned JSON covers file/directory sync, independent advisory
lock conflict and descriptor-close release, no-overwrite hard links,
same-directory rename, and a real indexed V2 commit/reopen followed by offline
full-graph/index verification. Schema 2 optionally binds the receipt to a source
revision, records the binary's VCS revision/dirty flag, and adds Go/runtime,
filesystem name/type/capacity, the verified
database graph summary and SHA-256. All artifacts live under a private temporary
directory that is removed and followed by a parent-directory sync. A pass is a
capability result for the current mount, not a power-cut durability certificate.
Linux/macOS CI validates and archives this JSON as a per-platform artifact so a
release can retain the exact device/filesystem type, block size and individual
check timings instead of relying only on a green job summary. The evidence
levels and still-pending destructive matrix are defined in
[`filesystem-qualification.md`](filesystem-qualification.md).

## V1 main file

Pages 0 and 1 are alternating meta pages. Each contains the database identity,
generation, checkpoint token, snapshot root page, page count, recorded page size,
and SHA-256 checksum. On open, the newest valid meta page wins; if a meta write is
torn, the previous fully synced generation remains authoritative.

Checkpoint data is written copy-on-write into newly appended slotted record
pages. The checkpoint's catalog manifest stores ordered generation-safe
`RecordID {page, slot, generation}` references; the meta page points to the first
catalog page. Record bodies grow upward and the fixed-size slot directory grows
downward. Deleting and reusing a slot advances its generation, so a stale index
or catalog reference cannot resolve to a different record.

Catalog pages and record pages carry page ID, generation, LSN/token, free-space
metadata, and checksums. The engine syncs all new record/catalog pages before
writing and syncing the alternate meta page. Older contiguous snapshot pages are
still readable during this experimental format transition, while all new
checkpoints use the catalog-rooted layout.

The snapshot payload uses Meldbase's typed binary document codec—not JSON—and
contains collections, document order, index definitions, and independently
addressed B+Tree node topology. Index contents are restored directly and
validated against the documents on open.

## V1 WAL

Each mutation or catalog operation is one framed commit record with an increasing
token, bounded payload length, and checksum. The record is synced before its
changes become visible in memory or to reactive subscribers. Recovery replays
only records newer than the checkpoint token. A provably partial tail is removed;
a complete record with a bad checksum is corruption and fails open.

After a successful checkpoint meta sync, the WAL is reset and synced. A crash
between those steps is safe because recovery filters records already represented
by the checkpoint token.

Legacy V1 opens use a bounded automatic checkpoint policy: the default triggers
after either 64 MiB of current WAL data or 10,000 commits since the previous
checkpoint. `OpenV1WithOptions` can select other byte/commit thresholds or
explicitly disable automatic maintenance. The triggering business commit is
WAL-synced, applied and assigned its logical token before checkpoint work starts;
the physical checkpoint does not create another logical commit. Checkpointing is
synchronous at the threshold, so it produces a bounded latency step instead of a
background snapshot race. New/default V2 databases do not use this policy because
every commit already publishes a COW DatabaseRoot and inactive Meta page.

Publication order is fixed: write all checkpoint pages, sync them, write and sync
the inactive Meta, then truncate and sync the WAL. A maintenance failure after
the business WAL fsync never changes that successful operation into a returned
error. The current writer enters a fail-stop durability state and must be reopened;
recovery selects the old Meta plus WAL, or the complete new Meta while filtering
or observing the reset WAL. Tests inject all five page/meta boundaries and all
four checkpoint/WAL-reset boundaries in addition to cutting framed WAL records at
header and payload positions.

## V2

V2 encoding revision 3 publishes immutable Catalog, Primary, insertion-Order,
IndexCatalog, Secondary, Commit Log and private System B+Trees through a
copy-on-write DatabaseRoot and checksummed inactive meta-page swap. One commit
makes document, order and secondary roots plus logical change history visible
together.
Historical CatalogRoots and replay leases support Snapshot N plus an ordered
N+1 stream after reconnect.

The logical Commit Log has mandatory count and canonical-byte budgets. Defaults
keep at most the latest 10,000 commits and 256 MiB of logical header/change
encoding; `V2CommitRetentionPolicy` can select either bound. Both are required:
a count alone cannot constrain a history made of unusually large commits.
Pruning old headers and change entries is staged in the same COW transaction as
the triggering business commit, so the inactive Meta page can expose only both
changes or neither. An active replay lease takes precedence over both watermarks
and can temporarily retain an overage rather than silently losing a reader's
history. On open, retained logical bytes are reconstructed by a bounded streaming
walk of the retained Commit Log without changing the revision-3 disk layout.
Current count/byte overage, pressure state/events and pruned commits are
observable. Logical pruning makes pages reclaimable; online reclamation or
compaction controls when physical file space is reused/reduced.

Every V2 handle also has an immutable physical high-water quota, defaulting to
8 GiB. It is an allocation policy and is not persisted in revision-3 metadata.
Page allocation consumes the validated reuse pool before appending and returns a
safe storage-limit rejection before any `WriteAt` when the next page would cross
the quota. Opening an existing file larger than a newly selected quota remains
possible for reads, verification, reclamation and compaction; appending remains
blocked unless reusable pages can satisfy the complete transaction. Quota
rejection never enters durability fail-stop. The quota is page aligned and
limits high-water bytes, not filesystem block accounting.

`CompactToV2` inherits the open source handle's quota. Migration defaults to the
normal V2 quota because V1 has no corresponding policy. The explicit
`CompactToV2WithOptions` and `MigrateToV2WithOptions` variants accept
`V2DestinationOptions` when the destination needs a different high-water mark;
a too-small destination remains private and is never published.

The private System tree is reached through a reserved NUL-prefixed Catalog entry
containing a versioned system-directory record. It stores bounded first-party
control-plane records with atomic compare-and-set mutation, inline values and
overflow chains. The public collection namespace cannot express the reserved
key, and collection/document counts and query APIs exclude it. System commits
still share the database's single-writer sequence and publish an empty logical
change batch so live resume positions remain contiguous.

`ApplyDocumentSystemTransaction` is the composite publication primitive. It
checks a bounded set of up to 256 distinct System-record mutations and stages
them with document/order/index mutations in the same `WriteTxn`; the first CAS
mismatch aborts before any page is published. A successful commit writes one
business Commit Log batch whose CatalogRoot already includes the new System
root. The inactive meta swap therefore exposes business state, the RPC terminal
and any policy-generation rotation together. Its multi-record ENOSPC matrix
proves recovery can select only the complete old business/pending/policy
generation or the complete new business/terminal/policy generation.

Open-time graph validation checks the directory, System page types, key bounds,
counts and overflow chains. Reachability/reclamation retain its pages;
`CompactToV2` copies and byte-verifies all system records. Fault tests cover
every system-record ENOSPC publication boundary and prove recovery selects only
the complete old or new generation. The root encoding remains revision 3.
Binaries that predate its required System feature fail closed; they are not
compatible readers of such a file.

Revision 3 is an intentional alpha format break for the new Order root and
Secondary suffix. Revision-2 files fail closed; they are not silently rewritten
or mixed with revision-3 pages. Storage V2 is used for newly created `Open()`
paths. The current declared revision-3 layouts now carry byte-exact
cross-release reader/writer evidence, but the engine remains alpha while real
power-loss and supported-filesystem qualification is incomplete.

`DB.MigrateToV2(ctx, destination)` operates on an already open durable V1 DB. It
holds a consistent source read lock, writes batches into a private V2 file,
checks full page reachability, reopens through the public V2 adapter, and compares
every collection, document, insertion order and index definition before an
atomic no-overwrite publication. Cancellation or validation failure leaves no
destination. A successful destination deliberately receives a new database
identity, invalidating old V1 resume tokens.

`DB.CompactToV2(ctx, destination)` performs the same fail-closed publication
from a fixed V2 snapshot, copying only live documents in Order-tree sequence and
rebuilding indexes. It never mutates or overwrites the source. The compacted
file has a new database identity and commit history, explicitly invalidating old
resume tokens; operators may validate and swap it during their own maintenance
window.

`DB.BackupV2(ctx, destination)` has deliberately different semantics. It holds a
source read lock for the full copy, which leaves query readers available but
blocks commits, maintenance and compaction until the artifact is published. It
copies every page-aligned physical byte, including the selected Meta generation,
database identity, Commit Log history, FreeSpace metadata and any aligned tail
left by an interrupted older publication. The result is therefore a restore
point, not a logical export or an independently writable branch.

Backup writes into a private file in the destination directory, syncs it,
re-hashes the complete file, reopens it through both the physical V2 reader and
public database adapter, audits reachability, and compares identity, generation
and commit sequence. Publication uses a same-directory no-overwrite hard link
followed by directory sync. Cancellation or any validation/publication failure
leaves no destination; an existing destination is never replaced.

The offline operational form is:

```sh
meld backup --db app.meld2 --out app-backup.meld2 --timeout 10m
meld inspect --db app-backup.meld2 --require-compatible
meld verify --db app-backup.meld2 --timeout 10m
```

Its JSON receipt is schema version 1 and identifies the artifact as
`physical-v2-restore`, with byte/page counts, captured sequence and generation,
database ID and SHA-256. The CLI first performs read-only Meta negotiation and
refuses missing, V1 or reader-incompatible inputs. It then opens the source
normally, so the exclusive process lock makes this an offline command; an
already open application should call `BackupV2` on its own `DB` instead.

Restore is an operator-controlled offline transition: stop every writer using
the original or any copy, retain the receipt, re-run compatibility inspection
and full verification, and start exactly one writable instance from the restored
artifact. Because the
identity and history are preserved, simultaneously writing the source and its
backup creates two divergent files in the same resume-token identity domain and
is unsupported. Restoring also rolls state back to the captured commit sequence;
clients holding later tokens must resynchronize. Use `CompactToV2` instead when
the desired result is a fork with a new identity and history.

`DB.ReclaimV2Pages(ctx)` performs an explicit reachability audit while protecting
both independently valid meta roots plus all active snapshot and replay roots.
Unprotected page IDs enter a process-local reusable pool. Later commits reserve
arbitrary page IDs from that pool in O(1); overflow chains do not assume adjacent
IDs. Reused immutable-cache entries are invalidated only after the new meta is
durably published, so a failed commit cannot corrupt fallback state.

The audit also publishes an optional immutable FreeSpace B+Tree in a physical
maintenance generation. Maintenance advances the Meta generation but not the
logical commit sequence, does not add a Commit Log batch and does not wake a
live stream with application data. Extents record the audit generation and safe
physical high-water mark. Normal commits keep that snapshot immutable and
consume the in-memory candidate slice from greatest page ID downward, so they
incur no FreeSpace-tree write amplification.

On reopen, the loader validates the optional tree and checks only the consumed
suffix of candidates: a valid page whose generation is newer than the audit was
reused and is excluded. A failed or torn later write is conservatively excluded
only when it forms a valid newer page; otherwise the page remains an unreachable
candidate. Corrupt acceleration metadata is discarded while business data still
opens with an empty reuse pool, and another explicit audit repairs it. Full
reachability across both Meta roots and active reader/replay pins remains the
safety authority. `ReclaimV2Pages` retains the original blocking audit.
`ReclaimV2PagesWithOptions` can instead duplicate the read handle, scan immutable
roots without the writer lock, and take the lock only to verify an unchanged
Meta generation/high-water/free-pool token before installing candidates. A
conflicting commit discards the result and triggers only the configured bounded
number of retries. `StartV2Maintenance` is an explicit, default-off, serial
online scheduler with an interval, per-run deadline, lifecycle-bound shutdown
and fixed aggregate statistics. Its default memory-only mode installs the
candidate slice in O(1) and does not publish a physical FreeSpace generation;
restart simply requires another audit. Persistent FreeSpace publication is an
explicit option because building fragmented extents still occupies the writer
lock. Maintenance never runs on the commit hot path.
Race tests pause an online graph walk while committing, exercise bounded retry,
then run writers, snapshot readers and reclamation concurrently before a full
close/reopen reachability audit.

V2 publication is tested with real `ENOSPC` errors at five boundaries: a partial
new-page append, before data sync, after data sync, a torn inactive-meta write,
and after meta sync. The same matrix runs with reclaimed page IDs. On reopen the
selected state must be either the complete previous generation or, when the new
meta was durably synced, the complete new generation; a returned disk-full error
after meta sync is therefore an intentionally ambiguous commit result. The
in-process writer is durability-poisoned after any such IO failure and must be
closed and reopened before another write is attempted.
The same matrix covers FreeSpace maintenance without changing logical state and
normal reuse from a persistent audit snapshot.

A separate subprocess matrix terminates the writer process directly after the
first new page, before/after data sync, after the inactive Meta write and after
Meta sync. The parent process acquires the released lock, reopens the file and
requires a complete old or complete new generation plus a clean reachability
audit. This covers abrupt process death without Go cleanup; it still does not
substitute for hardware power-cut testing of a particular filesystem/controller.

An opt-in large-database soak exercises batched load, a unique Secondary index,
cross-tree churn, a pinned historical snapshot, explicit reclamation, close/
reopen, reachability and full Primary-to-Secondary equivalence. Normal unit tests
skip it. Release or nightly runners select scale explicitly:

```sh
MELDBASE_V2_SOAK_DOCUMENTS=100000 MELDBASE_V2_SOAK_ROUNDS=6 \
  go test ./internal/storage/v2 -run '^TestConfigurableLargeDatabaseSoak$' -count=1
```

The separate operational online-maintenance soak runs an fsyncing writer, snapshot reader
and memory-only optimistic auditor concurrently, then persists a final audit and
fully verifies Primary/Secondary contents and reachability after every reopen:

```sh
go build -race -o /tmp/meldbase-qualification ./cmd/meld
/tmp/meldbase-qualification storage-soak \
  --dir /path/to/target-volume \
  --out ./storage-soak-receipt.json \
  --profile release \
  --seconds 14400 \
  --documents 10000 \
  --reopens 12 \
  --source-revision "$(git rev-parse HEAD)" \
  --require-clean-source
```

The scheduled workflow runs a shorter 30-minute `sentinel` profile. A release
qualification must select the `release` profile, which refuses to run below four
hours of measured concurrent worker time, 10,000 documents or 12 reopens.
Reopen, audit, final verification and cleanup time is tracked separately in the
larger `actualDurationNanos`; it cannot be used to satisfy
`concurrentDurationNanos`. The runner additionally requires nonzero work from
every concurrent worker in every phase and at least one observed optimistic
reclamation conflict. The writer starts unthrottled only until a release run
observes that real conflict, then uses a fixed two-writes-per-second cadence;
non-release profiles use the same cadence from their first write. This makes the
duration qualification reproducible across hardware and prevents a faster host
from exhausting the normal V2 physical safety quota before a phase-boundary
reclamation. It is deliberately not a throughput benchmark. The reader,
shadow-index catch-up and optimistic auditor remain concurrent. The runner also
fails before setup unless the actual Meld binary
was built with `-race`, carries clean Go VCS build metadata and exactly matches
the claimed 40- or 64-hex source revision. A `go test` binary carries no VCS
identity and is deliberately unable to produce this receipt. Every
configured reopen remains mandatory even when
verification overhead crosses the requested deadline.

Each per-reopen content check performs one bounded Primary scan and one bounded
Secondary scan, validates exact document IDs, values and insertion positions,
and rejects duplicates or omissions. It does not rescan Primary by point lookup
for every Secondary entry. Before reopening, the runner closes the writer and
uses the same offline semantic/FreeSpace verifier as the final receipt instead
of following one complete reachability scan immediately with another. It then
reopens and compares the stored values to its workload model. These
qualification-only arrays are bounded by the explicit `--documents` value and
are not allocated by normal database traffic.

The underlying reachability collection audit likewise scans Primary once into
an ID-to-position table, then validates Order and every Secondary against that
table. It retains the same bidirectional/count/uniqueness proof while avoiding
one B+Tree point lookup per cross-reference. The table replaces the previous
Order de-duplication map, so explicit audit memory remains O(document count);
normal open, query and commit paths never allocate it.

The optional receipt path is created with no-overwrite semantics and emits
schema 4 JSON containing claimed and actual binary revisions, dirty status,
profile, Go/OS/race identity, the
actual temp-volume device/filesystem identity, requested versus completed
reopens, total and per-phase concurrent duration, per-phase commit/storage state and
write/read/index-build/reclamation work. Before removing the private shadow
build, the runner closes the database and performs an offline semantic audit
that proves its nonempty build catalog and shadow index. It then reopens, aborts
the build, proves the record is absent, closes again and performs the final
full-graph/index/FreeSpace verification plus a complete-file SHA-256. This avoids
treating a verifier's vacuous “no index builds exist” result as shadow-build
evidence. The receipt contains no database path, collection name, key, document
or query.

The command emits an immediate sanitized state event and then a heartbeat to
stderr every 30 seconds during both concurrent work and potentially long
verification. Its ordered states are `started`, `phase_running`,
`phase_verifying`, `phase_verified`, `shadow_verifying`, `shadow_verified`,
`final_verifying` and `complete`; phase states repeat for each configured
reopen. Events contain only phase numbers, durations and aggregate worker
counters—never paths, keys, collection names or documents. Heartbeat output is
operational logging and is not included in the canonical JSON receipt or either
receipt hash.

The workflow validates and archives that receipt, the command log and a
same-revision, same-volume capability receipt for 90 days. A manual `release`
profile also runs `qualification-check` and archives the resulting Level 3 packet; scheduled
`sentinel` and exploratory `custom` profiles cannot create that packet. Merely
having the workflow is not treated as evidence that a particular release
completed it: release qualification requires the archived
schema-4 `release` receipt to name the released revision, match its clean binary
revision, report `raceEnabled`,
complete every requested reopen, prove the live shadow build before its final
abort and pass the final semantic/free-space checks.

V2 physical B+Tree pages choose leaf and branch split boundaries from their
exact encoded byte sizes in linear time. This prevents a count-median split from
rejecting a valid layout when one key or value is much larger than its peers.
Deletion performs path-copy sibling merges; if replacing a parent separator with
a longer new minimum overflows that branch, the split propagates back to the
root before publication. Randomized model tests, skewed-value reopen tests and
growing-separator structural tests cover these paths. Committed split and merge
counts are exposed as bounded process-session storage metrics.

Fresh secondary indexes use a separate sorted bulk-load path. It consumes
strictly increasing complete Secondary keys, fills immutable leaves to their
encoded page budget, and constructs branch levels from child minima. Leaves are
sealed incrementally; only one leaf and the current branch frontier remain as
decoded nodes. `ApplyCreateIndex` still publishes the resulting root, index
metadata, collection catalog and Commit Log event in one COW generation. Its
entry slice is explicitly consumed, allowing in-place ordering without a second
O(n) key copy. This changes construction behavior, not the V2 page encoding.

## V1 details still not final

The checkpoint catalog now references independently addressed typed blobs:
catalog metadata, each encoded document, and each encoded B+Tree topology. Large
blobs use ordered RecordID chunk chains. Legacy monolithic snapshots remain
readable and migrate on the next checkpoint.

B+Tree topology is durable and restored directly. Each logical node is an
independently addressed index blob written on physical `IndexPage` chains;
catalog metadata records the root ordinal, node blob IDs, child IDs, and leaf
next links. Recovery rejects missing, duplicate, disconnected, cyclic, or
semantically inconsistent node graphs instead of rebuilding them from documents.

The allocator protects every page reachable from both valid meta slots, writes a
third copy-on-write generation, and only then reuses
pages belonging to the overwritten generation. Free space is reconstructed from
reachability on open, so an unsynced free-list update cannot expose a live page.
Physical page reuse seeds slot generation from checkpoint generation; stale
RecordIDs therefore cannot alias the new contents. The current format fails
closed before its 32-bit RecordID generation would wrap.

The legacy V1 in-memory B+Tree splits by key count, uses local borrow/merge on
delete, and serializes oversized node blobs through RecordID chunk chains; its
logical nodes are not constrained to one physical page. An optional persisted
free-space acceleration structure remains. Checksums detect corruption; they are
not authentication.
