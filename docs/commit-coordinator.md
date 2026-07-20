# CommitCoordinator design

This is the required design before Meldbase enables group commit. It is based on
the current V2 publication algorithm, not on an assumption that repeated
`fsync` calls can safely be removed.

## Safety finding

V2 currently writes all new COW pages, syncs them, writes the inactive Meta
slot, then syncs it. The data-before-Meta barrier is what makes either existing
Meta slot a recoverable root after a process or power loss.

It is **unsafe** to run several existing `File.Update` calls with either
barrier suppressed and merely sync once at the end. Each call alternates the
two Meta slots. Before the final sync both slots may have been overwritten by
unacknowledged roots, while pages reachable from an earlier acknowledged root
may also have become reusable. A crash could then lose the last acknowledged
root or select a Meta whose reachability is incomplete.

Therefore phase one must not implement group commit as a loop around the
current `Update` method.

## Required storage shape

One group is a single physical COW generation containing ordered logical
commits `S+1 ... S+n`:

```text
durable root S
  -> build n ordered logical mutations in one WriteTxn
  -> append CommitBatch(S+1), ... CommitBatch(S+n), each with its own CatalogRoot
  -> sync all newly written pages once
  -> write one final Meta (sequence S+n, final CatalogRoot) to the inactive slot
  -> sync Meta once
  -> acknowledge the ordered group
```

Only the final Meta is published. Intermediate Catalog roots are immutable
pages retained by the final Commit Log, so historical replay can reconstruct
Snapshot `S+k` even though no intermediate Meta was written. The group must use
one `WriteTxn`; staging several ordinary COW generations separately is not
safe because pages freed by an uncommitted intermediate root cannot be reused
until the final Meta is durable.

This requires a new internal storage transaction primitive, not a weakening of
the V2 revision-3 reader/writer contract. The existing single-commit encoding
remains the default and reader-compatible; any on-disk group marker or changed
recovery interpretation requires an additive feature/revision decision and
fresh byte fixtures.

## Coordinator semantics

The coordinator owns the one writer lane. It admits only validated work and
never runs user callbacks, RPC handlers or authorization inside its batch.

- Admission order fixes logical sequence order.
- A request canceled before admission does not enter storage.
- After admission, cancellation cannot prove absence. The caller receives the
  existing operation-specific uncertain outcome rather than an unsafe retry
  signal.
- Every member receives success only after the final data sync, final Meta sync
  and, when configured, rollback-anchor advancement to the group's final
  sequence/generation.
- A logical conflict rejects that request without publishing it; earlier
  accepted members may still form a smaller group. I/O, checksum or anchor
  failure is database fail-stop and no later member is acknowledged.
- Reactive/query publication occurs only after the whole group is durable. The
  dispatcher then receives its `ChangeBatch` objects in increasing sequence.

Phase one groups ordinary V2 document mutations only. This includes a completed
public `RunWriteTransaction`: its callback has already closed, so the group owns
only frozen changes and an exact point-read set. A candidate conflict never
reruns that callback; the coordinator revalidates and publishes the frozen
operation in admission order, or returns its final conflict. A rollback-protected
group advances its external anchor from the final durable Meta coordinate before
acknowledging a member. An anchor error, including one that follows a successful
remote persist but loses its response, is fail-stop: no member succeeds and a
reopen reconciles the durable file and retained anchor. Index build maintenance,
transactional RPC terminal records, compaction, migration, backup, recovery
repair and any special operation with a separate physical lifecycle stay
exclusive until each receives its own group proof.

The anchor keeps two independently monotonic coordinates. Commit sequence is
logical and can advance by the number of group members; generation is physical
and advances once for the final Meta publication. Anchor backends must not
assume a fixed offset between them.

## Recovery and qualification matrix

For a group that starts from `S` and ends at `S+n`, recovery may observe only:

| Crash point | Allowed recovered state |
| --- | --- |
| Any staged-page write; before group data sync | exactly `S` |
| After group data sync; before final Meta write | exactly `S` |
| After final Meta write; before final Meta sync | exactly `S` or exactly `S+n` |
| After final Meta sync | exactly `S+n` |

With rollback protection, there is one additional acknowledgement interval
after the final Meta sync and before the anchor advances. A process loss there
may leave durable database state `S+n` and a retained anchor at `S`; no member
has been acknowledged. Reopen must retain the whole group and synchronously
advance the anchor to `(S+n, final generation)` before accepting operations.

No recovered prefix `S+k` where `0 < k < n` is allowed. Every allowed state
must pass full reachability, Commit Log continuity, canonical index audit and
reopen. Disk-full, injected write/flush errors and abrupt-process-loss tests
must exercise the same four outcomes. Physical filesystem/power qualification
remains a separate release gate.

## Delivery sequence

1. **Implemented, internal-only:** `ApplyDocumentTransactionGroup` builds two
   to 256 ordinary document transactions in one V2 `WriteTxn`, retains one
   ordered `CommitBatch` and CatalogRoot per logical sequence, and publishes a
   single final Meta. Its tests prove historical Snapshot N reconstruction,
   reopen/reachability and an abrupt-process-loss matrix with only whole-group
   outcomes. It is deliberately not reachable from public `DB` or server APIs yet.
   The DB-side `commitV2ChangeBatchesLocked` companion now also maps ordered
   `ChangeBatch` values to that primitive and preserves per-sequence DB stats,
   reactive dispatch and direct watcher ordering. It remains an internal
   coordinator boundary, not a CRUD fast path.
2. **Implemented:** ENOSPC and abrupt-process-loss matrices cover whole-group
   recovery plus historical replay from retained intermediate Catalog roots.
   Flush-error qualification remains part of the later physical campaign.
3. **Implemented, opt-in:** V2 now exposes `V2CommitCoordinatorOptions` on
   `OpenV2WithOptions` and format-neutral `OpenWithOptions`. It is disabled by
   default. When enabled, ordinary `Collection.InsertMany`, filter
   `UpdateOne/UpdateMany`, filter `DeleteOne/DeleteMany`, and completed public
   `RunWriteTransaction` calls enter a
   bounded FIFO admission queue (defaults: 32 requests, 1,024 pending, 1 ms
   coalescing window). A group of two to 256 requests reaches the internal
   group publisher; one request uses the established single-commit path.
   Logical duplicate/unique/index/resource conflicts reject the speculative
   group before publication and fall back in admission order to the exact
   single-request semantics, so one bad request cannot reject its valid peers.
   I/O, checksum and durability errors remain fail-stop. Cancellation before
   admission returns the Context error. Cancellation after admission returns
   `ErrCommitOutcomeUnknown`; inserts retain generated document ID(s), and
   mutations retain their selected result so callers can reconcile rather than
   retry blindly. A public write transaction instead waits for its final durable
   outcome after admission because it has no separate reconciliation handle and
   its callback must not be rerun. `DB.Close` first closes
   admission and lets a currently owned group finish before closing the V2
   file. `DB.CommitCoordinatorStats()` exposes the fixed Go snapshot of queue
   capacity/depth, admissions, rejection, batches, grouped transactions and
   uncertain outcomes. Embedding it in the admin HTTP/SSE payload remains a
   separate versioned schema change; existing admin metrics stay stable in this
   phase. The coordinator also supports external rollback-anchor stores: tests
   cover final-coordinate advancement, snapshot rollback rejection, and an
   anchor that persists then reports an acknowledgement failure. Protocol-v1 HTTP/WebSocket code
   maps this local error to its existing `rpc_outcome_unknown` contract rather
   than adding an incompatible new error-code spelling.
4. **Implemented publicly:** the DB group boundary accepts one V2
   `DocumentPrecondition` read set per logical member. Storage validates each
   set against that member's preceding in-group CatalogRoot. Tests prove that
   independent updates/deletes group successfully, while two members based on
   the same stale document reject as a whole with no prefix publication. A
   filter mutation takes its storage snapshot, selected before images, result
   count and read set during admission. If the speculative group has a logical
   conflict, the writer re-evaluates each original filter in admission order
   through the established single-request path, preserving count and index
   semantics rather than publishing an old selection. A public transaction uses
   the exact frozen changes/read set instead; its callback is never re-executed.
5. Compare dedicated-volume throughput, p99, write amplification and recovery
   evidence with the baseline. Enable only when the default synchronous contract
   and qualification evidence remain stronger than the current path.

Clustering, follower replication and public change-data-capture are consumers
of this boundary; they are not prerequisites for the single-node coordinator.
