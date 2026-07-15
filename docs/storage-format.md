# Storage format

The current experimental format uses 16 KiB pages and a sidecar redo WAL. It is
versioned but not frozen for compatibility yet.

## Main file

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

## WAL

Each mutation or catalog operation is one framed commit record with an increasing
token, bounded payload length, and checksum. The record is synced before its
changes become visible in memory or to reactive subscribers. Recovery replays
only records newer than the checkpoint token. A provably partial tail is removed;
a complete record with a bad checksum is corruption and fails open.

After a successful checkpoint meta sync, the WAL is reset and synced. A crash
between those steps is safe because recovery filters records already represented
by the checkpoint token.

## Not final yet

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

The in-memory B+Tree still splits primarily by key count; oversized node blobs
therefore use RecordID chunk chains. A later format can add byte-budgeted splits
and overflow-value pages without changing catalog identity. An optional persisted
free-space acceleration structure also remains. Checksums detect corruption;
they are not authentication.
