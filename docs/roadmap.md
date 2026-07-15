# Roadmap

## M0 — foundation

- [x] Module, product contract, closed document value model, errors, tests.

## M1 — semantic engine

- [x] In-memory collections, CRUD, compiled filters, update operators, cursors.
- [x] Sort, skip, limit, indexed plans, explain results, server projection.
- [x] Property-style indexed/scan query equivalence and reference-model update tests.

## M2 — durable records

- [x] Versioned pager, double meta pages, copy-on-write checkpoints, checksums.
- [x] Reopen, corruption, large snapshot, locking, and multi-collection tests.
- [x] Slotted record pages, generation-safe RecordID, and catalog-rooted checkpoints.
- [x] Independently addressed document/index blobs and collection catalog entries.
- [x] Crash-safe obsolete-generation reclamation by dual-meta reachability.
- [ ] Optional persisted free-space map for faster open; reachability remains authoritative.

## M3 — transactions and recovery

- [x] Framed redo WAL, atomic commit batches, checkpoints, recovery.
- [x] Partial-tail, checksum corruption, stale WAL, and durability-poison tests.
- [ ] Exhaustive crash-point/property matrix and automatic checkpoint policy.

## M4 — indexes and planning

- [x] Exact ordered scalar key codec, including lossless mixed Int64/Float64.
- [x] B+Tree split/range iteration, uniqueness, safe rebuild deletion.
- [x] Rule planner for `_id`, equality, range, and `Explain` evidence.
- [x] Persist individual B+Tree nodes as IndexPages with catalog-rooted topology.
- [ ] Replace rebuild-on-delete with local merge/rebalance and byte-budgeted splits.

## M5 — live data layer

- [x] Durable commit tokens and bounded subscriptions.
- [x] Bounded snapshot live queries, unsubscribe and cleanup.
- [x] Checkpoint/WAL-retained resume positions and context-bound signed resume tokens.
- [x] TypeScript local query engine and native reactive collection.
- [x] TypeScript HTTP/WebSocket client state machine and safe wire codec.
- [x] Secured HTTP query/insert and WebSocket snapshot server.
- [x] Shared local/remote insert, update, and delete mutation wire APIs.
- [x] Thin React `useLiveQuery` adapter over native live-query objects.

## M6 — MVP proof

- [x] Go end-to-end demo covering durability, indexes, updates, and reactive snapshots.
- [x] Browser realtime todo example using the TypeScript and React SDKs.
- [x] Race, recovery boundary, fuzz/property, benchmark, and Go/TypeScript compatibility-gate suite.

Explicitly deferred: MongoDB wire/BSON compatibility, clustering, sharding,
distributed transactions, offline conflict resolution, geospatial/full-text/vector
indexes, TTL, complex aggregation, and a built-in authorization product.
