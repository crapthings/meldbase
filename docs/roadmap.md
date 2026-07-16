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
- [x] Optional persistent V2 free-space audit snapshots with generation-filtered
  restart recovery; dual-meta/read-pin reachability remains authoritative.

## M3 — transactions and recovery

- [x] Framed redo WAL, atomic commit batches, checkpoints, recovery.
- [x] Immutable schema-versioned startup recovery receipt across memory, V1 and
  V2, including Meta/root selection, WAL replay, provable tail removal and
  optional-accelerator degradation; no unsafe online fail-stop reset.
- [x] Partial-tail, checksum corruption, stale WAL, and durability-poison tests.
- [x] V1 WAL byte/commit-bounded automatic checkpoint policy plus framed-WAL,
  page/meta publication and checkpoint/reset crash-boundary matrices.

## M4 — indexes and planning

- [x] Exact ordered scalar key codec, including lossless mixed Int64/Float64.
- [x] B+Tree split/range iteration, uniqueness, safe rebuild deletion.
- [x] Rule planner for `_id`, equality, range, and `Explain` evidence.
- [x] Persist individual B+Tree nodes as IndexPages with catalog-rooted topology.
- [x] Replace rebuild-on-delete with local borrow/merge, use encoded-byte-balanced
  V2 leaf/branch splits, and resplit deletion paths when separators grow.
- [x] Add a sorted immutable-tree bulk loader and optimistic V2 index snapshot
  extraction so long scans do not hold the database writer mutex; retain atomic
  catalog publication and bounded conflict retries.
- [x] Add a negotiated persistent shadow-index build catalog, bounded Commit Log
  catch-up and crash-resume/abort semantics for write-heavy background builds;
  the required format/state contract is pinned in `docs/index-builds.md`.
- [x] Persist the exact applied CatalogRoot for catch-up generations and audit
  scan prefixes/caught-up shadow trees bidirectionally without adding projector
  work to normal read, commit or reclamation paths.

## M5 — live data layer

- [x] Durable commit tokens and bounded subscriptions.
- [x] Bounded snapshot live queries, unsubscribe and cleanup.
- [x] Checkpoint/WAL-retained resume positions and context-bound signed resume tokens.
- [x] TypeScript local query engine and native reactive collection.
- [x] TypeScript HTTP/WebSocket client state machine and safe wire codec.
- [x] Secured HTTP query/insert and WebSocket snapshot/delta server.
- [x] Shared local/remote insert, update, and delete mutation wire APIs.
- [x] Shared incremental Reactive Views, ordered query deltas, connection-local
  visibility projection, and opaque client-visible token chains.
- [x] Thin React `useLiveQuery` adapter over native live-query objects.
- [x] Public Go server facade and versioned typed HTTP RPC with explicit method
  authorization, bounded concurrency/results, cancellation and TS `client.call`.
- [x] Add server-side WebSocket RPC multiplexing/cancel frames with shared global
  and per-connection budgets and disconnect cancellation.
- [x] Add optional TypeScript socket routing with shared subscription connection,
  AbortSignal cancellation, result-unknown disconnect errors and no call retry.
- [x] Define fail-closed RPC idempotency states and add the shared HTTP/WebSocket
  store adapter, canonical request fingerprints, TS key option and fixed metrics.
- [x] Add the built-in V2 private system tree, overflow records, retention,
  compaction/reclamation and crash-boundary matrix for durable idempotency.
- [x] Add optimistic transaction-aware point-method execution with atomic V2
  business/result publication without changing HTTP or WebSocket semantics.
- [x] Expose the shared optimistic V2 point transaction to Go applications with
  multi-collection atomic publication, compiled updates, one reactive token,
  exact terminal observability and explicit no-retry/conflict semantics.
- [x] Replace database-wide transaction sequence rejection with atomic canonical
  point-read-set CAS, disjoint-commit concurrency and callback-time overlay
  entry/byte admission; prove current-index maintenance and conflict recovery.
- [x] Add a server-side JavaScript method worker SDK over the same authorization,
  idempotency, point-transaction and fixed-cardinality observability contracts.
- [x] Add bounded server-side publication/policy functions over the worker
  protocol: Go-declared authority domains, static field/result caps, data-only
  constraint ASTs, local-policy intersection and disconnect-driven lease
  revocation without allowing workers to stream documents to clients.
- [x] Map database closure and durability fail-stop to one non-sensitive
  `database_unavailable` HTTP/WebSocket contract, retain
  `rpc_outcome_unknown` at uncertain commit boundaries, and expose structured
  TypeScript remote errors without automatic mutation/RPC retry.
- [x] Add bounded, opt-in protocol version/capability discovery for public
  realtime/RPC tickets and private server workers, including legacy rolling
  upgrade behavior, clean no-resume fallback and strict anti-downgrade modes.

## M6 — storage/replay hardening

- [x] Isolated Storage V2 COW B+Tree, durable Commit Log, streaming iterators,
  retention cursors, historical CatalogRoot snapshots, and N+1 live replay.
- [x] Internal historical snapshot-to-Shared-View bridge with durable insertion
  order and commit-image replay through the live incremental transition engine.
- [x] Server replay-source contract and `resumed` handshake, with explicit safe
  resync on unavailable, retained-out, corrupt, or policy-revoked history.
- [x] Explicit alpha `OpenV2` engine/server integration, atomic persistent
  secondary indexes, reopen audit, and real historical query deltas after
  reconnect.
- [x] Allocation-free DB/V2 statistics for query, realtime, durability, physical
  pages, cache health, retention watermarks and replay leases.
- [x] Atomic V2 Commit Log count plus canonical-byte retention with replay-lease
  precedence, open-time byte reconstruction, pressure/overage signals and
  crash-boundary coverage.
- [x] Format-neutral canonical document and transaction admission limits with
  atomic rejection, stable transport errors and aggregate observability.
- [x] Page-aligned V2 physical high-water quota with reuse-first allocation,
  pre-I/O rejection, non-fail-stop transport semantics and fixed telemetry.
- [x] Optional fixed-history admin sampler with derived rates, latest-only
  subscriber coalescing, explicit authentication and an isolated SSE transport.
- [x] Fixed derived health contract for database fail-stop state, reactive
  capacity pressure and windowed durability/realtime/telemetry/transport signals,
  exported through admin JSON, dashboard, Prometheus and OpenTelemetry.
- [x] Allocation-free database operational-state contract plus distinct public
  liveness and read/write readiness probes with fail-stop 503 semantics and
  fixed low-information responses.
- [x] Dependency-free embedded developer dashboard with in-memory credentials,
  restrictive CSP and a loopback-only development CLI integration.
- [x] Default-off, sampled fixed-capacity query/commit diagnostic ring with
  sanitized events, incremental admin pagination and panel rendering.
- [x] Diagnostics on/off throughput/allocation/p99 benchmarks and an opt-in
  dedicated-runner p99 budget gate, plus a manual revision-bound workflow that
  archives core/sampler/exporter/server/OTel benchmark evidence.
- [x] Authenticated, opt-in Prometheus text exporter with a fixed low-cardinality
  schema and no database hot-path dependency.
- [x] Separate OpenTelemetry aggregate adapter over immutable admin samples plus
  explicit time/byte-bounded Go runtime trace, CPU and heap captures; the core
  database package imports neither the OTel API nor SDK.
- [x] Read-only fail-closed V1/V2 format detection and logically verified,
  no-overwrite V1-to-V2 migration preserving empty collections, order and indexes.
- [x] Migration publication commit-point fault injection for link/fsync rollback
  and no-overwrite behavior.
- [x] Pinned V2 query snapshots with direct Primary lookup, streaming Secondary
  and collection iterators, bounded decoded-document LRU, and no rebuilt query
  B+Trees on open.
- [x] Metadata-only `OpenV2`: mutation selection, CreateIndex construction,
  SnapshotQuery and initial/resync Reactive Views read pinned storage snapshots;
  live CRUD never grows a full decoded-document mirror.
- [x] Persistent insertion-Order tree, insertion-position Secondary suffixes,
  lazy/closable unsorted COLLSCAN Cursor, and explicit Primary↔Order↔Secondary
  integrity validation.
- [x] One-to-four-field compound and descending indexes with canonical tuple
  codec V3, longest-left-prefix planning, complete-tuple uniqueness, V1
  rejection, V2 required-feature negotiation, resumable shadow construction,
  backup/compaction/verification coverage and an additive golden fixture.
- [x] Safe no-overwrite `CompactToV2` logical compaction with verification,
  new identity/resume-token boundary and bounded observability.
- [x] Explicit epoch-safe page reuse protected by both valid meta roots and all
  active reader/replay roots, with cache-safe post-publication reuse.
- [x] Add disk-full publication/reuse fault matrices and a configurable large-DB
  churn/reopen/reclaim soak before switching the default `Open()` path.
- [x] Make `Open()` read-only-format-aware, create new databases as V2, preserve
  existing V1 without migration, and fail closed on orphan WAL/cross-format use.
- [x] Linearizable revocable query-policy leases and safe reauthorization via
  `resync_required` when policy versions change.
- [x] Connect server-SDK publication sources to revocable leases, including
  local-policy intersection and fail-closed Worker disconnect/reconnect.
- [x] Define durable, commit-ordered policy invalidation events for authorization
  semantics that depend on writes outside the publication's queried collection.

## M7 — MVP proof

- [x] Go end-to-end demo covering durability, indexes, updates, and reactive snapshots.
- [x] Browser realtime todo example using the TypeScript and React SDKs.
- [x] Race, recovery boundary, fuzz/property, benchmark, and Go/TypeScript compatibility-gate suite.

## M8 — production format contract

- [x] Freeze the V2 Meta compatibility envelope and distinguish checksum-valid
  unsupported revisions/features from torn corruption without downgrade fallback.
- [x] Add a checked-in revision-3 Meta golden fixture that pins encoded bytes,
  checksum and page digest, then opens, advances and reopens it in CI.
- [x] Add a compressed deterministic revision-3 business-graph fixture covering
  all current authoritative/overflow/FreeSpace page families, exact writer bytes,
  reader audit and next-generation reopen.
- [x] Add read-only V1/V2 Meta inspection and a JSON CLI compatibility gate that
  reports checksum-valid future revisions/features without opening or downgrade.
- [x] Add shared-lock, byte-preserving V2 full-graph verification with a public
  schema-versioned report and offline CLI, including bidirectional canonical
  published/shadow-index proof, complete-file SHA-256, optional FreeSpace
  status, cancellation and active-writer rejection.
- [x] Add an applied-root revision-3 page-delta fixture that composes over the
  original business artifact, pins exact changed pages/compressed bytes and
  proves independent reader reconstruction, semantic audit, publication and
  next-generation reopen without rewriting earlier fixtures.
- [x] Add a revision-3 branch-page corpus generated by the production bulk
  builder for all nine B+Tree kinds, plus an exhaustive fixture-coverage gate
  proving every current PageType has byte-level reader/writer evidence.
- [x] Add a full openable business-database cross-release fixture whose reachable
  graph contains all nine branch kinds and all 22 current PageTypes, with exact
  writer reproduction, semantic published/shadow-index audit, persistent
  FreeSpace validation, historical snapshot reconstruction, advance and reopen.
  The separately negotiated shadow, applied-root and compound extensions retain
  their additive fixtures rather than rewriting earlier release evidence.
- [x] Run the format/locking/fault-injection portability subset on Linux and macOS
  CI while keeping real power-loss/filesystem qualification explicitly pending.
- [x] Add an abrupt subprocess-exit publication matrix at V2 page/data-sync/Meta
  boundaries, followed by lock reacquisition, reopen and reachability audit.
- [x] Add a target-volume CLI capability probe for file/directory sync, locks,
  no-overwrite link, rename and end-to-end V2 commit/reopen; run it on Linux/macOS CI.
- [x] Add exact V2 physical backup with writer-bound snapshot consistency,
  full-file hash/reopen/reachability verification, no-overwrite publication,
  fixed observability and a schema-versioned offline CLI receipt.
- [ ] Qualify fsync, rename, locking and disk-full behavior on the supported OS and
  filesystem matrix. The schema-2 target-volume runner and evidence-level contract
  are ready; actual destructive per-filesystem receipts remain required.
- [x] Add optimistic low-pause reclamation plus an explicit default-off,
  deadline-bounded, lifecycle-safe background scheduler and fixed telemetry.
- [x] Add a configurable timed race soak and scheduled 30-minute sentinel for
  concurrent writer/persistent shadow-index catch-up/snapshot/online-reclaim
  plus full verification after reopens, with a no-overwrite schema-4 receipt
  bound to the target volume and VCS-stamped binary,
  explicit sentinel/release profiles, non-vacuous pre-abort shadow verification
  and archived per-phase evidence.
- [x] Add a strict cross-receipt qualification verifier that binds the clean
  capability probe and race-enabled release soak to one revision and exact
  target volume, hashes both inputs, distinguishes Level 3 from Level 4, and
  requires a separately secured destructive-test record before emitting a
  production-qualified packet.
- [ ] Add multi-hour release soak evidence under concurrent writes, readers,
  reclamation conflicts and repeated reopen.

Explicitly deferred: MongoDB wire/BSON compatibility, clustering, sharding,
distributed transactions, offline conflict resolution, geospatial/full-text/vector
indexes, TTL, complex aggregation, and a built-in authorization product.
