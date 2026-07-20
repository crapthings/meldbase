# First-stage MVP audit

This audit maps the original two planning notes to executable evidence. It
distinguishes first-stage acceptance from later product claims.

## Required capabilities

| Area | Status | Evidence |
| --- | --- | --- |
| Closed document values and generated `_id` | Complete | `value.go`, `database_test.go`, shared wire corpora |
| CRUD, nested paths, arrays, required filters and updates | Complete | `database_test.go`, `update_property_test.go`, `testdata/*-conformance.json` |
| Go/TypeScript isomorphic data-only queries | Complete | `query_wire.go`, `sdk/client`, shared Go/TS conformance tests |
| Fixed pages, catalog, slotted records and generation-safe RecordID | Complete | `internal/storage`, storage corruption/reuse tests |
| WAL-before-visibility and restart recovery | Complete | `internal/wal`; bounded automatic V1 checkpoint policy; WAL-frame, page/Meta and checkpoint/reset crash-boundary matrices |
| Compound/unique/range B+Tree and stable tuple keys | Complete | one-to-four ascending/descending fields, longest-left-prefix planning, missing-suffix partial tuples, local borrow/merge deletion, V2 encoded-byte splits and growing-separator resplit tests; randomized model/fuzz tests; indexed/scan equivalence tests |
| Durable index topology | Complete | independent physical IndexPages and graph-validation tests |
| Offline semantic index verification | Complete | schema-3 byte-preserving verification recomputes published and phase-provable shadow Secondary keys from canonical Primary documents and proves reverse completeness, including applied-root retention, compound/partial tuples and count-consistent missing-entry corruption; legacy caught-up shadows are explicitly marked unproven |
| Additive format-extension evidence | Complete | immutable base/shadow/compound fixtures plus an applied-root page-delta artifact with independent reconstruction, exact writer reproduction, semantic audit, publication and reopen tests |
| Complete revision-3 PageType byte coverage | Complete | a byte-exact openable multi-level business fixture reaches all nine B+Tree branch kinds and all 22 current PageTypes; the production-built branch corpus isolates codec evidence; an exhaustive enum-to-artifact test prevents silent coverage regressions |
| Rule planner, sort/skip/limit and Explain | Complete | `indexes.go`, planner tests and query property tests |
| Ordered post-commit change feed and full-snapshot live query | Complete | `reactive.go`, atomic batch and slow-consumer tests |
| Secured HTTP mutations and WebSocket subscriptions | Complete | `internal/server`, policy/origin/ticket/limit tests |
| JWT-backed workspace isolation | Alpha path complete | HS256 and HTTPS JWKS/RS256 authenticators validate issuer, audience, expiry, subject and active workspace; `WorkspaceAuthorizer` server-injects workspace constraints, owns insert fields and denies workspace mutation; HTTP and JWT-ticket realtime isolation tests |
| Typed request/response RPC foundation | Complete | public `server` facade, explicit RPC authorization, bounded HTTP and multiplexed WebSocket call/cancel, optional TS socket routing, disconnect result-unknown/no-retry semantics, and panic/error isolation tests |
| Durable RPC idempotency | Alpha path complete | canonical principal/request hashing; fail-closed HTTP/WebSocket behavior; V2 private System tree with CAS, overflow values and bounded pruning; optimistic transactional point methods with atomic business/result publication; concurrent/conflict/reopen/compaction/reclamation and ENOSPC crash-boundary tests |
| Public optimistic write transaction | Alpha path complete | V2 multi-collection point overlay with read-your-writes, compiled updates, unique-index value swaps, atomic canonical point-read-set CAS, disjoint-commit concurrency, callback-time entry/byte admission, one reactive token, reopen proof and fixed terminal observability; V1 fails explicitly without changing its frozen WAL grammar |
| Reconnect and secure fallback | Complete | context-bound signed tokens, explicit `resumed`/`resync_required`, Go server and TS reconnect tests |
| Historical delta resume | Alpha path complete | Default-new V2, explicit `OpenV2`, real server reconnect and Snapshot N + ordered N+1 delta replay are integration/race tested; detected legacy V1 safely resynchronizes |
| Native JS/TS and React clients | Complete | `sdk/client`, `sdk/react`, shared query/mutation corpora, immutable cross-language protocol-v1 frame/capability contract and React external-store test |
| Server JavaScript method/publication SDK | Alpha path complete | private authenticated WorkerHub; single-owner method and Go-predeclared collection registration; typed ordinary/transactional method frames including negotiated compiled updates; data-only policy constraints intersected in Go; lease revocation; Go-owned point transactions; reconnect/cancel and real/fake-socket protocol tests |
| Runnable demo and realtime todo example | Complete | `cmd/meld`, `examples/realtime-todos`, CLI test and Vite production build |
| Bounded observability control plane | Complete | O(1), allocation-free `DBStats`; fixed sampler/history; authenticated SSE; embedded panel; sanitized diagnostic ring; versioned field/type/optionality golden schema; allocation-budgeted Prometheus and zero-allocation OTel aggregate adapters; dedicated-runner relative-p99 workflow; and explicit bounded runtime traces/profiles |
| Core resource governance | Complete | canonical document/transaction and index-build entry/byte limits, bounded mutation changes, atomic rejection, fixed transport error and aggregate telemetry; V2 Commit Log count/byte auto-retention plus reuse-first physical file quota with non-fail-stop rejection |
| Unit, integration, recovery, concurrency, property/fuzz and benchmarks | Complete | `go test -race ./...`, WAL boundary matrix, concurrent-reader test, B+Tree fuzz, six required operation benchmarks |

## Deliberate design corrections

- `_id` is an intrinsic unique primary-key map with an `ID_LOOKUP` plan. A
  duplicate B+Tree would add write amplification without improving V1 behavior.
- A synced WAL frame is one complete atomic mutation batch. There is no exposed
  half-transaction Begin/Commit state requiring undo recovery.
- HTTP update and delete share a versioned `MutationSpec` endpoint so browser,
  TypeScript, authorization, and the Go engine cannot drift between ad-hoc REST
  path semantics.
- Public filters are compiled into bounded AST data. Neither server nor client
  evaluates JavaScript, callbacks, arbitrary regex, or source strings.
- Resume checkpoints retain only contiguous commit positions, not historical
  Before/After document images. This is sufficient for V1 continuity validation
  and avoids extending the lifetime of deleted or redacted data.
- New checkpoints are catalog-rooted copy-on-write generations. V2 persistent
  free-space snapshots are optional acceleration; audit generation filtering
  and reachability across both valid meta pages prevent stale candidates from
  becoming allocation authority.

## Explicitly post-MVP

These are not claimed by the current build: exhaustive platform/filesystem durability evidence,
pause-free background reclamation, offline mutation/conflict
resolution, clustering, sharding, distributed transactions, complex aggregation,
and specialized indexes.
Automated visual browser QA is also not part of the current gate; the example is
type-checked and production-built, while protocol behavior is covered by real
server/Node and fake-socket tests.

The storage format is versioned; its V2 Meta negotiation envelope and current
declared revision-3 page layouts have byte-exact cross-release fixtures.
Checksum-valid unknown required features or revisions fail explicitly rather
than downgrading through the other Meta slot. A deterministic full business
graph fixture reaches all current PageTypes and proves reader open/audit,
historical reconstruction and advance/reopen behavior. This is a format
compatibility contract, not evidence of power-loss correctness on every target
filesystem. `Open` detects existing V1/V2
without migration and creates new V2 databases; explicit `OpenV2` remains
available, and an open V1 DB can create a separately audited V2 copy with
`MigrateToV2`. Broader migration/crash matrices and operational tooling remain
necessary before a production-ready claim.
