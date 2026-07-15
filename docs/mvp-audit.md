# First-stage MVP audit

This audit maps the original two planning notes to executable evidence. It
distinguishes first-stage acceptance from later product claims.

## Required capabilities

| Area | Status | Evidence |
| --- | --- | --- |
| Closed document values and generated `_id` | Complete | `value.go`, `database_test.go`, shared wire corpora |
| CRUD, nested paths, arrays, required filters and updates | Complete | `database_test.go`, `update_property_test.go`, `testdata/*-conformance.json` |
| Go/TypeScript isomorphic data-only queries | Complete | `query_wire.go`, `sdk/typescript`, shared Go/TS conformance tests |
| Fixed pages, catalog, slotted records and generation-safe RecordID | Complete | `internal/storage`, storage corruption/reuse tests |
| WAL-before-visibility and restart recovery | Complete | `internal/wal`, recovery and crash-boundary matrix tests |
| Single-field/unique/range B+Tree and stable scalar keys | Complete | `internal/index`, randomized model/fuzz tests, indexed/scan equivalence tests |
| Durable index topology | Complete | independent physical IndexPages and graph-validation tests |
| Rule planner, sort/skip/limit and Explain | Complete | `indexes.go`, planner tests and query property tests |
| Ordered post-commit change feed and full-snapshot live query | Complete | `reactive.go`, atomic batch and slow-consumer tests |
| Secured HTTP mutations and WebSocket subscriptions | Complete | `internal/server`, policy/origin/ticket/limit tests |
| Reconnect and secure resume | Complete | context-bound signed tokens, checkpoint/WAL position window, Go server and TS reconnect tests |
| Native JS/TS and React clients | Complete | `sdk/typescript`, `sdk/react`, 18 TS tests and React external-store test |
| Runnable demo and realtime todo example | Complete | `cmd/meld`, `examples/realtime-todos`, CLI test and Vite production build |
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
- New checkpoints are catalog-rooted copy-on-write generations. A persisted
  free-list is optional acceleration; reachability across both valid meta pages
  remains the crash-safety authority.

## Explicitly post-MVP

These are not claimed by the current build: WAL embedded into the main file,
automatic checkpoint scheduling, streaming query-plan iterators, local B+Tree
delete rebalance and byte-budgeted splits, incremental realtime change frames,
offline mutation/conflict resolution, clustering, sharding, distributed
transactions, complex aggregation, and specialized indexes. Automated visual
browser QA is also not part of the current gate; the example is type-checked and
production-built, while protocol behavior is covered by real server/Node and
fake-socket tests.

The storage format is versioned but not frozen, and the project remains
experimental rather than production-ready.
