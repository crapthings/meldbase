# Query correctness and hardening backlog

This file tracks the remediation work for Meldbase's core query path. Items
are ordered by dependency and risk. Check an item only after its implementation
and the listed verification are complete.

## P0 — ordering and pagination correctness

- [x] Define one canonical scalar comparison contract shared by Go query
  execution, reactive ordering, index-key ordering, and the TypeScript SDK.
  It must include `DocumentID` and binary values, preserve exact Int64/Float64
  comparison, and use UTF-8 byte ordering for strings.
- [x] Make Go `_id` sort and range predicates work, and add regressions for
  ascending/descending sort plus `$gt`/`$gte`/`$lt`/`$lte`.
- [x] Reject duplicate sort paths in Go wire and SDK query compilation.
- [x] Make seek pagination safe: use a canonical `_id` tie-breaker, handle
  missing sort values, and prevent a projected-away sort field from making a
  cursor unusable.
- [x] Add Go/TypeScript/durable/realtime differential tests for mixed values,
  missing values, Unicode, ties, and complete page traversal.

## P1 — query validation and capability boundaries

- [x] Add one `QuerySpec` validation/normalization boundary used by compile,
  wire decode, policy composition, and every public compiled-query API.
- [x] Reject zero or malformed compiled queries with `ErrInvalidFilter` rather
  than allowing a nil expression panic.
- [x] Make compiler and decoder limits identical: node count, depth, array
  items, per-value bytes, and final wire bytes.
- [x] Align Go and TypeScript strict wire-value rules, including canonical IDs,
  nested value cardinality, null handling, extra fields, and path byte limits.
- [x] Split policy permission checks by filter operator, sort path, aggregate
  path, and result path; do not treat all query-path use as equivalent.

## P1 — operational query budgets

- [x] Introduce execution budgets for documents examined, index keys examined,
  candidates/sort bytes, skip, and deadline/cancellation.
- [x] Apply the budgets consistently to reads, counts, group counts, and
  filtered mutations, with a stable query-budget error and metrics.
- [x] Avoid materializing all matches when a sorted/limited query can use a
  bounded Top-K strategy or an index-provided order.

## P2 — planning, observability, and documentation

- [x] Add index plans for singleton/multi-value `$in`, eligible `$or`, and
  order-compatible compound indexes; retain residual predicate validation.
- [x] Make Explain accept compiled query options and report bounds, residual
  predicate, sort work, estimates, and actual scan counts.
- [x] Publish the query semantic contract: null vs missing, arrays, range type
  rules, total sort order, pagination consistency, limits, and index scope.
- [x] Expand the shared conformance corpus and add property/fuzz tests for
  compiler round trips, comparison laws, planner equivalence, and policy
  composition.
