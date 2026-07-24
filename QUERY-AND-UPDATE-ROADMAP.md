# Query and update roadmap

Meldbase adopts familiar document-query vocabulary only where it fits its own
semantics, safety boundary, and consistent Go, TypeScript, and server
contracts. This roadmap is not a MongoDB compatibility promise.

**Status as of 2026-07-24:** the audited baseline includes `$size`, `$type`,
`$all`, and `$elemMatch` across Go, TypeScript, wire encoding, authorization,
memory and durable backends, Update/Delete, realtime queries, and shared
conformance. The predicate-work budget covers execution, Explain, metrics, and
the observer; observer runs can vary array length and duplication to compare
predicate-work amplification for `$all` and `$elemMatch`.

## Design principles

- [x] Define Meldbase semantics before adopting a familiar operator name.
- [x] Keep the query protocol data-only: no scripts, functions, expression
  strings, or arbitrary code.
- [x] Use one semantic contract for local, remote, HTTP, WebSocket, realtime,
  Update, and Delete operations.
- [x] Distinguish missing fields, `null`, empty arrays, empty objects, and type
  mismatch.
- [x] Apply input-size, nesting-depth, node-count, and execution-work budgets
  to each supported operator; do not expose operators that cannot be safely
  accounted for.
- [x] Use indexes only to generate candidates; always re-evaluate the complete
  predicate.
- [x] Expose index use, residual predicates, scan work, and budget pressure in
  Explain.
- [x] Exclude arbitrary-code execution such as `$where`.

## Audited baseline

- [x] Query operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`,
  `$nin`, and `$exists`.
- [x] Logical operators: `$and`, `$or`, and `$not`.
- [x] Query options: sorting, `skip`, `limit`, and seek pagination.
- [x] Update operators: `$set`, `$unset`, `$inc`, `$push`, and `$pull`.
- [x] `_id`, scalar-array membership, missing-field, and `null` semantics.

## Array queries

### `$elemMatch`

- [x] Account for array-element traversal through the predicate-work budget.
- [x] Support scalar array elements.
- [x] Support nested conditions for object array elements.
- [x] Define missing, non-array, and empty-array behavior as non-matching; reject
  an empty operand.
- [x] Keep multiple conditions bound to one array element rather than combining
  different elements.
- [x] Support nesting with `$and`, `$or`, `$not`, and outer query conditions.
- [x] Cover Go, TypeScript, wire, memory, and durable conformance.
- [x] Start as a residual predicate; consider array-index optimization only with
  workload evidence.

### `$all`

- [x] Reuse the predicate-work budget so large document arrays cannot bypass
  query accounting.
- [x] Require the array to contain every requested value.
- [x] Deduplicate query values while preserving first-occurrence order.
- [x] Reject an empty operand; missing and non-array fields do not match.
- [x] Define structural equality for objects, `null`, and missing fields, and
  allow composition with `$elemMatch` and logical conditions.
- [x] Defer multi-value index intersection; the initial implementation remains
  residual-only.

### `$size`

- [x] Support array-length queries.
- [x] Accept only integral values in `0..2^53-1`; reject negative, floating,
  and unsafe integer values.
- [x] Treat non-array and missing fields as non-matching; an empty array matches
  `0`.
- [x] Support composition with `$and`, `$or`, `$not`, and other field
  conditions.
- [x] Keep it residual while allowing another predicate in the same `$and` to
  provide indexed candidates.

## Type and string queries

### `$type`

- [x] Define Meldbase type names: `null`, `boolean`, `int64`, `float64`,
  `string`, `date`, `id`, `binary`, `array`, and `object`.
- [x] Do not provide a `number` alias; use `["int64", "float64"]` to match
  both numeric types explicitly.
- [x] Treat a missing field as no type.
- [x] Support one type or a non-empty type array; type arrays are OR conditions
  and are canonicalized in fixed order.
- [x] Cover composition with `$exists`, `$not`, `$and`, and `$or`.
- [x] Keep the initial implementation residual-only and do not emit ordinary
  B-tree index suggestions.

### `$regex`

- [ ] Confirm a real workload need; prefer a dedicated prefix query for prefix
  search.
- [ ] Match string fields only; non-string and missing fields do not match.
- [ ] Select a safe regular-expression engine with expression length,
  complexity, and execution-time limits.
- [ ] Define UTF-8, case, newline, invalid-pattern, and flag semantics.
- [ ] Mark the initial Explain plan as a residual scan.
- [ ] Never place regular-expression text in a metrics label.

### `$mod`

- [ ] Support integer modulo queries.
- [ ] Reject a zero divisor and define negative-number behavior.
- [ ] Treat the initial form as a scan predicate and account it against the
  query budget.
- [ ] Cover interaction with range predicates, `$and`, and indexed fields.

## Field updates

- [ ] `$min`: update when the new value is smaller and define missing-field
  behavior.
- [ ] `$max`: update when the new value is larger and define missing-field
  behavior.
- [ ] `$mul`: reuse numeric precision, overflow, and non-finite-value checks.
- [ ] `$rename`: handle parent/child path conflicts and forbid `_id` changes.
- [ ] `$currentDate`: first define the Meldbase date type and clock source.
- [ ] Preserve transaction atomicity, indexes, change events, realtime views,
  and replay consistency for every update.
- [ ] Define `MatchedCount` and `ModifiedCount` for no-op updates.

## Array updates

- [ ] `$addToSet`: append only when an equal value is absent.
- [ ] `$pop`: remove the first or last element and define empty-array behavior.
- [ ] `$pullAll`: remove every matching array value.
- [ ] `$push` modifiers: `$each`, `$position`, `$slice`, and `$sort`.
- [ ] Fix `$push` modifier evaluation order and wire canonical order.
- [ ] Bound element count, value size, and final document size.

## Positional array updates

- [ ] Design a distinct mutation AST; do not treat `$`, `$[]`, and `$[name]`
  as ordinary path strings.
- [ ] Define first-matching-element behavior for `$`, or adopt a clearer
  Meldbase-specific name.
- [ ] Define `$[]` as updating all array elements.
- [ ] Define `$[name]` with bounded `arrayFilters` for matching elements.
- [ ] Add authorization, node, depth, and execution budgets for `arrayFilters`.
- [ ] Cover nested arrays, object arrays, empty arrays, missing arrays, and
  concurrent updates.
- [ ] Do not expose the remote protocol before semantics and tests are complete.

## Expression boundary

### `$nor`

- [ ] Evaluate as syntax sugar for `$not + $or`.
- [ ] Define empty-array and Explain presentation.

### `$expr`

- [ ] Do not copy MongoDB `$expr` directly.
- [ ] If a real need emerges, design a bounded, typed, data-only expression AST.
- [ ] Limit the initial form to field-to-constant comparison; do not expose
  arbitrary functions.
- [ ] Require authorization and resource budgets for every expression.

### `$jsonSchema`

- [x] Do not implement MongoDB `$jsonSchema` directly.
- [ ] Decide whether Meldbase schema and validation belong outside query.
- [x] Keep query support focused on `$type` and field existence.

## Explicitly unsupported today

- [x] `$where`: arbitrary scripts violate the data-only and audit boundary.
- [x] `$text`: provide through an independent full-text-search capability.
- [x] `$near`, `$geoWithin`, and `$geoIntersects`: require dedicated geographic
  types and spatial indexes.
- [x] Bitwise queries unless a concrete permission bitmap or state-mask use case
  appears.
- [x] A complete aggregation pipeline; treat it as a separate analytical-query
  project.

## Protocol, SDK, and security

- [x] Delivered operators update the Go AST, TypeScript types, wire codecs, and
  canonical marshaling together.
- [x] `$size` and `$type` have shared Go/TypeScript conformance fixtures.
- [x] Local and remote modes share strict query wire encoding, match semantics,
  ordering, and error classes.
- [x] The server authorizes operator capabilities, not only fields.
- [x] Filter, sort, projection, and aggregate paths remain independently
  authorized.
- [ ] Add low-cardinality operator-category metrics to Explain, diagnostics,
  Prometheus, and the dashboard.
- [x] Update and Delete reuse query planning, complete predicate rechecks, and
  query budgets.
- [ ] Cover new predicates in realtime deltas and replay/resume.

## Acceptance evidence

- [x] `$size` and `$type` cover matching, non-matching, missing, `null`, and
  type errors.
- [x] `$size` and `$type` cover `$and`, `$or`, and `$not`.
- [ ] Cover empty and nested arrays for array operators.
- [x] Current indexable operators have collection-scan comparison tests.
- [x] `$size` and `$type` have equivalent memory/durable results and Explain
  structures.
- [ ] Cover sorting, pagination, realtime subscriptions, Update, Delete, and
  authorization rejection.
- [ ] Cover budget, cancellation, closed-database, and corrupted-data errors.
- [x] Observer produces stable JSON baselines without timing or temporary paths.
- [x] Pass `go test ./...`, race, `go vet ./...`, SDK check/test/build, and
  `git diff --check`.

## Sequencing

- [x] Deliver constant-work residual predicates `$size` and `$type` first.
- [x] Add predicate-work budgets and Explain/metrics accounting.
- [ ] Deliver `$elemMatch` and `$all` on the new budget.
- [ ] Use real workloads to decide between `$regex` and a dedicated prefix
  query.
- [ ] `$addToSet`, `$min`, `$max`, `$mul`, and `$rename`.
- [ ] `$currentDate`, array modifiers, and positional updates.
- [ ] Start `$expr`, full-text search, geospatial support, and aggregation only
  when each has an independent real workload.
- [ ] Collect Explain and observer evidence before broadening execution
  optimizations at each stage.

## References

- [MongoDB Query Predicates](https://www.mongodb.com/docs/manual/reference/mql/query-predicates/)
- [MongoDB Array Query Predicates](https://www.mongodb.com/docs/manual/reference/mql/query-predicates/arrays/)
- [MongoDB Logical Query Predicates](https://www.mongodb.com/docs/manual/reference/mql/query-predicates/logical/)
- [MongoDB Update Operators](https://www.mongodb.com/docs/manual/reference/mql/update/)
