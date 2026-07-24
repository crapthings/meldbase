# Query semantics

This page defines the executable query contract shared by the Go database,
the HTTP server, realtime views, and the TypeScript SDK. It is intentionally
more specific than an API overview: indexes and optimizations must preserve
these results exactly.

## Values, missing fields, and equality

A missing path is distinct from a stored `null`.

- `{field: {$exists: false}}` matches only a missing path.
- Equality with a scalar against an array field means direct element
  membership. Equality between two arrays or two objects is structural.
- `$ne` matches a missing path; other comparisons and membership checks do
  not match a missing path unless `$nin` is used.
- `_id` predicates accept only a non-zero, lower-case, 32-character hexadecimal
  document ID. The wire form uses the separate `id` value tag.

## Array length and exact value types

`$size` matches only an array whose direct element count equals the requested
size. Its operand must be an integer from zero through JavaScript's maximum
safe integer. Missing paths, `null`, objects, strings, and other non-array
values do not match; an empty array matches size zero.

`$type` matches a present value's exact Meldbase type. The supported names are
`null`, `boolean`, `int64`, `float64`, `string`, `date`, `id`, `binary`,
`array`, and `object`. A non-empty array of names is an OR and is normalized to
canonical order with duplicates removed. Meldbase deliberately has no
ambiguous `number` alias; use `["int64", "float64"]` when both numeric kinds
are intended. A missing path has no type, so it does not match `$type` (and,
consequently, does match a direct `$not` of that predicate).

Both operators are constant-work residual predicates for each decoded
document. An indexed sibling in the same `$and` can still provide the candidate
source, but `$size` and `$type` do not claim an ordinary B-tree access path or
produce B-tree index advice themselves. Document admission and rechecking
remain charged to the normal query budgets.

## Array containment

`$all` matches only a present array containing every requested value. Query
values are structurally compared to individual array elements: `null`, arrays,
and objects are valid values, while a missing path or non-array never matches.
The operand must be a non-empty bounded array; duplicate query values are
removed in first-occurrence order, so they never require repeated stored
elements. `$all` is a residual predicate and charges a predicate step for each
required value and examined array element. An indexed sibling in the same
`$and` may provide candidates, but `$all` itself has no B-tree access path or
multi-index intersection behavior.

`$elemMatch` matches only a present array with at least one qualifying
element. An operand whose keys are field names is object mode: its nested
filter is evaluated against each object element independently. An operand
whose keys are operators is scalar mode, supporting comparisons, `$in`,
`$nin`, `$and`, `$or`, and `$not` against each element. The two styles cannot
be mixed, and an empty operand is rejected. This prevents conditions from
being satisfied by different elements of one array. Missing paths, non-arrays,
and incompatible element kinds do not match. Like `$all`, `$elemMatch` is a
predicate-budgeted residual predicate with no standalone B-tree access path.

## Comparisons and sorting

Range predicates (`$gt`, `$gte`, `$lt`, `$lte`) apply to scalar values only.
Numeric Int64 and Float64 values are compared exactly without first converting
Int64 to floating point. Other scalar kinds have the following ascending order:

```text
null < false < true < number < string < date < document ID < binary
```

Strings use lexicographic UTF-8 byte order. Binary values and document IDs use
lexicographic byte order. Arrays and objects are never range-comparable.

Sorting uses the same scalar order. Missing values sort before present values in
ascending order and after present values in descending order. Arrays and
objects have stable type positions after scalars; two values of the same complex
type retain collection insertion order unless a later sort field differs.
Collection insertion order is the final tie-breaker for ordinary queries.

## Seek pagination

SDK seek pagination requires `first` and at least one sort field. It appends
`_id` ascending when the caller did not include it, producing a canonical,
complete ordering. Cursors explicitly encode whether each sort path was
missing, so pages do not skip rows around absent fields. Cursor values must be
scalars; arrays and objects are rejected rather than producing an unsafe
continuation filter.

A server rejects an SDK seek request when a restricted result projection would
hide a requested sort field. This avoids returning a page whose cursor cannot
be safely constructed without exposing hidden data.

Pagination is a continuation over a changing collection, not a snapshot lease:
writes between pages can still insert, remove, or move later results. Use a
transaction or a retained/replay token when a fixed historical view is needed.

## Validation limits

The query compiler and wire decoder enforce the same defaults:

| Limit | Default |
| --- | ---: |
| Wire query bytes | 1 MiB |
| Expression/value depth | 16 |
| Expression nodes | 128 |
| Items or object fields per query value | 256 |
| Bytes per query value | 16 KiB |
| Sort fields | 4 |
| Limit | 10,000 |

Duplicate sort paths, malformed compiled query objects, unsafe paths,
non-canonical IDs, unknown wire fields, and non-finite values are rejected.
Public Go execution APIs return `ErrInvalidFilter` for an invalid `QuerySpec`;
the zero value never matches documents.

## Execution budgets

Every database query, including lazy reads, snapshots, server counts and group
counts, and the selection phase of multi-document updates/deletes, has the
following normalized `ResourceLimits`. The same limits apply whether the
planner chooses a collection scan, a primary-key lookup, or a secondary index.

| Limit | Default | What it bounds |
| --- | ---: | --- |
| `MaxQueryDocumentsExamined` | 100,000 | Documents decoded and predicate-checked |
| `MaxQueryKeysExamined` | 100,000 | Secondary-index or primary-key entries inspected |
| `MaxQueryCandidates` | 100,000 | Retained candidates needed to produce the requested window |
| `MaxQuerySortBytes` | 64 MiB | Canonical bytes of retained sorted candidates |
| `MaxQuerySkip` | 100,000 | Requested offset before execution begins |
| `MaxQueryPredicateSteps` | 10,000,000 | Residual expression visits and data-dependent array comparisons |

An execution overrun returns `ErrQueryBudget` (which also satisfies normal
query-error handling through `errors.Is`). Each rejected request increments the
database resource-limit rejection metric. When `limit` is set, scan plans keep
only the best `skip + limit` candidates; they still evaluate every required
predicate unless the selected index has a proven insertion-order early-stop.
`Explain.Budget` reports predicate-step use, pressure, and any
`predicate_steps` rejection; the fixed-cardinality query metrics and optional
diagnostics expose the same completed work without recording query values.

## Authorization and indexes

Authorization evaluates filter operations, sort paths, aggregate paths, and
result projection paths independently. For example, a policy can allow
`rank == value` while denying range scans on `rank`, or allow sorting by a
field without returning unrelated result fields.

An index is an optimization only. The planner can use primary-key equality,
`$in`, and `$or` unions; secondary equality / membership unions; scalar ranges;
and compound-prefix candidates. An `$or` expression may union different primary
or secondary access paths, including branch-specific compound bounds and
same-field range spans, only when every non-empty branch has an indexed
candidate source. A pure `$or` with an unindexed branch therefore falls back to
a collection scan. When that `$or` is nested inside an `$and`, another indexed
conjunct may still supply one safe candidate source; the complete predicate,
including the `$or`, is then rechecked as a residual filter. The complete query
falls back only when no such source is usable.

An `$and` does not currently intersect independent indexes. It selects one
access path and rechecks the other predicates, while a compound index can
constrain a complete left prefix of multiple `$and` fields. For example, two
separate indexes on `workspaceId` and `status` still produce one scan plus a
residual filter, whereas `(workspaceId, status)` can constrain both equalities.
`workspaceId` is the conventional Server authorization field, not a
schema-level database keyword; applications using another configured workspace
field retain the same planning behavior. Compound indexes should be justified
by observed candidate amplification and write cost rather than created for
every field combination.

Every union deduplicates document IDs and rechecks the complete predicate after
index admission. Exact-key spans are merged by collection insertion position
when an unsorted `skip` / `limit` window permits early completion; each physical
key inspected to establish that order still counts against
`MaxQueryKeysExamined`.

`ExplainQuery(ctx, compiledQuery)` accepts the same compiled options used by
`FindQuery`; `ExplainWithOptions` is available when the caller still has a
filter and `QueryOptions`. Explain reports selected logical bounds, whether a
residual predicate is rechecked, sort requirements and compound sort-prefix
compatibility, every primary/secondary name in an index union (`IndexNames`),
retained sort work, conservative estimates, and actual document and key scans.
`IndexName` remains populated for a single access path. Sort-compatible
compound indexes win ties in filter selectivity; stable insertion-position
ties are still enforced by the query executor.

Explain also exposes the execution facts needed to decide whether another
optimization is justified:

- `PlanReason` distinguishes a point lookup, one secondary scan, a same-index
  union, a multi-index union, and a collection scan. `FallbackReason` uses
  fixed codes such as `unindexed_or_branch`, `no_secondary_indexes`, and
  `no_usable_index`; `UnindexedPaths` contains schema paths but never query
  values.
- `IndexableConjunctPaths` identifies distinct `$and` predicate paths that each
  have an independently usable index candidate. It is collected only for an
  explicit Explain and does not claim that an index intersection was selected.
  `CompoundIndexOpportunity` is true when the selected non-unique source
  constrains only a subset of those paths; it remains a structural signal even
  when the conservative advice threshold is not met.
- `Sources` attributes spans, physical keys, candidate IDs, deduplicated IDs,
  and decoded documents to each selected primary or secondary access path.
  Ordered union execution may read one key ahead from each source, so
  `KeysExamined` can exceed `CandidateIDs` when a limit stops the merge.
- `CandidateIDs`, `UniqueCandidateIDs`, and `DuplicateCandidateIDs` make index
  overlap visible without weakening the physical-key budget.
- `EarlyStopEligible`, `EarlyStopped`, `EarlyStopScope`, and
  `EarlyStopReason` state whether the result window can stop document work,
  key work, both, or neither. `range_scan` and `sort_required` are explicit
  reasons for completing the scan.
- `Budget` reports used and configured document, key, candidate, sort-byte,
  and skip limits. `Pressure` names the most-used dimension once it reaches 80%;
  `Exceeded` identifies the limit that returned `ErrQueryBudget`. Partial
  Explain results retain completed work when execution ends at a budget.

`Advice` contains conservative fixed-code observations. The current codes are
`consider_filter_index`, `consider_sort_index`, `consider_compound_index`,
`limit_requires_full_scan`, `high_union_overlap`, and `budget_pressure`. They
are inputs to workload analysis, not instructions to create an index:
cardinality and write cost must still be measured. Sort advice preserves the
requested path directions in its `Sort` field. Compound-index advice is emitted
only for an unmodified query with multiple independently indexed conjunct paths
when a non-unique single source examines at least 32 documents and the observed
document-to-retained-candidate ratio is at least four. Its `Paths` are a stable
set of candidate fields, not a prescribed index order.

Process-level `QueryStats`, bounded diagnostic events, the admin dashboard, and
Prometheus export the aggregate physical-key, candidate-ID, duplicate-ID,
retained-sort, early-stop, and query-budget counters. Diagnostic events retain
the fixed plan, fallback, early-stop, pressure, and exceeded reason codes, but
exclude filters, query values, collection names, index names, and document IDs.

## Synthetic workload observer

`cmd/meld-query-observer` is a repeatable local workload for checking these
signals before changing the planner. It creates an isolated deterministic
collection, builds the indexes required by the selected profile, runs one
detailed Explain per scenario, then measures the `QueryStats` delta from
repeated `FindQuery` calls. Explain and warm-up executions are deliberately
excluded from the measured counters.

```sh
# Fast in-memory baseline.
go run ./cmd/meld-query-observer

# Exercise the durable executor with a temporary database.
go run ./cmd/meld-query-observer -backend durable

# Save the complete report, including timings.
go run ./cmd/meld-query-observer -format json > query-observer.json

# Compare controlled overlap and limit curves across both executors.
go run ./cmd/meld-query-observer \
  -profile matrix \
  -backend both

# Produce byte-comparable structural CI evidence without timing noise.
go run ./cmd/meld-query-observer \
  -profile matrix \
  -backend both \
  -format json-stable > query-observer-stable.json

# Deliberately expose physical-key budget pressure.
go run ./cmd/meld-query-observer \
  -scenarios overlapping-or \
  -max-query-keys 1000

# Measure array-predicate work with longer, more repetitive arrays.
go run ./cmd/meld-query-observer \
  -backend both \
  -scenarios array-all-miss,array-elem-scalar,array-elem-object \
  -array-items 128 \
  -array-duplicate-percent 75
```

The fixed scenarios isolate different execution properties:

| Scenario | Intended signal |
| --- | --- |
| `indexed-limit` | Exact secondary lookup and a proven unsorted limit stop |
| `overlapping-or` | Cross-index union, physical keys, and duplicate IDs |
| `range-limit` | Range work that a result limit cannot stop safely |
| `sort-pressure` | Retained candidates and bytes for an unindexed sort |
| `collection-scan` | Full document scan and conservative filter-index advice |
| `and-separate-indexes` | One selected index, residual amplification, and conservative compound-index advice |
| `and-compound-index` | A compound-index control with the same workspace/status selectivity shape |
| `array-all-miss` | `$all` residual work when a required value is absent after indexed admission |
| `array-elem-scalar` | Scalar `$elemMatch` work with the qualifying element at the array tail |
| `array-elem-object` | Object `$elemMatch` work that preserves the same-element constraint |

The optional `matrix` profile creates two equally sized indexed branches and
controls their intersection. Its default `-overlaps 0,10,25,50` values target
those duplicate-ID percentages out of all physical candidate IDs; rounding is
only to whole documents. The default `-limits 1,10,100,none` scenarios query
the same exact secondary key. This keeps selectivity constant while revealing
how memory execution can stop document decoding and durable execution can stop
both key and document work. `-backend both` prints a structural comparison
table after the two complete reports. Either list can be replaced with a
comma-separated subset.

The table output includes per-source Explain work and per-run observed
counters. `json` includes the complete Explain object, normalized resource
limits, timings, ratios, and measured counter deltas. Its timings are trend
data and require environment-specific tolerances; the complete JSON file is not
a strict golden result. `json-stable` excludes setup/query timings, temporary
paths, and free-form error text while retaining structured plans, reason codes,
parameters, counters, ratios, budget state, and error classes. It is intended
for exact CI comparison for the same observer version, backend, dataset, and
limits. Baseline updates should remain explicit review decisions rather than
being regenerated automatically.

A key/unique-candidate ratio above one shows read amplification; the
document/retained-candidate ratio exposes residual-filter amplification;
duplicate percentage isolates union overlap; the peak budget percentage shows
the dimension nearest rejection. The array scenarios additionally report
predicate steps, predicate-steps/document, and predicate-steps/retained
candidate. `-array-items` (1–4096) and
`-array-duplicate-percent` (0–90) control their deterministic data shape.
Pressure events begin at 80% of a configured query budget.

The durable mode uses a temporary database and removes it after the run. Passing
`-database PATH` retains a newly generated database for inspection, but the
observer refuses to open or overwrite an existing path. This command is a
synthetic reproducer, not a production query tracer: use Prometheus, the admin
dashboard, or bounded diagnostics to identify a real workload shape, then
select or adapt the matching scenario. Run with `-help` for dataset, iteration,
payload, profile, matrix, scenario, timeout, and budget controls.
