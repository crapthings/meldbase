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

An execution overrun returns `ErrQueryBudget` (which also satisfies normal
query-error handling through `errors.Is`). Each rejected request increments the
database resource-limit rejection metric. When `limit` is set, scan plans keep
only the best `skip + limit` candidates; they still evaluate every required
predicate unless the selected index has a proven insertion-order early-stop.

## Authorization and indexes

Authorization evaluates filter operations, sort paths, aggregate paths, and
result projection paths independently. For example, a policy can allow
`rank == value` while denying range scans on `rank`, or allow sorting by a
field without returning unrelated result fields.

An index is an optimization only. The planner can use primary-key lookups,
equality / `$in` key unions, eligible `$or` key unions whose every branch is
indexed on the same field, and scalar-range or compound-prefix candidates. It
always rechecks the full predicate after index admission. `$or` plans that have
an unindexed branch deliberately fall back to a collection scan.

`ExplainQuery(ctx, compiledQuery)` accepts the same compiled options used by
`FindQuery`; `ExplainWithOptions` is available when the caller still has a
filter and `QueryOptions`. Explain reports selected logical bounds, whether a
residual predicate is rechecked, sort requirements and compound sort-prefix
compatibility, retained sort work, conservative estimates, and actual document
and key scans. Sort-compatible compound indexes win ties in filter
selectivity; stable insertion-position ties are still enforced by the query
executor.
