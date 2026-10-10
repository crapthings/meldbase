# Queries, sorting, and pagination

These examples use the `todos` collection from a quickstart. Remote SDK reads
are asynchronous; the in-memory local SDK returns results synchronously.

## Filter documents

```js
const open = await todos.find({ done: false }).fetch();
const important = await todos.find({ priority: { $gte: 2 } }).fetch();
const selected = await todos
  .find({
    $and: [{ done: false }, { priority: { $in: [2, 3] } }],
  })
  .fetch();
```

Supported predicates include equality, comparisons (`$gt`, `$gte`, `$lt`,
`$lte`), `$in`, `$nin`, `$exists`, `$type`, and array predicates such as
`$size`, `$all`, and `$elemMatch`. Logical composition uses `$and` and `$or`. Filters are validated data; functions are not query predicates.

## Missing values and arrays

Missing fields differ from explicit nulls. `$ne` and `$nin` also match missing
fields. Scalar equality against an array can match a direct array element;
array and object equality use structural equality. Nested paths use dot notation.

Numeric comparisons preserve int64/float64 semantics. Do not assume every
predicate can use an index: array and type predicates can require residual
filtering. See [indexes](/indexes).

## Sort and offset pagination

```js
const page = await todos
  .find({ done: false }, { sort: [{ path: "createdAt", direction: -1 }], skip: 0, limit: 20 })
  .fetch();
```

A direction of `1` sorts ascending; `-1` sorts descending. Missing values sort
before present values in ascending order. Supply an explicit sort when result
order matters.

## Seek pagination

```js
const options = { sort: [{ path: "createdAt", direction: 1 }], first: 20 };
const page = await todos.find({ done: false }, options).fetchPage();
if (page.nextCursor) {
  const next = await todos.find({ done: false }, { ...options, after: page.nextCursor }).fetchPage();
  console.log(next.documents);
}
```

Use the same filter and sort on subsequent requests. Seek pagination adds an
ascending `_id` tie-breaker. `after` requires `first`; do not combine `first`
with `limit`, or `after` with `skip`. A cursor is not a frozen snapshot:
concurrent writes can change which documents a later page sees.

## Next step

[Design an index](/indexes) or [keep a query live](/realtime).
