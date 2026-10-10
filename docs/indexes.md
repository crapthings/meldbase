# Indexes

Indexes support access patterns in a collection. Create them deliberately for
the fields you filter or sort on, then measure your actual workload.

## Create an index from Go

Inside the Go application from the [Go tour](/getting-started), after obtaining
`todos`, add:

```go
err = todos.CreateIndex(ctx, "by_done", []meldbase.IndexField{
	{Field: "done", Order: 1},
}, meldbase.IndexOptions{})
if err != nil {
	log.Fatal(err)
}
```

`Order` is `1` for ascending and `-1` for descending. Compound definitions
list fields in order:

```go
err = todos.CreateIndex(ctx, "by_done_created", []meldbase.IndexField{
	{Field: "done", Order: 1},
	{Field: "createdAt", Order: -1},
}, meldbase.IndexOptions{})
if err != nil {
	log.Fatal(err)
}
```

Set `Unique: true` in `IndexOptions` to enforce complete-tuple uniqueness.
Index creation returns errors for invalid definitions and conflicting data;
handle those errors before accepting the index as available.

## Know what an index does

An index is not a promise that every query avoids a scan. Filters such as
`$size`, `$type`, `$all`, and `$elemMatch` can need residual evaluation.
Compound field order and query sort order matter. Each additional index also
adds work to writes and occupies storage.

Start with a query that needs help, add the corresponding index, and verify
its behavior with representative data. Keep correctness independent of which
execution path the planner chooses.

## Next step

Review [query semantics and pagination](/queries), then add
[live queries](/realtime) if your application needs updates.
