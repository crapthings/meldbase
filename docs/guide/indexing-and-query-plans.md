# Indexing and query plans

Indexes are a deployment concern in Meldbase. Browser and mobile SDKs can use
the data API, but cannot create indexes or inspect application query payloads.
An operator creates access paths from known application queries, then verifies
the planner chooses them before relying on a performance claim.

## Start from a real access pattern

For a workspace-scoped task board, a common access pattern is:

```text
workspaceId = active workspace
AND status = "open"
ORDER BY updatedAt DESC
```

The corresponding compound index starts with equality fields and then the
ordered range or sort field:

```go
err := db.Collection("tasks").CreateIndex(ctx, "by_workspace_status_updated", []meldbase.IndexField{
  {Field: "workspaceId", Order: 1},
  {Field: "status", Order: 1},
  {Field: "updatedAt", Order: -1},
}, meldbase.IndexOptions{})
```

Do not add an index for every field. Each index adds write work, storage, and
build time. Prefer the smallest index that serves a repeated, user-visible
filter. A unique index constrains the complete tuple, so a workspace-local
unique slug is typically `{ workspaceId, slug }`, not `slug` alone.

## Verify with Explain

Use the same filter your application sends. `Explain` returns only plan
metadata—never documents or query values:

```go
plan, err := db.Collection("tasks").Explain(ctx, meldbase.Filter{
  "workspaceId": "team-a",
  "status": "open",
})
if err != nil { log.Fatal(err) }
fmt.Printf("%s via %s: %d keys, %d documents\n",
  plan.Stage, plan.IndexName, plan.KeysExamined, plan.DocumentsExamined)
// IXSCAN via by_workspace_status_updated: ...
```

`IXSCAN` means a published secondary index was selected. `ID_LOOKUP` is the
safe direct `_id` path. `COLLSCAN` means no suitable index was selected. A
collection scan is not automatically wrong for a small collection or a rare
administrative task; it is a signal to compare measured work before adding a
new write-time cost.

The planner uses a compound index for a contiguous left prefix: equality
components first, optionally followed by one range component. It rechecks the
complete predicate against resolved documents, so an index can improve work but
cannot change query membership. See the [compound index contract](../compound-indexes)
for the precise matching rules.

## Build safely on a single node

For an existing durable file, stop the writer before running an offline index
operation. The CLI creates a resumable build record; it does not overwrite the
database.

```sh
# Stop meld serve first.
meld index-build start \
  --db /srv/meldbase/data/app.meld \
  --collection tasks \
  --name by_workspace_status_updated \
  --field workspaceId \
  --field status \
  --field updatedAt:-1

meld index-build list --db /srv/meldbase/data/app.meld
meld index-build resume --db /srv/meldbase/data/app.meld --id <build-id>
```

If a build cannot complete, inspect its terminal state, correct the cause, then
either resume or explicitly abort it. A failed build is never half-published.
The [index-build reference](../index-builds) documents lifecycle, limits, and
recovery behavior.

## Verify the operational outcome

Use this compact release checklist after adding an index:

1. Run `Explain` against the real application filter and record the selected
   stage and index name.
2. Exercise the normal query, `count`, and any permitted `groupCount` calls
   with representative data volume. Aggregates remain policy-capped, even when
   their source query can use an index.
3. Open the authenticated embedded dashboard’s **Indexes & planner** view. It
   lists published definitions and shows process-session `IXSCAN`, `COLLSCAN`,
   and `_id` lookup totals without exposing query filters, collection values,
   tenants, or document data.
4. Keep the before/after measurement with the deployment change. If a new
   index does not materially reduce examined work on a repeated path, remove
   the proposal before carrying its write cost into production.

The dashboard is read-only. It never creates or drops indexes; retain the CLI
and change review as the administrative control point.
