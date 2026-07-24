# Five-minute embedded Go tour

This tour creates one durable local file, performs CRUD, adds an index, and
receives a live query snapshot. It is the fastest way to evaluate Meldbase's
embedded core; it does not start an HTTP server or weaken authentication.

```sh
mkdir meldbase-tour && cd meldbase-tour
go mod init example.com/meldbase-tour
go get github.com/crapthings/meldbase@latest
```

Create `main.go`:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/crapthings/meldbase"
)

func main() {
    ctx := context.Background()
    db, err := meldbase.Open("todos.meld") // durable file; use meldbase.New() for memory only
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    todos := db.Collection("todos")

    // Create and index data. Values deliberately use the typed Go constructors.
    id, err := todos.InsertOne(ctx, meldbase.Document{
        "title": meldbase.String("Ship the tour"),
        "done":  meldbase.Bool(false),
    })
    if err != nil {
        log.Fatal(err)
    }
    if err := todos.CreateIndex(ctx, "by_done", []meldbase.IndexField{{Field: "done", Order: 1}}, meldbase.IndexOptions{}); err != nil {
        log.Fatal(err)
    }

    // Read and update by a selection condition. The same filter grammar is used
    // for Find, Update, Delete, indexes, subscriptions, and the remote SDK.
    cursor, err := todos.Find(ctx, meldbase.Filter{"done": false})
    if err != nil {
        log.Fatal(err)
    }
    open, err := cursor.All(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("open todos:", len(open))
    if _, err := todos.UpdateOne(ctx, meldbase.Filter{"_id": id}, meldbase.Update{
        "$set": map[string]any{"done": true},
    }); err != nil {
        log.Fatal(err)
    }

    // A live query first delivers a complete snapshot, then another snapshot
    // whenever this query's result changes. Always close subscriptions.
    query, err := meldbase.CompileQuery(meldbase.Filter{"done": true}, meldbase.QueryOptions{})
    if err != nil {
        log.Fatal(err)
    }
    subscription, err := todos.SubscribeQuery(ctx, query, 4)
    if err != nil {
        log.Fatal(err)
    }
    defer subscription.Close()
    snapshot := <-subscription.Snapshots
    fmt.Println("completed todos:", len(snapshot.Documents))

    if _, err := todos.DeleteOne(ctx, meldbase.Filter{"_id": id}); err != nil {
        log.Fatal(err)
    }
}
```

Run it twice:

```sh
go run .
go run .
```

`todos.meld` persists between runs. Only one writable process may open that
path. Use `meldbase.New()` when data may disappear with the process; use
`meldbase.Open(path)` for the single-file durable mode. For an executable tour
of the same lifecycle, including reopen and verification, run
`go run ./cmd/meld demo` in the Meldbase repository.

## Query, sorting, and pagination

Use `Find` for ordinary reads and pass a `QueryOptions` value when a result
needs sort, limit, skip, or seek pagination. A deterministic sort is required
for durable pagination; inspect the complete filter and budget behavior in
[query semantics](/query-semantics). Explain before adding an index, because
every index also adds write and storage cost.

```go
limit := 20
cursor, err := todos.Find(ctx, meldbase.Filter{"done": false}, meldbase.QueryOptions{
    Sort:  []meldbase.SortField{{Path: "title", Direction: 1}},
    Limit: &limit,
})
```

For a changing remote UI, use seek pagination (`first`/`after`) in the
[TypeScript SDK guide](/sdk); it adds a stable `_id` tie-breaker and validates
the cursor against the requested sort.

## Moving beyond the embedded process

The Go database is the source of truth. The optional server lets a browser or
separate process access that database through JWT-derived workspace policy,
HTTP, and ticket-authenticated realtime WebSockets. Do not treat an in-memory
`LocalCollection` as a durable replica or automatic offline cache: it is an
explicit application-owned store. The supported choices and their boundaries
are summarized in [product scope](/product-scope), while remote TypeScript,
React, and worker usage is covered in the [SDK guide](/sdk).
