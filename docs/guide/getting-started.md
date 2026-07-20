# Getting started

Meldbase has two useful starting points. Embed it directly when Go owns the
process; run the server when a browser or another service needs a secured
boundary.

## 1. Open a durable local database

```sh
go get github.com/crapthings/meldbase/core@latest
```

```go
package main

import (
    "context"
    "log"

    meldbase "github.com/crapthings/meldbase/core"
)

func main() {
    db, err := meldbase.Open("app.meld")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    _, err = db.Collection("notes").InsertOne(context.Background(), meldbase.Document{
        "title": meldbase.String("First note"),
        "done":  meldbase.Bool(false),
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

The [query contract](../query) covers filters, updates, sorting, and indexes.

## 2. Explore the engine

From a checkout, run the self-contained demo:

```sh
go run ./cmd/meld demo
```

It performs durable writes, creates an index, observes a reactive query, closes
the database, reopens it, and verifies the result.

## 3. Run a secured local node

Build the CLI once, then create a new local bundle:

```sh
go build -o ./meld ./cmd/meld
./meld init --dir ./meldbase-local
MELDBASE_BIN="$(pwd)/meld" ./meldbase-local/start.sh
```

The generated launcher binds the application API to `127.0.0.1:8080` and the
embedded operator dashboard to `127.0.0.1:9091`. It expects an identity service
to issue HS256 JWTs with `sub`, `exp`, `iss`, `aud`, and `workspace_id` claims.
It does not create users or enable unauthenticated access.

Read [identity, users, and workspaces](./identity-and-workspaces) before
connecting an application identity service. Continue with
[single-node deployment and recovery](../single-node-deployment) for the
generated token material, dashboard access, reverse proxy, backups, and restore
rehearsal.

## Next steps

- [Build realtime UI with TypeScript and React](../reactive)
- [Run the live todo example](./realtime-todos)
- [Configure HTTP and WebSocket access](../client-protocol)
- [Add indexes and inspect query plans](../query)
- [Review the current capability boundary](../mvp-audit)
