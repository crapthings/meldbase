# Meldbase

> Documents that stay live. Local by design.

Meldbase is an experimental, embedded reactive document database written in Go.
It combines a typed document model, local durable storage, query planning, and
live query subscriptions behind one coherent API. It is a new database—not a
MongoDB protocol or behavior clone.

The current implementation contains a typed Go engine, crash-recoverable
copy-on-write checkpoints with a redo WAL, scalar B+Tree indexes, a shared
Go/TypeScript query contract, and a secured HTTP/WebSocket reactive server.

It is still early-stage: normal operation uses a `.wal` sidecar, realtime V1
recomputes full snapshots instead of sending incremental diffs, B+Tree deletion
still rebuilds instead of locally rebalancing, and the property/crash/benchmark
matrix is incomplete. “One file” remains a product target, not a claim about
this build.

## Installation

The Go module is published from:

```sh
go get github.com/crapthings/meldbase@latest
```

The TypeScript packages currently live in this pnpm workspace and are not yet
published to npm. Use the repository workspace for the developer preview; do not
assume `@meldbase/client` or `@meldbase/react` is available from the public npm
registry until an npm release is announced.

## Go quick start

```go
db, err := meldbase.Open("app.meld")
if err != nil { log.Fatal(err) }
defer db.Close()

users := db.Collection("users")
id, err := users.InsertOne(ctx, meldbase.Document{
  "name": meldbase.String("Ada"),
  "age":  meldbase.Int(30),
})

err = users.CreateIndex(ctx, "users_age", []meldbase.IndexField{
  {Field: "age", Order: 1},
}, meldbase.IndexOptions{})
```

## TypeScript SDK

The same compiled, data-only query is used by the local cache and sent to the
server. Native JS applications can use a local reactive collection directly:

```ts
import { LocalCollection } from "@meldbase/client"

const todos = new LocalCollection([
  { _id: "one", title: "Learn Meldbase", done: false }
])

const open = todos.find({ done: false }, {
  sort: [{ path: "title", direction: 1 }]
})

const stop = open.subscribe(snapshot => render(snapshot))
```

Remote queries use HTTP for fetch and a ticket-authenticated WebSocket for their
ongoing state:

```ts
import { MeldbaseClient } from "@meldbase/client"

const db = new MeldbaseClient({
  baseUrl: "https://data.example.com",
  accessToken: () => auth.currentAccessToken()
})

const query = db.collection("todos").find({ done: false })
const stop = query.subscribe(render, {
  onStatus: status => showSyncState(status.state)
})

const created = await db.collection("todos").insertOne({
  title: "Build something live",
  done: false
})

await db.collection("todos").updateOne(
  { _id: created._id },
  { $set: { done: true } }
)
```

React uses a thin `useSyncExternalStore` adapter over that same query object; it
does not introduce a second query language:

```tsx
import { useMemo } from "react"
import { useLiveQuery } from "@meldbase/react"

function OpenTodos({ db }: { db: MeldbaseClient }) {
  const query = useMemo(
    () => db.collection("todos").find({ done: false }),
    [db]
  )
  const { documents, status, error } = useLiveQuery(query)
  // Keep the query object stable; updates arrive over its WebSocket subscription.
  return <TodoList todos={documents} syncState={status} error={error} />
}
```

See
[`docs/client-protocol.md`](docs/client-protocol.md) for the realtime and security
model.

A complete browser example lives in
[`examples/realtime-todos`](examples/realtime-todos). Run the development server
above, then:

```sh
pnpm --filter @meldbase/example-realtime-todos dev
```

The example performs real HTTP mutations and WebSocket snapshots through the
React adapter. Open it twice to observe the same query update in both views.

## Run the end-to-end demo

The demo performs durable insert/update, creates and uses an index, observes a
reactive query, closes the database, and proves the data after reopen:

```sh
go run ./cmd/meld demo
```

Run the HTTP/WebSocket server locally only with the explicit development-auth
switch:

```sh
go run ./cmd/meld serve \
  --db ./app.meld \
  --addr :8080 \
  --dev-no-auth
```

`--dev-no-auth` grants every request full access and is intentionally required;
it is not a production authentication mode. A production embedding supplies the
server `Authenticator` and `Authorizer` implementations itself.

The implemented transport endpoints are:

```text
GET  /health
POST /v1/collections/{collection}/query
POST /v1/collections/{collection}/documents
POST /v1/collections/{collection}/mutations
POST /v1/realtime/tickets
GET  /v1/realtime
```

HTTP queries carry the same versioned, data-only AST used by the SDK. With the
development server above:

```sh
curl -X POST http://localhost:8080/v1/collections/todos/query \
  -H 'Content-Type: application/json' \
  --data '{
    "version": 1,
    "query": {
      "version": 1,
      "where": {"op":"compare","cmp":"eq","path":"done","value":{"t":"bool","v":false}},
      "sort": [{"path":"title","direction":1}]
    }
  }'
```

Browser realtime authentication is two-step: obtain a short-lived, single-use
ticket over authenticated HTTP, then send it in the first WebSocket message.
Credentials never appear in the WebSocket URL. The core V1 exchange is:

```json
{"v":1,"type":"authenticate","ticket":"<single-use-ticket>"}
{"v":1,"type":"subscribe","requestId":"open-todos","collection":"todos","query":{"version":1,"where":{"op":"compare","cmp":"eq","path":"done","value":{"t":"bool","v":false}}}}
{"v":1,"type":"snapshot","requestId":"open-todos","subscriptionId":"<id>","token":"<signed-resume-token>","documents":[]}
{"v":1,"type":"unsubscribe","subscriptionId":"<id>"}
```

See [`docs/client-protocol.md`](docs/client-protocol.md) for reconnect,
`resync_required`, limits, origin checks, and row/field authorization.

## Status

Early-stage and not suitable for production data. See
[`docs/architecture.md`](docs/architecture.md) and
[`docs/roadmap.md`](docs/roadmap.md). The first-stage requirement-to-evidence map
is in [`docs/mvp-audit.md`](docs/mvp-audit.md).

Supported query operators are `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`,
`$in`, `$nin`, `$exists`, `$and`, `$or`, and `$not`. Meldbase defines these
semantics itself and does not promise MongoDB compatibility.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
go run ./cmd/meld demo
pnpm check
pnpm test
pnpm build:example
```

Contributions should follow [`CONTRIBUTING.md`](CONTRIBUTING.md). Security
reports must use the private process described in [`SECURITY.md`](SECURITY.md),
not a public issue. Maintainer release gates are documented in
[`docs/releasing.md`](docs/releasing.md).

## License

Licensed under the [Apache License 2.0](LICENSE).
