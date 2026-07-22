# Meldbase

> A local document database that keeps application data live.

Meldbase is an experimental, embedded reactive document database for Go and
TypeScript applications. It stores documents in one local durable file, exposes
typed queries and indexes, and can keep those queries live over HTTP and
WebSockets when an application needs a server boundary.

It is designed for product teams that want a small, application-owned data
layer—not a hosted MongoDB clone, an ORM, or a distributed database to operate.

**[Read the documentation →](https://crapthings.github.io/meldbase/)**

## Why Meldbase

- **Start local.** Open one file from Go; there is no separate database service
  required for the embedded path.
- **Keep reads live.** The same query model powers local collections, server
  fetches, and ordered realtime updates.
- **Own the boundary.** A declarative collection policy enforces JWT-derived
  workspace and owner scope; your application keeps ownership of users, roles,
  and identity.
- **Operate with evidence.** Health probes, an authenticated dashboard,
  physical backup/restore, logical export/import, offline verification, and a
  single-node runbook are part of the project—not afterthoughts.

## Start in two minutes

### Embed it in Go

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
    ctx := context.Background()
    db, err := meldbase.Open("app.meld")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    users := db.Collection("users")
    _, err = users.InsertOne(ctx, meldbase.Document{
        "name": meldbase.String("Ada"),
        "role": meldbase.String("engineer"),
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

For a runnable tour of durable writes, indexes, reactive queries, reopen and
verification:

```sh
go run ./cmd/meld demo
```

### Create a secured local node

`meld init` creates a new, non-overwriting single-node bundle with private
credentials, a data directory, backup/rehearsal directories, and a loopback
launcher. It does not create users or weaken authentication.

```sh
go build -o ./meld ./cmd/meld
./meld init --dir ./meldbase-local
MELDBASE_BIN="$(pwd)/meld" ./meldbase-local/start.sh
```

The API starts on `127.0.0.1:8080` and the embedded operator dashboard on
`127.0.0.1:9091`. Your identity service signs the JWTs; the server verifies
them and applies the configured collection-access boundary. See the
[single-node guide](docs/single-node-deployment.md) for token requirements,
dashboard access, reverse-proxy boundaries, backups, recovery drills, and
upgrades.

## What you can build today

| Need | Meldbase provides |
| --- | --- |
| Product data | Typed documents, CRUD, safe filters and updates, compound and unique indexes. |
| Live UI | Reactive queries with snapshots, ordered deltas, resume tokens, and a React adapter. |
| Application API | HTTP fetch/mutation endpoints plus ticket-authenticated WebSocket realtime. |
| Workspace boundary | JWT-derived workspace/owner scoping, optional field limits, and RPC-only collections; clients never choose a trusted workspace. |
| Business logic | Typed RPC, durable idempotency, and an optional authenticated Node.js worker boundary. |
| Operations | `/livez`, `/readyz`, authenticated metrics/dashboard, inspect, verify, backup, restore, export/import, and restore drills. |

### TypeScript and React

The TypeScript packages are currently a repository-workspace preview; they are
not yet published to npm. The client uses one data-only query contract locally
and remotely:

```ts
import { MeldbaseClient } from "@meldbase/client"

const db = new MeldbaseClient({
  baseUrl: "https://data.example.com",
  accessToken: () => auth.currentAccessToken(),
})

const query = db.collection("todos").find({ done: false })
const stop = query.subscribe(render, {
  onStatus: (status) => showSyncState(status.state),
})

const todo = await db.collection("todos").insertOne({
  title: "Build something live",
  done: false,
})
```

React is a thin adapter over the same query object:

```tsx
const query = useMemo(() => db.collection("todos").find({ done: false }), [db])
const { documents, status, error } = useLiveQuery(query)
```

See [`examples/realtime-todos`](examples/realtime-todos) for a runnable browser
example, and the [client protocol](docs/client-protocol.md) for the exact
HTTP/WebSocket contract.

## How the pieces fit

```text
your Go process                    your application boundary
┌──────────────────┐              ┌─────────────────────────┐
│ core             │              │ HTTP + WebSocket server │
│ documents/indexes│── optional ─▶│ JWT + workspace policy  │
│ durable .meld    │              │ SDKs / browser clients  │
└──────────────────┘              └─────────────────────────┘
```

The embedded core is useful on its own. Add the server only when another
process, browser, or service needs access. The server does not become your user
directory: it trusts a verified identity provider, derives an actor and active
workspace from the token, and constrains configured business collections. The
[collection access guide](docs/guide/access-policies.md) describes the small,
declarative policy surface and where application business authorization begins.

## Use it when

Meldbase is a good fit when you want application-owned documents, reactive UI
state, and a small operational footprint—for example, a local-first product
component, an internal tool, an edge-adjacent service, or a Go application that
needs durable live queries without introducing a separate database product.

It is deliberately not a fit when you need MongoDB wire compatibility,
sharding, distributed transactions, automatic HA/failover, offline conflict
resolution, complex aggregation, or built-in end-user identity. Those are not
hidden roadmap promises; see the [capability audit](docs/mvp-audit.md).

## Durable data and operations

Meldbase uses a checksummed copy-on-write format with a durable Commit Log,
bounded history, crash recovery, and full offline verification. A physical
backup retains database identity and history for recovery; `Compact` creates an
independent file with a new identity when that is what you need.

```sh
# The source must be offline: both commands take the exclusive process lock.
meld backup --db /srv/meldbase/data/app.meld \
  --out /srv/meldbase/backups/app.meld > app.receipt.json
meld restore --in /srv/meldbase/backups/app.meld \
  --receipt app.receipt.json \
  --out /srv/meldbase/rehearsals/app-restored.meld
meld verify --db /srv/meldbase/rehearsals/app-restored.meld
```

`export` and `import` provide a **logical archive** for portable data snapshots
and import rehearsals. It carries collections, typed documents, and index
definitions—not pages, database identity, commit history, or credentials. It is
not a replacement for the physical recovery backup above; both paths must be
new.

```sh
meld export --db /srv/meldbase/data/app.meld \
  --out /srv/meldbase/archives/app.jsonl
meld import --in /srv/meldbase/archives/app.jsonl \
  --out /srv/meldbase/rehearsals/app-imported.meld
meld verify --db /srv/meldbase/rehearsals/app-imported.meld
```

Read the [single-node deployment and recovery guide](docs/single-node-deployment.md)
before operating real data. The deeper storage guarantees, resource limits,
rollback-anchor model, filesystem qualification, and release evidence are
documented separately so the getting-started path stays readable:

- [Storage format and recovery](docs/storage-format.md)
- [Observability and dashboard](docs/observability.md)
- [Filesystem qualification](docs/filesystem-qualification.md)
- [Rollback protection](docs/rollback-anchor-service.md)
- [Release process](docs/releasing.md)

## Current alpha status

Meldbase is early-stage and should not yet hold production data. The current
format is revision 3 and intentionally evolves during alpha; older alpha files
are unsupported. There is no cross-version compatibility promise yet: a future
breaking change will receive release-specific guidance when it is actually
planned. The project has one current storage path—there is no legacy runtime or
fallback engine.

The core, server, SDKs, and single-node tooling are implemented and tested, but
the project does **not** claim blanket power-loss qualification for every
filesystem, production-grade automatic HA, or the deferred features listed
above. The [capability audit](docs/mvp-audit.md) and [roadmap](docs/roadmap.md)
are the authoritative boundary.

## Documentation

### Start with your goal

- **Try Meldbase.** Run the [two-minute Go example](#start-in-two-minutes),
  then use the [getting-started guide](docs/guide/getting-started.md) or the
  [live todo example](docs/guide/realtime-todos.md). Read [safe alpha
  evaluation](docs/alpha-evaluation.md) before putting important data in it.
- **Build an application.** Start with [identity and workspace
  isolation](docs/guide/identity-and-workspaces.md), [collection access
  policies](docs/guide/access-policies.md), [realtime UI](docs/guide/realtime-todos.md),
  and the [query contract](docs/query.md).
- **Run a production service.** Follow [single-node deployment and
  recovery](docs/single-node-deployment.md), then the [backup and upgrade
  runbook](docs/operations/backup-and-upgrade.md), [observability
  guide](docs/observability.md), and [current capability audit](docs/mvp-audit.md).
- **Extend Meldbase.** Read the [terminology and semantic boundaries](docs/terminology.md),
  [architecture](docs/architecture.md), [client
  protocol](docs/client-protocol.md), [Worker SDK
  guide](docs/guide/worker-sdk.md), and [CONTRIBUTING.md](CONTRIBUTING.md)
  before changing storage, protocols, or SDKs.

Need a specific guide or API reference? Browse the **[documentation
site](https://crapthings.github.io/meldbase/)** or the [documentation index in
this repository](docs/README.md).

## Contributing and verification

```sh
go test ./...
go test -race ./...
go vet ./...
pnpm check
pnpm test
pnpm build:example
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidance,
[SECURITY.md](SECURITY.md) for private vulnerability reporting, and
[docs/releasing.md](docs/releasing.md) for maintainer release gates.

## License

Licensed under the [Apache License 2.0](LICENSE).
