# Meldbase

Meldbase is a single-file document database for Go applications. Open a file,
write documents, query them, and close it. When a browser or another process
needs access, run the optional `meld` server in front of the same file.

It is an alpha project. It is a good fit for local development and
single-node applications; do not use it for data you cannot afford to lose.

## Start in two minutes

Install the CLI on macOS:

```sh
brew tap crapthings/tap
brew trust --tap crapthings/tap
brew install crapthings/tap/meldbase
```

Start a local database and HTTP server:

```sh
mkdir -p ./data
meld serve --db ./data/app.meld --dev-no-auth
```

The server listens on `http://127.0.0.1:8080`. In another terminal, check that
it is ready:

```sh
curl http://127.0.0.1:8080/readyz
```

`--dev-no-auth` is intentionally for local development only. Do not expose
that server to a network.

## Embed it in Go

For the smallest setup, use the database directly from your Go process:

```sh
go get github.com/crapthings/meldbase@latest
```

```go
package main

import (
	"context"
	"log"

	"github.com/crapthings/meldbase"
)

func main() {
	ctx := context.Background()
	db, err := meldbase.Open("app.meld")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	todos := db.Collection("todos")
	_, err = todos.InsertOne(ctx, meldbase.Document{
		"title": meldbase.String("Ship something"),
		"done":  meldbase.Bool(false),
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

The `app.meld` file remains after the process exits. One writable process owns
one database file at a time.

## Choose a path

| If you want to… | Start here |
| --- | --- |
| Put durable documents in a Go application | [Five-minute Go tour](docs/getting-started.md) |
| Use Meldbase from a browser | Run `meld serve`, then read the [Browser guide](docs/browser.md) |
| Use Meldbase from another Node.js process | Run `meld serve`, then read the [Node.js guide](docs/node.md) |

## What it is — and is not

Meldbase gives one application a durable document file, typed queries, indexes,
and live query updates. It can add an HTTP and WebSocket boundary when needed.

It is not a hosted database, a MongoDB-compatible service, or a distributed
system. It does not provide automatic failover, sharding, or built-in user
identity.

## Next

- [Five-minute Go tour](docs/getting-started.md)
- [Browser](docs/browser.md)
- [Node.js](docs/node.md)

For project contributors, see [CONTRIBUTING.md](CONTRIBUTING.md). Security
reports are covered by [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License 2.0](LICENSE).
