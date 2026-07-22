# Reference

This section is the stable starting point for symbols, transport contracts,
commands, and configuration. During alpha, each release tag is the source of
truth for the matching API and storage contract.

## Go API

The public Go packages are documented from their source comments on
[pkg.go.dev](https://pkg.go.dev/github.com/crapthings/meldbase/core):

- [`core`](https://pkg.go.dev/github.com/crapthings/meldbase/core) — embedded
  database, documents, queries, indexes, transactions, backup, and recovery.
- [`server`](https://pkg.go.dev/github.com/crapthings/meldbase/server) — public
  server configuration and security contracts.
- [`admin`](https://pkg.go.dev/github.com/crapthings/meldbase/admin) — embedded
  dashboard, metrics, and runtime diagnostics.

Go documentation is generated from exported source comments, so it stays tied
to the exact module version a Go application imports.

## TypeScript API

The workspace packages are currently a preview and are not published to npm:

- `@meldbase/client` — remote HTTP/WebSocket client and shared query types.
- `@meldbase/client/local` — explicit standalone in-memory collections.
- `@meldbase/react` — `useLiveQuery` adapter for the client query object.
- `@meldbase/worker` — trusted Node.js Worker control boundary; it is not the
  Meldbase server or a client endpoint.

`LocalCollection` and `RemoteCollection` share a query grammar, not a promise
of identical authority, synchronization, or method sets. The local subpath is
not a cache or replica of the remote collection. Read the [local/remote
collection boundary](../client-protocol#local-and-remote-collection-boundary)
before switching an application flow between them.

Start with the [client SDK API guide](./client-sdk) for user-facing methods,
purposes, and examples. The generated [TypeScript API reference](/api/typescript/)
is published with this site from the exact SDK source in each release and is the
symbol-level reference. Use the [Worker SDK guide](../guide/worker-sdk)
for integration patterns, examples, and lifecycle guidance; use the [worker
control protocol](../worker-protocol) for its control-plane contract.

## Transport and CLI

- [HTTP and realtime endpoints](./http)
- [CLI commands and configuration](./cli)
- [Client SDK API guide](./client-sdk)
- [Full versioned client protocol](../client-protocol)
- [RPC idempotency contract](../rpc-idempotency)
