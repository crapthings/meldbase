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

- `@meldbase/client` — local collections and remote HTTP/WebSocket client.
- `@meldbase/react` — `useLiveQuery` adapter for the client query object.
- `@meldbase/server` — authenticated Node.js worker control boundary.

The generated [TypeScript API reference](/api/typescript/) is published with
this site from the exact SDK source in each release. Use the
[server worker SDK guide](../guide/server-worker-sdk) for integration patterns,
examples, and lifecycle guidance; use the
[worker protocol](../server-js-sdk) for its control-plane contract.

## Transport and CLI

- [HTTP and realtime endpoints](./http)
- [CLI commands and configuration](./cli)
- [Full versioned client protocol](../client-protocol)
- [RPC idempotency contract](../rpc-idempotency)
