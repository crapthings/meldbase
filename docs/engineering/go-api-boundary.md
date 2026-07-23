# Go API boundary

Meldbase's stable embedded-database API is the module-root package:

```go
import "github.com/crapthings/meldbase"
```

The root package carries the product name that callers use in code:

```go
db, err := meldbase.Open("app.meld")
```

This is intentionally separate from the optional public packages:

```text
github.com/crapthings/meldbase/server
github.com/crapthings/meldbase/integrations/<name>
```

`server` provides the authenticated HTTP/WebSocket boundary. Each integration
has its own deployment contract and versioning implications. Commands belong
under `cmd/` and are never imported as a library.

## Implementation boundary

`internal/` is the repository-private implementation boundary. It contains
physical storage, indexes, protocol implementation, qualification support and
other code that external modules must not import. It is shared by the root
library, server, integrations and commands, so it intentionally lives at the
repository root rather than below one of those packages.

`internal/database/` implements the root database API. It is intentionally
private: external modules cannot import it. The root package mirrors its
approved public declarations via `api_gen.go`. It owns document, collection,
query, transaction, subscription, and backup semantics. `internal/storage/`
owns the physical file format, persistence, and recovery mechanics; keeping
those names separate prevents a generic `core` or ambiguous `engine` layer.

## API design rules

- Prefer the product package as the public entry point; do not create generic
  `api`, `common`, `types` or `util` packages.
- Use short, lower-case package names. In exported Go identifiers write common
  initialisms consistently: `ID`, `URL`, `HTTP`, `JSON`, `RPC`, `TLS`, and
  `SHA256`.
- Do not repeat the package name in exported identifiers. For example,
  `server.Config` is clearer than `server.ServerConfig` when the package has
  only one server configuration.
- Use `Config` for the full configuration of a long-lived component. Use
  `Options` for optional controls of a specific operation.
- Make programmatically meaningful failures testable with a sentinel error or
  typed error. Error strings explain failures; they are not protocol values.
- Every public API or wire-contract change needs an executable compatibility
  test, not only a documentation change.

## Guardrails

Run `go generate .` after changing the `internal/database` API. The generated
root façade is committed, and CI regenerates it and rejects a diff. CI also
checks every tracked Go source file with `gofmt`.
