# Contributing

Meldbase is an experimental database, so correctness and recovery evidence take
priority over API breadth.

## Development setup

Requirements:

- Go 1.23 or newer
- Node.js 22
- pnpm 11.10.0 through Corepack or a compatible installation

Install JavaScript dependencies with:

```sh
pnpm install --frozen-lockfile
```

## Required checks

Run these before opening a pull request:

```sh
go test ./...
go test -race ./...
go vet ./...
go generate .
test -z "$(find . -name '*.go' -not -path './.git/*' -not -path './node_modules/*' -not -path './local-playground/*' -exec gofmt -l {} +)"
go run ./cmd/meld demo
pnpm check
pnpm test
pnpm pack:check
pnpm build:example
```

Storage, WAL, index, query, and reactive changes should include tests at the
same boundary they modify. Recovery-sensitive changes should add a crash or
corruption case; query and mutation changes must update the shared Go/TypeScript
conformance corpus when wire behavior changes.

## Design constraints

- Do not add executable query callbacks or source evaluation.
- Preserve exact Int64 semantics across Go and JavaScript.
- Do not publish change events before durable commit.
- Do not weaken row/field authorization, resource limits, or origin checks for
  convenience.
- Storage-format changes must remain fail-closed and document their migration or
  compatibility behavior.

Review the affected implementation, tests, and public API boundary before
proposing a structural change.

## Go API boundary

The stable embedded-database API is the module-root package:

```go
import "github.com/crapthings/meldbase"
```

Use `server` for the public HTTP/WebSocket API and `integrations/*` only for
their named optional integration contracts. `internal/*` is not a public
contract and may be refactored freely within the repository.

Keep public Go names idiomatic: package names are short lower-case nouns;
initialisms use consistent casing (`ID`, `URL`, `HTTP`, `JSON`, `TLS`); and
callers should not need to repeat a package name in an exported identifier.
Use a `Config` type for long-lived component construction and an `Options` type
for an operation's optional controls. Expose a typed or sentinel error whenever
callers need to branch on failure; do not make an error-message string part of a
public contract.

`api_gen.go` is generated from `internal/database`. Run
`go generate .` whenever that transition surface changes; CI rejects stale
generated API code.
