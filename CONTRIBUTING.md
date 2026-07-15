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
go run ./cmd/meld demo
pnpm check
pnpm test
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

See `docs/architecture.md`, `docs/storage-format.md`, and `docs/mvp-audit.md`
before proposing a structural change.
