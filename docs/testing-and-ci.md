# Test databases and CI lifecycle

Meldbase tests must never reuse a developer or production database path. Every
test owns a new database and has one clear cleanup owner. This is both a data
safety rule and a parallel-test requirement.

## Go tests

Use `t.TempDir()` for the directory and create the durable file below it. The
test framework removes the entire directory whether the test succeeds, fails,
or is skipped. Do not use a fixed `/tmp/*.meld` name.

```go
func TestDurableQuery(t *testing.T) {
    path := filepath.Join(t.TempDir(), "query.meld")
    db, err := meldbase.Open(path)
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = db.Close() })

    // Exercise the database.
}
```

For a child-process service test, reserve an ephemeral listener with
`--addr 127.0.0.1:0`, wait for the explicit startup line or readiness probe,
and always send `SIGTERM` followed by `Wait` from `t.Cleanup`. The CLI tests
cover both the unsafe local path and the JWT/policy path this way; their
database, key, and policy files all live in `t.TempDir()`.

Never remove a database file while it is open. Close the database or let the
child process exit first. A test that needs to verify recovery should reopen
the same temporary path; a test that needs a new database should use another
temporary directory.

## Node, browser, and SDK tests

Node tests create temporary server/database directories with the platform
temporary-directory API and remove them in `finally`/test teardown. Browser
tests start an isolated loopback server and close it after the test. They must
not depend on `local-playground/`, which is a maintainer-private manual
environment and is intentionally outside CI and released examples.

When a fixture needs a retained artifact for inspection, make that explicit
with a path argument and refuse an existing destination. Normal CI fixtures
must remain self-cleaning.

## CI contract

The main workflow runs, on every relevant source change:

- `go test ./...`, `go test -race ./...`, and `go vet ./...`;
- a multi-platform storage/locking/recovery contract plus a disposable target
  volume capability probe;
- the end-to-end CLI demo and process-start/stop tests;
- locked Node dependencies, SDK tests, package-contract checks, and browser
  HTTP/realtime/React hydration coverage.

The SDK compatibility workflow repeats SDK checks on supported Node versions.
Long-running soak, destructive power/I/O campaigns, and target-hardware
capacity evidence are separate, explicitly controlled workflows; they do not
run in a pull-request test job because they require dedicated disposable
storage or operator-managed infrastructure.

Use the normal local gate before a pull request:

```sh
go test ./...
go test -race ./...
go vet ./...
pnpm check
pnpm test
pnpm test:sdk:browser
```

The browser command requires a local Chrome installation. CI supplies Chrome
only on its selected browser matrix row. See [production qualification]
(/production-qualification) for evidence that is intentionally not a generic
CI gate.
