# Storage format

Meldbase has one supported durable format. `Open` and `OpenWithOptions` create
or open that single-file copy-on-write database; there is no runtime format
selection or automatic migration path.

`DetectStorageFormat` distinguishes a missing/empty path from a valid current
file. `InspectStorageFormat` reads the two Meta pages without taking a writer
lock and reports the revision, generation, commit sequence, negotiated feature
bits, database identity, and reader compatibility. Unknown or malformed
non-empty files fail closed with `ErrCorrupt`; a valid but unsupported future
revision fails with `ErrUnsupportedFormat` when opened.

Each successful mutation publishes a new COW root and Meta record in the same
file. The Commit Log retains the configured replay window, and recovery may
discard only a provably incomplete main-file tail or fall back to an older valid
root. `RecoveryRequireClean` rejects either action before it changes bytes.

## Alpha compatibility

This is an intentional alpha-format boundary. A file from an earlier alpha may
not open, and Meldbase makes no cross-version migration commitment yet. A future
breaking change will have release-specific guidance when it is planned.
Production deployments should always verify a backup/restore exercise before
upgrading.

`meld export` produces a versioned JSON Lines logical archive containing
collection names, typed documents, and index definitions. Its final record
binds the preceding records with SHA-256. `meld import` checks that structure,
builds and offline-verifies a private temporary database, then publishes it only
if every check succeeds. It does not preserve database identity, commit history,
rollback anchors, or credentials; configure those again for the new database.

Use it as a portable data snapshot or an import rehearsal with the current
build:

```sh
meld export --db /data/app.meld --out /archive/app.jsonl
meld import --in /archive/app.jsonl --out /data/app-imported.meld
meld verify --db /data/app-imported.meld
```

## Operational commands

```sh
meld inspect --db /data/app.meld --require-compatible
meld verify --db /data/app.meld --timeout 10m
meld backup --db /data/app.meld --out /backup/app.meld
meld restore --in /backup/app.meld --receipt /backup/app.receipt.json --out /data/restored.meld
meld export --db /data/app.meld --out /archive/app.jsonl
meld import --in /archive/app.jsonl --out /data/app-imported.meld
```

`verify` is read-only and performs the complete protected-page and index audit.
`backup` and `restore` use a no-overwrite publication step and preserve database
identity; a restored copy should be treated as the same database for rollback
protection and replication purposes.

`export` and `import` are a portable, data-only archive path—not a substitute
for physical recovery backup or a cross-version compatibility promise. They use
new paths.
