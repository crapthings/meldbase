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

This is an intentional alpha-format boundary. A file from an earlier alpha is
not opened or migrated in place. Export it with its matching build and import it
into a newly created database. Production deployments should always verify a
backup/restore exercise before upgrading.

`meld export` produces a versioned JSON Lines logical archive containing
collection names, typed documents, and index definitions. Its final record
binds the preceding records with SHA-256. `meld import` checks that structure,
builds and offline-verifies a private temporary database, then publishes it only
if every check succeeds. It does not preserve database identity, commit history,
rollback anchors, or credentials; configure those again for the new database.

Use the old executable for export and the new executable for import:

```sh
old-meld export --db /data/app.meld --out /migration/app.jsonl
new-meld import --in /migration/app.jsonl --out /data/app-upgraded.meld
new-meld verify --db /data/app-upgraded.meld
```

## Operational commands

```sh
meld inspect --db /data/app.meld --require-compatible
meld verify --db /data/app.meld --timeout 10m
meld backup --db /data/app.meld --out /backup/app.meld
meld restore --in /backup/app.meld --receipt /backup/app.receipt.json --out /data/restored.meld
meld export --db /data/app.meld --out /migration/app.jsonl
meld import --in /migration/app.jsonl --out /data/app-upgraded.meld
```

`verify` is read-only and performs the complete protected-page and index audit.
`backup` and `restore` use a no-overwrite publication step and preserve database
identity; a restored copy should be treated as the same database for rollback
protection and replication purposes.

`export` and `import` are the format-upgrade path, not a substitute for physical
backup. They use new paths and their archive is intentionally data-only.
