# Storage format

Meldbase has one supported durable format. `Open` and `OpenWithOptions` create
or open that single-file copy-on-write database; there is no runtime format
selection, legacy WAL, or automatic migration path.

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

This is an intentional alpha-format boundary. Builds after this change do not
open historical V1 files and do not ship a V1-to-current migration API. Before
upgrading a personal test database, export the data with the old build and
import it into a newly created database. Production deployments should always
verify a backup/restore exercise before upgrading.

## Operational commands

```sh
meld inspect --db /data/app.meld --require-compatible
meld verify --db /data/app.meld --timeout 10m
meld backup --db /data/app.meld --out /backup/app.meld
meld restore --in /backup/app.meld --out /data/restored.meld
```

`verify` is read-only and performs the complete protected-page and index audit.
`backup` and `restore` use a no-overwrite publication step and preserve database
identity; a restored copy should be treated as the same database for rollback
protection and replication purposes.
