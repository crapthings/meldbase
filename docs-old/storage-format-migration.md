# Storage-format and data-migration policy

Meldbase currently supports one on-disk engine revision at a time. An unknown
or incompatible non-empty database fails closed: the binary will not guess at a
legacy layout or mutate it in place. `meld inspect` is read-only and reports
the metadata revision, required features, database identity, commit sequence,
and whether the current reader understands it.

```sh
meld inspect --db /srv/meldbase/data/app.meld --require-compatible
```

## Compatible release upgrade

When the target binary reports `readerCompatible: true`, follow the normal
[upgrade and rollback procedure](/production-single-node#upgrade-and-rollback):
take a new receipt-bound physical backup, verify a disposable restore, stop the
writer, install the new immutable binary, then require `/readyz` and a
representative authenticated query before resuming traffic. Keep the previous
binary and the pre-upgrade artifact until the change window is closed.

Physical backup/restore is the recovery path for a compatible durable file. It
preserves the database identity, indexes, commit history, and unfinished
durable index-build state. It is not a format-conversion mechanism.

## Incompatible or legacy database

There is no automatic in-place migration and no promise that a newer binary can
open an older incompatible file. Migrate in a controlled, offline window using
the last binary that can still read the source and a **new** target path:

```sh
# Run with the prior compatible binary while the source writer is stopped.
/opt/meldbase/previous/meld verify --db /srv/meldbase/data/app.meld
/opt/meldbase/previous/meld export \
  --db /srv/meldbase/data/app.meld \
  --out /srv/meldbase/migrations/app.logical.jsonl > export.receipt.json

# Run with the target binary; the destination must not already exist.
/opt/meldbase/target/meld import \
  --in /srv/meldbase/migrations/app.logical.jsonl \
  --out /srv/meldbase/migrations/app-target.meld > import.receipt.json
/opt/meldbase/target/meld verify --db /srv/meldbase/migrations/app-target.meld
/opt/meldbase/target/meld inspect \
  --db /srv/meldbase/migrations/app-target.meld --require-compatible
```

The logical archive is versioned JSON Lines containing collection names, typed
documents, and index definitions. It is portable data migration, not physical
recovery: it creates a new database identity and does **not** preserve commit
history, resume tokens, active subscriptions, replay continuity, backup
receipts, or unfinished online index builds. Clients must obtain fresh
snapshots after cutover.

Before switching the service path, compare expected document/index counts from
the export and import receipts, run representative indexed queries, test the
least-privilege access manifest, and perform a realtime reconnect. Retain the
source file, the physical pre-migration backup and receipt, both logical
receipts, verification reports, binary revisions, and the approved cutover
record. Roll back by returning to the previous binary and untouched source
path; do not overwrite either file.

## Release policy

Any release that changes storage compatibility must state this in its release
notes before deployment and provide an executable migration path for the
affected source revision. Until such a release exists, operators should treat
an incompatible inspection result as a stop condition and use the prior
compatible binary to export data. Never use filesystem copies, page editing, or
a `force` flag as a substitute for a verified migration.
