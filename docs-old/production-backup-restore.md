# Single-node backup and restore

Meldbase durable databases can create verified physical restore artifacts while
the server is running. This document describes the minimum production policy;
it is intentionally limited to a single durable node and does not claim HA,
cross-region replication, or point-in-time restore.

## Backup policy

Configure all scheduled-backup flags together on the `meld serve` process:

```sh
meld serve --db /srv/meldbase/data/app.meld \
  --addr 127.0.0.1:8080 \
  --jwt-hs256-secret-file /etc/meldbase/jwt-hs256.secret \
  --jwt-issuer example.internal \
  --jwt-audience meldbase-api \
  --access-policy-file /etc/meldbase/access-policy.json \
  --backup-dir /srv/meldbase/backups \
  --backup-interval 1h \
  --backup-keep 168 \
  --backup-timeout 10m \
  --backup-drill-dir /srv/meldbase/rehearsals
```

`--backup-dir`, `--backup-interval`, `--backup-keep`, and
`--backup-timeout` are an all-or-nothing configuration. The process creates the
directory with owner-only permissions, emits artifacts named
`meldbase-backup-<UTC timestamp>.meld`, and writes the matching JSON receipt as
`…meld.json`. A successful backup is copied, checksummed, opened, and graph
verified before its receipt is published. Retention removes only older files
that match this generated name and have a valid matching receipt; unrelated
files are never selected for deletion.

Backups run inside the serving process. They block writers for the copy duration
but leave reads available. The server cancels and waits for an in-flight backup
before it closes the database.

`--backup-drill-dir` is optional. When set, every successful scheduled artifact
is restored to a new temporary path below that directory and graph-verified;
the restored database is removed after success and a private
`meldbase-restore-drill-*.json` receipt remains as evidence. Put this directory
on independently administered rehearsal storage. Meldbase can require a
different path but cannot prove it is a different host or failure domain.

Copy each artifact and receipt together to independently protected storage.
The retention count only applies to the local backup directory; it is not an
off-site retention policy.

## Lower-cost artifact storage

For an offline backup or a separately coordinated maintenance window, the
standalone command can store the same verified physical artifact as gzip:

```sh
meld backup --db /srv/meldbase/data/app.meld \
  --out /srv/meldbase/backups/app.meld.gz \
  --compression gzip > /srv/meldbase/backups/app.meld.gz.json

meld restore --in /srv/meldbase/backups/app.meld.gz \
  --receipt /srv/meldbase/backups/app.meld.gz.json \
  --out /srv/meldbase/rehearsals/app-restored.meld
```

The command first creates and validates the normal physical snapshot, then
compresses it and publishes the `.gz` file without replacing an existing path.
The receipt records the compressed artifact's byte count and SHA-256. Restore
checks those values before decompression, then verifies the original physical
receipt, database identity, and full storage graph. Keep the compressed file
and its receipt together.

This is a storage and transfer cost reduction, not an incremental backup or
point-in-time recovery feature: it has the same RPO and writer pause as a full
physical backup. Scheduled `meld serve` backups intentionally remain raw
physical artifacts so their restore drill and retention pair format stay
simple and immediately inspectable.

## Recovery objective and drill

With an interval of `1h`, the policy target is an RPO of at most one hour **only
if** the last verified artifact has been copied off the failed host. A failed or
unexported backup has no RPO value. RTO depends on artifact size, storage
throughput, and index verification; record actual restore time during drills
instead of relying on a generic promise.

At least once per release and on the production cadence chosen by the operator,
perform a drill in an empty, independent directory. The source database must be
offline for this script because it invokes the standalone CLI:

```sh
scripts/single-node-backup-restore-drill.sh \
  --meld /usr/local/bin/meld \
  --db /srv/meldbase/data/app.meld \
  --out-dir /srv/meldbase/rehearsals/2026-07-24T150000Z \
  --timeout 10m
```

The drill saves source inspection and verification reports, the backup receipt,
the restored database, and restored inspection and verification reports. Do not
reuse its output directory. Preserve this evidence with the release or change
record.

Physical restore requires the artifact, its exact JSON receipt, and a new output
path. It preserves database identity, indexes, commit history, and resumable
index-build state. The artifact may be raw or gzip-compressed as identified by
the receipt. Logical `export` / `import` is a separate portable archive path and
does not replace physical recovery backup. Meldbase does not support
point-in-time recovery between retained physical artifacts.

For an incompatible storage revision or a planned data-format transition, use
the prior compatible binary to create a logical archive and import it into a
new path with the target binary. This creates a new identity and intentionally
does not preserve replay continuity; see [storage-format migration](/storage-format-migration).

## Failure response

Treat a scheduled-backup failure as an operational alert. Check the server log,
free space and permissions of the backup directory, then run `meld verify` on a
safe copy before retrying. Never overwrite a backup, receipt, or restored file;
all production commands intentionally fail closed when the destination exists.
