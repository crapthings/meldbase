# Single-node deployment and recovery

This guide is the operational baseline for one V2 Meldbase database on one
host. It deliberately does not provide high availability: loss of the host,
disk or its surrounding failure domain still makes the service unavailable.
Use it to make a local or single-node deployment repeatable before adding
replication or an external rollback anchor.

The embedded dashboard already provides runtime health, storage/durability
signals and optional Prometheus metrics. It is not a backup scheduler, restore
tool or remote administration plane; the CLI below supplies those operational
steps.

## 1. Prepare the host and data directory

Keep the database, backup artifacts and rehearsal evidence on paths that the
service account can read and write. Backup artifacts must ultimately be copied
to a distinct failure domain; a backup on the same disk does not protect that
disk.

Before trusting a filesystem, run the built binary's non-destructive durability
check against the real data directory. It creates and removes an isolated probe
directory, checks locking, fsync and no-overwrite publication, and emits a
receipt.

```sh
install -d -m 0750 /srv/meldbase/data /srv/meldbase/backups /srv/meldbase/rehearsals
meld durability-check \
  --dir /srv/meldbase/data \
  --out /srv/meldbase/durability-receipt.json
```

Keep that receipt with the deployed build and rerun the check after moving to a
new filesystem or mount configuration. The V2 file is a single database file;
do not copy or overwrite it while the process is running.

## 2. Start locally and observe it

`meld serve` currently has an explicit development-only authenticator. It is
suitable for a local/private development service, not as an internet-facing
production endpoint. Put a production authentication and TLS boundary in the
application that constructs `server.New`, rather than exposing
`--dev-no-auth`.

For a local process with the embedded dashboard, generate a long random token,
keep it out of shell history where practical, and bind the dashboard only to
loopback:

```sh
export MELDBASE_ADMIN_TOKEN='replace-with-at-least-32-random-bytes'
meld serve \
  --db /srv/meldbase/data/app.meld2 \
  --addr 127.0.0.1:8080 \
  --dev-no-auth \
  --admin-addr 127.0.0.1:9091 \
  --admin-diagnostics \
  --admin-metrics
```

Open `http://127.0.0.1:9091/` and enter that token. The dashboard and
authenticated `/metrics` endpoint stay loopback-only by design. For service
orchestration, probe the application listener instead:

```sh
curl --fail http://127.0.0.1:8080/livez
curl --fail http://127.0.0.1:8080/readyz
```

`/livez` says the Go handler is responsive. `/readyz` (and `/health`) also
requires the database to be readable and writable, and returns HTTP 503 after
a fail-stop durability error.

## 3. Back up and rehearse recovery

The physical backup preserves the source database identity and complete
physical history. It is a recovery artifact, not an independently writable
clone. The CLI takes the exclusive process lock, so stop the database process
before this procedure; do not start an original database and one of its
physical backups/restores at the same time.

Create a backup and retain both the artifact and its JSON receipt together:

```sh
meld backup \
  --db /srv/meldbase/data/app.meld2 \
  --out /srv/meldbase/backups/app-20260720.meld2 \
  --timeout 10m > /srv/meldbase/backups/app-20260720.receipt.json
```

Restore only to a new, absent path. `meld restore` verifies the receipt's byte
count, SHA-256, physical shape, identity and complete V2 graph before it makes
the restored file visible:

```sh
meld restore \
  --in /srv/meldbase/backups/app-20260720.meld2 \
  --receipt /srv/meldbase/backups/app-20260720.receipt.json \
  --out /srv/meldbase/rehearsals/app-20260720-restored.meld2 \
  --timeout 10m
meld inspect --db /srv/meldbase/rehearsals/app-20260720-restored.meld2 --require-compatible
meld verify --db /srv/meldbase/rehearsals/app-20260720-restored.meld2 --timeout 10m
```

For a repeatable offline drill that retains all evidence, use:

```sh
scripts/single-node-backup-restore-drill.sh \
  --meld "$(command -v meld)" \
  --db /srv/meldbase/data/app.meld2 \
  --out-dir /srv/meldbase/rehearsals/20260720 \
  --timeout 10m
```

The drill verifies source and restored files, and checks that the restore's
receipt exactly matches the backup receipt. It cannot validate application
semantics, so run application-level smoke queries against the restored file
before treating a new schema or release as recoverable. Copy the finished
artifact and receipt to the off-host backup destination after local
verification.

## 4. Upgrade and rollback

Treat an upgrade as an offline operation until the target release's compatibility
has been qualified:

1. Record the deployed binary version and stop the database process cleanly.
2. Run `meld inspect --db ... --require-compatible` and `meld verify --db ...`.
3. Create a physical backup, copy its artifact and receipt off-host, and run the
   restore drill against the exact backup.
4. Replace the binary, then start it with the existing database path and check
   `/readyz`, the dashboard and application smoke queries.
5. If startup or smoke checks fail before a format-changing operation, stop the
   process and return to the previous binary. Do not assume a newer database
   file can be opened by an older binary; restore the verified pre-upgrade
   artifact to a new path when data rollback is required.

Keep the original database file until the new binary has passed the health and
application checks. Never solve a failed upgrade by overwriting an existing
database or restore target: all backup and restore commands intentionally
refuse that operation.
