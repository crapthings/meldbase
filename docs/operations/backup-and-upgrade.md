# Backup, recovery, and upgrade runbook

Use this runbook for one Meldbase database on one host. It assumes the
[single-node deployment](../single-node-deployment) is already configured and
that the database process can be stopped for a physical backup.

The commands below assume the Linux systemd service from that deployment guide;
replace the `systemctl` calls with your own process supervisor where needed.

This is an operator procedure, not a background backup scheduler. Your
deployment should run it from its own timer/orchestrator and copy completed
artifacts to a separate failure domain.

## Routine physical backup

1. Stop the process cleanly and confirm no other process has the database open.
2. Create a new, absent artifact path and redirect the JSON receipt into the
   same backup set.
3. Verify the resulting artifact before copying it off-host.

```sh
systemctl stop meldbase

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
backup_root="/srv/meldbase/backups/$run_id"
install -d -m 0750 "$backup_root"

meld backup \
  --db /srv/meldbase/data/app.meld \
  --out "$backup_root/app.meld" \
  --timeout 10m > "$backup_root/backup-receipt.json"

meld inspect --db "$backup_root/app.meld" --require-compatible \
  > "$backup_root/inspect.json"
meld verify --db "$backup_root/app.meld" --timeout 10m \
  > "$backup_root/verify.json"

# Copy the complete $backup_root directory to independent storage here.
systemctl start meldbase
```

`meld backup` and `meld restore` intentionally refuse to overwrite a path.
Treat that as a safety property: never delete or replace a known-good artifact
to make a new backup fit.

## Retention policy

Retain a backup set as one unit: the physical artifact, `backup-receipt.json`,
and its verification reports. Do not retain an artifact without its receipt;
the restore command requires the receipt to verify byte count, SHA-256,
database identity, and graph shape before publishing a new restored file.

Choose the count and schedule for your recovery objective, then document it in
your deployment. A conservative starting pattern is seven daily, four weekly,
and twelve monthly *verified off-host* sets, plus the pre-upgrade set until the
new release has been accepted. The database cannot decide this for you because
it does not know your legal retention, capacity, or recovery-time requirements.

Before pruning, verify that the off-host copy contains every file in the backup
set. Prune whole dated directories only; never delete a receipt or artifact from
an otherwise retained set. Record the run ID, artifact SHA-256, copy location,
and prune decision in your normal operations log.

## Restore rehearsal

Run a rehearsal regularly and before every upgrade. Restore to a new, absent
path—never over the running database—and keep the output as evidence:

```sh
rehearsal="/srv/meldbase/rehearsals/$run_id"
install -d -m 0750 "$rehearsal"

meld restore \
  --in "$backup_root/app.meld" \
  --receipt "$backup_root/backup-receipt.json" \
  --out "$rehearsal/app-restored.meld" \
  --timeout 10m > "$rehearsal/restore-receipt.json"

meld inspect --db "$rehearsal/app-restored.meld" --require-compatible \
  > "$rehearsal/inspect.json"
meld verify --db "$rehearsal/app-restored.meld" --timeout 10m \
  > "$rehearsal/verify.json"
```

Then run application-level smoke queries using the restored file in an isolated
environment. Storage verification proves the physical database graph; it cannot
prove that a particular application schema, identity configuration, or external
integration still behaves as intended.

The repository also supplies
[`single-node-backup-restore-drill.sh`](https://github.com/crapthings/meldbase/blob/main/scripts/single-node-backup-restore-drill.sh)
for an offline all-in-one backup and restore rehearsal.

## Upgrade and data rollback

1. Record the running binary version and configuration, then stop the service.
2. Run `meld inspect --require-compatible` and `meld verify` on the current
   file.
3. Create and rehearse a physical pre-upgrade backup; copy the complete set
   off-host.
4. Install the new binary, start it with the existing database path, check
   `/readyz`, and run application smoke queries.
5. If the new binary fails before it changes the file, stop it and return to the
   previous binary. If data rollback is needed, restore the pre-upgrade artifact
   to a **new** path and make the promotion explicit in your deployment.

Do not assume that an older binary can open a file once a newer binary has
performed a format-changing operation. Do not solve a failed upgrade by
overwriting the old file. Keep the old file and the pre-upgrade backup until the
new deployment passes the acceptance window you chose.

## Rollback-protection boundary

An ordinary physical backup preserves database identity and history. It is not
an independently writable clone, and a single-host backup strategy cannot stop
an operator with full host access from restoring an older valid file. The local
single-node package therefore makes no rollback-protection claim.

If that threat matters, use the separately operated remote rollback-anchor
service and its qualification procedure. It must live in independent failure
domains and has its own mTLS keys, quorum, and retained evidence; see
[rollback-anchor service](../rollback-anchor-service). Do not treat a local
anchor file on the same host as independent protection.
