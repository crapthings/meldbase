# Single-node deployment and change runbook

This is the supported operational shape for one durable Meldbase node: the
server and embedded admin listener bind to loopback, while an application-owned
TLS proxy provides the public boundary. It is not an HA deployment and does not
provide failover or a cross-version storage compatibility promise during alpha.

The repository includes a systemd unit, restricted environment template, access
policy example, and launcher under `deploy/single-node/systemd/`. The unit runs
as the dedicated `meldbase` account with `UMask=0077`, a private temporary
directory, a protected system filesystem, and write access only below
`/var/lib/meldbase`.

## Install and configure

Build or obtain the exact release binary, then install the supplied files. Keep
the service account, state directory, secret, and configuration permissions
deliberately separate from the public TLS proxy configuration.

Meldbase also ships a non-root container build for environments that distribute
applications as OCI images; see [Container image](/container-image). The
container is a delivery format, not a replacement for the same host-bound
proxy, persistent-volume, secret, backup, and restore-drill requirements
described here.

```sh
install -d -m 0750 -o meldbase -g meldbase /var/lib/meldbase/data /var/lib/meldbase/backups
install -d -m 0750 -o root -g meldbase /etc/meldbase
install -m 0750 deploy/single-node/systemd/meldbase-single-node /usr/local/libexec/meldbase/meldbase-single-node
install -m 0644 deploy/single-node/systemd/meldbase.service /etc/systemd/system/meldbase.service
install -m 0640 -o root -g meldbase deploy/single-node/systemd/meldbase.env.example /etc/meldbase/meldbase.env
install -m 0640 -o root -g meldbase deploy/single-node/systemd/access-policy.json.example /etc/meldbase/access-policy.json
```

Create `/etc/meldbase/jwt-hs256.secret` with mode `0640`, owner `root:meldbase`,
and at least 32 bytes of secret material. Edit `/etc/meldbase/meldbase.env`:
every `MELDBASE_*` value is the source of configuration for the launcher. Set
the JWT issuer/audience, exact browser origins, access policy, random admin
token, and the public realtime URL used by clients. Do not expose either
`MELDBASE_ADDR` or `MELDBASE_ADMIN_ADDR` beyond loopback; TLS, host routing, and
public health policy belong in the reverse proxy.

For HS256 key rotation, create the new secret file, point
`MELDBASE_JWT_HS256_SECRET_FILE` to it, and temporarily set
`MELDBASE_JWT_HS256_PREVIOUS_SECRET_FILE` to the former file. Restart, switch
the token issuer to the new secret, wait for the maximum token lifetime plus
clock skew, then clear the previous-secret setting and restart again.

To enable scheduled physical backups, set all four values together. Leaving all
four blank disables the schedule.

```ini
MELDBASE_BACKUP_DIR=/var/lib/meldbase/backups
MELDBASE_BACKUP_INTERVAL=1h
MELDBASE_BACKUP_KEEP=168
MELDBASE_BACKUP_TIMEOUT=10m
```

The supplied sandbox allows that directory because it is below
`/var/lib/meldbase`. See [Backup and restore](/production-backup-restore) for
RPO/RTO and off-host-copy requirements.

The systemd environment enables `MELDBASE_INDEX_BUILD_SCHEDULER=1` by default.
It resumes unfinished durable online index builds after a restart in bounded
background slices, while normal reads and writes continue. Set it to `0` only
for a controlled maintenance window in which an operator will use
`meld index-build resume` manually. Failed builds remain inspectable and are
never silently retried as a different index definition.

## Resource budgets and capacity configuration

Resource limits are immutable for one opened database: change them only through
a reviewed configuration file and service restart. The defaults are intentionally
finite; zero fields in the file select those defaults rather than disabling a
budget. Start with the defaults, collect your own workload evidence, and lower
untrusted-query/realtime limits before raising any scan, candidate, or document
budget. Do not treat a larger limit as a performance optimization—it increases
the maximum work admitted to one request or view.

For the supplied unit, create a root-owned strict JSON file such as
`/etc/meldbase/resource-limits.json`, then set
`MELDBASE_RESOURCE_LIMITS_FILE` in `meldbase.env`:

```json
{
  "maxDocumentBytes": 4194304,
  "maxTransactionBytes": 16777216,
  "maxReactiveViewDocuments": 2000,
  "maxReactiveViewBytes": 16777216,
  "maxQueryDocumentsExamined": 25000,
  "maxQueryKeysExamined": 25000,
  "maxQueryCandidates": 25000,
  "maxQuerySortBytes": 16777216,
  "maxQuerySkip": 25000,
  "maxQueryPredicateSteps": 2500000
}
```

Unknown fields, malformed JSON, multiple JSON values, and invalid combinations
fail before the database opens. The full field contract and production defaults
are in [query execution budgets](/query-semantics#execution-budgets). Record
the file digest beside each capacity run; an operator needs to distinguish a
budget rejection from a workload regression.

Run capacity evidence on the intended production hardware and storage class,
not a developer laptop. The bundle is deliberately only an input to a rollout
decision—its synthetic workload is not an SLA:

```sh
bash scripts/capacity-evidence.sh \
  --out-dir /var/lib/meldbase/evidence/capacity-20260724 \
  --documents 1024,8192,65536 \
  --iterations 200 \
  --runtime-count 5 \
  --runtime-benchtime 3s \
  --resource-limits-file /etc/meldbase/resource-limits.json
```

It creates a fresh owner-only directory containing source revision/status,
non-identifying CPU/memory/filesystem capability data, query observer JSON for
memory and durable backends, Go benchmark JSON, and the SHA-256 digest/byte
count of the supplied resource-limit file. Preserve that bundle with
representative document-shape evidence and the accepted
concurrency/subscription target. Re-run it after changing the database volume,
Go version, resource limits, index mix, or material workload shape.

Create a reviewable Markdown summary without changing the evidence files:

```sh
node scripts/summarize-capacity-evidence.mjs \
  --in-dir /var/lib/meldbase/evidence/capacity-20260724 \
  --out /var/lib/meldbase/evidence/capacity-20260724-summary.md
```

The summary is a convenience view; its source JSON remains authoritative for
latency, work, allocation, and benchmark comparisons.

Start and inspect the unit:

```sh
systemctl daemon-reload
systemctl enable --now meldbase
systemctl status meldbase --no-pager
journalctl -u meldbase -f
```

Only one writable process may open a database path. Stop the service before
running standalone `backup`, `restore`, `import`, or other writer commands
against its database. `verify` obtains a non-blocking shared advisory lock and
therefore deliberately fails while the server owns the writer lock; use a
verified backup or a maintenance window for a full offline verification.

## Probes and normal operation

The service provides JSON probes with `Cache-Control: no-store`:

| URL | Meaning | Expected use |
| --- | --- | --- |
| `GET /livez` | process is serving HTTP | liveness only |
| `GET /readyz` | database is readable and writable | readiness and traffic admission |
| `GET /health` | alias of readiness | simple load-balancer compatibility |

`/readyz` and `/health` return HTTP 503 when the database is closed or in a
fail-stop state; `/livez` remains 200 while the process is alive. Probe them on
the loopback listener or through an explicitly configured proxy route:

```sh
curl --fail --silent --show-error http://127.0.0.1:8080/readyz
curl --fail --silent --show-error http://127.0.0.1:8080/livez
```

`systemctl stop meldbase` sends `SIGTERM`. The server stops accepting HTTP,
waits for its current scheduled backup if one is in progress, shuts down admin
and worker listeners, then closes the database. Treat an unexpected stop or a
scheduled-backup failure in the journal as an alert. Do not delete, truncate,
or manually copy the open database file.

## Disk, logs, and alerts

The embedded admin listener can expose authenticated Prometheus metrics when
`--admin-metrics` is enabled (the supplied systemd launcher enables it). Scrape
it only through the authenticated loopback/admin boundary. Alert before either
filesystem or database admission is exhausted:

- Node-level free bytes/inodes for the database, backup, and drill filesystems;
  Meldbase cannot replace an operating-system filesystem monitor.
- `meldbase_storage_used_bytes` against `meldbase_storage_max_bytes` when a
  storage quota is configured.
- `meldbase_storage_quota_exhausted`,
  `meldbase_storage_limit_rejections_total`, and
  `meldbase_storage_free_space_load_failures_total`.
- Retention pressure and rollback-anchor failure metrics when those features
  are enabled.

On an ENOSPC or quota rejection, the write is rejected before a new physical
generation is published; it is not a fail-stop by itself. Stop traffic growth,
preserve the journal and metric evidence, free capacity outside the database
file, take or copy a verified backup if space permits, then validate `/readyz`
and a controlled write before resuming traffic. Never delete pages from the
database file to make space.

The single-node service emits low-cardinality JSONL lifecycle and backup events
to stderr; the supplied unit explicitly sends both stdout and stderr to
`journald` with `SyslogIdentifier=meldbase`. Set `MELDBASE_LOG_LEVEL` to
`debug`, `info` (default), `warn`, or `error` to select ordinary service events.
It never enables request-content logging: request bodies, JWTs, actor/workspace
values, query values, and document values are excluded. Security and admin
audit events are deliberately not filtered by this threshold.

Configure rotation, forwarding, and alerting with the host's `journald`/log
platform; Meldbase does not rotate arbitrary log files itself. Establish a
host policy such as a `/etc/systemd/journald.conf.d/meldbase.conf` drop-in with
an appropriate `SystemMaxUse=` and `MaxRetentionSec=` for your evidence
retention requirement, then restart `systemd-journald` through the distribution
approved procedure. Treat scheduled-backup or restore-drill failures as
alertable events. Retain audit JSONL separately from high-volume service logs
when the log platform supports routing.
Authenticated admin reads and rejected admin requests produce safe JSONL audit
events in the same journal. Data API rejections (401,
403, and 429) produce separate safe JSONL security events with a templated
route and no client address, query string, token, or request body.

## Upgrade and rollback

Perform an upgrade during a controlled window. Because alpha storage has no
cross-version compatibility promise, an upgrade must have a verified physical
backup and a tested restore path; an executable rollback by itself is not a
data rollback.

1. Record the current binary version, unit status, `/readyz`, and active
   configuration revision.
2. Stop the service gracefully: `systemctl stop meldbase`.
3. Create a new physical backup and receipt on a separate protected path, then
   copy both off the host:

   ```sh
   stamp=$(date -u +%Y%m%dT%H%M%SZ)
   meld backup --db /var/lib/meldbase/data/app.meld \
     --out "/var/lib/meldbase/backups/pre-upgrade-$stamp.meld" \
     --timeout 10m \
     > "/var/lib/meldbase/backups/pre-upgrade-$stamp.meld.json"
   ```

4. Restore that pair to a disposable path and run `meld verify` before changing
   the production binary. Preserve the receipt and verification output.
5. Install the new binary at a new immutable path, set `MELDBASE_BIN` to that
   path, run `systemctl daemon-reload` if the unit changed, then
   `systemctl start meldbase`.
6. Require `/readyz` to return 200, inspect `journalctl -u meldbase`, execute a
   representative authenticated query and realtime reconnect, and retain the
   release evidence.

If the new release must be rolled back, stop it first. Restore the pre-upgrade
artifact **to a new database path** using its exact receipt, point
`MELDBASE_DB` at that restored path, point `MELDBASE_BIN` at the prior binary,
and start the unit. Do not overwrite the failed database in place and do not
restart an old binary against data written by the newer release unless that
release explicitly documented compatibility. Verify readiness and the restored
commit sequence before resuming traffic.

Run the independent restore drill regularly, not only during upgrades. The
single-node production boundary remains: no automatic failover, no point-in-time
recovery between retained artifacts, and no generic capacity/SLA promise.
