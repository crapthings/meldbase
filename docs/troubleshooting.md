# Troubleshooting a single-node deployment

Use this guide with the [single-node deployment](/production-single-node),
[backup and restore](/production-backup-restore), and [security](/production-security)
guides. Preserve the journal, metrics, backup receipt, and command output before
trying a second action; a retry must not erase the evidence from the first
failure.

## Probe states

| Observation | Meaning | Immediate action |
| --- | --- | --- |
| `/livez` is 200 and `/readyz` is 503 | The process is alive but the database is unavailable or fail-stopped. | Remove it from traffic, preserve `journalctl -u meldbase` output, and inspect the last database/backup error. Do not copy or edit the open database file. |
| Both probes fail | The listener/process or local proxy is unavailable. | Check `systemctl status meldbase`, the configured bind address, firewall/proxy health, and recent unit logs. |
| Both probes are 200 but requests fail | The data path is ready; investigate authentication, access policy, rate limiting, or request-specific resource limits. | Use the stable external error code and safe rejection metrics; do not enable diagnostics for an untrusted requester. |

`/livez` proves only that HTTP is serving. Route production traffic only when
`/readyz` succeeds.

## File lock, verification, and corruption reports

Exactly one writable process may open a database path. A standalone writer
command failing while the unit is running is expected protection, not a reason
to force-remove a lock. Stop the unit before `backup`, `restore`, `import`, or
other writer commands. `meld verify` deliberately takes a non-blocking shared
lock and therefore also fails while the writable server owns the file; verify a
physical backup or use a maintenance window instead.

If `meld verify` reports corruption or a server enters fail-stop:

1. Remove public traffic and preserve logs, metrics, the database file, and any
   rollback-anchor evidence. Do not attempt in-place repair.
2. Identify the newest receipt-bound physical backup that passed its independent
   restore drill.
3. Restore it **to a new path**, verify that path offline, and record the
   recovered commit sequence.
4. Point the service at the restored path, require `/readyz`, and perform a
   least-privilege authenticated read before reopening traffic.

The recovery point is the backup receipt's commit sequence, not the wall-clock
time at which an operator notices the incident.

## Disk, quota, and backup failures

On ENOSPC or a storage-quota rejection, a write is rejected before a new
generation is published. Pause traffic growth, retain the journal and
`meldbase_storage_*` metrics, free capacity **outside** the database file, and
then make a new verified backup if space permits. Never truncate, delete pages
from, or manually copy an open database file to recover space.

For a scheduled-backup or restore-drill failure, retain its JSON receipt and
the journal event, verify that the backup and drill directories are distinct
protected targets, and run a controlled `meld restore` plus `meld verify` on a
new disposable path before declaring the schedule healthy again. Check the
backup interval, timeout, retention count, filesystem free space/inodes, and
the service account's read/write permissions.

## Authentication, authorization, and admission rejections

| Code/status | Usual cause | Operator check |
| --- | --- | --- |
| `unauthenticated` / 401 | Missing, expired, malformed, wrong issuer/audience, or wrong workspace JWT claim. | Inspect identity-provider/proxy logs and the configured issuer, audience, secret/JWKS, and workspace claim name. Do not log the bearer token. |
| `forbidden` / 403 | Origin policy or collection/field/query policy denied the request. | Validate the access manifest and exact HTTP/WebSocket origin lists; use `meld access-policy explain` only with operator-held policy access. |
| `rate_limited` / 429 | The bounded per-client bucket is exhausted. | Inspect aggregate rejection metrics and trusted-proxy CIDRs; confirm the proxy overwrites `X-Forwarded-For`. Keep a broader proxy-side limiter. |
| `request_too_large` or `resource_limit_exceeded` | Body, result, query-work, realtime, or RPC bound was hit. | Inspect Explain and low-cardinality metrics; reduce result shape, add an appropriate index, paginate, or tune a reviewed deployment limit. |

Data API rejection journal events intentionally contain only a template route,
status, and outcome. Correlate them with the identity provider or trusted proxy
when actor identity is required.

## Query latency or budget pressure

Start with Explain and the query observer rather than raising a limit blindly:

```sh
go run ./cmd/meld-query-observer --backend durable --documents 8192 --format json
bash scripts/query-capacity-matrix.sh --out /var/lib/meldbase/query-capacity
```

High physical keys relative to unique candidates often means overlapping OR
branches; high documents examined usually means a weak access path or residual
filter; high sort bytes means sorting more candidates than the requested window.
An Explain recommendation is an investigation lead, not an instruction to add
an index without measuring write and storage cost. See [query semantics](/query-semantics)
for budgets and Explain fields.

## Realtime resync and index builds

`resync_required` means the client must obtain a fresh snapshot and subscribe
again; it must not keep applying deltas from the old token. This can occur when
history retention cannot satisfy a resume request or a policy lease changed.

Durable online index builds are visible to operators through the CLI:

```sh
meld index-build list --db /var/lib/meldbase/data/app.meld
meld index-build resume --db /var/lib/meldbase/data/app.meld --id <build-id>
meld index-build abort --db /var/lib/meldbase/data/app.meld --id <build-id>
```

An interrupted non-terminal build is persistent and can be resumed after a
clean reopen. The supplied production systemd launcher enables the bounded
background scheduler; a custom `meld serve` invocation must opt in with
`--index-build-scheduler` (and may tune its poll interval, work slice, and
concurrency). A terminal unique-key, invalid-index, resource, or history-loss
failure is intentionally inspectable and must be aborted before starting a
corrected build. To change an already published index while keeping the old
definition query-visible until one atomic catalog commit, start a replacement
with the same name and then resume it:

```sh
meld index-build rebuild --db /var/lib/meldbase/data/app.meld \
  --collection items --name by_lookup --field rank:-1
meld index-build resume --db /var/lib/meldbase/data/app.meld --id <build-id>
```

The rebuild record is persistent and includes `replaceExisting: true` in
`index-build list`. If it fails or is aborted, the old published index remains
unchanged. A replacement is durable/online-index-build functionality; memory
mode still uses direct in-process index creation only.
