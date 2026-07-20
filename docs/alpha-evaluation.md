# Evaluate Meldbase safely

Meldbase is ready for **small-scope alpha evaluation**: a single application,
one durable local file, non-critical data, and a team that can inspect logs and
restore from a backup. This is not a production qualification or an HA claim.

Use this guide to decide whether Meldbase fits your application and to produce
feedback that can improve the database without exposing credentials or user
data.

## Good alpha workloads

- an internal tool, prototype, local service, or a feature with an independent
  copy of its data;
- one Go process embedding the database, or one single-node HTTP/WebSocket
  server behind your application boundary;
- ordinary typed documents, bounded queries, indexes, and realtime UI updates;
- an application-owned identity service that can issue test JWTs when using the
  public server API.

Keep an export or another system of record for anything you would regret losing.
The current alpha format may change directly between releases.

## Do not use it for these yet

- the only copy of production or regulated data;
- automatic failover, multi-primary writes, sharding, or distributed
  transactions;
- an application that needs built-in end-user accounts, passwords, or
  membership management;
- a filesystem/mount/controller combination that you want to call
  production-qualified without running the retained evidence process.

See the [capability audit](./mvp-audit) and [roadmap](./roadmap) for the exact
boundary.

## A minimal evaluation loop

1. Run the embedded [Go quick start](./guide/getting-started), or create a
   private local bundle with `meld init --dir ./meldbase-local`.
2. Keep the server on loopback while evaluating. Use `--dev-no-auth` only for
   disposable local data. For JWT mode, let your own test issuer sign tokens;
   Meldbase deliberately does not create user accounts.
3. Declare the generic browser/data API with an
   [access-policy manifest](./guide/access-policies). Start with
   `collaborative` only for genuinely shared records; use `owner` for
   self-owned data and `rpc_only` for approval, money, membership, or other
   business-sensitive operations.
4. Exercise the UI with two active workspaces. Confirm that inserts receive the
   server-owned workspace/owner fields, cross-workspace reads are absent, and
   updates to those fields fail.
5. Create a backup, restore it to a **new** path, then verify the restored file:

   ```sh
   meld backup --db ./data/app.meld --out ./backups/app.meld > backup-receipt.json
   meld restore --in ./backups/app.meld --receipt backup-receipt.json \
     --out ./rehearsals/app-restored.meld
   meld verify --db ./rehearsals/app-restored.meld
   ```

6. Before upgrading an alpha build, export any valuable test data and rehearse
   restore. Do not assume an older alpha file will open in a newer format.

The [single-node guide](./single-node-deployment) is the authority for secret
permissions, probes, reverse proxies, dashboards, backups, and upgrades.

## What to verify

| Area | Practical check |
| --- | --- |
| Data API | Query, insert, update, delete, index creation, and validation errors match the documented typed contract. |
| Realtime | Initial snapshot, ordered delta, reconnect, and resync behavior keep the UI correct. |
| Isolation | Two JWT workspaces cannot query, subscribe to, update, or delete each other's scoped records. |
| Recovery | A physical backup restores to a new file and `meld verify` succeeds. |
| Operations | `/livez`, `/readyz`, the authenticated operator dashboard, and metrics behave as expected behind your chosen proxy. |

For a quick local concurrency signal, `meld storage-soak --profile custom` can
run for seconds on an empty target directory. It exercises writers, snapshot
readers, index catch-up, reclamation, and reopen verification. A custom run is
exploratory only: it is not the retained four-hour release soak or a production
qualification. See [filesystem qualification](./filesystem-qualification) for
the evidence hierarchy.

## Useful feedback

Include the following in a bug report or evaluation note, after removing
credentials, bearer tokens, user documents, hostnames, and private URLs:

- Meldbase revision, Go version, OS/architecture, and filesystem type;
- whether the database was embedded or served, plus the relevant non-secret
  configuration shape;
- a minimal typed query/mutation or protocol sequence that reproduces the
  problem;
- expected versus observed behavior, stable error code, and sanitized logs;
- whether `meld inspect` or `meld verify` succeeds on a copied test file.

Do not attach production database files, JWTs, secrets, or dashboard tokens.
Use the security reporting path in [SECURITY.md](https://github.com/crapthings/meldbase/blob/main/SECURITY.md)
for a vulnerability rather than a public issue.
