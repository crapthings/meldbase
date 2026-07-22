# Single-node deployment and recovery

This guide is the operational baseline for one Meldbase database on one host.
It deliberately does not provide high availability: loss of the host,
disk or its surrounding failure domain still makes the service unavailable.
Use it to make a local or single-node deployment repeatable before adding
replication or an external rollback anchor.

The embedded dashboard already provides runtime health, storage/durability
signals and optional Prometheus metrics. It is not a backup scheduler, restore
tool or remote administration plane; the CLI below supplies those operational
steps.

## 1. Initialize a local bundle

For a workstation or a new single-node host, create the directory layout,
private HS256 secret, admin token and loopback-only launcher in one command:

```sh
meld init --dir ./meldbase-local
./meldbase-local/start.sh
```

`init` refuses an existing directory; it never rotates or overwrites a running
deployment's credentials. Its generated `config/meldbase.env`,
`config/access-policy.json`, and `secrets/jwt-hs256.secret` are mode `0600`.
The issuer, audience and initial collaborative `projects`, `tasks`, and
`comments` policy default to `meldbase-local` and `meldbase-api`. Edit the
manifest before exposing a generic data API, then validate it without starting
the server:

```sh
meld access-policy validate --file ./meldbase-local/config/access-policy.json
```

Use `--jwt-issuer`, `--jwt-audience`, `--collections` and `--workspace-field`
to shape the initial manifest at creation time. See
[collection access policies](./guide/access-policies) for owner-only,
RPC-only, and field-limited declarations.

The command does not create application accounts or mint JWTs. The application
identity service must sign tokens with the generated secret and include `sub`,
`exp`, `iss`, `aud` and `workspace_id`. The launcher starts the secured API on
`127.0.0.1:8080` and the dashboard on `127.0.0.1:9091`; set `MELDBASE_BIN` if
the `meld` executable is not on `PATH`.

When the dashboard asks for its token, source the generated private config in
the same shell and copy the value it prints:

```sh
set -a
. ./meldbase-local/config/meldbase.env
set +a
printf '%s\n' "$MELDBASE_ADMIN_TOKEN"
```

## 2. Prepare the host and data directory

Keep the database, backup artifacts and rehearsal evidence on paths that the
service account can read and write. Backup artifacts must ultimately be copied
to a distinct failure domain; a backup on the same disk does not protect that
disk.

Before trusting a filesystem, run the built binary's non-destructive durability
check against the real data directory. It creates and removes an isolated probe
directory, checks locking, fsync and no-overwrite storage publication, and emits a
receipt.

```sh
install -d -m 0750 /srv/meldbase/data /srv/meldbase/backups /srv/meldbase/rehearsals
meld durability-check \
  --dir /srv/meldbase/data \
  --out /srv/meldbase/durability-receipt.json
```

Keep that receipt with the deployed build and rerun the check after moving to a
new filesystem or mount configuration. The database is a single COW file;
do not copy or overwrite it while the process is running.

## 3. Start locally and observe it

`meld serve` verifies JWTs itself and applies workspace isolation to every
configured collection. Use either a locally managed HS256 secret or an HTTPS
JWKS endpoint. This template uses HS256; keep the public listener on loopback
until an application-owned TLS proxy is in place.

For a Linux host using systemd, the repository includes a conservative local
service template in [`deploy/single-node/systemd`](https://github.com/crapthings/meldbase/tree/main/deploy/single-node/systemd).
Its launcher forces both listeners to loopback, requires a real admin token and
JWT/collection-policy settings, runs as an unprivileged `meldbase` user and
permits writes only under `/var/lib/meldbase`. It is intentionally not a
public-network TLS termination recipe.

```sh
sudo useradd --system --user-group --home-dir /var/lib/meldbase --shell /usr/sbin/nologin meldbase
sudo install -d -o meldbase -g meldbase -m 0750 /var/lib/meldbase/data
sudo install -d -o root -g meldbase -m 0750 /etc/meldbase
sudo install -d -o root -g root -m 0755 /usr/local/libexec/meldbase
sudo install -m 0755 deploy/single-node/systemd/meldbase-single-node /usr/local/libexec/meldbase/meldbase-single-node
sudo install -m 0640 deploy/single-node/systemd/meldbase.env.example /etc/meldbase/meldbase.env
sudo install -o root -g meldbase -m 0640 deploy/single-node/systemd/access-policy.json.example /etc/meldbase/access-policy.json
sudo install -m 0644 deploy/single-node/systemd/meldbase.service /etc/systemd/system/meldbase.service
sudoedit /etc/meldbase/meldbase.env
sudo systemctl daemon-reload
sudo systemctl enable --now meldbase
```

Before enabling the service, replace `MELDBASE_ADMIN_TOKEN`, write at least 32
random bytes to `/etc/meldbase/jwt-hs256.secret`, and edit/validate
`/etc/meldbase/access-policy.json`. The launcher passes that strict manifest to
`meld serve`. Keep the primary listener at `127.0.0.1:8080` and access it
through an application-owned TLS boundary. The JWT must contain `sub`, `exp`,
the configured `iss` and `aud`, plus `workspace_id`. Check startup with
`systemctl status meldbase` and `journalctl -u meldbase`.

For a local process with the embedded dashboard, provide the same JWT settings
and bind the dashboard only to loopback:

```sh
export MELDBASE_ADMIN_TOKEN='replace-with-at-least-32-random-bytes'
meld serve \
  --db /srv/meldbase/data/app.meld \
  --addr 127.0.0.1:8080 \
  --jwt-hs256-secret-file /etc/meldbase/jwt-hs256.secret \
  --jwt-issuer https://identity.example/ \
  --jwt-audience meldbase-api \
  --access-policy-file /etc/meldbase/access-policy.json \
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

### Browser application behind a TLS proxy

Keep Meldbase itself on loopback, then configure the public browser boundary in
the generated bundle's `config/meldbase.env` or the systemd
`/etc/meldbase/meldbase.env`. The HTTP list is exact; the WebSocket list can
also pin the scheme. For one application origin:

```sh
MELDBASE_PUBLIC_REALTIME_URL=wss://api.example.com/v1/realtime
MELDBASE_HTTP_ORIGINS=https://app.example.com
MELDBASE_REALTIME_ORIGIN_PATTERNS=https://app.example.com
```

Restart the service after editing. The application proxy must forward HTTP
upgrade requests to `127.0.0.1:8080`; it must not expose the dashboard listener
at `127.0.0.1:9091`. The ticket endpoint is governed by the exact HTTP origin
list, while the WebSocket handshake is governed by the realtime pattern list.
Neither replaces JWT authentication or collection/RPC authorization.

`/livez` says the Go handler is responsive. `/readyz` (and `/health`) also
requires the database to be readable and writable, and returns HTTP 503 after
a fail-stop durability error.

## 4. Prove recovery before production

Before serving production traffic—and before every upgrade—follow the
[backup and upgrade runbook](operations/backup-and-upgrade). It is the sole
authority for backup commands, retention, recovery rehearsal, and rollback.

The non-negotiable boundaries are: stop the database before a physical backup;
keep the artifact and receipt together in another failure domain; restore only
to a new, absent path; and run application smoke checks against the restore.
Never overwrite a database or restore target. A physical backup preserves the
source identity and history; it is not an independently writable clone.
