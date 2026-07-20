# CLI and configuration reference

`meld` is the operational CLI for a local database file. It intentionally keeps
destructive or qualifying operations explicit; normal application traffic uses
the embedded Go API or the HTTP/WebSocket server.

## Everyday commands

| Command | Purpose |
| --- | --- |
| `meld init --dir <new-dir>` | Create a non-overwriting, JWT-secured local single-node bundle. |
| `meld access-policy validate|explain ...` | Validate or statically explain a strict collection access manifest. |
| `meld demo` | Exercise durable writes, indexing, reactive updates, reopen, and verification. |
| `meld serve --db <path> ...` | Run the HTTP/WebSocket server and optional loopback admin dashboard. |
| `meld inspect --db <path>` | Read compatibility and physical metadata without opening the database. |
| `meld verify --db <path>` | Perform a full offline verification. |
| `meld backup --db <path> --out <new-path>` | Create a physical recovery artifact and JSON receipt. |
| `meld restore --in <path> --receipt <path> --out <new-path>` | Verify and restore only to a new path. |
| `meld index-build <start|list|resume|abort>` | Manage durable resumable index builds offline. |

Use `meld <command> --help` for the complete flags. The
[single-node guide](../single-node-deployment) is the authoritative operational
sequence for deployment, backup, restore, and upgrades.

## Server authentication and workspace configuration

`meld serve` requires production authentication unless `--dev-no-auth` is
explicitly supplied for local development. Choose exactly one verifier:

- `--jwt-hs256-secret-file <private-file>` for a first-party HS256 issuer;
- `--jwt-jwks-url <https-url>` for RS256 OIDC/JWKS verification.

Both modes also require `--jwt-issuer`, `--jwt-audience`, and
`--access-policy-file`. The strict, versioned JSON manifest declares
collaborative, owner-only, or RPC-only collections; see
[collection access policies](../guide/access-policies).
The server derives the principal's active workspace from `workspace_id` by
default; use `--jwt-workspace-claim` only when your issuer uses another claim
name. The manifest's `workspaceField` remains server-owned for every configured
collection.

For a browser application behind TLS, configure both boundaries explicitly:
`--http-origins` is a comma-separated list of exact `http(s)` origins permitted
to call the API and obtain a realtime ticket; `--realtime-origin-patterns` is a
comma-separated list of WebSocket Origin patterns. A pattern may be a host
pattern such as `app.example` or a scheme+host pattern such as
`https://app.example`; use the latter when the scheme matters. The default is
limited to local development origins. `--public-realtime-url` supplies the
external `wss://…/v1/realtime` address returned in tickets when a TLS proxy
terminates in front of the loopback listener.

The optional embedded dashboard uses `MELDBASE_ADMIN_TOKEN`, with a minimum of
32 bytes, and must bind to a loopback `--admin-addr`. See
[observability](../observability) for dashboard and Prometheus details.

## Safety rules

- Backup, restore, verification, and index build commands use the normal file
  locking model; stop the writer before offline work.
- Backup and restore destinations must be absent. A restore never overwrites a
  database.
- `--dev-no-auth` grants unrestricted application access. Do not expose it on
  a public listener.
- Run `meld durability-check` against an actual target volume before treating a
  new filesystem or mount configuration as deployment-ready.
