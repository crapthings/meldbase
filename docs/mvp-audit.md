# Current alpha capability audit

This is the current-state audit for Meldbase, not a migration history. The
database has one durable copy-on-write storage format (currently revision 3).
There is no legacy engine, WAL replay mode, runtime format selection, or
compatibility fallback. A non-empty file that is not the current format fails
closed. Alpha does not currently make a cross-version compatibility promise;
future breaking changes will receive release-specific guidance when planned.

## Available now

| Area | Current contract | Evidence |
| --- | --- | --- |
| Durable storage | One checksummed COW file with dual Meta pages, catalog roots, Commit Log retention, reopen recovery and offline verification. | `core`, `internal/storage`, `cmd/meld verify` |
| Documents and indexes | Closed document values, CRUD, bounded filters and updates, ordered compound/unique indexes, explain plans and persistent index builds. | `core/*_test.go`, `docs/query.md`, `docs/compound-indexes.md` |
| Realtime | Ordered live queries, bounded queues, authenticated HTTP tickets, WebSocket snapshots/deltas and safe resumptions. | `core/reactive*`, `internal/server` |
| Tenant and direct-API isolation | HS256 or JWKS JWT verification maps `sub` to actor ID and `workspace_id` to actor tenant ID. A strict, versioned collection-access manifest provides collaborative, owner-only, or RPC-only surfaces; optional field limits only narrow query paths, results, inserts, and updates. The server owns workspace/owner fields, intersects reads, writes, and subscriptions, and never lets a client select a trusted tenant. | `internal/server/auth_*.go`, `internal/server/workspace_authorizer*.go`, `cmd/meld/access_policy.go` |
| SDKs | Go API plus TypeScript client, React adapter and server-worker SDK share typed request/query contracts. | `sdk/client`, `sdk/react`, `sdk/server` |
| Operations | Health probes, authenticated admin dashboard and metrics, diagnostics, inspect, verify, backup, restore and a single-node backup/restore drill. | `admin`, `cmd/meld`, `docs/single-node-deployment.md` |
| Resource limits | Bounded request/query/result, transaction, index-build, retention, page-cache and physical-file usage with aggregate telemetry. | `core/resource_limits.go`, `docs/observability.md` |

## Deliberate boundaries

- A tenant is an application identity boundary, not a second database file.
  The server derives it from a verified token; clients never provide a trusted
  tenant selector.
- Built-in collection access is intentionally a small generic-data policy, not
  an account, role, or membership system. Dynamic read visibility can narrow it;
  role-dependent writes remain in an application `Authorizer` or explicit RPC.
- The embedded dashboard is an operator surface. It does not own business users,
  schedule backups or expose public administration endpoints.
- Physical backup preserves database identity and is a recovery artifact, not an
  independently writable clone.
- The protocol has its own versioned frames. That protocol version is distinct
  from the storage format revision.

## Not claimed yet

Meldbase does not claim production qualification for every filesystem or power
loss environment, automatic HA/failover, sharding, distributed transactions,
offline conflict resolution, complex aggregation, or specialized search/vector
indexes. The remaining evidence work is tracked in `docs/roadmap.md`.

## Alpha change policy

Before the first production-compatible release, source structure and on-disk
contracts may change directly when that makes the current design simpler or
safer. We do not retain a dormant implementation merely to preserve an internal
fallback path. Each such change must update the format, deployment and recovery
documentation together.
