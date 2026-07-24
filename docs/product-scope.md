# Product scope and boundaries

Meldbase is an experimental, application-owned document database. Its primary
form is an embedded Go database with typed documents, indexes, query results
that can stay live, and one durable local file. The optional HTTP/WebSocket
server is a narrow application boundary for browser or service clients; it is
not a separately operated, hosted database product.

## Modes and intended use

| Mode | Intended use | Persistence and operational boundary |
| --- | --- | --- |
| Memory | Tests, ephemeral local state, and short-lived application data. | Process-local only. It has no physical backup, query-history replay, or durable online-index-build support. |
| Durable | A single application-owned `.meld` file on one host. | Copy-on-write storage, offline verification, physical backup/restore, bounded replay history, and durable online index builds. Exactly one writable process may own a path. |
| Remote | A TypeScript client talks to an application-owned Meldbase server. | The server uses the same data/query contract but adds JWT identity, workspace policy, HTTP and realtime boundaries. It does not turn the file into a shared distributed writer. |
| Realtime | A query is kept live in the embedding process or through a remote ticket-authenticated WebSocket. | The core emits ordered snapshots or deltas; the remote boundary adds identity-bound tickets, resume tokens, and resync rules. It is not an offline replica, cache, or conflict-resolution protocol. |

Choose memory when losing the data on process exit is acceptable. Choose durable
when the application owns one host, one writable file, backups, and recovery
operations. Choose remote only when a browser or separate process needs the
application's controlled API; it is not required for embedded Go use.

## Workspace isolation, not a hosted tenancy claim

The server can enforce a workspace field derived from an authenticated actor
and a reviewed collection-access manifest. This is the product's concrete
isolation mechanism. It is often described as *tenant isolation* in an
application architecture, but the protocol and policy use the more precise
term **workspace**: clients cannot choose a trusted workspace value, and field,
filter, sort, aggregate, mutation, and realtime access are separately checked.

This does **not** make Meldbase a multi-tenant managed service. It has no
built-in organization provisioning, billing, per-workspace storage or query
quotas, noisy-neighbour scheduling, cross-customer operational plane, or
hosted key lifecycle. Applications that need those properties must supply them
around the server boundary or choose a managed database service.

## Scale and availability boundary

The supported production shape is one durable writer on one host, with an
application-owned proxy/TLS boundary, external identity provider, backups, and
restore drills. The project does not currently promise automatic failover,
multi-writer replication, sharding, cross-region replication, horizontal query
scaling, or an availability SLA. A physical backup is a restore artifact, not
an active replica or a writable branch.

Capacity numbers are workload- and host-specific. Use the supplied query and
runtime capacity commands on the actual filesystem and hardware before setting
admission limits; do not infer an SLA from repository benchmarks. See
[production qualification](/production-qualification).

## Compatibility and deliberately unsupported features

Meldbase borrows familiar document-query concepts, but it is not MongoDB wire
compatible and does not promise MongoDB driver compatibility, BSON storage, or
drop-in migration. Use its documented typed document, query, update, HTTP, and
realtime contracts directly. Logical export/import can help a controlled data
move, but it is not a general MongoDB migration utility.

The following are deliberate current boundaries rather than implicit promises:

- No hosted service, automatic HA/failover, sharding, or cross-region topology.
- No built-in end-user identity, per-token immediate JWT revocation, or managed
  tenancy/key-lifecycle administration. HS256 supports a bounded old/new key
  overlap for rotation, as described in [security](/production-security).
- No transparent cross-format storage upgrade promise; follow the documented
  backup, qualification, and rollback procedure for an alpha release.
- No index rebuild in memory mode. Durable online index rebuilds keep the old
  same-named index query-visible and atomically replace it only after the new
  shadow tree catches up; see [troubleshooting](/troubleshooting).
- No MongoDB wire protocol or unrestricted aggregation engine.

These constraints keep the operational model small and reviewable. Re-evaluate
them explicitly before treating Meldbase as a distributed or managed platform.
