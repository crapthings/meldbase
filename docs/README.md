# Meldbase documentation map

Each page below has one job. Link to the page that owns a detail rather than
copying it into a guide, reference, or design note.

## Start here

- [Project overview and SDK quick start](https://github.com/crapthings/meldbase#meldbase)
- [Live todo application example](guide/realtime-todos.md)
- [Identity, users, and workspace isolation](guide/identity-and-workspaces.md)
- [Collection access policies](guide/access-policies.md)
- [Indexing and query plans](guide/indexing-and-query-plans.md)
- [Evaluate Meldbase safely](alpha-evaluation.md)
- [Single-node deployment and recovery](single-node-deployment.md)
- [Backup, recovery, and upgrade runbook](operations/backup-and-upgrade.md)
- [Current alpha capability audit](mvp-audit.md)
- [Roadmap](roadmap.md)

## Product, SDK, and protocol contracts

- [Terminology and semantic boundaries](terminology.md)
- [Architecture](architecture.md)
- [Storage format](storage-format.md)
- [Query contract](query.md)
- [Compound indexes](compound-indexes.md)
- [Reactive queries and durable changes](reactive.md)
- [Client SDK API guide](reference/client-sdk.md)
- [Client HTTP/WebSocket protocol](client-protocol.md)
- [RPC idempotency](rpc-idempotency.md)
- [Server worker SDK guide and examples](guide/server-worker-sdk.md)
- [Server worker protocol](server-js-sdk.md)

## Operations and qualification

- [Observability and embedded dashboard](observability.md)
- [Release process](releasing.md)
- [Filesystem qualification](filesystem-qualification.md)
- [Rollback-anchor service](rollback-anchor-service.md)
- [Rollback-anchor formal model](rollback-anchor-formal-model.md)

## Engineering design and advanced controls

- [Core runtime evolution](core-runtime.md)
- [Commit coordinator](commit-coordinator.md)
- [Primary write fence](primary-lease.md)
- [Replication protocol](replication-protocol.md)
- [Redfish power adapter](redfish-power-adapter.md)

## Maintaining this documentation

| Page type | Owns | Must link instead of repeat |
| --- | --- | --- |
| Guide | A task-oriented path and working examples. | Wire frames, exhaustive API signatures, and operational runbooks. |
| Reference | Public API, CLI, HTTP, and protocol contracts. | A second tutorial or an implementation history. |
| Operations | Repeatable deployment, recovery, and release procedures. | Application design or core storage internals. |
| Design and qualification | Invariants, implementation decisions, and retained evidence. | A user-facing quick-start claim. |

Before changing a public term, check [terminology](terminology.md). Before
adding a page, identify its single owner in this map and replace or link any
existing overlapping material. Historical implementation routes are not
compatibility commitments.
