# Meldbase documentation

## Start here

- [Project overview and SDK quick start](../README.md)
- [Single-node deployment and recovery](single-node-deployment.md)
- [Current alpha capability audit](mvp-audit.md)
- [Roadmap](roadmap.md)

## Product and storage contracts

- [Architecture](architecture.md)
- [Storage format](storage-format.md)
- [Query contract](query.md)
- [Compound indexes](compound-indexes.md)
- [Reactive queries and durable changes](reactive.md)
- [Client HTTP/WebSocket protocol](client-protocol.md)
- [RPC idempotency](rpc-idempotency.md)
- [Server JavaScript worker SDK](server-js-sdk.md)

## Operations

- [Observability and embedded dashboard](observability.md)
- [Release process](releasing.md)
- [Filesystem qualification](filesystem-qualification.md)
- [Rollback-anchor service](rollback-anchor-service.md)
- [Rollback-anchor formal model](rollback-anchor-formal-model.md)

## Advanced replication and control-plane material

- [Core runtime and commit coordination](core-runtime.md)
- [Commit coordinator](commit-coordinator.md)
- [Primary write fence](primary-lease.md)
- [Replication protocol](replication-protocol.md)
- [Redfish power adapter](redfish-power-adapter.md)

The current contract is described by the storage, protocol and deployment guides
above. Historical implementation routes are not compatibility commitments.
