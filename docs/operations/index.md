# Operations

Meldbase is a single-node database first. The embedded dashboard is an operator
surface; it is not a user administration product, a public control plane, or a
backup scheduler.

## Operate a single node

- [Single-node deployment and recovery](../single-node-deployment) — directory
  layout, JWT configuration, probes, dashboard, and the recovery boundary.
- [Backup and upgrade runbook](./backup-and-upgrade) — routine backup sets,
  retention, restore rehearsal, upgrade, and rollback boundaries.
- [Observability](../observability) — health signals, diagnostics, Prometheus,
  OpenTelemetry, and runtime profiling.
- [Release process](../releasing) — verification and release evidence.

## Qualification and advanced controls

These pages are for operators qualifying a particular deployment or engineers
building advanced topology controls; they are not prerequisites for a normal
single-node installation.

- [Filesystem qualification](../filesystem-qualification) — what the
  non-destructive probe proves, and what it cannot prove.
- [Rollback-anchor service](../rollback-anchor-service) — retained external
  evidence against rollback.
- [Primary write fence](../primary-lease) — evidence required before a primary
  can publish writes.
- [Replication protocol](../replication-protocol) — trusted server-to-server
  bootstrap and commit-tail delivery.

## Operational boundary

Keep the API and dashboard on loopback until an application-owned TLS boundary
is configured. Store recovery artifacts in another failure domain. Do not
overwrite database files or restore targets: Meldbase treats no-overwrite
storage publication as a safety property, not a convenience feature.
