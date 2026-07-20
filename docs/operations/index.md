# Operations

Meldbase is a single-node database first. The embedded dashboard is an operator
surface; it is not a user administration product, a public control plane, or a
backup scheduler.

## Start here

- [Single-node deployment and recovery](../single-node-deployment) — directory
  layout, JWT configuration, probes, dashboard, backup rehearsal, and upgrade.
- [Backup and upgrade runbook](./backup-and-upgrade) — routine backup sets,
  retention, restore rehearsal, upgrade, and rollback boundaries.
- [Observability](../observability) — health signals, diagnostics, Prometheus,
  OpenTelemetry, and runtime profiling.
- [Filesystem qualification](../filesystem-qualification) — what the
  non-destructive probe proves, and what it cannot prove.
- [Release process](../releasing) — verification and release evidence.

## Operational boundary

Keep the API and dashboard on loopback until an application-owned TLS boundary
is configured. Store recovery artifacts in another failure domain. Do not
overwrite database files or restore targets: Meldbase treats no-overwrite
publication as a safety property, not a convenience feature.
