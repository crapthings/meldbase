# Meldbase roadmap and production-readiness criteria

This checklist is an evidence index, not a product-support claim. Checked items
have implementation, automated-test, or operational-documentation evidence in
this repository. Unchecked items need further implementation, target-environment
evidence, or an explicit product-scope decision.

Meldbase should establish a workload-specific single-node capacity boundary
before considering high availability, horizontal scaling, or managed-service
features. See [Product scope](docs/product-scope.md) and
[production qualification](docs/production-qualification.md) for the supported
boundary and the evidence process.

For the detailed query and update operator sequence, see
[QUERY-AND-UPDATE-ROADMAP.md](QUERY-AND-UPDATE-ROADMAP.md).

## Correctness and resilience

- [x] Long-running concurrent read/write tests.
- [x] Stress coverage for queries, writes, and realtime subscriptions together.
- [x] Crash and abnormal-shutdown recovery tests.
- [x] Equivalent-result coverage for memory and durable backends.
- [x] Index and query correctness after substantial update workloads.
- [x] Index creation, rebuild, failure, and recovery tests.
- [x] Replay, resume, and query-delta coverage without lost or duplicate events.
- [x] Randomized model tests against an independent reference implementation.
- [x] Fault injection for process termination, disk errors, interrupted writes,
  cancellation, and timeouts.
- [x] Defined fail-stop behavior for unrecoverable database errors.

## Backup, restore, and data evolution

- [x] Scheduled backup policy.
- [x] Lower-cost backup option.
- [x] Backup-integrity verification.
- [x] Restore drills in an independent environment.
- [x] Point-in-time recovery capability, or an explicit statement that it is not
  supported.
- [x] Post-restore consistency for indexes, queries, realtime views, and replay.
- [x] Data export and import tools.
- [x] Storage-format upgrade and older-data migration guidance.
- [x] Defined RPO and RTO expectations.
- [x] Scheduled restore drills rather than backup-file existence checks alone.

## Deployment and operations

- [x] Clear single-node deployment documentation.
- [x] Container image or standard binary delivery.
- [x] Consistent configuration-file and environment-variable contract.
- [x] Log output, levels, and host-managed rotation guidance.
- [x] Disk-exhaustion behavior and alerting guidance.
- [x] File permissions, locking, and multi-process misuse handling.
- [x] Health endpoints.
- [x] Readiness and liveness probes.
- [x] Graceful shutdown and in-flight request handling.
- [x] Upgrade procedure.
- [x] Rollback procedure.
- [x] Resource limits and capacity-configuration guidance.
- [x] Troubleshooting guide.
- [x] Production operations runbook.

## Performance and capacity boundaries

- [ ] Measure read/write throughput across data sizes.
- [ ] Measure query P50, P95, and P99 latency.
- [ ] Measure concurrent read and write limits.
- [ ] Measure realtime subscriber limits.
- [x] Document suitable uses for memory and durable modes separately.
- [ ] Measure behavior for large documents, deep nesting, and large arrays.
- [ ] Measure index count, index size, and write-maintenance cost.
- [ ] Measure behavior before and after query-budget enforcement.
- [ ] Measure resource effects across multiple workspaces or collections.
- [ ] Publish a capacity guide covering document count, concurrency,
  subscriptions, memory, disk, and latency.
- [x] Provide repeatable observer and benchmark commands for key workloads.

## Security and workspace isolation

- [x] Authentication-key rotation.
- [x] Documented token-expiry and revocation boundary.
- [x] TLS deployment guidance.
- [x] Workspace-isolation tests.
- [x] Query-field authorization tests.
- [x] Filter-operator authorization tests.
- [x] Independent authorization for sorting, aggregation, and result fields.
- [x] Administrator-operation audit events.
- [x] Audit events for authentication and authorization rejection.
- [x] Request rate limiting.
- [x] Request and response size limits.
- [x] Malicious-query and resource-exhaustion stress coverage.
- [x] Sensitive fields excluded from logs, metrics, Explain, and diagnostics.
- [x] External errors do not disclose internal paths, credentials, or business
  data.

## API and protocol lifecycle

- [x] Defined protocol-version policy.
- [x] Wire backward-compatibility tests.
- [x] SDK semantic-versioning policy.
- [x] Stable and experimental markings for Go and TypeScript APIs.
- [x] Stable error-code and error-class documentation.
- [x] Data-format migration policy.
- [x] API deprecation process.
- [x] Release changelog.
- [x] Explicit upgrade compatibility statement.
- [x] Consistent behavior guidance for local, remote, durable, and realtime
  modes.

## Developer experience

- [x] Five-minute introduction.
- [x] Complete CRUD example.
- [x] Realtime-subscription example.
- [x] Query, sorting, and pagination guide.
- [x] Index-design guide.
- [x] Update and delete selection-criteria guide.
- [x] Memory, durable, and remote mode guide.
- [x] Go, Node, TypeScript, and React examples.
- [x] Production-deployment example.
- [x] Backup and restore guide.
- [x] Performance-tuning guidance.
- [x] Common-error troubleshooting.
- [x] Migration boundary for MongoDB and other document databases.

## Tooling and integrations

- [x] Stable CLI contract.
- [x] Container image or standard runtime package.
- [x] Administrative API.
- [x] Prometheus metrics.
- [x] OpenTelemetry integration.
- [x] Structured logging integration.
- [x] Data export and import tools.
- [x] Test-database and temporary-database approach.
- [x] CI startup, test, and data-cleanup practices.
- [x] Common authentication-system integration guidance.
- [x] Webhook or event-consumption guidance.

## Product boundaries

- [x] Define the primary form: embedded database with an optional realtime
  server boundary.
- [x] Define memory and durable mode boundaries.
- [x] Define the workspace-isolation boundary without claiming hosted tenancy.
- [x] State the horizontal-scaling boundary.
- [x] State the managed-service boundary.
- [x] State that MongoDB concepts are familiar vocabulary, not wire or driver
  compatibility.
- [x] Document unsupported capabilities and their alternatives.

## Release criteria

### Internal use

- [x] Core query and update semantics are stable.
- [x] Durable mode recovers.
- [x] Primary SDKs can integrate.
- [x] Realtime subscriptions are stable.
- [x] Basic backup exists.
- [x] Basic monitoring exists.

### Small-scale production consideration

- [ ] Complete target-environment concurrency and fault evidence.
- [ ] Complete backup and restore drills for the intended deployment.
- [ ] Establish performance and capacity boundaries.
- [ ] Complete security auditing and workspace-isolation validation.
- [ ] Establish upgrade and rollback evidence.
- [ ] Establish stable error-code and API-version policy evidence.
- [ ] Provide deployment and troubleshooting evidence for the intended
  environment.

### Large-scale production consideration

- [ ] High availability and failover.
- [ ] Horizontal scaling.
- [ ] Node-failure recovery.
- [ ] Sharding or partitioning strategy.
- [ ] Cross-region replication.
- [ ] More complete rate limiting and workspace resource isolation.
- [ ] SLA, support, and operating model.

## Next evidence priorities

- [ ] Crash recovery, backup, and restore drills on the intended storage.
- [ ] Concurrency, capacity, and fault injection on target hardware.
- [ ] Deployment, upgrade, and rollback evidence for the intended environment.
- [ ] Security audit and workspace-isolation validation.
- [ ] API, protocol, and error-code lifecycle evidence.
- [ ] Developer documentation and real-application examples.
- [ ] Re-evaluate high availability, horizontal scaling, and managed service
  only after the single-node boundary is evidenced.
