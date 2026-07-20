# Roadmap

## Current foundation

Meldbase already has one current durable COW format, typed Go/TypeScript/React
clients, authenticated HTTP and realtime transport, JWT workspace isolation,
backup/restore tooling and an embedded operator dashboard. The format and
deployment model are intentionally allowed to evolve during alpha; there is no
second runtime path to carry forward.

## Next: a dependable single-node package

- [x] Make the JWT-isolated single-node deployment the documented default,
  including systemd, secret-file permissions, health probes and reverse-proxy
  boundaries.
- [x] Publish an operator runbook for backup retention, restore rehearsal,
  upgrades and rollback-protection checks.
- [x] Add a concise documentation index so quick start, storage contract,
  transport contract and operational guides each have one authoritative home.

## Evidence before a production claim

- [ ] Retain multi-hour concurrent-write/read/reopen/reclamation soak evidence
  for each supported filesystem and release build.
- [ ] Continue real-volume filesystem and power-loss qualification with retained
  receipts; do not generalize one host's result to every platform.
- [ ] Keep backup/restore drills and format verification in the release gate.

## Later: fenced high availability

The current replication and primary-write-fence primitives are building blocks,
not automatic failover. A future HA offering needs an independent controller
membership/election and client-routing service, plus retained evidence for
controller loss, stale-primary isolation, follower lag, restore and
operator-led promotion.

## Explicitly deferred

MongoDB wire/BSON compatibility, sharding, distributed transactions, offline
conflict resolution, geospatial/full-text/vector indexes, TTL, complex
aggregation and a built-in end-user identity product are outside the current
roadmap.

An application-owned identity service and the private local playground are also
outside this repository's product scope. Meldbase documents their integration
boundary but does not publish a reference user service or demo data plane.

## Historical material

Older milestone-by-milestone implementation notes are intentionally not used as
the current plan. The current capability boundary is `docs/mvp-audit.md`; the
storage contract is `docs/storage-format.md`.
