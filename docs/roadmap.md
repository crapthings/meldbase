# Roadmap

The [current capability audit](mvp-audit) is the source of truth for supported
alpha behavior and known limits. This page records only future work and the
evidence required before expanding that claim.

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
