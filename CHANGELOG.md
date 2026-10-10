# Changelog

All notable public-contract changes are recorded here. Meldbase remains alpha;
entries under **Unreleased** are not a published compatibility commitment until
they are assigned to a release.

## Unreleased

### Added

- Scheduled verified physical backups with optional automatic restore drills.
- Operator JSONL audit events for admin requests and data API security
  rejections, plus bounded request rate limiting.
- Query observer latency percentiles and reproducible capacity-matrix commands.
- Non-root OCI container build and single-node operational documentation.
- Bounded HS256 signing-key overlap for controlled credential rotation, and
  bounded background resumption of persistent online index builds in `meld serve`.
- Low-cardinality JSONL service events with a configurable lifecycle/backup log
  threshold, plus explicit journald capture in the supplied systemd unit.
- Durable online indexes can now be rebuilt under the same name: the old index
  stays query-visible until the caught-up shadow index is atomically published.
- `meld backup --compression gzip` creates a receipt-bound, verified compressed
  physical backup artifact; `meld restore` verifies its compressed digest before
  decompression and normal physical restore validation.
- `meld serve --resource-limits-file` applies a strict, reviewable JSON budget
  configuration at database open; the supplied systemd launcher exposes it.
- Capacity-evidence bundles can bind a resource-limit file digest to the
  observed query and runtime measurements.
- The developer documentation now defines isolated temporary-database and
  service-lifecycle testing rules and maps them to the CI matrix.
- Durable database change-feed guidance now documents application-owned
  webhook/outbox delivery, acknowledgement, replay, and resynchronization.
- The single-node systemd launcher now supports either local HS256 secrets or
  an HTTPS RS256 OIDC/JWKS identity provider, with fail-closed mutual exclusion.
- The authenticated, read-only embedded admin API contract is now documented,
  including stats, stream, diagnostics, index catalog, dashboard, and metrics.

### Changed

- Realtime protocol v1 compatibility is covered by a frozen contract and raw
  legacy-frame acceptance test.
- Physical restore validation now covers indexes, query replay, and realtime
  deltas.
- Public Go and TypeScript entry points now carry explicit Alpha, Beta, or
  experimental lifecycle markers.

### Compatibility

- No storage-format compatibility guarantee is made across alpha releases; see
  [storage-format migration](docs/storage-format-migration.md).
- Protocol v1 envelope grammar and fixed engine error codes remain immutable;
  incompatible changes require a new version and migration notes.

## 0.1.0-alpha.14

### Changed

- Added plain-English summaries to top-level source declarations.
- Split query execution, document validation, and storage codecs into focused files and helpers.
- Updated SDK release checks to use the current guides and archived release metadata after the documentation migration.
- Aligned the contributor pnpm version with the workspace and CI.

### Compatibility

- This refactor does not change the storage format or protocol version.
