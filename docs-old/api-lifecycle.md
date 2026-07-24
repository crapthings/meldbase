# API lifecycle and deprecation

Meldbase is alpha, but public contracts still need an explicit lifecycle. This
page defines how Go, TypeScript, HTTP, realtime, worker, and CLI changes are
classified and released. The current release history is maintained in the
repository [changelog](https://github.com/crapthings/meldbase/blob/main/CHANGELOG.md).

## Lifecycle labels

| Label | Meaning | Change rule |
| --- | --- | --- |
| Stable v1 contract | Versioned realtime/worker envelopes, fixed engine error codes, and documented capability names. | Immutable within v1. Incompatible change requires a new version and executable compatibility coverage. |
| Beta SDK surface | The documented `@meldbase/client`, `@meldbase/react`, and `@meldbase/worker` package entry points. | Versioned semantically; breaking changes require a major version or a documented pre-1.0 release decision. |
| Alpha Go/database and server API | Public Go APIs and operational flags outside an explicitly versioned wire contract. | May change between alpha releases; release notes and migration instructions are required when operators or callers must act. |
| Experimental | APIs, capabilities, or tools explicitly named experimental in their documentation or source comments. | No compatibility promise; do not build irreversible production workflows around them without pinning an exact release. |

Generated API reference describes the documented SDK surface. The Go root and
`server` package comments explicitly say **Alpha**. The TypeScript public entry
points (`@meldbase/client`, `@meldbase/client/local`,
`@meldbase/client/types`, `@meldbase/react`, and `@meldbase/worker`) carry
`@beta` source markers; `@meldbase/client/internal` carries both `@internal`
and `@experimental`. Anything reached only through an implementation package,
an unexported symbol, or a test hook is not public API. Applications should
import the documented package entry points, not repository-internal paths.

## Deprecation process

1. Classify the affected surface and state the replacement or migration path.
2. For a stable or beta surface, add a deprecation notice in documentation and
   release notes before removal; retain an executable compatibility test for the
   supported transition period.
3. For protocol v1, do not repurpose a field, capability, error code, or frame.
   Add an optional field only when old readers safely ignore it; otherwise add a
   new protocol version and negotiate it explicitly.
4. For storage or operational changes, provide a backup, rollback, and
   migration procedure before the release is used in production.
5. Record the change, affected versions, deadline, and verification command in
   the changelog. Remove only after the declared support window ends.

An experimental API can be withdrawn, but the release notes must still say so
when it affects an operator workflow or published SDK package.

## Compatibility evidence required for a release

- Protocol contract and raw legacy-frame tests for a wire change.
- SDK type/package checks and consumer-tarball checks for published SDK changes.
- Backup/restore, verification, and migration evidence for durable format or
  operational changes.
- A changelog entry that names the external behavior, not just implementation
  files.

See [protocol stability](/protocol-stability), [SDK guide](/sdk), and
[storage-format migration](/storage-format-migration).
