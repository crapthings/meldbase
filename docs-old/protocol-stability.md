# Protocol and SDK stability

## Current status

Meldbase is an alpha release. The current durable storage format is revision 3;
older alpha files are not supported and there is no cross-version storage
compatibility promise. Treat an upgrade as a backup-and-restore change as
described in [Single-node deployment](/production-single-node#upgrade-and-rollback).

The HTTP, realtime, RPC, worker, and SDK contracts use protocol version 1. The
version field is mandatory in every supported envelope. Version 1 grammar is
immutable: compatible additions must be optional and safely ignored by existing
readers; an incompatible grammar change requires a new protocol version and an
explicit capability negotiation path. The server advertises its realtime/RPC
capabilities through the ticket capability media type and exposes the matching
worker capability descriptor on the worker control channel.

The automated protocol contract verifies canonical wire shapes, the version
registry, and the fixed engine error-code registry. Do not change a published
v1 envelope, capability name, or fixed code without adding executable
compatibility evidence.

The compatibility suite also sends the original minimal v1 realtime
`authenticate`, snapshot-mode `subscribe`, and `ping` JSON frames as raw bytes
to the current server. Capability discovery is opt-in: legacy ticket responses
and clients that omit `mode` continue to receive snapshot semantics.

## Errors

Public HTTP and realtime/RPC errors use stable, machine-readable codes such as
`unauthenticated`, `forbidden`, `invalid_query`, `request_too_large`,
`resource_limit_exceeded`, `database_unavailable`, and `outcome_unknown`.
These codes intentionally omit internal error text, local paths, secrets, and
document contents. Application-owned RPC errors must use a separately
namespaced application code; they are not additions to the Meldbase engine
registry.

Clients should branch on error code and documented outcome semantics, never on
the human text of a Go error or an HTTP status alone. A new fixed engine code is
an API-contract change and needs release notes and compatibility tests.

## SDKs

`@meldbase/client`, `@meldbase/worker`, and `@meldbase/react` are currently
alpha packages. Their package versions move together when their shared wire
contract changes. A planned beta release uses `x.y.z-beta.N`, publishes through
the protected provenance workflow, and requires the checklist in
[SDK beta readiness](/sdk-beta-checklist) and the release procedure in
[Publishing an SDK beta](/sdk-beta-release).

Until a stable SDK release explicitly labels an API as stable, public Go and
TypeScript APIs should be treated as alpha: source-compatible changes can still
change behavior, and storage migration support is not implied. The supported
imports, local/remote behavior, and realtime lifecycle are documented in the
[SDK guide](/sdk).

## Release rule

For any public contract change, update the protocol/SDK documentation,
compatibility tests, and release notes together. Preserve the previous version's
decoder and fixtures for as long as it is advertised. If a release cannot read
an older durable format, state that plainly in the upgrade instructions and
require a verified export/physical backup and restore plan rather than implying
an in-place migration.
