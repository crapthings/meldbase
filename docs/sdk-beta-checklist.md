# SDK beta readiness checklist

Workspace SDK version: `0.1.0-beta.2`

This page is the release-facing status record for `@meldbase/client`,
`@meldbase/worker`, and `@meldbase/react`. Keep the version above synchronized
with all three package manifests. `pnpm release:sdk:checklist` verifies that
link before packaging or publishing.

## Current package status

| Package            | Workspace version | Beta readiness evidence                                                             |
| ------------------ | ----------------- | ----------------------------------------------------------------------------------- |
| `@meldbase/client` | `0.1.0-beta.2`    | typed writes, strict wire boundaries, NodeNext/Bundler tarball consumers            |
| `@meldbase/worker` | `0.1.0-beta.2`    | strict worker protocol framing, transaction regressions, Worker Hub end-to-end test |
| `@meldbase/react`  | `0.1.0-beta.2`    | live-query hydration test and packed React consumer                                 |

All three packages are published as `0.1.0-beta.2` on npm through the verified GitHub OIDC trusted publisher. The database remains
alpha. For subsequent versions, verify the registry and the successful release
workflow before announcing availability.

## Required release checks

1. Update the three package versions together and update the version and table
   on this page.
2. Run `pnpm quality:sdk`: the public API baseline, Prettier, ESLint, package coverage thresholds, and
   the release-readiness policy must pass.
3. Run `pnpm pack:check` to build from fresh source, install the tarballs using
   npm, and check NodeNext and Bundler TypeScript consumers.
4. Confirm the SDK compatibility workflow passed on Node 20, 22, and 24, and
   that its Node 24 job completed the real-browser HTTP, realtime, and React
   hydration test.
5. Confirm the documentation workflow built this guide and the generated API
   reference from the same commit.
6. Commit the version, checklist, documentation, and implementation changes;
   only then run the manual **SDK beta release** GitHub Actions workflow.

## Release policy

Published SDK packages contain JavaScript and declaration files only. Source
maps and declaration maps are deliberately disabled in every SDK TypeScript
configuration, and tarball verification rejects map files. This keeps the
published artifact surface deterministic; source is available from the tagged
repository revision.

Use `pnpm release:sdk:beta` only from a clean, committed checkout with a
`x.y.z-beta.N` version. It rechecks this checklist and package contents before
publishing public npm tarballs with the `beta` tag and provenance.

## Reviewed public API

The five application entry points are reviewed in `sdk/api/`.
Run `pnpm api:sdk:check`; intentional updates require `pnpm api:sdk:update`,
a report review, appropriate tests, release notes, and a new package version.
Experimental `@meldbase/client/internal` is excluded.
