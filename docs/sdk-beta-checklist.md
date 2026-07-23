# SDK beta readiness checklist

Workspace SDK version: `0.1.0-alpha.12`

This page is the release-facing status record for `@meldbase/client`,
`@meldbase/worker`, and `@meldbase/react`. Keep the version above synchronized
with all three package manifests. `pnpm release:sdk:checklist` verifies that
link before packaging or publishing.

## Current package status

| Package            | Workspace version | Beta readiness evidence                                                             |
| ------------------ | ----------------- | ----------------------------------------------------------------------------------- |
| `@meldbase/client` | `0.1.0-alpha.12`  | typed writes, strict wire boundaries, NodeNext/Bundler tarball consumers            |
| `@meldbase/worker` | `0.1.0-alpha.12`  | strict worker protocol framing, transaction regressions, Worker Hub end-to-end test |
| `@meldbase/react`  | `0.1.0-alpha.12`  | live-query hydration test and packed React consumer                                 |

The workspace is still an alpha prerelease. Before a beta publication, update
all three package versions and this page to the exact `x.y.z-beta.N` version;
do not claim beta publication from an untagged source checkout.

## Required release checks

1. Update the three package versions together and update the version and table
   on this page.
2. Run `pnpm quality:sdk`: Prettier, ESLint, package coverage thresholds, and
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
