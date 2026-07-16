# Release checklist

Meldbase releases are developer previews until the roadmap explicitly promotes
them beyond that status.

## One-time repository setup

- Verify `go.mod`, Go imports, and package metadata still use the canonical
  `github.com/crapthings/meldbase` repository path.
- Keep the Apache-2.0 `LICENSE` and package metadata in sync.
- Enable GitHub private vulnerability reporting.
- Protect `main` and require every normal CI job.
- Keep every remote GitHub Action pinned to its reviewed 40-character commit;
  retain the release tag only as an adjacent comment. The CI workflow rejects
  floating action references.
- Confirm the repository name and npm scope before publishing package metadata.

## Every release

1. Start from a clean worktree and locked dependencies.
2. Run the complete command gate from `CONTRIBUTING.md`.
3. Run the B+Tree fuzz target for at least 30 seconds:

   ```sh
   go test ./internal/index -run '^$' \
     -fuzz '^FuzzBTreeMatchesOrderedSetModel$' -fuzztime=30s
   ```

4. Review `docs/roadmap.md` and `docs/mvp-audit.md` so claims match the build.
5. If the admin `Sample` JSON graph changed, increment `admin.SchemaVersion`,
   generate a new `admin-schema-vN.json` fixture and retain every older fixture.
   Never change field names, wire types, optionality or nullability under an
   existing schema version.
6. If realtime/RPC frame grammar changed incompatibly, increment the Go and
   TypeScript protocol constants together, add a new shared protocol contract
   artifact and retain `protocol-v1-contract.json`. Capability-only additions
   must remain sorted, bounded and safe for older peers to ignore.
7. Run `govulncheck ./...` with the current official scanner and
   `pnpm audit --prod --audit-level high`. Archive or record the tool versions
   and date; a clean result is a point-in-time database lookup, not a permanent
   security guarantee.
8. Run `pnpm pack:check`. It creates the real tarballs, verifies an exact
   allowlist, rejects tests and undeclared imports, checks rewritten workspace
   dependency versions, and imports/type-checks the packages from a synthetic
   consumer. Review `pnpm pack --dry-run` in each SDK package as a final human
   size check before publication.
9. Trigger the storage soak workflow for the intended release revision with the
   `release` profile (minimum four hours, 10,000 documents and 12 reopens).
   Retain its schema-4 receipt and require matching clean source/build revisions,
   `raceEnabled`, at least four hours of `concurrentDurationNanos` independent
   of reopen/verification time, exact reopen completion,
   nonzero per-phase work for every concurrent worker, at least one real
   reclamation conflict, non-vacuous shadow-index semantic verification, final
   build absence, valid FreeSpace/published-index semantics, target-volume
   identity and a 64-character final SHA-256. A workflow definition,
   `sentinel`/`custom` receipt or earlier
   revision's receipt is not release evidence.
10. Tag experimental releases as monotonically increasing prereleases. The
   existing `v0.1.0-alpha.1` tag is immutable and must never be reused for a
   different tree; the current unreleased work therefore needs a later version.
   Limit the storage compatibility claim to the current
   revision-3 layouts pinned by checked-in cross-release fixtures; do not claim
   broader filesystem or power-loss qualification.

For any deployment-specific durability claim, additionally collect a schema-2
`durability-check` receipt from every exact target filesystem class using the
release revision. Follow [filesystem qualification](filesystem-qualification.md)
and retain the secured destructive ENOSPC/power-cut record separately. The two
generic GitHub runner receipts are portability sentinels, not production-volume
certificates.

Run `meld qualification-check` over the exact capability and release-soak
receipts before accepting them as Level 3 evidence. Production qualification
requires `--require-level 4` plus a separately secured, hash-bound destructive
record; the command deliberately refuses to infer power-loss safety from normal
CI or successful `fsync` calls.

Never publish from a worktree containing database files, WAL files, credentials,
or development-auth deployments.
