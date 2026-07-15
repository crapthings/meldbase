# Release checklist

Meldbase releases are developer previews until the roadmap explicitly promotes
them beyond that status.

## One-time repository setup

- Verify `go.mod`, Go imports, and package metadata still use the canonical
  `github.com/crapthings/meldbase` repository path.
- Keep the Apache-2.0 `LICENSE` and package metadata in sync.
- Enable GitHub private vulnerability reporting.
- Protect `main` and require both CI jobs.
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
5. Review generated package contents before any npm publication with
   `pnpm pack --dry-run` in each SDK package.
6. Tag experimental releases as prereleases, beginning with
   `v0.1.0-alpha.1`; do not promise storage-format compatibility yet.

Never publish from a worktree containing database files, WAL files, credentials,
or development-auth deployments.
