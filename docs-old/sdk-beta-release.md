# Publishing an SDK beta

The SDK beta release is intentionally manual. Publish only a committed, clean
checkout on `main`, after the full CI suite has passed.

1. Set the same prerelease version in `sdk/client/package.json`,
   `sdk/worker/package.json`, and `sdk/react/package.json`. The command accepts
   only the form `x.y.z-beta.N`.
2. Push that commit to `main` and use the **SDK beta release** GitHub Actions
   workflow. It has the OIDC permission npm needs to attach provenance.
3. The workflow re-runs the SDK quality and documentation gates, then runs the
   release command:

```sh
pnpm release:sdk:beta
```

The command refuses a dirty worktree, verifies a fresh non-ignored source tree
against the installed locked dependency topology, and packs each SDK through its `prepack` hook.
It checks the exact tarball contents, non-empty build artifacts, runtime imports,
and a strict TypeScript consumer before publishing the verified tarballs in
dependency order: client, worker, then React.

Each tarball is published with npm using `--access public --tag beta --provenance`.
Do not add `--no-git-checks`, publish from a developer laptop, or
retag a different version as `beta`; those practices weaken the release receipt.
