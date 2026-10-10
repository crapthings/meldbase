---
next: false
---

# Publishing an SDK beta

The SDK beta release is intentionally manual. Publish only a committed, clean
checkout on `main`, after the full CI suite has passed.

1. Set the same prerelease version in `sdk/client/package.json`,
   `sdk/worker/package.json`, and `sdk/react/package.json`. The command accepts
   only the form `x.y.z-beta.N`.
2. Push that commit to `main` and use the **SDK beta release** GitHub Actions
   workflow. It has the OIDC permission npm needs to attach provenance and
   uses Node.js 24 with an OIDC-capable npm CLI.
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

## First publication

The `@meldbase` npm organization must exist, and the publishing account must
have permission to create public packages within it. The three packages do not
yet exist, so a trusted publisher cannot be assumed to be configured.

Use `prepare_only` to produce verified archives and a source-commit/integrity
receipt as the `meldbase-sdk-beta` workflow artifact without publishing.

For a first authenticated publication from GitHub Actions, a maintainer can
configure the repository secret `NPM_BOOTSTRAP_TOKEN` and dispatch the release
workflow with `bootstrap` enabled. Keep the credential out of the repository,
logs, and chat. The workflow still validates and publishes the same tarballs
with provenance; bootstrap changes only the authentication source.

After the packages exist, configure each package's trusted publisher for:

- GitHub owner: `crapthings`
- Repository: `meldbase`
- Workflow filename: `sdk-beta-release.yml`

Subsequent releases leave `bootstrap` disabled and use OIDC. Remove the bootstrap
secret after completing initial publication and publisher setup. See the
[npm trusted publishing guide](https://docs.npmjs.com/trusted-publishers/) for
package-side configuration and required permissions.
