# SDK compatibility and release status

TypeScript SDK `0.1.0-beta.1` is published on npm under the `beta` tag.
Install the client with `npm install @meldbase/client@beta`.
The Go database remains alpha.

## Public application entry points

| Entry point              | Purpose                                        |
| ------------------------ | ---------------------------------------------- |
| `@meldbase/client`       | Remote HTTP and realtime client                |
| `@meldbase/client/local` | In-memory local collections                    |
| `@meldbase/client/types` | Shared document, query, and update types       |
| `@meldbase/react`        | React live-query adapter                       |
| `@meldbase/worker`       | Trusted server-side RPC and read-policy worker |

`@meldbase/client/internal` is experimental, maintained for SDK implementation
sharing, and is excluded from the application compatibility baseline.

## What the baseline guarantees

Five API Extractor reports in `sdk/api/` record public signatures. An additional
export manifest records whether each name is a runtime value, a type, or both.
CI rejects unreviewed changes to this baseline. It still runs runtime tests,
strict TypeScript consumers of packed tarballs, and real-browser integration.

A baseline check detects a difference; it does not prove every behavioral change
is compatible. Additions must preserve existing consumers. Incompatible changes
require an explicit migration note and a new beta version. Published versions
are never replaced. Beta is not a 1.0 stability promise.

Protocol v1 retains its existing compatibility rule: incompatible wire grammar
changes require a new protocol version. SDK beta status does not promise storage
compatibility for the alpha Go database.

## Review an intentional API change

```sh
pnpm api:sdk:check
pnpm api:sdk:update
```

Review the generated diff; commit it with tests, release notes, and the package
version change. Update mode is a maintainer action and is not run in CI.

## Release evidence

The [beta checklist](/sdk-beta-checklist) describes the required validation.
The [publishing guide](/sdk-beta-release) covers initial npm authentication and
subsequent trusted publishing. The [SDK beta release](https://github.com/crapthings/meldbase/releases/tag/sdk-v0.1.0-beta.1)
includes verified archives and a source-commit/integrity receipt.
