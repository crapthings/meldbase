# Public SDK API baseline

The reviewed reports cover the five application entry points:

- `@meldbase/client`
- `@meldbase/client/local`
- `@meldbase/client/types`
- `@meldbase/react`
- `@meldbase/worker`

`@meldbase/client/internal` is experimental and is not an application contract.
API Extractor's `@public` labels describe visibility, not a 1.0 stability promise.
The packages are beta; the Go database remains alpha.

Run `pnpm api:sdk:check` to build declarations and compare them with these reports.
CI and the publication workflow run this check through `pnpm quality:sdk`.
Reports include exported signatures and referenced types. `exports.json` also
records whether each exported name is a runtime value, a type, or both.

For an intentional change, run `pnpm api:sdk:update`, inspect the report diff,
and commit it together with tests, release notes, and the package-version change.
Additive changes must preserve existing consumers. A removal, renamed export,
required option, narrowed accepted input, changed return type, or behavioral
incompatibility requires an explicit migration note and a new beta release.
Never silently replace a published package version. Stable wire protocol v1
continues to require a new protocol version for incompatible grammar changes.

This is a review gate, not an automatic semantic compatibility proof. Existing
runtime, type-consumer, packed-package, and browser tests remain required.
