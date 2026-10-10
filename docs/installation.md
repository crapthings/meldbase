# Installation

Choose whether your application owns the file or connects to a server.

## Embedded Go

Inside a Go module:

```sh
go get github.com/crapthings/meldbase@latest
```

The module targets Go 1.23 or later. Continue with the
[complete Go example](/getting-started); no database service is required.
Pin a release in `go.mod` when reproducible builds matter.

## CLI and server from source

With Go installed:

```sh
go install github.com/crapthings/meldbase/cmd/meld@latest
meld --help
```

Ensure your Go binary directory is on `PATH`. Alternatively, download the CLI
for your operating system from [GitHub Releases](https://github.com/crapthings/meldbase/releases).

For a local browser or Node.js experiment:

```sh
meld serve --db /tmp/meldbase-tour.meld --addr 127.0.0.1:8080 --dev-no-auth
```

Keep this development server local. An externally reachable service requires
an authentication and collection-policy configuration; inspect `meld serve --help`
and the server configuration in the repository before deploying it.

## TypeScript workspace preview

The SDK packages are not yet published to npm. Use the public workspace:

```sh
git clone https://github.com/crapthings/meldbase.git
cd meldbase
corepack enable
pnpm install --frozen-lockfile
pnpm --filter @meldbase/client build
```

The repository uses pnpm 10.34.5. Node.js 20 or later is required for the Node
client. The browser example runs directly from this checkout:

```sh
pnpm --filter @meldbase/example-realtime-todos dev
```

Start the server in another terminal and follow the [Browser guide](/browser).
For a runnable Node script importing the built workspace SDK, follow
[Node.js](/node).

## Next step

Learn the [document model](/documents), then [query your collection](/queries).
