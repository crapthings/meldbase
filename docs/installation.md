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

## CLI and server

On macOS, install the released CLI with Homebrew:

```sh
brew tap crapthings/tap
brew trust --tap crapthings/tap
brew install crapthings/tap/meldbase
meld version
meld --help
```

The Homebrew package is named `meldbase`; the installed command is `meld`.

### Install from source

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

## TypeScript SDK beta

Install the packages your application needs:

```sh
npm install @meldbase/client@beta
npm install @meldbase/react@beta react
npm install @meldbase/worker@beta
```

All three packages are published as `0.1.0-beta.2`. Pin that exact version for
reproducible builds. Node.js 20 or later is required for Node applications.
See [SDK compatibility and release status](/sdk-status) for the beta API policy.

Continue with the [Browser guide](/browser) or [Node.js guide](/node).
For a complete npm-installed React application with local JWT authentication,
use the [React starter](/react-starter).

## Run the repository example

To explore the public browser example, use the repository workspace:

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
For a runnable Node script using the published SDK, follow
[Node.js](/node).

## Next step

Learn the [document model](/documents), then [query your collection](/queries).
