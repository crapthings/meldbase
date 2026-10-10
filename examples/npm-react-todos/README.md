# React starter from npm

A standalone React application using the published `@meldbase/client` and
`@meldbase/react` `0.1.0-beta.2` packages. It has its own npm lockfile and is
excluded from the pnpm workspace. Copy this directory into your own project;
there are no imports from the Meldbase SDK source tree.

## Run

Requirements: Node.js 22.12 or later, npm, and a `meld` CLI built with Go 1.23
or later. From a Meldbase checkout:

```sh
mkdir -p tmp
go build -o ./tmp/meld-starter ./cmd/meld
cd examples/npm-react-todos
npm ci
MELDBASE_BIN="$(pwd)/../../tmp/meld-starter" npm run dev
```

If `meld` is already on your `PATH`, just run `npm ci` and `npm run dev` from
this directory. Open **http://127.0.0.1:5173** in two windows. Add a task, toggle
its completion, and delete it; changes appear in both windows.

The launcher starts a loopback Go server, a local JWT identity fixture, and Vite.
Ctrl+C stops both servers and removes the temporary signing key. Documents stay
in `.meldbase-starter/todos.meld` across restarts. That directory is ignored.
No token or signing secret is checked in or compiled into the browser bundle.

```sh
npm run check
npm run build
```

## Follow the data

- `src/App.tsx` imports the npm SDKs, requests a short-lived token via the
  `accessToken` callback, writes documents, and renders `useLiveQuery` results.
- `scripts/local-server.mjs` creates an ephemeral signing key and collection
  policy. Go authenticates the JWT and owns the `workspaceId` field. Only the
  `todos` collection is available to the demo identity.
- `scripts/dev.mjs` serves `/session` locally and proxies HTTP and WebSocket
  traffic to Go under the browser application's origin.
- A disconnected subscription retains its last snapshot and reconnects
  automatically. Writes show errors and are not automatically retried.

The local identity fixture always represents one demo user in the `starter`
workspace. It demonstrates authenticated database access, not user login.
For deployment, replace it with your identity provider and configure the Go
server's issuer, audience, workspace claim, policy, and allowed browser origins.
Serve the API and realtime route through your application proxy, or explicitly
configure `allowedRealtimeOrigins` when using a separate realtime origin.
The Vite development session endpoint is not part of the static production build.

## End-to-end verification

The launcher checks session issuance, an authenticated collection query, and
realtime ticket issuance through the application proxy before printing the URL.
Each request times out after five seconds. Failure stops the services and prints
the failing stage and a suggested fix, without exposing credentials. It does
not write test documents. Actual WebSocket subscriptions are verified below.

From the Meldbase repository root:

```sh
pnpm test:sdk:npm
```

This check copies only the standalone starter into a temporary directory, runs
`npm ci` from the registry and a strict production build, starts the real Go
service, and verifies authentication failures, collection restrictions,
workspace isolation, browser CRUD, and live-query recovery after a forced
transport interruption. It cleans up its test database and browser processes.
