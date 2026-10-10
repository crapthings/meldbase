# React starter

Start with a complete application that installs the released SDKs from npm,
connects to a real Meldbase service, and renders a live task list.

## Run locally

You need Go 1.23 or later, Node.js 22.12 or later, and npm:

```sh
git clone https://github.com/crapthings/meldbase.git
cd meldbase
mkdir -p tmp
go build -o ./tmp/meld-starter ./cmd/meld
cd examples/npm-react-todos
npm ci
MELDBASE_BIN="$(pwd)/../../tmp/meld-starter" npm run dev
```

Open **http://127.0.0.1:5173** in two windows. Add a task, mark it complete,
and delete it. Both windows update through the same live query.
Ctrl+C stops the starter. Your documents remain in its ignored
`.meldbase-starter/todos.meld` file across restarts.

## Use it in your application

Copy `examples/npm-react-todos` into your project and run `npm ci` there.
It is an independent npm project, excluded from the repository's pnpm
workspace, with `@meldbase/client` and `@meldbase/react` pinned to
`0.1.0-beta.1`. No workspace package links are needed.

The React component uses a module-level client and query, so React renders
subscribe to a stable query rather than opening a new connection each time:

```tsx
import { MeldbaseClient } from "@meldbase/client";
import { useLiveQuery } from "@meldbase/react";

const client = new MeldbaseClient({
  baseUrl: new URL("/api", window.location.origin).href,
  async accessToken() {
    const response = await fetch("/session", { cache: "no-store" });
    if (!response.ok) throw new Error("Session unavailable");
    return (await response.json()).accessToken;
  },
});
const query = client.collection("todos").find({});

function Tasks() {
  const { documents, status, error } = useLiveQuery(query);
  return <div>{error?.message ?? `${documents.length} tasks · ${status}`}</div>;
}
```

The complete component includes insert, update, delete, pending controls,
mutation errors, and connection status. During a disconnect it retains the last
snapshot; the SDK reconnects and refreshes the query. Failed writes are shown
without automatic retries.

## Authentication

The launcher generates a temporary signing key on the server and issues
five-minute JWTs for one local demo identity. The browser fetches a fresh token
for HTTP and realtime authentication. Go validates issuer, audience, and
workspace membership, and assigns the document's workspace field itself.

This fixture demonstrates database authentication. Replace `/session` with
your application's identity integration when adding real users. The fixture
runs only in the loopback development launcher; it is absent from the static
build. HTTP and realtime use the same application origin through its proxy.
With a separate realtime origin, configure the client's
`allowedRealtimeOrigins` explicitly.

## Verified integration

CI installs the starter from its npm lockfile in a fresh directory, checks
types, builds React, and starts the real Go service. It verifies unauthenticated
and unauthorized requests are rejected, workspaces are isolated, browser CRUD
reaches the database, and subscriptions recover after an offline interval.

[Browse the starter source](https://github.com/crapthings/meldbase/tree/main/examples/npm-react-todos)
or read the [SDK compatibility policy](/sdk-status) before upgrading.
