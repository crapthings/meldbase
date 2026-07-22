# Build a live todo app

The [`examples/realtime-todos`](https://github.com/crapthings/meldbase/tree/main/examples/realtime-todos)
application is the smallest browser example that uses the real client contract:
typed documents, HTTP mutations, a realtime ticket, WebSocket snapshots, and
the React `useLiveQuery` adapter.

It is an application example, not a replacement for the operator dashboard.
Open it in two browser windows and both views receive the same live query
updates.

## Run it on one machine

From a repository checkout, install the workspace dependencies once:

```sh
pnpm install
```

In the first terminal, start a loopback-only development server. This mode
intentionally has no authentication and must not be exposed beyond your own
machine:

```sh
go run ./cmd/meld serve \
  --db /tmp/meldbase-todos.meld \
  --addr 127.0.0.1:8080 \
  --dev-no-auth
```

In a second terminal, serve the React example:

```sh
pnpm --filter @meldbase/example-realtime-todos dev
```

Open `http://127.0.0.1:5173` twice. Add, complete, or delete a task in either
window and watch the other update. The example's default API URL is
`http://localhost:8080`; set `VITE_MELDBASE_URL` if your loopback listener uses
another port.

## Use it with real workspace isolation

For a JWT-configured server, the browser receives an access token from your
identity service, not from Meldbase. Start the example with that token and the
API URL:

```sh
VITE_MELDBASE_URL=https://api.example.test \
VITE_MELDBASE_TOKEN='eyJ...' \
pnpm --filter @meldbase/example-realtime-todos dev
```

The server verifies the bearer token for both the HTTP request and the realtime
ticket exchange. It derives the active workspace from the signed token; the
example never sends a client-selected workspace. To switch workspaces, obtain a
new token after your identity service has checked membership, then restart the
example with it.

Do not put a long-lived production token in a checked-in `.env` file. A real
application should supply refreshed credentials from its own authentication
flow. Read [identity, users, and workspaces](./identity-and-workspaces) for the
responsibility boundary and [the client protocol](../client-protocol) for the
wire contract.

## What to read in the source

[`App.tsx`](https://github.com/crapthings/meldbase/blob/main/examples/realtime-todos/src/App.tsx)
shows the complete client flow:

1. `new MeldbaseClient` configures the API URL and optional bearer token.
2. `collection("todos").find(...)` creates a typed query.
3. `useLiveQuery` subscribes React to its snapshots and sync status.
4. `insertOne`, `updateOne`, and `deleteOne` use the same collection contract;
   server commits appear in both windows through the subscription.

The [generated TypeScript API reference](/api/typescript/) describes the
symbols; this example shows how they fit together in an application.
