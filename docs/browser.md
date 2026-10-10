# Browser

Use the Meldbase client in a browser when a web application needs documents and
live queries. The browser connects to a running `meld serve`; it never opens a
`.meld` file directly.

Install the SDK in your browser application:

```sh
npm install @meldbase/client@beta
```

The package is published as `0.1.0-beta.1`; see the
[SDK compatibility policy](/sdk-status) before upgrading.
To run the included browser example, use a Meldbase checkout:

```sh
pnpm install --frozen-lockfile
go run ./cmd/meld serve --db /tmp/meldbase-todos.meld --addr 127.0.0.1:8080 --dev-no-auth
```

In a second terminal, from the same checkout:

```sh
pnpm --filter @meldbase/example-realtime-todos dev
```

Open `http://127.0.0.1:5173`. Open it in two windows to see a query update in
both places. The public example uses a `completed` field for todo state;
the API excerpts below use `done` to match the other guides.

`--dev-no-auth` is for local development only. Do not expose that server to a
network.

## Create a client

In a browser, the client uses the platform's `fetch` and `WebSocket`:

```js
import { MeldbaseClient } from "@meldbase/client";

const client = new MeldbaseClient({
  baseUrl: "http://127.0.0.1:8080",
});

const todos = client.collection("todos");
```

For a server that requires authentication, supply a token from your application
identity system:

```js
const client = new MeldbaseClient({
  baseUrl: "https://api.example.com",
  accessToken: () => session.accessToken,
});
```

The server, not the client, decides what that token is allowed to read or
change.

## Read and write documents

```js
const todo = await todos.insertOne({
  title: "Write the first document",
  done: false,
});

const openTodos = await todos.find({ done: false }).fetch();

await todos.updateOne({ _id: todo._id }, { $set: { done: true } });

await todos.deleteOne({ _id: todo._id });
```

`insertOne` adds `_id` when one is not supplied. Queries and updates are data,
not JavaScript expressions: the server validates them and applies its own
collection policy.

## Keep a query live

Call `subscribe` on a query to receive the first result and later updates. It
returns a function that stops the subscription.

```js
const stop = todos.find({ done: false }).subscribe((documents) => {
  renderTodos(documents);
});

window.addEventListener("beforeunload", stop);
```

For long-lived applications, also handle connection state:

```js
const stop = todos.find({ done: false }).subscribe(renderTodos, {
  onStatus(status) {
    setConnectionState(status.state);
  },
});
```

The client reconnects subscriptions after a temporary connection loss. Treat
`stale` and `resyncing` as display state, not as permission to write an
optimistic cache.

## What comes next

Use [the Go tour](/getting-started) when your Go process should own the
database file. Use [the Node.js guide](/node) when another server process
needs the HTTP and realtime boundary.
