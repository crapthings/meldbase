# Node.js

Use the Meldbase client in Node.js when another server process needs to work
with a running Meldbase service. Node connects over HTTP and WebSocket; it does
not open a `.meld` file directly.

The SDK is currently a workspace preview and is not yet published to npm. Its
public package name will be `@meldbase/client`. The API below is available from
the Meldbase workspace today.

## Start a local service

From a Meldbase checkout, start an explicitly local development server:

```sh
go run ./cmd/meld serve --db /tmp/meldbase-node.meld --dev-no-auth
```

`--dev-no-auth` is for local development only. Do not expose that server to a
network.

## Create a client

Node.js 20 or later is required. Install a WebSocket implementation for live
queries:

```sh
npm install ws
```

```js
import WebSocket from 'ws'
import { MeldbaseClient } from '@meldbase/client'

const client = new MeldbaseClient({
  baseUrl: process.env.MELDBASE_URL ?? 'http://127.0.0.1:8080',
  webSocketFactory: url => new WebSocket(url)
})

const todos = client.collection('todos')
```

For an authenticated service, supply a token issued by your identity system:

```js
const client = new MeldbaseClient({
  baseUrl: process.env.MELDBASE_URL,
  accessToken: () => process.env.MELDBASE_ACCESS_TOKEN,
  webSocketFactory: url => new WebSocket(url)
})
```

## Read and write documents

```js
const todo = await todos.insertOne({
  title: 'Process a job',
  done: false
})

await todos.updateOne(
  { _id: todo._id },
  { $set: { done: true } }
)

const completed = await todos.find({ done: true }).fetch()
console.log(completed)
```

Queries and updates are data, not JavaScript expressions. The server validates
them and applies its own collection policy.

## Subscribe to changes

```js
const stop = todos.find({ done: false }).subscribe(documents => {
  queueWork(documents)
}, {
  onStatus (status) {
    logger.info({ state: status.state }, 'meldbase connection')
  }
})

process.once('SIGTERM', () => {
  stop()
  client.close()
})
```

The client reconnects subscriptions after a temporary connection loss. Treat
`stale` and `resyncing` as connection state, not as permission to retry a write
without first reconciling its outcome.

## What comes next

Use [the Browser guide](/browser) for client-side applications. Use
[the Go tour](/getting-started) when the current Go process should own the
database file.
