# Client SDK API guide

`@meldbase/client` is the browser and isomorphic TypeScript SDK for local
collections, authenticated HTTP reads and writes, realtime query delivery, and
application RPC. `@meldbase/react` adds one React hook for those query objects.

This page is the task-oriented API reference: it explains the public methods
that application code should use, why they exist, and how to start with each
one. For every exported symbol and exact TypeScript signature, use the
[generated TypeScript API reference](/api/typescript/), which is built from the
SDK source for each release.

The SDK and its wire contract are alpha. Read the [client protocol](../client-protocol)
before implementing another client or relying on its transport behavior.

## Choose an entry point

Most applications use the root package and start with an authenticated remote
collection. `LocalCollection` is intentionally a separate import for
application-owned in-memory state; it is not a local cache or replica of the
remote collection.

| Import | Use it for |
| --- | --- |
| `@meldbase/client` | `MeldbaseClient`, remote collections, RPC, realtime, and shared types. |
| `@meldbase/client/local` | Explicit in-memory documents, local queries, and local observers. |

## Start a remote client

Create one `MeldbaseClient` for an API origin, then obtain typed collection
handles from it. The access-token callback is evaluated for each HTTP request;
applications can therefore refresh a short-lived token without recreating the
client.

```ts
import { MeldbaseClient, type Document } from "@meldbase/client";

type Todo = Document & {
  title: string;
  done: boolean;
  status: string;
};

const client = new MeldbaseClient({
  baseUrl: "https://api.example.com",
  accessToken: () => sessionToken,
  webSocketFactory: (url) => new WebSocket(url),
});

const todos = client.collection<Todo>("todos");
```

| Name | Purpose | Example |
| --- | --- | --- |
| `new MeldbaseClient(options)` | Creates the authenticated HTTP and realtime client. | `new MeldbaseClient({ baseUrl, accessToken })` |
| `client.collection<T>(name)` | Gets the normal application API for one remote collection. | `client.collection<Todo>("todos")` |
| `client.call<T>(method, args?, options?)` | Calls a named server Worker RPC. HTTP is the default transport; realtime can reuse a live socket. | `await client.call("todos.archive", [todoID], { idempotencyKey: newDocumentID() })` |
| `client.realtimeProtocol` | Reads the discovered realtime protocol descriptor. It is `undefined` until a ticket has supplied one. | `client.realtimeProtocol?.capabilities` |
| `client.close()` | Permanently closes this client and its realtime connection. Create a new client to resume work. | `client.close()` |

For an RPC that may have executed even if the caller loses its result, supply a
stable `idempotencyKey` and explicitly handle retry policy. The SDK never
automatically retries RPC calls. A realtime call that loses its terminal result
throws `MeldbaseInternalError` with code `outcome_unknown` rather than
pretending that it failed before execution. See [RPC idempotency](../rpc-idempotency)
for the server-side requirements.

## Work with a remote collection

`RemoteCollection` is the primary data API for browser and service code. It
shares its safe, data-only query and update grammar with `LocalCollection`, but
the server still applies authentication, workspace scope, collection policy,
and result limits.

| Name | Purpose | Example |
| --- | --- | --- |
| `collection.find(filter?, options?)` | Creates a remote query that can fetch once or subscribe live. | `todos.find({ done: false })` |
| `collection.findOne(filter?, options?)` | Fetches the first matching document, or `undefined`. | `await todos.findOne({ _id: todoID })` |
| `collection.insertOne(document, options?)` | Strictly inserts one document. If `_id` is omitted, the client creates it before sending the request. | `await todos.insertOne({ title: "Ship", done: false, status: "open" })` |
| `collection.updateOne(filter, update, options?)` | Applies a bounded update to the first matching document. | `await todos.updateOne({ _id: todoID }, { $set: { done: true } })` |
| `collection.updateMany(filter, update, options?)` | Applies a bounded update to every matching document. | `await todos.updateMany({ status: "open" }, { $set: { status: "active" } })` |
| `collection.deleteOne(filter, options?)` | Deletes the first matching document. | `await todos.deleteOne({ _id: todoID })` |
| `collection.deleteMany(filter, options?)` | Deletes every matching document. | `await todos.deleteMany({ done: true })` |
| `collection.count(filter?, options?)` | Counts visible matching documents. `capped: true` means the count is a lower bound. | `const result = await todos.count({ done: false })` |
| `collection.groupCount(filter, groupBy, options?)` | Counts visible documents by one permitted top-level field; at most 100 groups are returned. | `await todos.groupCount({ done: false }, "status")` |

Each remote operation accepts an optional `AbortSignal` in its options. An
aborted write does not prove that the server did not commit it. An insert whose
result cannot be verified throws `MeldbaseInternalError` with code
`outcome_unknown`; its `operation` identifies the insert and the original error
is retained as `cause`. Use a server-owned idempotent RPC when a business
operation needs stronger retry semantics.

There is deliberately no remote `replace()` or `upsert()` method. Full-document
replacement or filter-based upsert across an authorization boundary is easy to
misuse; use `$set` and the other bounded update operators, or expose an atomic
Worker RPC for a server-owned business operation.

### Naming and cardinality

`One` and `Many` are used only where a filter selects an effectful target set:
`updateOne` / `deleteOne` stop after one match, while `updateMany` /
`deleteMany` apply to every permitted match. `insertOne` is a strict creation
of one document. Local `upsert(document)` has no suffix because its mandatory
`_id` already names exactly one document; it creates that ID when absent or
fully replaces it when present. There is intentionally no `upsertOne` or
`upsertMany`: an unmatched filter must never silently become an insert.

### Filters and updates

Filters are data only. Use field equality, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`,
`$lte`, `$in`, `$nin`, `$exists`, nested `$not`, `$and`, and `$or`; callbacks
and executable expressions are not accepted. Updates support `$set`, `$unset`,
`$inc`, `$push`, and `$pull`.

```ts
await todos.updateMany(
  { $or: [{ status: "open" }, { priority: { $gte: 3 } }] },
  { $set: { status: "active" }, $inc: { revision: 1 } },
);
```

`Date`, `Uint8Array`, and `bigint` values are preserved by the SDK. Use
`bigint` for Meldbase Int64 values; JavaScript `number` represents Float64.

## Fetch and subscribe to a query

`collection.find()` returns a `RemoteLiveQuery`, not an array. The same object
can be fetched over HTTP, paged with a cursor, or kept live through the shared
realtime connection.

| Name | Purpose | Example |
| --- | --- | --- |
| `query.fetch(options?)` | Fetches the current query snapshot over HTTP. | `await todos.find({ done: false }).fetch()` |
| `query.fetchPage(options?)` | Fetches one seek page and derives a `nextCursor` from the final document. It requires `first`. | `await todos.find({}, { sort, first: 50 }).fetchPage()` |
| `query.subscribe(listener, options?)` | Publishes an initial snapshot and later realtime snapshots; returns an unsubscribe function. | `const stop = todos.find({ done: false }).subscribe(render)` |
| `SubscribeOptions.onStatus` | Observes connection state, an error, and the opaque resume token. | `query.subscribe(render, { onStatus: console.log })` |

For growing collections, use seek pagination rather than `skip`. `first` and
`after` require a sort; `after` also requires `first`. The SDK adds `_id` as a
stable tie-breaker when needed. `fetchPage()` is only available on such a
`first` query, so a cursor is never derived from an ordinary limited result.

```ts
const sort = [{ path: "createdAt", direction: -1 }] as const;

const first = await todos.find({}, { sort, first: 50 }).fetchPage();
const second = first.nextCursor
  ? await todos.find({}, { sort, first: 50, after: first.nextCursor }).fetchPage()
  : undefined;
```

## Use an in-memory local collection

`LocalCollection` is synchronous, in-memory state. It is useful for a local
cache, a preview, or tests; it has no network, persistence, authorization, or
automatic synchronization with a `RemoteCollection`.

```ts
import { LocalCollection } from "@meldbase/client/local";

const local = new LocalCollection<Todo>();
```

| Name | Purpose | Example |
| --- | --- | --- |
| `new LocalCollection(initial?)` | Creates an in-memory collection. | `new LocalCollection<Todo>()` |
| `local.insert(document)` | Strictly inserts a document that already has an `_id`; duplicate IDs throw. | `local.insert({ _id: todoID, title: "Draft", done: false, status: "open" })` |
| `local.insertOne(document)` | Inserts a document and generates `_id` when it is absent. | `const todo = local.insertOne({ title: "Draft", done: false, status: "open" })` |
| `local.upsert(document)` | Creates or fully replaces the one local document addressed by its `_id`. | `local.upsert({ ...todo, done: true })` |
| `local.remove(id)` | Removes by ID and reports whether a document was removed. | `local.remove(todoID)` |
| `local.find(filter?, options?)` | Creates a local live query. | `local.find({ done: false })` |
| `local.findOne(filter?, options?)` | Synchronously gets the first matching document. | `local.findOne({ _id: todoID })` |
| `local.updateOne(filter, update)` | Updates the first matching document. | `local.updateOne({ _id: todoID }, { $set: { done: true } })` |
| `local.updateMany(filter, update)` | Updates every matching document. | `local.updateMany({ done: false }, { $set: { status: "active" } })` |
| `local.deleteOne(filter)` | Deletes the first matching document. | `local.deleteOne({ _id: todoID })` |
| `local.deleteMany(filter)` | Deletes every matching document. | `local.deleteMany({ done: true })` |
| `local.batch(action)` | Coalesces several local changes into one observer notification. | `local.batch(() => { /* insert or update several documents */ })` |
| `local.execute(spec)` | Executes a compiled query specification. | `local.execute(compileQuery({ done: false }))` |
| `local.onChange(listener)` | Observes every local collection change. | `const stop = local.onChange(render)` |

All local stored IDs use the same non-zero, 32-character lowercase hexadecimal
format as remote documents. Use `newDocumentID()` or let `insertOne()` create
one.

`local.find()` returns `LiveQuery`, which has the same three user-facing query
methods as `RemoteLiveQuery`:

| Name | Purpose | Example |
| --- | --- | --- |
| `query.fetch()` | Returns a synchronous snapshot. | `local.find({ done: false }).fetch()` |
| `query.fetchPage()` | Returns a synchronous seek page and optional cursor; it requires a `first` query. | `local.find({}, { sort, first: 20 }).fetchPage()` |
| `query.subscribe(listener)` | Calls the listener initially and when this query result changes. | `local.find({ done: false }).subscribe(render)` |

## Use the React binding

`@meldbase/react` exports `useLiveQuery`. It accepts either a local `LiveQuery`
or a remote `RemoteLiveQuery` and returns documents plus synchronization state.

| Name | Purpose | Example |
| --- | --- | --- |
| `useLiveQuery(query, options?)` | Subscribes a React component to a local or remote query. | `const { documents, status } = useLiveQuery(query)` |

```tsx
import { useLiveQuery } from "@meldbase/react";
import type { Document, RemoteLiveQuery } from "@meldbase/client";

function OpenTodos({ query }: { query: RemoteLiveQuery<Document> }) {
  const { documents, status, error } = useLiveQuery(query);
  if (error) return <p>Could not sync: {error.message}</p>;
  return <p>{status}: {documents.length} open todos</p>;
}
```

For remote SSR, pass `initialData` so the server render and first browser render
agree. Keep the query object stable across renders; create it outside the
component or memoize it before passing it to the hook.

## Advanced utilities

These exports are useful for adapters, offline layers, and protocol tests. Most
application code should stay with `collection()` and its query objects.

| Name | Purpose | Example |
| --- | --- | --- |
| `compileQuery(filter, options?)` | Validates and compiles a filter into `QuerySpec`. | `compileQuery({ status: "open" }, { limit: 20 })` |
| `executeQuery(documents, spec)` | Executes a compiled query over an in-memory iterable. | `executeQuery(cache, spec)` |
| `matches(document, expression)` | Tests one document against a compiled query expression. | `matches(todo, spec.where)` |
| `compileUpdate(update)` | Validates and compiles an update into `MutationSpec`. | `compileUpdate({ $set: { done: true } })` |
| `applyMutation(document, spec)` | Applies a compiled update in memory. | `applyMutation(todo, spec)` |
| `encodeMutationSpec` / `decodeMutationSpec` | Encodes or validates a mutation wire envelope. | `decodeMutationSpec(encodeMutationSpec(spec))` |
| `pageCursorFor` / `pageFilterAfter` | Creates a cursor or expands one into a filter. | `pageFilterAfter(cursor, sort)` |
| `newDocumentID()` | Creates the portable 128-bit lower-case hexadecimal document ID. | `const id = newDocumentID()` |
| `encodeValue` / `decodeValue` | Encodes or validates typed wire values. | `decodeValue(encodeValue(42n))` |
| `encodeDocument` / `decodeDocument` | Encodes or validates a persisted document. | `decodeDocument(encodeDocument(todo))` |
| `encodeInputDocument` | Encodes an insert input where `_id` may be absent. | `encodeInputDocument({ title: "Draft" })` |
| `encodeQuerySpec` / `decodeQuerySpec` | Encodes or validates a query wire envelope. | `decodeQuerySpec(encodeQuerySpec(spec))` |
| `decodeProtocolDescriptor` | Validates a server protocol capability descriptor. | `decodeProtocolDescriptor(body.protocol)` |
| `supportsProtocol` | Tests a protocol version and required capabilities. | `supportsProtocol(protocol, 1, ["realtime.resume"])` |

### Transport-level client methods

`MeldbaseClient` also exposes the methods below. They accept a collection name
and compiled `QuerySpec` and are the implementation layer used by
`RemoteCollection`; prefer the collection methods in application code.

| Name | Purpose | Example |
| --- | --- | --- |
| `client.fetchQuery<T>(collection, query, signal?)` | Fetches a compiled query without creating `RemoteLiveQuery`. | `await client.fetchQuery<Todo>("todos", compileQuery({ done: false }))` |
| `client.count(collection, query, signal?)` | Counts a compiled query. | `await client.count("todos", compileQuery({ done: false }))` |
| `client.groupCount(collection, query, groupBy, signal?)` | Groups a compiled query by one field. | `await client.groupCount("todos", compileQuery({}), "status")` |
| `client.insertOne<T>(collection, document, signal?)` | Inserts without creating `RemoteCollection`. | `await client.insertOne<Todo>("todos", { title: "Ship", done: false, status: "open" })` |
| `client.executeMutation(collection, action, query, update?, signal?)` | Executes a compiled update or delete action. | `await client.executeMutation("todos", "updateOne", compileQuery({ _id: todoID }), compileUpdate({ $set: { done: true } }))` |
| `client.subscribe(collection, query, listener, options)` | Subscribes to a compiled query without creating `RemoteLiveQuery`. | `client.subscribe("todos", compileQuery({ done: false }), render, {})` |

## Key result and error types

- `MutationResult` has `matchedCount` and `modifiedCount`.
- `DeleteResult` has `deletedCount`.
- `CountResult` has `count` and `capped`; a capped count is not exact.
- `GroupCountResult` has typed `{ key, count }` groups and `capped`.
- `MeldbaseError` is an expected application RPC result. Its code is namespaced
  (for example `orders.already_paid`) and its optional `data` is safe,
  structured application data.
- `MeldbaseInternalError` is a Meldbase-owned failure. It preserves its safe
  engine code, HTTP status (zero for WebSocket), operation name, and optional
  local `cause`; `outcome_unknown` means the caller must reconcile before retrying.
- `MeldbaseClientClosedError` means code attempted new work after `client.close()`.
- `QueryValidationError` means the SDK rejected an unsafe, malformed, or
  over-limit local query, update, cursor, or wire value before using it.

## Maintaining this page

Keep this page focused on user-facing methods and stable examples. When an
export changes, update the matching table and run `pnpm docs:build`; the build
also regenerates TypeDoc, which remains the exact symbol-level reference.
