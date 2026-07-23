# @meldbase/client

Isomorphic TypeScript client for Meldbase. The root entry is the authenticated
remote client; the explicit `@meldbase/client/local` subpath provides a
standalone in-memory state collection with the same bounded query grammar. The
package is a beta API: use only the documented entry points below.

```ts
import { MeldbaseClient } from "@meldbase/client";

const client = new MeldbaseClient({
  baseUrl: "https://api.example.com",
  accessToken: () => sessionToken,
  webSocketFactory: (url) => new WebSocket(url),
});
type Todo = import("@meldbase/client").Document & {
  readonly title: string;
  readonly done: boolean;
};

const todos = client.collection<Todo>("todos");
const stop = todos.find({ done: false }).subscribe((documents) => {
  console.log(documents);
});
```

## Entry points

Use the root package for an authenticated Meldbase collection:

```ts
import { MeldbaseClient } from "@meldbase/client";

const client = new MeldbaseClient({ baseUrl, accessToken });
const todos = client.collection("todos");
```

Use the local subpath only when application-owned memory is what you need:

```ts
import { LocalCollection } from "@meldbase/client/local";

const drafts = new LocalCollection([{ _id: "00000000000000000000000000000001", done: false }]);
```

The supported imports are the root package, `@meldbase/client/local`, and
`@meldbase/client/types`. `@meldbase/client/internal` exists only so the
Meldbase-maintained worker package can share protocol code; it is not a stable
application API.

## Typed inserts

Give a collection its document type before writing. `insertOne` accepts the
complete document shape except for `_id`, which Meldbase can generate. It does
not accept a broad `InputDocument` and pretend the returned value has your
schema.

```ts
type Todo = import("@meldbase/client").Document & {
  readonly title: string;
  readonly done: boolean;
};

const todos = client.collection<Todo>("todos");
const todo = await todos.insertOne({ title: "Ship beta", done: false });
// todo is Todo and has a generated canonical _id.
```

Use `MeldbaseClient.insertOne()` only for intentionally untyped operations; it
returns `Document`. `LocalCollection<T>.insertOne()` follows the same typed
contract. A generic ID-valued field uses `documentID(value)`; the persisted
document `_id` remains a canonical string.

## Local and remote are not interchangeable stores

Both collection types use the same bounded filter, sort, pagination, and update
grammar, but they have different authority. `LocalCollection` is in-memory
state: its synchronous `upsert(document)` is a local upsert with no network or
policy check. `RemoteCollection` is an authenticated server boundary and
intentionally offers only `insertOne`, bounded updates, and bounded deletes.
It has no generic remote `replace` method.

`LocalCollection` is not a cache, replica, or offline mode for
`RemoteCollection`. Moving documents between them is explicit application code.

`count` and `groupCount` are remote-only because their `capped` result conveys
server policy and visibility limits. Do not substitute an exact local count for
a remote dashboard or permission decision. Use a named transactional RPC when a
server-owned full replacement must be atomic.

See the [client protocol](https://crapthings.github.io/meldbase/client-protocol#local-and-remote-collection-boundary)
for the complete authority and API-boundary contract.

## Seek pagination

For growing collections, prefer `first` and `after` to offset pagination.
The SDK derives a data-only cursor from the final visible document and expands
it into a lexicographic query predicate. It automatically appends `_id` as a
stable tie-breaker when the requested sort does not contain one, so adjacent
pages do not duplicate or omit documents with equal sort values.

```ts
const sort = [{ path: "createdAt", direction: -1 }] as const;

const firstPage = await todos.find({}, { sort, first: 50 }).fetchPage();
const secondPage = firstPage.nextCursor
  ? await todos.find({}, { sort, first: 50, after: firstPage.nextCursor }).fetchPage()
  : undefined;
```

The cursor is bounded and validated against the supplied sort. It is not an
authorization token: normal collection policy, workspace constraints, field
policy, and result limits are applied again to every page request. `after`
requires `first` and cannot be combined with `skip`; `fetchPage()` likewise
requires `first`.

`fetchPage()` is the public pagination boundary. Do not manufacture or persist
cursor payloads yourself: cursor encoding and the wire query AST are internal
details and can change between beta releases.

## Policy-capped counts

Use `count` for badges and dashboards when a document list is unnecessary:

```ts
const open = await todos.count({ status: "open" });
console.log(open.count, open.capped);
```

Counts use the same query authorization path as document reads. A policy may
cap the result budget, so `capped: true` means `count` is a lower bound rather
than an exact total. The API intentionally accepts a filter only: offset,
sort, and page modifiers are not meaningful for a safe aggregate count.

## Policy-constrained group counts

For a small dashboard breakdown, use the intentionally narrow `groupCount`
aggregate:

```ts
const byStatus = await todos.groupCount({ archived: false }, "status");
// [{ key: "open", count: 12 }, { key: "closed", count: 3 }]
console.log(byStatus.groups, byStatus.capped);
```

It groups by one top-level field and returns counts only—there is no generic
aggregation pipeline. The field must be allowed by both the server's aggregate
and result-field policy (keys are returned data), the query's normal row
constraints still apply, and at most 100 distinct groups are returned. As with
`count`, `capped: true` marks a policy-capped lower bound.

Remote realtime connections use a short-lived authenticated ticket. Cross-origin
realtime endpoints must be explicitly allowlisted. Queries contain no callbacks
or executable source and are validated on both sides of the connection. HTTP
responses are read through a streaming byte ceiling and decoded only from exact
versioned envelopes; chunked responses cannot bypass `maxInboundBytes`.

## Write outcomes and retries

A `MeldbaseInternalError` with code `outcome_unknown` means a write may have
reached the server, but the client could not verify its result—for example after
a transport loss or a malformed response envelope. Do not blindly retry that
operation. Reconcile it with application state first, or put non-idempotent
business work behind a named RPC and retry that RPC with the same
`idempotencyKey`. Authentication or validation failures before a request is
dispatched are reported directly instead of as an unknown outcome.

The explicit subpaths are `@meldbase/client/local` and
`@meldbase/client/types`; remote APIs are imported from the root package.

See the [SDK guide](https://crapthings.github.io/meldbase/sdk) for client,
React, worker, retry, security, pagination, and compatibility guidance.
