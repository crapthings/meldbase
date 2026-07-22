# @meldbase/client

Isomorphic TypeScript client for Meldbase. The root entry is the authenticated
remote client; the explicit `@meldbase/client/local` subpath provides a
standalone in-memory state collection with the same bounded query grammar. The
package is an alpha preview.

```ts
import { MeldbaseClient } from "@meldbase/client";

const client = new MeldbaseClient({
  baseUrl: "https://api.example.com",
  accessToken: () => sessionToken,
  webSocketFactory: (url) => new WebSocket(url),
});
const todos = client.collection("todos");
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
authorization token: normal collection policy, tenant constraints, field
policy, and result limits are applied again to every page request. `after`
requires `first` and cannot be combined with `skip`; `fetchPage()` likewise
requires `first`.

## Policy-capped counts

Use `count` for badges and dashboards when a document list is unnecessary:

```ts
const open = await todos.count({ status: 'open' });
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

The explicit subpaths are `@meldbase/client/local` and
`@meldbase/client/types`; remote APIs are imported from the root package.

See the repository documentation for the current protocol, security model, and
alpha compatibility boundary.
