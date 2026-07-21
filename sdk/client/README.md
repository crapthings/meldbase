# @meldbase/client

Isomorphic TypeScript client for Meldbase. It provides the same bounded,
data-only query and mutation grammar for local collections and remote Go-backed
collections. The package is an alpha preview.

```ts
import { LocalCollection, MeldbaseClient } from "@meldbase/client";

const local = new LocalCollection([{ _id: "local-1", done: false }]);
const open = local.find({ done: false });

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
policy, and result limits are applied again to every page request. `after` and
`skip` cannot be combined.

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

Subpath exports are available as `@meldbase/client/local`,
`@meldbase/client/remote`, and `@meldbase/client/types`.

See the repository documentation for the current protocol, security model, and
alpha compatibility boundary.
