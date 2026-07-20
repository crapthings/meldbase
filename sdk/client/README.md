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

Remote realtime connections use a short-lived authenticated ticket. Cross-origin
realtime endpoints must be explicitly allowlisted. Queries contain no callbacks
or executable source and are validated on both sides of the connection. HTTP
responses are read through a streaming byte ceiling and decoded only from exact
versioned envelopes; chunked responses cannot bypass `maxInboundBytes`.

Subpath exports are available as `@meldbase/client/local`,
`@meldbase/client/remote`, and `@meldbase/client/types`.

See the repository documentation for the current protocol, security model, and
alpha compatibility boundary.
