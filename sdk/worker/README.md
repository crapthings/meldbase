# @meldbase/worker

Trusted Node.js worker SDK for Meldbase RPC methods, `rpc.transactional` point writes,
and data-only read policies. The worker runs outside the Go database
process and connects to a private authenticated control listener. The package is
an alpha preview.

```ts
import { MeldbaseWorker, readPolicy, rpc } from "@meldbase/worker";
import WebSocket from "ws";

const worker = new MeldbaseWorker({
  url: process.env.MELDBASE_WORKER_URL ?? "meldbase://control.internal",
  token: process.env.MELDBASE_WORKER_TOKEN!,
  workerId: "application-1",
  webSocketFactory: (url, { headers }) => new WebSocket(url, { headers }),
  methods: {
    "health.echo": rpc((_context, input) => input),
  },
 readPolicies: {
    todos: readPolicy({
      version: "owner-v1",
      maxResults: 100,
      queryPaths: ["done"],
      resultFields: ["owner", "done", "title"],
    }, ({ actor }) => ({
      version: 1,
      where: { op: "compare", cmp: "eq", path: "owner", value: actor.id },
    })),
  },
});

await worker.start();
```

`url` accepts either a full `ws://` or `wss://` worker control endpoint, or the
secure Meldbase authority form `meldbase://host[:port]`. The authority form
always resolves to `wss://host[:port]/v1/workers`; it rejects paths,
credentials, query parameters, and fragments. Use an explicit `ws://` URL only
for local development or tests.

The application supplies its WebSocket implementation; no transport package is
bundled. Mount the Go worker hub on a private TLS listener and use a dedicated
worker credential, never a browser or admin token.

## Internal layout

The package remains one installable SDK. Its source is split by runtime
responsibility: `worker.ts` owns the connection lifecycle, `transaction.ts`
bridges transactional operations, `protocol.ts` validates capability discovery,
and `definitions.ts` owns public method/read-policy declarations. These are
implementation modules; consumers should continue importing only from
`@meldbase/worker`.
