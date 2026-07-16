# @meldbase/server

Trusted Node.js worker SDK for Meldbase RPC methods, transactional point writes,
and data-only publication policies. The worker runs outside the Go database
process and connects to a private authenticated control listener. The package is
an alpha preview.

```ts
import { MeldbaseWorker, publish, rpc } from "@meldbase/server";
import WebSocket from "ws";

const worker = new MeldbaseWorker({
  url: process.env.MELDBASE_WORKER_URL!,
  token: process.env.MELDBASE_WORKER_TOKEN!,
  workerId: "application-1",
  webSocketFactory: (url, { headers }) => new WebSocket(url, { headers }),
  methods: {
    "health.echo": rpc((_context, [value]) => value ?? null),
  },
  publications: {
    todos: publish({
      version: "owner-v1",
      maxResults: 100,
      queryPaths: ["done"],
      resultFields: ["owner", "done", "title"],
    }, ({ principal }) => ({
      version: 1,
      where: { op: "compare", cmp: "eq", path: "owner", value: principal.subject },
    })),
  },
});

await worker.start();
```

The application supplies its WebSocket implementation; no transport package is
bundled. Mount the Go worker hub on a private TLS listener and use a dedicated
worker credential, never a browser or admin token.

See `docs/server-js-sdk.md` in the repository for protocol and transaction
semantics.
