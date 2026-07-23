# TypeScript SDK beta guide

Meldbase publishes three TypeScript packages:

| Package                  | Use it for                                  | Public entry point       |
| ------------------------ | ------------------------------------------- | ------------------------ |
| `@meldbase/client`       | Authenticated remote collections and RPC    | `@meldbase/client`       |
| `@meldbase/client/local` | Explicit application-owned in-memory state  | `@meldbase/client/local` |
| `@meldbase/react`        | React subscriptions for a collection query  | `@meldbase/react`        |
| `@meldbase/worker`       | Trusted Node.js RPC and read-policy workers | `@meldbase/worker`       |

Install only the packages your runtime needs. `@meldbase/react` requires React
18 or newer and `@meldbase/worker` requires a WebSocket implementation supplied
by the application. The client and worker packages require Node 20 or newer
when run in Node.

```sh
npm install @meldbase/client
npm install @meldbase/client @meldbase/react react
npm install @meldbase/client @meldbase/worker ws
```

## Client collections

The root client is the authenticated remote boundary. Create a collection with
the document type your application owns:

```ts
import { MeldbaseClient, type Document } from "@meldbase/client";

type Todo = Document & {
  readonly title: string;
  readonly done: boolean;
  readonly createdAt: Date;
};

const client = new MeldbaseClient({
  baseUrl: "https://api.example.com",
  accessToken: () => sessionToken,
  webSocketFactory: (url) => new WebSocket(url),
});
const todos = client.collection<Todo>("todos");

const inserted = await todos.insertOne({
  title: "Review beta",
  done: false,
  createdAt: new Date(),
});
```

`insertOne` accepts the complete `Todo` shape except for `_id`; Meldbase adds a
canonical ID when it is omitted. A partial or arbitrary `InputDocument` cannot
be inserted through `RemoteCollection<T>` or `LocalCollection<T>` and then
claimed as `T`. The low-level `MeldbaseClient.insertOne` is intentionally
untyped and returns `Document`.

Use `documentID(value)` for an ID-valued field that is not the persisted
`_id`. Generic IDs preserve their typed wire kind; a document `_id` remains a
string at the SDK boundary.

`LocalCollection` is an explicit local store, not an offline replica or cache:

```ts
import { LocalCollection } from "@meldbase/client/local";

const drafts = new LocalCollection<Todo>();
drafts.insertOne({ title: "Local draft", done: false, createdAt: new Date() });
```

It shares the filter, sort, and update grammar, but it has no authentication,
server policy, durability, or synchronization semantics. Move data between
local and remote collections explicitly in application code.

## Pagination

For changing data, use seek pagination with `first` and `after`, then consume
the cursor returned by `fetchPage()`:

```ts
const sort = [{ path: "createdAt", direction: -1 }] as const;
const first = await todos.find({ done: false }, { sort, first: 50 }).fetchPage();
const second = first.nextCursor
  ? await todos.find({ done: false }, { sort, first: 50, after: first.nextCursor }).fetchPage()
  : undefined;
```

Meldbase appends `_id` as a stable tie-breaker, validates the cursor against the
sort, and bounds its size. Do not decode, alter, or reuse a cursor with a
different sort. `after` requires `first` and cannot be combined with `skip`.
Every page is re-authorized; a cursor is not an access token.

## Errors, cancellation, and retries

`MeldbaseError` is an expected application-owned RPC outcome. Handle its
stable `code` and optional typed `data`. `MeldbaseInternalError` represents a
server or protocol failure. Its `code === "outcome_unknown"` has special write
semantics: the request may have reached the server, but the SDK could not prove
the result after a transport loss or malformed terminal response.

Do not blindly retry an `outcome_unknown` insert, update, delete, or RPC. First
reconcile state, or put repeatable business work behind a named RPC and retry it
with the same `idempotencyKey`. The SDK never retries writes or RPC calls on
your behalf. An `AbortSignal` cancels local waiting and is sent to the realtime
RPC protocol when supported; it is not proof that a dispatched write was not
applied.

If a signal is already aborted before HTTP dispatch, including while an
asynchronous access-token lookup is still running, the SDK rejects with that
exact signal reason and sends no request. Only cancellation after dispatch can
become `outcome_unknown`.

```ts
try {
  await client.call("billing.charge", { invoiceId }, { idempotencyKey });
} catch (error) {
  if (error instanceof MeldbaseError) showBusinessError(error.code);
  else if (error instanceof MeldbaseInternalError && error.code === "outcome_unknown") reconcileInvoice(invoiceId);
  else throw error;
}
```

## Security boundaries

Use an HTTPS `baseUrl`, short-lived access tokens, and a browser-safe
`webSocketFactory`. The client rejects base URLs with userinfo, query strings,
or fragments. Realtime tickets are requested over authenticated HTTP; the
ticket is sent in a protocol frame rather than added to the WebSocket URL.
Ticket endpoints with URL userinfo are rejected too, so a credential cannot be
silently embedded in a browser-visible URL.

By default realtime may connect only to the base URL's corresponding `ws` or
`wss` origin. Add a cross-origin endpoint only through
`allowedRealtimeOrigins`; never infer it from untrusted data. Bounded JSON
decoding, strict versioned envelopes, and bounded typed values protect the SDK
boundary, but server-side collection policy remains the authority for reads,
writes, fields, and aggregates.

## React hydration and live state

Create the query outside render when practical, then pass it to `useLiveQuery`:

```tsx
import type { RemoteLiveQuery } from "@meldbase/client";
import { useLiveQuery } from "@meldbase/react";

function TodoList({ query, initialData }: { query: RemoteLiveQuery<Todo>; initialData: readonly Todo[] }) {
  const { documents, status, error } = useLiveQuery(query, { initialData });
  if (status === "error") return <p>{error?.message}</p>;
  return (
    <ul aria-busy={status !== "live"}>
      {documents.map((todo) => (
        <li key={todo._id}>{todo.title}</li>
      ))}
    </ul>
  );
}
```

For remote queries, supply the same `initialData` on the server and first
client render to avoid hydration drift. Treat `stale` and `resyncing` as live
state transitions, not as permission to write an optimistic cache. The adapter
does not add a cache, mutation layer, or a second query language.

## Worker RPC and read policies

Workers are trusted Node.js processes, not browser code. Connect them only to a
private TLS worker listener with a dedicated credential:

```ts
import { MeldbaseWorker, readPolicy, rpc } from "@meldbase/worker";
import WebSocket from "ws";

const worker = new MeldbaseWorker({
  url: process.env.MELDBASE_WORKER_URL!,
  token: process.env.MELDBASE_WORKER_TOKEN!,
  workerId: "todos-worker",
  webSocketFactory: (url, { headers }) => new WebSocket(url, { headers }),
  methods: {
    "todos.complete": rpc.transactional(async (_context, input, tx) => {
      if (typeof input !== "string") throw new Error("todo ID required");
      await tx.update("todos", input, {
        version: 1,
        operations: [{ op: "set", path: "done", value: true }],
      });
      return null;
    }),
  },
  readPolicies: {
    todos: readPolicy(
      {
        version: "owner-v1",
        maxResults: 100,
        queryPaths: ["done"],
        resultFields: ["owner", "done", "title"],
      },
      ({ actor }) => ({
        version: 1,
        where: { op: "compare", cmp: "eq", path: "owner", value: actor.id },
      }),
    ),
  },
});

await worker.start();
```

Await transactional point operations sequentially. Respect `context.signal`,
use idempotent external effects, and return `MeldbaseError` only for expected
business outcomes. A read policy is an allowlist declaration plus a data-only
constraint; never trust a browser-supplied filter as authorization.

Worker tokens are checked by UTF-8 byte length. The worker accepts only bounded
strict JSON control frames, canonical non-zero document IDs, and the exact
registration limits advertised by the current Worker Hub; a malformed control
frame closes the session before it can affect a handler.

## Supported imports and compatibility

The stable client imports are:

- `@meldbase/client` for remote collections, errors, IDs, pagination result
  types, and client options.
- `@meldbase/client/local` for `LocalCollection`.
- `@meldbase/client/types` for document and query types.
- `@meldbase/react` for `useLiveQuery`.
- `@meldbase/worker` for `MeldbaseWorker`, `rpc`, and `readPolicy`.

Do not import `@meldbase/client/internal` in application code. It is an
unstable implementation bridge for Meldbase packages and is excluded from the
task-oriented API documentation.

The published packages target ES2022, require Node 20+ in Node runtimes, and
are checked as NodeNext and Bundler TypeScript consumers. Browser use requires
a current evergreen browser with `fetch`, `URL`, `TextEncoder`,
`AbortController`, and a WebSocket implementation. See the beta release
checklist for the exact tested Node and browser matrix, source/declaration-map
policy, and the release commands.
