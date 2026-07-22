# Worker SDK guide

`@meldbase/worker` is a trusted Worker SDK, not the Meldbase server. Clients
connect to Go; this package connects to Go's private control endpoint to add
**named application operations** and optional **read-visibility narrowing**.
It is the place for trusted business rules that should not run in a browser.

It is deliberately not a general database client and it is not an accounts or
user-management system. Your application still owns users, passwords, sessions,
roles, and business data. Meldbase authenticates the request, applies its
configured access controls, then gives the worker a small, typed capability for
the operation being handled.

Use the worker when you need one of these three things:

| Need | Helper | Handler receives | Typical use |
| --- | --- | --- | --- |
| A named non-atomic server operation | `rpc` | actor, input, cancellation signal | calculate a quote; call an idempotent internal service |
| A named operation with atomic Meldbase point writes | `rpc.transactional` | actor, input, `WriteTransaction` | create an order; change an owned record's state |
| Per-user visibility for HTTP queries and realtime subscriptions | `readPolicy` | actor, requested query | show only documents owned by the current user |

The worker connects to Meldbase's **private** worker control endpoint. Do not
put its URL or token in a browser bundle. For the corresponding Go hub setup,
protocol frames, and security guarantees, see the
[Worker control protocol](../worker-protocol).

## Start a worker

Install the Worker SDK, the shared query helpers, and a WebSocket implementation
in the trusted Node.js application:

```sh
pnpm add @meldbase/worker @meldbase/client ws
```

This small worker exposes one ordinary RPC. The method name is part of your
application's API, so keep it stable and descriptive.

```ts
import WebSocket from "ws";
import { MeldbaseError, MeldbaseWorker, rpc } from "@meldbase/worker";
import type { Value } from "@meldbase/client";

function requiredString(input: Value): string {
  const value = input;
  if (typeof value !== "string" || value.length === 0) {
    throw new MeldbaseError("orders.invalid_argument");
  }
  return value;
}

const token = process.env.MELDBASE_WORKER_TOKEN;
if (!token) throw new Error("MELDBASE_WORKER_TOKEN is required");

const worker = new MeldbaseWorker({
  url: "meldbase://meldbase-control.internal",
  token,
  workerId: "orders-worker-1",
  webSocketFactory: (url, { headers }) => new WebSocket(url, { headers }),
  methods: {
    "orders.echo": rpc(({ actor }, input) => {
      const message = requiredString(input);
      return {
        message,
        id: actor.id,
        workspaceId: actor.workspaceId,
      };
    }),
  },
  onStateChange: (state) => console.info("Meldbase worker state:", state),
  onError: (error) => console.error("Meldbase worker connection error", error),
});

await worker.start(); // resolves after the first successful registration

process.once("SIGTERM", () => void worker.stop());
process.once("SIGINT", () => void worker.stop());
```

`start()` keeps reconnecting after a later connection loss; it does not mean the
process should exit after the first registration. `stop()` stops that loop and
aborts active handlers. The supplied WebSocket factory sends the worker token in
an authorization header, never in the URL.

The short `meldbase://host[:port]` form is a secure worker authority: the SDK
resolves it to `wss://host[:port]/v1/workers`. It accepts no path, credentials,
query parameters, or fragment. Use a full `wss://` URL when the private control
endpoint has a nonstandard path; reserve `ws://` for local development and
tests.

Input has the shared `Value` type rather than an application-specific schema.
Validate its shape, type, range, and ownership before performing any business
action. A small validation function such as `requiredString` makes the boundary
clear and keeps AI-generated handlers from treating untrusted input as typed data.

## Ordinary RPC: `rpc`

`rpc(handler)` is for a named non-atomic operation. It receives:

- `context.actor.id` — the authenticated application identity;
- `context.actor.workspaceId` — the authenticated active-workspace identifier;
- `context.signal` — aborted when the client call is canceled or the worker
  connection ends;
- `input` — the one typed value sent to the method.

Use it for computation, reads from an application service, or a downstream call
that already has its own idempotency contract. A normal RPC has no Meldbase
transaction: any external effect, and any database write reached outside a
supplied transaction, is outside its atomicity guarantee. Cancellation is
best-effort: pass the signal to cancellable work, but do not assume a canceled
request proves that an external side effect did not happen.

```ts
"orders.quote": rpc(async ({ actor, signal }, input) => {
  const sku = requiredString(input);
  const quote = await pricingService.quote({
    accountID: actor.id,
    workspaceID: actor.workspaceId,
    sku,
    signal,
  });
  return { sku, totalCents: quote.totalCents };
}),
```

To return a deliberate, stable application error to the caller, throw
`MeldbaseError`. Its code must be namespaced lower-case segments (for example
`"orders.invalid_argument"`, `"catalog.unknown_sku"`, or
`"billing.payment_required"`). Its optional second argument is safe structured
data. Any other exception is reported as `internal`; its message and stack stay
in the worker.

```ts
if (!catalog.has(sku)) {
  throw new MeldbaseError("catalog.unknown_sku", { sku });
}
```

## Atomic write RPC: `rpc.transactional`

`rpc.transactional(handler)` runs the handler against a Go-owned optimistic
transaction. It is the normal choice when a method must validate a request and
write business documents as one atomic outcome.

The transaction object provides only sequential point operations:

| Method | Use |
| --- | --- |
| `get(collection, id)` | Read one document from the transaction snapshot. |
| `insert(collection, document)` | Insert a document and return its generated ID. |
| `replace(collection, id, document)` | Fully replace one existing document at a known ID. An absent ID returns `not_found`; this is not an upsert. |
| `update(collection, id, mutation)` | Apply a compiled `$set`/`$inc`/etc. mutation. |
| `delete(collection, id)` | Delete one document at a known ID. |
| `invalidateReadPolicy(collection)` | Force subscriptions to re-authorize after a related visibility change. |

Await every transaction operation before starting the next one. The worker SDK
will reject concurrent transaction operations. Use `compileUpdate` from
`@meldbase/client`; do not hand-build the wire mutation object.

```ts
import { compileUpdate } from "@meldbase/client";
import { MeldbaseError, rpc } from "@meldbase/worker";

const methods = {
  "orders.create": rpc.transactional(async ({ actor }, input, tx) => {
    const description = requiredString(input);

    const id = await tx.insert("orders", {
      workspace: actor.workspaceId,
      owner: actor.id,
      description,
      status: "draft",
      attempts: 0n,
    });

    await tx.update("orders", id, compileUpdate({
      $set: { status: "submitted" },
      $inc: { attempts: 1n },
    }));

    return { id, status: "submitted" };
  }),

  "orders.cancel": rpc.transactional(async ({ actor }, input, tx) => {
    const id = requiredString(input);
    const order = await tx.get("orders", id);

    if (order.owner !== actor.id || order.workspace !== actor.workspaceId) {
      throw new MeldbaseError("orders.not_owner");
    }
    if (order.status !== "submitted") {
      throw new MeldbaseError("orders.invalid_state");
    }

    await tx.update("orders", id, compileUpdate({
      $set: { status: "cancelled" },
    }));
    return { id, status: "cancelled" };
  }),
};
```

Do **not** make network calls, send email, charge a card, or mutate another
database while a `rpc.transactional` handler is running. Only Meldbase point
mutations participate in the atomic commit. If a touched document conflicts,
the operation ends with `rpc_transaction_conflict`; JavaScript is not silently
re-run for you.

## Read visibility: `readPolicy`

`readPolicy(options, handler)` declares how one pre-approved collection may be
read by HTTP queries and realtime subscriptions. It is a read constraint, not a
generic write permission, event publisher, or way to return documents from Node.

The handler returns a predicate built with `compileQuery`, or `null` to deny the
request. It may only narrow what the client requested: sorting, skipping, and
limiting are not valid policy constraints. Meldbase combines the policy with
its local authorizer, so both sides must allow the read.

```ts
import { compileQuery } from "@meldbase/client";
import { readPolicy } from "@meldbase/worker";

const readPolicies = {
  orders: readPolicy({
    // Change this whenever the handler or its static declarations change.
    version: "orders-owner-v1",
    maxResults: 100,
    // Fields a browser query is allowed to filter on.
    queryPaths: ["status", "createdAt"],
    // Fields it may receive through this Worker read policy.
    resultFields: ["workspace", "owner", "description", "status", "createdAt"],
  }, ({ actor }) => {
    if (!actor.id) return null;
    return compileQuery({
      workspace: actor.workspaceId,
      owner: actor.id,
    });
  }),
};
```

`version`, `queryPaths`, `resultFields`, and `maxResults` are security
declarations, not UI hints. Keep them as narrow as the application needs. A
change to the predicate or those declarations must receive a new `version`, so
old authorization and resume state cannot be reused under a different policy.

If visibility for `orders` also depends on a membership record in another
collection, a transactional RPC that changes membership should additionally
call `await tx.invalidateReadPolicy("orders")`. This makes existing `orders`
subscriptions re-authorize. Use it only for that cross-collection visibility
case; ordinary writes to `orders` already flow through the normal reactive
change path. It may be called once per collection in a transaction and must be
paired with a business write.

## Lifecycle and protocol checks

`MeldbaseWorker` has a deliberately small lifecycle API:

| API | Meaning |
| --- | --- |
| `await worker.start()` | Connect and wait for the first successful registration. |
| `await worker.stop()` | Stop reconnecting, close the socket, and abort active handlers. |
| `worker.state` | `idle`, `connecting`, `registering`, `ready`, or `stopped`. |
| `worker.protocol` | The required server capability descriptor. |
| `onStateChange` / `onError` | Integrate state and connection failures into application logs or health checks. |

The SDK requires the worker capability descriptor. A missing descriptor, an
unsupported worker protocol version, or a missing required capability stops the
worker with `MeldbaseWorkerProtocolError`; it never falls back to an ambiguous
legacy control frame. This is a local SDK compatibility error, not a
`MeldbaseError` or `MeldbaseInternalError` result from an application call.

## Before deploying

- Mount the Go worker hub on a private TLS or mutually authenticated listener.
- Store a dedicated worker token as a server secret; it must be at least 32
  bytes and must never be a browser or admin credential.
- Keep business user authentication and roles in the application; use
  `actor` as already-authenticated input, not as a substitute for an
  accounts system.
- When using the built-in collection access manifest, list every browser-callable
  method under `rpcMethods`; omitted means no RPC method is reachable. A custom
  Go `RPCAuthorizer` can express more dynamic admission rules.
- Validate every method argument and authorize every record-level action in the
  handler or Go-side authorizer.
- Prefer a named `rpc.transactional` method for business writes instead of exposing
  broad browser write access.
- Keep Worker read policies narrow, version them when changed, and invalidate
  only when an external collection changes their visibility meaning.

For the complete control-plane contract and Go hub setup, continue to the
[Worker control protocol](../worker-protocol). The generated
[TypeScript API reference](../api/typescript/) is the symbol-level reference.
