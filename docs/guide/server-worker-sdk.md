# Server worker SDK guide

`@meldbase/server` lets a trusted Node.js process add **named application
operations** and **read-visibility policies** to a Meldbase server. It is the
place for server-side business rules that should not run in a browser.

It is deliberately not a general database client and it is not an accounts or
user-management system. Your application still owns users, passwords, sessions,
roles, and business data. Meldbase authenticates the request, applies its
configured access controls, then gives the worker a small, typed capability for
the operation being handled.

Use the worker when you need one of these three things:

| Need | Helper | Handler receives | Typical use |
| --- | --- | --- | --- |
| A named server operation without an atomic database write | `rpc` | principal, arguments, cancellation signal | calculate a quote; call an idempotent internal service |
| A named operation with atomic Meldbase point writes | `transactional` | principal, arguments, `WriteTransaction` | create an order; change an owned record's state |
| Per-user visibility for HTTP queries and realtime subscriptions | `publish` | principal, requested query | show only documents owned by the current user |

The worker connects to Meldbase's **private** worker control endpoint. Do not
put its URL or token in a browser bundle. For the corresponding Go hub setup,
protocol frames, and security guarantees, see the
[server worker protocol](../server-js-sdk).

## Start a worker

Install the server SDK, the shared query helpers, and a WebSocket implementation
in the trusted Node.js application:

```sh
pnpm add @meldbase/server @meldbase/client ws
```

This small worker exposes one ordinary RPC. The method name is part of your
application's API, so keep it stable and descriptive.

```ts
import WebSocket from "ws";
import { MeldbaseMethodError, MeldbaseWorker, rpc } from "@meldbase/server";
import type { Value } from "@meldbase/client";

function requiredString(arguments_: readonly Value[], index: number): string {
  const value = arguments_[index];
  if (typeof value !== "string" || value.length === 0) {
    throw new MeldbaseMethodError("invalid_argument");
  }
  return value;
}

const token = process.env.MELDBASE_WORKER_TOKEN;
if (!token) throw new Error("MELDBASE_WORKER_TOKEN is required");

const worker = new MeldbaseWorker({
  url: "wss://meldbase-control.internal/v1/workers",
  token,
  workerId: "orders-worker-1",
  webSocketFactory: (url, { headers }) => new WebSocket(url, { headers }),
  methods: {
    "orders.echo": rpc(({ principal }, arguments_) => {
      const message = requiredString(arguments_, 0);
      return {
        message,
        subject: principal.subject,
        tenant: principal.tenant,
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

Arguments have the shared `Value` type rather than an application-specific
schema. Validate their number, type, range, and ownership before performing any
business action. A small validation function such as `requiredString` makes the
boundary clear and keeps AI-generated handlers from treating untrusted input as
typed data.

## Ordinary RPC: `rpc`

`rpc(handler)` is for a named operation that does not need Meldbase's atomic
write transaction. It receives:

- `context.principal.subject` — the authenticated application identity;
- `context.principal.tenant` — the authenticated tenant/workspace identifier;
- `context.signal` — aborted when the client call is canceled or the worker
  connection ends;
- `arguments_` — the typed values sent to the method.

Use it for computation, reads from an application service, or a downstream call
that already has its own idempotency contract. Cancellation is best-effort: pass
the signal to cancellable work, but do not assume a canceled request proves that
an external side effect did not happen.

```ts
"orders.quote": rpc(async ({ principal, signal }, arguments_) => {
  const sku = requiredString(arguments_, 0);
  const quote = await pricingService.quote({
    accountID: principal.subject,
    tenantID: principal.tenant,
    sku,
    signal,
  });
  return { sku, totalCents: quote.totalCents };
}),
```

To return a deliberate, stable application error to the caller, throw
`MeldbaseMethodError`. Its code must be lower-case snake case (for example
`"invalid_argument"`, `"not_found"`, or `"payment_required"`). Any other
exception is reported as `internal`; its message and stack stay in the worker.

```ts
if (!catalog.has(sku)) {
  throw new MeldbaseMethodError("unknown_sku");
}
```

## Atomic write RPC: `transactional`

`transactional(handler)` runs the handler against a Go-owned optimistic
transaction. It is the normal choice when a method must validate a request and
write business documents as one atomic outcome.

The transaction object provides only sequential point operations:

| Method | Use |
| --- | --- |
| `get(collection, id)` | Read one document from the transaction snapshot. |
| `insert(collection, document)` | Insert a document and return its generated ID. |
| `replace(collection, id, document)` | Replace one document at a known ID. |
| `update(collection, id, mutation)` | Apply a compiled `$set`/`$inc`/etc. mutation. |
| `delete(collection, id)` | Delete one document at a known ID. |
| `invalidatePublication(collection)` | Force subscriptions to re-authorize after a related visibility change. |

Await every transaction operation before starting the next one. The worker SDK
will reject concurrent transaction operations. Use `compileUpdate` from
`@meldbase/client`; do not hand-build the wire mutation object.

```ts
import { compileUpdate } from "@meldbase/client";
import { MeldbaseMethodError, transactional } from "@meldbase/server";

const methods = {
  "orders.create": transactional(async ({ principal }, arguments_, tx) => {
    const description = requiredString(arguments_, 0);

    const id = await tx.insert("orders", {
      tenant: principal.tenant,
      owner: principal.subject,
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

  "orders.cancel": transactional(async ({ principal }, arguments_, tx) => {
    const id = requiredString(arguments_, 0);
    const order = await tx.get("orders", id);

    if (order.owner !== principal.subject || order.tenant !== principal.tenant) {
      throw new MeldbaseMethodError("forbidden");
    }
    if (order.status !== "submitted") {
      throw new MeldbaseMethodError("invalid_order_state");
    }

    await tx.update("orders", id, compileUpdate({
      $set: { status: "cancelled" },
    }));
    return { id, status: "cancelled" };
  }),
};
```

Do **not** make network calls, send email, charge a card, or mutate another
database while a `transactional` handler is running. Only Meldbase point
mutations participate in the atomic commit. If a touched document conflicts,
the operation ends with `rpc_transaction_conflict`; JavaScript is not silently
re-run for you.

## Read visibility: `publish`

`publish(options, handler)` defines how a collection can be read by HTTP
queries and realtime subscriptions. It is a **read filter**, not a generic
write permission and not a way to return documents from Node.

The handler returns a predicate built with `compileQuery`, or `null` to deny the
request. It may only narrow what the client requested: sorting, skipping, and
limiting are not valid policy constraints. Meldbase combines the policy with
its local authorizer, so both sides must allow the read.

```ts
import { compileQuery } from "@meldbase/client";
import { publish } from "@meldbase/server";

const publications = {
  orders: publish({
    // Change this whenever the handler or its static declarations change.
    version: "orders-owner-v1",
    maxResults: 100,
    // Fields a browser query is allowed to filter on.
    queryPaths: ["status", "createdAt"],
    // Fields it may receive from this publication.
    resultFields: ["tenant", "owner", "description", "status", "createdAt"],
  }, ({ principal }) => {
    if (!principal.subject) return null;
    return compileQuery({
      tenant: principal.tenant,
      owner: principal.subject,
    });
  }),
};
```

`version`, `queryPaths`, `resultFields`, and `maxResults` are security
declarations, not UI hints. Keep them as narrow as the application needs. A
change to the predicate or those declarations must receive a new `version`, so
old authorization and resume state cannot be reused under a different policy.

If visibility for `orders` also depends on a membership record in another
collection, a transactional method that changes membership should additionally
call `await tx.invalidatePublication("orders")`. This makes existing `orders`
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
| `worker.protocol` | The server capability descriptor, when the server provided one. |
| `onStateChange` / `onError` | Integrate state and connection failures into application logs or health checks. |

By default, the SDK accepts a server that does not provide a capability
descriptor, which helps a controlled server rollout. Set `requireProtocol: true`
when you want a missing descriptor to stop the worker instead of accepting that
downgrade. A required capability that is absent always stops the worker with
`MeldbaseWorkerProtocolError`.

## Before deploying

- Mount the Go worker hub on a private TLS or mutually authenticated listener.
- Store a dedicated worker token as a server secret; it must be at least 32
  bytes and must never be a browser or admin credential.
- Keep business user authentication and roles in the application; use
  `principal` as already-authenticated input, not as a substitute for an
  accounts system.
- Validate every method argument and authorize every record-level action in the
  handler or Go-side authorizer.
- Prefer a named `transactional` method for business writes instead of exposing
  broad browser write access.
- Keep publication policies narrow, version them when changed, and invalidate
  only when an external collection changes their visibility meaning.

For the complete control-plane contract and Go setup, continue to the
[server worker protocol](../server-js-sdk). The generated
[TypeScript API reference](../api/typescript/) is the symbol-level reference.
