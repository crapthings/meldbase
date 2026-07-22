# Server JavaScript SDK and worker protocol

The server JavaScript SDK runs in a separate trusted Node.js worker process. It
does not embed an unrestricted JavaScript runtime inside the Go database
process. Go remains authoritative for authentication, authorization, resource
limits, typed-value validation, idempotency, transaction commit and reactive
publication.

This page specifies the control-plane and security contract. For a
method-by-method introduction with copy-adaptable TypeScript examples, start
with the [server worker SDK guide](guide/server-worker-sdk).

The control protocol is language-neutral so future runtimes can implement the
same contract. The Node package is the first implementation, not a privileged
wire fork.

## Process and trust boundary

Applications mount the worker hub on a private control listener, separate from
the public client API. A worker connects outbound over `wss`, authenticates with
a dedicated worker credential and registers a bounded set of method names and
modes. Browser-originated control connections are rejected. Client bearer
tokens, idempotency keys and transport request IDs are never forwarded to a
worker; it receives only the authenticated actor (`id`, `workspaceId`), method
arguments or canonical requested query, and a hub-generated call ID.

One live worker owns a method name or managed publication collection. Registration conflicts fail closed instead
of load-balancing nondeterministically. Disconnect atomically removes all of the
worker's registrations and fails its in-flight calls. A later worker may then
register the names. Routing always occurs after the normal client
`RPCAuthorizer` check.

## Version-one worker frames

All frames are strict bounded JSON objects and use Meldbase's existing typed
wire values.

The Node SDK requires the v1 capability descriptor with the private upgrade
header `Meldbase-Protocol: capabilities-v1`. The Hub includes `protocol` in
every `registered` frame; an older Hub or an omitted descriptor is rejected:

```json
{
  "v": 1,
  "type": "registered",
  "sessionId": "opaque-session",
  "limits": {},
  "protocol": {
    "versions": [1],
    "capabilities": [
      "cancel",
      "publication.policy",
      "rpc",
      "rpc.transactional",
      "transaction.compiled_update",
      "transaction.invalidate_publication",
      "transaction.point_operations"
    ]
  }
}
```

The descriptor uses the same canonical bounded decoder as the browser SDK.
Unknown sorted capabilities are forward-additive. The SDK verifies every
capability required by its registered method/publication modes before becoming
ready. A missing descriptor, wrong worker protocol version, or missing required
support is terminal rather than a reconnect loop. The request header is confined
to the separately authenticated, non-browser control listener and contains no
worker token or application identity.

Worker registration:

```json
{
  "v": 1,
  "type": "register",
  "workerId": "orders-worker-1",
  "methods": [
    {"name": "orders.quote", "mode": "rpc"},
    {"name": "orders.create", "mode": "transactional"}
  ],
  "publications": [
    {
      "collection": "orders",
      "version": "orders-owner-v1",
      "maxResults": 100,
      "queryPaths": ["status", "description"],
      "resultFields": ["owner", "description", "status"]
    }
  ]
}
```

The hub replies with `registered` and an opaque process-session ID. The hub then
sends `invoke` frames containing `callId`, method, mode, the authenticated
identity and typed arguments. The SDK exposes that identity to handlers as
`context.actor` (`id`, `workspaceId`). A worker answers with exactly one `result`
or stable coded `error`.
Arbitrary exception text and stack traces never cross back to clients.

`invoke` and `authorize_query` use an exact `actor` object. There is no
`principal`, `subject`, or `workspace` fallback field in worker protocol v1:

```json
{
  "v": 1,
  "type": "invoke",
  "callId": "hub-generated-id",
  "method": "orders.quote",
  "mode": "rpc",
  "actor": {"id": "user_42", "workspaceId": "team_a"},
  "arguments": []
}
```

Cancellation is best-effort. The hub sends `cancel` when the client context is
done. An ordinary remote method has the same external-side-effect uncertainty as
a local ordinary Go RPC method. It should use the supplied abort signal but must
still design its own downstream idempotency.

## Transactional calls

For `transactional` mode, the Go handler opens the same optimistic
`WriteTransaction` used by local transactional methods. The worker may issue one
point operation at a time:

- `get(collection, id)`;
- `insert(collection, document)`;
- `replace(collection, id, document)` — fully replaces an existing known ID;
  an absent ID returns `not_found` and is never created implicitly;
- `update(collection, id, compiledMutation)`;
- `delete(collection, id)`;
- `invalidatePublication(collection)`.

Each `tx_op` carries a call-local `opId`; the hub returns one typed `tx_result`
or stable `tx_error`. Operations are executed against the fixed snapshot and
isolated overlay in Go. A successful worker result triggers the existing atomic
business/result publication. A write to a point read or touched by the worker
returns the durable `rpc_transaction_conflict` terminal; disjoint commits remain
concurrent, and JavaScript is never reinvoked. Point entries and retained
base/current document values are bounded during execution by the Go-owned
transaction resource limits.
`compiledMutation` is produced by the client package's shared `compileUpdate`
and is decoded by Go under the same bounded, data-only mutation grammar used by
HTTP and local TypeScript collections.

`invalidatePublication` is for the narrower case where a publication's
authorization meaning depends on data outside the collection it queries. For
example, a transaction that changes an `organization_members` record can
invalidate the `orders` publication. It stages a new random policy generation
in the private System tree beside the business mutations and RPC terminal.
The old policy lease is revoked only after that root is durable and before the
business ChangeBatch is emitted, so existing subscriptions resync instead of
continuing under stale visibility. A conflict, handler error or failed commit
publishes neither the generation nor the lease rotation. The operation may be
issued at most once per publication in one transaction and must accompany at
least one business mutation; it is not needed for ordinary changes to documents
already covered by the publication query. An invalidation-only call completes
durably with `rpc_transaction_requires_write`; it is never reported as an
ambiguous outcome.

Transactional worker methods must not perform network calls, normal database
writes or external side effects. The worker disconnecting, throwing, violating
the protocol or being canceled discards its staged overlay. The transaction is
not a distributed transaction: only Meldbase point mutations participate in the
atomic commit.

## Backpressure and observability

Registration count, frame bytes, in-flight calls and point operations are
bounded. Each worker has a fixed pending-call budget; overload fails before an
`invoke` is sent. Writes are deadline-bound and serialized per control socket.
Unknown, duplicate or malformed terminal/operation frames close the worker
connection.

The hub exports fixed-cardinality totals and gauges only: connected workers,
registered methods/publications, active calls/policy evaluations, their bounded
outcomes, protocol failures, bytes and transactional operations. It never labels metrics
with worker IDs, method names, actors, workspaces or application error codes.
Committed policy invalidations have their own total, making unexpected resync
pressure visible without putting collection names into metrics.

## Data-only publications

Publication ownership is a Go-side trust anchor. `PublicationCollections` must
list every collection whose query visibility is delegated. A Worker cannot add
a new authority domain by registering an arbitrary name. A managed collection
with no connected owner fails closed; collections not listed continue through
the local `Authorizer` alone.

A publication is a read-visibility extension only: it can narrow HTTP queries
and realtime subscriptions, but cannot authorize generic inserts, updates, or
deletes. Put role-dependent writes in a Go `Authorizer` or an explicitly
authorized RPC method.

The registration contains the static maximum result count, client-query paths
and projected result fields. Those declarations are hashed into the policy
version. Per query or subscription, Go sends one `authorize_query` containing
the authenticated actor and canonical requested query. The handler returns
only a predicate-only query AST in `policy`, or `policy_error/forbidden`. Sort,
skip and limit are forbidden in constraints. The worker never reads storage and
never returns documents.

Go intersects the returned policy with the local `Authorizer`: row constraints
are ANDed, path/field sets are intersected and the smaller result cap wins. It
then executes the effective query, projects fields and maintains the
connection-local visibility overlay. A policy function runs once when an HTTP
query or subscription is authorized, not on each database commit. Evaluation
has an independent two-second default deadline (configurable up to 30 seconds)
and shares the worker's pending-call budget, so a slow policy cannot accumulate
unbounded subscription starts.

Every registered publication owns a `QueryPolicyLease`. Disconnect first marks
that lease revoked and removes the owner. Existing subscriptions stop before
any new authorized output and request a safe resync; new requests fail closed
until a worker registers again. Effective-query and policy fingerprints bind
resume tokens, so changed constraints or static visibility declarations cannot
reuse an older authorization result.

The policy version also includes a durable per-collection generation. Worker
registration loads that generation before taking ownership, so a restart cannot
forget a previously committed invalidation. The generation store must be the
same durable database used by transactional RPC.

## Current setup

The Go process creates one hub and mounts it on a private listener. The same hub
is passed to both resolver fields when transactional worker methods are enabled:

```go
workerAuth, err := server.NewWorkerTokenAuthenticator(os.Getenv("MELDBASE_WORKER_TOKEN"))
if err != nil { log.Fatal(err) }
policyGenerations, err := server.NewDurablePolicyGenerationStore(db)
if err != nil { log.Fatal(err) }
workerHub, err := server.NewWorkerHub(server.WorkerHubConfig{
    Authenticator: workerAuth,
    PublicationCollections: []string{"orders"},
    PolicyGenerationStore: policyGenerations,
})
if err != nil { log.Fatal(err) }

idempotency, err := server.NewDurableRPCIdempotencyStore(db)
if err != nil { log.Fatal(err) }

api, err := server.New(server.Config{
    DB: db,
    RPCMethodResolver: workerHub,
    RPCTransactionalMethodResolver: workerHub,
    QueryPolicyResolver: workerHub,
    RPCIdempotencyStore: idempotency,
    // Client authenticator, data authorizer, RPC authorizer and transport keys...
})

controlMux := http.NewServeMux()
controlMux.Handle("GET /v1/workers", workerHub)
// Serve controlMux only on a private or mutually authenticated listener.
```

The Node worker supplies a WebSocket factory capable of setting an
`Authorization` header. The SDK never places the credential in the URL:

```ts
import WebSocket from "ws";
import { compileQuery } from "@meldbase/client";
import { MeldbaseWorker, publish, rpc, transactional } from "@meldbase/server";

const worker = new MeldbaseWorker({
  url: "wss://meldbase-control.internal/v1/workers",
  token: process.env.MELDBASE_WORKER_TOKEN!,
  workerId: "orders-worker-1",
  webSocketFactory: (url, { headers }) => new WebSocket(url, { headers }),
  methods: {
    "orders.quote": rpc(async ({ actor, signal }, [orderId]) => {
      return calculateQuote(actor, orderId, signal);
    }),
    "orders.create": transactional(async ({ actor }, [description], tx) => {
      const id = await tx.insert("orders", { owner: actor.id, description });
      const order = await tx.get("orders", id);
      return order;
    }),
  },
 publications: {
    orders: publish({
      version: "orders-owner-v1",
      maxResults: 100,
      queryPaths: ["status", "description"],
      resultFields: ["owner", "description", "status"],
    }, ({ actor }) => compileQuery({ owner: actor.id })),
  },
});

await worker.start(); // resolves after registration; reconnect continues internally
```

`MeldbaseError` is the only way to expose a stable application error. Its code
is a namespaced identifier such as `orders.already_paid`, and it may include
safe structured data. Other exceptions become internal failures; their messages
and stacks stay inside the worker process. `stop()` aborts active handlers and
ends the reconnect loop.
