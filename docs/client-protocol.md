# Client and isomorphic query protocol

## Goal

The TypeScript SDK is both a local reactive collection and a client for the
durable server. A public filter is compiled once into versioned `QuerySpec` data.
The same AST is evaluated locally and sent to the server. A shared conformance
corpus will be run by TypeScript and Go so semantic drift fails CI.

JavaScript `bigint` maps to Meldbase Int64 and is encoded as a bounded decimal
string. JavaScript `number` maps to Float64. This deliberately avoids JSON's
silent loss of 64-bit integer precision while retaining natural JS syntax.
JavaScript exposes document IDs as 32-character lowercase hexadecimal strings,
but document and `_id` query encoding tags them as the distinct wire `id` type.

This resembles Minimongo's useful capability—client-side collections and shared
query vocabulary—but Meldbase owns the AST, behavior, synchronization model, and
security boundary.

## Local and remote collection boundary

`LocalCollection` and `RemoteCollection` deliberately share the data-only
filter, sort, pagination, and mutation grammar. They are not interchangeable
database handles and do not promise method-for-method parity or automatic
synchronization. The root `@meldbase/client` entry is remote-first; import
`LocalCollection` explicitly from `@meldbase/client/local` for application-owned
in-memory state.

| Surface | Purpose | Deliberate boundary |
| --- | --- | --- |
| `LocalCollection` | In-memory application state, tests, and local reactive views. | Its synchronous `upsert(document)` creates or fully replaces the one document addressed by its canonical `_id`. It makes no network request and applies no server policy. |
| `RemoteCollection` | Authenticated HTTP/realtime access to a Go server. | It exposes `insertOne`, bounded `updateOne`/`updateMany`, and bounded `deleteOne`/`deleteMany`; every request is re-authorized and server-limited. It intentionally has no generic public `replace` or `upsert` operation. |
| `RemoteCollection.count` / `groupCount` | Policy-aware dashboard summaries. | They exist only remotely because `capped` describes server visibility and result budgets. A local exact count would have different authority and must not be mistaken for it. |

For a server-owned full replacement inside an atomic business operation, use a
named `transactional` RPC and its worker `tx.replace(...)` capability. Do not
emulate it in a browser by reading, changing, and writing a whole document:
that loses the server's record-level authorization and optimistic transaction
boundary.

The names encode cardinality deliberately. `updateOne` and `deleteOne` stop
after one filter match; their `Many` variants affect every permitted match.
`insertOne` is strict creation. Local `upsert(document)` has no `One` suffix
because its canonical `_id` already selects exactly one document. There is no
`upsertOne` or `upsertMany`: a filter miss must not silently create a document.
The worker's `tx.replace(...)` remains a different, strict operation: it
replaces an existing known ID and returns `not_found` otherwise.

## Trust boundary

The client is always untrusted. Compiling locally improves feedback but grants no
authority. The server must decode and compile the AST again with stricter limits.
It must reject unknown versions/operators/fields and must never evaluate source
text, JavaScript, regex with unbounded execution, or user callbacks.

For every implemented fetch and subscription, the server applies:

1. authenticated actor and tenant scoping;
2. collection/action policy;
3. row predicate injected into the plan (not post-filtered after pagination);
4. field projection/redaction before serialization;
5. query depth/node/result limits plus separately configured HTTP-result and
   realtime-frame byte limits;
6. subscription-count, outbound-frame, and outbound-byte queue limits.

The database also admits each shared reactive view by its full matching-member
count and canonical document bytes. A query `limit` bounds the emitted window,
not the internal matching set needed for stable incremental ordering. Exceeding
that database resource limit returns the fixed `resource_limit_exceeded` error;
the client must narrow the query or use pagination rather than rely on a large
unbounded subscription.

Authorizer results are frozen at this boundary: allowed-path/field maps,
constraints, and server-owned insert fields are copied into server-owned state.
Changing an application-side policy object later cannot mutate a live
projection or bypass the policy lease.

Insert authorization separately validates client-writable fields, then applies
server-owned fields such as tenant and owner after validation. Client values can
never override those fields.

The built-in workspace authorizer is the standard multi-tenant policy: the
authenticated JWT contributes the active `workspace_id`, while a configured
collection stores a server-owned `workspaceId` field. The handler injects its
equality constraint into every read and subscription before pagination, sets it
on insert, and denies any update path at or below that field. A browser SDK may
therefore query a collection normally; it must not be given a separate,
client-chosen tenant parameter. Switching workspaces means obtaining a new JWT.

Update and delete authorization recompile the data-only mutation on the server,
apply the server-owned row predicate, validate writable paths, and enforce a
separate `MaxAffected` bound. That bound is checked while the engine holds its
write lock; exceeding it rejects the entire mutation without a partial write.

Resume tokens are opaque, expiring HMAC-authenticated values bound to database
identity, authenticated actor ID and tenant ID, collection, the canonical authorized
query, policy version, and durable commit position. A token whose history is no
longer available, whose security context changed, or whose signature is invalid
yields `resync_required`. A configured replay source reconstructs the exact
historical query view and tails N+1 without a registration gap. Without such a
source the server explicitly requests a clean resync; it never labels a fresh
snapshot as historical replay. Resume tokens are continuity capabilities, never
authorization credentials: authentication and policy evaluation always run
again.

On successful delta-mode resume, the server first sends a `resumed` control
frame containing the request ID, a new server subscription ID, and the exact
opaque token presented by the client. This rebinds the existing client-side
documents to the new connection without invoking listeners or transmitting a
full snapshot. Later deltas use that opaque token as `fromToken`. Retention loss,
replay corruption, policy revocation, and a replay source that cannot serve the
requested position produce `resync_required`; the client discards its old state
token and subscribes again without one. Database fail-stop/closure is different:
it produces the terminal structured `database_unavailable` error instead of a
futile resync loop.

Long-lived query policies may carry a revocable lease bound to their policy
version. Revocation linearizes against response generation: it prevents new
authorized output, waits for any response/delta currently being encoded or
enqueued, and then causes each affected subscription to emit
`resync_required`. The client discards its old token and subscribes again, which
re-enters the Authorizer and receives a newly constrained query/projection. Data
already materialized as an HTTP response or placed in the WebSocket outbound
queue before revocation is treated as authorized in flight; it is not
retroactively recallable.

Production servers should configure a stable, randomly generated resume-token
key (at least 32 bytes) across process restarts. An omitted key is generated in
memory and is safe, but deliberately turns all pre-restart tokens into clean
resyncs. Key rotation can use the same resync behavior until multi-key validation
is introduced.

## Service probes

The public Go handler exposes unauthenticated, fixed-schema operational probes:

- `GET /livez` returns 200 while the handler can respond and does not inspect the
  database;
- `GET /readyz` returns 200 only while the database is both readable and writable;
- `GET /health` is the compatibility alias for readiness.

A fail-stop durability error preserves reads from the last committed state, so
readiness reports `readable: true`, `writable: false` and HTTP 503. A closed DB
reports both false, while liveness remains 200. Responses contain only schema
version 1, a fixed `live|ready|not_ready` status and those booleans. They are
`no-store`, carry no error string, path, engine, identity, sequence, tenant or
user data, and remain subject to the server's exact Origin policy. Liveness must
not be used as a readiness probe.

## Availability errors and retry boundary

Engine closure and durability fail-stop are exposed over data HTTP endpoints as
HTTP 503 with the fixed non-sensitive body
`{"error":{"kind":"internal","code":"database_unavailable"}}`. Initial and running realtime
subscriptions use the matching strict error envelope:

```json
{"v":1,"type":"error","requestId":"client-request-id","error":{"kind":"internal","code":"database_unavailable"}}
```

The TypeScript SDK exposes this as `MeldbaseInternalError`, preserving `code`,
HTTP `status` (zero for WebSocket) and operation. It does not infer that a
mutation is safe to retry. Callers may retry only under an application-level
idempotency contract after readiness recovers. Reads may be retried after
recovery.

The TypeScript remote client assigns a random 128-bit `_id` before an insert is
sent when the caller omitted one, and requires the successful response to carry
that exact ID. If the transport fails after dispatch, or a successful response
cannot be verified, it throws `MeldbaseInternalError` with code
`outcome_unknown`; its operation identifies the insert and the original error is
available as `cause`. Reconcile the exact document ID with an authorized point
query instead of submitting a duplicate. Non-SDK clients should likewise supply
their own stable document ID whenever they need this recovery property.

Database resource admission failures use HTTP 413 and the fixed
`resource_limit_exceeded` code. This is a terminal rejection: no document,
index, Commit Log entry or reactive event was published. The client should
reduce the document or transaction rather than retry the identical operation.

RPC uses the same `database_unavailable` code when the database rejects work
before a safe terminal is available. Once an idempotent/transactional RPC may
have executed but its terminal publication cannot be proven, the stronger
`rpc_outcome_unknown` result takes precedence. The client never automatically
retries either condition, and raw engine or filesystem errors never cross the
transport boundary.

## Bounded HTTP response decoding

The TypeScript client applies `maxInboundBytes` while reading query, insert and
HTTP RPC response streams, not after calling `Response.text()` or
`Response.json()`. It rejects an oversized declared `Content-Length` before
reading the body and otherwise counts bytes chunk by chunk, cancelling the
reader immediately when the limit is crossed. This also bounds chunked
responses and representations whose decompressed size is larger than the wire
declaration. Mutation and realtime-ticket responses use a narrower 64 KiB
ceiling.

Successful HTTP data responses are strict versioned envelopes: query requires
exactly `version` and `documents`, insert requires `version` and `document`, and
mutation requires `version` plus its fixed count fields. `version` must equal
`1`; unknown, missing or additional fields fail closed. Ticket responses accept
only `url`, `ticket`, and the optional negotiated `protocol` descriptor, with
bounded non-empty URL and ticket strings. The byte stream must be valid UTF-8
before JSON parsing. These checks protect SDK consumers from a faulty proxy,
misconfigured endpoint or non-conforming server before wire values are decoded.

## Realtime state machine

```text
idle -> authenticating -> connecting -> live -> stale -> resyncing
  ^          |               |              |           |
  +----------+---------------+--------------+-----------+
                         retry / closed
```

Browsers obtain a short-lived, single-use realtime ticket over authenticated
HTTPS. Credentials are not placed in WebSocket URLs. Each connection has an
epoch; messages from an older socket are ignored. Each subscription has a client
request ID, server subscription ID, last commit token, and local revision.

The SDK accepts realtime URLs only from the configured origin allowlist and
requires `wss` when the HTTP origin uses HTTPS. Protocol violations are fatal for
that connection rather than automatically retried forever.

HTTP CORS and WebSocket origins are separate policies. HTTP requests with an
`Origin` header must match an exact configured http(s) origin; preflight accepts
only `GET`/`POST` and the `Authorization`/`Content-Type` headers. WebSocket
upgrades bypass that HTTP CORS check and use their own host-pattern validation.
This lets a deployment keep ticket-issuing HTTP endpoints on an exact allowlist
while configuring a distinct realtime-origin boundary. CORS is a browser
boundary, not a replacement for authentication or row/field authorization.
Realtime patterns may match a host or a full scheme+host; use a full
scheme+host when the browser scheme must be pinned. Once configured, the
realtime patterns are a strict allowlist even when the Origin host equals the
WebSocket request host.

The remote SDK requests delta mode. The server first delivers one atomic
`snapshot`, followed by strictly ordered `delta` frames. The SDK applies those
frames to private ordered state and continues to expose immutable-looking full
snapshots to the native `LiveQuery` callback and React `useLiveQuery` store.
Callbacks run from a microtask outside socket parsing and receive deep clones, so
listener mutation cannot corrupt synchronization state.

Before a query snapshot or delta accumulates its wire documents, the server
enforces the applicable result/frame byte budget. An oversized result receives
the terminal `resource_limit_exceeded` error for that request/subscription and
does not enter the socket queue. The server then serializes every remaining
outbound realtime frame before it enters that queue. A connection whose
queued/in-flight frames exceed its configured byte budget is closed rather than
retaining an unbounded backlog. This is a transport slow-consumer boundary: the
database write remains committed and the client reconnects from its last opaque
token. The client validates exact delta operation shapes, IDs, anchors, subscription
identity and the opaque token chain. It also enforces inbound byte, snapshot
document, delta operation, and resulting document limits. A malformed or
out-of-order frame closes the connection as a protocol failure; the client never
tries to repair an ambiguous partial application. The current remote SDK does
not yet maintain an offline mutation cache or optimistic conflict-resolution
layer.

## Wire messages

Implemented socket client messages: `authenticate`, `subscribe`, `unsubscribe`,
`call`, `cancel`, `ping`. Implemented server messages: `authenticated`,
`snapshot`, `resumed`, `delta`, `result`, `error`, `resync_required`, `pong`.

All envelopes include protocol version `v: 1`. Subscriptions use
`mode: "delta"` for an initial snapshot followed by ordered transformations.
The checked-in `testdata/protocol-v1-contract.json` artifact freezes the ticket
media type, base and conditional capabilities, all client and server frame
names, their required/optional top-level fields, and the nested delta/error
shapes. Its `workerProtocol` section freezes the private control descriptor,
capabilities, frames, and nested `actor`/`error` shapes. The artifact also
freezes the sorted engine/transport-owned error-code registry; namespaced
application-defined `MeldbaseError` codes remain explicit extensions rather
than masquerading as engine codes. Both the Go server and TypeScript SDK read it in their compatibility
suites. The public Go `server.ProtocolVersion` and
TypeScript `MELDBASE_PROTOCOL_VERSION` constants must match it, and production
encoders use those constants rather than independent numeric literals.

Protocol v1 frame grammar is immutable. Adding required fields, changing field
meaning or type, or removing a frame requires a new protocol version and a new
contract artifact.

### Version and capability discovery

The authenticated ticket endpoint is also the compatibility envelope for the
socket opened with that single-use ticket. A client opts in without changing
the request body by sending:

```http
Accept: application/vnd.meldbase.realtime-ticket+json; capabilities=1
```

The response then adds a bounded descriptor:

```json
{
  "url": "wss://db.example/v1/realtime",
  "ticket": "single-use-secret",
  "protocol": {
    "versions": [1],
    "capabilities": ["query.delta", "query.resume", "rpc", "rpc.cancel"]
  }
}
```

Versions are strictly increasing positive integers. Capabilities are sorted,
unique, lowercase fixed-schema names; both arrays have hard decoder bounds.
Unknown capabilities are additive and ignored. A known version or a capability
required by current work may never be inferred from an unknown name.

`query.delta`, `rpc`, and `rpc.cancel` describe their corresponding v1 frames.
`query.resume` means the client may present its previous opaque token; when it
is absent the TypeScript SDK deliberately reconnects with a clean snapshot.
`rpc.idempotency` is advertised only when a store is configured, and
`rpc.transactional` only when a transactional registry or resolver exists.

The current SDK requires the protocol descriptor. A malformed descriptor,
unsupported version, or missing required capability is terminal and does not
enter reconnect backoff. Capability discovery is authenticated and contains no method,
collection, actor, tenant or database identity.

Delta mode always begins with:

```json
{
  "v": 1,
  "type": "snapshot",
  "requestId": "client-request-id",
  "subscriptionId": "server-subscription-id",
  "token": "opaque-token-1",
  "documents": []
}
```

Later visible revisions use:

```json
{
  "v": 1,
  "type": "delta",
  "requestId": "client-request-id",
  "subscriptionId": "server-subscription-id",
  "fromToken": "opaque-token-1",
  "token": "opaque-token-2",
  "operations": []
}
```

Operations are applied in array order:

- `{"op":"remove","id":ID}` removes an existing document;
- `{"op":"add_before","id":ID,"before":ID?,"document":DOC}` inserts a new
  document before an existing anchor, or at the end when `before` is absent;
- `{"op":"move_before","id":ID,"before":ID?}` moves an existing document;
- `{"op":"change","id":ID,"document":DOC}` replaces an existing document.

IDs are canonical non-zero 32-character lowercase hexadecimal document IDs.
Documents on add/change must contain the same `_id`. Unknown fields, missing
targets, duplicate adds, invalid anchors, no-op moves, empty operation arrays,
and a `fromToken` different from the last client-visible token are protocol
errors.

Authorization is evaluated before subscribing. A connection-local visibility
overlay applies field projection/redaction before wire encoding and suppresses
changes that affect only hidden fields without changing visible membership or
order. Such revisions do not advance the client-visible token; the next visible
delta chains from the last token actually sent to that client. This prevents
redacted values and internal commit gaps from leaking through delta contents.
Membership and ordering changes remain observable because they are part of the
authorized query result.

For dynamic authorization, deployments attach a shared `QueryPolicyLease` to
the returned policy and revoke it when the backing authorization version
changes. A policy without a lease is explicitly static for the lifetime of that
request/subscription; the server does not poll the Authorizer on every commit.
The server JavaScript SDK can supply an additional data-only policy for
Go-predeclared collections. Its constraint, path set, projection and cap are
intersected with the local Authorizer; Worker disconnect revokes its lease and
forces a safe resync instead of falling back to the local policy.

## RPC calls

Application methods use the same closed typed-value codec rather than inventing
a second JSON type system. The TypeScript API is intentionally close to
`Meteor.call`, but returns a Promise and supports `AbortSignal`:

```ts
const result = await client.call("billing.calculate", [accountId, 10n], {
  signal: controller.signal,
});

const realtimeResult = await client.call("billing.calculate", [accountId, 10n], {
  transport: "realtime",
  signal: controller.signal,
});

// Explicit caller retry identity; the SDK still never retries automatically.
const receipt = await client.call("orders.checkout", [orderId], {
  idempotencyKey: crypto.randomUUID(),
});
```

The default transport used by TypeScript `client.call` is authenticated HTTP
`POST /v1/rpc`. This is the baseline for work that does not need a live
connection and does not open a WebSocket. Setting `transport: "realtime"`
multiplexes the same call over the ticket-authenticated subscription connection.
Concurrent calls share an in-progress connection attempt; calls and subscriptions
added after authentication are sent immediately on that socket. A call-only
socket closes when its last call settles. Both transports use the same envelope:

```json
{"v":1,"type":"call","requestId":"uuid","method":"billing.calculate","arguments":[{"t":"int64","v":"10"}]}
{"v":1,"type":"result","requestId":"uuid","result":{"t":"string","v":"accepted"}}
```

Arguments and the single result are wire `Value`s, preserving Int64/`bigint`,
Date, Binary, arrays and safe objects. They contain no executable source,
callbacks, prototypes or arbitrary class instances. Method names match
`[A-Za-z][A-Za-z0-9_.-]{0,127}`.

The public Go `server` package registers a fixed method map and a separate
`RPCAuthorizer`. Registration does not grant permission: every call is first
authenticated, then authorized by actor and method. The server copies the
registry at construction, bounds body bytes, argument count, result bytes and
concurrent handler executions, propagates HTTP cancellation through `context`,
and converts handler panics or arbitrary errors to the non-sensitive
`{"kind":"internal","code":"internal"}` result. A handler may intentionally
return `&server.MeldbaseError{Code: "orders.already_paid"}` with a namespaced
code and optional safe `Data`; raw error text is never serialized.

Application and server failures use the matching `type: "error"` envelope with
the same request ID. HTTP connection cancellation cancels the handler context.
The WebSocket server accepts the identical `call` envelope and a strict
`{"v":1,"type":"cancel","requestId":"..."}` frame. It multiplexes calls with
subscriptions, enforces a per-connection budget plus the shared server budget,
rejects request IDs already owned by a call or subscription, and cancels every
in-flight method when the connection closes.

For a realtime call, aborting its `AbortSignal` removes the local pending call
and sends a `cancel` frame if the call was already sent. Cancellation is
best-effort: the method may finish before the server observes it. If the socket
closes before a terminal frame arrives, the Promise rejects with
`MeldbaseInternalError` code `outcome_unknown`; the method may already have
executed.

The SDK never automatically retries RPC calls; the protocol never infers retry
safety. An optional `idempotencyKey` is accepted only when the server
has an explicitly configured `RPCIdempotencyStore`; otherwise the attempt fails
closed with `rpc_idempotency_unavailable`. The store contract durably claims
execution before invoking application code, replays completed terminals, rejects
key/fingerprint conflicts, and turns interrupted claims into
`rpc_outcome_unknown` rather than rerunning them. The built-in store is explicit:

```go
db, err := meldbase.Open("app.meld")
if err != nil { log.Fatal(err) }

idempotency, err := server.NewDurableRPCIdempotencyStore(db)
if err != nil { log.Fatal(err) }

handler, err := server.New(server.Config{
    DB: db,
    RPCIdempotencyStore: idempotency,
    // Authenticator, Authorizer, RPCAuthorizer, methods and transport keys...
})
```

The durable store requires an open current-format database. The returned store
also exposes bounded `PruneExpired`; pending claims
are never removed by retention alone. See
[`rpc-idempotency.md`](rpc-idempotency.md).

Methods registered in Go through `RPCTransactionalMethods` additionally commit
their supported Meldbase point writes and success result under one root/meta
publication. They require `idempotencyKey`; a keyless call fails with
`rpc_idempotency_required`. Optimistic snapshot contention returns the durable,
replayable `rpc_transaction_conflict` error and never reruns the method. This is
the same over HTTP and WebSocket and requires no TypeScript wire/API variant.

Subscription resume remains independent and continues to reconnect. Callers
must not interpret transport failure or local cancellation as proof that a
mutation did not happen. A future server-side JavaScript method SDK must reuse
this exact contract rather than introduce new semantics. HTTP remains the
baseline when realtime transport is unnecessary.

## React adapter

`@meldbase/react` accepts the exact `LiveQuery` or `RemoteLiveQuery` returned by
the native SDK. It uses React's external-store contract and exposes documents,
sync status, token, and error without recompiling filters. Keep the query object
stable with `useMemo`; a new object intentionally means a new subscription.
`initialData` is only for a matching remote SSR/hydration snapshot and is not an
ongoing prop-driven cache.
