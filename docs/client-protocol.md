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

## Trust boundary

The client is always untrusted. Compiling locally improves feedback but grants no
authority. The server must decode and compile the AST again with stricter limits.
It must reject unknown versions/operators/fields and must never evaluate source
text, JavaScript, regex with unbounded execution, or user callbacks.

For every implemented fetch and subscription, the server applies:

1. authenticated principal and tenant scoping;
2. collection/action policy;
3. row predicate injected into the plan (not post-filtered after pagination);
4. field projection/redaction before serialization;
5. query depth/node/result/byte limits;
6. subscription-count and outbound-buffer limits.

Insert authorization separately validates client-writable fields, then applies
server-owned fields such as tenant and owner after validation. Client values can
never override those fields.

Update and delete authorization recompile the data-only mutation on the server,
apply the server-owned row predicate, validate writable paths, and enforce a
separate `MaxAffected` bound. That bound is checked while the engine holds its
write lock; exceeding it rejects the entire mutation without a partial write.

Resume tokens are opaque, expiring HMAC-authenticated values bound to database
identity, authenticated subject and tenant, collection, the canonical authorized
query, policy version, and durable commit position. The engine retains a bounded,
contiguous position window in checkpoints and extends it during WAL recovery. A token outside that window,
with a changed security context, or with an invalid signature yields
`resync_required`. A valid V1 resume still produces one atomic current snapshot;
the retained position proves continuity but is not yet used to send incremental
change frames. Only positions—not historical document images—are copied into the
checkpoint. Resume tokens are continuity capabilities, never authorization
credentials: authentication and policy evaluation always run again.

Production servers should configure a stable, randomly generated resume-token
key (at least 32 bytes) across process restarts. An omitted key is generated in
memory and is safe, but deliberately turns all pre-restart tokens into clean
resyncs. Key rotation can use the same resync behavior until multi-key validation
is introduced.

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
upgrades continue to use host-pattern validation. CORS is a browser boundary,
not a replacement for authentication or row/field authorization.

Snapshots are delivered atomically to the native `LiveQuery` callback and React
`useLiveQuery` store. Callbacks run from a microtask outside socket parsing. The
client validates message shape and enforces inbound document/byte limits even
though the server is trusted operationally. The current remote SDK does not yet
maintain an offline mutation cache or optimistic conflict-resolution layer.

## Wire messages

Implemented client messages: `authenticate`, `subscribe`, `unsubscribe`, `ping`.
Implemented server messages: `authenticated`, `snapshot`, `error`,
`resync_required`, `pong`.

All envelopes include protocol version. Subscription data includes an ordered
commit token. V1 sends full snapshots; incremental `change` frames are reserved
for a later protocol version.

## React adapter

`@meldbase/react` accepts the exact `LiveQuery` or `RemoteLiveQuery` returned by
the native SDK. It uses React's external-store contract and exposes documents,
sync status, token, and error without recompiling filters. Keep the query object
stable with `useMemo`; a new object intentionally means a new subscription.
`initialData` is only for a matching remote SSR/hydration snapshot and is not an
ongoing prop-driven cache.
