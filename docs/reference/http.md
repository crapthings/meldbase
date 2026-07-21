# HTTP and realtime reference

Meldbase uses one versioned, data-only query and value contract for HTTP,
WebSocket realtime, and the TypeScript SDK. The complete frame, limit, error,
origin, and authorization rules are in the [client protocol](../client-protocol).

## Operational probes

| Endpoint | Meaning |
| --- | --- |
| `GET /livez` | The Go handler is responsive. |
| `GET /readyz` | The database is readable and writable. |
| `GET /health` | Compatibility alias for readiness. |

`/readyz` returns `503` after a fail-stop durability error. Probe responses do
not expose database paths or internal error text.

## Application endpoints

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/collections/{collection}/query` | Query documents with the versioned query AST. |
| `POST /v1/collections/{collection}/count` | Return a policy-capped count for a query. |
| `POST /v1/collections/{collection}/group-count` | Return policy-constrained counts for one top-level field. |
| `POST /v1/collections/{collection}/documents` | Insert a document. |
| `POST /v1/collections/{collection}/mutations` | Apply a safe data-only mutation. |
| `POST /v1/realtime/tickets` | Exchange an authenticated HTTP request for a single-use realtime ticket. |
| `GET /v1/realtime` | Upgrade to WebSocket; authenticate with the ticket in the first frame. |
| `POST /v1/rpc` | Call an explicitly registered application RPC method. |

When workspace isolation is enabled, the server intersects reads, updates,
deletes, and subscriptions with the verified token workspace. It writes the
server-owned workspace field on inserts. Clients never send a trusted tenant
selector.

## Authentication

Use `Authorization: Bearer <access-token>` for HTTP. Browser clients first
request a short-lived ticket over that authenticated channel, then send the
ticket in their first WebSocket message. Credentials never appear in a
WebSocket URL.

## Calling data endpoints without the SDK

The HTTP API is a typed wire protocol, not a JSON-shaped MongoDB clone. Every
document and query operand uses a closed value envelope. Prefer
`@meldbase/client` unless your environment needs a raw protocol client.

| Value | Wire form |
| --- | --- |
| Null | `{"t":"null"}` |
| Boolean | `{"t":"bool","v":true}` |
| Int64 | `{"t":"int64","v":"42"}` |
| Number | `{"t":"number","v":1.5}` |
| String | `{"t":"string","v":"hello"}` |
| Date | `{"t":"date","v":"2026-07-20T12:00:00.000Z"}` |
| Binary | `{"t":"binary","v":"base64..."}` |
| Document ID | `{"t":"id","v":"0123456789abcdef0123456789abcdef"}` |
| Array | `{"t":"array","v":[VALUE,...]}` |
| Object / document | `{"t":"object","v":[["field",VALUE],...]}` |

Int64 values are strings on the wire so JavaScript cannot round them. Object
entries are field/value pairs rather than a JSON object; this preserves typed
values and rejects duplicate or dangerous field names. All public payloads
reject duplicate JSON keys, unknown envelope fields, executable values, and
non-version-1 contracts.

Set the server URL and a short-lived application-issued token before using the
examples below:

```sh
export MELDBASE_URL='https://api.example.test'
export MELDBASE_TOKEN='eyJ...'
```

### Query documents

This queries open todos, sorted by creation time and capped at 100 results:

```sh
curl --fail-with-body "$MELDBASE_URL/v1/collections/todos/query" \
  -H "Authorization: Bearer $MELDBASE_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{
    "version": 1,
    "query": {
      "version": 1,
      "where": {
        "op": "compare",
        "cmp": "eq",
        "path": "completed",
        "value": {"t": "bool", "v": false}
      },
      "sort": [{"path": "createdAt", "direction": 1}],
      "limit": 100
    }
  }'
```

A successful response is exactly `{"version":1,"documents":[...]}`. Each
entry in `documents` is a typed object value. When workspace isolation is
enabled, the server adds its own workspace constraint before this query runs;
do not add a trusted workspace selector to the request.

### Count and group documents

`POST /v1/collections/{collection}/count` accepts the same query envelope and
returns exactly `{"version":1,"count":N,"capped":BOOLEAN}`. It is for
badges and summaries: `capped: true` means `N` is a policy-bounded lower bound,
not a complete cardinality.

`POST /v1/collections/{collection}/group-count` additionally accepts a
top-level `groupBy` field and returns
`{"version":1,"groups":[{"key":VALUE,"count":N}],"capped":BOOLEAN}`.
The field must be permitted by both aggregate and result-field policy because
each returned key is data. There is no arbitrary aggregation pipeline; at most
100 groups are returned. For TypeScript usage, see `RemoteCollection.count()`
and `RemoteCollection.groupCount()` in `@meldbase/client`.

### Insert a document

Supply a stable `_id` when a caller may need to reconcile an interrupted
request. Do not include a server-owned workspace field; the server overwrites
it for scoped collections.

```sh
curl --fail-with-body "$MELDBASE_URL/v1/collections/todos/documents" \
  -H "Authorization: Bearer $MELDBASE_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{
    "version": 1,
    "document": {
      "t": "object",
      "v": [
        ["_id", {"t": "id", "v": "0123456789abcdef0123456789abcdef"}],
        ["title", {"t": "string", "v": "Write the release notes"}],
        ["completed", {"t": "bool", "v": false}],
        ["createdAt", {"t": "date", "v": "2026-07-20T12:00:00.000Z"}]
      ]
    }
  }'
```

Success is HTTP `201` with exactly `{"version":1,"document":DOC}`. The
returned document is the authorized projection, so it may omit server-owned or
redacted fields.

### Update or delete documents

Mutations use a query envelope plus a data-only list of operations. Valid
actions are `updateOne`, `updateMany`, `deleteOne`, and `deleteMany`.

```sh
curl --fail-with-body "$MELDBASE_URL/v1/collections/todos/mutations" \
  -H "Authorization: Bearer $MELDBASE_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{
    "version": 1,
    "action": "updateOne",
    "query": {
      "version": 1,
      "where": {
        "op": "compare",
        "cmp": "eq",
        "path": "_id",
        "value": {"t": "id", "v": "0123456789abcdef0123456789abcdef"}
      }
    },
    "update": {
      "version": 1,
      "operations": [
        {"op": "set", "path": "completed", "value": {"t": "bool", "v": true}}
      ]
    }
  }'
```

An update returns exactly `{"version":1,"matchedCount":N,"modifiedCount":N}`;
a delete returns exactly `{"version":1,"deletedCount":N}`. The server applies
the authorization constraint under its write lock and enforces the configured
affected-document cap, so an over-broad `updateMany` or `deleteMany` is rejected
as a whole rather than partially applied.

## Errors and retries

Data endpoint errors use the non-sensitive shape
`{"error":{"code":"..."}}`. Typical boundaries are `401 unauthenticated`,
`403 forbidden`, `400` for a malformed or unsupported envelope, `413
resource_limit_exceeded`, and `503 database_unavailable`. Raw storage errors,
paths, credentials, and policy details never cross the API.

Do not blindly retry writes after a timeout or connection failure. Use a stable
document ID and reconcile through an authorized query, or use an explicitly
idempotent RPC method. The [RPC idempotency contract](../rpc-idempotency)
describes that stronger workflow.

See [getting started](../guide/getting-started), then read the full
[client protocol](../client-protocol) before implementing a non-SDK client.
