# Meldbase rollback-anchor HTTP protocol v2

`anchorhttp.ProtocolVersion == 2` defines this contract. Unknown fields,
versions, trailing JSON values and bodies larger than 4096 bytes are rejected.

## Resource

```text
GET /v2/anchors/{anchorName}
PUT /v2/anchors/{anchorName}
```

`anchorName` is 1–128 ASCII letters, digits, `.`, `_` or `-`, excluding `.` and
`..`. Query parameters are forbidden.

## Anchor JSON

```json
{
  "version": 2,
  "databaseId": "00112233445566778899aabbccddeeff",
  "minimumCommitSequence": 42,
  "minimumGeneration": 57
}
```

`databaseId` is exactly 16 non-zero bytes encoded as 32 hexadecimal characters.
`minimumGeneration` must be greater than `minimumCommitSequence`; Storage V2
starts at generation 1/sequence 0 and every logical commit advances both while
maintenance may advance generation alone.

## Request authentication

Every request carries:

```text
Meldbase-Anchor-Configuration-ID: <lowercase configuration digest>
Meldbase-Anchor-Member-ID: <expected static member ID>
Meldbase-Anchor-Timestamp: <signed base-10 Unix milliseconds>
Meldbase-Anchor-Signature: <lowercase hex HMAC-SHA-256>
Meldbase-Anchor-Key-ID: <1–64 safe ASCII characters>
```

The HMAC key contains at least 32 bytes. The signed bytes are UTF-8:

```text
METHOD + "\n" +
ESCAPED_PATH + "\n" +
CONFIGURATION_ID_HEADER_EXACTLY + "\n" +
MEMBER_ID_HEADER_EXACTLY + "\n" +
KEY_ID_HEADER_EXACTLY + "\n" +
TIMESTAMP_HEADER_EXACTLY + "\n" +
LOWERCASE_HEX_SHA256(EXACT_BODY_BYTES) + "\n"
```

For `GET`, the exact body is empty. The server compares the decoded MAC in
constant time and rejects timestamps outside its configured clock-skew window.
The HMAC authenticates requests but does not encrypt traffic or authenticate
responses; clients therefore require ordinary server-authenticated HTTPS.

Servers may accept a bounded set of key IDs for rotation. Install the new ID
everywhere before moving clients, then remove the old ID only after the maximum
request and clock-skew window.

Replay of an authenticated `PUT` cannot lower state because advance is atomic
and monotonic. A replay after a later advance receives conflict. `GET` has no
mutation effect.

## Operations

### GET

- `200`: body is one strict Anchor JSON record.
- `404`: anchor does not exist.
- `401`: authentication failed.
- `412`: the signed configuration or expected member does not match this node.
- `503`: trusted storage is unavailable.

### PUT

The server atomically creates an absent anchor or advances an existing anchor.
It must durably persist the result before returning success.

- `204`: requested tuple is durably retained; an exact duplicate is allowed.
- `400`: malformed or unsupported record.
- `401`: authentication failed.
- `409`: database identity differs, sequence regresses, or generation regresses.
- `412`: the signed configuration or expected member does not match this node.
- `503`: durable storage is unavailable or the outcome cannot be confirmed.

Error responses are bounded JSON objects:

```json
{"version":2,"code":"anchor_conflict"}
```

Codes are diagnostic only; clients make decisions from the HTTP status and
must not expose response bodies as database errors.

## Static crash-fault quorum

The caller supplies a stable cluster ID and the full unique member-ID list.
Both client and server derive the configuration ID as SHA-256 over a v2 domain
separator, the cluster ID and the lexically sorted member IDs. Each endpoint is
configured with that full list and exactly one member ID. Its trusted directory
is atomically and durably bound to the resulting configuration/member pair on
first startup; a directory containing anchors but no manifest is rejected.

Requests sign both the derived configuration ID and expected member ID. A
server returns `412` if either differs from its durable binding. Consequently,
multiple URL or proxy aliases to one physical member cannot manufacture a
quorum, and adding, removing or replacing a member necessarily creates a new
configuration rejected by old nodes. Protocol v2 intentionally supplies no
automatic migration from an unbound v1 directory.

The exact configuration digest input is UTF-8:

```text
"meldbase-anchor-http-static-configuration-v2\0" +
CLUSTER_ID +
for each lexically sorted MEMBER_ID: "\0" + MEMBER_ID
```

The resulting configuration ID is lowercase hexadecimal SHA-256. Cluster and
member IDs use the same safe ASCII alphabet as anchor names, with a maximum of
128 bytes. The member list is either one member or odd-sized with at least
three members; duplicates are invalid.

For `N = 2f+1` fixed endpoints, read quorum and write quorum are both `f+1`.
Single-endpoint mode uses quorum 1 for development or a separately highly
available service.

A read starts all requests concurrently and completes when the successful
responses contain a read-quorum subset with an observed maximum. Missing
records count as successful empty responses. The maximum must have one identity
and dominate every existing tuple selected into that quorum in both sequence
and generation. Lower tuples need not be mutually comparable when the observed
maximum dominates them all. A crossed minority therefore does not poison a
clean majority; if all responses complete without a safe quorum, the read fails
closed with conflict.

A write starts all requests concurrently and succeeds when a write quorum
durably accepts the same record. A monotonic-conflict response is a rejecting
vote: it is counted and observed, but a crash-fault minority of rejecting nodes
cannot override a durable accepting majority. The write returns conflict when
conflicts leave fewer than a quorum of possible accepting votes.
Authentication, static-configuration and request-protocol responses fail the
operation immediately; transport and 5xx failures count as unavailable nodes.
Calls are bounded by the caller context, which Meldbase further bounds
with `V2RollbackProtection.OperationTimeout`.

An unsuccessful or canceled write has an ambiguous outcome. Any member may have
durably accepted the tuple before its response was lost, and cancellation does
not retract that state. A caller must not interpret an error as an aborted
advance. Meldbase commits the database first, disables further writes on anchor
failure, and reconciles the durable database with a fresh read quorum when the
process opens again.

Concurrent comparable advances may both succeed and converge on the dominating
tuple. Concurrent crossed advances cannot both succeed: two accepting majorities
intersect, and their shared monotonic member cannot accept both crossed tuples.
Either operation can still have an ambiguous failed outcome if responses are
lost.

Read/write quorum intersection ensures every read quorum observes at least one
copy of the last quorum-acknowledged floor. A stale minority can rejoin on a
later advance. Endpoint membership and identity are configuration state and
must remain static; v2 defines no safe live reconfiguration protocol.

## Fault model

Protocol v2 tolerates up to `f` crash, delay, stale-state or partitioned nodes.
It is not Byzantine-fault tolerant. Nodes share one request key, there is no
threshold signature, and a compromised node can lie about its own fsync. A
Byzantine deployment must put a consensus/threshold service behind the same
`RollbackAnchorStore` behavior or define a future protocol version.
