# Replication protocol v1

Replication is a trusted-server protocol, distinct from the browser realtime
protocol. Its unit of ordering is one Commit Log token, not a WebSocket
message. Version 1 defines a strict JSON frame codec in `replication_wire.go`;
the codec can run over WebSocket, QUIC or a framed RPC stream.

The transport must authenticate both peers before the first frame (normally
mTLS plus an operator-authorized replica identity). The frame `databaseId` is a
namespace binding, not authentication: a receiver rejects a mismatched source
identity rather than accepting a superficially valid token sequence. The
optional `integrations/replicationws` adapter is a server-to-server WebSocket
transport: it rejects browser Origins and requires an explicit authorization
callback that returns the peer's stable durable-consumer name. That callback is
where deployments enforce mTLS or their internal identity system.
`replicationws.NewMTLSAuthorizer` is a strict helper for the common mTLS case:
it maps the SHA-256 fingerprint of an already verified leaf certificate to the
consumer name, avoiding ambiguous subject/SAN string authorization. The host
HTTP server still must require and validate client certificates in its TLS
configuration before requests reach the adapter.

Both supplied replication transports require TLS 1.2 or newer. The WebSocket
handler rejects a non-TLS or obsolete-TLS upgrade and
`replicationws.Receive` accepts only a non-redirected `wss://` endpoint with a
TLS 1.2-or-newer **verified server certificate chain**; the HTTP bootstrap has
the equivalent `https://` rule. Both reject a client configured with
`InsecureSkipVerify`. A deployment using a TLS
terminator must keep this authenticated hop end-to-end to the handler, rather
than forwarding plaintext and synthesizing peer identity headers.

## Bootstrap and tail

1. The primary calls `BeginArchive(name, destination, buffer)`. It creates a
   durable checkpoint before it writes and verifies the physical backup.
2. The receiver passes the authenticated byte stream and the source's
   `BackupResult` receipt to `ImportPhysicalBackup`. The receiver-owned
   byte cap, streaming SHA-256, full offline graph/index audit and no-overwrite
  publication must all succeed before it opens the result through
   `OpenFollower` and records `SnapshotToken`. A transport may not treat a
   successful download as a verified bootstrap.
3. It drains source batches through `SnapshotToken` without applying them, then
   sends its first `ack` only after the backup is locally durable.
4. It sends `hello(afterToken=SnapshotToken, maxBytes=...)`. The source resumes
   the named durable consumer or sends `resync_required(history_lost)` when the
   requested window no longer exists.
5. Each later `batch` is decoded strictly and passed to `Follower.Apply`.
   The receiver sends `ack(token)` only after that call succeeds. The source
   advances its durable consumer checkpoint only after validating the matching,
   authenticated acknowledgement.

The source must send one batch at a time within negotiated flow-control credit;
an ACK is both durability confirmation and the byte/window credit return. A
peer that disconnects, exceeds its frame cap, sends an invalid token, or fails
authentication loses its process-local stream but not its named checkpoint.

Before opening that stream, a source transport should acquire
`DB.AcquireReplicationSourceLease` for the authenticated durable-consumer name.
The lease permits exactly one active source connection for that name in the
local DB process, preventing two transports from racing one checkpoint or
duplicating delivery. It is released on disconnect and never advances the
checkpoint itself. The WebSocket adapter acquires it automatically. This is not
a distributed leadership lease: primary authority and cross-process fencing
remain external control-plane responsibilities.

The Go core implements this source-side rule as `ReplicationSourceSession`.
It accepts `hello` only when `afterToken` equals the durable checkpoint, exposes
at most one batch frame in flight, and calls the durable consumer's `Ack` only
when an identity-bound matching `ack` arrives. A bad hello yields a terminal
resync response; a stale/future/duplicate ACK cannot release history.

`integrations/replicationws.Receive` is the matching follower-side client. For
an ordinary reconnect it uses the follower's durable token as `afterToken` and
applies every later batch before acknowledging it. For a `BeginArchive`
bootstrap, configure both the returned `CheckpointToken` and `SnapshotToken`:
the client consumes and acknowledges the interval
`(CheckpointToken, SnapshotToken]` without applying it because the verified
physical snapshot already contains those effects; it then atomically applies
later batches. This is the explicit proof that snapshot/tail handoff does not
double-apply data or skip retention acknowledgement.

### HTTPS bootstrap adapter

`integrations/replicationhttp` provides the v1 physical-transfer transport.
Mount `replicationhttp.NewSource` only behind an HTTPS server that requires and
verifies client certificates. Its `Authorize` callback must map the verified
peer identity to the stable durable-consumer name; the return value is never
accepted from an HTTP header, URL or replication frame.
`replicationhttp.NewMTLSAuthorizer` and
`replicationws.NewMTLSAuthorizer` are wrappers over the same shared verifier:
they map a verified leaf-certificate SHA-256 fingerprint to that name, so the
bootstrap and tail cannot use subtly different peer mappings.

The handler rejects browser `Origin`s and plain HTTP, takes the same
process-local source lease used by the WebSocket source, calls `BeginArchive`,
and returns a no-store `application/octet-stream` response. Version 1 carries
the exact artifact receipt and bridge tokens in single-valued headers:

| Header | Value |
| --- | --- |
| `X-Meldbase-Bootstrap-Version` | `1` |
| `X-Meldbase-Bytes`, `-Pages`, `-Commit-Sequence`, `-Meta-Generation` | Canonical unsigned decimal backup receipt fields. |
| `X-Meldbase-Database-ID`, `-SHA256` | Lowercase canonical receipt fields. |
| `X-Meldbase-Checkpoint-Token`, `-Snapshot-Token` | Canonical unsigned decimal bootstrap bridge fields. |

`replicationhttp.Fetch` accepts only a non-redirected `https://` URL, requires
a real TLS response, requires a declared content length matching `Bytes`, and
passes the stream plus receipt to `ImportPhysicalBackup`. Configure its
`HTTPClient` with the receiver's mTLS certificate and trusted server roots, and
set `MaxBytes` on the receiver. It never publishes a partially downloaded file.
After an archive has been created, a disconnect or a failed receiving audit does
not delete the source durable checkpoint: retaining history is safer than
silently making a retry impossible. An operator may resume the named tail or
explicitly remove that consumer before intentionally bootstrapping it again.

## Frames

Every frame has `v: 1`, `type`, and lowercase canonical 128-bit hex
`databaseId`.

| Type | Required fields | Meaning |
| --- | --- | --- |
| `hello` | `afterToken`, `maxBytes` | Receiver identity/position and maximum decompressed frame size. |
| `batch` | `token`, `transactionId`, `committedAtMs`, `changes` | One ordered public database change batch. |
| `ack` | `token` | Durable receiver acknowledgement. |
| `resync_required` | `reason` | Safe terminal response: `history_lost`, `identity_mismatch`, `snapshot_required`, or `protocol_error`. |

`batch` document images are base64 of the versioned typed document codec, not
ad-hoc JSON objects. The decoder rejects unknown/duplicate fields, malformed or
non-canonical identities/base64, oversized frames, invalid document IDs and
invalid index definitions before data reaches the follower.

Collection creation, completed index publication and document changes are
applied atomically at the target. A source token whose public projection is
empty becomes a target-private marker so later source tokens remain contiguous.
This does **not** replicate private RPC/idempotency System records; followers
are read-only and must not serve primary-side method ownership.

## Explicit non-goals

Meldbase does not provide built-in TLS termination or deployment configuration,
compression, multi-primary conflict resolution, leader election or failover.
`replicationhttp` and `replicationws` supply the fixed HTTPS physical-transfer
and authenticated WSS-tail adapters. `primarylease` supplies a concrete local
fence, quorum-backed controller primitives and renewal boundary, but deployment
still owns certificate issuance, trusted roots, peer authorization, independent
member failure domains, leader election and client routing. The frame contract,
transport adapters and local state machines exist so those layers cannot weaken
durable ordering or silently retry a non-idempotent change.

## Follower promotion

`Follower.Promote` is intentionally not an automatic failover mechanism. It
requires a caller-provided `FollowerPromotionAuthority` to durably certify a
fence containing the exact local `databaseId`, current commit sequence and a
non-empty fencing epoch. The external authority must revoke the old primary's
write lease through a quorum/controller before returning that fence. A missing,
stale or mismatched fence leaves the database read-only.

Writer fencing alone is not follower-history proof. An asynchronous source can
have committed after its last controller checkpoint, so a production promotion
authority must separately verify durable source/follower acknowledgement and
the deployment RPO policy. `integrations/primarylease.PromotionAuthority`
requires an explicit `PromotionReadiness` implementation for that reason; it
refuses a default automatic promotion. Current Meldbase does not claim RPO=0
failover without a future synchronous replication/quorum-commit design.
`primarylease.DurableConsumerPromotionReadiness` supplies the conservative
connected-source case by requiring the source's durable ACK, source token and
candidate token to agree; it deliberately refuses a partitioned/unreachable
source unless a deployment supplies a stronger external attestation.

After promotion the local database accepts ordinary writes and the
`Follower` permanently rejects replication input. Leader election, health
assessment, lease revocation and client routing remain deployment/controller
responsibilities; this API merely refuses to hide those decisions behind an
unsafe boolean switch.

## Optional primary write fence

`OpenOptions.PrimaryWriteFence` gives a primary process a local fail-closed
enforcement point for its external controller. Before every business commit
(including each logical member of a coordinator group, durable index-build
visibility publication, a standalone private system-record commit, and the
sequence-one private consumer initialization of an empty source), Meldbase passes the
database identity and exact next source sequence to
`ValidatePrimaryWrite`. A rejected guard leaves the database/token unchanged
and returns `ErrPrimaryWriteFence`; it is not a storage durability failure.

`Follower.Promote` additionally requires this local guard to have been
configured when the follower was opened. A matching promotion certificate alone
cannot make a process safely writable forever: after promotion, every local
logical write is still checked by that guard. The guard must also implement
`FollowerPromotionFenceBinder`; Meldbase passes it the exact validated
certificate after the external authority returns and before it lifts read-only
mode. Missing local enforcement/binding returns
`ErrReplicaPromotionWriteFence` before the external authority is called.

The guard must be a fast local lease/epoch/expiry check, refreshed by a
controller outside the database writer. It must not call the network or reenter
the DB. This makes a lost/revoked primary unable to accept new business writes
without pretending Meldbase has implemented leader election. Read-only follower
application bypasses this guard because it applies an already identity-bound,
ordered primary history; after promotion a configured guard applies normally.
`integrations/primarylease` supplies a signed short-lived certificate guard for
that local boundary; its controller contract and remaining distributed
responsibilities are documented in [primary write lease](primary-lease.md).
