# Primary write lease

`integrations/primarylease` is a small, local enforcement component for a V2
primary. It is designed for a real external controller, not as an in-process
leader-election substitute.

The controller holds an Ed25519 private key. Each database process holds only
the public key and a stable, controller-assigned `Owner` identity. Before it
permits a process to write, the controller must atomically fence the prior
owner, allocate a monotonically increasing epoch, and sign a short-lived
certificate containing:

- database identity;
- owner identity;
- epoch;
- current committed sequence;
- `NotBefore` and `NotAfter`.

The database process installs that certificate into a `primarylease.Guard` and
passes the guard as `OpenOptions.PrimaryWriteFence`. The normal write path does
no controller RPC: it checks the installed signature-derived lease, database
identity, source sequence and expiry locally. No installed or expired
certificate means `ErrPrimaryWriteFence` before storage mutation.

```go
guard, err := primarylease.NewGuard(controllerPublicKey, primarylease.GuardOptions{
    Owner:            "db-writer-a",
    MaxLeaseDuration: 30 * time.Second,
})
if err != nil { /* configuration failure */ }

// Delivered over an authenticated control channel by the controller.
if err := guard.Install(controllerCertificate); err != nil { /* stay closed */ }

db, err := meldbase.OpenWithOptions(path, meldbase.OpenOptions{
    PrimaryWriteFence: guard,
})
```

The default maximum lease lifetime is 30 seconds; deployments may choose a
value from one second through ten minutes. Choose it from a measured failure
detection and clock-discipline budget, not convenience. A partitioned old
writer can continue only until its local certificate expires. The controller
should also send `guard.Revoke()` through its local agent whenever it fences a
reachable old owner.

`Install` rejects certificates for the wrong owner, invalid signature, invalid
time window or an epoch older than one the guard has already accepted. `Revoke`
raises the guard's minimum acceptable epoch, so the last certificate cannot be
replayed locally after revocation. Same-epoch renewal is permitted only for the
same database, owner and source position.

## Controller state

`primarylease.Authority` is the controller-side issuer. It requires a
`LeaseStore` with linearizable per-database `Load` and compare-and-swap; it
does not default to an in-memory store. Its `Grant` operation persists the new
owner/epoch/sequence/expiry state before returning a signed certificate.
`MemoryStore` exists only for tests and deterministic local demonstrations.

For a different owner, `Grant` returns `ErrLeaseActive` with `RetryAfter` until
the previous certificate's `NotAfter + MaxClockSkew`. It never treats a revoke
as permission to shorten that window: a partitioned old process may not have
received the revoke notification. A same-owner renewal can be granted early,
but still receives a new epoch. The authority refuses a sequence lower than its
stored sequence and advances epoch on every renewal, handoff and revocation.

`PromotionAuthority` adapts `Authority` to Meldbase's follower-promotion API;
the target owner identity is configured at construction rather than supplied
by a replication frame, while `PromotionReadiness` supplies the separate
follower-history proof. A production implementation should put a quorum-backed
CAS adapter behind `LeaseStore` and authenticate all grant/revoke/readiness
control requests outside this package.

`Authority.Stats()` exposes a fixed-cardinality, allocation-free snapshot for
controller monitoring: grant/revoke attempts and successes, safe-handoff
waits, promotion/readiness rejections, sequence rejections, store failures and
CAS conflicts. It deliberately contains no database ID, owner, epoch,
certificate, endpoint or error text. Sample it from a metrics collector rather
than from the controller request path; it is separate from `DBStats` because
one authority may serve many databases without participating in their commits.

## Authority mTLS API

`integrations/authorityhttp` is the supplied private HTTPS control-plane
adapter for an `Authority`. Mount its `Handler` behind TLS 1.2-or-newer with
`RequireAndVerifyClientCert`; the handler itself only validates the already
verified request chain and configured identity mappings. It accepts no browser
Origins and only bounded, strict JSON `POST` requests.

The handler has two deliberately distinct mTLS authorizers. `NodeAuthorize`
maps a verified node leaf fingerprint to the lease `Owner`, and that owner is
never accepted from JSON, a URL, or a header. A node can therefore request or
renew only its own certificate. `OperatorAuthorize` is independently
configured and is required for a revoke; granting a node identity never grants
the ability to fence a different writer. The supplied pinned HTTPS `Client`
supports the same one-to-four server-leaf rotation set as `leasehttp.Client`.

The API intentionally exposes grant/renew and revoke only. It does **not**
expose follower promotion, because a safe promotion must evaluate a local,
deployment-specific `PromotionReadiness` proof against follower history. An
HTTP caller must not be able to bypass that proof by claiming a token in JSON.
Likewise, a successful revoke advances controller state but cannot revoke an
already issued certificate at an unreachable old node; distribute
`Guard.Revoke` to reachable nodes as an optimization and retain the normal
expiry-plus-clock-skew handoff window as the safety rule.

## Primary renewal agent

`primarylease.Renewer` is the optional single-primary supervisor that connects
one V2 database, its already-configured `Guard`, and a narrow `RenewalClient`
(implemented by `authorityhttp.Client`). It snapshots the current committed
sequence, requests a certificate, and installs it locally. Controller I/O
never runs under the database writer or inside `ValidateV2PrimaryWrite`; every
business commit still makes only the bounded local Guard check.

For normal primary startup, prefer `OpenV2Primary`. It creates the V2 database,
the exact `Guard` installed as its only `PrimaryWriteFence`, and the matching
`Renewer` in one object; it rejects a caller-provided competing fence or
follower mode. The result begins closed to writes. Call `Renew` before serving
mutations, or run `Run` under the process supervisor. This avoids accidentally
renewing one Guard while the database checks another one.

The renewer requires a live V2 primary with its write fence currently enforced;
it rejects a read-only follower or a durability fail-stop database. `Run`
renews immediately, then renews one third of a lease duration before expiry and
retries controller failures at a bounded interval. A request failure leaves an
already valid certificate in force only until its signed `NotAfter`; it never
extends a lease or reopens an initially closed guard. By default, ending the
supervisory context calls `Guard.Revoke` locally, so an intentional agent stop
makes the process non-writable immediately.

Its sequence snapshot is a controller checkpoint, not a synchronous replication
acknowledgement. A primary can commit after the snapshot while a renewal is in
flight. Therefore the renewer improves primary liveness only; it does not prove
follower completeness, permit automatic promotion, or change the project's
zero-RPO boundary.

## Quorum store

`primarylease.QuorumStore` is the transport-neutral majority implementation of
`LeaseStore`. Configure it with one development member or an odd static set of
at least three distinct `QuorumReplica` member IDs. Each replica is itself a
linearizable `LeaseStore` adapter; in production that adapter should be a
separately authenticated mTLS RPC connection to an independent controller
member.

A CAS succeeds only after a strict majority acknowledges the exact transition.
A read returns only a record (or absence) that an exact strict majority holds.
It deliberately does **not** select the largest epoch across member responses:
an interrupted write can exist on one member, and two crossed partial writes
can have the same epoch but different owners. The former may be repaired by a
new majority CAS from the retained majority predecessor; the latter returns
`ErrLeaseConflict` and needs explicit operator/controller reconciliation.

This gives a controller a safe quorum primitive but is not an HTTP service,
membership protocol or certificate deployment system. In particular,
`anchorhttp.QuorumStore` cannot be substituted for it: rollback anchors have
monotonic coordinate semantics, while lease records require exact CAS and an
expiry-aware owner handoff.

`primarylease.FileStore` supplies one durable local member implementation. It
stores one bounded, checksummed record per database identity in an existing
controller-owned directory, serializes CAS through a per-record advisory lock,
and publishes with a synced temporary file, atomic rename and directory fsync.
It is appropriate as the private disk state of an independently deployed
member; three `FileStore` instances on the same host or disk are not three
failure domains and must not be configured as a production quorum.

The remaining deployment layer is an authenticated RPC adapter that exposes a
member's `LoadPrimaryLease` and `CompareAndSwapPrimaryLease` operations only to
the configured controller clients, with mTLS member identity, request bounds,
timeout/cancellation behavior and a fixed membership configuration. That
adapter must preserve the exact-CAS contract; it must not translate a conflict
into a successful retry.

## HTTPS/mTLS member adapter

`integrations/leasehttp` is the supplied narrow member adapter. Its handler
exposes only `GET /v1/lease/{databaseId}` and exact `PUT` CAS; it cannot issue
certificates or invoke promotion by itself. Mount it behind a TLS 1.2-or-newer
`http.Server` configured with `RequireAndVerifyClientCert` and the controller
client CA. Configure `leasehttp.NewMTLSAuthorizer` with the SHA-256
fingerprints of the allowed verified client leaf certificates.

Each `leasehttp.Client` requires an `https://` origin, an mTLS-capable
`http.Client`, and one to four lowercase SHA-256 fingerprints of its expected
**server** leaf certificates. `ExpectedServerFingerprint` remains the
single-leaf compatibility field; new configuration uses
`ExpectedServerFingerprints`. The client rejects redirects, unverified TLS and
a trusted-but-unpinned server leaf. `primarylease.QuorumStore` treats the whole
configured pin set as the member's cryptographically bound identity: it rejects
both duplicate pins within one member and any pin shared by two configured
members. Thus a rotating old/new certificate pair for one member cannot be
listed as two static votes to manufacture a quorum. Give each production member
a distinct server certificate and independent state volume; distinct leaf
certificates alone do not prove independent failure domains.

This adapter is deliberately not a public API: it rejects browser Origins,
requires a verified client chain before authorization, uses bounded strict JSON
and returns only low-information failures. Production rollout still needs
member health checks, request deadlines and real partition/failover exercises.

For a member leaf rotation, first deploy its controller client configuration
with exactly the old and new pins, then replace the server leaf, verify a fresh
mTLS request, and only then remove the old pin. Do not reuse either pin for a
different quorum member at any stage. The pin set is fixed when a
`QuorumStore` is constructed, so reload/recreate that control-plane client
configuration deliberately; do not mutate it in place under live operations.

## Promotion

For follower promotion, the external `FollowerPromotionAuthority` returns the
same compact signed certificate in `FollowerPromotionFence.Epoch`. The guard
implements `FollowerPromotionFenceBinder`; Meldbase validates that the fence
matches the follower identity and sequence, then asks the guard to verify and
install the certificate before it makes the follower writable. This keeps the
promotion certificate and ongoing write admission bound to the same identity,
sequence, owner and expiry.

`primarylease.PromotionAuthority` additionally requires a
`PromotionReadiness` implementation. It is called against the controller
record read by the same CAS attempt; omitting it refuses promotion. Readiness
must prove that the candidate follower has the required durable source history
and satisfies the deployment's recovery policy. A simple equality check against
the controller checkpoint is useful in tests, but is not a replication proof.
The supplied authority/lease protocol fences writers; it does not magically
make asynchronous replication synchronous.

Therefore this project currently makes no zero-RPO failover claim. If a former
primary commits after its last controller/replication checkpoint, an automatic
promotion needs an external source-position/ack proof or must stay closed.
Deployments seeking RPO=0 need a synchronous replication/quorum-commit design,
which is outside the current data path. Lease duration and controller renewal
frequency alone are not a write-count or data-loss bound.

`DurableConsumerPromotionReadiness` is the first supplied readiness policy for
a single-writer follower. It opens the source's authenticated named durable
database consumer and requires all four values to match exactly: source
database identity, source current token, durable consumer ACK checkpoint, and
candidate follower token/controller checkpoint. It fails closed when the source
is unavailable, history is missing, the consumer is absent, or the source has
advanced after that ACK. Use the same stable consumer name that the replication
transport derives from the follower's mTLS identity; never accept it from a
promotion request. This is a conservative connected-source policy: a network
partition without a separately attested checkpoint remains unavailable rather
than guessing that a follower is current.

The signed certificate is only a local verification format. It does **not**
provide the controller's required distributed state machine. A production
controller must separately provide durable owner/epoch compare-and-swap,
quorum-backed lease renewal, old-owner fencing, certificate rotation, clock
discipline and a tested partition/failover procedure. Do not reuse a rollback
anchor for that state: a monotonic recovery floor is not a transferable write
lease.

## Deliberate boundaries

- Certificates are not stored in the database file. Reopening starts closed
  until the controller delivers a current lease.
- `Guard.ValidateV2PrimaryWrite` does not perform network I/O, disk I/O or
  callback execution.
- A controller signing key must be kept outside the database process; key
  rotation needs a deployment-level dual-key rollout before changing guards.
- The format is versioned and compact for the existing bounded promotion-fence
  field. It is not a public browser or replication-wire credential.
