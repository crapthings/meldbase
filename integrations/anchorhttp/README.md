# Meldbase HTTP quorum rollback anchor

`anchorhttp` is a reference authenticated remote implementation of
`meldbase.RollbackAnchorStore`. It uses majority reads and writes over one or an
odd number of at least three independently operated HTTPS endpoints.

It protects the monotonic tuple:

```text
(database identity, logical commit sequence, physical generation)
```

`QuorumStore.RollbackAnchorStatus` exposes only fixed-cardinality, process-local
operation and failure totals. Meldbase includes these in `DB.Stats()`, the admin
dashboard, Prometheus and OpenTelemetry; endpoint URLs and anchor identities are
never exported.
`CheckReplicas` is a cold-path qualification operation that waits for and
classifies every static member instead of returning at the first quorum.

## Server

Each endpoint needs its own trusted persistent directory and TLS identity:

```go
handler, err := anchorhttp.NewHandler(anchorhttp.HandlerOptions{
    Directory: "/var/lib/meldbase-anchor",
    ClusterID: "orders-production",
    Members:   []string{"anchor-a", "anchor-b", "anchor-c"},
    MemberID:  "anchor-a",
    Keys: map[string][]byte{
        "2026-primary": sharedKey, // at least 32 random bytes
    },
})
if err != nil {
    log.Fatal(err)
}

server := &http.Server{
    Addr:              ":8443",
    Handler:           handler,
    ReadHeaderTimeout: 5 * time.Second,
}
log.Fatal(server.ListenAndServeTLS("server.crt", "server.key"))
```

The directory must already exist. On first startup it is durably bound to the
cluster's complete static member list and this node's unique member ID. Reusing
it with another cluster, member or member list fails closed; an unbound
directory that already contains anchors is never adopted implicitly. The
handler stores one atomically replaced, fsynced file per validated anchor name.
Deploy the nodes on independent failure and rollback domains; three processes
writing the same disk are not a quorum.

## Client

```go
anchor, err := anchorhttp.NewQuorumStore(anchorhttp.QuorumOptions{
    ClusterID: "orders-production",
    Replicas: []anchorhttp.Replica{
        {Endpoint: "https://anchor-a.example.com:8443", MemberID: "anchor-a"},
        {Endpoint: "https://anchor-b.example.com:8443", MemberID: "anchor-b"},
        {Endpoint: "https://anchor-c.example.com:8443", MemberID: "anchor-c"},
    },
    AnchorName: "orders-production",
    KeyID:      "2026-primary",
    SharedKey:  sharedKey,
})
if err != nil {
    log.Fatal(err)
}

db, err := meldbase.OpenWithOptions("/data/orders.meld", meldbase.Options{
    RollbackProtection: meldbase.RollbackProtection{
        AnchorStore:      anchor,
        InitializeAnchor: true, // first audited provisioning only
        OperationTimeout: 3 * time.Second,
    },
})
```

Omit `InitializeAnchor` after provisioning. The supplied `http.Client` controls
CA roots, mTLS and transport policy. Redirects are always rejected. Plain HTTP
is rejected unless `AllowInsecureHTTP` is explicitly enabled for tests.

## Wire security

Every request is HMAC-SHA-256 authenticated over method, escaped path, derived
static-configuration ID, expected member ID, key ID, millisecond timestamp and
the exact body digest. Servers reject timestamps
outside a bounded clock-skew window. TLS remains mandatory because HMAC does
not encrypt traffic or authenticate the server. Captured `PUT` replay cannot
lower state because the server operation is atomic monotonic advance; captured
`GET` replay has no mutation effect.

For key rotation, install both old and new IDs on every server, move clients to
the new ID, then remove the old ID after the maximum in-flight/skew window.

## Quorum behavior

- Endpoint count is fixed at one or odd `2f+1`; read and write quorum are both
  `f+1`.
- Duplicate endpoint URLs and member IDs are rejected. A node checks its signed
  expected member identity, so URL/proxy aliases to one member cannot manufacture
  a majority. Operators must still place members in independent failure domains.
- Reads select a quorum subset with an observed maximum. A crossed minority
  cannot poison a clean majority; a completed response set with no safe quorum
  fails closed.
- Writes succeed after a majority durably accepts the same monotonic advance.
  Unavailable, rejecting or stale minority nodes cannot override that majority
  and can rejoin on a later dominating advance.
- A write that reaches fewer than a majority returns `ErrQuorum`; Meldbase then
  treats a preceding physical database commit as ambiguous and disables writes.
- An error or cancellation never means the remote state was unchanged. A member
  may have persisted the tuple before losing its response; restart performs a
  new quorum read and can recover the corresponding durable database commit.
- Concurrent crossed writers cannot both receive quorum success. Comparable
 writers converge on the dominating tuple.
- Any member-list change derives a different configuration and is rejected by
  existing directories. Membership must not be changed ad hoc. Safe reconfiguration requires a future
  explicit joint-consensus/membership protocol or an operator-controlled outage
  and state audit.

This implementation tolerates crash, delay and partition faults within a static
membership. It is not Byzantine-fault tolerant: all quorum nodes share one HMAC
key and a compromised node can lie about its own durability. Byzantine or
multi-administrator deployments need a consensus service with authenticated
per-node identities or threshold signatures behind the same
`RollbackAnchorStore` contract.

For deployment evidence, `meld anchor-qualification history-agent` runs an
ephemeral one-run executor with TLS 1.3/mTLS, an exact controller-certificate
SPKI pin, an independent Ed25519 signing key and a durable request/result
journal. `history-run` concurrently dispatches a strict wave plan to at least
two such agents and derives every controller-log operation from a verified
signed fragment. `history-sign` then binds that exact controller-ordinal log,
two stable full-member probes, a final quorum load and the bounded
linearizability result into a separate Ed25519-signed receipt.
`history-verify` requires the original schema-3 controller log, verifies all
agent fragments, and recomputes both its digest and the checker result offline.
