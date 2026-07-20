# Rollback-anchor service

`meld anchor-serve` runs one member of the independently trusted rollback-anchor
plane. It is deliberately separate from `meld serve`: the database and its
rollback floor must not share one process, disk or failure domain.

## Static deployment

Provision an odd number of members, normally three, on independent hosts and
storage. Every member receives the same stable cluster ID and complete member
list, but a different `--member` value and private data directory:

```sh
install -d -m 0700 /var/lib/meldbase-anchor
umask 077
openssl rand -base64 32 > /etc/meldbase/anchor-primary.key
chmod 0600 /etc/meldbase/anchor-primary.key

meld anchor-serve \
  --addr 0.0.0.0:8443 \
  --dir /var/lib/meldbase-anchor \
  --cluster orders-production \
  --members anchor-a,anchor-b,anchor-c \
  --member anchor-a \
  --tls-cert /etc/meldbase/server.crt \
  --tls-key /etc/meldbase/server.key \
  --client-ca /etc/meldbase/database-client-ca.crt \
  --key 2026-primary=/etc/meldbase/anchor-primary.key
```

The service requires TLS 1.3 and a client certificate signed by `--client-ca`.
The application-layer HMAC is still mandatory. TLS authenticates and encrypts
the connection; HMAC binds the exact cluster configuration, expected member,
request path, timestamp and body. The TLS private key and HMAC files, and the
data directory, must not grant group or other-user permissions. Symlinks and
unbounded files are rejected.

The server certificate must explicitly contain Server Authentication extended
key usage and Digital Signature key usage. Supply certificates with the member's
real DNS names or IP addresses; clients must use ordinary hostname verification.

## Database client

The development database CLI can connect directly to the static quorum. Its
client certificate must explicitly allow Client Authentication and Digital
Signature, and its private key and HMAC file must be mode `0600` or stricter:

```sh
meld serve \
  --db /var/lib/app/orders.meld \
  --jwt-hs256-secret-file /etc/meldbase/jwt-hs256.secret \
  --jwt-issuer https://identity.example/ \
  --jwt-audience meldbase-api \
  --access-policy-file /etc/meldbase/access-policy.json \
  --rollback-anchor-cluster orders-production \
  --rollback-anchor-replica anchor-a=https://anchor-a.example.com:8443 \
  --rollback-anchor-replica anchor-b=https://anchor-b.example.com:8443 \
  --rollback-anchor-replica anchor-c=https://anchor-c.example.com:8443 \
  --rollback-anchor-name orders-database \
  --rollback-anchor-key-id 2026-primary \
  --rollback-anchor-key-file /etc/meldbase/anchor-primary.key \
  --rollback-anchor-ca /etc/meldbase/anchor-server-ca.crt \
  --rollback-anchor-client-cert /etc/meldbase/database-client.crt \
  --rollback-anchor-client-key /etc/meldbase/database-client.key \
  --rollback-anchor-timeout 10s \
  --rollback-anchor-init
```

Use `--rollback-anchor-init` only during the first audited provisioning and
remove it from every subsequent start. Local `--rollback-anchor` and the remote
flags are mutually exclusive. Partial remote configuration is rejected before
the database opens. The trust-plane HTTP transport deliberately ignores
environment proxy variables, requires TLS 1.3 and performs normal DNS/IP
certificate verification.

The JWT settings apply to the database HTTP API, not to the anchor connection.
The anchor trust plane remains independently authenticated while the database
server derives tenant scope from verified application tokens.

## Immutable membership

The configuration digest is derived from the cluster ID and the complete sorted
member list. Each directory is fsynced and permanently bound to that digest and
one member ID before serving. Changing `--cluster`, `--members` or `--member`
against an existing directory fails startup. Multiple URLs pointing to one
member cannot manufacture a quorum because every signed request names the
expected member.

There is no live membership change command. Do not replace the member list by
editing flags. A future reconfiguration design must transfer the retained floor
through an intersecting joint configuration or a separately audited offline
procedure.

## Probes and shutdown

`GET /livez` and `GET /readyz` return `204` through the same mTLS listener.
They expose no anchor names, database identity or retained coordinates.
`readyz` turns unavailable when graceful shutdown begins. `SIGTERM` and
`SIGINT` stop accepting traffic and wait up to `--shutdown-timeout` for active
requests.

Readiness proves that the process loaded its configuration and is accepting
TLS. Individual anchor I/O can still fail later and returns `503`; clients must
retain their operation deadline and alert on endpoint/quorum failure counters.

## Key rotation

Repeat `--key` to install both old and new key IDs on all members. Move database
clients to the new ID only after every member has restarted successfully. Remove
the old ID after the maximum request duration plus clock-skew window. Never
reuse one cluster's HMAC key for another trust plane.

## Partition qualification

Before production, exercise the exact hosts, network and storage:

1. Initialize a disposable database and anchor name through all three members.
2. Block all traffic to one member. Confirm advances still succeed and endpoint
   failure telemetry increases.
3. Block a second member. Confirm the next database publication fails closed and
   writes become disabled; never treat the operation outcome as known.
4. Restart the database from its last verified file and restore one member.
   Confirm the anchor read/advance handshake succeeds before writes resume.
5. Restart the stale member with its original directory and confirm a later
   advance repairs it.
6. Point two configured URLs at one physical member and confirm
   `ErrConfiguration`, not quorum success.
7. Repeat while killing a member during its file fsync/rename window and on the
   production filesystem or volume type.

The repository integration test performs the same quorum/partition/rejoin flow
over real TCP, TLS 1.3 and mTLS on one host. It is evidence for protocol behavior,
not a substitute for independent-machine power, routing and storage tests.

## Signed multi-host qualification chain

Generate a dedicated receipt-signing key once on a secured qualification host:

```sh
meld anchor-qualification keygen \
  --private /secure/anchor-qualification.key \
  --public /secure/anchor-qualification.pub
```

The private file is created exclusively with mode `0600`; existing files are
never overwritten. Keep it separate from the anchor HMAC and TLS keys.

Stop the disposable database process before every probe. Run these phases in
order while changing the real network state with an independently logged
controller:

```text
healthy → degraded → minority → recovered → rollback-rejected
```

Every probe uses the same remote connection flags as the database, without the
`rollback-anchor-` prefix:

```sh
meld anchor-qualification probe \
  --phase healthy \
  --db /qualification/orders.meld \
  --out /evidence/01-healthy.json \
  --signing-key /secure/anchor-qualification.key \
  --source-revision "$(git rev-parse HEAD)" \
  --require-clean-source \
  --external-evidence-sha256 "$DEPLOYMENT_RECORD_SHA256" \
  --cluster orders-production \
  --replica anchor-a=https://anchor-a.example.com:8443 \
  --replica anchor-b=https://anchor-b.example.com:8443 \
  --replica anchor-c=https://anchor-c.example.com:8443 \
  --anchor-name qualification-orders \
  --key-id 2026-primary \
  --key-file /etc/meldbase/anchor-primary.key \
  --ca /etc/meldbase/anchor-server-ca.crt \
  --client-cert /etc/meldbase/database-client.crt \
  --client-key /etc/meldbase/database-client.key
```

Later phases add `--previous` pointing to the immediately preceding receipt.
`--external-evidence-sha256` hashes the exact secured deployment, firewall,
route, cloud-control-plane or recovery log for that phase. The tool observes
reachability; this digest prevents it from pretending to have caused a physical
partition.

The phase requirements are strict:

- `healthy`: every configured member responds with an existing anchor.
- `degraded`: exactly the maximum crash-fault set is unavailable while quorum
  load still succeeds.
- `minority`: more than the tolerated set is unavailable and quorum load fails.
- `recovered`: every member responds and the database completes its anchor open
  handshake.
- `rollback-rejected`: after installing a retained older disposable database
  image, every member responds and opening returns `ErrRollbackDetected`.

Receipts bind the protocol/configuration digest, hashed endpoint mapping,
per-member classifications and coordinates, offline graph verification, runtime,
timeline, previous receipt, external evidence digest and Ed25519 signature.
They contain no HMAC, private key or raw endpoint URL.

Verify the complete chain offline:

```sh
meld anchor-qualification verify \
  --public-key /secure/anchor-qualification.pub \
  --require-complete \
  --receipt /evidence/01-healthy.json \
  --receipt /evidence/02-degraded.json \
  --receipt /evidence/03-minority.json \
  --receipt /evidence/04-recovered.json \
  --receipt /evidence/05-rollback-rejected.json
```

The verifier rejects unknown fields, altered signatures, another key,
skipped/replayed/reordered phases, changed endpoints or identities, overlapping
timelines, premature sequence regression and a final image that is not older
than the recovered image. Signing makes later modification detectable; it does
not make a compromised qualification host truthful.

## Signed concurrent-history qualification

Use a separate disposable anchor name, at least two independently operated
execution agents, and an independent controller. Generate a different Ed25519
key pair for every agent. The controller's mTLS leaf certificate is also an
authorization capability: calculate its subject-public-key-info SHA-256 and pin
that exact digest on every agent. Trusting the controller CA without the SPKI
pin is insufficient because another certificate issued by that CA could submit
qualification operations.

Each agent requires an empty private state directory. Before touching the
anchor it durably writes the exact request reservation. It durably writes the
signed result before replying. A retry with the same operation ID and request
returns the cached result; changed content is rejected. On restart, a completed
result is replayed without executing again and a reserved incomplete operation
may resume. The agent binds its directory to one run ID, anchor configuration,
agent ID and signing key, so use a fresh directory for every qualification run:

```sh
meld anchor-qualification history-agent \
  --agent-id writer-a \
  --state-dir /var/lib/meldbase-qualification/run-001/writer-a \
  --source-revision "$(git rev-parse HEAD)" \
  --require-clean-source \
  --addr :9443 \
  --tls-cert /etc/meldbase/agent-server.crt \
  --tls-key /etc/meldbase/agent-server.key \
  --controller-ca /etc/meldbase/controller-ca.crt \
  --controller-spki-sha256 "$CONTROLLER_LEAF_SPKI_SHA256" \
  --signing-key /secure/writer-a-signing.key \
  --cluster orders-production \
  --replica anchor-a=https://anchor-a.example.com:8443 \
  --replica anchor-b=https://anchor-b.example.com:8443 \
  --replica anchor-c=https://anchor-c.example.com:8443 \
  --anchor-name qualification-history-orders \
  --key-id 2026-primary \
  --key-file /etc/meldbase/anchor-primary.key \
  --anchor-ca /etc/meldbase/anchor-server-ca.crt \
  --anchor-client-cert /etc/meldbase/anchor-client.crt \
  --anchor-client-key /etc/meldbase/anchor-client.key
```

The controller consumes a strict schema-2 plan. Operations in one wave are
dispatched concurrently; the next wave starts only after every operation in
the preceding wave has returned. Every declared agent must execute at least one
operation. The complete plan supports at most 23 operations, leaving one slot
for the signer's final quorum load:

```json
{
  "schemaVersion": 2,
  "runId": "00112233445566778899aabbccddeeff",
  "sourceRevision": "0123456789abcdef0123456789abcdef01234567",
  "agents": [
    {"agentId": "writer-a", "endpoint": "https://writer-a.example.com:9443", "signingPublicKey": "BASE64_ED25519_PUBLIC_KEY_A"},
    {"agentId": "writer-b", "endpoint": "https://writer-b.example.com:9443", "signingPublicKey": "BASE64_ED25519_PUBLIC_KEY_B"}
  ],
  "waves": [
    {"operations": [
      {"id": "crossed-a", "agentId": "writer-a", "kind": "advance", "target": {"exists": true, "databaseIdHex": "0123456789abcdef0123456789abcdef", "minimumCommitSequence": 10, "minimumGeneration": 21}},
      {"id": "crossed-b", "agentId": "writer-b", "kind": "advance", "target": {"exists": true, "databaseIdHex": "0123456789abcdef0123456789abcdef", "minimumCommitSequence": 11, "minimumGeneration": 20}}
    ]}
  ]
}
```

`kind` is `load` or `advance`; a load omits its zero `target`. `runId` is 16
random bytes encoded as lowercase hexadecimal. Run the plan using controller
mTLS credentials for the agents and separate anchor credentials for the initial
quorum observation:

```sh
meld anchor-qualification history-run \
  --plan /evidence/history-plan.json \
  --out /evidence/controller-history.json \
  --source-revision "$(git rev-parse HEAD)" \
  --require-clean-source \
  --agent-ca /etc/meldbase/agent-server-ca.crt \
  --agent-client-cert /etc/meldbase/controller-client.crt \
  --agent-client-key /etc/meldbase/controller-client.key \
  --cluster orders-production \
  --replica anchor-a=https://anchor-a.example.com:8443 \
  --replica anchor-b=https://anchor-b.example.com:8443 \
  --replica anchor-c=https://anchor-c.example.com:8443 \
  --anchor-name qualification-history-orders \
  --key-id 2026-primary \
  --key-file /etc/meldbase/anchor-primary.key \
  --ca /etc/meldbase/anchor-server-ca.crt \
  --client-cert /etc/meldbase/anchor-client.crt \
  --client-key /etc/meldbase/anchor-client.key
```

The generated controller history is schema 3. The controller assigns every
wave's unique invocation ordinals before releasing its dispatch barrier and a
return ordinal after validating each signed response. These ordinals
conservatively bracket the real call and prove operations in one wave overlap;
agent wall clocks are evidence about leases, not a source of cross-host ordering.
Every operation is derived from, and must exactly match, one agent-signed
fragment containing the request digest, actual outcome and observed value.
The controller record and every fragment also bind source/build revision,
dirty status and runtime identity; release qualification rejects an old or
dirty agent even when its signature is otherwise valid.
Transport response loss triggers one idempotent retry of the same request.
Operations whose intervals overlap may be linearized in either order. Every
failed advance remains ambiguous because it may already have taken effect.

After the controller has quiesced all writers, sign the exact log. Use the same
remote TLS/HMAC flags as the five-phase probe:

```sh
meld anchor-qualification history-sign \
  --history /evidence/controller-history.json \
  --out /evidence/history-receipt.json \
  --signing-key /secure/anchor-qualification.key \
  --source-revision "$(git rev-parse HEAD)" \
  --require-clean-source \
  --external-evidence-sha256 "$FAULT_CONTROLLER_LOG_SHA256" \
  --cluster orders-production \
  --replica anchor-a=https://anchor-a.example.com:8443 \
  --replica anchor-b=https://anchor-b.example.com:8443 \
  --replica anchor-c=https://anchor-c.example.com:8443 \
  --anchor-name qualification-history-orders \
  --key-id 2026-primary \
  --key-file /etc/meldbase/anchor-primary.key \
  --ca /etc/meldbase/anchor-server-ca.crt \
  --client-cert /etc/meldbase/database-client.crt \
  --client-key /etc/meldbase/database-client.key
```

The signer hashes the exact controller file, requires every member to be
reachable, performs a full-member probe, a final quorum load and a second
identical full-member probe, appends that load after the controller ordinals,
and runs the bounded linearizability checker. It writes no receipt unless the
history is linearizable and the recorded members support the final load.

Verify offline with all three evidence objects:

```sh
meld anchor-qualification history-verify \
  --public-key /secure/anchor-qualification.pub \
  --receipt /evidence/history-receipt.json \
  --history /evidence/controller-history.json
```

Verification strictly parses both files, verifies every independent agent
signature and request binding, checks the receipt signer, recomputes the
controller SHA-256, rejects changed ordinals or operations, reruns the checker
and validates final quorum support. The external evidence digest should identify
the independently secured process, firewall, route, host-failure and agent
journal collection. Signatures detect later changes but cannot make a
simultaneously compromised controller and agent truthful.
