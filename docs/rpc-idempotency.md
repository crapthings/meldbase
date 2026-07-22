# Durable RPC idempotency contract

RPC idempotency is an explicit, opt-in execution contract. It is not a retry
flag and it is not described as exactly-once delivery. Network delivery cannot
prove whether application code ran, and a generic RPC wrapper cannot atomically
commit arbitrary external side effects together with a Meldbase record.

## Guarantees

Within the configured retention window, a correctly implemented durable store
provides these guarantees for one `(actor scope, idempotency key)` pair:

- the first accepted call durably claims execution before its method starts;
- a different method or canonical argument list using the same key conflicts;
- concurrent duplicates do not start another method execution;
- a durably completed result or exposed error is replayed without running the
  method again;
- an interrupted pending claim is never automatically stolen or retried;
- storage failure fails the call closed before application code starts.

The contract therefore gives at-most-one method start plus durable terminal
replay during the retention window. It does **not** claim that arbitrary business
side effects and the terminal result commit atomically. If the process stops
after a side effect but before recording the result, the durable state becomes
`outcome_unknown`. Retrying the key returns that state instead of risking a
duplicate execution.

Transactional methods may atomically commit supported Meldbase point mutations
and the terminal record. That is a stronger, opt-in execution mode built on this
state machine, not a reinterpretation of the wire protocol. Generic methods and
external side effects retain the outcome-unknown boundary described above.

## Wire contract

The existing version-one `call` envelope gains one optional field:

```json
{
  "v": 1,
  "type": "call",
  "requestId": "transport-attempt-id",
  "idempotencyKey": "caller-generated-128-bit-or-stronger-key",
  "method": "orders.checkout",
  "arguments": []
}
```

`requestId` belongs to one HTTP or WebSocket attempt. `idempotencyKey` identifies
the logical operation across attempts and transports. Keys use
`[A-Za-z0-9_-]{22,128}`; clients should generate at least 128 random bits and
must not put user data or credentials in a key.

Omitting the key preserves the current non-idempotent behavior. Supplying a key
when the server has no idempotency store fails with
`rpc_idempotency_unavailable`; it is never silently ignored. HTTP remains the
default transport and WebSocket calls use the identical state machine.

Terminal replay uses the existing `result` or structured `error` envelope. No
transport is allowed to silently retry a call. The stable protocol errors are:

- `rpc_idempotency_conflict` — the key is already bound to another canonical
  method/argument fingerprint;
- `rpc_in_progress` — the same server session currently owns the claim;
- `rpc_outcome_unknown` — a previous session may have executed the method but
  did not durably publish a terminal result;
- `rpc_idempotency_unavailable` — the durable store could not safely claim or
  finish the operation;
- `database_unavailable` — the database was closed or in durability fail-stop
  before method execution could safely begin; callers wait for readiness
  recovery and retry explicitly;
- `rpc_idempotency_required` — a transactional method was called without an
  idempotency key;
- `rpc_transaction_conflict` — the transactional method's immutable base
  snapshot lost optimistic commit validation; staged writes were discarded and
  the conflict terminal is replayable.

## Identity and fingerprint

The server derives, rather than accepts, the storage scope. It hashes a
length-framed encoding of the authenticated actor's `workspaceId` and `id`; raw
identity strings are not stored in the idempotency keyspace. Authentication
adapters must provide non-empty, stable UTF-8 actor values; the server bounds
both values before hashing them.

The request fingerprint is SHA-256 over a versioned, length-framed encoding of:

1. the RPC method name;
2. each argument re-encoded with Meldbase's canonical typed-value codec.

Object field order and superficial JSON spelling therefore cannot change the
fingerprint, while Int64, Float64, Date, Binary and ID remain distinct. The
fingerprint is compared in constant time. Authorization is still evaluated on
every attempt before any cached terminal result is returned, so revoking access
also revokes replay access.

## Persistent state machine

Each record contains only hashes, bounded timestamps, fixed state, ownership
tokens and a bounded canonical result or error code:

```text
absent
  └─ atomic claim ─> pending(session, claim, fingerprint)
                        ├─ atomic finish ─> result(bytes, expiresAt)
                        ├─ atomic finish ─> error(code, status, expiresAt)
                        ├─ explicit uncertainty ─> outcome_unknown(expiresAt)
                        └─ observed by a new session ─> outcome_unknown(expiresAt)
```

There is deliberately no lease-expiry transition from `pending` back to
`absent`. Time cannot prove that an old handler did not commit a side effect.
Completed and unknown records may be removed only after the advertised retention
window. Reusing a key after that window is a new operation by contract.

The store API is compare-and-set based. `Complete` must match the persisted
session and claim tokens; a late handler cannot overwrite another record. Result
encoding and the state transition are one durable publication. A handler result
is not sent to the client until that publication succeeds. Cancellation after
method start is recorded as unknown unless a transaction-aware handler can prove
that no business effect committed.

## Transactional method execution

The Go server has a separate `RPCTransactionalMethods` registry. It cannot be
silently enabled for an ordinary method: construction requires the built-in
durable store created from the exact same `DB`, and every call requires an
`idempotencyKey`.

```go
RPCTransactionalMethods: map[string]server.RPCTransactionalMethod{
    "orders.create": func(
        ctx context.Context,
        actor server.Actor,
        arguments []meldbase.Value,
        tx *meldbase.WriteTransaction,
    ) (meldbase.Value, error) {
        id, err := tx.InsertOne("orders", meldbase.Document{
            "status": meldbase.String("created"),
        })
        if err != nil { return meldbase.Value{}, err }
        return meldbase.ID(id), nil
    },
},
```

`WriteTransaction` exposes `GetOne`, `InsertOne`, `ReplaceOne`, `UpdateOne` and
`DeleteOne`. `UpdateOne` consumes the same compiled data-only `MutationSpec` as
normal collection updates. It reads one immutable snapshot and keeps an isolated point
overlay. The handler does not hold the database writer lock. At commit Meldbase
validates every point read or touched by the handler against the current root,
then publishes document, order, index and Commit Log roots plus the idempotency
result in one COW generation. A competing write to a related point discards the
overlay and durably returns `rpc_transaction_conflict`; a disjoint commit may
serialize first without rejecting the handler. The method is not automatically
rerun.

Transactional handlers must be short, use the supplied transaction for business
data and avoid network calls, normal `DB`/`Collection` writes and external side
effects. Those effects cannot be rolled back by Meldbase and would invalidate
the transactional contract. The transaction is not concurrency-safe and becomes
closed immediately when the callback returns. A handler error or panic discards
all staged writes before its stable error terminal is stored. A successful
handler with no staged write uses the normal terminal-only transition.
Tracked point entries and retained base/current values are admitted against the
database transaction entry/byte limits during the callback, so a read-heavy
handler cannot build an unbounded in-memory overlay before final validation.

First-party server plumbing may additionally stage bounded private System
mutations. The JavaScript server SDK uses this for
`tx.invalidatePublication(collection)`: its policy generation and the
idempotency terminal share the business commit's Catalog/meta publication. The
post-commit lease rotation runs synchronously after durability and before the
matching ChangeBatch is offered to reactive subscribers. It never runs for a
handler rollback, CAS mismatch or optimistic conflict. Policy invalidation is a
business-write operation, not a standalone control-plane RPC.
If no business mutation remains, Meldbase durably records
`rpc_transaction_requires_write` rather than incorrectly marking the outcome
unknown.

## storage placement

The built-in store is below the server package in a private System B+Tree.
A reserved NUL-prefixed entry in the immutable Catalog B+Tree points to a small
system-directory record, which points to the System root. This indirection keeps
the revision-3 `DatabaseRoot` encoding unchanged while making the private root
part of the same atomic Catalog/meta publication. The reserved entry cannot be
created through the public collection API. The tree participates in:

- the same COW root/meta publication and fsync boundary as all state;
- root reachability, reclamation, compaction and corruption validation;
- the database's single-writer sequence so normal commits and realtime resume
  positions remain contiguous;
- bounded overflow storage for canonical terminal results;
- explicit format feature/version handling and operational tooling.

It is not represented as a user collection. Doing so would leak internal
counts, create observable reactive changes, allow name collisions, and mix
retention/security rules with business data.

`server.NewDurableRPCIdempotencyStore(db)` requires an open database and has
and stores bounded inline/overflow values,
compare-and-set transitions and the database's single-writer sequence. System
commits advance the global sequence and reactive resume position without
appearing as public collections, documents, or business changes. Reachability,
reclamation, compaction, reopen recovery, concurrent claims and each system
record ENOSPC publication boundary are tested.

Composite business commits can carry up to 256 distinct System mutations,
including the RPC terminal and durable policy generations. Their disk-full
matrix verifies that recovery never selects a split combination.

Expired terminal and unknown records are reused lazily on a new claim. Operators
may also call `PruneExpired(ctx, limit)` in bounded batches (`1..256`). Pruning
uses compare-and-set deletion, never removes pending records, and is deliberately
not a hidden background goroutine. This keeps maintenance scheduling and I/O
budget under application control.
