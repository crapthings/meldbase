# Architecture

## Product contract

Meldbase is an embedded, single-node document database whose distinguishing
feature is that a query can remain live after its initial result. It owns its
file format, query semantics, change tokens, and network protocol. Familiar
operator names are an ergonomic influence, not a compatibility promise.

The dependency direction is:

```text
server / SDK
    -> reactive query coordinator
        -> planner and execution
            -> transactions and catalog
                -> indexes and record store
                    -> pager and WAL
```

Lower layers never import server, transport, or UI concepts. A committed change
is the single source for storage visibility, index maintenance, and live events.

## Decisions that supersede the initial sketch

### A closed value model

The engine does not store `any` inside a tagged `Value`. Each kind has a checked
constructor and private representation. This makes invalid states unrepresentable
at the storage, comparison, and index-key boundaries. Loose Go values are accepted
only through an explicit conversion function.

Object field order is not semantically significant. Numeric values retain their
integer or floating-point kind. Cross-kind numeric comparison is specified by the
query layer rather than accidentally inherited from Go or JSON.

### Query language, not MongoDB emulation

Filters are compiled to an internal expression tree before execution. Unknown
operators and malformed operands are errors. We document every operator and do
not inherit undocumented edge cases from another product. This leaves room for
first-class live-query and local-first operators later.

### Commit and recovery

The first durable engine will use a single writer and redo-only, no-steal
transactions:

1. Build private page images and index changes.
2. Append transaction records and a commit record to the WAL.
3. `fsync` the WAL.
4. Publish the committed version to readers.
5. Write dirty pages later during checkpoint.
6. Emit the change event from the committed transaction record.

Uncommitted page images are never written to the database file. Recovery redoes
only transactions with a valid commit record; replay is idempotent through page
LSNs. This is simpler and safer than mixing page writes with an undefined undo
policy. The WAL may be a sidecar during development. A release may still call
the database “single-file” only when normal operation does not require one.

### Stable live-query boundary

Subscriptions consume ordered commit batches rather than observing collection
methods directly. This prevents events for failed transactions and allows the
same log position to support reconnect, audit, and future replication. V1 may
re-run affected queries and send snapshots; the protocol includes revision and
resume tokens so incremental diffs can be added without changing correctness.

## Concurrency model

Milestone one uses collection-level read/write locks. The durable milestone moves
the lock boundary to a database transaction manager: one writer, snapshot readers,
and immutable committed roots. User callbacks are never invoked while an engine
lock is held.

## Package boundaries

- Root package `meldbase`: public embedded API and stable value types.
- `internal/storage`: page IO, checksums, allocation, and record identifiers.
- `internal/wal`: framing, durability, and recovery scanning.
- `internal/query`: parsed expression tree and evaluation.
- `internal/index`: ordered key encoding and B+Tree implementation.
- `internal/reactive`: subscriptions driven by committed batches.
- `internal/server`: HTTP/WebSocket adapters only.

Packages are introduced when they have executable behavior; empty architecture
folders are deliberately avoided.
