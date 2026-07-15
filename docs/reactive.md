# Reactive queries

A live query is an initial deterministic query result plus later revisions driven
by committed change batches. Each message carries a monotonically increasing
commit token and a subscription-local revision.

V1 recomputes a query after a relevant collection commit and sends a full
snapshot only when document IDs or returned values changed. Delivery is ordered
per connection. Slow consumers are disconnected with a resumable error rather
than allowed to grow memory without bound.

Recomputation reads the current database token and documents under the same read
lock. If several commits arrive before evaluation, intermediate notifications are
coalesced and the snapshot is labeled with the latest token; a snapshot is never
labeled with an older token than the state it contains. Initial watch registration
precedes the initial snapshot, closing the subscribe/snapshot race window.

Reconnect tokens identify durable commit positions, not WebSocket messages.
Authentication and authorization are evaluated both when a subscription starts
and when it resumes.

The server signs each resume position and binds it to the database, principal,
tenant, collection, canonical policy-constrained query, policy version, and an
expiry. The most recent 1,024 contiguous commit positions survive checkpoints
and WAL recovery; historical document images are not duplicated for this purpose.
A valid position inside that window resumes with an atomic current
snapshot in V1; invalid, expired, context-mismatched, future, or aged-out tokens
require a clean resync.
