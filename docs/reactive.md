# Reactive queries

A live query is an initial deterministic query result plus later revisions driven
by committed change batches. Internally, each revision carries a monotonically
increasing commit token. The public compatibility API can emit full snapshots;
the delta API emits one initial snapshot and later ordered transformations.

Canonical queries share one Reactive View keyed by collection and effective
`QuerySpec`. Before/after document images update persistent ordered membership
incrementally; full recomputation is an observable fallback for queue overflow or
explicit resynchronization. One shared immutable delta is generated per changed
view revision and then cloned only at public mutable-document boundaries.

Delivery is ordered per subscriber. Slow consumers are disconnected with a
resumable error rather than allowed to grow memory without bound. Commit
publication only appends to a bounded central queue and never waits for a client.

Initial registration and snapshot publication share the database read boundary,
closing the subscribe/snapshot race window: a subscriber receives Snapshot N and
then only revisions newer than N. If the bounded reactive queue overflows, its
pending work is discarded and affected collections are explicitly rebuilt from a
new atomic snapshot instead of attempting an incomplete delta chain.

A delta transforms exactly `FromToken` into the greater `Token`. Its ordered
operations are remove, add-before, move-before, and change. Application validates
all IDs, documents and anchors and never mutates the input snapshot. This format
preserves sort, skip and limit window changes without sending the complete query
result after every commit.

Reconnect tokens identify durable commit positions, not WebSocket messages.
Authentication and authorization are evaluated both when a subscription starts
and when it resumes.

A revocable query-policy lease binds all output to the policy version that
created the effective query. Revocation closes the lease, blocks new delta or
snapshot publication, waits for in-progress encoding/enqueue operations, removes
the old server subscription, and emits `resync_required`. Reusing the request ID
is safe because removal happens before that control frame is published. The
fresh subscription is authorized from scratch; an old opaque token cannot cross
a changed policy version.

The server signs each resume position and binds it to the database, principal,
tenant, collection, canonical policy-constrained query, policy version, and an
expiry. A replay source must atomically reconstruct the query at N and stream
N+1 onward. Successful resume sends a `resumed` control frame to bind a new
server subscription ID while retaining the client's existing documents; it does
not manufacture a replacement snapshot. V2 implements this source using
historical CatalogRoots, replay leases and commit images. New `Open()` databases
use V2; automatically detected existing V1 databases have no historical images
and safely fall back to `resync_required`. Invalid,
expired, context-mismatched, future, retained-out, corrupt or discontinuous
history does the same on either path.

The WebSocket server supports both legacy snapshot mode and explicit delta mode.
In delta mode, field projection/redaction occurs through a connection-local
visibility overlay. Hidden-only document changes are suppressed and do not
advance the opaque client token, so later visible changes remain strictly chained
without exposing internal commit positions.
