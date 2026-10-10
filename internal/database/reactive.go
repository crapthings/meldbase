package database

import (
	"context"
	"sync"
)

// QuerySnapshot pairs an ordered query result with its commit token.
type QuerySnapshot struct {
	Token     uint64
	Documents []Document
}

// QuerySubscription provides full query snapshots as the result changes.
type QuerySubscription struct {
	Snapshots <-chan QuerySnapshot
	Errors    <-chan error
	cancel    context.CancelFunc
	once      sync.Once
}

// QueryDeltaSubscription provides an initial snapshot followed by ordered result changes.
//
// Transports and reactive clients should prefer it to the full-snapshot adapter.
type QueryDeltaSubscription struct {
	Initial QuerySnapshot
	Deltas  <-chan QueryDelta
	Errors  <-chan error
	cancel  context.CancelFunc
	once    sync.Once
}

// Close cancels full-snapshot delivery and is safe to call repeatedly.
func (s *QuerySubscription) Close() {
	if s != nil {
		s.once.Do(s.cancel)
	}
}

// Close cancels delta delivery and is safe to call repeatedly.
func (s *QueryDeltaSubscription) Close() {
	if s != nil {
		s.once.Do(s.cancel)
	}
}

// SnapshotQuery reads query results and their token from the same committed state.
func (c *Collection) SnapshotQuery(ctx context.Context, query QuerySpec) (QuerySnapshot, error) {
	if err := query.Validate(); err != nil {
		return QuerySnapshot{}, err
	}
	if err := contextError(ctx); err != nil {
		return QuerySnapshot{}, err
	}
	if err := c.validate(); err != nil {
		return QuerySnapshot{}, err
	}
	budget, err := c.db.newQueryBudget(query)
	if err != nil {
		return QuerySnapshot{}, err
	}
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	if c.db.closed {
		return QuerySnapshot{}, ErrClosed
	}
	if c.db.querySource != nil {
		documents, _, err := c.planStorageLocked(ctx, query, budget)
		if err != nil {
			return QuerySnapshot{}, err
		}
		return QuerySnapshot{Token: c.db.token, Documents: documents}, nil
	}
	return snapshotQueryBudgetedUnlocked(ctx, c.db, c.name, query, budget)
}

// SubscribeQuery starts a bounded stream of full snapshots for a compiled query.
func (c *Collection) SubscribeQuery(ctx context.Context, query QuerySpec, buffer int) (*QuerySubscription, error) {
	subscriber, cancel, err := c.subscribeSharedQuery(ctx, query, buffer, false)
	if err != nil {
		return nil, err
	}
	return &QuerySubscription{Snapshots: subscriber.snapshots, Errors: subscriber.errors, cancel: cancel}, nil
}

// SubscribeQueryDeltas returns an initial snapshot and a bounded stream of ordered changes.
func (c *Collection) SubscribeQueryDeltas(ctx context.Context, query QuerySpec, buffer int) (*QueryDeltaSubscription, error) {
	subscriber, cancel, err := c.subscribeSharedQuery(ctx, query, buffer, true)
	if err != nil {
		return nil, err
	}
	return &QueryDeltaSubscription{Initial: subscriber.initial, Deltas: subscriber.deltas, Errors: subscriber.errors, cancel: cancel}, nil
}

// subscribeSharedQuery validates a request and registers it with a shared live view.
func (c *Collection) subscribeSharedQuery(ctx context.Context, query QuerySpec, buffer int, deltas bool) (*sharedQuerySubscriber, context.CancelFunc, error) {
	if buffer <= 0 || buffer > 1024 {
		return nil, nil, ErrSlowConsumer
	}
	if err := contextError(ctx); err != nil {
		return nil, nil, err
	}
	if err := c.validate(); err != nil {
		return nil, nil, err
	}
	if err := query.Validate(); err != nil {
		return nil, nil, err
	}
	canonical, err := MarshalQuerySpecJSON(query)
	if err != nil {
		return nil, nil, err
	}
	child, cancel := context.WithCancel(ctx)
	if c.db.reactive == nil {
		cancel()
		return nil, nil, ErrClosed
	}
	subscriber, err := c.db.reactive.subscribe(child, c.name, query, canonical, buffer, cancel, deltas)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return subscriber, cancel, nil
}

// cloneSnapshot copies a snapshot and all of its documents.
func cloneSnapshot(snapshot QuerySnapshot) QuerySnapshot {
	result := QuerySnapshot{Token: snapshot.Token, Documents: make([]Document, len(snapshot.Documents))}
	for i := range snapshot.Documents {
		result.Documents[i] = snapshot.Documents[i].Clone()
	}
	return result
}

// documentSlicesEqual compares document contents in slice order.
func documentSlicesEqual(left, right []Document) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !left[i].Equal(right[i]) {
			return false
		}
	}
	return true
}
