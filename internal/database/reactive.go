package database

import (
	"context"
	"sync"
)

type QuerySnapshot struct {
	Token     uint64
	Documents []Document
}

type QuerySubscription struct {
	Snapshots <-chan QuerySnapshot
	Errors    <-chan error
	cancel    context.CancelFunc
	once      sync.Once
}

// QueryDeltaSubscription returns one safe initial snapshot and then ordered
// deltas. It is the preferred core stream for transports and reactive clients;
// QuerySubscription remains the full-snapshot compatibility adapter.
type QueryDeltaSubscription struct {
	Initial QuerySnapshot
	Deltas  <-chan QueryDelta
	Errors  <-chan error
	cancel  context.CancelFunc
	once    sync.Once
}

func (s *QuerySubscription) Close() {
	if s != nil {
		s.once.Do(s.cancel)
	}
}

func (s *QueryDeltaSubscription) Close() {
	if s != nil {
		s.once.Do(s.cancel)
	}
}

func (c *Collection) SnapshotQuery(ctx context.Context, query QuerySpec) (QuerySnapshot, error) {
	if err := contextError(ctx); err != nil {
		return QuerySnapshot{}, err
	}
	if err := c.validate(); err != nil {
		return QuerySnapshot{}, err
	}
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	if c.db.closed {
		return QuerySnapshot{}, ErrClosed
	}
	if c.db.querySource != nil {
		documents, _, err := c.planStorageLocked(ctx, query)
		if err != nil {
			return QuerySnapshot{}, err
		}
		return QuerySnapshot{Token: c.db.token, Documents: documents}, nil
	}
	return snapshotQueryUnlocked(c.db, c.name, query), nil
}

func (c *Collection) SubscribeQuery(ctx context.Context, query QuerySpec, buffer int) (*QuerySubscription, error) {
	subscriber, cancel, err := c.subscribeSharedQuery(ctx, query, buffer, false)
	if err != nil {
		return nil, err
	}
	return &QuerySubscription{Snapshots: subscriber.snapshots, Errors: subscriber.errors, cancel: cancel}, nil
}

func (c *Collection) SubscribeQueryDeltas(ctx context.Context, query QuerySpec, buffer int) (*QueryDeltaSubscription, error) {
	subscriber, cancel, err := c.subscribeSharedQuery(ctx, query, buffer, true)
	if err != nil {
		return nil, err
	}
	return &QueryDeltaSubscription{Initial: subscriber.initial, Deltas: subscriber.deltas, Errors: subscriber.errors, cancel: cancel}, nil
}

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

func cloneSnapshot(snapshot QuerySnapshot) QuerySnapshot {
	result := QuerySnapshot{Token: snapshot.Token, Documents: make([]Document, len(snapshot.Documents))}
	for i := range snapshot.Documents {
		result.Documents[i] = snapshot.Documents[i].Clone()
	}
	return result
}
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
