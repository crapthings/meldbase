package meldbase

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

func (s *QuerySubscription) Close() {
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
	documents := []Document{}
	if data := c.db.collections[c.name]; data != nil {
		documents = make([]Document, 0, len(data.order))
		for _, id := range data.order {
			if document, ok := data.documents[id]; ok {
				documents = append(documents, document)
			}
		}
	}
	return QuerySnapshot{Token: c.db.token, Documents: query.Execute(documents)}, nil
}

func (c *Collection) SubscribeQuery(ctx context.Context, query QuerySpec, buffer int) (*QuerySubscription, error) {
	if buffer <= 0 || buffer > 1024 {
		return nil, ErrSlowConsumer
	}
	child, cancel := context.WithCancel(ctx)
	batches, feedErrors, err := c.db.WatchChanges(child, c.name, buffer)
	if err != nil {
		cancel()
		return nil, err
	}
	initial, err := c.SnapshotQuery(child, query)
	if err != nil {
		cancel()
		return nil, err
	}
	snapshots := make(chan QuerySnapshot, buffer)
	errorsOut := make(chan error, 1)
	snapshots <- cloneSnapshot(initial)
	subscription := &QuerySubscription{Snapshots: snapshots, Errors: errorsOut, cancel: cancel}
	go func() {
		defer close(snapshots)
		defer close(errorsOut)
		defer cancel()
		lastToken, lastDocuments := initial.Token, initial.Documents
		for {
			select {
			case <-child.Done():
				return
			case err, ok := <-feedErrors:
				if ok && err != nil && err != context.Canceled {
					select {
					case errorsOut <- err:
					default:
					}
				}
				return
			case batch, ok := <-batches:
				if !ok {
					return
				}
				if batch.Token <= lastToken {
					continue
				}
				next, err := c.SnapshotQuery(child, query)
				if err != nil {
					select {
					case errorsOut <- err:
					default:
					}
					return
				}
				if next.Token <= lastToken {
					continue
				}
				changed := !documentSlicesEqual(lastDocuments, next.Documents)
				lastToken = next.Token
				lastDocuments = next.Documents
				if !changed {
					continue
				}
				select {
				case snapshots <- cloneSnapshot(next):
				case <-child.Done():
					return
				default:
					select {
					case errorsOut <- ErrSlowConsumer:
					default:
					}
					return
				}
			}
		}
	}()
	return subscription, nil
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
