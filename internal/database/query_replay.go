package database

import (
	"context"
	"errors"
	"sync"
	"time"

	storage "github.com/crapthings/meldbase/internal/storage"
)

// QueryReplaySource opens a historical query snapshot with an ordered stream of later changes.
//
// Initial.Token must equal afterToken; unavailable history returns ErrHistoryLost.
type QueryReplaySource interface {
	OpenQueryReplay(ctx context.Context, collection string, query QuerySpec, afterToken uint64, buffer int) (*QueryReplaySubscription, error)
}

// QueryReplaySubscription provides a historical query snapshot and its later changes.
type QueryReplaySubscription struct {
	Initial QuerySnapshot
	Deltas  <-chan QueryDelta
	Errors  <-chan error
	cancel  context.CancelFunc
	done    <-chan struct{}
	once    sync.Once
}

// Close cancels replay and waits for delivery to stop.
func (subscription *QueryReplaySubscription) Close() {
	if subscription != nil {
		subscription.once.Do(func() {
			if subscription.cancel != nil {
				subscription.cancel()
			}
			if subscription.done != nil {
				<-subscription.done
			}
		})
	}
}

// queryReplaySource reconstructs and tails queries using retained storage commits.
type queryReplaySource struct {
	file            *storage.File
	deliveryTimeout time.Duration
	onSlowConsumer  func()
	onResourceLimit func()
	resourceLimits  ResourceLimits
}

// OpenQueryReplay starts a query at the requested token and streams its later changes.
func (source *queryReplaySource) OpenQueryReplay(ctx context.Context, collection string, query QuerySpec, afterToken uint64, buffer int) (*QueryReplaySubscription, error) {
	if source == nil || source.file == nil || buffer <= 0 || buffer > 1024 {
		return nil, ErrCorrupt
	}
	snapshot, stream, err := source.file.OpenSnapshotAndStreamAt(afterToken)
	if err != nil {
		if errors.Is(err, storage.ErrHistoryLost) {
			return nil, ErrHistoryLost
		}
		return nil, replayCorrupt(err)
	}
	limits := source.resourceLimits
	if limits.MaxReactiveViewDocuments == 0 || limits.MaxReactiveViewBytes == 0 {
		limits, err = normalizeResourceLimits(limits)
		if err != nil {
			_ = snapshot.Close()
			_ = stream.Close()
			return nil, err
		}
	}
	view, err := newReactiveReplayView(snapshot, collection, query, limits)
	if closeErr := snapshot.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		if errors.Is(err, ErrResourceLimit) && source.onResourceLimit != nil {
			source.onResourceLimit()
		}
		_ = stream.Close()
		return nil, err
	}
	child, cancel := context.WithCancel(ctx)
	deltas := make(chan QueryDelta, buffer)
	errorsOut := make(chan error, 1)
	done := make(chan struct{})
	subscription := &QueryReplaySubscription{Initial: view.Snapshot(), Deltas: deltas, Errors: errorsOut, cancel: cancel, done: done}
	go func() {
		defer close(done)
		defer stream.Close()
		defer close(deltas)
		defer close(errorsOut)
		visibleToken := afterToken
		for {
			batch, err := stream.Next(child)
			if err != nil {
				if child.Err() == nil {
					if errors.Is(err, storage.ErrHistoryLost) {
						err = ErrHistoryLost
					} else {
						err = replayCorrupt(err)
					}
					errorsOut <- err
				}
				return
			}
			_, shared, err := view.ApplyCommit(stream, batch)
			if err != nil {
				if errors.Is(err, ErrResourceLimit) && source.onResourceLimit != nil {
					source.onResourceLimit()
				}
				errorsOut <- err
				return
			}
			if shared == nil {
				continue
			}
			delta := cloneSharedQueryDelta(shared, visibleToken)
			if !source.deliver(child, deltas, delta) {
				if child.Err() == nil && source.onSlowConsumer != nil {
					source.onSlowConsumer()
					errorsOut <- ErrSlowConsumer
				}
				return
			}
			visibleToken = delta.Token
		}
	}()
	return subscription, nil
}

// deliver sends a replay delta until delivery succeeds, times out, or is canceled.
//
// Only a full caller buffer allocates a timer; ending replay releases its
// durable history pin.
func (source *queryReplaySource) deliver(ctx context.Context, deltas chan<- QueryDelta, delta QueryDelta) bool {
	select {
	case <-ctx.Done():
		return false
	case deltas <- delta:
		return true
	default:
	}
	timeout := DefaultReplayDeliveryTimeout
	if source != nil && source.deliveryTimeout > 0 {
		timeout = source.deliveryTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case deltas <- delta:
		return true
	case <-timer.C:
		return false
	}
}
