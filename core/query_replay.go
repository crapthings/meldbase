package meldbase

import (
	"context"
	"errors"
	"sync"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

// QueryReplaySource atomically reconstructs a query at afterToken and tails
// later ordered revisions. Initial.Token must equal afterToken. Implementations
// return ErrHistoryLost when retention can no longer satisfy that contract.
type QueryReplaySource interface {
	OpenQueryReplay(ctx context.Context, collection string, query QuerySpec, afterToken uint64, buffer int) (*QueryReplaySubscription, error)
}

type QueryReplaySubscription struct {
	Initial QuerySnapshot
	Deltas  <-chan QueryDelta
	Errors  <-chan error
	cancel  context.CancelFunc
	done    <-chan struct{}
	once    sync.Once
}

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

// v2QueryReplaySource backs the explicit alpha OpenV2 path. It remains internal
// so callers depend on the transport-independent QueryReplaySource contract,
// not Storage V2 implementation details.
type v2QueryReplaySource struct {
	file            *storagev2.File
	deliveryTimeout time.Duration
	onSlowConsumer  func()
	onResourceLimit func()
	resourceLimits  ResourceLimits
}

func (source *v2QueryReplaySource) OpenQueryReplay(ctx context.Context, collection string, query QuerySpec, afterToken uint64, buffer int) (*QueryReplaySubscription, error) {
	if source == nil || source.file == nil || buffer <= 0 || buffer > 1024 {
		return nil, ErrCorrupt
	}
	snapshot, stream, err := source.file.OpenSnapshotAndStreamAt(afterToken)
	if err != nil {
		if errors.Is(err, storagev2.ErrHistoryLost) {
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
	view, err := newV2ReactiveReplayView(snapshot, collection, query, limits)
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
					if errors.Is(err, storagev2.ErrHistoryLost) {
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

// deliver bounds a stalled replay consumer. The immediate send preserves the
// normal zero-allocation path; only a full caller buffer allocates a timer.
// Ending the stream releases its durable replay pin so retention can recover.
func (source *v2QueryReplaySource) deliver(ctx context.Context, deltas chan<- QueryDelta, delta QueryDelta) bool {
	select {
	case <-ctx.Done():
		return false
	case deltas <- delta:
		return true
	default:
	}
	timeout := DefaultV2ReplayDeliveryTimeout
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
