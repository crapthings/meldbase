package database

import (
	"context"
	"fmt"
)

// changeWatcher owns one bounded queue of collection changes and its delivery lifecycle.
type changeWatcher struct {
	collection   string
	afterToken   uint64
	maxBatches   int
	maxBytes     uint64
	events       chan ChangeBatch
	done         chan error
	queue        []queuedChangeBatch // protected by DB.feedMu
	pendingBytes uint64              // protected by DB.feedMu
	changed      chan struct{}       // protected by DB.feedMu
	stop         chan struct{}       // closed under DB.feedMu
	stopped      chan struct{}
	closed       bool
	terminalErr  error
}

// queuedChangeBatch pairs a retained change batch with its byte charge.
type queuedChangeBatch struct {
	batch ChangeBatch
	bytes uint64
}

// WatchChanges subscribes to future collection changes through a bounded delivery queue.
func (db *DB) WatchChanges(ctx context.Context, collection string, buffer int) (<-chan ChangeBatch, <-chan error, error) {
	if !collectionNamePattern.MatchString(collection) {
		return nil, nil, ErrInvalidCollection
	}
	if buffer <= 0 || buffer > 4096 {
		return nil, nil, fmt.Errorf("invalid change buffer")
	}
	// Holding the database read lock until the watcher is registered creates a
	// precise subscription boundary. A dispatcher may still hold older batches
	// that were acknowledged before this call; afterToken prevents those from
	// being delivered to a newly registered watcher.
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, nil, ErrClosed
	}
	afterToken := db.token
	db.feedMu.Lock()
	db.nextWatcher++
	id := db.nextWatcher
	watcher := &changeWatcher{
		collection: collection, afterToken: afterToken, maxBatches: buffer, maxBytes: maxPendingChangeWatcherBytes,
		events: make(chan ChangeBatch), done: make(chan error, 1),
		changed: make(chan struct{}), stop: make(chan struct{}), stopped: make(chan struct{}),
	}
	db.watchers[id] = watcher
	db.feedMu.Unlock()
	db.mu.RUnlock()
	go db.runChangeWatcher(watcher)
	go func() {
		<-ctx.Done()
		db.feedMu.Lock()
		if current, ok := db.watchers[id]; ok {
			db.finishChangeWatcherLocked(id, current, ctx.Err())
		}
		db.feedMu.Unlock()
	}()
	return watcher.events, watcher.done, nil
}

// deliverChangeWatchers queues copied collection changes and closes overloaded watchers.
func (db *DB) deliverChangeWatchers(batch ChangeBatch) {
	if db == nil || len(batch.Changes) == 0 {
		return
	}
	db.feedMu.Lock()
	defer db.feedMu.Unlock()
	for id, watcher := range db.watchers {
		if batch.Token <= watcher.afterToken {
			continue
		}
		filtered := make([]Change, 0, len(batch.Changes))
		for _, change := range batch.Changes {
			if watcher.collection == change.Collection {
				filtered = append(filtered, cloneChange(change))
			}
		}
		if len(filtered) == 0 {
			continue
		}
		delivery := ChangeBatch{Token: batch.Token, Changes: filtered}
		bytes := changeBatchDispatchBytes(delivery)
		if watcher.maxBatches <= 0 || watcher.maxBytes == 0 || len(watcher.queue) >= watcher.maxBatches ||
			bytes > watcher.maxBytes || watcher.pendingBytes > watcher.maxBytes-bytes ||
			bytes > maxPendingChangeWatchersBytes || db.pendingWatcherBytes > maxPendingChangeWatchersBytes-bytes {
			db.metrics.slowConsumers.Add(1)
			db.finishChangeWatcherLocked(id, watcher, ErrSlowConsumer)
			continue
		}
		watcher.queue = append(watcher.queue, queuedChangeBatch{batch: delivery, bytes: bytes})
		watcher.pendingBytes += bytes
		db.pendingWatcherBytes += bytes
		db.metrics.watcherDeliveries.Add(1)
		close(watcher.changed)
		watcher.changed = make(chan struct{})
	}
}

// failChangeWatchers stops all direct change watchers with the supplied error.
func (db *DB) failChangeWatchers(err error) {
	if db == nil {
		return
	}
	db.feedMu.Lock()
	defer db.feedMu.Unlock()
	for id, watcher := range db.watchers {
		db.metrics.slowConsumers.Add(1)
		db.finishChangeWatcherLocked(id, watcher, err)
	}
}

// finishChangeWatcherLocked stops a watcher and releases its queue charges under db.feedMu.
func (db *DB) finishChangeWatcherLocked(id uint64, watcher *changeWatcher, err error) {
	if watcher == nil || watcher.closed {
		return
	}
	delete(db.watchers, id)
	watcher.closed = true
	watcher.terminalErr = err
	if watcher.pendingBytes > db.pendingWatcherBytes {
		// All queue ownership changes are serialized by feedMu. A mismatch means
		// an internal accounting fault; retaining an underflowed gauge would be
		// less safe than dropping the watcher backlog at this terminal boundary.
		db.pendingWatcherBytes = 0
	} else {
		db.pendingWatcherBytes -= watcher.pendingBytes
	}
	watcher.queue = nil
	watcher.pendingBytes = 0
	close(watcher.stop)
}

// runChangeWatcher forwards a watcher's bounded queue to its public channel.
//
// Only this goroutine sends or closes events, avoiding a close/send race while
// commit delivery stays non-blocking under feedMu.
func (db *DB) runChangeWatcher(watcher *changeWatcher) {
	if db == nil || watcher == nil {
		return
	}
	defer close(watcher.stopped)
	defer close(watcher.events)
	defer func() {
		db.feedMu.Lock()
		err := watcher.terminalErr
		db.feedMu.Unlock()
		if err != nil {
			watcher.done <- err
		}
		close(watcher.done)
	}()
	for {
		db.feedMu.Lock()
		if watcher.closed {
			db.feedMu.Unlock()
			return
		}
		if len(watcher.queue) == 0 {
			changed, stop := watcher.changed, watcher.stop
			db.feedMu.Unlock()
			select {
			case <-stop:
				return
			case <-changed:
			}
			continue
		}
		queued := watcher.queue[0]
		stop := watcher.stop
		db.feedMu.Unlock()

		select {
		case <-stop:
			return
		case watcher.events <- queued.batch:
			db.feedMu.Lock()
			if !watcher.closed && len(watcher.queue) > 0 {
				current := watcher.queue[0]
				if current.batch.Token == queued.batch.Token && current.bytes == queued.bytes {
					watcher.queue[0] = queuedChangeBatch{}
					watcher.queue = watcher.queue[1:]
					watcher.pendingBytes -= current.bytes
					if current.bytes > db.pendingWatcherBytes {
						db.pendingWatcherBytes = 0
					} else {
						db.pendingWatcherBytes -= current.bytes
					}
				}
			}
			db.feedMu.Unlock()
		}
	}
}

// cloneChange copies a change's document images, index descriptor, and changed paths.
func cloneChange(change Change) Change {
	if change.Before != nil {
		value := change.Before.Clone()
		change.Before = &value
	}
	if change.After != nil {
		value := change.After.Clone()
		change.After = &value
	}
	if change.Index != nil {
		value := *change.Index
		change.Index = &value
	}
	change.ChangedPaths = append([]string(nil), change.ChangedPaths...)
	return change
}
