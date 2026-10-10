package database

import "sync"

const (
	// maxPendingChangeDispatchBatches limits the number of queued committed batches.
	maxPendingChangeDispatchBatches = 1024
	// maxPendingChangeDispatchChanges limits the number of queued document changes.
	//
	// A dispatcher batch contains already-admitted document images. Keep this
	// lower than the downstream reactive-view limit so a stalled dispatcher
	// cannot retain many large write transactions before consumers get a
	// resync/slow-consumer boundary.
	maxPendingChangeDispatchChanges = 8 * 1024
	// maxPendingChangeDispatchBytes limits retained payload bytes in the dispatcher.
	//
	// Count bounds alone do not cap retained document images: a valid mutation
	// may contain a large document or a high-byte transaction. This independent
	// canonical-image budget is deliberately fixed rather than inherited from
	// configurable transaction admission, so changing a write limit cannot make
	// the asynchronous realtime boundary unbounded.
	maxPendingChangeDispatchBytes uint64 = 64 << 20
	// maxPendingChangeWatcherBytes limits retained payload bytes for one direct watcher.
	//
	// Direct Go change watchers retain cloned images for application code. Their
	// queue and the aggregate across every watcher are bounded independently of
	// both dispatcher and reactive-view budgets.
	maxPendingChangeWatcherBytes uint64 = 64 << 20
	// maxPendingChangeWatchersBytes limits retained payload bytes across all direct watchers.
	maxPendingChangeWatchersBytes uint64 = 128 << 20

	// changeDispatchEnvelopeBytes charges a fixed byte allowance for each change envelope.
	//
	// A conservative fixed charge covers the Change envelope, ID, operation and
	// slice/map references. Document images are charged at their exact canonical
	// binary size below. This is a deterministic payload budget, not a promise
	// about Go's allocator internals.
	changeDispatchEnvelopeBytes uint64 = 96
)

// changeDispatcher delivers committed changes to asynchronous consumers in order.
//
// Writers enqueue each batch once; reactive views and direct watchers then
// receive it through their own bounded delivery paths.
//
// The dispatcher never makes a commit durable and never changes its token. A
// full dispatcher queue is therefore a consumer failure, not a write failure:
// reactive views rebuild from an atomic snapshot and direct watchers are closed
// with ErrSlowConsumer rather than receiving a discontinuous stream.
type changeDispatcher struct {
	db *DB

	mu             sync.Mutex
	queue          []ChangeBatch
	maxBatches     int
	maxChanges     int
	maxBytes       uint64
	pendingChanges int
	pendingBytes   uint64
	overflow       bool
	changed        chan struct{}
	done           chan struct{}
	stopped        chan struct{}
	started        bool
	closed         bool

	// testBeforeDispatch is used only by package tests to hold the asynchronous
	// side of the boundary after a commit has been admitted.
	testBeforeDispatch func()
}

// newChangeDispatcher creates a bounded queue for committed change batches.
func newChangeDispatcher(db *DB) *changeDispatcher {
	return &changeDispatcher{
		db: db, maxBatches: maxPendingChangeDispatchBatches, maxChanges: maxPendingChangeDispatchChanges, maxBytes: maxPendingChangeDispatchBytes,
		changed: make(chan struct{}), done: make(chan struct{}), stopped: make(chan struct{}),
	}
}

// enqueue transfers an immutable committed batch into the bounded delivery queue.
func (dispatcher *changeDispatcher) enqueue(batch ChangeBatch) {
	if dispatcher == nil || len(batch.Changes) == 0 {
		return
	}
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if dispatcher.closed {
		return
	}
	if !dispatcher.started {
		dispatcher.started = true
		go dispatcher.run()
	}
	if !dispatcher.overflow {
		changes := len(batch.Changes)
		bytes := changeBatchDispatchBytes(batch)
		if dispatcher.maxBatches <= 0 || dispatcher.maxChanges <= 0 || len(dispatcher.queue) >= dispatcher.maxBatches ||
			changes > dispatcher.maxChanges || dispatcher.pendingChanges > dispatcher.maxChanges-changes ||
			dispatcher.maxBytes == 0 || bytes > dispatcher.maxBytes || dispatcher.pendingBytes > dispatcher.maxBytes-bytes {
			dispatcher.queue = nil
			dispatcher.pendingChanges = 0
			dispatcher.pendingBytes = 0
			dispatcher.overflow = true
		} else {
			dispatcher.queue = append(dispatcher.queue, batch)
			dispatcher.pendingChanges += changes
			dispatcher.pendingBytes += bytes
		}
	}
	close(dispatcher.changed)
	dispatcher.changed = make(chan struct{})
}

// run delivers queued batches in order and handles queue overflow.
func (dispatcher *changeDispatcher) run() {
	defer close(dispatcher.stopped)
	for {
		dispatcher.mu.Lock()
		if dispatcher.closed {
			dispatcher.mu.Unlock()
			return
		}
		if dispatcher.overflow {
			dispatcher.overflow = false
			dispatcher.mu.Unlock()
			dispatcher.handleOverflow()
			continue
		}
		if len(dispatcher.queue) > 0 {
			batches := append([]ChangeBatch(nil), dispatcher.queue...)
			dispatcher.queue = nil
			dispatcher.pendingChanges = 0
			dispatcher.pendingBytes = 0
			dispatcher.mu.Unlock()
			for _, batch := range batches {
				if dispatcher.testBeforeDispatch != nil {
					dispatcher.testBeforeDispatch()
				}
				dispatcher.dispatch(batch)
			}
			continue
		}
		changed, done := dispatcher.changed, dispatcher.done
		dispatcher.mu.Unlock()
		select {
		case <-done:
			return
		case <-changed:
		}
	}
}

// changeDispatcherStats records queued work and its configured limits.
type changeDispatcherStats struct {
	pendingBatches, pendingChanges, pendingBytes uint64
	batchCapacity, changeCapacity, byteCapacity  uint64
}

// stats takes a synchronized snapshot of the dispatcher queue and its limits.
func (dispatcher *changeDispatcher) stats() changeDispatcherStats {
	if dispatcher == nil {
		return changeDispatcherStats{}
	}
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	return changeDispatcherStats{
		pendingBatches: uint64(len(dispatcher.queue)), pendingChanges: uint64(dispatcher.pendingChanges), pendingBytes: dispatcher.pendingBytes,
		batchCapacity: uint64(max(dispatcher.maxBatches, 0)), changeCapacity: uint64(max(dispatcher.maxChanges, 0)), byteCapacity: dispatcher.maxBytes,
	}
}

// changeDispatchBaseBytes measures retained change metadata without document images.
//
// It must stay fixed-cardinality: collection, changed-path and
// index-definition text is charged by bytes, but no application value is used
// as an observability label or retained outside the batch itself.
func changeDispatchBaseBytes(change Change) (uint64, bool) {
	total := changeDispatchEnvelopeBytes + uint64(len(change.Collection))
	if total < changeDispatchEnvelopeBytes {
		return 0, false
	}
	for _, path := range change.ChangedPaths {
		// Account for the string header/reference as well as its immutable text,
		// matching the conservative index-field accounting below.
		addition := uint64(len(path)) + 8
		if addition < uint64(len(path)) || total > ^uint64(0)-addition {
			return 0, false
		}
		total += addition
	}
	if change.Index == nil {
		return total, true
	}
	indexBytes := uint64(len(change.Index.Name)) + uint64(len(change.Index.Field))
	if indexBytes < uint64(len(change.Index.Name)) {
		return 0, false
	}
	for _, field := range change.Index.Fields {
		addition := uint64(len(field.Field)) + 8
		if addition < uint64(len(field.Field)) || indexBytes > ^uint64(0)-addition {
			return 0, false
		}
		indexBytes += addition
	}
	if total > ^uint64(0)-indexBytes {
		return 0, false
	}
	return total + indexBytes, true
}

// changeBatchDispatchBytes measures the payload retained by a queued batch.
//
// Normal mutations reuse the image
// sizes recorded during resource validation. The defensive fallback covers
// internal catalog events and private test construction; an invalid or
// unmeasurable image deliberately consumes the whole budget and takes the
// safe overflow/resync path.
func changeBatchDispatchBytes(batch ChangeBatch) uint64 {
	var total uint64
	for _, change := range batch.Changes {
		bytes, known := change.dispatchBytes, change.dispatchBytesKnown
		if !known {
			base, valid := changeDispatchBaseBytes(change)
			if !valid {
				return ^uint64(0)
			}
			bytes = base
			for _, document := range []*Document{change.Before, change.After} {
				if document == nil {
					continue
				}
				size, err := canonicalDocumentSize(*document)
				if err != nil || size > ^uint64(0)-bytes {
					return ^uint64(0)
				}
				bytes += size
			}
		}
		if bytes > ^uint64(0)-total {
			return ^uint64(0)
		}
		total += bytes
	}
	return total
}

// dispatch sends a committed batch to reactive views and direct change watchers.
func (dispatcher *changeDispatcher) dispatch(batch ChangeBatch) {
	if dispatcher == nil || dispatcher.db == nil || len(batch.Changes) == 0 {
		return
	}
	if dispatcher.db.reactive != nil {
		dispatcher.db.reactive.notify(batch)
	}
	dispatcher.db.deliverChangeWatchers(batch)
}

// handleOverflow closes direct watchers and rebuilds reactive views after changes are dropped.
func (dispatcher *changeDispatcher) handleOverflow() {
	if dispatcher == nil || dispatcher.db == nil {
		return
	}
	// Realtime.QueueOverflows is the fixed-cardinality public overload signal
	// for both the shared-view queue and this upstream ordered dispatcher. It
	// must advance even when no reactive view is currently registered.
	dispatcher.db.metrics.reactiveQueueOverflows.Add(1)
	// A full dispatch queue means no direct watcher can receive a contiguous
	// stream. Their API has no durable resume token, so fail them explicitly.
	dispatcher.db.failChangeWatchers(ErrSlowConsumer)
	if dispatcher.db.reactive != nil {
		dispatcher.db.reactive.resyncAll()
	}
}

// close stops the dispatcher and waits for its delivery loop to exit.
func (dispatcher *changeDispatcher) close() {
	if dispatcher == nil {
		return
	}
	dispatcher.mu.Lock()
	if dispatcher.closed {
		dispatcher.mu.Unlock()
		return
	}
	dispatcher.closed = true
	close(dispatcher.done)
	started := dispatcher.started
	dispatcher.mu.Unlock()
	if started {
		<-dispatcher.stopped
	}
}
