package meldbase

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

// v2CommitCoordinator is a bounded, single-writer admission boundary for the
// first public group-commit path: ordinary V2 collection CRUD mutations.
//
// It deliberately owns neither authorization nor user callbacks. Input has
// already been cloned/selected, validated and resource-admitted by the public
// collection method. Once a request is admitted, its durable outcome is
// independent of its Context: cancellation can race the final Meta
// acknowledgement, so the caller receives an explicit reconciliation error
// instead of an unsafe blind-retry signal.
type v2CommitCoordinator struct {
	db      *DB
	store   *v2DurableStore
	options V2CommitCoordinatorOptions

	mu      sync.Mutex
	queue   []*v2CommitRequest
	changed chan struct{}
	done    chan struct{}
	stopped chan struct{}
	closed  bool

	admitted, admissionRejected  atomic.Uint64
	batches, groupedTransactions atomic.Uint64
	outcomeUnknown               atomic.Uint64

	// testBeforeCommit runs after a group has been admitted and before it takes
	// db.mu. It is package-test-only plumbing that makes batching deterministic
	// without adding a production scheduling hook.
	testBeforeCommit func()
	// testBeforeCoalesce runs after the first request has been taken and before
	// the bounded coalescing wait observes additional admissions.
	testBeforeCoalesce func()
}

type v2CommitResult struct {
	ids    []DocumentID
	update UpdateResult
	delete DeleteResult
	err    error
}

type v2CommitRequest struct {
	collection        string
	ids               []DocumentID
	copies            []Document
	changes           []Change
	readSet           []storagev2.DocumentPrecondition
	collectionReadSet []storagev2.CollectionPrecondition
	success           v2CommitResult
	// fallback recomputes filter selection under db.mu after a speculative
	// group conflict. It is nil only for an insert, whose immutable changes are
	// already its exact original operation.
	fallback func() v2CommitResult
	// waitForOutcome preserves the public write-transaction contract. A callback
	// has already run by the time its frozen point writes enter this queue, so a
	// post-admission Context cancellation cannot safely invite a second callback
	// execution or return an ambiguous result to a caller with no reconciliation
	// handle. The coordinator therefore waits for its durable outcome.
	waitForOutcome bool
	result         chan v2CommitResult
}

func newV2CommitCoordinator(db *DB, store *v2DurableStore, options V2CommitCoordinatorOptions) *v2CommitCoordinator {
	coordinator := &v2CommitCoordinator{
		db: db, store: store, options: options,
		changed: make(chan struct{}), done: make(chan struct{}), stopped: make(chan struct{}),
	}
	go coordinator.run()
	return coordinator
}

func (coordinator *v2CommitCoordinator) submit(
	ctx context.Context,
	collection string,
	ids []DocumentID,
	copies []Document,
	changes []Change,
) ([]DocumentID, error) {
	if coordinator == nil || coordinator.db == nil || coordinator.store == nil {
		return nil, ErrCorrupt
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	request := &v2CommitRequest{
		collection: collection,
		ids:        append([]DocumentID(nil), ids...),
		copies:     append([]Document(nil), copies...),
		changes:    append([]Change(nil), changes...),
		success:    v2CommitResult{ids: append([]DocumentID(nil), ids...)},
		result:     make(chan v2CommitResult, 1),
	}
	result, err := coordinator.admit(ctx, request)
	if err != nil {
		return result.ids, err
	}
	return result.ids, nil
}

func (coordinator *v2CommitCoordinator) admit(ctx context.Context, request *v2CommitRequest) (v2CommitResult, error) {
	if coordinator == nil || coordinator.db == nil || coordinator.store == nil || request == nil {
		return v2CommitResult{}, ErrCorrupt
	}
	if err := contextError(ctx); err != nil {
		return v2CommitResult{}, err
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return v2CommitResult{}, ErrClosed
	}
	if len(coordinator.queue) >= coordinator.options.MaxPending {
		coordinator.mu.Unlock()
		coordinator.admissionRejected.Add(1)
		coordinator.db.metrics.resourceLimitRejections.Add(1)
		return v2CommitResult{}, ErrResourceLimit
	}
	coordinator.queue = append(coordinator.queue, request)
	coordinator.admitted.Add(1)
	coordinator.signalLocked()
	coordinator.mu.Unlock()

	select {
	case result := <-request.result:
		if result.err != nil {
			return result, result.err
		}
		return result, nil
	case <-ctx.Done():
		if request.waitForOutcome {
			result := <-request.result
			return result, result.err
		}
		// The request crossed the admission boundary. Its immutable IDs make
		// reconciliation possible even when this caller no longer waits for the
		// final durable result.
		coordinator.outcomeUnknown.Add(1)
		return request.success, errors.Join(ErrCommitOutcomeUnknown, ctx.Err())
	}
}

func (coordinator *v2CommitCoordinator) signalLocked() {
	close(coordinator.changed)
	coordinator.changed = make(chan struct{})
}

func (coordinator *v2CommitCoordinator) run() {
	defer close(coordinator.stopped)
	for {
		requests, ok := coordinator.nextGroup()
		if !ok {
			return
		}
		if coordinator.testBeforeCommit != nil {
			coordinator.testBeforeCommit()
		}
		coordinator.commitGroup(requests)
	}
}

func (coordinator *v2CommitCoordinator) nextGroup() ([]*v2CommitRequest, bool) {
	for {
		coordinator.mu.Lock()
		if coordinator.closed {
			coordinator.mu.Unlock()
			return nil, false
		}
		if len(coordinator.queue) > 0 {
			break
		}
		changed, done := coordinator.changed, coordinator.done
		coordinator.mu.Unlock()
		select {
		case <-done:
			return nil, false
		case <-changed:
		}
	}

	count := min(len(coordinator.queue), coordinator.options.MaxBatch)
	requests := append([]*v2CommitRequest(nil), coordinator.queue[:count]...)
	coordinator.queue = coordinator.queue[count:]
	coordinator.mu.Unlock()
	if len(requests) == coordinator.options.MaxBatch || coordinator.options.MaxDelay <= 0 {
		return requests, true
	}
	if coordinator.testBeforeCoalesce != nil {
		coordinator.testBeforeCoalesce()
	}

	// A single bounded wait coalesces nearby independently submitted writes. It
	// never delays a full group and it never extends the deadline when more work
	// arrives, which keeps tail latency predictable under sustained load.
	timer := time.NewTimer(coordinator.options.MaxDelay)
	defer timer.Stop()
	for len(requests) < coordinator.options.MaxBatch {
		coordinator.mu.Lock()
		if coordinator.closed {
			coordinator.mu.Unlock()
			return requests, true
		}
		space := coordinator.options.MaxBatch - len(requests)
		if len(coordinator.queue) > 0 {
			take := min(len(coordinator.queue), space)
			requests = append(requests, coordinator.queue[:take]...)
			coordinator.queue = coordinator.queue[take:]
			coordinator.mu.Unlock()
			continue
		}
		changed, done := coordinator.changed, coordinator.done
		coordinator.mu.Unlock()
		select {
		case <-timer.C:
			return requests, true
		case <-done:
			return requests, true
		case <-changed:
		}
	}
	return requests, true
}

func (coordinator *v2CommitCoordinator) commitGroup(requests []*v2CommitRequest) {
	if len(requests) == 0 {
		return
	}
	coordinator.batches.Add(1)
	if len(requests) > 1 {
		coordinator.groupedTransactions.Add(uint64(len(requests)))
	}
	coordinator.db.mu.Lock()
	defer coordinator.db.mu.Unlock()
	if coordinator.db.closed {
		coordinator.finish(requests, v2CommitResult{err: ErrClosed})
		return
	}
	if coordinator.db.fatalErr != nil {
		coordinator.finish(requests, v2CommitResult{err: coordinator.db.fatalErr})
		return
	}
	if len(requests) == 1 {
		coordinator.finishOne(requests[0], coordinator.commitFallbackLocked(requests[0]))
		return
	}

	batches := make([]ChangeBatch, len(requests))
	readSets := make([][]storagev2.DocumentPrecondition, len(requests))
	collectionReadSets := make([][]storagev2.CollectionPrecondition, len(requests))
	for index, request := range requests {
		batches[index] = ChangeBatch{Token: coordinator.db.token + uint64(index) + 1, Changes: request.changes}
		readSets[index] = request.readSet
		collectionReadSets[index] = request.collectionReadSet
	}
	err := coordinator.db.commitV2ChangeBatchesWithPreconditionsLocked(context.Background(), coordinator.store, batches, readSets, collectionReadSets)
	if err == nil {
		for _, request := range requests {
			coordinator.finishOne(request, request.success)
		}
		return
	}
	if !v2CommitLogicalConflict(err) {
		coordinator.finish(requests, v2CommitResult{err: err})
		return
	}

	// Storage validates all group members against one private WriteTxn. A
	// duplicate in one request therefore rejects the whole candidate group. Run
	// the original public semantics in admission order so independent valid
	// requests still commit and only the conflicting member is rejected.
	for _, request := range requests {
		coordinator.finishOne(request, coordinator.commitFallbackLocked(request))
		if coordinator.db.fatalErr != nil {
			// A physical failure after a successful earlier fallback member is
			// fail-stop. Every not-yet-run request receives that same boundary.
			for _, remaining := range requestsAfter(requests, request) {
				coordinator.finishOne(remaining, v2CommitResult{err: coordinator.db.fatalErr})
			}
			return
		}
	}
}

func (coordinator *v2CommitCoordinator) stats() V2CommitCoordinatorStats {
	if coordinator == nil {
		return V2CommitCoordinatorStats{}
	}
	coordinator.mu.Lock()
	pending, capacity := len(coordinator.queue), coordinator.options.MaxPending
	enabled := !coordinator.closed
	coordinator.mu.Unlock()
	return V2CommitCoordinatorStats{
		Enabled: enabled, Pending: uint64(pending), PendingCapacity: uint64(capacity),
		Admitted: coordinator.admitted.Load(), AdmissionRejected: coordinator.admissionRejected.Load(),
		Batches: coordinator.batches.Load(), GroupedTransactions: coordinator.groupedTransactions.Load(),
		OutcomeUnknown: coordinator.outcomeUnknown.Load(),
	}
}

func v2CommitLogicalConflict(err error) bool {
	return errors.Is(err, ErrDuplicateID) || errors.Is(err, ErrDuplicateKey) || errors.Is(err, ErrInvalidIndex) ||
		errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrWriteConflict)
}

func (coordinator *v2CommitCoordinator) commitFallbackLocked(request *v2CommitRequest) v2CommitResult {
	if request == nil {
		return v2CommitResult{err: ErrCorrupt}
	}
	if request.fallback != nil {
		return request.fallback()
	}
	err := (&Collection{db: coordinator.db, name: request.collection}).commitInsertManyLocked(context.Background(), request.ids, request.copies, request.changes)
	result := request.success
	result.err = err
	return result
}

func requestsAfter(requests []*v2CommitRequest, current *v2CommitRequest) []*v2CommitRequest {
	for index, request := range requests {
		if request == current {
			return requests[index+1:]
		}
	}
	return nil
}

func (coordinator *v2CommitCoordinator) finish(requests []*v2CommitRequest, result v2CommitResult) {
	for _, request := range requests {
		coordinator.finishOne(request, result)
	}
}

func (coordinator *v2CommitCoordinator) finishOne(request *v2CommitRequest, result v2CommitResult) {
	if request != nil {
		request.result <- result
	}
}

func (coordinator *v2CommitCoordinator) close() {
	if coordinator == nil {
		return
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return
	}
	coordinator.closed = true
	pending := coordinator.queue
	coordinator.queue = nil
	close(coordinator.done)
	coordinator.signalLocked()
	coordinator.mu.Unlock()
	coordinator.finish(pending, v2CommitResult{err: ErrClosed})
	// The worker may already own one admitted group. Let it finish before the
	// V2 file is closed, preserving the durable outcome of that group.
	<-coordinator.stopped
}
