package meldbase

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

const (
	defaultIndexBuildPollInterval = time.Second
	defaultIndexBuildRunTimeout   = 250 * time.Millisecond
	maxIndexBuildSchedulers       = 8
)

// IndexBuildSchedulerOptions configures an explicit default-off runner. Each
// task receives a bounded time quantum, then yields durable progress so CRUD and
// other builds can proceed between quanta.
type IndexBuildSchedulerOptions struct {
	PollInterval   time.Duration
	RunTimeout     time.Duration
	MaxConcurrency int
	RunImmediately bool
}

type IndexBuildSchedulerStats struct {
	Polls        uint64        `json:"polls"`
	Runs         uint64        `json:"runs"`
	Completed    uint64        `json:"completed"`
	Yielded      uint64        `json:"yielded"`
	MarkedFailed uint64        `json:"markedFailed"`
	Conflicts    uint64        `json:"conflicts"`
	Failed       uint64        `json:"failed"`
	Active       uint64        `json:"active"`
	LastDuration time.Duration `json:"lastDurationNanos"`
	LastError    string        `json:"lastError,omitempty"`
}

type IndexBuildScheduler struct {
	cancel       context.CancelFunc
	done         chan struct{}
	stopOnce     sync.Once
	wg           sync.WaitGroup
	sem          chan struct{}
	runningMu    sync.Mutex
	running      map[IndexBuildID]struct{}
	polls        atomic.Uint64
	runs         atomic.Uint64
	completed    atomic.Uint64
	yielded      atomic.Uint64
	markedFailed atomic.Uint64
	conflicts    atomic.Uint64
	failed       atomic.Uint64
	active       atomic.Uint64
	lastDuration atomic.Uint64
	lastMu       sync.Mutex
	lastError    string
	cursor       int
}

func (db *DB) StartIndexBuildScheduler(parent context.Context, options IndexBuildSchedulerOptions) (*IndexBuildScheduler, error) {
	if db == nil {
		return nil, ErrIndexBuildUnsupported
	}
	if err := contextError(parent); err != nil {
		return nil, err
	}
	interval := options.PollInterval
	if interval == 0 {
		interval = defaultIndexBuildPollInterval
	}
	timeout := options.RunTimeout
	if timeout == 0 {
		timeout = defaultIndexBuildRunTimeout
	}
	concurrency := options.MaxConcurrency
	if concurrency == 0 {
		concurrency = 1
	}
	if interval < minV2MaintenanceDuration || interval > maxV2MaintenanceInterval ||
		timeout < minV2MaintenanceDuration || timeout > maxV2MaintenanceTimeout ||
		concurrency < 1 || concurrency > maxIndexBuildSchedulers {
		return nil, ErrInvalidIndexBuildSchedulerOptions
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, ErrClosed
	}
	store, ok := db.durability.(*v2DurableStore)
	if !ok || store == nil || store.file == nil {
		db.mu.Unlock()
		return nil, ErrIndexBuildUnsupported
	}
	if db.indexBuildSchedulerActive {
		db.mu.Unlock()
		return nil, ErrIndexBuildSchedulerRunning
	}
	db.indexBuildSchedulerActive = true
	closedCh := db.closedCh
	db.mu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	scheduler := &IndexBuildScheduler{
		cancel: cancel, done: make(chan struct{}), sem: make(chan struct{}, concurrency),
		running: make(map[IndexBuildID]struct{}),
	}
	go func() {
		select {
		case <-closedCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	go scheduler.run(ctx, db, interval, timeout, options.RunImmediately)
	return scheduler, nil
}

func (scheduler *IndexBuildScheduler) run(ctx context.Context, db *DB, interval, timeout time.Duration, immediately bool) {
	defer close(scheduler.done)
	defer func() {
		scheduler.wg.Wait()
		db.mu.Lock()
		db.indexBuildSchedulerActive = false
		db.mu.Unlock()
	}()
	delay := interval
	if immediately {
		delay = 0
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		scheduler.dispatch(ctx, db, timeout)
		timer.Reset(interval)
	}
}

func (scheduler *IndexBuildScheduler) dispatch(ctx context.Context, db *DB, timeout time.Duration) {
	scheduler.polls.Add(1)
	builds, err := db.IndexBuilds()
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
			scheduler.recordError(err)
		}
		return
	}
	if len(builds) == 0 {
		return
	}
	start := scheduler.cursor % len(builds)
	for offset := 0; offset < len(builds); offset++ {
		index := (start + offset) % len(builds)
		build := builds[index]
		if build.Phase == IndexBuildPhaseFailed || !scheduler.claim(build.ID) {
			continue
		}
		select {
		case scheduler.sem <- struct{}{}:
			scheduler.cursor = (index + 1) % len(builds)
		case <-ctx.Done():
			scheduler.release(build.ID)
			return
		default:
			scheduler.release(build.ID)
			return
		}
		scheduler.wg.Add(1)
		go scheduler.runOne(ctx, db, build.ID, timeout)
	}
}

func (scheduler *IndexBuildScheduler) runOne(parent context.Context, db *DB, id IndexBuildID, timeout time.Duration) {
	defer scheduler.wg.Done()
	defer func() { <-scheduler.sem }()
	defer scheduler.release(id)
	scheduler.runs.Add(1)
	db.metrics.indexBuildSchedulerRuns.Add(1)
	scheduler.active.Add(1)
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, timeout)
	err := db.ResumeIndexBuild(ctx, id)
	cancel()
	scheduler.active.Add(^uint64(0))
	scheduler.lastDuration.Store(uint64(time.Since(started)))
	switch {
	case err == nil:
		scheduler.completed.Add(1)
		scheduler.recordError(nil)
	case errors.Is(err, context.DeadlineExceeded):
		scheduler.yielded.Add(1)
		db.metrics.indexBuildSchedulerYields.Add(1)
		scheduler.recordError(nil)
	case errors.Is(err, context.Canceled) && parent.Err() != nil:
		scheduler.recordError(nil)
	case errors.Is(err, ErrWriteConflict):
		scheduler.conflicts.Add(1)
		scheduler.recordError(err)
	default:
		failure, terminal := persistentIndexBuildFailure(err)
		if terminal && parent.Err() == nil {
			if _, failErr := db.failIndexBuild(parent, id, failure); failErr == nil {
				scheduler.markedFailed.Add(1)
				db.metrics.indexBuildSchedulerFailures.Add(1)
				scheduler.recordError(err)
				return
			} else if !errors.Is(failErr, ErrIndexBuildNotFound) && !errors.Is(failErr, ErrWriteConflict) {
				err = errors.Join(err, failErr)
			}
		}
		scheduler.failed.Add(1)
		db.metrics.indexBuildSchedulerFailures.Add(1)
		scheduler.recordError(err)
	}
}

func persistentIndexBuildFailure(err error) (storagev2.IndexBuildFailure, bool) {
	switch {
	case errors.Is(err, ErrDuplicateKey):
		return storagev2.IndexBuildFailureUniqueConflict, true
	case errors.Is(err, ErrResourceLimit):
		return storagev2.IndexBuildFailureResourceLimit, true
	case errors.Is(err, ErrHistoryLost):
		return storagev2.IndexBuildFailureHistoryLost, true
	case errors.Is(err, ErrInvalidIndex):
		return storagev2.IndexBuildFailureInvalidIndex, true
	default:
		return storagev2.IndexBuildFailureNone, false
	}
}

func (scheduler *IndexBuildScheduler) claim(id IndexBuildID) bool {
	scheduler.runningMu.Lock()
	defer scheduler.runningMu.Unlock()
	if _, exists := scheduler.running[id]; exists {
		return false
	}
	scheduler.running[id] = struct{}{}
	return true
}

func (scheduler *IndexBuildScheduler) release(id IndexBuildID) {
	scheduler.runningMu.Lock()
	delete(scheduler.running, id)
	scheduler.runningMu.Unlock()
}

func (scheduler *IndexBuildScheduler) recordError(err error) {
	scheduler.lastMu.Lock()
	if err == nil {
		scheduler.lastError = ""
	} else {
		scheduler.lastError = err.Error()
	}
	scheduler.lastMu.Unlock()
}

func (scheduler *IndexBuildScheduler) Stop() {
	if scheduler == nil {
		return
	}
	scheduler.stopOnce.Do(scheduler.cancel)
	<-scheduler.done
}

func (scheduler *IndexBuildScheduler) Done() <-chan struct{} {
	if scheduler == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return scheduler.done
}

func (scheduler *IndexBuildScheduler) Stats() IndexBuildSchedulerStats {
	if scheduler == nil {
		return IndexBuildSchedulerStats{}
	}
	scheduler.lastMu.Lock()
	lastError := scheduler.lastError
	scheduler.lastMu.Unlock()
	return IndexBuildSchedulerStats{
		Polls: scheduler.polls.Load(), Runs: scheduler.runs.Load(), Completed: scheduler.completed.Load(),
		Yielded: scheduler.yielded.Load(), MarkedFailed: scheduler.markedFailed.Load(), Conflicts: scheduler.conflicts.Load(),
		Failed: scheduler.failed.Load(), Active: scheduler.active.Load(), LastDuration: time.Duration(scheduler.lastDuration.Load()),
		LastError: lastError,
	}
}
