package meldbase

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultV2MaintenanceInterval = 5 * time.Minute
	defaultV2MaintenanceTimeout  = time.Minute
	minV2MaintenanceDuration     = 10 * time.Millisecond
	maxV2MaintenanceInterval     = 24 * time.Hour
	maxV2MaintenanceTimeout      = time.Hour
)

// V2MaintenanceOptions configures an explicit default-off maintenance loop.
// Every run uses online optimistic reclamation; runs never overlap.
type V2MaintenanceOptions struct {
	Interval       time.Duration
	Timeout        time.Duration
	MaxAttempts    int
	RunImmediately bool
	// PersistFreeSpace opts into a physical maintenance generation after each
	// successful scan. The default memory-only mode minimizes writer pauses.
	PersistFreeSpace bool
}

type V2MaintenanceStats struct {
	Runs         uint64
	Completed    uint64
	Conflicts    uint64
	Failed       uint64
	Active       bool
	LastDuration time.Duration
	LastError    string
}

// V2Maintenance owns one background reclamation loop. Stop is idempotent and
// waits for an active scan to observe cancellation. Closing the DB also stops
// the loop through the DB lifecycle channel.
type V2Maintenance struct {
	cancel       context.CancelFunc
	done         chan struct{}
	stopOnce     sync.Once
	runs         atomic.Uint64
	completed    atomic.Uint64
	conflicts    atomic.Uint64
	failed       atomic.Uint64
	active       atomic.Bool
	lastDuration atomic.Uint64
	lastMu       sync.Mutex
	lastError    string
}

func (db *DB) StartV2Maintenance(parent context.Context, options V2MaintenanceOptions) (*V2Maintenance, error) {
	if db == nil {
		return nil, ErrReclamationUnsupported
	}
	if err := contextError(parent); err != nil {
		return nil, err
	}
	interval := options.Interval
	if interval == 0 {
		interval = defaultV2MaintenanceInterval
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = defaultV2MaintenanceTimeout
	}
	if interval < minV2MaintenanceDuration || interval > maxV2MaintenanceInterval ||
		timeout < minV2MaintenanceDuration || timeout > maxV2MaintenanceTimeout ||
		options.MaxAttempts < 0 || options.MaxAttempts > 32 {
		return nil, ErrInvalidReclamationOptions
	}
	db.mu.RLock()
	_, v2 := db.durability.(*v2DurableStore)
	closed := db.closed
	closedCh := db.closedCh
	db.mu.RUnlock()
	if !v2 {
		return nil, ErrReclamationUnsupported
	}
	if closed {
		return nil, ErrClosed
	}
	ctx, cancel := context.WithCancel(parent)
	handle := &V2Maintenance{cancel: cancel, done: make(chan struct{})}
	go func() {
		select {
		case <-closedCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	go handle.run(ctx, db, interval, timeout, options.MaxAttempts, options.RunImmediately, options.PersistFreeSpace)
	return handle, nil
}

func (maintenance *V2Maintenance) run(ctx context.Context, db *DB, interval, timeout time.Duration, maxAttempts int, immediately, persistFreeSpace bool) {
	defer close(maintenance.done)
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
		maintenance.runOnce(ctx, db, timeout, maxAttempts, persistFreeSpace)
		timer.Reset(interval)
	}
}

func (maintenance *V2Maintenance) runOnce(parent context.Context, db *DB, timeout time.Duration, maxAttempts int, persistFreeSpace bool) {
	maintenance.runs.Add(1)
	maintenance.active.Store(true)
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, timeout)
	_, err := db.ReclaimV2PagesWithOptions(ctx, ReclaimV2Options{
		Online: true, MaxAttempts: maxAttempts, MemoryOnly: !persistFreeSpace,
	})
	cancel()
	maintenance.lastDuration.Store(uint64(time.Since(started)))
	maintenance.active.Store(false)
	maintenance.lastMu.Lock()
	defer maintenance.lastMu.Unlock()
	switch {
	case err == nil:
		maintenance.completed.Add(1)
		maintenance.lastError = ""
	case errors.Is(err, ErrReclamationConflict):
		maintenance.conflicts.Add(1)
		maintenance.lastError = err.Error()
	case errors.Is(err, context.Canceled) && parent.Err() != nil:
		maintenance.lastError = ""
	default:
		maintenance.failed.Add(1)
		maintenance.lastError = err.Error()
	}
}

func (maintenance *V2Maintenance) Stop() {
	if maintenance == nil {
		return
	}
	maintenance.stopOnce.Do(maintenance.cancel)
	<-maintenance.done
}

func (maintenance *V2Maintenance) Done() <-chan struct{} {
	if maintenance == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return maintenance.done
}

func (maintenance *V2Maintenance) Stats() V2MaintenanceStats {
	if maintenance == nil {
		return V2MaintenanceStats{}
	}
	maintenance.lastMu.Lock()
	lastError := maintenance.lastError
	maintenance.lastMu.Unlock()
	return V2MaintenanceStats{
		Runs: maintenance.runs.Load(), Completed: maintenance.completed.Load(),
		Conflicts: maintenance.conflicts.Load(), Failed: maintenance.failed.Load(),
		Active: maintenance.active.Load(), LastDuration: time.Duration(maintenance.lastDuration.Load()), LastError: lastError,
	}
}
