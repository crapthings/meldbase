package meldbase

import (
	"context"
	"errors"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

type ReclaimV2Result struct {
	PhysicalPages   uint64
	ReachablePages  uint64
	ReusablePages   uint64
	PinnedSnapshots uint64
	Attempts        int
	Online          bool
	Persisted       bool
}

// ReclaimV2Options controls explicit page reclamation. Online scans a duplicate
// read handle without holding the storage writer lock and installs its result
// only if the Meta generation is unchanged. MaxAttempts bounds complete graph
// rescans after concurrent commits; zero selects three attempts.
type ReclaimV2Options struct {
	Online      bool
	MaxAttempts int
	// MemoryOnly skips the physical FreeSpace maintenance generation. It keeps
	// the final installation pause O(1), but another audit is needed after reopen.
	MemoryOnly bool
}

// ReclaimV2Pages audits both valid Meta roots and every active snapshot/replay
// lease, then makes only unreachable pages available to future COW commits. The
// free pool is process-local and safely reconstructed by another call on reopen.
func (db *DB) ReclaimV2Pages(ctx context.Context) (result ReclaimV2Result, resultErr error) {
	return db.ReclaimV2PagesWithOptions(ctx, ReclaimV2Options{})
}

// ReclaimV2PagesWithOptions runs explicit synchronous or low-pause optimistic
// reclamation. Online mode is opt-in and may return an error wrapping
// ErrReclamationConflict when every bounded attempt overlaps a commit.
func (db *DB) ReclaimV2PagesWithOptions(ctx context.Context, options ReclaimV2Options) (result ReclaimV2Result, resultErr error) {
	if db == nil {
		return result, ErrReclamationUnsupported
	}
	if err := contextError(ctx); err != nil {
		return result, err
	}
	store, ok := db.durability.(*v2DurableStore)
	if !ok || store == nil || store.file == nil {
		return result, ErrReclamationUnsupported
	}
	maxAttempts := options.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	if maxAttempts < 1 || maxAttempts > 32 {
		return result, ErrInvalidReclamationOptions
	}
	store.compactMu.Lock()
	defer store.compactMu.Unlock()
	db.metrics.reclamationAttempts.Add(1)
	db.metrics.reclamationActive.Add(1)
	started := time.Now()
	succeeded := false
	defer func() {
		db.metrics.reclamationActive.Add(^uint64(0))
		db.metrics.reclamationLastNanos.Store(uint64(time.Since(started)))
		if !succeeded {
			db.metrics.reclamationFailed.Add(1)
		}
	}()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return result, ErrClosed
	}
	if db.fatalErr != nil {
		err := db.fatalErr
		db.mu.RUnlock()
		return result, err
	}
	minimumSequence := db.token
	readLockHeld := true
	defer func() {
		if readLockHeld {
			db.mu.RUnlock()
		}
	}()
	if options.Online {
		db.mu.RUnlock()
		readLockHeld = false
	}
	var (
		stats    storagev2.ReachabilityStats
		attempts = 1
		err      error
	)
	if options.Online {
		stats, attempts, err = store.file.ReclaimPagesOptimisticContext(ctx, maxAttempts, !options.MemoryOnly)
	} else {
		stats, err = store.file.ReclaimPagesContext(ctx)
	}
	db.metrics.reclamationScans.Add(uint64(attempts))
	db.metrics.reclamationLastAttempts.Store(uint64(attempts))
	db.metrics.reclamationLastOnline.Store(options.Online)
	if err != nil {
		if errors.Is(err, storagev2.ErrReclamationConflict) {
			db.metrics.reclamationConflicts.Add(1)
		}
		return result, mapStorageV2Error(err)
	}
	if !options.MemoryOnly {
		if err := store.file.PersistFreeSpaceContext(ctx); err != nil {
			return result, mapStorageV2Error(err)
		}
		if readLockHeld {
			db.mu.RUnlock()
			readLockHeld = false
		}
		if err := db.advanceV2RollbackAnchor(ctx, store, minimumSequence); err != nil {
			return result, err
		}
	}
	physical := store.file.StorageStats()
	result = ReclaimV2Result{
		PhysicalPages: physical.PhysicalPages, ReachablePages: stats.ReachablePages,
		ReusablePages: physical.ReusablePages, PinnedSnapshots: stats.PinnedSnapshots,
		Attempts: attempts, Online: options.Online, Persisted: physical.PersistentFreeSpace,
	}
	db.metrics.reclamationReachable.Store(stats.ReachablePages)
	db.metrics.reclamationReclaimable.Store(physical.ReusablePages)
	db.metrics.reclamationCompleted.Add(1)
	succeeded = true
	return result, nil
}
