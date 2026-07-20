package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// VerificationResult describes a read-only, full protected-page graph audit.
// ReachablePages excludes the two Meta pages and optional FreeSpace acceleration
// pages, matching ReachabilityStats.
type VerificationResult struct {
	Meta                        Meta
	FileBytes                   uint64
	TrailingBytes               uint64
	PhysicalPages               uint64
	ReachablePages              uint64
	ReclaimablePages            uint64
	ValidMetaSlots              int
	PersistentFreeSpace         bool
	FreeSpaceValid              bool
	SemanticIndexesVerified     bool
	SemanticIndexBuildsVerified bool
	SHA256                      [sha256.Size]byte
}

// VerifyPathContext opens an existing file read-only under a non-blocking
// shared advisory lock. It never creates, truncates, repairs, reclaims, or
// publishes a Meta generation. A running writer's exclusive lock fails closed.
func VerifyPathContext(ctx context.Context, path string) (result VerificationResult, resultErr error) {
	return VerifyPathContextWithIndexAudit(ctx, path, nil)
}

// VerifyPathContextWithIndexAudit additionally proves logical Secondary keys
// against canonical stored documents. The callback runs only under the offline
// shared lock and must be deterministic, bounded and side-effect free.
func VerifyPathContextWithIndexAudit(ctx context.Context, path string, indexAudit IndexAuditFunc) (result VerificationResult, resultErr error) {
	if path == "" {
		return result, errors.New("meldbase storage: empty path")
	}
	if err := contextErr(ctx); err != nil {
		return result, err
	}
	file, err := os.Open(path)
	if err != nil {
		return result, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return result, fmt.Errorf("%w: %v", ErrLocked, err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, syscall.Flock(int(file.Fd()), syscall.LOCK_UN), file.Close())
	}()
	info, err := file.Stat()
	if err != nil {
		return result, err
	}
	if info.Size() < 2*PageSize {
		return result, ErrCorrupt
	}
	fileBytes := uint64(info.Size())
	physicalPages := fileBytes / PageSize
	state, err := inspectExistingFile(file, physicalPages)
	if err != nil {
		return result, err
	}
	opened := state.open(file)
	persistentFreeSpace := state.meta.OptionalFeatures&OptionalFeaturePersistentFreeSpace != 0
	freeSpaceValid := true
	if persistentFreeSpace {
		if err := opened.restoreFreeSpaceContext(ctx, state.root, state.meta); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return result, err
			}
			// FreeSpace is explicitly non-authoritative acceleration metadata. The
			// normal opener discards it and continues with an empty pool too.
			opened.freePages = nil
			opened.freeSpaceTracked = false
			freeSpaceValid = false
		}
	}
	opened.mu.RLock()
	stats, walker, err := opened.reachabilityUnlockedWithIndexAudit(ctx, indexAudit)
	opened.mu.RUnlock()
	if err != nil {
		return result, err
	}
	digest, err := hashReadOnlyFileContext(ctx, file, fileBytes)
	if err != nil {
		return result, err
	}
	validMetaSlots := 0
	for _, valid := range state.metaOK {
		if valid {
			validMetaSlots++
		}
	}
	return VerificationResult{
		Meta: state.meta, FileBytes: fileBytes, TrailingBytes: fileBytes % PageSize,
		PhysicalPages: stats.PhysicalPages, ReachablePages: stats.ReachablePages,
		ReclaimablePages: stats.ReclaimablePages, ValidMetaSlots: validMetaSlots,
		PersistentFreeSpace: persistentFreeSpace, FreeSpaceValid: freeSpaceValid, SHA256: digest,
		SemanticIndexesVerified:     indexAudit != nil,
		SemanticIndexBuildsVerified: walker.semanticIndexBuildsVerified,
	}, nil
}

func hashReadOnlyFileContext(ctx context.Context, file *os.File, size uint64) ([sha256.Size]byte, error) {
	var result [sha256.Size]byte
	if file == nil {
		return result, ErrCorrupt
	}
	hash := sha256.New()
	buffer := make([]byte, 1024*1024)
	for offset := uint64(0); offset < size; {
		if err := contextErr(ctx); err != nil {
			return result, err
		}
		length := min(uint64(len(buffer)), size-offset)
		count, err := file.ReadAt(buffer[:length], int64(offset))
		if err != nil && !errors.Is(err, io.EOF) {
			return result, err
		}
		if uint64(count) != length {
			return result, io.ErrUnexpectedEOF
		}
		_, _ = hash.Write(buffer[:count])
		offset += uint64(count)
	}
	copy(result[:], hash.Sum(nil))
	return result, nil
}
