package v2

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStorageQuotaRejectsBeforePublicationWithoutFailStop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quota.meld2")
	file, _, _, err := OpenWithOptions(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	applyRetentionRevision(t, file, 1)
	usedPages := file.StorageStats().PhysicalPages
	if usedPages <= 2 {
		t.Fatalf("unexpected physical pages=%d", usedPages)
	}
	quotaPages := usedPages - 1
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	beforeBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	file, _, _, err = OpenWithOptions(path, OpenOptions{MaxFileBytes: quotaPages * PageSize})
	if err != nil {
		t.Fatal(err)
	}
	before, err := file.DatabaseRoot()
	if err != nil {
		t.Fatal(err)
	}
	if err := applyRetentionRevisionError(file, 2); !errors.Is(err, ErrStorageLimit) {
		t.Fatalf("quota error=%v", err)
	}
	after, err := file.DatabaseRoot()
	if err != nil {
		t.Fatal(err)
	}
	stats := file.StorageStats()
	if after != before || file.fatalErr != nil || stats.StorageUsedBytes != usedPages*PageSize ||
		stats.StorageMaxBytes != quotaPages*PageSize || stats.StorageByteOverage != PageSize ||
		stats.StorageLimitRejections != 1 || !stats.StorageQuotaExhausted {
		t.Fatalf("before=%+v after=%+v fatal=%v stats=%+v", before, after, file.fatalErr, stats)
	}
	afterBytes, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(afterBytes, beforeBytes) {
		t.Fatalf("quota rejection changed file bytes: equal=%t err=%v", bytes.Equal(afterBytes, beforeBytes), err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, _, _, err := OpenWithOptions(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if root, err := reopened.DatabaseRoot(); err != nil || root.CommitSequence != 1 {
		t.Fatalf("reopened root=%+v err=%v", root, err)
	}
}

func TestStorageQuotaStillAllowsReclaimedPageReuse(t *testing.T) {
	file, _, _, err := OpenWithOptions(filepath.Join(t.TempDir(), "quota-reuse.meld2"), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for revision := byte(1); revision <= 8; revision++ {
		applyRetentionRevision(t, file, revision)
	}
	if _, _, err := file.ReclaimPagesOptimisticContext(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	before := file.StorageStats()
	if before.ReusablePages == 0 {
		t.Fatal("reclamation produced no reusable pages")
	}
	file.maxPhysicalPages = before.PhysicalPages
	applyRetentionRevision(t, file, 9)
	after := file.StorageStats()
	if after.CommitSequence != before.CommitSequence+1 || after.PhysicalPages != before.PhysicalPages || after.StorageLimitRejections != 0 {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
}

func TestInvalidStorageQuotaDoesNotCreateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-quota.meld2")
	if _, _, _, err := OpenWithOptions(path, OpenOptions{MaxFileBytes: 2*PageSize + 1}); !errors.Is(err, ErrInvalidStorageLimit) {
		t.Fatalf("invalid quota error=%v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid quota mutated path: %v", err)
	}
}
