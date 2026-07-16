package v2

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPersistentFreeSpaceRestoresAndFiltersLaterReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persistent-free-space.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{15: 1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("v0"),
	}}}); err != nil {
		t.Fatal(err)
	}
	for revision := byte(1); revision <= 10; revision++ {
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{revision + 1}, Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte{'v', revision},
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	logicalSequence := file.Meta().CommitSequence
	if stats, err := file.ReclaimPages(); err != nil || stats.ReclaimablePages == 0 {
		t.Fatalf("reclaim=%+v err=%v", stats, err)
	}
	if err := file.PersistFreeSpace(); err != nil {
		t.Fatal(err)
	}
	persistedMeta := file.Meta()
	persistedRoot, err := file.DatabaseRoot()
	if err != nil || persistedMeta.CommitSequence != logicalSequence || persistedRoot.FreeSpaceRoot == 0 ||
		persistedMeta.OptionalFeatures&OptionalFeaturePersistentFreeSpace == 0 {
		t.Fatalf("persisted meta=%+v root=%+v err=%v", persistedMeta, persistedRoot, err)
	}
	beforeReuse := file.StorageStats().ReusablePages
	if beforeReuse == 0 {
		t.Fatal("maintenance publication consumed the complete free pool")
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{30}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("after-audit"),
	}}}); err != nil {
		t.Fatal(err)
	}
	afterReuse := file.StorageStats().ReusablePages
	currentRoot, err := file.DatabaseRoot()
	if err != nil || afterReuse >= beforeReuse || currentRoot.FreeSpaceRoot != persistedRoot.FreeSpaceRoot {
		t.Fatalf("reuse before=%d after=%d roots=%d/%d err=%v", beforeReuse, afterReuse, persistedRoot.FreeSpaceRoot, currentRoot.FreeSpaceRoot, err)
	}
	physical := file.Meta().PhysicalPageCount
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restoredStats := reopened.StorageStats()
	if restored := restoredStats.ReusablePages; restored != afterReuse {
		restoreErr := reopened.restoreFreeSpace(reopened.root, reopened.meta)
		t.Fatalf("restored reusable=%d want=%d loads=%d failures=%d restoreErr=%v root=%+v meta=%+v", restored, afterReuse,
			reopened.freeSpaceLoads.Load(), reopened.freeSpaceLoadFailures.Load(), restoreErr, reopened.root, reopened.meta)
	}
	wantChecks := beforeReuse - afterReuse + 1
	if !restoredStats.PersistentFreeSpace || restoredStats.FreeSpaceLoads != 1 || restoredStats.FreeSpaceLoadFailures != 0 ||
		restoredStats.FreeSpaceCandidateChecks != wantChecks {
		t.Fatalf("restored stats=%+v wantChecks=%d", restoredStats, wantChecks)
	}
	if value, exists, err := reopened.GetDocument("items", id); err != nil || !exists || string(value) != "after-audit" {
		t.Fatalf("reopened value=%q exists=%t err=%v", value, exists, err)
	}
	if _, err := reopened.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{31}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("reused-after-reopen"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if reopened.Meta().PhysicalPageCount > physical {
		t.Fatalf("reopen lost reusable pool: before=%d after=%d", physical, reopened.Meta().PhysicalPageCount)
	}
	if _, err := reopened.Reachability(); err != nil {
		t.Fatal(err)
	}
	remaining := reopened.StorageStats().ReusablePages
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedAgain, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedAgain.Close()
	if restored := reopenedAgain.StorageStats().ReusablePages; restored != remaining {
		t.Fatalf("second reopen reusable=%d want=%d", restored, remaining)
	}
}

func TestEmptyDatabaseFreeSpacePersistenceIsNoop(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "empty-free-space.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	before := file.Meta()
	if stats, err := file.ReclaimPages(); err != nil || stats.ReclaimablePages != 0 {
		t.Fatalf("reclaim=%+v err=%v", stats, err)
	}
	if err := file.PersistFreeSpace(); err != nil {
		t.Fatal(err)
	}
	if after := file.Meta(); after != before || file.StorageStats().PersistentFreeSpace {
		t.Fatalf("empty maintenance changed meta before=%+v after=%+v stats=%+v", before, after, file.StorageStats())
	}
}

func TestFreeSpaceMaintenanceDoesNotEmitLogicalCommit(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "free-space-live.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{15: 1}
	for revision := byte(1); revision <= 6; revision++ {
		operation := DocumentInsert
		if revision > 1 {
			operation = DocumentUpdate
		}
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{revision}, Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: operation, Document: []byte{'v', revision},
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	sequence := file.Meta().CommitSequence
	stream, err := file.OpenLiveCommitStream(sequence)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan CommitBatch, 1)
	failure := make(chan error, 1)
	go func() {
		batch, err := stream.Next(ctx)
		if err != nil {
			failure <- err
			return
		}
		result <- batch
	}()
	if _, err := file.ReclaimPages(); err != nil {
		t.Fatal(err)
	}
	if err := file.PersistFreeSpace(); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-result:
		t.Fatalf("maintenance emitted logical batch %+v", batch)
	case err := <-failure:
		t.Fatal(err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{20}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("logical"),
	}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-result:
		if batch.Sequence != sequence+1 {
			t.Fatalf("batch sequence=%d want=%d", batch.Sequence, sequence+1)
		}
	case err := <-failure:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestCorruptPersistentFreeSpaceDegradesToEmptyPool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt-free-space.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{15: 1}
	for revision := byte(1); revision <= 8; revision++ {
		operation := DocumentInsert
		if revision > 1 {
			operation = DocumentUpdate
		}
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{revision}, Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: operation, Document: []byte{'v', revision},
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := file.ReclaimPages(); err != nil {
		t.Fatal(err)
	}
	if err := file.PersistFreeSpace(); err != nil {
		t.Fatal(err)
	}
	root, err := file.DatabaseRoot()
	if err != nil || root.FreeSpaceRoot == 0 {
		t.Fatalf("root=%+v err=%v", root, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := flipByte(raw, int64(root.FreeSpaceRoot)*PageSize+PageHeaderSize); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	strict, _, report, err := OpenWithOptions(path, OpenOptions{RequireClean: true})
	if !errors.Is(err, ErrRecoveryRequired) || strict != nil || !report.FreeSpaceLoadDegraded {
		t.Fatalf("strict open file=%v report=%+v err=%v", strict, report, err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("require-clean free-space validation modified the file")
	}

	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	stats := reopened.StorageStats()
	if stats.ReusablePages != 0 || stats.PersistentFreeSpace || stats.FreeSpaceLoads != 1 || stats.FreeSpaceLoadFailures != 1 {
		t.Fatalf("corrupt acceleration stats=%+v", stats)
	}
	if value, exists, err := reopened.GetDocument("items", id); err != nil || !exists || len(value) != 2 {
		t.Fatalf("business value=%q exists=%t err=%v", value, exists, err)
	}
	if _, err := reopened.Reachability(); err != nil {
		t.Fatal(err)
	}
	if stats, err := reopened.ReclaimPages(); err != nil || stats.ReclaimablePages == 0 {
		t.Fatalf("repair reclaim=%+v err=%v", stats, err)
	}
	if err := reopened.PersistFreeSpace(); err != nil {
		t.Fatal(err)
	}
	if stats := reopened.StorageStats(); !stats.PersistentFreeSpace || stats.ReusablePages == 0 {
		t.Fatalf("repaired free space=%+v", stats)
	}
}

func TestReachabilityRejectsReusableCandidateThatBecomesProtected(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "free-space-authority.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{15: 1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("protected"),
	}}}); err != nil {
		t.Fatal(err)
	}
	rootPage := file.Meta().RootPage
	file.freePages = append(file.freePages, rootPage)
	defer func() { file.freePages = file.freePages[:len(file.freePages)-1] }()

	if _, err := file.Reachability(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reachability accepted protected reusable page %d: %v", rootPage, err)
	}
	if _, err := file.ReclaimPages(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reclamation accepted protected reusable page %d: %v", rootPage, err)
	}
}
