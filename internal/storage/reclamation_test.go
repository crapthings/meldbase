package storage

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestOptimisticReclamationDoesNotHoldWriterLockAndRetriesConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "online-reclaim.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{15: 1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("before"),
	}}}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	file.reclamationScanStep = func() {
		once.Do(func() {
			close(started)
			<-release
		})
	}
	type auditResult struct {
		stats    ReachabilityStats
		attempts int
		err      error
	}
	audited := make(chan auditResult, 1)
	go func() {
		stats, attempts, err := file.ReclaimPagesOptimisticContext(context.Background(), 2, true)
		audited <- auditResult{stats: stats, attempts: attempts, err: err}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("online audit did not start")
	}

	committed := make(chan error, 1)
	go func() {
		_, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{2}, Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("during-audit"),
		}}})
		committed <- err
	}()
	select {
	case err := <-committed:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("commit waited for the paused graph scan")
	}
	close(release)

	select {
	case result := <-audited:
		if result.err != nil || result.attempts != 2 || result.stats.ReachablePages == 0 {
			t.Fatalf("online audit=%+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("online audit did not retry after the conflicting commit")
	}
	if value, exists, err := file.GetDocument("items", id); err != nil || !exists || string(value) != "during-audit" {
		t.Fatalf("committed value=%q exists=%t err=%v", value, exists, err)
	}
	if _, err := file.Reachability(); err != nil {
		t.Fatal(err)
	}
}

func TestOptimisticReclamationConcurrentWriteReadAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "online-reclaim-stress.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{15: 1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte{0},
	}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errorsSeen := make(chan error, 8)
	recordError := func(err error) {
		select {
		case errorsSeen <- err:
		default:
		}
	}
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for ctx.Err() == nil {
			snapshot, err := file.OpenSnapshot()
			if err != nil {
				if ctx.Err() == nil {
					recordError(err)
				}
				return
			}
			_, exists, readErr := snapshot.GetDocument("items", id)
			closeErr := snapshot.Close()
			if readErr != nil || !exists || closeErr != nil {
				if !exists && readErr == nil {
					readErr = ErrCorrupt
				}
				recordError(errors.Join(readErr, closeErr))
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for ctx.Err() == nil {
			_, _, err := file.ReclaimPagesOptimisticContext(ctx, 1, true)
			switch {
			case err == nil:
				if err := file.PersistFreeSpaceContext(ctx); err != nil && !errors.Is(err, context.Canceled) {
					recordError(err)
					return
				}
			case errors.Is(err, ErrReclamationConflict), errors.Is(err, context.Canceled):
			default:
				recordError(err)
				return
			}
		}
	}()

	const revisions = 100
	for revision := 1; revision <= revisions; revision++ {
		transactionID := [16]byte{byte(revision + 1), byte(revision >> 7)}
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: transactionID, Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte{byte(revision)},
		}}}); err != nil {
			cancel()
			workers.Wait()
			t.Fatal(err)
		}
	}
	cancel()
	workers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := file.ReclaimPagesOptimisticContext(context.Background(), 3, true); err != nil {
		t.Fatal(err)
	}
	if err := file.PersistFreeSpace(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, exists, err := reopened.GetDocument("items", id)
	if err != nil || !exists || len(value) != 1 || value[0] != byte(revisions) {
		t.Fatalf("reopened value=%v exists=%t err=%v", value, exists, err)
	}
	if _, err := reopened.Reachability(); err != nil {
		t.Fatal(err)
	}
}

func TestReclaimPagesProtectsSnapshotsInvalidatesCacheAndReusesPhysicalPages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reclaim.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{15: 1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("v0"),
	}}}); err != nil {
		t.Fatal(err)
	}
	pinned, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if value, exists, err := pinned.GetDocument("items", id); err != nil || !exists || string(value) != "v0" {
		t.Fatalf("pinned initial=%q exists=%t err=%v", value, exists, err)
	}
	for revision := byte(1); revision <= 8; revision++ {
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{revision + 1}, Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte{'v', '0' + revision},
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	protected, err := file.ReclaimPages()
	if err != nil || protected.PinnedSnapshots != 1 || protected.ReclaimablePages == 0 {
		t.Fatalf("protected reclamation=%+v err=%v", protected, err)
	}
	if reusable := file.StorageStats().ReusablePages; reusable != protected.ReclaimablePages {
		t.Fatalf("reusable=%d reclaimable=%d", reusable, protected.ReclaimablePages)
	}
	if err := file.PersistFreeSpace(); err != nil {
		t.Fatal(err)
	}
	physicalBeforeReuse := file.Meta().PhysicalPageCount
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{20}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("v9"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if file.Meta().PhysicalPageCount > physicalBeforeReuse {
		t.Fatalf("transaction did not consume reusable pages: before=%d after=%d", physicalBeforeReuse, file.Meta().PhysicalPageCount)
	}
	if value, exists, err := pinned.GetDocument("items", id); err != nil || !exists || string(value) != "v0" {
		t.Fatalf("reclamation changed pinned value=%q exists=%t err=%v", value, exists, err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	// Rotate both Meta slots beyond the former pinned generation, then reclaim
	// its cached pages. A subsequent reuse must never return stale cached bytes.
	for revision := byte(10); revision <= 12; revision++ {
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{revision + 20}, Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte{'v', revision},
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
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{40}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("fresh"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if value, exists, err := file.GetDocument("items", id); err != nil || !exists || string(value) != "fresh" {
		t.Fatalf("reused page read=%q exists=%t err=%v", value, exists, err)
	}
	if _, err := file.Reachability(); err != nil {
		t.Fatal(err)
	}
}

func TestReusedPagesBeforeMetaPublicationCannotCorruptFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reclaim-fallback.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{15: 1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("stable"),
	}}}); err != nil {
		t.Fatal(err)
	}
	for revision := byte(2); revision <= 8; revision++ {
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{revision}, Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("stable"),
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	if stats, err := file.ReclaimPages(); err != nil || stats.ReclaimablePages == 0 {
		t.Fatalf("reclaim before fault=%+v err=%v", stats, err)
	}
	stableSequence := file.Meta().CommitSequence
	injected := errors.New("injected before meta publication")
	file.fault = func(point faultPoint) error {
		if point == faultAfterDataSync {
			return injected
		}
		return nil
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{20}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("unpublished"),
	}}}); !errors.Is(err, injected) {
		t.Fatalf("fault error=%v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Meta().CommitSequence != stableSequence {
		t.Fatalf("fallback sequence=%d want=%d", reopened.Meta().CommitSequence, stableSequence)
	}
	if value, exists, err := reopened.GetDocument("items", id); err != nil || !exists || string(value) != "stable" {
		t.Fatalf("fallback value=%q exists=%t err=%v", value, exists, err)
	}
	if _, err := reopened.Reachability(); err != nil {
		t.Fatal(err)
	}
}
