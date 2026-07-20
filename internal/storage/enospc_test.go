package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"syscall"
	"testing"
)

func TestENOSPCSystemRecordMatrixPublishesOnlyOldOrDurableGeneration(t *testing.T) {
	for _, point := range commitFaultPoints() {
		t.Run(faultPointName(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "system-enospc.meld2")
			file, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			key, oldValue, newValue := []byte("rpc-key"), []byte("old"), []byte("new")
			if result, err := file.ApplySystemRecordTransaction(SystemRecordTransaction{
				TransactionID: [16]byte{1}, Key: key, NewValue: oldValue,
			}); err != nil || !result.Applied {
				t.Fatalf("seed=%+v err=%v", result, err)
			}
			stableSequence := file.Meta().CommitSequence
			appendStart := file.nextPage
			installENOSPCFault(t, file, point, func() {
				if err := file.file.Truncate(int64(appendStart)*PageSize + PageSize/2); err != nil {
					t.Fatal(err)
				}
			})
			_, err = file.ApplySystemRecordTransaction(SystemRecordTransaction{
				TransactionID: [16]byte{2}, Key: key, ExpectedExists: true,
				ExpectedHash: sha256.Sum256(oldValue), NewValue: newValue,
			})
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("system commit error=%v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			expectedSequence, expectedValue := stableSequence, oldValue
			if point == faultAfterMetaSync {
				expectedSequence, expectedValue = stableSequence+1, newValue
			}
			if reopened.Meta().CommitSequence != expectedSequence {
				t.Fatalf("sequence=%d want=%d", reopened.Meta().CommitSequence, expectedSequence)
			}
			actual, exists, err := reopened.GetSystemRecord(key)
			if err != nil || !exists || string(actual) != string(expectedValue) {
				t.Fatalf("system value=%q exists=%t want=%q err=%v", actual, exists, expectedValue, err)
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestENOSPCDocumentSystemMatrixNeverSplitsBusinessAndTerminalState(t *testing.T) {
	for _, point := range commitFaultPoints() {
		t.Run(faultPointName(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "document-system-enospc.meld2")
			file, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			key, pending, terminal := []byte("rpc-key"), []byte("pending"), []byte("terminal")
			policyKey, policyGeneration := []byte("policy-generation"), []byte("generation-2")
			if result, err := file.ApplySystemRecordTransaction(SystemRecordTransaction{
				TransactionID: [16]byte{1}, Key: key, NewValue: pending,
			}); err != nil || !result.Applied {
				t.Fatalf("seed=%+v err=%v", result, err)
			}
			stableSequence := file.Meta().CommitSequence
			appendStart := file.nextPage
			installENOSPCFault(t, file, point, func() {
				if err := file.file.Truncate(int64(appendStart)*PageSize + PageSize/2); err != nil {
					t.Fatal(err)
				}
			})
			id := [16]byte{15: 1}
			_, err = file.ApplyDocumentSystemTransaction(DocumentSystemTransaction{
				DocumentTransaction: DocumentTransaction{TransactionID: [16]byte{2}, Mutations: []DocumentMutation{{
					Collection: "orders", DocumentID: id, Operation: DocumentInsert, Document: []byte("created"),
				}}},
				SystemRecords: []SystemRecordMutation{
					{Key: key, ExpectedExists: true, ExpectedHash: sha256.Sum256(pending), NewValue: terminal},
					{Key: policyKey, NewValue: policyGeneration, Unconditional: true},
				},
			})
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("composite commit error=%v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			expectNew := point == faultAfterMetaSync
			expectedSequence, expectedSystem := stableSequence, pending
			if expectNew {
				expectedSequence, expectedSystem = stableSequence+1, terminal
			}
			if reopened.Meta().CommitSequence != expectedSequence {
				t.Fatalf("sequence=%d want=%d", reopened.Meta().CommitSequence, expectedSequence)
			}
			document, exists, err := reopened.GetDocument("orders", id)
			if err != nil || exists != expectNew || (expectNew && string(document) != "created") {
				t.Fatalf("document=%q exists=%t wantNew=%t err=%v", document, exists, expectNew, err)
			}
			stored, exists, err := reopened.GetSystemRecord(key)
			if err != nil || !exists || string(stored) != string(expectedSystem) {
				t.Fatalf("system=%q exists=%t want=%q err=%v", stored, exists, expectedSystem, err)
			}
			storedPolicy, policyExists, err := reopened.GetSystemRecord(policyKey)
			if err != nil || policyExists != expectNew || (expectNew && string(storedPolicy) != string(policyGeneration)) {
				t.Fatalf("policy=%q exists=%t wantNew=%t err=%v", storedPolicy, policyExists, expectNew, err)
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestENOSPCPersistentFreeSpaceMaintenanceNeverChangesLogicalState(t *testing.T) {
	for _, point := range commitFaultPoints() {
		t.Run(faultPointName(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "free-space-enospc.meld2")
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
			stableSequence := file.Meta().CommitSequence
			if stats, err := file.ReclaimPages(); err != nil || stats.ReclaimablePages == 0 {
				t.Fatalf("reclaim=%+v err=%v", stats, err)
			}
			appendStart := file.nextPage
			installENOSPCFault(t, file, point, func() {
				if err := file.file.Truncate(int64(appendStart)*PageSize + PageSize/2); err != nil {
					t.Fatal(err)
				}
			})
			if err := file.PersistFreeSpace(); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("maintenance error=%v", err)
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
				t.Fatalf("logical sequence=%d want=%d", reopened.Meta().CommitSequence, stableSequence)
			}
			value, exists, err := reopened.GetDocument("items", id)
			if err != nil || !exists || len(value) != 2 || value[1] != 8 {
				t.Fatalf("document=%q exists=%t err=%v", value, exists, err)
			}
			persistent := reopened.StorageStats().PersistentFreeSpace
			if persistent != (point == faultAfterMetaSync) {
				t.Fatalf("persistent=%t point=%s", persistent, faultPointName(point))
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestENOSPCAppendMatrixPublishesOnlyOldOrDurableIndexedGeneration(t *testing.T) {
	for _, point := range commitFaultPoints() {
		t.Run(faultPointName(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "append-enospc.meld2")
			file, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			id := [16]byte{15: 1}
			if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
				TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
					Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("old"),
				}},
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
				TransactionID: [16]byte{2}, Collection: "items", Name: "by_value",
				FieldPath: "value", Entries: []IndexEntry{{Key: []byte("a"), DocumentID: id}},
			}); err != nil {
				t.Fatal(err)
			}
			stableSequence := file.Meta().CommitSequence
			appendStart := file.nextPage
			installENOSPCFault(t, file, point, func() {
				// Simulate a real short append: the kernel accepted only half of
				// the first new page before reporting ENOSPC.
				if err := file.file.Truncate(int64(appendStart)*PageSize + PageSize/2); err != nil {
					t.Fatal(err)
				}
			})

			_, err = file.ApplyDocumentTransaction(DocumentTransaction{
				TransactionID: [16]byte{3}, Mutations: []DocumentMutation{{
					Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("new"),
					Indexes: []IndexMutation{{Name: "by_value", BeforeKey: []byte("a"), AfterKey: []byte("b")}},
				}},
			})
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("commit error=%v", err)
			}
			if _, retryErr := file.ApplyDocumentTransaction(DocumentTransaction{
				TransactionID: [16]byte{4}, Mutations: []DocumentMutation{{
					Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("retry"),
				}},
			}); !errors.Is(retryErr, syscall.ENOSPC) {
				t.Fatalf("poisoned writer error=%v", retryErr)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			expectNew := point == faultAfterMetaSync
			expectedSequence := stableSequence
			expectedDocument, expectedKey := "old", "a"
			if expectNew {
				expectedSequence++
				expectedDocument, expectedKey = "new", "b"
			}
			assertENOSPCIndexedGeneration(t, reopened, id, expectedSequence, expectedDocument, expectedKey)
		})
	}
}

func TestENOSPCReusedPageMatrixCannotCorruptProtectedFallback(t *testing.T) {
	for _, point := range commitFaultPoints() {
		t.Run(faultPointName(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "reuse-enospc.meld2")
			file, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			id := [16]byte{15: 1}
			if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
				TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
					Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("stable"),
				}},
			}); err != nil {
				t.Fatal(err)
			}
			for revision := byte(2); revision <= 9; revision++ {
				if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
					TransactionID: [16]byte{revision}, Mutations: []DocumentMutation{{
						Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("stable"),
					}},
				}); err != nil {
					t.Fatal(err)
				}
			}
			if stats, err := file.ReclaimPages(); err != nil || stats.ReclaimablePages == 0 || len(file.freePages) == 0 {
				t.Fatalf("reclaim stats=%+v free=%d err=%v", stats, len(file.freePages), err)
			}
			if err := file.PersistFreeSpace(); err != nil {
				t.Fatal(err)
			}
			stableSequence := file.Meta().CommitSequence
			firstReused := file.freePages[len(file.freePages)-1]
			oldPage := make([]byte, PageSize)
			if _, err := file.file.ReadAt(oldPage, int64(firstReused)*PageSize); err != nil {
				t.Fatal(err)
			}
			installENOSPCFault(t, file, point, func() {
				// WriteAt completed in the test harness. Restore the old second
				// half to model a physical ENOSPC short overwrite of a reused page.
				if _, err := file.file.WriteAt(oldPage[PageSize/2:], int64(firstReused)*PageSize+PageSize/2); err != nil {
					t.Fatal(err)
				}
			})

			_, err = file.ApplyDocumentTransaction(DocumentTransaction{
				TransactionID: [16]byte{20}, Mutations: []DocumentMutation{{
					Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("new"),
				}},
			})
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("commit error=%v", err)
			}
			if _, retryErr := file.ApplyDocumentTransaction(DocumentTransaction{
				TransactionID: [16]byte{21}, Mutations: []DocumentMutation{{
					Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("retry"),
				}},
			}); !errors.Is(retryErr, syscall.ENOSPC) {
				t.Fatalf("poisoned writer error=%v", retryErr)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			expectedSequence, expectedDocument := stableSequence, "stable"
			if point == faultAfterMetaSync {
				expectedSequence, expectedDocument = stableSequence+1, "new"
			}
			if reopened.Meta().CommitSequence != expectedSequence {
				t.Fatalf("sequence=%d want=%d", reopened.Meta().CommitSequence, expectedSequence)
			}
			document, exists, err := reopened.GetDocument("items", id)
			if err != nil || !exists || string(document) != expectedDocument {
				t.Fatalf("document=%q exists=%t want=%q err=%v", document, exists, expectedDocument, err)
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func commitFaultPoints() []faultPoint {
	return []faultPoint{
		faultAfterPageWrite, faultBeforeDataSync, faultAfterDataSync,
		faultAfterMetaWrite, faultAfterMetaSync,
	}
}

func faultPointName(point faultPoint) string {
	switch point {
	case faultAfterPageWrite:
		return "short_page_write"
	case faultBeforeDataSync:
		return "before_data_sync"
	case faultAfterDataSync:
		return "after_data_sync"
	case faultAfterMetaWrite:
		return "torn_meta_write"
	case faultAfterMetaSync:
		return "after_meta_sync"
	default:
		return fmt.Sprintf("fault_%d", point)
	}
}

func installENOSPCFault(t *testing.T, file *File, point faultPoint, tearPage func()) {
	t.Helper()
	metaSlot := 1 - file.metaSlot
	oldMeta := make([]byte, PageSize)
	if _, err := file.file.ReadAt(oldMeta, int64(metaSlot)*PageSize); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	fired := false
	file.fault = func(current faultPoint) error {
		if fired || current != point {
			return nil
		}
		fired = true
		switch point {
		case faultAfterPageWrite:
			tearPage()
		case faultAfterMetaWrite:
			// Meta fields and their checksum occupy the first 256 bytes. Restore
			// the old first half after the new WriteAt to model a short overwrite
			// that never reached the new publication record.
			if _, err := file.file.WriteAt(oldMeta[:PageSize/2], int64(metaSlot)*PageSize); err != nil {
				t.Fatal(err)
			}
		}
		return syscall.ENOSPC
	}
}

func assertENOSPCIndexedGeneration(t *testing.T, file *File, id [16]byte, sequence uint64, document, key string) {
	t.Helper()
	if file.Meta().CommitSequence != sequence {
		t.Fatalf("sequence=%d want=%d", file.Meta().CommitSequence, sequence)
	}
	actual, exists, err := file.GetDocument("items", id)
	if err != nil || !exists || string(actual) != document {
		t.Fatalf("document=%q exists=%t want=%q err=%v", actual, exists, document, err)
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	entries, err := snapshot.ScanIndex("items", "by_value", nil, nil, 0)
	if err != nil || len(entries) != 1 || entries[0].DocumentID != id || string(entries[0].Key) != key {
		t.Fatalf("entries=%+v want_key=%q err=%v", entries, key, err)
	}
	if _, err := file.Reachability(); err != nil {
		t.Fatal(err)
	}
}
