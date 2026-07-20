package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestDocumentCatalogAndCommitLogPublishAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "documents.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	large := bytes.Repeat([]byte("document-payload"), 4000)
	firstID, secondID := [16]byte{1}, [16]byte{2}
	txID := randomTransactionID(t)
	sequence, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: txID, CommittedAt: time.Unix(100, 7),
		Mutations: []DocumentMutation{
			{Collection: "tasks", DocumentID: firstID, Operation: DocumentInsert, Document: large, ChangedPaths: []string{"title"}},
			{Collection: "tasks", DocumentID: secondID, Operation: DocumentInsert, Document: []byte("second")},
		},
	})
	if err != nil || sequence != 1 {
		t.Fatalf("sequence=%d err=%v", sequence, err)
	}
	root1, err := file.DatabaseRoot()
	if err != nil || root1.CommitSequence != 1 || root1.CollectionCount != 1 || root1.DocumentCount != 2 || root1.CatalogRoot == 0 || root1.CommitLogRoot == 0 {
		t.Fatalf("root1=%+v err=%v", root1, err)
	}
	value, ok, err := file.GetDocument("tasks", firstID)
	if err != nil || !ok || !bytes.Equal(value, large) {
		t.Fatalf("large document len=%d ok=%t err=%v", len(value), ok, err)
	}
	batch1, err := file.ReadCommit(root1.CommitLogRoot, 1)
	if err != nil || batch1.CatalogRoot != root1.CatalogRoot || len(batch1.Changes) != 3 || batch1.Changes[1].AfterRef == nil || batch1.Changes[2].AfterRef == nil {
		t.Fatalf("batch1=%+v err=%v", batch1, err)
	}
	version1 := *batch1.Changes[1].AfterRef
	versionValue, err := file.ReadDocumentVersion(version1)
	if err != nil || !bytes.Equal(versionValue, large) {
		t.Fatalf("version1 len=%d err=%v", len(versionValue), err)
	}

	updated := []byte("updated-small-document")
	sequence, err = file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t), CommittedAt: time.Unix(101, 8),
		Mutations: []DocumentMutation{
			{Collection: "tasks", DocumentID: firstID, Operation: DocumentUpdate, Document: updated, ChangedPaths: []string{"title"}},
			{Collection: "tasks", DocumentID: secondID, Operation: DocumentDelete},
		},
	})
	if err != nil || sequence != 2 {
		t.Fatalf("sequence=%d err=%v", sequence, err)
	}
	root2, err := file.DatabaseRoot()
	if err != nil || root2.DocumentCount != 1 || root2.CollectionCount != 1 || root2.CatalogGeneration != 1 {
		t.Fatalf("root2=%+v err=%v", root2, err)
	}
	current, ok, err := file.GetDocument("tasks", firstID)
	if err != nil || !ok || !bytes.Equal(current, updated) {
		t.Fatalf("current=%q ok=%t err=%v", current, ok, err)
	}
	if _, ok, err := file.GetDocument("tasks", secondID); err != nil || ok {
		t.Fatalf("deleted document ok=%t err=%v", ok, err)
	}
	batch2, err := file.ReadCommit(root2.CommitLogRoot, 2)
	if err != nil || batch2.CatalogRoot != root2.CatalogRoot || len(batch2.Changes) != 2 || batch2.Changes[0].BeforeRef == nil || batch2.Changes[0].AfterRef == nil || batch2.Changes[1].BeforeRef == nil || batch2.Changes[1].AfterRef != nil {
		t.Fatalf("batch2=%+v err=%v", batch2, err)
	}
	// Both pre- and post-images resolve through immutable primary roots without
	// duplicating the large document in the Commit Log.
	beforeUpdate, err := file.ReadDocumentVersion(*batch2.Changes[0].BeforeRef)
	if err != nil || !bytes.Equal(beforeUpdate, large) {
		t.Fatalf("before update len=%d err=%v", len(beforeUpdate), err)
	}
	afterUpdate, err := file.ReadDocumentVersion(*batch2.Changes[0].AfterRef)
	if err != nil || !bytes.Equal(afterUpdate, updated) {
		t.Fatalf("after update=%q err=%v", afterUpdate, err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, ok, err := reopened.GetDocument("tasks", firstID)
	if err != nil || !ok || !bytes.Equal(recovered, updated) {
		t.Fatalf("recovered=%q ok=%t err=%v", recovered, ok, err)
	}
	recoveredRoot, err := reopened.DatabaseRoot()
	if err != nil || recoveredRoot.CommitSequence != 2 || recoveredRoot.CommitLogRoot != root2.CommitLogRoot {
		t.Fatalf("recovered root=%+v err=%v", recoveredRoot, err)
	}
}

func TestDocumentTransactionRejectsDuplicateAndFailedPreconditionsWithoutCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "preconditions.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{1}
	_, err = file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations: []DocumentMutation{
			{Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("one")},
			{Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("two")},
		},
	})
	if err == nil || file.Meta().CommitSequence != 0 {
		t.Fatalf("duplicate mutation err=%v sequence=%d", err, file.Meta().CommitSequence)
	}
	_, err = file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations:     []DocumentMutation{{Collection: "items", DocumentID: id, Operation: DocumentDelete}},
	})
	if err == nil || file.Meta().CommitSequence != 0 {
		t.Fatalf("missing delete err=%v sequence=%d", err, file.Meta().CommitSequence)
	}
}

func TestDocumentReadSetPreconditionsAreAtomicAndRejectOnlyPointConflicts(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "document-read-set.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	first, second, missing := [16]byte{1}, [16]byte{2}, [16]byte{3}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations: []DocumentMutation{
			{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("one")},
			{Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: []byte("two")},
		},
	}); err != nil {
		t.Fatal(err)
	}
	readSet := []DocumentPrecondition{
		{Collection: "items", DocumentID: first, ExpectedExists: true, ExpectedHash: sha256.Sum256([]byte("one"))},
		{Collection: "items", DocumentID: missing},
	}
	if err := file.ValidateDocumentPreconditions(readSet); err != nil {
		t.Fatalf("current read set: %v", err)
	}
	sequence, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t), Preconditions: readSet,
		Mutations: []DocumentMutation{{Collection: "items", DocumentID: second, Operation: DocumentUpdate, Document: []byte("three")}},
	})
	if err != nil || sequence != 2 {
		t.Fatalf("disjoint sequence=%d err=%v", sequence, err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations:     []DocumentMutation{{Collection: "items", DocumentID: first, Operation: DocumentUpdate, Document: []byte("changed")}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := file.ValidateDocumentPreconditions(readSet); !errors.Is(err, ErrDocumentConflict) {
		t.Fatalf("stale validation err=%v", err)
	}
	sequence, err = file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t), Preconditions: readSet,
		Mutations: []DocumentMutation{{Collection: "items", DocumentID: second, Operation: DocumentUpdate, Document: []byte("must-not-publish")}},
	})
	if !errors.Is(err, ErrDocumentConflict) || sequence != 0 || file.Meta().CommitSequence != 3 {
		t.Fatalf("stale mutation sequence=%d meta=%d err=%v", sequence, file.Meta().CommitSequence, err)
	}
	value, exists, err := file.GetDocument("items", second)
	if err != nil || !exists || !bytes.Equal(value, []byte("three")) {
		t.Fatalf("rejected value=%q exists=%t err=%v", value, exists, err)
	}
	malformed := DocumentPrecondition{Collection: "items", DocumentID: missing, ExpectedHash: sha256.Sum256([]byte("invalid"))}
	if err := file.ValidateDocumentPreconditions([]DocumentPrecondition{malformed}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("malformed precondition err=%v", err)
	}
	if err := file.ValidateDocumentPreconditions([]DocumentPrecondition{readSet[1], readSet[1]}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("duplicate precondition err=%v", err)
	}
}

func TestCollectionPreconditionsRejectPhantomsAndRemainAtomic(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "collection-preconditions.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	first, second := [16]byte{1}, [16]byte{2}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations: []DocumentMutation{
			{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("one")},
			{Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: []byte("two")},
		},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	meta, exists, err := snapshot.CollectionMeta("items")
	if closeErr := snapshot.Close(); err != nil {
		t.Fatal(err)
	} else if closeErr != nil {
		t.Fatal(closeErr)
	}
	if !exists || meta.ID == 0 || meta.UpdatedSequence != 1 {
		t.Fatalf("snapshot meta=%+v exists=%t", meta, exists)
	}
	items := CollectionPrecondition{Collection: "items", ExpectedExists: true, ExpectedID: meta.ID, ExpectedUpdatedSequence: meta.UpdatedSequence}
	if err := file.ValidateCollectionPreconditions([]CollectionPrecondition{items}); err != nil {
		t.Fatalf("current collection fence: %v", err)
	}
	missing := CollectionPrecondition{Collection: "future", ExpectedExists: false}
	if err := file.ValidateCollectionPreconditions([]CollectionPrecondition{missing}); err != nil {
		t.Fatalf("missing collection fence: %v", err)
	}

	// This independent write is a phantom relative to any predicate read of
	// items. The broad collection fence must reject a later, otherwise disjoint
	// point mutation atomically.
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations:     []DocumentMutation{{Collection: "items", DocumentID: second, Operation: DocumentUpdate, Document: []byte("two-updated")}},
	}); err != nil {
		t.Fatal(err)
	}
	sequence, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID:           randomTransactionID(t),
		CollectionPreconditions: []CollectionPrecondition{items},
		Mutations:               []DocumentMutation{{Collection: "items", DocumentID: first, Operation: DocumentUpdate, Document: []byte("must-not-publish")}},
	})
	if !errors.Is(err, ErrDocumentConflict) || sequence != 0 || file.Meta().CommitSequence != 2 {
		t.Fatalf("stale collection transaction sequence=%d meta=%d err=%v", sequence, file.Meta().CommitSequence, err)
	}
	value, exists, err := file.GetDocument("items", first)
	if err != nil || !exists || !bytes.Equal(value, []byte("one")) {
		t.Fatalf("atomic rejection value=%q exists=%t err=%v", value, exists, err)
	}
	if err := file.ValidateCollectionPreconditions([]CollectionPrecondition{items}); !errors.Is(err, ErrDocumentConflict) {
		t.Fatalf("stale collection validation err=%v", err)
	}

	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations:     []DocumentMutation{{Collection: "future", DocumentID: [16]byte{3}, Operation: DocumentInsert, Document: []byte("created")}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := file.ValidateCollectionPreconditions([]CollectionPrecondition{missing}); !errors.Is(err, ErrDocumentConflict) {
		t.Fatalf("created collection validation err=%v", err)
	}
	if err := file.ValidateCollectionPreconditions([]CollectionPrecondition{{Collection: "items", ExpectedExists: true}, {Collection: "items", ExpectedExists: true}}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("malformed/duplicate collection fences err=%v", err)
	}
}

func TestDocumentInsertionPositionsSurviveUpdateSnapshotAndReinsert(t *testing.T) {
	path := filepath.Join(t.TempDir(), "document-order.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Insert in the opposite order from the primary-key traversal order. The
	// durable position, not the B+Tree key, defines the stable query tie-breaker.
	first, second := [16]byte{2}, [16]byte{1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations: []DocumentMutation{
			{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("first")},
			{Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: []byte("second")},
		},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot1, stream, err := file.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot1.Close()
	defer stream.Close()
	records, err := snapshot1.ScanCollection("items", nil, nil, 0)
	if err != nil || len(records) != 2 {
		t.Fatalf("records=%+v err=%v", records, err)
	}
	positions := map[[16]byte]uint64{}
	for _, record := range records {
		positions[record.DocumentID] = record.InsertionPosition
	}
	if positions[first] != 1 || positions[second] != 2 {
		t.Fatalf("initial positions=%v", positions)
	}
	meta, exists, err := snapshot1.CollectionMeta("items")
	if err != nil || !exists || meta.NextDocumentPosition != 2 {
		t.Fatalf("meta=%+v exists=%t err=%v", meta, exists, err)
	}

	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations:     []DocumentMutation{{Collection: "items", DocumentID: first, Operation: DocumentUpdate, Document: []byte("updated")}},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot2, stream2, err := file.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	records, err = snapshot2.ScanCollection("items", nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.DocumentID == first && record.InsertionPosition != 1 {
			t.Fatalf("update changed position to %d", record.InsertionPosition)
		}
	}
	_ = snapshot2.Close()
	_ = stream2.Close()

	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations:     []DocumentMutation{{Collection: "items", DocumentID: first, Operation: DocumentDelete}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations:     []DocumentMutation{{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("reinserted")}},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot3, stream3, err := file.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot3.Close()
	defer stream3.Close()
	records, err = snapshot3.ScanCollection("items", nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	positions = map[[16]byte]uint64{}
	for _, record := range records {
		positions[record.DocumentID] = record.InsertionPosition
	}
	if positions[second] != 2 || positions[first] != 3 {
		t.Fatalf("reinsert positions=%v", positions)
	}
	meta, exists, err = snapshot3.CollectionMeta("items")
	if err != nil || !exists || meta.NextDocumentPosition != 3 {
		t.Fatalf("reinsert meta=%+v exists=%t err=%v", meta, exists, err)
	}
}

func TestDocumentRecordDescriptorRejectsNonCanonicalHeaders(t *testing.T) {
	tx := &WriteTxn{}
	if _, _, err := tx.loadDocumentRecord(nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("nil record error=%v", err)
	}
	valid := make([]byte, documentRecordHeaderBytes+2)
	copy(valid[:8], documentRecordMagic[:])
	binary.LittleEndian.PutUint16(valid[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(valid[10:12], documentRecordHeaderBytes)
	binary.LittleEndian.PutUint64(valid[16:24], 1)
	copy(valid[24:], []byte{0, 'x'})
	position, value, err := tx.loadDocumentRecord(valid)
	if err != nil || position != 1 || string(value) != "x" {
		t.Fatalf("position=%d value=%q err=%v", position, value, err)
	}
	for _, offset := range []int{0, 8, 10, 12} {
		corrupt := append([]byte(nil), valid...)
		corrupt[offset] ^= 0xff
		if _, _, err := tx.loadDocumentRecord(corrupt); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("offset=%d error=%v", offset, err)
		}
	}
	zeroPosition := append([]byte(nil), valid...)
	clear(zeroPosition[16:24])
	if _, _, err := tx.loadDocumentRecord(zeroPosition); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("zero position error=%v", err)
	}
}
