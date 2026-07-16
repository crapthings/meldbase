package v2

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCreateIndexPublishesCatalogSecondaryTreeAndCommitAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "indexes.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ids := [][16]byte{{1}, {2}, {3}}
	mutations := make([]DocumentMutation, len(ids))
	for index, id := range ids {
		mutations[index] = DocumentMutation{Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte{byte(index + 1)}}
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: mutations}); err != nil {
		t.Fatal(err)
	}
	sequence, err := file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: randomTransactionID(t), Collection: "items", Name: "by_value", FieldPath: "value",
		Entries: []IndexEntry{{Key: []byte("a"), DocumentID: ids[1]}, {Key: []byte("b"), DocumentID: ids[2]}, {Key: []byte("a"), DocumentID: ids[0]}},
	})
	if err != nil || sequence != 2 {
		t.Fatalf("sequence=%d err=%v", sequence, err)
	}
	root, err := file.DatabaseRoot()
	if err != nil || root.CommitSequence != 2 || root.CatalogGeneration != 2 || root.CollectionCount != 1 || root.DocumentCount != 3 {
		t.Fatalf("root=%+v err=%v", root, err)
	}
	batch, err := file.ReadCommit(root.CommitLogRoot, 2)
	if err != nil || len(batch.Changes) != 1 || batch.Changes[0].Operation != CommitCatalog || batch.Changes[0].ChangedPaths[0] != "_indexes.by_value" {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	snapshot, stream, err := file.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	meta, exists, err := snapshot.IndexMeta("items", "by_value")
	if err != nil || !exists || meta.Root < 2 || meta.EntryCount != 3 || meta.Unique || meta.FieldPath != "value" {
		t.Fatalf("meta=%+v exists=%t err=%v", meta, exists, err)
	}
	collections, err := snapshot.Collections()
	if err != nil || len(collections) != 1 || collections[0].Name != "items" || collections[0].Meta.ID != metaForCollection(t, snapshot, "items").ID {
		t.Fatalf("collections=%+v err=%v", collections, err)
	}
	indexes, err := snapshot.Indexes("items")
	if err != nil || len(indexes) != 1 || indexes[0].Name != "by_value" || indexes[0].Root != meta.Root {
		t.Fatalf("indexes=%+v err=%v", indexes, err)
	}
	entries, err := snapshot.ScanIndex("items", "by_value", []byte("a"), []byte("b"), 0)
	if err != nil || len(entries) != 2 || entries[0].DocumentID != ids[0] || entries[1].DocumentID != ids[1] {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
	_ = snapshot.Close()
	_ = stream.Close()
	stats, err := file.Reachability()
	if err != nil || stats.ReachablePages == 0 {
		t.Fatalf("reachability=%+v err=%v", stats, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, recoveredStream, err := reopened.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	defer recoveredStream.Close()
	entries, err = recovered.ScanIndex("items", "by_value", nil, nil, 0)
	if err != nil || len(entries) != 3 {
		t.Fatalf("recovered entries=%+v err=%v", entries, err)
	}
}

func metaForCollection(t *testing.T, snapshot *ReadSnapshot, name string) CollectionMeta {
	t.Helper()
	meta, exists, err := snapshot.CollectionMeta(name)
	if err != nil || !exists {
		t.Fatalf("collection %q meta=%+v exists=%t err=%v", name, meta, exists, err)
	}
	return meta
}

func TestCreateUniqueIndexRejectsConflictWithoutPublishing(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "unique-index.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ids := [][16]byte{{1}, {2}}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: ids[0], Operation: DocumentInsert, Document: []byte("one")},
		{Collection: "items", DocumentID: ids[1], Operation: DocumentInsert, Document: []byte("two")},
	}}); err != nil {
		t.Fatal(err)
	}
	_, err = file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: randomTransactionID(t), Collection: "items", Name: "unique_value", FieldPath: "value", Unique: true,
		Entries: []IndexEntry{{Key: []byte("same"), DocumentID: ids[0]}, {Key: []byte("same"), DocumentID: ids[1]}},
	})
	if err == nil || file.Meta().CommitSequence != 1 {
		t.Fatalf("unique conflict err=%v sequence=%d", err, file.Meta().CommitSequence)
	}
	root, _ := file.DatabaseRoot()
	if root.CatalogGeneration != 1 {
		t.Fatalf("catalog generation=%d", root.CatalogGeneration)
	}
}

func TestCreateIndexCanCreateEmptyCollection(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "empty-index.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: randomTransactionID(t), Collection: "items", Name: "by_value", FieldPath: "value", Unique: true,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, stream, err := file.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	defer stream.Close()
	collection, exists, err := snapshot.CollectionMeta("items")
	if err != nil || !exists || collection.DocumentCount != 0 || collection.PrimaryRoot < 2 || collection.IndexCatalogRoot < 2 {
		t.Fatalf("collection=%+v exists=%t err=%v", collection, exists, err)
	}
	meta, exists, err := snapshot.IndexMeta("items", "by_value")
	if err != nil || !exists || meta.EntryCount != 0 || !meta.Unique {
		t.Fatalf("index=%+v exists=%t err=%v", meta, exists, err)
	}
}

func TestIndexMetaCodecRejectsReservedAndInvalidValues(t *testing.T) {
	meta := IndexMeta{Name: "by_value", FieldPath: "value", Root: 2, CreatedSequence: 1, UpdatedSequence: 1, KeyCodecVersion: indexKeyCodecV2}
	encoded, err := encodeIndexMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeIndexMeta(meta.Name, encoded)
	if err != nil || decoded.Name != meta.Name || decoded.FieldPath != meta.FieldPath {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	for _, offset := range []int{0, 8, 10, 13, 52} {
		corrupt := append([]byte(nil), encoded...)
		corrupt[offset] ^= 0xff
		if _, err := decodeIndexMeta(meta.Name, corrupt); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("offset=%d err=%v", offset, err)
		}
	}
}

func TestCompoundIndexMetaCodecRoundTripsAndRejectsCorruption(t *testing.T) {
	meta := IndexMeta{
		Name: "tenant_score", FieldPath: "tenant",
		Fields: []IndexField{{Path: "tenant", Direction: 1}, {Path: "score", Direction: -1}},
		Unique: true, Root: 2, CreatedSequence: 1, UpdatedSequence: 2,
	}
	encoded, err := encodeIndexMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeIndexMeta(meta.Name, encoded)
	if err != nil || decoded.KeyCodecVersion != indexKeyCodecV3 || !decoded.Unique ||
		!reflect.DeepEqual(decoded.Fields, meta.Fields) || decoded.FieldPath != meta.FieldPath {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	for name, mutate := range map[string]func([]byte){
		"count":     func(value []byte) { binary.LittleEndian.PutUint16(value[52:54], 3) },
		"direction": func(value []byte) { value[indexMetaHeaderBytes] = 0 },
		"reserved":  func(value []byte) { value[54] = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			corrupt := append([]byte(nil), encoded...)
			mutate(corrupt)
			if _, err := decodeIndexMeta(meta.Name, corrupt); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	duplicate := meta
	duplicate.Fields = []IndexField{{Path: "tenant", Direction: 1}, {Path: "tenant", Direction: -1}}
	if _, err := encodeIndexMeta(duplicate); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("duplicate field error = %v", err)
	}
}

func TestCompoundIndexRequiredFeatureCannotBeDetachedFromReachableMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compound-feature.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("document"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: [16]byte{2}, Collection: "items", Name: "a_b", FieldPath: "a",
		Fields:  []IndexField{{Path: "a", Direction: 1}, {Path: "b", Direction: -1}},
		Entries: []IndexEntry{{Key: []byte("tuple"), DocumentID: id}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	selectedSlot, selected := -1, Meta{}
	for slot := range 2 {
		page := make([]byte, PageSize)
		if _, err := raw.ReadAt(page, int64(slot*PageSize)); err != nil {
			t.Fatal(err)
		}
		meta, err := DecodeMeta(page)
		if err == nil && (selectedSlot < 0 || meta.Generation > selected.Generation) {
			selectedSlot, selected = slot, meta
		}
	}
	if selectedSlot < 0 || selected.RequiredFeatures&RequiredFeatureCompoundIndexes == 0 {
		t.Fatalf("selected meta=%+v slot=%d", selected, selectedSlot)
	}
	selected.RequiredFeatures &^= RequiredFeatureCompoundIndexes
	encoded, err := EncodeMeta(selected)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.WriteAt(encoded, int64(selectedSlot*PageSize)); err != nil {
		t.Fatal(err)
	}
	if err := raw.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	opened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.Reachability(); !errors.Is(err, ErrCorrupt) {
		_ = opened.Close()
		t.Fatalf("detached feature reachability error = %v", err)
	}
	_ = opened.Close()
	if _, err := VerifyPathContext(context.Background(), path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("detached feature verification error = %v", err)
	}
}

func TestIndexIteratorOwnsSnapshotPinAndStreamsEntries(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "index-iterator.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ids := [][16]byte{{3}, {1}, {2}}
	mutations := make([]DocumentMutation, len(ids))
	entries := make([]IndexEntry, len(ids))
	for index, id := range ids {
		mutations[index] = DocumentMutation{Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte{byte(index + 1)}}
		entries[index] = IndexEntry{Key: []byte{byte('c' - index)}, DocumentID: id}
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: mutations}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: randomTransactionID(t), Collection: "items", Name: "by_value", FieldPath: "value", Entries: entries,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	iterator, err := snapshot.OpenIndexIterator("items", "by_value", []byte("a"), []byte("d"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if stats := file.StorageStats(); stats.ActiveReaders != 2 {
		t.Fatalf("active readers=%d", stats.ActiveReaders)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := file.StorageStats(); stats.ActiveReaders != 1 {
		t.Fatalf("iterator did not retain independent pin: %+v", stats)
	}
	var keys []string
	for iterator.Next() {
		keys = append(keys, string(iterator.Entry().Key))
	}
	if err := iterator.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(keys) != "[a b c]" {
		t.Fatalf("keys=%v", keys)
	}
	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := file.StorageStats(); stats.ActiveReaders != 0 {
		t.Fatalf("iterator leaked pin: %+v", stats)
	}
}

func TestDocumentTransactionMaintainsIndexesAndAllowsUniqueKeySwap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index-maintenance.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, second, third := [16]byte{1}, [16]byte{2}, [16]byte{3}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("first-a")},
		{Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: []byte("second-b")},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: randomTransactionID(t), Collection: "items", Name: "unique_value", FieldPath: "value", Unique: true,
		Entries: []IndexEntry{{Key: []byte("a"), DocumentID: first}, {Key: []byte("b"), DocumentID: second}},
	}); err != nil {
		t.Fatal(err)
	}
	sequence, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: first, Operation: DocumentUpdate, Document: []byte("first-b"), Indexes: []IndexMutation{{Name: "unique_value", BeforeKey: []byte("a"), AfterKey: []byte("b")}}},
		{Collection: "items", DocumentID: second, Operation: DocumentUpdate, Document: []byte("second-a"), Indexes: []IndexMutation{{Name: "unique_value", BeforeKey: []byte("b"), AfterKey: []byte("a")}}},
	}})
	if err != nil || sequence != 3 {
		t.Fatalf("swap sequence=%d err=%v", sequence, err)
	}
	snapshot, stream, err := file.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := snapshot.ScanIndex("items", "unique_value", []byte("a"), []byte("b"), 0)
	if err != nil || len(entries) != 1 || entries[0].DocumentID != second {
		t.Fatalf("a entries=%+v err=%v", entries, err)
	}
	entries, err = snapshot.ScanIndex("items", "unique_value", []byte("b"), []byte("c"), 0)
	if err != nil || len(entries) != 1 || entries[0].DocumentID != first {
		t.Fatalf("b entries=%+v err=%v", entries, err)
	}
	_ = snapshot.Close()
	_ = stream.Close()

	// A conflicting insert and an incomplete index mutation both leave the
	// document, secondary tree, and commit sequence unchanged.
	_, err = file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: third, Operation: DocumentInsert, Document: []byte("third-a"),
		Indexes: []IndexMutation{{Name: "unique_value", AfterKey: []byte("a")}},
	}}})
	if err == nil || file.Meta().CommitSequence != 3 {
		t.Fatalf("conflict err=%v sequence=%d", err, file.Meta().CommitSequence)
	}
	if _, ok, err := file.GetDocument("items", third); err != nil || ok {
		t.Fatalf("conflicting document ok=%t err=%v", ok, err)
	}
	_, err = file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: first, Operation: DocumentUpdate, Document: []byte("missing-index-change"),
	}}})
	if !errors.Is(err, ErrCorrupt) || file.Meta().CommitSequence != 3 {
		t.Fatalf("missing index mutation err=%v sequence=%d", err, file.Meta().CommitSequence)
	}

	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: second, Operation: DocumentDelete,
		Indexes: []IndexMutation{{Name: "unique_value", BeforeKey: []byte("a")}},
	}}}); err != nil {
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
	recovered, recoveredStream, err := reopened.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	defer recoveredStream.Close()
	entries, err = recovered.ScanIndex("items", "unique_value", nil, nil, 0)
	if err != nil || len(entries) != 1 || entries[0].DocumentID != first || string(entries[0].Key) != "b" {
		t.Fatalf("recovered entries=%+v err=%v", entries, err)
	}
	meta, exists, err := recovered.IndexMeta("items", "unique_value")
	if err != nil || !exists || meta.EntryCount != 1 || meta.UpdatedSequence != 4 {
		t.Fatalf("recovered meta=%+v exists=%t err=%v", meta, exists, err)
	}
}

func TestIndexedDocumentCommitFaultsNeverExposeMixedPrimaryAndSecondaryRoots(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "indexed-base.meld2")
	base, _, err := Open(basePath)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{1}
	if _, err := base.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("old"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := base.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: randomTransactionID(t), Collection: "items", Name: "unique_value", FieldPath: "value", Unique: true,
		Entries: []IndexEntry{{Key: []byte("a"), DocumentID: id}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	points := []faultPoint{faultAfterPageWrite, faultBeforeDataSync, faultAfterDataSync, faultAfterMetaWrite, faultAfterMetaSync}
	for _, point := range points {
		t.Run(fmt.Sprint(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.meld2")
			if err := os.WriteFile(path, baseBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			candidate, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected indexed commit crash")
			candidate.fault = func(current faultPoint) error {
				if current == point {
					return injected
				}
				return nil
			}
			if _, err := candidate.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
				Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("new"),
				Indexes: []IndexMutation{{Name: "unique_value", BeforeKey: []byte("a"), AfterKey: []byte("b")}},
			}}}); !errors.Is(err, injected) {
				t.Fatalf("commit error=%v", err)
			}
			if err := candidate.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			root, err := reopened.DatabaseRoot()
			if err != nil || (root.CommitSequence != 2 && root.CommitSequence != 3) {
				t.Fatalf("root=%+v err=%v", root, err)
			}
			document, exists, err := reopened.GetDocument("items", id)
			if err != nil || !exists {
				t.Fatalf("document=%q exists=%t err=%v", document, exists, err)
			}
			snapshot, stream, err := reopened.OpenSnapshotAndStream()
			if err != nil {
				t.Fatal(err)
			}
			entries, err := snapshot.ScanIndex("items", "unique_value", nil, nil, 0)
			_ = snapshot.Close()
			_ = stream.Close()
			if err != nil || len(entries) != 1 || entries[0].DocumentID != id {
				t.Fatalf("entries=%+v err=%v", entries, err)
			}
			if root.CommitSequence == 2 && (string(document) != "old" || string(entries[0].Key) != "a") {
				t.Fatalf("old generation mixed document=%q entries=%+v", document, entries)
			}
			if root.CommitSequence == 3 && (string(document) != "new" || string(entries[0].Key) != "b") {
				t.Fatalf("new generation mixed document=%q entries=%+v", document, entries)
			}
		})
	}
}
