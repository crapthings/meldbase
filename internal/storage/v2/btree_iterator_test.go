package v2

import (
	"bytes"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

func TestTreeIteratorRangeLimitAndOldRoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iterator.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var oldRoot uint64
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		tree, err := tx.OpenTree(0, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		for index := 0; index < 5_000; index++ {
			if err := tree.Put([]byte(fmt.Sprintf("key-%05d", index)), bytes.Repeat([]byte{byte(index)}, 24)); err != nil {
				return DatabaseRoot{}, err
			}
		}
		oldRoot, err = tree.Flush()
		return DatabaseRoot{CommitSequence: tx.Sequence(), CatalogRoot: oldRoot, DocumentCount: 5_000}, err
	}); err != nil {
		t.Fatal(err)
	}

	iterator, err := newTreeIterator(file, oldRoot, TreePrimary, []byte("key-01234"), []byte("key-01300"), 17)
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.Close()
	for index := 0; index < 17; index++ {
		if !iterator.Next() {
			t.Fatalf("entry %d missing: %v", index, iterator.Err())
		}
		want := fmt.Sprintf("key-%05d", 1234+index)
		if string(iterator.Key()) != want || len(iterator.Value()) != 24 {
			t.Fatalf("entry %d key=%q value=%d", index, iterator.Key(), len(iterator.Value()))
		}
	}
	if iterator.Next() || iterator.Err() != nil {
		t.Fatalf("iterator exceeded limit err=%v", iterator.Err())
	}

	var newRoot uint64
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		tree, err := tx.OpenTree(oldRoot, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := tree.Put([]byte("key-01234"), []byte("new")); err != nil {
			return DatabaseRoot{}, err
		}
		newRoot, err = tree.Flush()
		return DatabaseRoot{CommitSequence: tx.Sequence(), CatalogRoot: newRoot, DocumentCount: 5_000}, err
	}); err != nil {
		t.Fatal(err)
	}
	old, err := newTreeIterator(file, oldRoot, TreePrimary, []byte("key-01234"), []byte("key-01235"), 0)
	if err != nil || !old.Next() || len(old.Value()) != 24 || old.Next() || old.Err() != nil {
		t.Fatalf("old root value=%q err=%v", old.Value(), err)
	}
	current, err := newTreeIterator(file, newRoot, TreePrimary, []byte("key-01234"), []byte("key-01235"), 0)
	if err != nil || !current.Next() || string(current.Value()) != "new" {
		t.Fatalf("new root value=%q err=%v", current.Value(), err)
	}

	empty, err := newTreeIterator(file, oldRoot, TreePrimary, []byte("same"), []byte("same"), 0)
	if err != nil || empty.Next() || empty.Err() != nil {
		t.Fatalf("equal-bound iterator err=%v", err)
	}
}

func TestDocumentIteratorOwnsPinAndPreservesSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "document-iterator.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	mutations := make([]DocumentMutation, 200)
	for index := range mutations {
		value := []byte(fmt.Sprintf("old-%03d", index))
		if index == 53 {
			value = bytes.Repeat([]byte("overflow"), 2_000)
		}
		mutations[index] = DocumentMutation{
			Collection: "items", DocumentID: iteratorDocumentID(index), Operation: DocumentInsert, Document: value,
		}
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: mutations}); err != nil {
		t.Fatal(err)
	}
	snapshot, stream, err := file.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	start, end := iteratorDocumentID(50), iteratorDocumentID(80)
	iterator, err := snapshot.OpenCollectionIterator("items", &start, &end, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.Close()
	stats, err := file.Reachability()
	if err != nil || stats.PinnedSnapshots != 2 {
		t.Fatalf("pins with snapshot+iterator=%d err=%v", stats.PinnedSnapshots, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	stats, err = file.Reachability()
	if err != nil || stats.PinnedSnapshots != 1 {
		t.Fatalf("iterator pin after snapshot close=%d err=%v", stats.PinnedSnapshots, err)
	}

	updated := iteratorDocumentID(52)
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: [16]byte{2},
		Mutations:     []DocumentMutation{{Collection: "items", DocumentID: updated, Operation: DocumentUpdate, Document: []byte("new")}},
	}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 7; index++ {
		if !iterator.Next() {
			t.Fatalf("document %d missing: %v", index, iterator.Err())
		}
		record := iterator.Record()
		if record.DocumentID != iteratorDocumentID(50+index) {
			t.Fatalf("document %d id=%x", index, record.DocumentID)
		}
		if index == 2 && string(record.Document) != "old-052" {
			t.Fatalf("iterator observed future update: %q", record.Document)
		}
		if index == 3 && len(record.Document) != len(bytes.Repeat([]byte("overflow"), 2_000)) {
			t.Fatalf("overflow document length=%d", len(record.Document))
		}
	}
	if iterator.Next() || iterator.Err() != nil {
		t.Fatalf("document iterator exceeded limit err=%v", iterator.Err())
	}
	stats, err = file.Reachability()
	if err != nil || stats.PinnedSnapshots != 0 {
		t.Fatalf("pin after iterator exhaustion=%d err=%v", stats.PinnedSnapshots, err)
	}
}

func TestInsertionOrderIteratorOwnsPinAndPreservesDeleteReinsertOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "order-iterator.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	first, second, third := [16]byte{15: 9}, [16]byte{15: 1}, [16]byte{15: 7}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("first-old")},
		{Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: []byte("second-old")},
		{Collection: "items", DocumentID: third, Operation: DocumentInsert, Document: []byte("third-old")},
	}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	iterator, err := snapshot.OpenInsertionOrderIterator("items", nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := file.StorageStats(); stats.ActiveReaders != 1 {
		t.Fatalf("ordered iterator readers=%d", stats.ActiveReaders)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{2}, Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: second, Operation: DocumentUpdate, Document: []byte("second-new")},
		{Collection: "items", DocumentID: first, Operation: DocumentDelete},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{3}, Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("first-new")},
	}}); err != nil {
		t.Fatal(err)
	}
	for index, expected := range []struct {
		id       [16]byte
		position uint64
		document string
	}{{first, 1, "first-old"}, {second, 2, "second-old"}, {third, 3, "third-old"}} {
		if !iterator.Next() {
			t.Fatalf("old ordered record %d missing: %v", index, iterator.Err())
		}
		record := iterator.Record()
		if record.DocumentID != expected.id || record.InsertionPosition != expected.position || string(record.Document) != expected.document {
			t.Fatalf("old ordered record %d=%+v", index, record)
		}
	}
	if iterator.Next() || iterator.Err() != nil {
		t.Fatalf("old ordered iterator tail err=%v", iterator.Err())
	}
	if stats := file.StorageStats(); stats.ActiveReaders != 0 {
		t.Fatalf("ordered iterator leaked readers=%d", stats.ActiveReaders)
	}

	current, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	start, end := uint64(2), uint64(4)
	bounded, err := current.OpenInsertionOrderIterator("items", &start, &end, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer bounded.Close()
	for index, expected := range []struct {
		id       [16]byte
		position uint64
		document string
	}{{second, 2, "second-new"}, {third, 3, "third-old"}} {
		if !bounded.Next() {
			t.Fatalf("bounded ordered record %d missing: %v", index, bounded.Err())
		}
		record := bounded.Record()
		if record.DocumentID != expected.id || record.InsertionPosition != expected.position || string(record.Document) != expected.document {
			t.Fatalf("bounded ordered record %d=%+v", index, record)
		}
	}
	if bounded.Next() || bounded.Err() != nil {
		t.Fatalf("bounded ordered iterator tail err=%v", bounded.Err())
	}

	reinserted, err := current.OpenInsertionOrderIterator("items", nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reinserted.Close()
	positions := []uint64{}
	ids := [][16]byte{}
	for reinserted.Next() {
		record := reinserted.Record()
		positions = append(positions, record.InsertionPosition)
		ids = append(ids, record.DocumentID)
	}
	if reinserted.Err() != nil || !reflect.DeepEqual(positions, []uint64{2, 3, 4}) || !reflect.DeepEqual(ids, [][16]byte{second, third, first}) {
		t.Fatalf("current insertion order positions=%v ids=%v err=%v", positions, ids, reinserted.Err())
	}
}

func iteratorDocumentID(index int) [16]byte {
	index++
	return [16]byte{12: byte(index >> 24), 13: byte(index >> 16), 14: byte(index >> 8), 15: byte(index)}
}
