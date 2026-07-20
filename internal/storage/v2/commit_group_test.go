package v2

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestDocumentTransactionGroupPublishesOneGenerationWithOrderedHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "group.meld2")
	file, initial, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	firstID, secondID := [16]byte{1}, [16]byte{2}
	sequences, err := file.ApplyDocumentTransactionGroup([]DocumentTransaction{
		{TransactionID: [16]byte{11}, CommittedAt: time.Unix(100, 0), Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: firstID, Operation: DocumentInsert, Document: []byte("first"),
		}}},
		{TransactionID: [16]byte{12}, CommittedAt: time.Unix(101, 0), Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: secondID, Operation: DocumentInsert, Document: []byte("second"),
		}}},
	})
	if err != nil || len(sequences) != 2 || sequences[0] != 1 || sequences[1] != 2 {
		t.Fatalf("sequences=%v err=%v", sequences, err)
	}
	meta := file.Meta()
	root, err := file.DatabaseRoot()
	if err != nil || meta.Generation != initial.Generation+1 || meta.CommitSequence != 2 || root.CommitSequence != 2 || root.DocumentCount != 2 {
		t.Fatalf("initial=%+v meta=%+v root=%+v err=%v", initial, meta, root, err)
	}
	first, err := file.ReadCommit(root.CommitLogRoot, 1)
	if err != nil || first.Sequence != 1 || first.CatalogRoot == 0 || len(first.Changes) != 2 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := file.ReadCommit(root.CommitLogRoot, 2)
	if err != nil || second.Sequence != 2 || second.CatalogRoot != root.CatalogRoot || len(second.Changes) != 1 {
		t.Fatalf("second=%+v root=%+v err=%v", second, root, err)
	}
	// Historical Snapshot 1 must reconstruct from the first group's retained
	// CatalogRoot even though there was never a physical Meta page for it.
	snapshot, stream, err := file.OpenSnapshotAndStreamAt(1)
	if err != nil {
		t.Fatal(err)
	}
	iterator, err := snapshot.OpenCollectionIterator("items", nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !iterator.Next() || !bytes.Equal(iterator.Record().Document, []byte("first")) || iterator.Next() {
		t.Fatalf("historical records first=%+v err=%v", iterator.Record(), iterator.Err())
	}
	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil || batch.Sequence != 2 {
		t.Fatalf("historical stream batch=%+v err=%v", batch, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, reopenedMeta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopenedMeta.Generation != meta.Generation || reopenedMeta.CommitSequence != 2 {
		t.Fatalf("reopened meta=%+v", reopenedMeta)
	}
	for id, want := range map[[16]byte][]byte{firstID: []byte("first"), secondID: []byte("second")} {
		got, exists, err := reopened.GetDocument("items", id)
		if err != nil || !exists || !bytes.Equal(got, want) {
			t.Fatalf("id=%x got=%q exists=%t err=%v", id, got, exists, err)
		}
	}
	if _, err := reopened.Reachability(); err != nil {
		t.Fatal(err)
	}
}

func TestDocumentTransactionGroupRejectsWholeGroupBeforePublication(t *testing.T) {
	file, initial, err := Open(filepath.Join(t.TempDir(), "group-reject.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{9}
	_, err = file.ApplyDocumentTransactionGroup([]DocumentTransaction{
		{TransactionID: [16]byte{21}, Mutations: []DocumentMutation{{Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("first")}}},
		{TransactionID: [16]byte{22}, Mutations: []DocumentMutation{{Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("duplicate")}}},
	})
	if err == nil {
		t.Fatal("group unexpectedly succeeded")
	}
	meta := file.Meta()
	root, rootErr := file.DatabaseRoot()
	if rootErr != nil || meta != initial || root.CommitSequence != 0 || root.DocumentCount != 0 {
		t.Fatalf("initial=%+v meta=%+v root=%+v rootErr=%v", initial, meta, root, rootErr)
	}
	if _, exists, err := file.GetDocument("items", id); err != nil || exists {
		t.Fatalf("group rejection document exists=%t err=%v", exists, err)
	}
}

func TestENOSPCDocumentTransactionGroupNeverPublishesPrefix(t *testing.T) {
	for _, point := range commitFaultPoints() {
		t.Run(faultPointName(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "group-enospc.meld2")
			file, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			appendStart := file.nextPage
			installENOSPCFault(t, file, point, func() {
				if err := file.file.Truncate(int64(appendStart)*PageSize + PageSize/2); err != nil {
					t.Fatal(err)
				}
			})
			_, err = file.ApplyDocumentTransactionGroup([]DocumentTransaction{
				{TransactionID: [16]byte{41}, Mutations: []DocumentMutation{{Collection: "items", DocumentID: [16]byte{1}, Operation: DocumentInsert, Document: []byte("first")}}},
				{TransactionID: [16]byte{42}, Mutations: []DocumentMutation{{Collection: "items", DocumentID: [16]byte{2}, Operation: DocumentInsert, Document: []byte("second")}}},
			})
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("group error=%v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, meta, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			expectedSequence := uint64(0)
			if point == faultAfterMetaSync {
				expectedSequence = 2
			}
			if meta.CommitSequence != expectedSequence {
				t.Fatalf("sequence=%d want=%d", meta.CommitSequence, expectedSequence)
			}
			for id, want := range map[[16]byte]string{{1}: "first", {2}: "second"} {
				value, exists, err := reopened.GetDocument("items", id)
				if expectedSequence == 0 {
					if err != nil || exists {
						t.Fatalf("old id=%x exists=%t err=%v", id, exists, err)
					}
					continue
				}
				if err != nil || !exists || string(value) != want {
					t.Fatalf("new id=%x value=%q exists=%t err=%v", id, value, exists, err)
				}
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
