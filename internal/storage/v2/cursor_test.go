package v2

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestCommitCursorReplayRetentionAndPinnedOldRoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{1}
	for sequence := 1; sequence <= 5; sequence++ {
		operation := DocumentUpdate
		if sequence == 1 {
			operation = DocumentInsert
		}
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
			TransactionID: randomTransactionID(t), CommittedAt: time.Unix(int64(sequence), 0),
			Mutations: []DocumentMutation{{Collection: "items", DocumentID: id, Operation: operation, Document: []byte{byte(sequence)}}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	oldCursor, err := file.OpenCommitCursor(1)
	if err != nil || oldCursor.Through() != 5 {
		t.Fatalf("old cursor through=%d err=%v", oldCursor.Through(), err)
	}
	defer oldCursor.Close()
	if sequence, err := file.RetainCommitsFrom(4, randomTransactionID(t), time.Unix(6, 0)); err != nil || sequence != 6 {
		t.Fatalf("retention sequence=%d err=%v", sequence, err)
	}
	// The cursor opened before retention remains attached to its immutable root.
	for want := uint64(2); want <= 5; want++ {
		batch, ok, err := oldCursor.Next()
		if err != nil || !ok || batch.Sequence != want {
			t.Fatalf("old cursor want=%d got=%d ok=%t err=%v", want, batch.Sequence, ok, err)
		}
	}
	if _, ok, err := oldCursor.Next(); err != nil || ok {
		t.Fatalf("old cursor end ok=%t err=%v", ok, err)
	}

	if _, err := file.OpenCommitCursor(1); !errors.Is(err, ErrHistoryLost) {
		t.Fatalf("old resume error = %v", err)
	}
	current, err := file.OpenCommitCursor(3)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	for want := uint64(4); want <= 6; want++ {
		batch, ok, err := current.Next()
		if err != nil || !ok || batch.Sequence != want {
			t.Fatalf("current cursor want=%d got=%d ok=%t err=%v", want, batch.Sequence, ok, err)
		}
	}
	retentionBatch, err := file.ReadCommit(fileRoot(t, file).CommitLogRoot, 6)
	if err != nil || len(retentionBatch.Changes) != 1 || string(retentionBatch.Changes[0].ChangedPaths[0]) != "_system.retention" {
		t.Fatalf("retention batch=%+v err=%v", retentionBatch, err)
	}
}

func fileRoot(t *testing.T, file *File) DatabaseRoot {
	t.Helper()
	root, err := file.DatabaseRoot()
	if err != nil {
		t.Fatal(err)
	}
	return root
}
