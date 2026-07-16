package v2

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestCommitLogIsAtomicOrderedAndSupportsOverflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-log.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	committedAt := time.Unix(123, 456)
	large := bytes.Repeat([]byte("large-change-image"), 3000)
	var firstRoot uint64
	firstID := randomTransactionID(t)
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		batch := CommitBatch{
			Sequence: tx.Sequence(), TransactionID: firstID, CommittedAt: committedAt,
			Changes: []CommitChange{
				{CollectionID: 7, DocumentID: [16]byte{1}, Operation: CommitInsert, ChangedPaths: []string{"title", "owner.id"}, After: large},
				{CollectionID: 7, DocumentID: [16]byte{2}, Operation: CommitDelete, Before: []byte("old")},
			},
		}
		var err error
		firstRoot, err = tx.AppendCommit(0, batch)
		return DatabaseRoot{CommitSequence: tx.Sequence(), CommitLogRoot: firstRoot, OldestRetainedSequence: 1}, err
	}); err != nil {
		t.Fatal(err)
	}
	first, err := file.ReadCommit(firstRoot, 1)
	if err != nil || first.Sequence != 1 || first.TransactionID != firstID || !first.CommittedAt.Equal(committedAt) || len(first.Changes) != 2 || !bytes.Equal(first.Changes[0].After, large) {
		t.Fatalf("first commit=%+v err=%v", first, err)
	}

	var secondRoot uint64
	secondID := randomTransactionID(t)
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		var err error
		secondRoot, err = tx.AppendCommit(firstRoot, CommitBatch{
			Sequence: tx.Sequence(), TransactionID: secondID, CommittedAt: committedAt.Add(time.Second),
			Changes: []CommitChange{{CollectionID: 9, DocumentID: [16]byte{3}, Operation: CommitUpdate, ChangedPaths: []string{"done"}, Before: []byte("false"), After: []byte("true")}},
		})
		return DatabaseRoot{CommitSequence: tx.Sequence(), CommitLogRoot: secondRoot, OldestRetainedSequence: 1}, err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadCommit(secondRoot, 1); err != nil {
		t.Fatalf("first commit missing from second root: %v", err)
	}
	second, err := file.ReadCommit(secondRoot, 2)
	if err != nil || second.TransactionID != secondID || string(second.Changes[0].After) != "true" {
		t.Fatalf("second commit=%+v err=%v", second, err)
	}
	if _, err := file.ReadCommit(firstRoot, 2); err == nil {
		t.Fatal("old root observed future commit")
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	root, err := reopened.DatabaseRoot()
	if err != nil || root.CommitLogRoot != secondRoot || root.OldestRetainedSequence != 1 {
		t.Fatalf("root=%+v err=%v", root, err)
	}
	recovered, err := reopened.ReadCommit(root.CommitLogRoot, 1)
	if err != nil || !bytes.Equal(recovered.Changes[0].After, large) {
		t.Fatalf("recovered commit changes=%d err=%v", len(recovered.Changes), err)
	}
}

func TestCommitChangePathsHaveCanonicalEncoding(t *testing.T) {
	change := CommitChange{
		CollectionID: 1, DocumentID: [16]byte{1}, Operation: CommitUpdate,
		ChangedPaths: []string{"title", "owner.id", "title", "done"}, After: []byte("value"),
	}
	encoded, err := encodeCommitChange(change)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCommitChange(encoded)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"done", "owner.id", "title"}
	if len(decoded.ChangedPaths) != len(want) {
		t.Fatalf("paths = %v", decoded.ChangedPaths)
	}
	for index := range want {
		if decoded.ChangedPaths[index] != want[index] {
			t.Fatalf("paths = %v", decoded.ChangedPaths)
		}
	}
}

func TestCatalogChangeCarriesNameWithoutInflatingDocumentChanges(t *testing.T) {
	catalog := CommitChange{CollectionID: 7, CollectionName: "tasks", Operation: CommitCatalog, ChangedPaths: []string{"_catalog"}, After: []byte("meta")}
	encodedCatalog, err := encodeCommitChange(catalog)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCommitChange(encodedCatalog)
	if err != nil || decoded.CollectionName != "tasks" || decoded.CollectionID != 7 {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	document := CommitChange{CollectionID: 7, DocumentID: [16]byte{1}, Operation: CommitInsert, After: []byte("doc")}
	encodedDocument, err := encodeCommitChange(document)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint16(encodedDocument[48:50]) != 0 {
		t.Fatalf("document event unexpectedly carries a name: %x", encodedDocument[48:52])
	}
	document.CollectionName = "tasks"
	if _, err := encodeCommitChange(document); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("named document change error=%v", err)
	}
}

func TestCommitRejectsNonCatalogHistoricalRootWithoutPublishing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-catalog-root.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	err = file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		primary, err := tx.OpenTree(0, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := primary.Put([]byte("document"), []byte("value")); err != nil {
			return DatabaseRoot{}, err
		}
		primaryRoot, err := primary.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		commitRoot, err := tx.AppendCommit(0, CommitBatch{
			Sequence: tx.Sequence(), TransactionID: randomTransactionID(t), CatalogRoot: primaryRoot,
			Changes: []CommitChange{{CollectionID: 1, DocumentID: [16]byte{1}, Operation: CommitInsert, After: []byte("value")}},
		})
		return DatabaseRoot{CommitSequence: tx.Sequence(), CommitLogRoot: commitRoot, OldestRetainedSequence: 1}, err
	})
	if !errors.Is(err, ErrCorrupt) || file.Meta().CommitSequence != 0 {
		t.Fatalf("invalid historical root err=%v sequence=%d", err, file.Meta().CommitSequence)
	}
}

func randomTransactionID(t *testing.T) [16]byte {
	t.Helper()
	var result [16]byte
	if _, err := rand.Read(result[:]); err != nil {
		t.Fatal(err)
	}
	return result
}
