package v2

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestSemanticAuditTreeViewCacheIsBoundedAndTypeSafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "semantic-view-cache.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: [16]byte{1}, Operation: DocumentInsert, Document: []byte("one"),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	walker := &reachabilityWalker{
		file: file, ctx: context.Background(), treeViews: make(map[uint64]semanticAuditTreeView, maxSemanticAuditTreeViews),
	}
	file.mu.RLock()
	for range 2 {
		if value, exists, err := walker.treeGetUnlocked(file.root.CatalogRoot, TreeCatalog, []byte("items")); err != nil || !exists || len(value) == 0 {
			file.mu.RUnlock()
			t.Fatalf("catalog lookup exists=%t bytes=%d err=%v", exists, len(value), err)
		}
	}
	if _, _, err := walker.treeGetUnlocked(file.root.CatalogRoot, TreePrimary, []byte("items")); !errors.Is(err, ErrCorrupt) {
		file.mu.RUnlock()
		t.Fatalf("cross-kind cached lookup error=%v", err)
	}
	file.mu.RUnlock()
	if walker.treeViewHits == 0 || len(walker.treeViews) == 0 || len(walker.treeViews) > maxSemanticAuditTreeViews {
		t.Fatalf("views=%d hits=%d", len(walker.treeViews), walker.treeViewHits)
	}
}

func TestReachabilityProtectsTwoMetasAndSnapshotPins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reachability.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("one")}},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, stream, err := file.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"two", "three", "four"} {
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
			TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte(value)}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	withPin, err := file.Reachability()
	if err != nil || withPin.PinnedSnapshots != 1 || withPin.ReachablePages == 0 {
		t.Fatalf("with pin=%+v err=%v", withPin, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	withoutPin, err := file.Reachability()
	if err != nil || withoutPin.PinnedSnapshots != 0 || withoutPin.ReachablePages >= withPin.ReachablePages || withoutPin.ReclaimablePages <= withPin.ReclaimablePages {
		t.Fatalf("with=%+v without=%+v err=%v", withPin, withoutPin, err)
	}
	if withoutPin.PhysicalPages != withPin.PhysicalPages || withoutPin.ReachablePages+withoutPin.ReclaimablePages != withoutPin.PhysicalPages-2 {
		t.Fatalf("invalid page accounting: %+v", withoutPin)
	}
}
