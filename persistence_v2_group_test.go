package meldbase

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

func TestCommitV2ChangeBatchesLockedPublishesOrderedLogicalBatches(t *testing.T) {
	db, err := OpenV2(filepath.Join(t.TempDir(), "db-group.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, ok := db.durability.(*v2DurableStore)
	if !ok || store == nil {
		t.Fatal("missing V2 store")
	}
	initialGeneration := store.file.Meta().Generation
	watchContext, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	users, usersDone, err := db.WatchChanges(watchContext, "users", 2)
	if err != nil {
		t.Fatal(err)
	}
	orders, ordersDone, err := db.WatchChanges(watchContext, "orders", 2)
	if err != nil {
		t.Fatal(err)
	}
	userID, orderID := DocumentID{15: 1}, DocumentID{15: 2}
	user := Document{"_id": ID(userID), "name": String("Ada")}
	order := Document{"_id": ID(orderID), "state": String("created")}
	batches := []ChangeBatch{
		{Token: 1, Changes: []Change{{Collection: "users", Operation: InsertOperation, DocumentID: userID, After: &user}}},
		{Token: 2, Changes: []Change{{Collection: "orders", Operation: InsertOperation, DocumentID: orderID, After: &order}}},
	}
	db.mu.Lock()
	err = db.commitV2ChangeBatchesLocked(context.Background(), store, batches)
	db.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-users:
		if batch.Token != 1 || len(batch.Changes) != 1 || batch.Changes[0].Collection != "users" {
			t.Fatalf("users batch=%+v", batch)
		}
	case err := <-usersDone:
		t.Fatalf("users watcher ended: %v", err)
	case <-time.After(time.Second):
		t.Fatal("users watcher timed out")
	}
	select {
	case batch := <-orders:
		if batch.Token != 2 || len(batch.Changes) != 1 || batch.Changes[0].Collection != "orders" {
			t.Fatalf("orders batch=%+v", batch)
		}
	case err := <-ordersDone:
		t.Fatalf("orders watcher ended: %v", err)
	case <-time.After(time.Second):
		t.Fatal("orders watcher timed out")
	}
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Collections != 2 || stats.Documents != 2 ||
		stats.Storage.CommittedTransactions != 2 || store.file.Meta().Generation != initialGeneration+1 {
		t.Fatalf("stats=%+v generation=%d", stats, store.file.Meta().Generation)
	}
	if got, err := db.Collection("users").FindOne(context.Background(), Filter{"_id": userID}); err != nil || !got.Equal(user) {
		t.Fatalf("user=%+v err=%v", got, err)
	}
	if got, err := db.Collection("orders").FindOne(context.Background(), Filter{"_id": orderID}); err != nil || !got.Equal(order) {
		t.Fatalf("order=%+v err=%v", got, err)
	}
}

func TestCommitV2ChangeBatchesLockedHonorsPerMemberPointPreconditions(t *testing.T) {
	db, err := OpenV2(filepath.Join(t.TempDir(), "db-group-preconditions.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	firstID, secondID := DocumentID{14: 1}, DocumentID{14: 2}
	first := Document{"_id": ID(firstID), "n": Int(1)}
	second := Document{"_id": ID(secondID), "n": Int(2)}
	if _, err := items.InsertMany(context.Background(), []Document{first, second}); err != nil {
		t.Fatal(err)
	}
	store := db.durability.(*v2DurableStore)
	initialGeneration := store.file.Meta().Generation
	firstNext := Document{"_id": ID(firstID), "n": Int(11)}
	secondNext := Document{"_id": ID(secondID), "n": Int(22)}
	firstBefore, firstAfter := first.Clone(), firstNext.Clone()
	secondBefore, secondAfter := second.Clone(), secondNext.Clone()
	batches := []ChangeBatch{
		{Token: 2, Changes: []Change{{Collection: "items", Operation: UpdateOperation, DocumentID: firstID, Before: &firstBefore, After: &firstAfter}}},
		{Token: 3, Changes: []Change{{Collection: "items", Operation: UpdateOperation, DocumentID: secondID, Before: &secondBefore, After: &secondAfter}}},
	}
	preconditions := [][]storagev2.DocumentPrecondition{
		{testV2DocumentPrecondition(t, "items", firstID, first)},
		{testV2DocumentPrecondition(t, "items", secondID, second)},
	}
	db.mu.Lock()
	err = db.commitV2ChangeBatchesWithPreconditionsLocked(context.Background(), store, batches, preconditions, nil)
	db.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if meta := store.file.Meta(); meta.CommitSequence != 3 || meta.Generation != initialGeneration+1 {
		t.Fatalf("successful precondition group meta=%+v initial generation=%d", meta, initialGeneration)
	}

	// Both later members read the same now-stale value. Storage must reject the
	// entire candidate group rather than publishing the first update as a prefix.
	staleFirst := Document{"_id": ID(firstID), "n": Int(111)}
	staleSecond := Document{"_id": ID(firstID), "n": Int(222)}
	firstCurrent, staleFirstAfter := firstNext.Clone(), staleFirst.Clone()
	staleSecondBefore, staleSecondAfter := firstNext.Clone(), staleSecond.Clone()
	staleBatches := []ChangeBatch{
		{Token: 4, Changes: []Change{{Collection: "items", Operation: UpdateOperation, DocumentID: firstID, Before: &firstCurrent, After: &staleFirstAfter}}},
		{Token: 5, Changes: []Change{{Collection: "items", Operation: UpdateOperation, DocumentID: firstID, Before: &staleSecondBefore, After: &staleSecondAfter}}},
	}
	staleRead := testV2DocumentPrecondition(t, "items", firstID, firstNext)
	db.mu.Lock()
	err = db.commitV2ChangeBatchesWithPreconditionsLocked(context.Background(), store, staleBatches, [][]storagev2.DocumentPrecondition{{staleRead}, {staleRead}}, nil)
	db.mu.Unlock()
	if !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("stale group error=%v", err)
	}
	if meta := store.file.Meta(); meta.CommitSequence != 3 || meta.Generation != initialGeneration+1 {
		t.Fatalf("stale group published a prefix meta=%+v initial generation=%d", meta, initialGeneration)
	}
	if got, err := items.FindOne(context.Background(), Filter{"_id": firstID}); err != nil || !got.Equal(firstNext) {
		t.Fatalf("first after stale group=%+v err=%v", got, err)
	}
	if stats := db.Stats(); stats.WritesDisabled {
		t.Fatalf("stale point conflict disabled writes: %+v", stats)
	}
}

func testV2DocumentPrecondition(t *testing.T, collection string, id DocumentID, document Document) storagev2.DocumentPrecondition {
	t.Helper()
	encoded, err := encodeStoredDocument(document)
	if err != nil {
		t.Fatal(err)
	}
	return storagev2.DocumentPrecondition{
		Collection: collection, DocumentID: [16]byte(id), ExpectedExists: true, ExpectedHash: sha256.Sum256(encoded),
	}
}
