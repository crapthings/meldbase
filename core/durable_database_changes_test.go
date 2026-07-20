package meldbase

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestDurableDatabaseChangesCarriesCatalogAndDocumentsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable-database-changes.meld2")
	db, err := OpenV2WithOptions(path, V2Options{CommitRetention: V2CommitRetentionPolicy{MaxCommits: 2}})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := db.CreateDurableDatabaseChanges(context.Background(), "follower", 0, 2)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if sequence := db.Stats().CommitSequence; sequence != 1 {
		subscription.Close()
		_ = db.Close()
		t.Fatalf("initial durable checkpoint did not synchronize DB token: %d", sequence)
	}
	if duplicate, err := db.CreateDurableDatabaseChanges(context.Background(), "follower", 0, 1); duplicate != nil || !errors.Is(err, ErrDurableConsumerExists) {
		subscription.Close()
		_ = db.Close()
		t.Fatalf("duplicate database checkpoint subscription=%v err=%v", duplicate, err)
	}
	id, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)})
	if err != nil {
		subscription.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	first := receiveDurableDatabaseBatch(t, subscription)
	if first.Token != 2 || first.TransactionID == [16]byte{} || first.CommittedAt.IsZero() || len(first.Changes) != 2 ||
		first.Changes[0].Collection != "items" || first.Changes[0].Operation != CreateCollectionOperation ||
		first.Changes[1].Collection != "items" || first.Changes[1].Operation != InsertOperation || first.Changes[1].DocumentID != id {
		subscription.Close()
		_ = db.Close()
		t.Fatalf("first database batch=%+v", first)
	}
	if err := subscription.Ack(first.Token); err != nil {
		subscription.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Collection("items").CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		subscription.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	second := receiveDurableDatabaseBatch(t, subscription)
	if second.Token != 3 || len(second.Changes) != 1 || second.Changes[0].Collection != "items" || second.Changes[0].Operation != CreateIndexOperation ||
		second.Changes[0].Index == nil || second.Changes[0].Index.Name != "by_value" || !second.Changes[0].Index.Unique ||
		len(second.Changes[0].Index.Fields) != 1 || second.Changes[0].Index.Fields[0] != (IndexField{Field: "value", Order: 1}) {
		subscription.Close()
		_ = db.Close()
		t.Fatalf("second database batch=%+v", second)
	}
	if err := subscription.Ack(second.Token); err != nil {
		subscription.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	subscription.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenV2WithOptions(path, V2Options{CommitRetention: V2CommitRetentionPolicy{MaxCommits: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	subscription, err = db.OpenDurableDatabaseChanges(context.Background(), "follower", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if _, err := db.Collection("items").UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": 2}}); err != nil {
		t.Fatal(err)
	}
	third := receiveDurableDatabaseBatch(t, subscription)
	if third.Token != 4 || len(third.Changes) != 1 || third.Changes[0].Operation != UpdateOperation || third.Changes[0].Collection != "items" ||
		len(third.Changes[0].ChangedPaths) != 1 || third.Changes[0].ChangedPaths[0] != "value" {
		t.Fatalf("third database batch=%+v", third)
	}
	if err := subscription.Ack(third.Token); err != nil {
		t.Fatal(err)
	}
	subscription.Close()
	if err := db.DeleteDurableDatabaseChanges(context.Background(), "follower"); err != nil {
		t.Fatal(err)
	}
	if reopened, err := db.OpenDurableDatabaseChanges(context.Background(), "follower", 1); reopened != nil || !errors.Is(err, ErrDurableConsumerNotFound) {
		t.Fatalf("deleted database checkpoint subscription=%v err=%v", reopened, err)
	}
}

func TestDurableDatabaseChangesRejectUnsupportedAndInvalidArguments(t *testing.T) {
	memory := New()
	defer memory.Close()
	if subscription, err := memory.CreateDurableDatabaseChanges(context.Background(), "follower", 0, 1); subscription != nil || !errors.Is(err, ErrDurableConsumerUnsupported) {
		t.Fatalf("memory durable database subscription=%v err=%v", subscription, err)
	}
	db, err := OpenV2(filepath.Join(t.TempDir(), "invalid-database-feed.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range []string{"", "has space", string(make([]byte, 129))} {
		if subscription, err := db.CreateDurableDatabaseChanges(context.Background(), name, 0, 1); subscription != nil || !errors.Is(err, ErrInvalidDocument) {
			t.Fatalf("invalid database name %q subscription=%v err=%v", name, subscription, err)
		}
	}
	if subscription, err := db.CreateDurableDatabaseChanges(context.Background(), "follower", 0, 0); subscription != nil || !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("invalid database buffer subscription=%v err=%v", subscription, err)
	}
}

func receiveDurableDatabaseBatch(t *testing.T, subscription *DurableDatabaseChangeSubscription) DurableDatabaseChangeBatch {
	t.Helper()
	select {
	case batch, ok := <-subscription.Batches:
		if !ok {
			t.Fatal("durable database batch channel closed")
		}
		return batch
	case err, ok := <-subscription.Errors:
		if ok {
			t.Fatalf("durable database change error=%v", err)
		}
		t.Fatal("durable database error channel closed")
	case <-time.After(3 * time.Second):
		t.Fatal("durable database change timeout")
	}
	return DurableDatabaseChangeBatch{}
}
