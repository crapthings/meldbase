package meldbase

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestDurableCollectionChangesPersistAcrossReopenAndRequireAck(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable-collection-changes.meld2")
	db, err := OpenWithOptions(path, OpenOptions{CommitRetention: V2CommitRetentionPolicy{MaxCommits: 2}})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := db.CreateDurableCollectionChanges(context.Background(), "archive", "items", 0, 2)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if sequence := db.Stats().CommitSequence; sequence != 1 {
		subscription.Close()
		_ = db.Close()
		t.Fatalf("initial durable checkpoint did not synchronize DB token: %d", sequence)
	}
	if duplicate, err := db.CreateDurableCollectionChanges(context.Background(), "archive", "items", 0, 1); duplicate != nil || !errors.Is(err, ErrDurableConsumerExists) {
		subscription.Close()
		_ = db.Close()
		t.Fatalf("duplicate checkpoint subscription=%v err=%v", duplicate, err)
	}
	id, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)})
	if err != nil {
		subscription.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	first := receiveDurableChangeBatch(t, subscription)
	if first.Token != 2 || len(first.Changes) != 1 || first.Changes[0].Operation != InsertOperation || first.Changes[0].DocumentID != id {
		subscription.Close()
		_ = db.Close()
		t.Fatalf("first batch=%+v", first)
	}
	if err := subscription.Ack(first.Token); err != nil {
		subscription.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	if sequence := db.Stats().CommitSequence; sequence != 2 {
		subscription.Close()
		_ = db.Close()
		t.Fatalf("ack advanced business token: %d", sequence)
	}
	subscription.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenWithOptions(path, OpenOptions{CommitRetention: V2CommitRetentionPolicy{MaxCommits: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	subscription, err = db.OpenDurableCollectionChanges(context.Background(), "archive", "items", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if _, err := db.Collection("items").UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": 2}}); err != nil {
		t.Fatal(err)
	}
	second := receiveDurableChangeBatch(t, subscription)
	if second.Token != 3 || len(second.Changes) != 1 || second.Changes[0].Operation != UpdateOperation ||
		len(second.Changes[0].ChangedPaths) != 1 || second.Changes[0].ChangedPaths[0] != "value" {
		t.Fatalf("second batch=%+v", second)
	}
	if err := subscription.Ack(second.Token); err != nil {
		t.Fatal(err)
	}
	subscription.Close()
	if err := db.DeleteDurableCollectionChanges(context.Background(), "archive", "items"); err != nil {
		t.Fatal(err)
	}
	if restored, err := db.OpenDurableCollectionChanges(context.Background(), "archive", "items", 1); restored != nil || !errors.Is(err, ErrDurableConsumerNotFound) {
		t.Fatalf("deleted checkpoint subscription=%v err=%v", restored, err)
	}
}

func TestDurableCollectionChangesRejectUnsupportedAndUnsafeArguments(t *testing.T) {
	memory := New()
	defer memory.Close()
	if subscription, err := memory.CreateDurableCollectionChanges(context.Background(), "archive", "items", 0, 1); subscription != nil || !errors.Is(err, ErrDurableConsumerUnsupported) {
		t.Fatalf("memory durable consumer subscription=%v err=%v", subscription, err)
	}
	db, err := Open(filepath.Join(t.TempDir(), "durable-invalid.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range []string{"", "has space", string(make([]byte, 129))} {
		if subscription, err := db.CreateDurableCollectionChanges(context.Background(), name, "items", 0, 1); subscription != nil || !errors.Is(err, ErrInvalidDocument) {
			t.Fatalf("invalid name %q subscription=%v err=%v", name, subscription, err)
		}
	}
	if subscription, err := db.CreateDurableCollectionChanges(context.Background(), "archive", "items", 0, 0); subscription != nil || !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("invalid buffer subscription=%v err=%v", subscription, err)
	}
}

func receiveDurableChangeBatch(t *testing.T, subscription *DurableChangeSubscription) DurableChangeBatch {
	t.Helper()
	select {
	case batch, ok := <-subscription.Batches:
		if !ok {
			t.Fatal("durable batch channel closed")
		}
		return batch
	case err, ok := <-subscription.Errors:
		if ok {
			t.Fatalf("durable change error=%v", err)
		}
		t.Fatal("durable error channel closed")
	case <-time.After(3 * time.Second):
		t.Fatal("durable change timeout")
	}
	return DurableChangeBatch{}
}
