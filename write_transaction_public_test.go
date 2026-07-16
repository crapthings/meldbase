package meldbase

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crapthings/meldbase/internal/systemrecord"
)

func TestRunWriteTransactionCommitsMultiCollectionUniqueSwapAndOneReactiveBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "public-write-transaction.meld2")
	db, err := OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	first, second := DocumentID{15: 1}, DocumentID{15: 2}
	if _, err := db.Collection("users").InsertMany(context.Background(), []Document{
		{"_id": ID(first), "handle": String("alpha"), "visits": Int(1)},
		{"_id": ID(second), "handle": String("beta"), "visits": Int(2)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Collection("users").CreateIndex(context.Background(), "by_handle", []IndexField{{Field: "handle", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	doomed := DocumentID{15: 3}
	if _, err := db.Collection("orders").InsertOne(context.Background(), Document{"_id": ID(doomed), "state": String("old")}); err != nil {
		t.Fatal(err)
	}
	watchContext, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	userEvents, _, err := db.WatchChanges(watchContext, "users", 2)
	if err != nil {
		t.Fatal(err)
	}
	orderEvents, _, err := db.WatchChanges(watchContext, "orders", 2)
	if err != nil {
		t.Fatal(err)
	}
	firstUpdate, err := CompileUpdate(Update{"$set": map[string]any{"handle": "beta"}, "$inc": map[string]any{"visits": 1}})
	if err != nil {
		t.Fatal(err)
	}
	secondUpdate, err := CompileUpdate(Update{"$set": map[string]any{"handle": "alpha"}})
	if err != nil {
		t.Fatal(err)
	}
	created := DocumentID{15: 4}
	if err := db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
		if err := tx.UpdateOne("users", first, firstUpdate); err != nil {
			return err
		}
		if err := tx.UpdateOne("users", second, secondUpdate); err != nil {
			return err
		}
		if err := tx.DeleteOne("orders", doomed); err != nil {
			return err
		}
		inserted, err := tx.InsertOne("orders", Document{"_id": ID(created), "state": String("new")})
		if err != nil {
			return err
		}
		if inserted != created {
			return ErrCorrupt
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	users := receiveWriteTransactionBatch(t, userEvents)
	orders := receiveWriteTransactionBatch(t, orderEvents)
	if users.Token != orders.Token || users.Token != 4 || len(users.Changes) != 2 || len(orders.Changes) != 2 {
		t.Fatalf("users=%+v orders=%+v", users, orders)
	}
	if stats := db.Stats(); stats.CommitSequence != 4 || stats.Collections != 2 || stats.Documents != 3 || stats.Indexes != 1 ||
		stats.Transactions.Started != 1 || stats.Transactions.Committed != 1 || stats.Transactions.Active != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	firstDocument, err := db.Collection("users").FindOne(context.Background(), Filter{"_id": first})
	if err != nil || !firstDocument["handle"].Equal(String("beta")) || !firstDocument["visits"].Equal(Int(2)) {
		t.Fatalf("first=%v err=%v", firstDocument, err)
	}
	secondDocument, err := db.Collection("users").FindOne(context.Background(), Filter{"_id": second})
	if err != nil || !secondDocument["handle"].Equal(String("alpha")) {
		t.Fatalf("second=%v err=%v", secondDocument, err)
	}
	if _, err := db.Collection("orders").FindOne(context.Background(), Filter{"_id": doomed}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted order err=%v", err)
	}
	if _, err := db.Collection("orders").FindOne(context.Background(), Filter{"_id": created}); err != nil {
		t.Fatalf("created order err=%v", err)
	}
}

func TestRunWriteTransactionNoopRollbackLifetimeAndResourceLimit(t *testing.T) {
	db, err := OpenV2WithOptions(filepath.Join(t.TempDir(), "bounded-write-transaction.meld2"), V2Options{
		ResourceLimits: ResourceLimits{MaxTransactionChanges: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := DocumentID{15: 1}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(id), "value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	var retained *WriteTransaction
	if err := db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
		retained = tx
		document, err := tx.GetOne("items", id)
		if err != nil {
			return err
		}
		return tx.ReplaceOne("items", id, document)
	}); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats.CommitSequence != 1 {
		t.Fatalf("no-op advanced sequence: %+v", stats)
	}
	if _, err := retained.GetOne("items", id); !errors.Is(err, ErrClosed) {
		t.Fatalf("retained transaction err=%v", err)
	}

	want := errors.New("application rollback")
	if err := db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
		if err := tx.DeleteOne("items", id); err != nil {
			return err
		}
		return want
	}); !errors.Is(err, want) {
		t.Fatalf("rollback err=%v", err)
	}
	if _, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err != nil {
		t.Fatalf("rollback removed document: %v", err)
	}

	if err := db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
		if _, err := tx.InsertOne("items", Document{"_id": ID(DocumentID{15: 2})}); err != nil {
			return err
		}
		_, err := tx.InsertOne("items", Document{"_id": ID(DocumentID{15: 3})})
		return err
	}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("resource limit err=%v", err)
	}
	if stats := db.Stats(); stats.CommitSequence != 1 || stats.Documents != 1 {
		t.Fatalf("rejected transaction published: %+v", stats)
	}
	if err := db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
		return tx.MeldbaseStageSystemMutation(systemrecord.Mutation{
			Key: []byte("private-test"), NewValue: []byte("must-not-publish"), Unconditional: true,
		}, nil)
	}); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("public system mutation err=%v", err)
	}
	if stats := db.Stats(); stats.Transactions.Started != 4 || stats.Transactions.Committed != 0 ||
		stats.Transactions.Noops != 1 || stats.Transactions.Conflicts != 0 || stats.Transactions.Aborted != 3 || stats.Transactions.Active != 0 {
		t.Fatalf("transaction terminal accounting=%+v", stats.Transactions)
	}
}

func TestRunWriteTransactionBoundsReadSetEntriesBeforeCommit(t *testing.T) {
	db, err := OpenV2WithOptions(filepath.Join(t.TempDir(), "bounded-read-set.meld2"), V2Options{
		ResourceLimits: ResourceLimits{MaxTransactionChanges: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	first, second := DocumentID{15: 1}, DocumentID{15: 2}
	for _, id := range []DocumentID{first, second} {
		if _, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(id), "value": Int(1)}); err != nil {
			t.Fatal(err)
		}
	}
	before := db.Stats()
	err = db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
		if _, err := tx.GetOne("items", first); err != nil {
			return err
		}
		_, err := tx.GetOne("items", second)
		return err
	})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("read-set entry limit err=%v", err)
	}
	after := db.Stats()
	if after.CommitSequence != before.CommitSequence || after.Resources.Rejections != before.Resources.Rejections+1 ||
		after.Transactions.Aborted != before.Transactions.Aborted+1 || after.Transactions.Active != 0 {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
}

func TestRunWriteTransactionBoundsRetainedOverlayBytesWithoutCumulativeUpdates(t *testing.T) {
	limits := ResourceLimits{MaxDocumentBytes: 256, MaxTransactionBytes: 300, MaxTransactionChanges: 4}
	db, err := OpenV2WithOptions(filepath.Join(t.TempDir(), "bounded-overlay-bytes.meld2"), V2Options{ResourceLimits: limits})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	first, second := DocumentID{15: 1}, DocumentID{15: 2}
	document := func(id DocumentID, marker string) Document {
		return Document{"_id": ID(id), "payload": String(marker + "-01234567890123456789012345678901234567890123456789")}
	}
	for _, id := range []DocumentID{first, second} {
		if _, err := db.Collection("items").InsertOne(context.Background(), document(id, "base")); err != nil {
			t.Fatal(err)
		}
	}
	if firstSize, err := canonicalDocumentSize(document(first, "base")); err != nil || firstSize*2 > limits.MaxTransactionBytes {
		t.Fatalf("invalid test fixture size=%d err=%v", firstSize, err)
	}
	before := db.Stats()
	err = db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
		if _, err := tx.GetOne("items", first); err != nil {
			return err
		}
		_, err := tx.GetOne("items", second)
		return err
	})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("retained overlay limit err=%v", err)
	}
	if stats := db.Stats(); stats.CommitSequence != before.CommitSequence || stats.Resources.Rejections != before.Resources.Rejections+1 {
		t.Fatalf("rejected overlay stats=%+v before=%+v", stats, before)
	}

	if err := db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
		for revision := 0; revision < 32; revision++ {
			marker := "back"
			if revision%2 == 1 {
				marker = "next"
			}
			if err := tx.ReplaceOne("items", first, document(first, marker)); err != nil {
				return fmt.Errorf("replacement %d: %w", revision, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("replacements accumulated overlay bytes: %v", err)
	}
	updated, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": first})
	if err != nil || !updated["payload"].Equal(document(first, "next")["payload"]) {
		t.Fatalf("updated=%v err=%v", updated, err)
	}
}

func TestRunWriteTransactionDoesNotHoldWriterLockAndRejectsPointConflict(t *testing.T) {
	db, err := OpenV2(filepath.Join(t.TempDir(), "public-write-conflict.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ready, release := make(chan struct{}), make(chan struct{})
	var callbackReturned atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			if _, err := tx.InsertOne("items", Document{"_id": ID(DocumentID{15: 1})}); err != nil {
				return err
			}
			close(ready)
			<-release
			callbackReturned.Store(true)
			return nil
		})
	}()
	<-ready
	if stats := db.Stats(); stats.Transactions.Active != 1 || stats.Transactions.Started != 1 {
		t.Fatalf("active transaction stats=%+v", stats.Transactions)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(DocumentID{15: 1}), "value": Int(1)}); err != nil {
		t.Fatalf("concurrent writer was blocked: %v", err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrWriteConflict) || !callbackReturned.Load() {
		t.Fatalf("conflict err=%v callbackReturned=%t", err, callbackReturned.Load())
	}
	if document, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": DocumentID{15: 1}}); err != nil || !document["value"].Equal(Int(1)) {
		t.Fatalf("winning document=%v err=%v", document, err)
	}
	if stats := db.Stats(); stats.Transactions.Active != 0 || stats.Transactions.Conflicts != 1 || stats.Transactions.Aborted != 0 {
		t.Fatalf("conflict stats=%+v", stats.Transactions)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(DocumentID{15: 2})}); err != nil {
		t.Fatalf("write after safe conflict failed: %v", err)
	}
	if db.Stats().WritesDisabled {
		t.Fatal("ordinary optimistic conflict disabled writes")
	}
}

func TestRunWriteTransactionAllowsDisjointConcurrentCommit(t *testing.T) {
	db, err := OpenV2(filepath.Join(t.TempDir(), "public-write-disjoint.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ready, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			if _, err := tx.InsertOne("items", Document{"_id": ID(DocumentID{15: 1})}); err != nil {
				return err
			}
			close(ready)
			<-release
			return nil
		})
	}()
	<-ready
	if _, err := db.Collection("other").InsertOne(context.Background(), Document{"_id": ID(DocumentID{15: 2})}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("disjoint transaction err=%v", err)
	}
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Documents != 2 || stats.Transactions.Committed != 1 || stats.Transactions.Conflicts != 0 {
		t.Fatalf("disjoint stats=%+v", stats)
	}
}

func TestRunWriteTransactionMaintainsIndexCreatedAfterSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "public-write-concurrent-index.meld2")
	db, err := OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	id := DocumentID{15: 1}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(id), "value": String("old")}); err != nil {
		t.Fatal(err)
	}
	ready, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			if err := tx.ReplaceOne("items", id, Document{"value": String("new")}); err != nil {
				return err
			}
			close(ready)
			<-release
			return nil
		})
	}()
	<-ready
	if err := db.Collection("items").CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("transaction after index creation: %v", err)
	}
	explain, err := db.Collection("items").Explain(context.Background(), Filter{"value": "new"})
	if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "by_value" {
		t.Fatalf("explain=%+v err=%v", explain, err)
	}
	found, err := db.Collection("items").FindOne(context.Background(), Filter{"value": "new"})
	if err != nil || !found["value"].Equal(String("new")) {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if _, err := db.Collection("items").FindOne(context.Background(), Filter{"value": "old"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale index key err=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	found, err = db.Collection("items").FindOne(context.Background(), Filter{"value": "new"})
	if err != nil || !found["value"].Equal(String("new")) {
		t.Fatalf("reopened found=%v err=%v", found, err)
	}
}

func TestRunWriteTransactionReadOnlySnapshotRejectsConcurrentCommit(t *testing.T) {
	db, err := OpenV2(filepath.Join(t.TempDir(), "public-read-conflict.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := DocumentID{15: 1}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(id), "value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	ready, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			document, err := tx.GetOne("items", id)
			if err != nil || !document["value"].Equal(Int(1)) {
				return errors.Join(err, ErrCorrupt)
			}
			close(ready)
			<-release
			return nil
		})
	}()
	<-ready
	if _, err := db.Collection("items").UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": 2}}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("read-only conflict err=%v", err)
	}
	if stats := db.Stats(); stats.Transactions.Conflicts != 1 || stats.Transactions.Noops != 0 || stats.Transactions.Aborted != 0 {
		t.Fatalf("read-only conflict stats=%+v", stats.Transactions)
	}
}

func TestRunWriteTransactionReadOnlyAllowsDisjointConcurrentCommit(t *testing.T) {
	db, err := OpenV2(filepath.Join(t.TempDir(), "public-read-disjoint.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := DocumentID{15: 1}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(id), "value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	ready, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			if _, err := tx.GetOne("items", id); err != nil {
				return err
			}
			close(ready)
			<-release
			return nil
		})
	}()
	<-ready
	if _, err := db.Collection("other").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("disjoint read-only err=%v", err)
	}
	if stats := db.Stats(); stats.Transactions.Noops != 1 || stats.Transactions.Conflicts != 0 {
		t.Fatalf("disjoint read-only stats=%+v", stats.Transactions)
	}
}

func TestRunWriteTransactionPanicClosesAndAccountsAbort(t *testing.T) {
	db, err := OpenV2(filepath.Join(t.TempDir(), "public-write-panic.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var retained *WriteTransaction
	func() {
		defer func() {
			if recovered := recover(); recovered != "application panic" {
				t.Fatalf("recovered=%v", recovered)
			}
		}()
		_ = db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			retained = tx
			if _, err := tx.InsertOne("items", Document{"value": Int(1)}); err != nil {
				return err
			}
			panic("application panic")
		})
	}()
	if _, err := retained.GetOne("items", DocumentID{15: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("retained panic transaction err=%v", err)
	}
	if stats := db.Stats(); stats.CommitSequence != 0 || stats.Documents != 0 || stats.Transactions.Active != 0 ||
		stats.Transactions.Started != 1 || stats.Transactions.Aborted != 1 {
		t.Fatalf("panic stats=%+v", stats)
	}
}

func TestRunWriteTransactionRejectsUnsupportedEnginesAndCanceledContext(t *testing.T) {
	memory := New()
	defer memory.Close()
	if err := memory.RunWriteTransaction(context.Background(), func(*WriteTransaction) error { return nil }); !errors.Is(err, ErrWriteTransactionUnsupported) {
		t.Fatalf("memory err=%v", err)
	}
	v1, err := OpenV1(filepath.Join(t.TempDir(), "legacy.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer v1.Close()
	if err := v1.RunWriteTransaction(context.Background(), func(*WriteTransaction) error { return nil }); !errors.Is(err, ErrWriteTransactionUnsupported) {
		t.Fatalf("V1 err=%v", err)
	}
	v2, err := OpenV2(filepath.Join(t.TempDir(), "canceled.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := v2.RunWriteTransaction(ctx, func(*WriteTransaction) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err=%v", err)
	}
}

func receiveWriteTransactionBatch(t *testing.T, events <-chan ChangeBatch) ChangeBatch {
	t.Helper()
	select {
	case batch := <-events:
		return batch
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for transaction change batch")
		return ChangeBatch{}
	}
}
