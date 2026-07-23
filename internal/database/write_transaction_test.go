package database

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/crapthings/meldbase/internal/systemrecord"
)

func TestMeldbaseSystemWriteCommitsPointMutationsAndTerminalAtomically(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "atomic-write.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	backend := db.MeldbaseSystemRecordBackend()
	key, pending, terminal := []byte("rpc-key"), []byte("pending"), []byte("terminal")
	if result, err := backend.CompareAndSwap(context.Background(), systemrecord.Mutation{
		TransactionID: [16]byte{1}, Key: key, NewValue: pending,
	}); err != nil || !result.Applied {
		t.Fatalf("seed=%+v err=%v", result, err)
	}
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := db.Collection("orders").SubscribeQuery(context.Background(), query, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if initial := receiveSnapshot(t, subscription.Snapshots); initial.Token != 1 || len(initial.Documents) != 0 {
		t.Fatalf("initial=%+v", initial)
	}
	id := DocumentID{15: 1}
	var retained *WriteTransaction
	policyKey, generation := []byte("query.policy.generation.test"), []byte("generation-1")
	var invalidated atomic.Bool
	result, composite, err := db.MeldbaseSystemWrite(context.Background(), systemrecord.Mutation{
		Key: key, ExpectedExists: true, ExpectedHash: sha256.Sum256(pending), NewValue: terminal,
	}, func(tx *WriteTransaction) ([]byte, error) {
		retained = tx
		inserted, err := tx.InsertOne("orders", Document{"_id": ID(id), "status": String("new")})
		if err != nil || inserted != id {
			t.Fatalf("inserted=%s err=%v", inserted, err)
		}
		if err := tx.ReplaceOne("orders", id, Document{"status": String("created")}); err != nil {
			return nil, err
		}
		document, err := tx.GetOne("orders", id)
		if err != nil || !document["status"].Equal(String("created")) {
			t.Fatalf("transaction view=%v err=%v", document, err)
		}
		canceledID := DocumentID{15: 2}
		if _, err := tx.InsertOne("orders", Document{"_id": ID(canceledID)}); err != nil {
			return nil, err
		}
		if err := tx.DeleteOne("orders", canceledID); err != nil {
			return nil, err
		}
		if err := tx.MeldbaseStageSystemMutation(systemrecord.Mutation{
			Key: policyKey, NewValue: generation, Unconditional: true,
		}, func(uint64) { invalidated.Store(true) }); err != nil {
			return nil, err
		}
		return terminal, nil
	})
	if err != nil || !composite || !result.Applied {
		t.Fatalf("composite=%t result=%+v err=%v", composite, result, err)
	}
	if _, err := retained.GetOne("orders", id); !errors.Is(err, ErrClosed) {
		t.Fatalf("retained transaction err=%v", err)
	}
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Documents != 1 || stats.Collections != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	if snapshot := receiveSnapshot(t, subscription.Snapshots); snapshot.Token != 2 || len(snapshot.Documents) != 1 {
		t.Fatalf("atomic reactive snapshot=%+v", snapshot)
	}
	if !invalidated.Load() {
		t.Fatal("commit hook did not run before reactive publication")
	}
	document, err := db.Collection("orders").FindOne(context.Background(), Filter{"_id": id})
	if err != nil || !document["status"].Equal(String("created")) {
		t.Fatalf("committed document=%v err=%v", document, err)
	}
	records, err := backend.Scan(context.Background(), key, append(append([]byte(nil), key...), 0), 1)
	if err != nil || len(records) != 1 || !bytes.Equal(records[0].Value, terminal) {
		t.Fatalf("terminal records=%v err=%v", records, err)
	}
	policyRecords, err := backend.Scan(context.Background(), policyKey, append(append([]byte(nil), policyKey...), 0), 1)
	if err != nil || len(policyRecords) != 1 || !bytes.Equal(policyRecords[0].Value, generation) {
		t.Fatalf("policy generation records=%v err=%v", policyRecords, err)
	}
}

func TestMeldbaseSystemWriteRollsBackOnCASMismatchAndHandlerError(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "atomic-rollback.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	backend := db.MeldbaseSystemRecordBackend()
	key, pending := []byte("rpc-key"), []byte("pending")
	if result, err := backend.CompareAndSwap(context.Background(), systemrecord.Mutation{
		TransactionID: [16]byte{1}, Key: key, NewValue: pending,
	}); err != nil || !result.Applied {
		t.Fatalf("seed=%+v err=%v", result, err)
	}
	firstID := DocumentID{15: 1}
	result, composite, err := db.MeldbaseSystemWrite(context.Background(), systemrecord.Mutation{
		Key: key, ExpectedExists: true, ExpectedHash: sha256.Sum256([]byte("wrong")), NewValue: []byte("terminal"),
	}, func(tx *WriteTransaction) ([]byte, error) {
		_, err := tx.InsertOne("orders", Document{"_id": ID(firstID)})
		return []byte("terminal"), err
	})
	if err != nil || !composite || result.Applied || !bytes.Equal(result.Current, pending) {
		t.Fatalf("mismatch composite=%t result=%+v err=%v", composite, result, err)
	}
	if stats := db.Stats(); stats.CommitSequence != 1 || stats.Documents != 0 {
		t.Fatalf("mismatch stats=%+v", stats)
	}

	wantErr := errors.New("business rejected")
	secondID := DocumentID{15: 2}
	var rollbackHook atomic.Bool
	_, composite, err = db.MeldbaseSystemWrite(context.Background(), systemrecord.Mutation{
		Key: key, ExpectedExists: true, ExpectedHash: sha256.Sum256(pending), NewValue: []byte("terminal"),
	}, func(tx *WriteTransaction) ([]byte, error) {
		if _, err := tx.InsertOne("orders", Document{"_id": ID(secondID)}); err != nil {
			return nil, err
		}
		if err := tx.MeldbaseStageSystemMutation(systemrecord.Mutation{
			Key: []byte("policy-rollback"), NewValue: []byte("generation"), Unconditional: true,
		}, func(uint64) { rollbackHook.Store(true) }); err != nil {
			return nil, err
		}
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) || composite {
		t.Fatalf("handler rollback composite=%t err=%v", composite, err)
	}
	if stats := db.Stats(); stats.CommitSequence != 1 || stats.Documents != 0 {
		t.Fatalf("handler rollback stats=%+v", stats)
	}
	if rollbackHook.Load() {
		t.Fatal("rolled-back transaction ran commit hook")
	}
}

func TestMeldbaseSystemWriteSharesOverlayAndSystemByteBudget(t *testing.T) {
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "shared-overlay-system-budget.meld2"), OpenOptions{
		ResourceLimits: ResourceLimits{MaxDocumentBytes: 256, MaxTransactionBytes: 300, MaxTransactionChanges: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := DocumentID{15: 1}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{
		"_id": ID(id), "payload": String("01234567890123456789012345678901234567890123456789"),
	}); err != nil {
		t.Fatal(err)
	}
	before := db.Stats()
	_, composite, err := db.MeldbaseSystemWrite(context.Background(), systemrecord.Mutation{}, func(tx *WriteTransaction) ([]byte, error) {
		if _, err := tx.GetOne("items", id); err != nil {
			return nil, err
		}
		err := tx.MeldbaseStageSystemMutation(systemrecord.Mutation{
			Key: []byte("policy-generation"), NewValue: bytes.Repeat([]byte{'x'}, 160), Unconditional: true,
		}, nil)
		return nil, err
	})
	if composite || !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("composite=%t err=%v", composite, err)
	}
	after := db.Stats()
	if after.CommitSequence != before.CommitSequence || after.Resources.Rejections != before.Resources.Rejections+1 || after.WritesDisabled {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
}

func TestMeldbaseSystemWriteDoesNotHoldWriterLockAndRejectsStaleSnapshot(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "atomic-conflict.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	backend := db.MeldbaseSystemRecordBackend()
	key, pending := []byte("rpc-key"), []byte("pending")
	if result, err := backend.CompareAndSwap(context.Background(), systemrecord.Mutation{
		TransactionID: [16]byte{1}, Key: key, NewValue: pending,
	}); err != nil || !result.Applied {
		t.Fatalf("seed=%+v err=%v", result, err)
	}
	ready, release := make(chan struct{}), make(chan struct{})
	var handlerReturned atomic.Bool
	var conflictHook atomic.Bool
	policyKey := []byte("policy-conflict")
	type outcome struct {
		result    systemrecord.Result
		composite bool
		err       error
	}
	done := make(chan outcome, 1)
	go func() {
		result, composite, err := db.MeldbaseSystemWrite(context.Background(), systemrecord.Mutation{
			Key: key, ExpectedExists: true, ExpectedHash: sha256.Sum256(pending),
		}, func(tx *WriteTransaction) ([]byte, error) {
			if _, err := tx.InsertOne("orders", Document{"_id": ID(DocumentID{15: 1})}); err != nil {
				return nil, err
			}
			if err := tx.MeldbaseStageSystemMutation(systemrecord.Mutation{
				Key: policyKey, NewValue: []byte("generation"), Unconditional: true,
			}, func(uint64) { conflictHook.Store(true) }); err != nil {
				return nil, err
			}
			close(ready)
			<-release
			handlerReturned.Store(true)
			return []byte("terminal"), nil
		})
		done <- outcome{result: result, composite: composite, err: err}
	}()
	<-ready
	if _, err := db.Collection("orders").InsertOne(context.Background(), Document{"_id": ID(DocumentID{15: 1}), "winner": Bool(true)}); err != nil {
		t.Fatalf("concurrent writer was blocked or failed: %v", err)
	}
	close(release)
	got := <-done
	if !handlerReturned.Load() || !errors.Is(got.err, ErrWriteConflict) || !got.composite || got.result.Applied {
		t.Fatalf("stale outcome=%+v handlerReturned=%t", got, handlerReturned.Load())
	}
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Documents != 1 {
		t.Fatalf("conflict stats=%+v", stats)
	}
	if document, err := db.Collection("orders").FindOne(context.Background(), Filter{"_id": DocumentID{15: 1}}); err != nil || !document["winner"].Equal(Bool(true)) {
		t.Fatalf("winning document=%v err=%v", document, err)
	}
	if conflictHook.Load() {
		t.Fatal("conflicted transaction ran commit hook")
	}
	if records, err := backend.Scan(context.Background(), policyKey, append(append([]byte(nil), policyKey...), 0), 1); err != nil || len(records) != 0 {
		t.Fatalf("conflicted policy records=%v err=%v", records, err)
	}
}

func TestMeldbaseSystemWriteReadOnlyResultRejectsStaleSnapshot(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "atomic-read-conflict.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := DocumentID{15: 1}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(id), "value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	ready, release := make(chan struct{}), make(chan struct{})
	type outcome struct {
		composite bool
		err       error
	}
	done := make(chan outcome, 1)
	go func() {
		_, composite, err := db.MeldbaseSystemWrite(context.Background(), systemrecord.Mutation{}, func(tx *WriteTransaction) ([]byte, error) {
			document, err := tx.GetOne("items", id)
			if err != nil || !document["value"].Equal(Int(1)) {
				return nil, errors.Join(err, ErrCorrupt)
			}
			close(ready)
			<-release
			return []byte("terminal"), nil
		})
		done <- outcome{composite: composite, err: err}
	}()
	<-ready
	if _, err := db.Collection("items").UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": 2}}); err != nil {
		t.Fatal(err)
	}
	close(release)
	got := <-done
	if got.composite || !errors.Is(got.err, ErrWriteConflict) {
		t.Fatalf("read-only system outcome=%+v", got)
	}
}
