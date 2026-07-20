package meldbase

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/crapthings/meldbase/internal/systemrecord"
)

type recordingPrimaryWriteFence struct {
	mu       sync.Mutex
	err      error
	requests []PrimaryWriteFenceRequest
	bound    []FollowerPromotionFence
	bindErr  error
}

func (fence *recordingPrimaryWriteFence) ValidatePrimaryWrite(request PrimaryWriteFenceRequest) error {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	fence.requests = append(fence.requests, request)
	return fence.err
}

func (fence *recordingPrimaryWriteFence) setError(err error) {
	fence.mu.Lock()
	fence.err = err
	fence.mu.Unlock()
}

func (fence *recordingPrimaryWriteFence) snapshot() []PrimaryWriteFenceRequest {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	return append([]PrimaryWriteFenceRequest(nil), fence.requests...)
}

func (fence *recordingPrimaryWriteFence) BindFollowerPromotion(_ context.Context, promotion FollowerPromotionFence) error {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	fence.bound = append(fence.bound, promotion)
	return fence.bindErr
}

func (fence *recordingPrimaryWriteFence) boundPromotions() []FollowerPromotionFence {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	return append([]FollowerPromotionFence(nil), fence.bound...)
}

func TestPrimaryWriteFenceRejectsBusinessCommitWithoutPoisoningDatabase(t *testing.T) {
	fence := &recordingPrimaryWriteFence{err: errors.New("primary lease expired")}
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "fenced.meld2"), OpenOptions{PrimaryWriteFence: fence})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); !errors.Is(err, ErrPrimaryWriteFence) {
		t.Fatalf("denied insert err=%v", err)
	}
	if got := db.Stats().CommitSequence; got != 0 {
		t.Fatalf("denied fence advanced token=%d", got)
	}
	requests := fence.snapshot()
	if len(requests) != 1 || requests[0].DatabaseID != db.DatabaseID() || requests[0].NextCommitSequence != 1 {
		t.Fatalf("denied fence requests=%+v", requests)
	}
	fence.setError(nil)
	id, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(2)})
	if err != nil {
		t.Fatal(err)
	}
	if got := db.Stats().CommitSequence; got != 1 {
		t.Fatalf("allowed fence token=%d", got)
	}
	if document, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err != nil || !document["value"].Equal(Int(2)) {
		t.Fatalf("allowed fence document=%v err=%v", document, err)
	}
	if requests = fence.snapshot(); len(requests) != 2 || requests[1].NextCommitSequence != 1 {
		t.Fatalf("allowed fence requests=%+v", requests)
	}
	fence.setError(errors.New("lease revoked"))
	err = db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
		_, err := tx.InsertOne("items", Document{"value": Int(3)})
		return err
	})
	if !errors.Is(err, ErrPrimaryWriteFence) {
		t.Fatalf("denied write transaction err=%v", err)
	}
	if got := db.Stats().CommitSequence; got != 1 {
		t.Fatalf("denied transaction advanced token=%d", got)
	}
	stats := db.Stats().PrimaryWriteFence
	if !stats.Configured || !stats.Enforced || stats.Checks != 3 || stats.Rejected != 2 {
		t.Fatalf("primary write fence stats=%+v", stats)
	}
}

func TestPrimaryWriteFenceGuardsEveryGroupedLogicalSequence(t *testing.T) {
	fence := &recordingPrimaryWriteFence{}
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "fenced-group.meld2"), OpenOptions{PrimaryWriteFence: fence})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil {
		t.Fatal("missing  store")
	}
	firstID, secondID := DocumentID{15: 1}, DocumentID{15: 2}
	first, second := Document{"_id": ID(firstID), "value": Int(1)}, Document{"_id": ID(secondID), "value": Int(2)}
	db.mu.Lock()
	err = db.commitChangeBatchesWithPreconditionsLocked(context.Background(), store, []ChangeBatch{
		{Token: 1, Changes: []Change{{Collection: "items", Operation: InsertOperation, DocumentID: firstID, After: &first}}},
		{Token: 2, Changes: []Change{{Collection: "items", Operation: InsertOperation, DocumentID: secondID, After: &second}}},
	}, nil, nil)
	db.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	requests := fence.snapshot()
	if len(requests) != 2 || requests[0].NextCommitSequence != 1 || requests[1].NextCommitSequence != 2 {
		t.Fatalf("group fence requests=%+v", requests)
	}
	if got := db.Stats().CommitSequence; got != 2 {
		t.Fatalf("group token=%d", got)
	}
}

func TestPrimaryWriteFenceGuardsIndexBuildVisibilityCommit(t *testing.T) {
	fence := &recordingPrimaryWriteFence{}
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "fenced-index-build.meld2"), OpenOptions{PrimaryWriteFence: fence})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	baseline := db.Stats().PrimaryWriteFence

	id, err := db.Collection("items").StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fence.setError(errors.New("primary lease expired before index publication"))
	if err := db.ResumeIndexBuild(context.Background(), id); !errors.Is(err, ErrPrimaryWriteFence) {
		t.Fatalf("fenced index build finalization err=%v", err)
	}
	if got := db.Stats().CommitSequence; got != 1 {
		t.Fatalf("fenced index build advanced token=%d", got)
	}
	db.mu.RLock()
	_, visible := db.collections["items"].indexes["by_value"]
	db.mu.RUnlock()
	if visible {
		t.Fatal("fenced index became query-visible")
	}
	stats := db.Stats().PrimaryWriteFence
	if stats.Checks != baseline.Checks+1 || stats.Rejected != baseline.Rejected+1 {
		t.Fatalf("fenced index build stats=%+v", stats)
	}

	fence.setError(nil)
	if err := db.ResumeIndexBuild(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if got := db.Stats().CommitSequence; got != 2 {
		t.Fatalf("published index build token=%d", got)
	}
	db.mu.RLock()
	_, visible = db.collections["items"].indexes["by_value"]
	db.mu.RUnlock()
	if !visible {
		t.Fatal("permitted index did not become query-visible")
	}
	stats = db.Stats().PrimaryWriteFence
	if stats.Checks != baseline.Checks+2 || stats.Rejected != baseline.Rejected+1 {
		t.Fatalf("published index build stats=%+v", stats)
	}
}

func TestPrimaryWriteFenceGuardsStandaloneSystemRecordCommit(t *testing.T) {
	fence := &recordingPrimaryWriteFence{err: errors.New("primary lease expired")}
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "fenced-system.meld2"), OpenOptions{PrimaryWriteFence: fence})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	backend := db.MeldbaseSystemRecordBackend()
	if backend == nil {
		t.Fatal("missing system record backend")
	}
	mutation := systemrecord.Mutation{TransactionID: [16]byte{1}, Key: []byte("rpc/idempotency"), NewValue: []byte("pending"), Unconditional: true}
	if result, err := backend.CompareAndSwap(context.Background(), mutation); result.Applied || !errors.Is(err, ErrPrimaryWriteFence) {
		t.Fatalf("fenced system record result=%+v err=%v", result, err)
	}
	if got := db.Stats().CommitSequence; got != 0 {
		t.Fatalf("fenced system record advanced token=%d", got)
	}
	fence.setError(nil)
	if result, err := backend.CompareAndSwap(context.Background(), mutation); err != nil || !result.Applied {
		t.Fatalf("permitted system record result=%+v err=%v", result, err)
	}
	stats := db.Stats().PrimaryWriteFence
	if stats.Checks != 2 || stats.Rejected != 1 || db.Stats().CommitSequence != 1 {
		t.Fatalf("system record primary write fence stats=%+v sequence=%d", stats, db.Stats().CommitSequence)
	}
}

func TestPrimaryWriteFenceGuardsInitialDurableConsumerControlCommit(t *testing.T) {
	fence := &recordingPrimaryWriteFence{err: errors.New("primary lease expired")}
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "fenced-consumer.meld2"), OpenOptions{PrimaryWriteFence: fence})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if subscription, err := db.CreateDurableDatabaseChanges(context.Background(), "follower", 0, 1); subscription != nil || !errors.Is(err, ErrPrimaryWriteFence) {
		t.Fatalf("fenced consumer subscription=%v err=%v", subscription, err)
	}
	if got := db.Stats().CommitSequence; got != 0 {
		t.Fatalf("fenced consumer advanced token=%d", got)
	}
	fence.setError(nil)
	subscription, err := db.CreateDurableDatabaseChanges(context.Background(), "follower", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	subscription.Close()
	stats := db.Stats().PrimaryWriteFence
	if stats.Checks != 2 || stats.Rejected != 1 || db.Stats().CommitSequence != 1 {
		t.Fatalf("durable consumer primary write fence stats=%+v sequence=%d", stats, db.Stats().CommitSequence)
	}
}

func TestFollowerApplyBypassesPrimaryWriteFence(t *testing.T) {
	directory := t.TempDir()
	source, err := Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	bootstrap := filepath.Join(directory, "bootstrap.meld2")
	if _, err := source.Backup(context.Background(), bootstrap); err != nil {
		t.Fatal(err)
	}
	follower, err := OpenFollower(bootstrap, OpenOptions{PrimaryWriteFence: &recordingPrimaryWriteFence{err: errors.New("not primary")}})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	id := DocumentID{15: 2}
	document := Document{"_id": ID(id), "value": Int(2)}
	if err := follower.Apply(context.Background(), DurableDatabaseChangeBatch{
		Token: 2, TransactionID: [16]byte{1}, Changes: []Change{{Collection: "items", Operation: InsertOperation, DocumentID: id, After: &document}},
	}); err != nil {
		t.Fatalf("follower apply was incorrectly fenced: %v", err)
	}
	stats := follower.DB().Stats().PrimaryWriteFence
	if !stats.Configured || stats.Enforced || stats.Checks != 0 || stats.Rejected != 0 {
		t.Fatalf("follower primary write fence stats=%+v", stats)
	}
}

func TestFormatDetectionForwardsPrimaryWriteFenceTo(t *testing.T) {
	fence := &recordingPrimaryWriteFence{err: errors.New("lease absent")}
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "new-store.meld2"), OpenOptions{PrimaryWriteFence: fence})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); !errors.Is(err, ErrPrimaryWriteFence) {
		t.Fatalf("format-neutral fenced  err=%v", err)
	}
}
