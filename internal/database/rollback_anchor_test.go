package database

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	storage "github.com/crapthings/meldbase/internal/storage"
	"github.com/crapthings/meldbase/internal/systemrecord"
)

type testRollbackAnchorStore struct {
	anchor      RollbackAnchor
	exists      bool
	saveErr     error
	saveThenErr error
}

type blockingAdvanceRollbackAnchorStore struct {
	anchor RollbackAnchor
	done   chan struct{}
}

type blockingLoadRollbackAnchorStore struct{ done chan struct{} }

func (store *blockingLoadRollbackAnchorStore) Load(ctx context.Context) (RollbackAnchor, bool, error) {
	<-ctx.Done()
	close(store.done)
	return RollbackAnchor{}, false, ctx.Err()
}

func (store *blockingLoadRollbackAnchorStore) Advance(ctx context.Context, _ RollbackAnchor) error {
	return ctx.Err()
}

func (store *blockingAdvanceRollbackAnchorStore) Load(ctx context.Context) (RollbackAnchor, bool, error) {
	return store.anchor, true, ctx.Err()
}

func (store *blockingAdvanceRollbackAnchorStore) Advance(ctx context.Context, _ RollbackAnchor) error {
	<-ctx.Done()
	close(store.done)
	return ctx.Err()
}

func (store *testRollbackAnchorStore) Load(ctx context.Context) (RollbackAnchor, bool, error) {
	if err := ctx.Err(); err != nil {
		return RollbackAnchor{}, false, err
	}
	return store.anchor, store.exists, nil
}
func (store *testRollbackAnchorStore) Advance(ctx context.Context, anchor RollbackAnchor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store.saveErr != nil {
		return store.saveErr
	}
	if store.exists && (store.anchor.DatabaseID != anchor.DatabaseID || store.anchor.MinimumCommitSequence > anchor.MinimumCommitSequence || store.anchor.MinimumGeneration > anchor.MinimumGeneration) {
		return ErrRollbackAnchor
	}
	store.anchor, store.exists = anchor, true
	return store.saveThenErr
}

func TestFileRollbackAnchorStoreRejectsCorruptionAndRegression(t *testing.T) {
	directory := t.TempDir()
	storeValue, err := NewFileRollbackAnchorStore(filepath.Join(directory, "db.anchor"))
	if err != nil {
		t.Fatal(err)
	}
	identity := [16]byte{1, 2, 3}
	first := RollbackAnchor{DatabaseID: identity, MinimumCommitSequence: 7, MinimumGeneration: 8}
	if err := storeValue.Advance(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	loaded, exists, err := storeValue.Load(context.Background())
	if err != nil || !exists || loaded != first {
		t.Fatalf("loaded=%+v exists=%t err=%v", loaded, exists, err)
	}
	if err := storeValue.Advance(context.Background(), RollbackAnchor{DatabaseID: identity, MinimumCommitSequence: 6, MinimumGeneration: 8}); !errors.Is(err, ErrRollbackAnchor) {
		t.Fatalf("regression error=%v", err)
	}
	if err := storeValue.Advance(context.Background(), RollbackAnchor{DatabaseID: identity, MinimumCommitSequence: 7, MinimumGeneration: 7}); !errors.Is(err, ErrRollbackAnchor) {
		t.Fatalf("generation regression error=%v", err)
	}
	if err := storeValue.Advance(context.Background(), RollbackAnchor{DatabaseID: identity, MinimumCommitSequence: 9, MinimumGeneration: 0}); !errors.Is(err, ErrRollbackAnchor) {
		t.Fatalf("zero generation error=%v", err)
	}
	other := first
	other.DatabaseID[0]++
	if err := storeValue.Advance(context.Background(), other); !errors.Is(err, ErrRollbackAnchor) {
		t.Fatalf("identity error=%v", err)
	}
	raw, err := os.ReadFile(filepath.Join(directory, "db.anchor"))
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 1
	if err := os.WriteFile(filepath.Join(directory, "db.anchor"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := storeValue.Load(context.Background()); !errors.Is(err, ErrRollbackAnchor) {
		t.Fatalf("corruption error=%v", err)
	}
}

func TestFileRollbackAnchorStoreSerializesIndependentWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.anchor")
	first, err := NewFileRollbackAnchorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewFileRollbackAnchorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	anchors := []RollbackAnchor{
		{DatabaseID: [16]byte{1}, MinimumCommitSequence: 1, MinimumGeneration: 2},
		{DatabaseID: [16]byte{2}, MinimumCommitSequence: 1, MinimumGeneration: 2},
	}
	stores := []RollbackAnchorStore{first, second}
	start := make(chan struct{})
	errorsByWriter := make([]error, len(stores))
	var writers sync.WaitGroup
	for index := range stores {
		writers.Add(1)
		go func(index int) {
			defer writers.Done()
			<-start
			errorsByWriter[index] = stores[index].Advance(context.Background(), anchors[index])
		}(index)
	}
	close(start)
	writers.Wait()
	succeeded := 0
	for _, err := range errorsByWriter {
		if err == nil {
			succeeded++
		} else if !errors.Is(err, ErrRollbackAnchor) {
			t.Fatalf("unexpected writer error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful writers=%d errors=%v", succeeded, errorsByWriter)
	}
	retained, exists, err := first.Load(context.Background())
	if err != nil || !exists || (retained != anchors[0] && retained != anchors[1]) {
		t.Fatalf("retained=%+v exists=%t err=%v", retained, exists, err)
	}
}

func TestFileRollbackAnchorStoreHonorsContextWhileLocallyContended(t *testing.T) {
	directory := t.TempDir()
	storeValue, err := NewFileRollbackAnchorStore(filepath.Join(directory, "context.anchor"))
	if err != nil {
		t.Fatal(err)
	}
	store := storeValue.(*fileRollbackAnchorStore)
	if err := store.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, _, err := store.Load(ctx); !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("contended load err=%v duration=%s", err, time.Since(started))
	}
	store.release()

	canceled, cancelAdvance := context.WithCancel(context.Background())
	cancelAdvance()
	if err := store.Advance(canceled, RollbackAnchor{DatabaseID: [16]byte{1}, MinimumGeneration: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled advance error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, "context.anchor")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled advance created anchor: %v", err)
	}
}

func TestRollbackAnchorRejectsAcknowledgedSnapshotRollback(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "current.meld")
	anchorDirectory := filepath.Join(directory, "trusted")
	if err := os.Mkdir(anchorDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	anchor, err := NewFileRollbackAnchorStore(filepath.Join(anchorDirectory, "current.anchor"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(databasePath, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: anchor, InitializeAnchor: true}})
	if err != nil {
		t.Fatal(err)
	}
	collection := db.Collection("items")
	if _, err := collection.InsertOne(context.Background(), Document{"value": String("one")}); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats().Storage; !stats.RollbackProtected || stats.RollbackAnchorSequence != 1 || stats.RollbackAnchorFailures != 0 || stats.RollbackAnchorNanos == 0 {
		t.Fatalf("rollback stats=%+v", stats)
	}
	identity := db.DatabaseIdentity()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	stale, err := os.ReadFile(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	state, exists, err := anchor.Load(context.Background())
	if err != nil || !exists || state.DatabaseID != identity || state.MinimumCommitSequence != 1 || state.MinimumGeneration != 2 {
		t.Fatalf("anchor=%+v exists=%t err=%v", state, exists, err)
	}

	db, err = OpenWithOptions(databasePath, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: anchor}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": String("two")}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	state, _, err = anchor.Load(context.Background())
	if err != nil || state.MinimumCommitSequence != 2 || state.MinimumGeneration != 3 {
		t.Fatalf("advanced anchor=%+v err=%v", state, err)
	}
	if err := os.WriteFile(databasePath, stale, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, err := OpenWithOptions(databasePath, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: anchor}})
	if !errors.Is(err, ErrRollbackDetected) || rolledBack != nil {
		t.Fatalf("rollback db=%v err=%v", rolledBack, err)
	}
	after, err := os.ReadFile(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("rollback rejection modified the stale database")
	}
}

func TestRollbackAnchorRejectsAcknowledgedMaintenanceGenerationRollback(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "maintenance.meld")
	anchor, err := NewFileRollbackAnchorStore(filepath.Join(directory, "maintenance.anchor"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(path, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: anchor, InitializeAnchor: true}})
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	stale, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	before := db.Stats().Storage
	if _, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	after := db.Stats().Storage
	if after.CommitSequence != before.CommitSequence || after.Generation != before.Generation+1 || after.RollbackAnchorGeneration != after.Generation {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, stale, 0o600); err != nil {
		t.Fatal(err)
	}
	if opened, err := OpenWithOptions(path, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: anchor}}); !errors.Is(err, ErrRollbackDetected) || opened != nil {
		t.Fatalf("maintenance rollback opened=%v err=%v", opened, err)
	}
}

func TestRollbackAnchorTracksIndexBuildAndReclamationGenerations(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "maintenance-lifecycle.meld")
	anchor, err := NewFileRollbackAnchorStore(filepath.Join(directory, "maintenance-lifecycle.anchor"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(path, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: anchor, InitializeAnchor: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	id, err := items.InsertOne(context.Background(), Document{"value": Int(0)})
	if err != nil {
		t.Fatal(err)
	}
	for revision := int64(1); revision <= 8; revision++ {
		if _, err := items.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": revision}}); err != nil {
			t.Fatal(err)
		}
	}
	build, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ResumeIndexBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	assertRollbackAnchorCurrent(t, db, anchor)

	aborted, err := items.StartIndexBuild(context.Background(), "aborted", []IndexField{{Field: "value", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AbortIndexBuild(context.Background(), aborted); err != nil {
		t.Fatal(err)
	}
	assertRollbackAnchorCurrent(t, db, anchor)

	failed, err := items.StartIndexBuild(context.Background(), "failed", []IndexField{{Field: "value", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.failIndexBuild(context.Background(), failed, storage.IndexBuildFailureResourceLimit); err != nil {
		t.Fatal(err)
	}
	assertRollbackAnchorCurrent(t, db, anchor)

	beforeReclaim := db.Stats().Storage.Generation
	result, err := db.ReclaimPages(context.Background())
	if err != nil || !result.Persisted || result.ReusablePages == 0 {
		t.Fatalf("reclaim=%+v err=%v", result, err)
	}
	if db.Stats().Storage.Generation <= beforeReclaim {
		t.Fatalf("reclamation did not publish a maintenance generation: before=%d after=%d", beforeReclaim, db.Stats().Storage.Generation)
	}
	assertRollbackAnchorCurrent(t, db, anchor)
}

func assertRollbackAnchorCurrent(t *testing.T, db *DB, store RollbackAnchorStore) {
	t.Helper()
	anchor, exists, err := store.Load(context.Background())
	stats := db.Stats().Storage
	if err != nil || !exists || anchor.DatabaseID != db.DatabaseIdentity() || anchor.MinimumCommitSequence != stats.CommitSequence ||
		anchor.MinimumGeneration != stats.Generation || stats.RollbackAnchorSequence != stats.CommitSequence || stats.RollbackAnchorGeneration != stats.Generation {
		t.Fatalf("anchor=%+v exists=%t storage=%+v err=%v", anchor, exists, stats, err)
	}
}

func TestRollbackAnchorFailureIsFailStopAndRecoversAheadDatabase(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "anchor-failure.meld")
	seed, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	identity := seed.DatabaseIdentity()
	generation := seed.Stats().Storage.Generation
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	store := &testRollbackAnchorStore{anchor: RollbackAnchor{DatabaseID: identity, MinimumGeneration: generation}, exists: true, saveErr: errors.New("injected anchor fsync failure")}
	db, err := OpenWithOptions(databasePath, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: store}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": String("committed-but-unacknowledged")}); !errors.Is(err, ErrDurability) {
		t.Fatalf("commit error=%v", err)
	}
	if db.Stats().CommitSequence != 0 || !db.Stats().WritesDisabled || db.Stats().Storage.RollbackAnchorFailures != 1 || db.Stats().Storage.RollbackAnchorSequence != 0 {
		t.Fatalf("fail-stop stats=%+v", db.Stats())
	}
	if err := db.Close(); !errors.Is(err, ErrDurability) {
		t.Fatalf("close error=%v", err)
	}
	store.saveErr = nil
	reopened, err := OpenWithOptions(databasePath, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: store}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Stats().CommitSequence != 1 || store.anchor.MinimumCommitSequence != 1 {
		t.Fatalf("reopened sequence=%d anchor=%+v", reopened.Stats().CommitSequence, store.anchor)
	}
	document, err := reopened.Collection("items").FindOne(context.Background(), Filter{})
	value, valueOK := document["value"].StringValue()
	if err != nil || !valueOK || value != "committed-but-unacknowledged" {
		t.Fatalf("document=%v err=%v", document, err)
	}
}

func TestRollbackAnchorCoversStandaloneSystemRecordCommit(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "anchor-system.meld")
	seed, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	identity := seed.DatabaseIdentity()
	generation := seed.Stats().Storage.Generation
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	store := &testRollbackAnchorStore{anchor: RollbackAnchor{DatabaseID: identity, MinimumGeneration: generation}, exists: true}
	db, err := OpenWithOptions(databasePath, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: store}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	backend := db.MeldbaseSystemRecordBackend()
	if backend == nil {
		t.Fatal("missing system record backend")
	}
	result, err := backend.CompareAndSwap(context.Background(), systemrecord.Mutation{TransactionID: [16]byte{1}, Key: []byte("anchor-test"), NewValue: []byte("value"), Unconditional: true})
	if err != nil || !result.Applied || db.Stats().CommitSequence != 1 || store.anchor.MinimumCommitSequence != 1 {
		t.Fatalf("result=%+v sequence=%d anchor=%+v err=%v", result, db.Stats().CommitSequence, store.anchor, err)
	}
}

func TestRollbackAnchorMissingRequiresExplicitInitialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store := &testRollbackAnchorStore{}
	if opened, err := OpenWithOptions(path, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: store}}); !errors.Is(err, ErrRollbackAnchorRequired) || opened != nil {
		t.Fatalf("opened=%v err=%v", opened, err)
	}
}

func TestRollbackAnchorOperationTimeoutIsFailStop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timeout.meld")
	seed, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingAdvanceRollbackAnchorStore{
		anchor: RollbackAnchor{DatabaseID: seed.DatabaseIdentity(), MinimumGeneration: seed.Stats().Storage.Generation},
		done:   make(chan struct{}),
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(path, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: store, OperationTimeout: 20 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": String("ambiguous")}); !errors.Is(err, ErrDurability) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("commit error=%v", err)
	}
	if time.Since(started) > time.Second || !db.Stats().WritesDisabled || db.Stats().Storage.RollbackAnchorFailures != 1 {
		t.Fatalf("timeout stats=%+v duration=%s", db.Stats(), time.Since(started))
	}
	select {
	case <-store.done:
	default:
		t.Fatal("anchor store did not observe deadline cancellation")
	}
	if err := db.Close(); !errors.Is(err, ErrDurability) {
		t.Fatalf("close error=%v", err)
	}
}

func TestRollbackAnchorOpenTimeoutAndInvalidOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "open-timeout.meld")
	store := &blockingLoadRollbackAnchorStore{done: make(chan struct{})}
	started := time.Now()
	opened, err := OpenWithOptions(path, OpenOptions{RollbackProtection: RollbackProtection{AnchorStore: store, OperationTimeout: 20 * time.Millisecond}})
	if !errors.Is(err, ErrRollbackAnchor) || !errors.Is(err, context.DeadlineExceeded) || opened != nil || time.Since(started) > time.Second {
		t.Fatalf("opened=%v err=%v duration=%s", opened, err, time.Since(started))
	}
	select {
	case <-store.done:
	default:
		t.Fatal("anchor load did not observe deadline cancellation")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("timed-out anchor load created database: %v", statErr)
	}
	for _, protection := range []RollbackProtection{
		{OperationTimeout: time.Second},
		{OperationTimeout: -time.Second, AnchorStore: &testRollbackAnchorStore{}},
		{InitializeAnchor: true},
	} {
		if db, err := OpenWithOptions(filepath.Join(t.TempDir(), "invalid.meld"), OpenOptions{RollbackProtection: protection}); !errors.Is(err, ErrInvalidRollbackProtection) || db != nil {
			t.Fatalf("protection=%+v db=%v err=%v", protection, db, err)
		}
	}
}

func TestRollbackStaticGuardsMapPublicErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "guarded.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	identity := db.DatabaseIdentity()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if opened, err := OpenWithOptions(path, OpenOptions{RollbackProtection: RollbackProtection{MinimumCommitSequence: 1}}); !errors.Is(err, ErrRollbackDetected) || opened != nil {
		t.Fatalf("stale opened=%v err=%v", opened, err)
	}
	if opened, err := OpenWithOptions(path, OpenOptions{RollbackProtection: RollbackProtection{MinimumGeneration: 2}}); !errors.Is(err, ErrRollbackDetected) || opened != nil {
		t.Fatalf("generation opened=%v err=%v", opened, err)
	}
	identity[0]++
	if opened, err := OpenWithOptions(path, OpenOptions{RollbackProtection: RollbackProtection{ExpectedDatabaseID: identity}}); !errors.Is(err, ErrDatabaseIdentity) || opened != nil {
		t.Fatalf("identity opened=%v err=%v", opened, err)
	}
}
