package meldbase

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestChangeDispatcherCommitDoesNotWaitForDeliveryAndPreservesWatcherBoundary(t *testing.T) {
	db := New()
	defer db.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	db.dispatcher.testBeforeDispatch = func() {
		once.Do(func() { close(entered) })
		<-release
	}

	inserted := make(chan error, 1)
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"n": Int(1)})
		inserted <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not receive first commit")
	}
	select {
	case err := <-inserted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("commit waited for asynchronous change delivery")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, done, err := db.WatchChanges(ctx, "items", 2)
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"n": Int(2)}); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-events:
		if batch.Token != 2 || len(batch.Changes) != 1 {
			t.Fatalf("watch batch=%+v", batch)
		}
	case err := <-done:
		t.Fatalf("watcher ended early: %v", err)
	case <-time.After(time.Second):
		t.Fatal("watcher did not receive post-subscription batch")
	}
	select {
	case batch := <-events:
		t.Fatalf("watcher received stale batch: %+v", batch)
	default:
	}
}

func TestChangeDispatcherOverflowResyncsReactiveAndFailsWatchers(t *testing.T) {
	db := New()
	defer db.Close()
	db.dispatcher.maxBatches = 0
	collection := db.Collection("items")
	query, err := CompileQuery(Filter{"active": true}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := collection.SubscribeQuery(context.Background(), query, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if initial := receiveSnapshot(t, subscription.Snapshots); initial.Token != 0 || len(initial.Documents) != 0 {
		t.Fatalf("initial snapshot=%+v", initial)
	}
	watchContext, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	_, done, err := db.WatchChanges(watchContext, "items", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.InsertOne(context.Background(), Document{"active": Bool(true)}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrSlowConsumer) {
			t.Fatalf("watcher error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("overflow did not close direct watcher")
	}
	updated := receiveSnapshot(t, subscription.Snapshots)
	if updated.Token != 1 || len(updated.Documents) != 1 {
		t.Fatalf("resynced snapshot=%+v", updated)
	}
	if stats := db.Stats().Realtime; stats.QueueOverflows != 1 || stats.SlowConsumers != 1 {
		t.Fatalf("overflow stats=%+v", stats)
	}
}

func TestChangeDispatcherOverflowIsObservableWithoutReactiveViews(t *testing.T) {
	db := New()
	defer db.Close()
	db.dispatcher.maxBatches = 0
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"n": Int(1)}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for db.Stats().Realtime.QueueOverflows == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := db.Stats().Realtime.QueueOverflows; got != 1 {
		t.Fatalf("queue overflows=%d want=1", got)
	}
}

func TestChangeDispatcherBoundsPendingChangesAndReportsPressure(t *testing.T) {
	db := New()
	defer db.Close()
	db.dispatcher.maxBatches = 2
	db.dispatcher.maxChanges = 1
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	db.dispatcher.testBeforeDispatch = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	collection := db.Collection("items")
	if _, err := collection.InsertOne(context.Background(), Document{"n": Int(1)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not begin delivery")
	}
	if _, err := collection.InsertOne(context.Background(), Document{"n": Int(2)}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		stats := db.Stats().Realtime
		if stats.DispatchPendingBatches == 1 && stats.DispatchPendingChanges == 1 {
			if stats.DispatchBatchCapacity != 2 || stats.DispatchChangeCapacity != 1 {
				t.Fatalf("dispatcher capacity stats=%+v", stats)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dispatcher did not report pending change: %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}
	// A third one-change batch would exceed the total change bound even though
	// the batch-count bound still has room. It must take the resync boundary.
	if _, err := collection.InsertOne(context.Background(), Document{"n": Int(3)}); err != nil {
		t.Fatal(err)
	}
	close(release)
	deadline = time.Now().Add(time.Second)
	for {
		if stats := db.Stats().Realtime; stats.QueueOverflows == 1 && stats.DispatchPendingBatches == 0 && stats.DispatchPendingChanges == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("change-bound overflow was not observed: %+v", db.Stats().Realtime)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestChangeDispatcherBoundsPendingCanonicalBytes(t *testing.T) {
	db := New()
	defer db.Close()
	db.dispatcher.maxBatches = 8
	db.dispatcher.maxChanges = 8
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	db.dispatcher.testBeforeDispatch = func() {
		once.Do(func() { close(entered) })
		<-release
	}

	makeDocument := func(id byte) Document {
		return Document{"_id": ID(DocumentID{15: id}), "payload": String(strings.Repeat("x", 256))}
	}
	first := Change{Collection: "items", Operation: InsertOperation, DocumentID: DocumentID{15: 1}}
	base, ok := changeDispatchBaseBytes(first)
	if !ok {
		t.Fatal("change dispatch base size is invalid")
	}
	size, err := canonicalDocumentSize(makeDocument(1))
	if err != nil {
		t.Fatal(err)
	}
	db.dispatcher.maxBytes = base + size + 1

	collection := db.Collection("items")
	if _, err := collection.InsertOne(context.Background(), makeDocument(1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not begin delivery")
	}
	if _, err := collection.InsertOne(context.Background(), makeDocument(2)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		stats := db.Stats().Realtime
		if stats.DispatchPendingBatches == 1 && stats.DispatchPendingBytes == base+size {
			if stats.DispatchByteCapacity != base+size+1 {
				t.Fatalf("dispatcher byte capacity stats=%+v", stats)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dispatcher did not report pending bytes: %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}
	// The event and batch-count budgets still have room. This commit crosses
	// only the canonical-image byte budget, so it must force the same safe
	// resync/slow-consumer boundary rather than retaining another image.
	if _, err := collection.InsertOne(context.Background(), makeDocument(3)); err != nil {
		t.Fatal(err)
	}
	close(release)
	deadline = time.Now().Add(time.Second)
	for {
		if stats := db.Stats().Realtime; stats.QueueOverflows == 1 && stats.DispatchPendingBatches == 0 && stats.DispatchPendingBytes == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("byte-bound overflow was not observed: %+v", db.Stats().Realtime)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestChangeWatcherBoundsPendingCanonicalBytes(t *testing.T) {
	db := New()
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, done, err := db.WatchChanges(ctx, "items", 8)
	if err != nil {
		t.Fatal(err)
	}

	base, ok := changeDispatchBaseBytes(Change{Collection: "items", Operation: InsertOperation})
	if !ok {
		t.Fatal("change dispatch base size is invalid")
	}
	makeDocument := func(id byte) Document {
		return Document{"_id": ID(DocumentID{14: id}), "payload": String(strings.Repeat("x", 256))}
	}
	size, err := canonicalDocumentSize(makeDocument(1))
	if err != nil {
		t.Fatal(err)
	}
	db.feedMu.Lock()
	var watcher *changeWatcher
	for _, candidate := range db.watchers {
		watcher = candidate
	}
	if watcher == nil {
		db.feedMu.Unlock()
		t.Fatal("watcher was not registered")
	}
	watcher.maxBytes = base + size + 1
	db.feedMu.Unlock()

	collection := db.Collection("items")
	if _, err := collection.InsertOne(context.Background(), makeDocument(1)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		db.feedMu.Lock()
		pending := watcher.pendingBytes
		db.feedMu.Unlock()
		if pending == base+size {
			if stats := db.Stats().Realtime; stats.WatcherPendingBytes != base+size || stats.WatcherByteCapacity != maxPendingChangeWatchersBytes {
				t.Fatalf("watcher byte stats=%+v", stats)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("watcher did not retain first byte-accounted batch: %d", pending)
		}
		time.Sleep(time.Millisecond)
	}
	// The second image crosses only the watcher byte budget. It must stop this
	// one consumer without waiting for it or weakening the committed write.
	if _, err := collection.InsertOne(context.Background(), makeDocument(2)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrSlowConsumer) {
			t.Fatalf("watcher error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("watcher byte overflow did not close consumer")
	}
	db.feedMu.Lock()
	globalPending := db.pendingWatcherBytes
	db.feedMu.Unlock()
	if globalPending != 0 {
		t.Fatalf("closed watcher retained global bytes=%d", globalPending)
	}
	if stats := db.Stats().Realtime; stats.WatcherPendingBytes != 0 {
		t.Fatalf("closed watcher stats retained bytes=%+v", stats)
	}
}

func TestChangeDispatchAccountsAndClonesChangedPaths(t *testing.T) {
	change := Change{Collection: "items", Operation: UpdateOperation, ChangedPaths: []string{"owner.name", "title"}}
	base, ok := changeDispatchBaseBytes(Change{Collection: change.Collection, Operation: change.Operation})
	if !ok {
		t.Fatal("base change bytes are invalid")
	}
	withPaths, ok := changeDispatchBaseBytes(change)
	if !ok {
		t.Fatal("changed-path bytes are invalid")
	}
	wantAdditional := uint64(len("owner.name") + 8 + len("title") + 8)
	if withPaths != base+wantAdditional {
		t.Fatalf("changed-path bytes=%d want=%d", withPaths, base+wantAdditional)
	}
	copy := cloneChange(change)
	copy.ChangedPaths[0] = "mutated"
	if change.ChangedPaths[0] != "owner.name" {
		t.Fatalf("clone aliases changed paths: original=%v clone=%v", change.ChangedPaths, copy.ChangedPaths)
	}
}
