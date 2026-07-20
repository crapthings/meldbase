package meldbase

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openV2WithCommitCoordinator(t *testing.T, options V2CommitCoordinatorOptions) *DB {
	t.Helper()
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "coordinator.meld2"), OpenOptions{CommitCoordinator: options})
	if err != nil {
		t.Fatal(err)
	}
	if db.commitCoordinator == nil {
		t.Fatal("V2 coordinator was not enabled")
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestV2CommitCoordinatorGroupsPublicInsertMany(t *testing.T) {
	db := openV2WithCommitCoordinator(t, V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Millisecond})
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	coordinator.testBeforeCoalesce = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	initialGeneration := coordinator.store.file.Meta().Generation
	firstID, secondID := DocumentID{1}, DocumentID{2}
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(firstID), "n": Int(1)})
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not begin coalescing")
	}
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(secondID), "n": Int(2)})
		second <- err
	}()
	close(release)
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("coordinated insert timed out")
		}
	}
	if meta := coordinator.store.file.Meta(); meta.CommitSequence != 2 || meta.Generation != initialGeneration+1 {
		t.Fatalf("meta=%+v initial generation=%d", meta, initialGeneration)
	}
	if stats := db.CommitCoordinatorStats(); !stats.Enabled || stats.Pending != 0 || stats.PendingCapacity != 8 ||
		stats.Admitted != 2 || stats.Batches != 1 || stats.GroupedTransactions != 2 || stats.AdmissionRejected != 0 {
		t.Fatalf("coordinator stats=%+v", stats)
	}
	if stats := db.Stats().CommitCoordinator; !stats.Enabled || stats.Admitted != 2 || stats.Batches != 1 || stats.GroupedTransactions != 2 {
		t.Fatalf("DBStats coordinator=%+v", stats)
	}
	for _, id := range []DocumentID{firstID, secondID} {
		if _, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err != nil {
			t.Fatalf("inserted %x missing: %v", id, err)
		}
	}
}

func TestV2CommitCoordinatorGroupsIndependentPublicWriteTransactions(t *testing.T) {
	db := openV2WithCommitCoordinator(t, V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Second})
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	coordinator.testBeforeCoalesce = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	defer func() { coordinator.testBeforeCoalesce = nil }()
	initialGeneration := coordinator.store.file.Meta().Generation
	firstID, secondID := DocumentID{12: 1}, DocumentID{12: 2}
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() {
		first <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			_, err := tx.InsertOne("items", Document{"_id": ID(firstID), "n": Int(1)})
			return err
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("transaction coordinator did not begin coalescing")
	}
	go func() {
		second <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			_, err := tx.InsertOne("items", Document{"_id": ID(secondID), "n": Int(2)})
			return err
		})
	}()
	close(release)
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("coordinated transaction timed out")
		}
	}
	if meta := coordinator.store.file.Meta(); meta.CommitSequence != 2 || meta.Generation != initialGeneration+1 {
		t.Fatalf("meta=%+v initial generation=%d", meta, initialGeneration)
	}
	if stats := db.Stats(); stats.Transactions.Started != 2 || stats.Transactions.Committed != 2 ||
		stats.CommitCoordinator.GroupedTransactions != 2 {
		t.Fatalf("stats=%+v", stats)
	}
	for _, id := range []DocumentID{firstID, secondID} {
		if _, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err != nil {
			t.Fatalf("transactional insert %x missing: %v", id, err)
		}
	}
}

func TestV2CommitCoordinatorGroupsRangeFencedWriteTransactions(t *testing.T) {
	db := openV2WithCommitCoordinator(t, V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Second})
	if _, err := db.Collection("readset").InsertOne(context.Background(), Document{"rank": Int(1)}); err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	coordinator.testBeforeCoalesce = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	defer func() { coordinator.testBeforeCoalesce = nil }()
	firstID, secondID := DocumentID{12: 11}, DocumentID{12: 12}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() {
		first <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			if _, err := tx.Find("readset", query); err != nil {
				return err
			}
			_, err := tx.InsertOne("items", Document{"_id": ID(firstID), "n": Int(1)})
			return err
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("range transaction coordinator did not begin coalescing")
	}
	go func() {
		second <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			if _, err := tx.Find("readset", query); err != nil {
				return err
			}
			_, err := tx.InsertOne("items", Document{"_id": ID(secondID), "n": Int(2)})
			return err
		})
	}()
	close(release)
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("coordinated range transaction timed out")
		}
	}
	if stats := db.CommitCoordinatorStats(); stats.GroupedTransactions == 0 {
		t.Fatalf("range transactions were not grouped: %+v", stats)
	}
}

func TestV2CommitCoordinatorTransactionConflictNeverRerunsCallback(t *testing.T) {
	db := openV2WithCommitCoordinator(t, V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Millisecond})
	items := db.Collection("items")
	id := DocumentID{12: 3}
	if _, err := items.InsertOne(context.Background(), Document{"_id": ID(id), "n": Int(1)}); err != nil {
		t.Fatal(err)
	}
	coordinator := db.commitCoordinator
	initialGeneration := coordinator.store.file.Meta().Generation
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	coordinator.testBeforeCoalesce = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	defer func() { coordinator.testBeforeCoalesce = nil }()
	mutation, err := CompileUpdate(Update{"$inc": map[string]any{"n": 1}})
	if err != nil {
		t.Fatal(err)
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() {
		first <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			return tx.UpdateOne("items", id, mutation)
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first transaction did not begin coalescing")
	}
	go func() {
		second <- db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
			return tx.UpdateOne("items", id, mutation)
		})
	}()
	deadline := time.Now().Add(time.Second)
	for {
		coordinator.mu.Lock()
		pending := len(coordinator.queue)
		coordinator.mu.Unlock()
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("second transaction was not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first transaction=%v", err)
	}
	if err := <-second; !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("second transaction=%v", err)
	}
	if meta := coordinator.store.file.Meta(); meta.CommitSequence != 2 || meta.Generation != initialGeneration+1 {
		t.Fatalf("meta=%+v initial generation=%d", meta, initialGeneration)
	}
	document, err := items.FindOne(context.Background(), Filter{"_id": id})
	if err != nil || !document["n"].Equal(Int(2)) {
		t.Fatalf("document=%+v err=%v", document, err)
	}
}

func TestV2CommitCoordinatorTransactionWaitsForDurableOutcomeAfterCancellation(t *testing.T) {
	db := openV2WithCommitCoordinator(t, V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Millisecond})
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	coordinator.testBeforeCommit = func() {
		close(entered)
		<-release
	}
	defer func() { coordinator.testBeforeCommit = nil }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id := DocumentID{12: 4}
	result := make(chan error, 1)
	go func() {
		result <- db.RunWriteTransaction(ctx, func(tx *WriteTransaction) error {
			_, err := tx.InsertOne("items", Document{"_id": ID(id), "n": Int(4)})
			return err
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("transaction did not reach coordinator commit boundary")
	}
	cancel()
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("admitted transaction result=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted transaction did not await durable outcome")
	}
	if stats := db.CommitCoordinatorStats(); stats.OutcomeUnknown != 0 {
		t.Fatalf("transaction cancellation created an unknown outcome: %+v", stats)
	}
	if _, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err != nil {
		t.Fatalf("durably committed transaction missing: %v", err)
	}
}

func TestV2CommitCoordinatorFallsBackPerRequestOnUniqueConflict(t *testing.T) {
	db := openV2WithCommitCoordinator(t, V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Millisecond})
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"_id": ID(DocumentID{9}), "n": Int(0)}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	coordinator.testBeforeCoalesce = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	initialGeneration := coordinator.store.file.Meta().Generation
	results := make(chan error, 2)
	for _, id := range []DocumentID{{1}, {2}} {
		id := id
		go func() {
			_, err := items.InsertOne(context.Background(), Document{"_id": ID(id), "n": Int(1)})
			results <- err
		}()
		if id == (DocumentID{1}) {
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("coordinator did not begin coalescing")
			}
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		coordinator.mu.Lock()
		pending := len(coordinator.queue)
		coordinator.mu.Unlock()
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("second request was not admitted, pending=%d", pending)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	var successes, duplicates int
	for range 2 {
		select {
		case err := <-results:
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrDuplicateKey):
				duplicates++
			default:
				t.Fatalf("unexpected insert result: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("coordinated conflicting insert timed out")
		}
	}
	if successes != 1 || duplicates != 1 {
		t.Fatalf("successes=%d duplicates=%d", successes, duplicates)
	}
	if meta := coordinator.store.file.Meta(); meta.CommitSequence != 3 || meta.Generation != initialGeneration+1 {
		t.Fatalf("meta=%+v initial generation=%d", meta, initialGeneration)
	}
	if stats := db.Stats(); stats.WritesDisabled || stats.Storage.RejectedTransactions != 1 {
		t.Fatalf("logical conflict stats=%+v", stats)
	}
}

func TestV2CommitCoordinatorBoundsAdmissionAndReportsCanceledOutcome(t *testing.T) {
	db := openV2WithCommitCoordinator(t, V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 2, MaxDelay: time.Millisecond})
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	coordinator.testBeforeCoalesce = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	first := make(chan error, 1)
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(DocumentID{1})})
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not begin coalescing")
	}
	second, third := make(chan error, 1), make(chan error, 1)
	for id, result := range map[DocumentID]chan error{{2}: second, {3}: third} {
		id, result := id, result
		go func() {
			_, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(id)})
			result <- err
		}()
	}
	deadline := time.Now().Add(time.Second)
	for {
		coordinator.mu.Lock()
		pending := len(coordinator.queue)
		coordinator.mu.Unlock()
		if pending == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending admission queue=%d want=2", pending)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(DocumentID{4})}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("overflow error=%v", err)
	}
	if stats := db.CommitCoordinatorStats(); stats.AdmissionRejected != 1 || stats.Pending != 2 || stats.PendingCapacity != 2 {
		t.Fatalf("overflow coordinator stats=%+v", stats)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.Collection("items").InsertOne(canceled, Document{"_id": ID(DocumentID{5})}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-admission cancellation=%v", err)
	}
	close(release)
	for _, result := range []<-chan error{first, second, third} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("admitted request did not complete")
		}
	}
}

func TestV2CommitCoordinatorCancellationAfterAdmissionIsReconciliable(t *testing.T) {
	db := openV2WithCommitCoordinator(t, V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Millisecond})
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	coordinator.testBeforeCommit = func() {
		close(entered)
		<-release
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultIDs := make(chan []DocumentID, 1)
	resultErr := make(chan error, 1)
	id := DocumentID{7}
	go func() {
		ids, err := db.Collection("items").InsertOne(ctx, Document{"_id": ID(id)})
		resultIDs <- []DocumentID{ids}
		resultErr <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator did not reach commit boundary")
	}
	cancel()
	if ids := <-resultIDs; len(ids) != 1 || ids[0] != id {
		t.Fatalf("reconciliation IDs=%v", ids)
	}
	if err := <-resultErr; !errors.Is(err, ErrCommitOutcomeUnknown) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admitted result=%v", err)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err == nil {
			break
		} else if !errors.Is(err, ErrNotFound) || time.Now().After(deadline) {
			t.Fatalf("reconcile lookup=%v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestV2CommitCoordinatorConcurrentWritesPreserveRealtimeAndReplayOrder(t *testing.T) {
	const writers = 16
	db := openV2WithCommitCoordinator(t, V2CommitCoordinatorOptions{
		Enabled: true, MaxBatch: writers, MaxPending: writers * 2, MaxDelay: 20 * time.Millisecond,
	})
	items := db.Collection("items")
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	live, err := items.SubscribeQueryDeltas(context.Background(), query, writers+2)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	initial := live.Initial

	start := make(chan struct{})
	errs := make(chan error, writers)
	var submitted sync.WaitGroup
	for index := range writers {
		index := index
		submitted.Add(1)
		go func() {
			defer submitted.Done()
			<-start
			_, err := items.InsertOne(context.Background(), Document{
				"_id": ID(DocumentID{byte(index + 1)}), "rank": Int(int64(index)),
			})
			errs <- err
		}()
	}
	close(start)
	submitted.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent insert: %v", err)
		}
	}

	state := initial
	for index := range writers {
		delta := receiveQueryDelta(t, live.Deltas)
		if delta.FromToken != state.Token || delta.Token <= state.Token {
			t.Fatalf("live delta %d token chain=%d->%d previous=%d", index, delta.FromToken, delta.Token, state.Token)
		}
		state, err = ApplyQueryDelta(state, delta)
		if err != nil {
			t.Fatalf("apply live delta %d: %v", index, err)
		}
	}
	full, err := items.SnapshotQuery(context.Background(), query)
	if err != nil || !documentSlicesEqual(state.Documents, full.Documents) || state.Token != full.Token {
		t.Fatalf("live state token=%d full=%+v err=%v", state.Token, full, err)
	}

	replay, err := db.OpenQueryReplay(context.Background(), "items", query, initial.Token, writers+2)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	replayed := replay.Initial
	if replayed.Token != initial.Token || !documentSlicesEqual(replayed.Documents, initial.Documents) {
		t.Fatalf("replay initial=%+v live initial=%+v", replayed, initial)
	}
	for index := range writers {
		select {
		case delta := <-replay.Deltas:
			if delta.FromToken != replayed.Token || delta.Token <= replayed.Token {
				t.Fatalf("replay delta %d token chain=%d->%d previous=%d", index, delta.FromToken, delta.Token, replayed.Token)
			}
			replayed, err = ApplyQueryDelta(replayed, delta)
			if err != nil {
				t.Fatalf("apply replay delta %d: %v", index, err)
			}
		case replayErr := <-replay.Errors:
			t.Fatalf("replay error: %v", replayErr)
		case <-time.After(3 * time.Second):
			t.Fatal("replay delta timeout")
		}
	}
	if replayed.Token != full.Token || !documentSlicesEqual(replayed.Documents, full.Documents) {
		t.Fatalf("replayed token=%d full=%+v", replayed.Token, full)
	}
}

func TestV2CommitCoordinatorOptionsAreExplicit(t *testing.T) {
	if _, err := normalizeV2CommitCoordinatorOptions(V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 1}); !errors.Is(err, ErrInvalidCommitCoordinatorOptions) {
		t.Fatalf("invalid batch error=%v", err)
	}
	db, err := Open(filepath.Join(t.TempDir(), "default.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	if db.commitCoordinator != nil {
		t.Fatal("default V2 open unexpectedly enabled coordinator")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	anchored, err := OpenWithOptions(filepath.Join(t.TempDir(), "anchor.meld2"), OpenOptions{
		CommitCoordinator:  V2CommitCoordinatorOptions{Enabled: true},
		RollbackProtection: V2RollbackProtection{AnchorStore: &testRollbackAnchorStore{}, InitializeAnchor: true},
	})
	if err != nil {
		t.Fatalf("coordinator plus rollback anchor error=%v", err)
	}
	if anchored.commitCoordinator == nil {
		t.Fatal("coordinator plus rollback anchor did not enable")
	}
	if err := anchored.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestV2CommitCoordinatorAnchorsWholeGroupAndRejectsRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "anchored-group.meld2")
	anchor, err := NewFileRollbackAnchorStore(filepath.Join(t.TempDir(), "anchored-group.anchor"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(path, OpenOptions{
		CommitCoordinator:  V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Second},
		RollbackProtection: V2RollbackProtection{AnchorStore: anchor, InitializeAnchor: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	coordinator.testBeforeCoalesce = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	initialGeneration := coordinator.store.file.Meta().Generation
	results := make(chan error, 2)
	firstID, secondID := DocumentID{12: 1}, DocumentID{12: 2}
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(firstID)})
		results <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not begin anchored coalescing")
	}
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(secondID)})
		results <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		coordinator.mu.Lock()
		pending := len(coordinator.queue)
		coordinator.mu.Unlock()
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("second anchored insert was not admitted, pending=%d", pending)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("anchored group insert error=%v", err)
		}
	}
	if meta := coordinator.store.file.Meta(); meta.CommitSequence != 2 || meta.Generation != initialGeneration+1 {
		t.Fatalf("anchored group meta=%+v initial generation=%d", meta, initialGeneration)
	}
	assertRollbackAnchorCurrent(t, db, anchor)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, before, 0o600); err != nil {
		t.Fatal(err)
	}
	if rolledBack, err := OpenWithOptions(path, OpenOptions{RollbackProtection: V2RollbackProtection{AnchorStore: anchor}}); rolledBack != nil || !errors.Is(err, ErrRollbackDetected) {
		t.Fatalf("rollback db=%v err=%v", rolledBack, err)
	}
}

func TestV2CommitCoordinatorAnchorAcknowledgementFailureIsFailStop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "anchored-group-failure.meld2")
	seed, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	anchor := &testRollbackAnchorStore{anchor: RollbackAnchor{
		DatabaseID: seed.DatabaseIdentity(), MinimumGeneration: seed.Stats().Storage.Generation,
	}, exists: true}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(path, OpenOptions{
		CommitCoordinator:  V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Second},
		RollbackProtection: V2RollbackProtection{AnchorStore: anchor},
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	coordinator.testBeforeCoalesce = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	results := make(chan error, 2)
	firstID, secondID := DocumentID{13: 1}, DocumentID{13: 2}
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(firstID)})
		results <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not begin failure coalescing")
	}
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(secondID)})
		results <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		coordinator.mu.Lock()
		pending := len(coordinator.queue)
		coordinator.mu.Unlock()
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("second failure insert was not admitted, pending=%d", pending)
		}
		time.Sleep(time.Millisecond)
	}
	anchor.saveThenErr = errors.New("injected anchor acknowledgement loss")
	close(release)
	for range 2 {
		if err := <-results; !errors.Is(err, ErrDurability) {
			t.Fatalf("anchored group failure error=%v", err)
		}
	}
	if stats := db.Stats(); stats.CommitSequence != 0 || !stats.WritesDisabled || stats.Storage.RollbackAnchorFailures != 1 {
		t.Fatalf("anchored group fail-stop stats=%+v", stats)
	}
	if anchor.anchor.MinimumCommitSequence != 2 {
		t.Fatalf("anchor did not retain durable group=%+v", anchor.anchor)
	}
	if err := db.Close(); !errors.Is(err, ErrDurability) {
		t.Fatalf("close error=%v", err)
	}
	anchor.saveThenErr = nil
	reopened, err := OpenWithOptions(path, OpenOptions{RollbackProtection: V2RollbackProtection{AnchorStore: anchor}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if stats := reopened.Stats(); stats.CommitSequence != 2 || stats.WritesDisabled {
		t.Fatalf("reopened stats=%+v", stats)
	}
	for _, id := range []DocumentID{{13: 1}, {13: 2}} {
		if _, err := reopened.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err != nil {
			t.Fatalf("recovered grouped document %x error=%v", id, err)
		}
	}
}

func TestV2DuplicateDocumentIDIsLogicalAndDoesNotDisableWrites(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "duplicate-id.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"_id": ID(DocumentID{1})}); err != nil {
		t.Fatal(err)
	}
	if _, err := items.InsertOne(context.Background(), Document{"_id": ID(DocumentID{1})}); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("duplicate ID error=%v", err)
	}
	if _, err := items.InsertOne(context.Background(), Document{"_id": ID(DocumentID{2})}); err != nil {
		t.Fatalf("logical duplicate disabled subsequent write: %v", err)
	}
	if stats := db.Stats(); stats.WritesDisabled || stats.CommitSequence != 2 {
		t.Fatalf("stats after duplicate=%+v", stats)
	}
}

func TestV2CommitCoordinatorCloseDrainsOwnedGroupBeforeFileClose(t *testing.T) {
	db := openV2WithCommitCoordinator(t, V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Millisecond})
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	coordinator.testBeforeCommit = func() {
		close(entered)
		<-release
	}
	inserted := make(chan error, 1)
	go func() {
		_, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(DocumentID{8})})
		inserted <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not own insert before close")
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before owned group was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-inserted:
		if err != nil {
			t.Fatalf("owned insert result=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("owned insert did not complete")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close result=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not complete")
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"_id": ID(DocumentID{9})}); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after close=%v", err)
	}
}

func TestV2CommitCoordinatorGroupsIndependentUpdateAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutation-group.meld2")
	seed, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	firstID, secondID := DocumentID{1}, DocumentID{2}
	if _, err := seed.Collection("items").InsertMany(context.Background(), []Document{
		{"_id": ID(firstID), "n": Int(1)}, {"_id": ID(secondID), "n": Int(2)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(path, OpenOptions{CommitCoordinator: V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	coordinator.testBeforeCoalesce = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	initialGeneration := coordinator.store.file.Meta().Generation
	updated, deleted := make(chan error, 1), make(chan error, 1)
	go func() {
		result, err := db.Collection("items").UpdateOne(context.Background(), Filter{"_id": firstID}, Update{"$inc": map[string]any{"n": int64(10)}})
		if err == nil && (result.MatchedCount != 1 || result.ModifiedCount != 1) {
			err = errors.New("unexpected update result")
		}
		updated <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not begin mutation coalescing")
	}
	go func() {
		result, err := db.Collection("items").DeleteOne(context.Background(), Filter{"_id": secondID})
		if err == nil && result.DeletedCount != 1 {
			err = errors.New("unexpected delete result")
		}
		deleted <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		coordinator.mu.Lock()
		pending := len(coordinator.queue)
		coordinator.mu.Unlock()
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delete was not admitted, pending=%d", pending)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	for _, result := range []<-chan error{updated, deleted} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("grouped mutation timed out")
		}
	}
	if meta := coordinator.store.file.Meta(); meta.CommitSequence != 3 || meta.Generation != initialGeneration+1 {
		t.Fatalf("meta=%+v initial generation=%d", meta, initialGeneration)
	}
	if got, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": firstID}); err != nil || !got["n"].Equal(Int(11)) {
		t.Fatalf("updated document=%+v err=%v", got, err)
	}
	if _, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": secondID}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted document lookup=%v", err)
	}
}

func TestV2CommitCoordinatorReevaluatesConflictingUpdatesInAdmissionOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutation-conflict.meld2")
	seed, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := DocumentID{5}
	if _, err := seed.Collection("items").InsertOne(context.Background(), Document{"_id": ID(id), "n": Int(0)}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(path, OpenOptions{CommitCoordinator: V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coordinator := db.commitCoordinator
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	coordinator.testBeforeCoalesce = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	initialGeneration := coordinator.store.file.Meta().Generation
	results := make(chan error, 2)
	for index := 0; index < 2; index++ {
		go func() {
			result, err := db.Collection("items").UpdateOne(context.Background(), Filter{"_id": id}, Update{"$inc": map[string]any{"n": int64(1)}})
			if err == nil && (result.MatchedCount != 1 || result.ModifiedCount != 1) {
				err = errors.New("unexpected update result")
			}
			results <- err
		}()
		if index == 0 {
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("coordinator did not begin update coalescing")
			}
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		coordinator.mu.Lock()
		pending := len(coordinator.queue)
		coordinator.mu.Unlock()
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("second update was not admitted, pending=%d", pending)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("conflicting update timed out")
		}
	}
	// The speculative group had no final Meta due to its stale second read;
	// fallback then committed the two original UpdateOne calls serially.
	if meta := coordinator.store.file.Meta(); meta.CommitSequence != 3 || meta.Generation != initialGeneration+2 {
		t.Fatalf("meta=%+v initial generation=%d", meta, initialGeneration)
	}
	if got, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err != nil || !got["n"].Equal(Int(2)) {
		t.Fatalf("serialized increments document=%+v err=%v", got, err)
	}
}

func TestV2CommitCoordinatorMutationCancellationReturnsReconciliationResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutation-cancel.meld2")
	seed, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := DocumentID{6}
	if _, err := seed.Collection("items").InsertOne(context.Background(), Document{"_id": ID(id), "n": Int(1)}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithOptions(path, OpenOptions{CommitCoordinator: V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	db.commitCoordinator.testBeforeCommit = func() {
		close(entered)
		<-release
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh, errCh := make(chan UpdateResult, 1), make(chan error, 1)
	go func() {
		result, err := db.Collection("items").UpdateOne(ctx, Filter{"_id": id}, Update{"$inc": map[string]any{"n": int64(1)}})
		resultCh <- result
		errCh <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not reach mutation commit boundary")
	}
	cancel()
	result := <-resultCh
	if result.MatchedCount != 1 || result.ModifiedCount != 1 {
		t.Fatalf("uncertain mutation result=%+v", result)
	}
	if err := <-errCh; !errors.Is(err, ErrCommitOutcomeUnknown) || !errors.Is(err, context.Canceled) {
		t.Fatalf("uncertain mutation error=%v", err)
	}
	if stats := db.CommitCoordinatorStats(); stats.OutcomeUnknown != 1 {
		t.Fatalf("uncertain mutation coordinator stats=%+v", stats)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for {
		got, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": id})
		if err == nil && got["n"].Equal(Int(2)) {
			break
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			t.Fatalf("mutation reconciliation lookup=%v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("mutation was not durably reconciled, document=%+v err=%v", got, err)
		}
		time.Sleep(time.Millisecond)
	}
}
