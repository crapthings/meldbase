package database

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	storage "github.com/crapthings/meldbase/internal/storage"
)

func TestOpenPersistsCRUDIndexesOrderAndIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "public-store.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	collection := db.Collection("items")
	first, second := DocumentID{2}, DocumentID{1}
	if _, err := collection.InsertMany(context.Background(), []Document{
		{"_id": ID(first), "value": Int(10), "group": String("same")},
		{"_id": ID(second), "value": Int(20), "group": String("same")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := collection.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if err := collection.CreateIndex(context.Background(), "by_group", []IndexField{{Field: "group", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	if result, err := collection.UpdateOne(context.Background(), Filter{"_id": first}, Update{"$set": map[string]any{"value": int64(30)}}); err != nil || result.ModifiedCount != 1 {
		t.Fatalf("update=%+v err=%v", result, err)
	}
	identity := db.DatabaseIdentity()
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.DatabaseIdentity() != identity || reopened.Stats().CommitSequence != 4 {
		t.Fatalf("identity/token changed identity=%x token=%d", reopened.DatabaseIdentity(), reopened.Stats().CommitSequence)
	}
	all, err := reopened.Collection("items").Find(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	documents, err := all.All(context.Background())
	if err != nil || len(documents) != 2 {
		t.Fatalf("documents=%+v err=%v", documents, err)
	}
	firstID, _ := documents[0].ID()
	secondID, _ := documents[1].ID()
	if firstID != first || secondID != second {
		t.Fatalf("insertion order=%v,%v", firstID, secondID)
	}
	cursor, err := reopened.Collection("items").Find(context.Background(), Filter{"value": int64(30)})
	if err != nil {
		t.Fatal(err)
	}
	found, err := cursor.All(context.Background())
	if err != nil || len(found) != 1 {
		t.Fatalf("indexed result=%+v err=%v", found, err)
	}
	explain, err := reopened.Collection("items").Explain(context.Background(), Filter{"value": int64(30)})
	if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "by_value" {
		t.Fatalf("explain=%+v err=%v", explain, err)
	}
	if _, err := reopened.Collection("items").InsertOne(context.Background(), Document{"value": Int(30)}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate key error=%v", err)
	}
	if result, err := reopened.Collection("items").DeleteOne(context.Background(), Filter{"_id": second}); err != nil || result.DeletedCount != 1 {
		t.Fatalf("delete=%+v err=%v", result, err)
	}
}

func TestOpenPersistsCompoundIndexCRUDAndPlanner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compound-store.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	first, second, other, missing := DocumentID{1}, DocumentID{2}, DocumentID{3}, DocumentID{4}
	if _, err := items.InsertMany(context.Background(), []Document{
		{"_id": ID(first), "workspace": String("a"), "score": Int(8)},
		{"_id": ID(second), "workspace": String("a"), "score": Int(9)},
		{"_id": ID(other), "workspace": String("b"), "score": Int(8)},
		{"_id": ID(missing), "workspace": String("a")},
	}); err != nil {
		t.Fatal(err)
	}
	fields := []IndexField{{Field: "workspace", Order: 1}, {Field: "score", Order: -1}}
	if err := items.CreateIndex(context.Background(), "workspace_score", fields, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if db.durability.(*durableStore).file.Meta().RequiredFeatures&storage.RequiredFeatureCompoundIndexes == 0 {
		t.Fatal("compound index did not negotiate its required storage feature")
	}
	if got := queryIDs(t, items, Filter{"workspace": "a"}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{first, second, missing}) {
		t.Fatalf("prefix query IDs = %v", got)
	}
	if explain, err := items.Explain(context.Background(), Filter{"workspace": "a"}); err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "workspace_score" {
		t.Fatalf("explain=%+v err=%v", explain, err)
	}
	if got := queryIDs(t, items, Filter{"workspace": "a", "score": map[string]any{"$gt": int64(8), "$lte": int64(9)}}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{second}) {
		t.Fatalf("descending range IDs = %v", got)
	}
	if _, err := items.InsertOne(context.Background(), Document{"workspace": String("a"), "score": Int(8)}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate tuple error = %v", err)
	}
	partial, err := items.InsertOne(context.Background(), Document{"workspace": String("a")})
	if err != nil {
		t.Fatalf("second partial tuple conflicted: %v", err)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": second}, Update{"$set": map[string]any{"score": int64(7)}}); err != nil {
		t.Fatal(err)
	}
	if got := queryIDs(t, items, Filter{"workspace": "a", "score": int64(9)}, QueryOptions{}); len(got) != 0 {
		t.Fatalf("old tuple remained after update: %v", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	items = reopened.Collection("items")
	if got := queryIDs(t, items, Filter{"workspace": "a"}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{first, second, missing, partial}) {
		t.Fatalf("reopened prefix IDs = %v", got)
	}
	if got := queryIDs(t, items, Filter{"workspace": "a", "score": int64(7)}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{second}) {
		t.Fatalf("reopened tuple IDs = %v", got)
	}
	snapshot, err := reopened.durability.(*durableStore).file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	meta, exists, err := snapshot.IndexMeta("items", "workspace_score")
	_ = snapshot.Close()
	if err != nil || !exists || meta.FieldPath != "workspace" || !meta.Unique || !reflect.DeepEqual(meta.Fields, []storage.IndexField{
		{Path: "workspace", Direction: 1}, {Path: "score", Direction: -1},
	}) {
		t.Fatalf("compound index meta=%+v exists=%t err=%v", meta, exists, err)
	}
	if _, err := items.DeleteOne(context.Background(), Filter{"_id": first}); err != nil {
		t.Fatal(err)
	}
	if _, err := items.InsertOne(context.Background(), Document{"workspace": String("a"), "score": Int(8)}); err != nil {
		t.Fatalf("deleted tuple was not released after reopen: %v", err)
	}
}

func TestOpenProvidesGapFreeQueryReplay(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "public-store-replay.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"value": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	query, _ := CompileQuery(Filter{}, QueryOptions{})
	replay, err := db.OpenQueryReplay(context.Background(), "items", query, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	stats := db.Stats()
	if stats.Storage.Engine != "current" || stats.Storage.PageSize != storage.PageSize ||
		stats.Storage.PhysicalPages <= 2 || stats.Storage.CommitSequence != 1 ||
		stats.Storage.CommitAttempts != 1 || stats.Storage.CommittedTransactions != 1 ||
		stats.Storage.RejectedTransactions != 0 || stats.Storage.CommitMaxLatency <= 0 ||
		stats.Storage.ActiveReplayLeases != 1 {
		t.Fatalf(" storage stats=%+v", stats.Storage)
	}
	assertSnapshotIDs(t, replay.Initial, []DocumentID{id})
	if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": int64(2)}}); err != nil {
		t.Fatal(err)
	}
	select {
	case delta := <-replay.Deltas:
		if delta.FromToken != 1 || delta.Token != 2 || len(delta.Operations) != 1 || delta.Operations[0].Kind != QueryDeltaChange {
			t.Fatalf("delta=%+v", delta)
		}
	case err := <-replay.Errors:
		t.Fatalf("replay error=%v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("replay timeout")
	}
	stats = db.Stats()
	if stats.Storage.CommitAttempts != 2 || stats.Storage.CommittedTransactions != 2 ||
		stats.Storage.CommitNanos < uint64(stats.Storage.CommitMaxLatency) {
		t.Fatalf(" commit stats=%+v", stats.Storage)
	}
}

func TestColdReactiveSubscriptionPinsSnapshotWithoutBlockingWriter(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "cold-reactive.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := db.durability.(*durableStore)
	pinned, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	store.testQuerySnapshotHook = func() {
		once.Do(func() {
			close(pinned)
			<-release
		})
	}
	defer func() { store.testQuerySnapshotHook = nil }()
	type subscriptionResult struct {
		subscription *QueryDeltaSubscription
		err          error
	}
	result := make(chan subscriptionResult, 1)
	go func() {
		subscription, err := items.SubscribeQueryDeltas(context.Background(), query, 2)
		result <- subscriptionResult{subscription: subscription, err: err}
	}()
	select {
	case <-pinned:
	case <-time.After(3 * time.Second):
		t.Fatal("cold subscription did not pin storage snapshot")
	}
	written := make(chan error, 1)
	go func() {
		_, err := items.InsertOne(context.Background(), Document{"value": Int(2)})
		written <- err
	}()
	select {
	case err := <-written:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("write waited for cold reactive snapshot scan")
	}
	close(release)
	select {
	case outcome := <-result:
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		defer outcome.subscription.Close()
		if outcome.subscription.Initial.Token != db.Stats().CommitSequence || len(outcome.subscription.Initial.Documents) != 2 {
			t.Fatalf("initial=%+v sequence=%d", outcome.subscription.Initial, db.Stats().CommitSequence)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cold subscription did not retry to a current snapshot")
	}
}

func TestWarmReactiveRecomputePinsSnapshotWithoutBlockingWriter(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "warm-reactive.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := items.SubscribeQueryDeltas(context.Background(), query, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()

	store := db.durability.(*durableStore)
	pinned, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	store.testQuerySnapshotHook = func() {
		once.Do(func() {
			close(pinned)
			<-release
		})
	}
	defer func() { store.testQuerySnapshotHook = nil }()
	recomputed := make(chan struct{})
	go func() {
		db.reactive.fullRecomputeCollection("items")
		close(recomputed)
	}()
	select {
	case <-pinned:
	case <-time.After(3 * time.Second):
		t.Fatal("warm recompute did not pin storage snapshot")
	}
	written := make(chan error, 1)
	go func() {
		_, err := items.InsertOne(context.Background(), Document{"value": Int(2)})
		written <- err
	}()
	select {
	case err := <-written:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("write waited for warm reactive snapshot scan")
	}
	close(release)
	select {
	case <-recomputed:
	case <-time.After(5 * time.Second):
		t.Fatal("warm reactive recompute did not converge")
	}

	state := subscription.Initial
	deadline := time.After(3 * time.Second)
	for state.Token < db.Stats().CommitSequence {
		select {
		case delta := <-subscription.Deltas:
			state, err = ApplyQueryDelta(state, delta)
			if err != nil {
				t.Fatal(err)
			}
		case err := <-subscription.Errors:
			t.Fatalf("subscription error=%v", err)
		case <-deadline:
			t.Fatalf("subscription did not converge: state=%+v sequence=%d", state, db.Stats().CommitSequence)
		}
	}
	if state.Token != db.Stats().CommitSequence || len(state.Documents) != 2 {
		t.Fatalf("state=%+v sequence=%d", state, db.Stats().CommitSequence)
	}
}

func TestUpdateChangedPathsReachWatchersAndDurableCommitLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "changed-paths.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	id, err := items.InsertOne(context.Background(), Document{
		"title": String("before"), "score": Int(1), "owner": Object(Document{"name": String("old")}),
	})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	events, eventErrors, err := db.WatchChanges(context.Background(), "items", 1)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": id}, Update{
		"$inc": map[string]any{"score": 1},
		"$set": map[string]any{"title": "after", "owner.name": "new"},
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	var watched ChangeBatch
	select {
	case watched = <-events:
	case err := <-eventErrors:
		_ = db.Close()
		t.Fatalf("watch error=%v", err)
	case <-time.After(3 * time.Second):
		_ = db.Close()
		t.Fatal("watch did not receive update")
	}
	wantPaths := []string{"owner.name", "score", "title"}
	if len(watched.Changes) != 1 || !reflect.DeepEqual(watched.Changes[0].ChangedPaths, wantPaths) {
		_ = db.Close()
		t.Fatalf("watch batch=%+v want paths=%v", watched, wantPaths)
	}
	// Watcher delivery owns its metadata just as it owns document images.
	watched.Changes[0].ChangedPaths[0] = "mutated"
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	store := reopened.durability.(*durableStore)
	cursor, err := store.file.OpenCommitCursor(1)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	commit, ok, err := cursor.Next()
	if err != nil || !ok || commit.Sequence != 2 || len(commit.Changes) != 1 || !reflect.DeepEqual(commit.Changes[0].ChangedPaths, wantPaths) {
		t.Fatalf("commit=%+v ok=%t err=%v want paths=%v", commit, ok, err, wantPaths)
	}
}

func TestOpenStatsTrackRejectedTransactionAndResetOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "public-store-stats.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	collection := db.Collection("items")
	if _, err := collection.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	if err := collection.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	conflictingID := DocumentID{9}
	conflicting := Document{"_id": ID(conflictingID), "value": Int(1)}
	store := db.durability.(*durableStore)
	if err := store.appendDBCommit(context.Background(), db, 3, []Change{{
		Collection: "items", Operation: InsertOperation, DocumentID: conflictingID, After: &conflicting,
	}}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("backend duplicate error=%v", err)
	}
	stats := db.Stats().Storage
	if stats.CommitAttempts != 3 || stats.CommittedTransactions != 2 || stats.RejectedTransactions != 1 {
		t.Fatalf(" rejected stats=%+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	stats = reopened.Stats().Storage
	if stats.Engine != "current" || stats.CommitSequence != 2 || stats.CommitAttempts != 0 ||
		stats.CommittedTransactions != 0 || stats.RejectedTransactions != 0 {
		t.Fatalf("reopened  stats=%+v", stats)
	}
}

func TestOpenDetectsCorruptPersistentIndexWhenRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store-index-audit.meld2")
	file, _, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := DocumentID{1}
	encoded, err := encodeStoredDocument(Document{"_id": ID(id), "value": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(storage.DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []storage.DocumentMutation{{
		Collection: "items", DocumentID: [16]byte(id), Operation: storage.DocumentInsert, Document: encoded,
	}}}); err != nil {
		t.Fatal(err)
	}
	wrongKey, _ := encodeIndexKey(Int(999))
	if _, err := file.ApplyCreateIndex(storage.CreateIndexTransaction{
		TransactionID: [16]byte{2}, Collection: "items", Name: "by_value", FieldPath: "value",
		Entries: []storage.IndexEntry{{Key: wrongKey, DocumentID: [16]byte(id)}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyFile(context.Background(), path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("semantic verifier accepted wrong index key: %v", err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").Find(context.Background(), Filter{"value": int64(999)}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt index query error=%v", err)
	}
}

func TestOpenQueriesUsePinnedStorageSnapshotNotMemoryMirror(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "storage-query.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	first, second, third, fourth := DocumentID{9}, DocumentID{1}, DocumentID{7}, DocumentID{2}
	if _, err := collection.InsertMany(context.Background(), []Document{
		{"_id": ID(first), "n": Int(3), "group": String("same"), "visible": Bool(true)},
		{"_id": ID(second), "n": Int(1), "group": String("same"), "visible": Bool(false)},
		{"_id": ID(third), "n": Int(2), "group": String("same"), "visible": Bool(true)},
		{"_id": ID(fourth), "n": Int(4), "group": String("other"), "visible": Bool(true)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := collection.CreateIndex(context.Background(), "by_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if err := collection.CreateIndex(context.Background(), "by_group", []IndexField{{Field: "group", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	for name, state := range db.collections["items"].indexes {
		if state == nil || state.tree != nil {
			t.Fatalf(" index %s retained a process-local B+Tree", name)
		}
	}
	if _, err := collection.InsertOne(context.Background(), Document{"n": Int(3), "group": String("duplicate")}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("storage-backed unique rejection=%v", err)
	}
	fifth := DocumentID{5}
	if _, err := collection.InsertOne(context.Background(), Document{
		"_id": ID(fifth), "n": Int(5), "group": String("same"), "visible": Bool(true),
	}); err != nil {
		t.Fatal(err)
	}

	// Remove every process-local document/index. Any successful query below must
	// therefore come from the pinned roots rather than the compatibility mirror.
	db.mu.Lock()
	db.collections["items"] = newCollectionData()
	db.mu.Unlock()

	assertQueryIDs := func(filter Filter, options QueryOptions, expected []DocumentID, stage, index string) {
		t.Helper()
		actual := queryIDs(t, collection, filter, options)
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("filter=%v actual=%v expected=%v", filter, actual, expected)
		}
		explain, err := collection.Explain(context.Background(), filter)
		if err != nil || explain.Stage != stage || explain.IndexName != index {
			t.Fatalf("filter=%v explain=%+v err=%v", filter, explain, err)
		}
		if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
			t.Fatalf("query leaked %d storage reader pins", readers)
		}
	}
	assertQueryIDs(Filter{"_id": third}, QueryOptions{}, []DocumentID{third}, "ID_LOOKUP", "_id")
	assertQueryIDs(Filter{"group": "same"}, QueryOptions{}, []DocumentID{first, second, third, fifth}, "IXSCAN", "by_group")
	assertQueryIDs(Filter{"$and": []Filter{{"group": "same"}, {"visible": true}}}, QueryOptions{}, []DocumentID{first, third, fifth}, "IXSCAN", "by_group")
	assertQueryIDs(Filter{"n": map[string]any{"$gt": int64(1), "$lte": int64(3)}}, QueryOptions{}, []DocumentID{first, third}, "IXSCAN", "by_n")
	limit := 2
	assertQueryIDs(Filter{"n": map[string]any{"$gte": int64(1), "$lt": int64(4)}}, QueryOptions{
		Sort: []SortField{{Path: "n", Direction: 1}}, Skip: 1, Limit: &limit,
	}, []DocumentID{third, first}, "IXSCAN", "by_n")
	assertQueryIDs(Filter{"visible": true}, QueryOptions{}, []DocumentID{first, third, fourth, fifth}, "COLLSCAN", "")
}

func TestOpenCreateIndexAndMutationsDoNotReadDocumentMirror(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage-mutations.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	collection := db.Collection("items")
	first, second, third := DocumentID{1}, DocumentID{2}, DocumentID{3}
	if _, err := collection.InsertMany(context.Background(), []Document{
		{"_id": ID(first), "n": Int(1), "group": String("same")},
		{"_id": ID(second), "n": Int(2), "group": String("same")},
		{"_id": ID(third), "n": Int(3), "group": String("other")},
	}); err != nil {
		t.Fatal(err)
	}

	// Retain collection metadata, but remove every decoded document and order
	// entry. CreateIndex must enumerate the pinned Primary tree instead.
	db.mu.Lock()
	data := db.collections["items"]
	data.documents = make(map[DocumentID]Document)
	data.order = nil
	db.mu.Unlock()
	if err := collection.CreateIndex(context.Background(), "by_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if actual := queryIDs(t, collection, Filter{"n": int64(2)}, QueryOptions{}); !reflect.DeepEqual(actual, []DocumentID{second}) {
		t.Fatalf("storage-built index result=%v", actual)
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
		t.Fatalf("CreateIndex leaked %d reader pins", readers)
	}

	// Selection for both mutations comes from a storage snapshot. The small
	// process-local structure supplies only index definitions to the commit.
	result, err := collection.UpdateMany(context.Background(), Filter{"group": "same"}, Update{"$set": map[string]any{"active": true}})
	if err != nil || result.MatchedCount != 2 || result.ModifiedCount != 2 {
		t.Fatalf("storage-backed update=%+v err=%v", result, err)
	}
	if _, err := collection.UpdateOne(context.Background(), Filter{"_id": second}, Update{"$set": map[string]any{"n": int64(1)}}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("storage-backed unique update error=%v", err)
	}
	deleted, err := collection.DeleteOne(context.Background(), Filter{"n": int64(2)})
	if err != nil || deleted.DeletedCount != 1 {
		t.Fatalf("storage-backed delete=%+v err=%v", deleted, err)
	}
	if actual := queryIDs(t, collection, Filter{"active": true}, QueryOptions{}); !reflect.DeepEqual(actual, []DocumentID{first}) {
		t.Fatalf("post-mutation result=%v", actual)
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
		t.Fatalf("mutations leaked %d reader pins", readers)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if actual := queryIDs(t, reopened.Collection("items"), Filter{}, QueryOptions{}); !reflect.DeepEqual(actual, []DocumentID{first, third}) {
		t.Fatalf("reopened result=%v", actual)
	}
}

func TestOpenReactiveInitializationAndResyncDoNotReadDocumentMirror(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "storage-reactive.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	if _, err := collection.InsertMany(context.Background(), []Document{
		{"rank": Int(3), "visible": Bool(true)},
		{"rank": Int(1), "visible": Bool(false)},
		{"rank": Int(2), "visible": Bool(true)},
	}); err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	data := db.collections["items"]
	data.documents = make(map[DocumentID]Document)
	data.order = nil
	db.mu.Unlock()

	query, err := CompileQuery(Filter{"visible": true}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	full, err := collection.SnapshotQuery(context.Background(), query)
	if err != nil || len(full.Documents) != 2 {
		t.Fatalf("storage snapshot=%+v err=%v", full, err)
	}
	subscription, err := collection.SubscribeQuery(context.Background(), query, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	initial := receiveSnapshot(t, subscription.Snapshots)
	if initial.Token != full.Token || !documentSlicesEqual(initial.Documents, full.Documents) {
		t.Fatalf("reactive initial=%+v full=%+v", initial, full)
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
		t.Fatalf("initialization leaked %d reader pins", readers)
	}

	// Force the bounded queue to abandon incremental application. The fallback
	// must rebuild from one current storage snapshot, including the original
	// documents that are absent from the compatibility mirror.
	db.reactive.mu.Lock()
	db.reactive.maxChanges = 1
	db.reactive.mu.Unlock()
	if _, err := collection.InsertMany(context.Background(), []Document{
		{"rank": Int(4), "visible": Bool(true)},
		{"rank": Int(5), "visible": Bool(true)},
	}); err != nil {
		t.Fatal(err)
	}
	resynced := receiveSnapshot(t, subscription.Snapshots)
	if resynced.Token != db.Stats().CommitSequence || len(resynced.Documents) != 4 {
		t.Fatalf("reactive resync=%+v token=%d", resynced, db.Stats().CommitSequence)
	}
	for index, expected := range []int64{2, 3, 4, 5} {
		actual, ok := resynced.Documents[index]["rank"].Int64()
		if !ok || actual != expected {
			t.Fatalf("resync rank[%d]=%d/%t expected=%d", index, actual, ok, expected)
		}
	}
	waitForRealtimeStats(t, db, func(stats RealtimeStats) bool {
		return stats.QueueOverflows == 1 && stats.FullViewRecomputes == 1
	})
	if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
		t.Fatalf("resync leaked %d reader pins", readers)
	}
}

func TestOpenKeepsLargeCollectionMetadataOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata-only.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	const count = defaultDocumentCacheEntries + 1
	documents := make([]Document, count)
	for index := range documents {
		var id DocumentID
		binary.BigEndian.PutUint64(id[8:], uint64(index+1))
		documents[index] = Document{"_id": ID(id), "ordinal": Int(int64(index))}
	}
	if _, err := db.Collection("items").InsertMany(context.Background(), documents); err != nil {
		t.Fatal(err)
	}
	data := db.collections["items"]
	if data == nil || len(data.documents) != 0 || len(data.order) != 0 {
		t.Fatalf("live  retained document mirror: documents=%d order=%d", len(data.documents), len(data.order))
	}
	if stats := db.Stats(); stats.Documents != count || stats.Collections != 1 || stats.Storage.Documents != count {
		t.Fatalf("live physical counts=%+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	data = reopened.collections["items"]
	if data == nil || len(data.documents) != 0 || len(data.order) != 0 {
		t.Fatalf("reopened  retained document mirror: documents=%d order=%d", len(data.documents), len(data.order))
	}
	stats := reopened.Stats()
	if stats.Documents != count || stats.Collections != 1 || stats.Storage.Documents != count || stats.Storage.Collections != 1 {
		t.Fatalf("reopened physical counts=%+v", stats)
	}
	if stats.Storage.DocumentCache.Entries != 0 || stats.Storage.DocumentCache.Bytes != 0 || stats.Storage.DocumentCache.Misses != 0 {
		t.Fatalf("Open decoded documents into cache: %+v", stats.Storage.DocumentCache)
	}
}

func TestOpenCreateIndexScanHonorsContextBeforePublication(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "cancel-index.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	documents := make([]Document, 100)
	for index := range documents {
		documents[index] = Document{"value": Int(int64(index))}
	}
	if _, err := collection.InsertMany(context.Background(), documents); err != nil {
		t.Fatal(err)
	}
	before := db.Stats().CommitSequence
	ctx := newCancelAfterChecksContext(5)
	if err := collection.CreateIndex(ctx, "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateIndex cancellation=%v", err)
	}
	if db.Stats().CommitSequence != before || len(db.collections["items"].indexes) != 0 {
		t.Fatalf("cancelled index published: token=%d indexes=%d", db.Stats().CommitSequence, len(db.collections["items"].indexes))
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
		t.Fatalf("cancelled index leaked %d reader pins", readers)
	}
}

func TestOpenCreateIndexScansWithoutBlockingWritesAndRetriesSnapshot(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "optimistic-index.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	store := db.durability.(*durableStore)
	snapshotOpened := make(chan struct{})
	releaseScan := make(chan struct{})
	var once sync.Once
	store.testIndexBuildSnapshotHook = func() {
		once.Do(func() {
			close(snapshotOpened)
			<-releaseScan
		})
	}
	buildDone := make(chan error, 1)
	go func() {
		buildDone <- items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true})
	}()
	select {
	case <-snapshotOpened:
	case <-time.After(3 * time.Second):
		t.Fatal("index scan did not start")
	}
	if stats := db.Stats().IndexBuilds; stats.Active != 1 || stats.Attempts != 1 {
		t.Fatalf("active index build not observable: %+v", stats)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := items.InsertOne(context.Background(), Document{"value": Int(2)})
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("concurrent write=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("index snapshot held the database writer lock")
	}
	close(releaseScan)
	select {
	case err := <-buildDone:
		if err != nil {
			t.Fatalf("CreateIndex=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("index build did not finish")
	}
	if got := queryIDs(t, items, Filter{}, QueryOptions{}); len(got) != 2 {
		t.Fatalf("documents=%v", got)
	}
	explain, err := items.Explain(context.Background(), Filter{"value": int64(2)})
	if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "by_value" {
		t.Fatalf("explain=%+v err=%v", explain, err)
	}
	stats := db.Stats().IndexBuilds
	if stats.Attempts != 1 || stats.Completed != 1 || stats.Failed != 0 || stats.Retries != 1 || stats.Conflicts != 1 {
		t.Fatalf("index build stats=%+v", stats)
	}
}

func TestOpenCreateIndexBoundsContinuousWriteConflictsWithoutPublication(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "conflicted-index.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	store := db.durability.(*durableStore)
	var inserted int64 = 1
	store.testIndexBuildSnapshotHook = func() {
		inserted++
		if _, err := items.InsertOne(context.Background(), Document{"value": Int(inserted)}); err != nil {
			t.Errorf("concurrent write=%v", err)
		}
	}
	err = items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true})
	if !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("CreateIndex conflict=%v", err)
	}
	stats := db.Stats()
	if stats.WritesDisabled || stats.Indexes != 0 || stats.CommitSequence != 4 ||
		stats.IndexBuilds.Attempts != 1 || stats.IndexBuilds.Completed != 0 || stats.IndexBuilds.Failed != 1 ||
		stats.IndexBuilds.Retries != 2 || stats.IndexBuilds.Conflicts != 3 {
		t.Fatalf("stats=%+v", stats)
	}
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(5)}); err != nil {
		t.Fatalf("write after index conflict=%v", err)
	}
}

func TestOpenOversizedIndexKeyIsValidationErrorNotFatal(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "oversized-index-key.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	if _, err := collection.InsertOne(context.Background(), Document{"value": String(strings.Repeat("x", storage.MaxSecondaryScalarKeyBytes+1))}); err != nil {
		t.Fatal(err)
	}
	if err := collection.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("oversized index key error=%v", err)
	}
	if db.fatalErr != nil {
		t.Fatalf("validation error poisoned writes: %v", db.fatalErr)
	}
	if _, err := collection.InsertOne(context.Background(), Document{"value": String("small")}); err != nil {
		t.Fatalf("write after validation error=%v", err)
	}
}

func TestOpenFindStreamsInsertionOrderCOLLSCANAndReleasesPins(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "streaming-cursor.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	first, second, third, fourth := DocumentID{15: 9}, DocumentID{15: 1}, DocumentID{15: 7}, DocumentID{15: 2}
	if _, err := collection.InsertMany(context.Background(), []Document{
		{"_id": ID(first), "rank": Int(1), "visible": Bool(true), "group": String("same")},
		{"_id": ID(second), "rank": Int(2), "visible": Bool(false), "group": String("same")},
		{"_id": ID(third), "rank": Int(3), "visible": Bool(true), "group": String("same")},
		{"_id": ID(fourth), "rank": Int(4), "visible": Bool(true), "group": String("other")},
	}); err != nil {
		t.Fatal(err)
	}
	one := 1
	cursor, err := collection.Find(context.Background(), Filter{"visible": true}, QueryOptions{Skip: 1, Limit: &one})
	if err != nil {
		t.Fatal(err)
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 1 {
		t.Fatalf("lazy cursor readers=%d", readers)
	}
	if active := db.Stats().Queries.ActiveCursors; active != 1 {
		t.Fatalf("active lazy cursors=%d", active)
	}
	// The cursor owns the old immutable roots. A later insert must not appear,
	// and the returned match must follow insertion position rather than _id.
	if _, err := collection.InsertOne(context.Background(), Document{"rank": Int(5), "visible": Bool(true)}); err != nil {
		t.Fatal(err)
	}
	document, exists, err := cursor.Next(context.Background())
	if err != nil || !exists {
		t.Fatalf("lazy Next exists=%t err=%v", exists, err)
	}
	id, _ := document.ID()
	if id != third {
		t.Fatalf("lazy insertion order id=%v want=%v", id, third)
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
		t.Fatalf("limit completion leaked readers=%d", readers)
	}
	if active := db.Stats().Queries.ActiveCursors; active != 0 {
		t.Fatalf("completed lazy cursors=%d", active)
	}
	stats := db.Stats().Queries
	if stats.Total == 0 || stats.CollectionScans == 0 || stats.DocumentsExamined < 3 || stats.DocumentsReturned == 0 {
		t.Fatalf("streaming query stats=%+v", stats)
	}

	early, err := collection.Find(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 1 {
		t.Fatalf("early cursor readers=%d", readers)
	}
	if err := early.Close(); err != nil {
		t.Fatal(err)
	}
	if err := early.Close(); err != nil {
		t.Fatal(err)
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
		t.Fatalf("explicit Close leaked readers=%d", readers)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelled, err := collection.Find(ctx, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 1 {
		t.Fatalf("context cursor readers=%d", readers)
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for db.Stats().Storage.ActiveReaders != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
		t.Fatalf("context cancellation leaked readers=%d", readers)
	}
	if _, exists, err := cancelled.Next(context.Background()); err != nil || exists {
		t.Fatalf("cancelled cursor Next exists=%t err=%v", exists, err)
	}

	if err := collection.CreateIndex(context.Background(), "by_group", []IndexField{{Field: "group", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	indexed, err := collection.Find(context.Background(), Filter{"group": "same"})
	if err != nil {
		t.Fatal(err)
	}
	if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
		t.Fatalf("materialized IXSCAN retained readers=%d", readers)
	}
	indexedDocuments, err := indexed.All(context.Background())
	if err != nil || len(indexedDocuments) != 3 {
		t.Fatalf("indexed documents=%d err=%v", len(indexedDocuments), err)
	}
}

type cancelAfterChecksContext struct {
	checks    int
	threshold int
	done      chan struct{}
	cancelled bool
}

func newCancelAfterChecksContext(threshold int) *cancelAfterChecksContext {
	return &cancelAfterChecksContext{threshold: threshold, done: make(chan struct{})}
}

func (ctx *cancelAfterChecksContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *cancelAfterChecksContext) Done() <-chan struct{} {
	ctx.checks++
	if !ctx.cancelled && ctx.checks >= ctx.threshold {
		ctx.cancelled = true
		close(ctx.done)
	}
	return ctx.done
}
func (ctx *cancelAfterChecksContext) Err() error {
	if ctx.cancelled {
		return context.Canceled
	}
	return nil
}
func (ctx *cancelAfterChecksContext) Value(any) any { return nil }
