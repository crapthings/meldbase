package database

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestIndexBuildLimitsRejectAtomically(t *testing.T) {
	constructors := map[string]func(*testing.T) (*DB, string){
		"memory": func(t *testing.T) (*DB, string) {
			db, err := NewWithOptions(DatabaseOptions{ResourceLimits: ResourceLimits{MaxIndexBuildEntries: 2}})
			if err != nil {
				t.Fatal(err)
			}
			return db, ""
		},
		"durable": func(t *testing.T) (*DB, string) {
			path := filepath.Join(t.TempDir(), "database.meld2")
			db, err := OpenWithOptions(path, OpenOptions{ResourceLimits: ResourceLimits{MaxIndexBuildEntries: 2}})
			if err != nil {
				t.Fatal(err)
			}
			return db, path
		},
	}
	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			db, path := construct(t)
			defer db.Close()
			items := db.Collection("items")
			if _, err := items.InsertMany(context.Background(), []Document{{"value": Int(1)}, {"value": Int(2)}, {"value": Int(3)}}); err != nil {
				t.Fatal(err)
			}
			sequence := db.Stats().CommitSequence
			var before []byte
			if path != "" {
				before, _ = os.ReadFile(path)
			}
			if err := items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("CreateIndex error=%v", err)
			}
			stats := db.Stats()
			if stats.CommitSequence != sequence || stats.Resources.Rejections != 1 || stats.WritesDisabled || stats.Indexes != 0 ||
				stats.IndexBuilds.Active != 0 || stats.IndexBuilds.Attempts != 1 || stats.IndexBuilds.Completed != 0 ||
				stats.IndexBuilds.Failed != 1 || stats.IndexBuilds.LastEntries != 2 || stats.IndexBuilds.LastBytes == 0 {
				t.Fatalf("stats=%+v", stats)
			}
			if path != "" {
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("rejected index changed durable file: equal=%v err=%v", bytes.Equal(before, after), err)
				}
			}
			if _, err := items.InsertOne(context.Background(), Document{"value": Int(4)}); err != nil {
				t.Fatalf("write after rejection=%v", err)
			}
		})
	}
}

func TestIndexBuildByteLimitUsesCanonicalSecondaryKeyBytes(t *testing.T) {
	db, err := NewWithOptions(DatabaseOptions{ResourceLimits: ResourceLimits{MaxIndexBuildBytes: 24}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": String("x")}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("CreateIndex error=%v", err)
	}
}

func TestCanonicalDocumentSizeMatchesEncoder(t *testing.T) {
	document := Document{
		"array":  Array(Int(1), Object(Document{"nested": String("value")})),
		"binary": Binary([]byte{1, 2, 3}),
		"flag":   Bool(true),
	}
	size, err := canonicalDocumentSize(document)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeDocumentBinary(document)
	if err != nil {
		t.Fatal(err)
	}
	if size != uint64(len(encoded)) {
		t.Fatalf("canonical size=%d encoded=%d", size, len(encoded))
	}
	if allocations := testing.AllocsPerRun(100, func() {
		_, _ = canonicalDocumentSize(document)
	}); allocations != 0 {
		t.Fatalf("canonical measurement allocations = %g", allocations)
	}
}

func TestResourceLimitsRejectAtomicallyAndAreObservable(t *testing.T) {
	db, err := NewWithOptions(DatabaseOptions{ResourceLimits: ResourceLimits{
		MaxDocumentBytes: 64, MaxTransactionBytes: 128, MaxTransactionChanges: 2,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")

	if _, err := collection.InsertOne(context.Background(), Document{"value": String(strings.Repeat("x", 35))}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("large document error = %v", err)
	}
	if _, err := collection.InsertMany(context.Background(), []Document{{"value": Int(1)}, {"value": Int(2)}, {"value": Int(3)}}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("large transaction error = %v", err)
	}
	cursor, err := collection.Find(context.Background(), Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := cursor.All(context.Background())
	if err != nil || len(got) != 0 {
		t.Fatalf("rejected writes became visible: len=%d err=%v", len(got), err)
	}
	stats := db.Stats()
	if stats.Resources.Limits.MaxDocumentBytes != 64 || stats.Resources.Rejections != 2 || stats.CommitSequence != 0 {
		t.Fatalf("resource stats = %+v sequence=%d", stats.Resources, stats.CommitSequence)
	}

	for index := int64(0); index < 3; index++ {
		if _, err := collection.InsertOne(context.Background(), Document{"value": Int(index)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := collection.UpdateMany(context.Background(), Filter{}, Update{"$set": map[string]any{"changed": true}}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unbounded update error = %v", err)
	}
	if stats := db.Stats(); stats.Resources.Rejections != 3 || stats.CommitSequence != 3 {
		t.Fatalf("post-update stats = %+v sequence=%d", stats.Resources, stats.CommitSequence)
	}
}

func TestQueryBudgetsApplyToReadsSnapshotsAndMutations(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(t *testing.T) *DB {
			db, err := NewWithOptions(DatabaseOptions{ResourceLimits: ResourceLimits{MaxQueryDocumentsExamined: 2}})
			if err != nil {
				t.Fatal(err)
			}
			return db
		},
		"durable": func(t *testing.T) *DB {
			db, err := OpenWithOptions(filepath.Join(t.TempDir(), "query-budget.meld2"), OpenOptions{ResourceLimits: ResourceLimits{MaxQueryDocumentsExamined: 2}})
			if err != nil {
				t.Fatal(err)
			}
			return db
		},
	}
	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			db := construct(t)
			defer db.Close()
			items := db.Collection("items")
			if _, err := items.InsertMany(context.Background(), []Document{{"value": Int(1)}, {"value": Int(2)}, {"value": Int(3)}}); err != nil {
				t.Fatal(err)
			}
			cursor, err := items.Find(context.Background(), Filter{}, QueryOptions{})
			if err == nil {
				_, err = cursor.All(context.Background())
			}
			if !errors.Is(err, ErrQueryBudget) {
				t.Fatalf("read error=%v", err)
			}
			query, err := CompileQuery(Filter{}, QueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := items.SnapshotQuery(context.Background(), query); !errors.Is(err, ErrQueryBudget) {
				t.Fatalf("snapshot error=%v", err)
			}
			if _, err := items.UpdateMany(context.Background(), Filter{}, Update{"$set": map[string]any{"changed": true}}); !errors.Is(err, ErrQueryBudget) {
				t.Fatalf("mutation error=%v", err)
			}
			if stats := db.Stats(); stats.Resources.Rejections < 3 {
				t.Fatalf("resource rejections=%d", stats.Resources.Rejections)
			}
		})
	}
}

func TestQueryBudgetsBoundIndexKeysSortBytesAndSkip(t *testing.T) {
	db, err := NewWithOptions(DatabaseOptions{ResourceLimits: ResourceLimits{
		MaxQueryKeysExamined: 1, MaxQuerySortBytes: 32, MaxQuerySkip: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertMany(context.Background(), []Document{{"n": Int(1), "payload": String("large enough to exceed the sort budget")}, {"n": Int(2), "payload": String("large enough to exceed the sort budget")}}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := items.Find(context.Background(), Filter{"n": map[string]any{"$gte": int64(1)}}, QueryOptions{}); !errors.Is(err, ErrQueryBudget) {
		t.Fatalf("index key budget error=%v", err)
	}
	if _, err := items.Find(context.Background(), Filter{}, QueryOptions{Sort: []SortField{{Path: "payload", Direction: 1}}}); !errors.Is(err, ErrQueryBudget) {
		t.Fatalf("sort byte budget error=%v", err)
	}
	if _, err := items.Find(context.Background(), Filter{}, QueryOptions{Skip: 2}); !errors.Is(err, ErrQueryBudget) {
		t.Fatalf("skip budget error=%v", err)
	}
}

func TestSortedLimitedQueryUsesBoundedTopKCandidates(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(t *testing.T) *DB {
			db, err := NewWithOptions(DatabaseOptions{ResourceLimits: ResourceLimits{MaxQueryCandidates: 2, MaxQueryDocumentsExamined: 10}})
			if err != nil {
				t.Fatal(err)
			}
			return db
		},
		"durable": func(t *testing.T) *DB {
			db, err := OpenWithOptions(filepath.Join(t.TempDir(), "top-k.meld2"), OpenOptions{ResourceLimits: ResourceLimits{MaxQueryCandidates: 2, MaxQueryDocumentsExamined: 10}})
			if err != nil {
				t.Fatal(err)
			}
			return db
		},
	}
	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			db := construct(t)
			defer db.Close()
			items := db.Collection("items")
			for _, value := range []int64{5, 1, 4, 0, 2} {
				if _, err := items.InsertOne(context.Background(), Document{"score": Int(value)}); err != nil {
					t.Fatal(err)
				}
			}
			limit := 2
			cursor, err := items.Find(context.Background(), Filter{}, QueryOptions{Sort: []SortField{{Path: "score", Direction: 1}}, Limit: &limit})
			if err != nil {
				t.Fatal(err)
			}
			documents, err := cursor.All(context.Background())
			if err != nil || len(documents) != 2 {
				t.Fatalf("documents=%+v err=%v", documents, err)
			}
			first, _ := documents[0]["score"].Int64()
			second, _ := documents[1]["score"].Int64()
			if first != 0 || second != 1 {
				t.Fatalf("scores=%d,%d", first, second)
			}
		})
	}
}

func TestInvalidResourceLimitsFailBeforeOpen(t *testing.T) {
	invalid := ResourceLimits{MaxDocumentBytes: 128, MaxTransactionBytes: 64}
	if _, err := NewWithOptions(DatabaseOptions{ResourceLimits: invalid}); !errors.Is(err, ErrInvalidResourceLimits) {
		t.Fatalf("memory error = %v", err)
	}
	path := t.TempDir() + "/database.meld"
	if _, err := OpenWithOptions(path, OpenOptions{ResourceLimits: invalid}); !errors.Is(err, ErrInvalidResourceLimits) {
		t.Fatalf("open error = %v", err)
	}
}

func TestReactiveViewResourceLimitsRejectInitialAndIncrementalGrowth(t *testing.T) {
	limits := ResourceLimits{MaxReactiveViewDocuments: 1, MaxReactiveViewBytes: 1024}
	initial, err := NewWithOptions(DatabaseOptions{ResourceLimits: limits})
	if err != nil {
		t.Fatal(err)
	}
	defer initial.Close()
	items := initial.Collection("items")
	if _, err := items.InsertMany(context.Background(), []Document{{"value": Int(1)}, {"value": Int(2)}}); err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if subscription, err := items.SubscribeQueryDeltas(context.Background(), query, 2); subscription != nil || !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("initial reactive admission subscription=%v err=%v", subscription, err)
	}
	if stats := initial.Stats(); stats.Resources.Rejections != 1 || stats.Realtime.SharedViews != 0 {
		t.Fatalf("initial reactive stats=%+v", stats)
	}

	growing, err := NewWithOptions(DatabaseOptions{ResourceLimits: limits})
	if err != nil {
		t.Fatal(err)
	}
	defer growing.Close()
	items = growing.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	subscription, err := items.SubscribeQueryDeltas(context.Background(), query, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if len(subscription.Initial.Documents) != 1 {
		t.Fatalf("initial documents=%d", len(subscription.Initial.Documents))
	}
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(2)}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-subscription.Errors:
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("incremental reactive error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("incremental reactive limit did not terminate view")
	}
	if stats := growing.Stats(); stats.Resources.Rejections != 1 || stats.Realtime.SharedViews != 0 || stats.Realtime.QuerySubscribers != 0 {
		t.Fatalf("incremental reactive stats=%+v", stats)
	}
}

func TestReactiveViewByteLimitUsesCanonicalDocumentBytes(t *testing.T) {
	db, err := NewWithOptions(DatabaseOptions{ResourceLimits: ResourceLimits{
		MaxReactiveViewDocuments: 10, MaxReactiveViewBytes: 64,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": String(strings.Repeat("x", 80))}); err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if subscription, err := db.Collection("items").SubscribeQueryDeltas(context.Background(), query, 1); subscription != nil || !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("byte-limited reactive admission subscription=%v err=%v", subscription, err)
	}
}

func TestReplayReactiveViewResourceLimitTerminatesAndReleasesLease(t *testing.T) {
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "replay-view-limit.meld2"), OpenOptions{
		ResourceLimits: ResourceLimits{MaxReactiveViewDocuments: 1, MaxReactiveViewBytes: 1024},
	})
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
	replay, err := db.OpenQueryReplay(context.Background(), "items", query, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(2)}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-replay.Errors:
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("replay resource error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("replay resource limit did not terminate")
	}
	deadline := time.Now().Add(time.Second)
	for {
		stats := db.Stats()
		if stats.Resources.Rejections == 1 && stats.Storage.ActiveReplayLeases == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replay resource stats=%+v", stats)
		}
		time.Sleep(time.Millisecond)
	}
}

func BenchmarkCanonicalDocumentSize(b *testing.B) {
	document := Document{
		"array":  Array(Int(1), Object(Document{"nested": String("value")})),
		"binary": Binary([]byte{1, 2, 3}),
		"flag":   Bool(true),
	}
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		_, _ = canonicalDocumentSize(document)
	}
}
