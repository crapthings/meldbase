package database

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// BenchmarkInsert measures inserting individual documents into a memory database.
func BenchmarkInsert(b *testing.B) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := collection.InsertOne(context.Background(), Document{"n": Int(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFindCollectionScan measures finding individual documents without a secondary index.
func BenchmarkFindCollectionScan(b *testing.B) {
	db, collection := benchmarkCollection(b, 10_000, false)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := collection.FindOne(context.Background(), Filter{"n": int64(index % 10_000)}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIndexedPointQuery measures finding individual documents through a secondary index.
func BenchmarkIndexedPointQuery(b *testing.B) {
	db, collection := benchmarkCollection(b, 10_000, true)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := collection.FindOne(context.Background(), Filter{"n": int64(index % 10_000)}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIndexedRangeScan measures retrieving one hundred documents through an indexed range.
func BenchmarkIndexedRangeScan(b *testing.B) {
	db, collection := benchmarkCollection(b, 10_000, true)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		start := int64(index % 9_900)
		cursor, err := collection.Find(context.Background(), Filter{"n": map[string]any{"$gte": start, "$lt": start + 100}})
		if err != nil {
			b.Fatal(err)
		}
		if documents, err := cursor.All(context.Background()); err != nil || len(documents) != 100 {
			b.Fatalf("documents=%d err=%v", len(documents), err)
		}
	}
}

// BenchmarkLargeOrUnionPlanning measures explaining a large union of indexed membership filters.
func BenchmarkLargeOrUnionPlanning(b *testing.B) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	if err := collection.CreateIndex(context.Background(), "by_bucket", []IndexField{{Field: "bucket", Order: 1}}, IndexOptions{}); err != nil {
		b.Fatal(err)
	}
	branches := make([]Filter, 64)
	var next int64
	for branch := range branches {
		values := make([]any, 128)
		for index := range values {
			values[index] = next
			next++
		}
		branches[branch] = Filter{"bucket": map[string]any{"$in": values}}
	}
	query, err := CompileQuery(Filter{"$or": branches}, QueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := collection.ExplainQuery(context.Background(), query); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIndexedOrUnionLimit measures retrieving one result from a union of indexed equality filters.
func BenchmarkIndexedOrUnionLimit(b *testing.B) {
	db, collection := benchmarkCollection(b, 10_000, true)
	defer db.Close()
	limit := 1
	query, err := CompileQuery(Filter{"$or": []Filter{
		{"n": int64(9_999)},
		{"n": int64(5_000)},
		{"n": int64(1)},
	}}, QueryOptions{Limit: &limit})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		cursor, err := collection.FindQuery(context.Background(), query)
		if err != nil {
			b.Fatal(err)
		}
		documents, err := cursor.All(context.Background())
		if err != nil || len(documents) != 1 {
			b.Fatalf("documents=%d err=%v", len(documents), err)
		}
	}
}

// BenchmarkCompoundIndexPointQuery measures finding a document through a unique compound index.
func BenchmarkCompoundIndexPointQuery(b *testing.B) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	documents := make([]Document, 10_000)
	for index := range documents {
		documents[index] = Document{"workspace": Int(int64(index % 100)), "score": Int(int64(index)), "payload": String("small")}
	}
	if _, err := collection.InsertMany(context.Background(), documents); err != nil {
		b.Fatal(err)
	}
	if err := collection.CreateIndex(context.Background(), "workspace_score", []IndexField{
		{Field: "workspace", Order: 1}, {Field: "score", Order: -1},
	}, IndexOptions{Unique: true}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		target := int64(index % 10_000)
		if _, err := collection.FindOne(context.Background(), Filter{"workspace": target % 100, "score": target}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompoundIndexKeyEncoding measures encoding a compound key with ascending and descending fields.
func BenchmarkCompoundIndexKeyEncoding(b *testing.B) {
	values := []Value{String("workspace-a"), Int(42), Time(time.UnixMilli(1_700_000_000_000))}
	fields := []IndexField{{Field: "workspace", Order: 1}, {Field: "score", Order: -1}, {Field: "createdAt", Order: -1}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := encodeCompoundIndexKey(values, fields); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpdate measures repeatedly incrementing one document in a memory database.
func BenchmarkUpdate(b *testing.B) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"count": Int(0)})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$inc": map[string]any{"count": int64(1)}}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReactiveBroadcast measures updating a document and receiving its live query snapshot.
func BenchmarkReactiveBroadcast(b *testing.B) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"active": Bool(false)})
	if err != nil {
		b.Fatal(err)
	}
	query, err := CompileQuery(Filter{"active": true}, QueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	subscription, err := collection.SubscribeQuery(context.Background(), query, 2)
	if err != nil {
		b.Fatal(err)
	}
	defer subscription.Close()
	<-subscription.Snapshots
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"active": index%2 == 0}}); err != nil {
			b.Fatal(err)
		}
		select {
		case <-subscription.Snapshots:
		case err := <-subscription.Errors:
			b.Fatal(err)
		}
	}
}

// BenchmarkReactiveFanoutOneHundred measures delivering a changed query result to one hundred subscribers.
func BenchmarkReactiveFanoutOneHundred(b *testing.B) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"active": Bool(false)})
	if err != nil {
		b.Fatal(err)
	}
	query, err := CompileQuery(Filter{"active": true}, QueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	const subscribers = 100
	subscriptions := make([]*QuerySubscription, subscribers)
	for index := range subscriptions {
		subscriptions[index], err = collection.SubscribeQuery(context.Background(), query, 2)
		if err != nil {
			b.Fatal(err)
		}
		defer subscriptions[index].Close()
		<-subscriptions[index].Snapshots
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"active": index%2 == 0}}); err != nil {
			b.Fatal(err)
		}
		for _, subscription := range subscriptions {
			select {
			case <-subscription.Snapshots:
			case err := <-subscription.Errors:
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkReactiveIrrelevantCollectionWrite measures writes to another collection while a live query is subscribed.
func BenchmarkReactiveIrrelevantCollectionWrite(b *testing.B) {
	db := New()
	defer db.Close()
	items := db.Collection("items")
	other := db.Collection("other")
	if _, err := items.InsertOne(context.Background(), Document{"active": Bool(true)}); err != nil {
		b.Fatal(err)
	}
	id, err := other.InsertOne(context.Background(), Document{"count": Int(0)})
	if err != nil {
		b.Fatal(err)
	}
	query, err := CompileQuery(Filter{"active": true}, QueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	subscription, err := items.SubscribeQuery(context.Background(), query, 2)
	if err != nil {
		b.Fatal(err)
	}
	defer subscription.Close()
	<-subscription.Snapshots
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := other.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"count": int64(index)}}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReactiveSelectiveViewTenThousand compares incremental updates and full recomputation for a selective query over ten thousand documents.
func BenchmarkReactiveSelectiveViewTenThousand(b *testing.B) {
	for _, test := range []struct {
		name          string
		forceFallback bool
	}{
		{name: "Incremental"},
		{name: "FullFallback", forceFallback: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			db := New()
			defer db.Close()
			collection := db.Collection("items")
			documents := make([]Document, 10_000)
			for index := range documents {
				documents[index] = Document{"selected": Bool(false), "n": Int(int64(index))}
			}
			ids, err := collection.InsertMany(context.Background(), documents)
			if err != nil {
				b.Fatal(err)
			}
			query, err := CompileQuery(Filter{"selected": true}, QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			subscription, err := collection.SubscribeQuery(context.Background(), query, 2)
			if err != nil {
				b.Fatal(err)
			}
			defer subscription.Close()
			<-subscription.Snapshots
			if test.forceFallback {
				db.reactive.mu.Lock()
				db.reactive.maxChanges = 0
				db.reactive.mu.Unlock()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if _, err := collection.UpdateOne(context.Background(), Filter{"_id": ids[0]}, Update{"$set": map[string]any{"selected": index%2 == 0}}); err != nil {
					b.Fatal(err)
				}
				select {
				case <-subscription.Snapshots:
				case err := <-subscription.Errors:
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReactiveOrderedWindowTenThousand compares incremental updates and full recomputation for a sorted ten-document window.
func BenchmarkReactiveOrderedWindowTenThousand(b *testing.B) {
	for _, test := range []struct {
		name          string
		forceFallback bool
	}{
		{name: "PersistentIncremental"},
		{name: "FullFallback", forceFallback: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			db := New()
			defer db.Close()
			collection := db.Collection("items")
			documents := make([]Document, 10_000)
			for index := range documents {
				documents[index] = Document{"n": Int(int64(index)), "payload": String("small")}
			}
			ids, err := collection.InsertMany(context.Background(), documents)
			if err != nil {
				b.Fatal(err)
			}
			limit := 10
			query, err := CompileQuery(Filter{}, QueryOptions{Sort: []SortField{{Path: "n", Direction: 1}}, Limit: &limit})
			if err != nil {
				b.Fatal(err)
			}
			subscription, err := collection.SubscribeQuery(context.Background(), query, 2)
			if err != nil {
				b.Fatal(err)
			}
			defer subscription.Close()
			<-subscription.Snapshots
			if test.forceFallback {
				db.reactive.mu.Lock()
				db.reactive.maxChanges = 0
				db.reactive.mu.Unlock()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				value := int64(-1)
				if index%2 == 1 {
					value = 20_000
				}
				if _, err := collection.UpdateOne(context.Background(), Filter{"_id": ids[0]}, Update{"$set": map[string]any{"n": value}}); err != nil {
					b.Fatal(err)
				}
				select {
				case snapshot := <-subscription.Snapshots:
					if len(snapshot.Documents) != limit {
						b.Fatalf("window documents=%d", len(snapshot.Documents))
					}
				case err := <-subscription.Errors:
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReactiveBroadFanoutTenThousand compares snapshot and delta delivery to ten subscribers viewing ten thousand documents.
func BenchmarkReactiveBroadFanoutTenThousand(b *testing.B) {
	const subscribers = 10
	for _, deltas := range []bool{false, true} {
		name := "Snapshots"
		if deltas {
			name = "Deltas"
		}
		b.Run(name, func(b *testing.B) {
			db := New()
			defer db.Close()
			collection := db.Collection("items")
			documents := make([]Document, 10_000)
			for index := range documents {
				documents[index] = Document{"n": Int(int64(index)), "value": String("initial")}
			}
			ids, err := collection.InsertMany(context.Background(), documents)
			if err != nil {
				b.Fatal(err)
			}
			query, err := CompileQuery(Filter{}, QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			full := make([]*QuerySubscription, 0, subscribers)
			incremental := make([]*QueryDeltaSubscription, 0, subscribers)
			for index := 0; index < subscribers; index++ {
				if deltas {
					subscription, err := collection.SubscribeQueryDeltas(context.Background(), query, 2)
					if err != nil || len(subscription.Initial.Documents) != len(documents) {
						b.Fatalf("delta initial=%d err=%v", len(subscription.Initial.Documents), err)
					}
					incremental = append(incremental, subscription)
					defer subscription.Close()
				} else {
					subscription, err := collection.SubscribeQuery(context.Background(), query, 2)
					if err != nil {
						b.Fatal(err)
					}
					if initial := <-subscription.Snapshots; len(initial.Documents) != len(documents) {
						b.Fatalf("snapshot initial=%d", len(initial.Documents))
					}
					full = append(full, subscription)
					defer subscription.Close()
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := collection.UpdateOne(context.Background(), Filter{"_id": ids[0]}, Update{"$set": map[string]any{"value": fmt.Sprintf("value-%d", iteration)}}); err != nil {
					b.Fatal(err)
				}
				for _, subscription := range full {
					if snapshot := <-subscription.Snapshots; len(snapshot.Documents) != len(documents) {
						b.Fatalf("snapshot documents=%d", len(snapshot.Documents))
					}
				}
				for _, subscription := range incremental {
					if delta := <-subscription.Deltas; len(delta.Operations) != 1 || delta.Operations[0].Kind != QueryDeltaChange {
						b.Fatalf("delta operations=%+v", delta.Operations)
					}
				}
			}
		})
	}
}

// BenchmarkDurableStatsSnapshot measures reading statistics from an indexed memory database.
func BenchmarkDurableStatsSnapshot(b *testing.B) {
	db, _ := benchmarkCollection(b, 10_000, true)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = db.Stats()
	}
}

// BenchmarkStatsSnapshotCollectionCardinality compares statistics reads with one and ten thousand memory collections.
func BenchmarkStatsSnapshotCollectionCardinality(b *testing.B) {
	for _, count := range []int{1, 10_000} {
		b.Run(fmt.Sprintf("collections_%d", count), func(b *testing.B) {
			db := New()
			defer db.Close()
			db.mu.Lock()
			for index := 0; index < count; index++ {
				db.collections[fmt.Sprintf("collection-%05d", index)] = newCollectionData()
			}
			db.initializeLogicalStats(nil)
			db.mu.Unlock()
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				_ = db.Stats()
			}
		})
	}
}

// BenchmarkStatsSnapshotDiagnosticsEnabled measures statistics reads while diagnostics are enabled.
func BenchmarkStatsSnapshotDiagnosticsEnabled(b *testing.B) {
	db, _ := benchmarkCollection(b, 10_000, true)
	defer db.Close()
	diagnostics, err := db.EnableDiagnostics(DiagnosticsOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer diagnostics.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = db.Stats()
	}
}

// BenchmarkStatsSnapshot measures statistics reads from a durable database.
func BenchmarkStatsSnapshot(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "stats.meld2"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"n": Int(1)}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = db.Stats()
	}
}

// BenchmarkStatsSnapshotWithPersistentIndexBuilds measures statistics reads with eight pending durable index builds.
func BenchmarkStatsSnapshotWithPersistentIndexBuilds(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "stats-builds.meld2"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"n": Int(1)}); err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 8; index++ {
		name := fmt.Sprintf("by_n_%d", index)
		if _, err := items.StartIndexBuild(context.Background(), name, []IndexField{{Field: "n", Order: 1}}, IndexOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	if stats := db.Stats().IndexBuilds; stats.Persistent != 8 || stats.Scanning != 8 {
		b.Fatalf("index build stats=%+v", stats)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = db.Stats()
	}
}

// BenchmarkPointQueryDiagnosticsModes compares point query cost and p99 latency across diagnostic recording modes.
func BenchmarkPointQueryDiagnosticsModes(b *testing.B) {
	for _, mode := range []string{"disabled", "enabled_filtered", "enabled_record_all"} {
		b.Run(mode, func(b *testing.B) {
			db := New()
			defer db.Close()
			collection := db.Collection("items")
			id := DocumentID{1}
			if _, err := collection.InsertOne(context.Background(), Document{"_id": ID(id), "n": Int(1)}); err != nil {
				b.Fatal(err)
			}
			query, err := CompileQuery(Filter{"_id": id}, QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			var diagnostics *Diagnostics
			switch mode {
			case "enabled_filtered":
				diagnostics, err = db.EnableDiagnostics(DiagnosticsOptions{
					Capacity: 1, SlowQueryThreshold: time.Hour, SlowCommitThreshold: time.Hour,
					ExcludeFailures: true,
				})
			case "enabled_record_all":
				diagnostics, err = db.EnableDiagnostics(DiagnosticsOptions{Capacity: 1, RecordAll: true})
			}
			if err != nil {
				b.Fatal(err)
			}
			if diagnostics != nil {
				defer diagnostics.Close()
			}
			latencies := make([]int64, min(b.N, 100_000))
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				started := time.Now()
				cursor, err := collection.FindQuery(context.Background(), query)
				if err != nil {
					b.Fatal(err)
				}
				documents, err := cursor.All(context.Background())
				if err != nil || len(documents) != 1 {
					b.Fatalf("documents=%d err=%v", len(documents), err)
				}
				if iteration < len(latencies) {
					latencies[iteration] = time.Since(started).Nanoseconds()
				}
			}
			b.StopTimer()
			if len(latencies) > 0 {
				sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
				p99 := latencies[(len(latencies)-1)*99/100]
				b.ReportMetric(float64(p99), "p99-ns")
			}
		})
	}
}

// BenchmarkDiagnosticHookModes compares diagnostic hook cost across disabled, filtered, and full recording modes.
func BenchmarkDiagnosticHookModes(b *testing.B) {
	for _, mode := range []string{"disabled", "enabled_filtered", "enabled_record_all"} {
		b.Run(mode, func(b *testing.B) {
			db := New()
			defer db.Close()
			var diagnostics *Diagnostics
			var err error
			switch mode {
			case "enabled_filtered":
				diagnostics, err = db.EnableDiagnostics(DiagnosticsOptions{
					Capacity: 1, SlowQueryThreshold: time.Hour, SlowCommitThreshold: time.Hour,
					ExcludeFailures: true,
				})
			case "enabled_record_all":
				diagnostics, err = db.EnableDiagnostics(DiagnosticsOptions{Capacity: 1, RecordAll: true})
			}
			if err != nil {
				b.Fatal(err)
			}
			if diagnostics != nil {
				defer diagnostics.Close()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				span := db.beginDiagnostic(DiagnosticQuery)
				db.finishQueryDiagnostic(span, ExplainResult{Stage: "ID_LOOKUP", DocumentsExamined: 1}, 1, nil)
			}
		})
	}
}

// BenchmarkStorageBackedPointQuery measures document identifier lookups in a durable collection of one thousand documents.
func BenchmarkStorageBackedPointQuery(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "point-query.meld2"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	documents := make([]Document, 1_000)
	var target DocumentID
	for index := range documents {
		var id DocumentID
		binary.BigEndian.PutUint64(id[8:], uint64(index+1))
		documents[index] = Document{"_id": ID(id), "n": Int(int64(index)), "payload": String("small payload")}
		if index == len(documents)/2 {
			target = id
		}
	}
	if _, err := collection.InsertMany(context.Background(), documents); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		cursor, err := collection.Find(context.Background(), Filter{"_id": target})
		if err != nil {
			b.Fatal(err)
		}
		result, err := cursor.All(context.Background())
		if err != nil || len(result) != 1 {
			b.Fatalf("result=%d err=%v", len(result), err)
		}
	}
}

// BenchmarkStorageBackedCompiledPointQuery measures durable identifier lookups using a precompiled query.
func BenchmarkStorageBackedCompiledPointQuery(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "compiled-point-query.meld2"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	target := DocumentID{1}
	if _, err := collection.InsertOne(context.Background(), Document{
		"_id": ID(target), "n": Int(1), "payload": String("small payload"),
	}); err != nil {
		b.Fatal(err)
	}
	query, err := CompileQuery(Filter{"_id": target}, QueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		cursor, err := collection.FindQuery(context.Background(), query)
		if err != nil {
			b.Fatal(err)
		}
		result, err := cursor.All(context.Background())
		if err != nil || len(result) != 1 {
			b.Fatalf("result=%d err=%v", len(result), err)
		}
	}
}

// BenchmarkStreamingCollectionScanFirstTen measures streaming the first ten matching documents from a durable collection scan.
func BenchmarkStreamingCollectionScanFirstTen(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "streaming-scan.meld2"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	documents := make([]Document, 10_000)
	for index := range documents {
		var id DocumentID
		binary.BigEndian.PutUint64(id[8:], uint64(index+1))
		documents[index] = Document{"_id": ID(id), "ordinal": Int(int64(index)), "even": Bool(index%2 == 0)}
	}
	collection := db.Collection("items")
	if _, err := collection.InsertMany(context.Background(), documents); err != nil {
		b.Fatal(err)
	}
	limit := 10
	query, err := CompileQuery(Filter{"even": true}, QueryOptions{Limit: &limit})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		cursor, err := collection.FindQuery(context.Background(), query)
		if err != nil {
			b.Fatal(err)
		}
		result, err := cursor.All(context.Background())
		if err != nil || len(result) != limit {
			b.Fatalf("result=%d err=%v", len(result), err)
		}
	}
	b.StopTimer()
	if readers := db.Stats().Storage.ActiveReaders; readers != 0 {
		b.Fatalf("streaming benchmark leaked readers=%d", readers)
	}
}

// benchmarkCollection creates a populated memory collection with an optional unique index for benchmarks.
func benchmarkCollection(b *testing.B, count int, indexed bool) (*DB, *Collection) {
	b.Helper()
	db := New()
	collection := db.Collection("items")
	documents := make([]Document, count)
	for index := range documents {
		documents[index] = Document{"n": Int(int64(index)), "name": String(fmt.Sprintf("item-%d", index))}
	}
	if _, err := collection.InsertMany(context.Background(), documents); err != nil {
		db.Close()
		b.Fatal(err)
	}
	if indexed {
		if err := collection.CreateIndex(context.Background(), "items_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true}); err != nil {
			db.Close()
			b.Fatal(err)
		}
	}
	return db, collection
}

// BenchmarkDurableSyncOneThousandDocuments measures creating, populating, syncing, and closing a database with one thousand documents.
func BenchmarkDurableSyncOneThousandDocuments(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		path := filepath.Join(b.TempDir(), fmt.Sprintf("checkpoint-%d.meld", iteration))
		db, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}
		documents := make([]Document, 1_000)
		for index := range documents {
			documents[index] = Document{"n": Int(int64(index)), "payload": String("small benchmark payload")}
		}
		if _, err := db.Collection("items").InsertMany(context.Background(), documents); err != nil {
			b.Fatal(err)
		}
		if err := db.Sync(); err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCreateIndexTenThousandDocuments measures creating a unique index over ten thousand durable documents.
func BenchmarkCreateIndexTenThousandDocuments(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		path := filepath.Join(b.TempDir(), fmt.Sprintf("index-build-%d.meld2", iteration))
		db, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}
		documents := make([]Document, 10_000)
		for index := range documents {
			documents[index] = Document{"n": Int(int64(index)), "payload": String("small benchmark payload")}
		}
		collection := db.Collection("items")
		if _, err := collection.InsertMany(context.Background(), documents); err != nil {
			_ = db.Close()
			b.Fatal(err)
		}
		b.StartTimer()
		err = collection.CreateIndex(context.Background(), "items_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true})
		b.StopTimer()
		if err != nil {
			_ = db.Close()
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
