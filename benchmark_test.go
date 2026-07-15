package meldbase

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

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

func BenchmarkDurableCheckpointOneThousandDocuments(b *testing.B) {
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
