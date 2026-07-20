package meldbase

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestV2DocumentCacheIsByteAndEntryBoundedAndVersionChecked(t *testing.T) {
	cache := newV2DocumentCache(2, 64*1024)
	firstID, secondID, thirdID := DocumentID{1}, DocumentID{2}, DocumentID{3}
	encode := func(id DocumentID, value int64) []byte {
		t.Helper()
		encoded, err := encodeStoredDocument(Document{"_id": ID(id), "value": Int(value), "payload": String("payload")})
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	firstV1 := encode(firstID, 1)
	document, err := cache.decode("items", firstID, firstV1)
	if err != nil {
		t.Fatal(err)
	}
	value, _ := document["value"].Int64()
	if value != 1 {
		t.Fatalf("value=%d", value)
	}
	if _, err := cache.decode("items", firstID, firstV1); err != nil {
		t.Fatal(err)
	}
	firstV2 := encode(firstID, 10)
	document, err = cache.decode("items", firstID, firstV2)
	if err != nil {
		t.Fatal(err)
	}
	value, _ = document["value"].Int64()
	if value != 10 {
		t.Fatalf("updated value=%d", value)
	}
	if _, err := cache.decode("items", secondID, encode(secondID, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.decode("items", thirdID, encode(thirdID, 3)); err != nil {
		t.Fatal(err)
	}
	stats := cache.stats()
	if stats.Entries != 2 || stats.Bytes > stats.CapacityBytes || stats.Hits != 1 || stats.Misses != 4 || stats.Evictions != 1 {
		t.Fatalf("cache stats=%+v", stats)
	}
	cache.remove("items", thirdID)
	if stats := cache.stats(); stats.Entries != 1 {
		t.Fatalf("remove stats=%+v", stats)
	}

	tiny := newV2DocumentCache(10, 1)
	if _, err := tiny.decode("items", firstID, firstV1); err != nil {
		t.Fatal(err)
	}
	if stats := tiny.stats(); stats.Entries != 0 || stats.Bytes != 0 || stats.Misses != 1 {
		t.Fatalf("oversized cache stats=%+v", stats)
	}
}

func TestV2DocumentCacheConcurrentAccessRemainsBounded(t *testing.T) {
	cache := newV2DocumentCache(8, 64*1024)
	encoded := make([][]byte, 32)
	for index := range encoded {
		id := DocumentID{byte(index + 1)}
		var err error
		encoded[index], err = encodeStoredDocument(Document{"_id": ID(id), "value": Int(int64(index))})
		if err != nil {
			t.Fatal(err)
		}
	}
	var wait sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wait.Add(1)
		go func(offset int) {
			defer wait.Done()
			for iteration := 0; iteration < 500; iteration++ {
				index := (offset + iteration) % len(encoded)
				id := DocumentID{byte(index + 1)}
				if _, err := cache.decode("items", id, encoded[index]); err != nil {
					t.Errorf("decode: %v", err)
					return
				}
				if iteration%17 == 0 {
					cache.remove("items", id)
				}
			}
		}(worker)
	}
	wait.Wait()
	stats := cache.stats()
	if stats.Entries > stats.CapacityEntries || stats.Bytes > stats.CapacityBytes {
		t.Fatalf("unbounded cache stats=%+v", stats)
	}
}

func TestOpenV2DocumentCacheNeverLeaksMutableOrStaleDocuments(t *testing.T) {
	db, err := OpenV2(filepath.Join(t.TempDir(), "document-cache.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"value": Int(1), "nested": Object(Document{"safe": Bool(true)})})
	if err != nil {
		t.Fatal(err)
	}
	first, err := collection.FindOne(context.Background(), Filter{"_id": id})
	if err != nil {
		t.Fatal(err)
	}
	first["value"] = Int(999)
	first["nested"] = Object(Document{"safe": Bool(false)})
	second, err := collection.FindOne(context.Background(), Filter{"_id": id})
	if err != nil {
		t.Fatal(err)
	}
	value, _ := second["value"].Int64()
	if value != 1 || !second["nested"].Equal(Object(Document{"safe": Bool(true)})) {
		t.Fatalf("cached document was mutated: %+v", second)
	}
	stats := db.Stats().Storage.DocumentCache
	if stats.Misses != 1 || stats.Hits != 1 || stats.Entries != 1 || stats.Bytes == 0 {
		t.Fatalf("warm cache stats=%+v", stats)
	}
	if result, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": int64(2)}}); err != nil || result.ModifiedCount != 1 {
		t.Fatalf("update=%+v err=%v", result, err)
	}
	updated, err := collection.FindOne(context.Background(), Filter{"_id": id})
	if err != nil {
		t.Fatal(err)
	}
	value, _ = updated["value"].Int64()
	if value != 2 {
		t.Fatalf("stale cached value=%d", value)
	}
	stats = db.Stats().Storage.DocumentCache
	if stats.Misses != 2 || stats.Entries != 1 {
		t.Fatalf("updated cache stats=%+v", stats)
	}
	if result, err := collection.DeleteOne(context.Background(), Filter{"_id": id}); err != nil || result.DeletedCount != 1 {
		t.Fatalf("delete=%+v err=%v", result, err)
	}
	if _, err := collection.FindOne(context.Background(), Filter{"_id": id}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted query error=%v", err)
	}
	if stats := db.Stats().Storage.DocumentCache; stats.Entries != 0 {
		t.Fatalf("deleted entry remained cached: %+v", stats)
	}
}
