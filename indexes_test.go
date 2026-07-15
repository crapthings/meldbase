package meldbase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"testing"

	btree "github.com/crapthings/meldbase/internal/index"
)

func TestIndexedRangeQueriesMatchCollectionScanAcrossRandomData(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			db := New()
			defer db.Close()
			collection := db.Collection("items")
			random := rand.New(rand.NewPCG(seed, seed^0x517cc1b727220a95))
			permutation := random.Perm(80)
			for _, number := range permutation {
				if _, err := collection.InsertOne(context.Background(), Document{"n": Int(int64(number)), "bucket": Int(int64(number % 7))}); err != nil {
					t.Fatal(err)
				}
			}
			type sample struct {
				filter  Filter
				options QueryOptions
				ids     []DocumentID
			}
			samples := make([]sample, 24)
			for index := range samples {
				lower := int64(random.Uint64() % 70)
				upper := lower + int64(random.Uint64()%12)
				limit := 1 + int(random.Uint64()%12)
				filter := Filter{"n": map[string]any{"$gte": lower, "$lte": upper}}
				options := QueryOptions{Sort: []SortField{{Path: "n", Direction: 1}}, Skip: int(random.Uint64() % 3), Limit: &limit}
				samples[index] = sample{filter: filter, options: options, ids: queryIDs(t, collection, filter, options)}
			}
			if err := collection.CreateIndex(context.Background(), "items_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true}); err != nil {
				t.Fatal(err)
			}
			for _, sample := range samples {
				actual := queryIDs(t, collection, sample.filter, sample.options)
				if !reflect.DeepEqual(actual, sample.ids) {
					t.Fatalf("filter=%v options=%+v indexed=%v scan=%v", sample.filter, sample.options, actual, sample.ids)
				}
				explain, err := collection.Explain(context.Background(), sample.filter)
				if err != nil || explain.Stage != "IXSCAN" {
					t.Fatalf("explain=%+v err=%v", explain, err)
				}
			}
		})
	}
}

func queryIDs(t *testing.T, collection *Collection, filter Filter, options QueryOptions) []DocumentID {
	t.Helper()
	cursor, err := collection.Find(context.Background(), filter, options)
	if err != nil {
		t.Fatal(err)
	}
	documents, err := cursor.All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]DocumentID, len(documents))
	for index, document := range documents {
		ids[index], _ = document.ID()
	}
	return ids
}

func TestUniqueIndexMaintainedAcrossCRUDAndPlanner(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	users := db.Collection("users")
	first, _ := users.InsertOne(context.Background(), Document{"email": String("a@example.com"), "age": Int(20)})
	second, _ := users.InsertOne(context.Background(), Document{"email": String("b@example.com"), "age": Int(30)})
	if err := users.CreateIndex(context.Background(), "users_email", []IndexField{{Field: "email", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	explain, err := users.Explain(context.Background(), Filter{"email": "a@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if explain.Stage != "IXSCAN" || explain.IndexName != "users_email" || explain.DocumentsExamined != 1 {
		t.Fatalf("explain = %+v", explain)
	}
	if _, err := users.InsertOne(context.Background(), Document{"email": String("a@example.com")}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate insert error = %v", err)
	}
	if _, err := users.UpdateOne(context.Background(), Filter{"_id": second}, Update{"$set": map[string]any{"email": "a@example.com"}}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate update error = %v", err)
	}
	if _, err := users.UpdateOne(context.Background(), Filter{"_id": first}, Update{"$set": map[string]any{"email": "new@example.com"}}); err != nil {
		t.Fatal(err)
	}
	old, _ := users.Find(context.Background(), Filter{"email": "a@example.com"})
	oldDocs, _ := old.All(context.Background())
	if len(oldDocs) != 0 {
		t.Fatal("old index key remained")
	}
	if _, err := users.DeleteOne(context.Background(), Filter{"_id": first}); err != nil {
		t.Fatal(err)
	}
	if _, err := users.InsertOne(context.Background(), Document{"email": String("new@example.com")}); err != nil {
		t.Fatalf("deleted unique key not released: %v", err)
	}
}

func TestIndexRangeScanAndExactMixedNumericUniqueness(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	values := db.Collection("values")
	for i := int64(0); i < 20; i++ {
		if _, err := values.InsertOne(context.Background(), Document{"n": Int(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := values.CreateIndex(context.Background(), "values_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	cursor, err := values.Find(context.Background(), Filter{"n": map[string]any{"$gt": int64(5), "$lte": int64(9)}}, QueryOptions{Sort: []SortField{{Path: "n", Direction: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	documents, _ := cursor.All(context.Background())
	numbers := []int64{}
	for _, document := range documents {
		number, _ := document["n"].Int64()
		numbers = append(numbers, number)
	}
	if !reflect.DeepEqual(numbers, []int64{6, 7, 8, 9}) {
		t.Fatalf("numbers = %v", numbers)
	}
	query, _ := CompileQuery(Filter{"n": map[string]any{"$gte": int64(5), "$lt": int64(10)}}, QueryOptions{})
	_, explain, err := values.plan(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if explain.Stage != "IXSCAN" || explain.KeysExamined > 6 {
		t.Fatalf("range explain = %+v", explain)
	}
	if _, err := values.InsertOne(context.Background(), Document{"n": Float(10)}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("numeric duplicate error = %v", err)
	}
}

func TestIndexDefinitionAndContentsRecoverFromWALAndCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "indexed.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	users := db.Collection("users")
	if _, err := users.InsertOne(context.Background(), Document{"email": String("a@example.com")}); err != nil {
		t.Fatal(err)
	}
	if err := users.CreateIndex(context.Background(), "users_email", []IndexField{{Field: "email", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	crashClose(t, db)
	recovered, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	explain, err := recovered.Collection("users").Explain(context.Background(), Filter{"email": "a@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if explain.Stage != "IXSCAN" {
		t.Fatalf("WAL index explain = %+v", explain)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	explain, err = reopened.Collection("users").Explain(context.Background(), Filter{"email": "a@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if explain.Stage != "IXSCAN" {
		t.Fatalf("checkpoint index explain = %+v", explain)
	}
	if _, err := reopened.Collection("users").InsertOne(context.Background(), Document{"email": String("a@example.com")}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("reopened unique error = %v", err)
	}
}

func TestUniqueIndexBatchUpdateRecoversWithoutFalseIntermediateConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	values := db.Collection("values")
	for _, number := range []int64{1, 2} {
		if _, err := values.InsertOne(context.Background(), Document{"n": Int(number)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := values.CreateIndex(context.Background(), "values_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := values.UpdateMany(context.Background(), Filter{}, Update{"$inc": map[string]any{"n": int64(1)}}); err != nil {
		t.Fatal(err)
	}
	crashClose(t, db)
	recovered, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	cursor, err := recovered.Collection("values").Find(context.Background(), Filter{}, QueryOptions{Sort: []SortField{{Path: "n", Direction: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	documents, _ := cursor.All(context.Background())
	got := []int64{}
	for _, document := range documents {
		number, _ := document["n"].Int64()
		got = append(got, number)
	}
	if !reflect.DeepEqual(got, []int64{2, 3}) {
		t.Fatalf("numbers = %v", got)
	}
}

func TestSnapshotRestoresPersistedBTreeTopologyInsteadOfRebuilding(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	collection := db.Collection("items")
	ids := make([]DocumentID, 200)
	for number := range ids {
		id, err := collection.InsertOne(context.Background(), Document{"n": Int(int64(number))})
		if err != nil {
			t.Fatal(err)
		}
		ids[number] = id
	}
	if err := collection.CreateIndex(context.Background(), "items_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	// Give the valid tree a topology that differs from rebuilding in document
	// insertion order. Persistence must retain these nodes, not infer them again.
	reordered := btree.New()
	for number := len(ids) - 1; number >= 0; number-- {
		key, err := encodeIndexKey(Int(int64(number)))
		if err != nil {
			t.Fatal(err)
		}
		reordered.Insert(key, ids[number][:])
	}
	db.collections["items"].indexes["items_n"].tree = reordered
	before, err := reordered.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := encodeSnapshot(db.collections)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	after, err := decoded["items"].indexes["items_n"].tree.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("B+Tree topology was rebuilt instead of restored")
	}
}
