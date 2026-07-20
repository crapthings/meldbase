package meldbase

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
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

func TestCompoundIndexPlannerMixedDirectionsAndStableOrder(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	items := db.Collection("items")
	for _, document := range []Document{
		{"group": String("a"), "score": Int(8), "label": String("first")},
		{"group": String("b"), "score": Int(99), "label": String("other")},
		{"group": String("a"), "score": Int(9), "label": String("second")},
		{"group": String("a"), "score": Int(7), "label": String("third")},
		{"group": String("a"), "label": String("missing")},
	} {
		if _, err := items.InsertOne(context.Background(), document); err != nil {
			t.Fatal(err)
		}
	}
	fields := []IndexField{{Field: "group", Order: 1}, {Field: "score", Order: -1}}
	if err := items.CreateIndex(context.Background(), "group_score", fields, IndexOptions{}); err != nil {
		t.Fatal(err)
	}

	assertIndex := func(filter Filter) {
		t.Helper()
		explain, err := items.Explain(context.Background(), filter)
		if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "group_score" {
			t.Fatalf("filter=%v explain=%+v err=%v", filter, explain, err)
		}
	}
	assertIndex(Filter{"group": "a", "score": int64(8)})
	assertIndex(Filter{"group": "a"})
	assertIndex(Filter{"group": "a", "score": map[string]any{"$gt": int64(7), "$lte": int64(9)}})

	cursor, err := items.Find(context.Background(), Filter{"group": "a"})
	if err != nil {
		t.Fatal(err)
	}
	documents, err := cursor.All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	labels := make([]string, 0, len(documents))
	for _, document := range documents {
		label, _ := document["label"].StringValue()
		labels = append(labels, label)
	}
	if !reflect.DeepEqual(labels, []string{"first", "second", "third", "missing"}) {
		t.Fatalf("unsorted compound index changed insertion order: %v", labels)
	}

	cursor, err = items.Find(context.Background(), Filter{
		"group": "a",
		"score": map[string]any{"$gt": int64(7), "$lte": int64(9)},
	}, QueryOptions{Sort: []SortField{{Path: "score", Direction: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	documents, err = cursor.All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	scores := make([]int64, 0, len(documents))
	for _, document := range documents {
		score, _ := document["score"].Int64()
		scores = append(scores, score)
	}
	if !reflect.DeepEqual(scores, []int64{8, 9}) {
		t.Fatalf("descending physical range returned %v", scores)
	}
}

func TestCompoundIndexPlannerMatchesCollectionScanAcrossDirections(t *testing.T) {
	for _, directions := range [][2]int{{1, 1}, {1, -1}, {-1, 1}, {-1, -1}} {
		name := fmt.Sprintf("%d_%d", directions[0], directions[1])
		t.Run(name, func(t *testing.T) {
			db := New()
			defer db.Close()
			items := db.Collection("items")
			random := rand.New(rand.NewPCG(uint64(directions[0]+2), uint64(directions[1]+7)))
			for ordinal := range 120 {
				document := Document{"a": Int(int64(random.Uint64() % 6)), "ordinal": Int(int64(ordinal))}
				if ordinal%11 != 0 {
					document["b"] = Int(int64(random.Uint64() % 30))
				}
				if _, err := items.InsertOne(context.Background(), document); err != nil {
					t.Fatal(err)
				}
			}
			type sample struct {
				filter Filter
				want   []DocumentID
			}
			samples := make([]sample, 0, 30)
			options := QueryOptions{Sort: []SortField{{Path: "b", Direction: 1}, {Path: "ordinal", Direction: 1}}}
			for range 10 {
				a := int64(random.Uint64() % 6)
				lower := int64(random.Uint64() % 25)
				upper := lower + int64(random.Uint64()%6)
				for _, filter := range []Filter{
					{"a": a},
					{"a": a, "b": lower},
					{"a": a, "b": map[string]any{"$gt": lower, "$lte": upper}},
				} {
					samples = append(samples, sample{filter: filter, want: queryIDs(t, items, filter, options)})
				}
			}
			if err := items.CreateIndex(context.Background(), "a_b", []IndexField{
				{Field: "a", Order: directions[0]}, {Field: "b", Order: directions[1]},
			}, IndexOptions{}); err != nil {
				t.Fatal(err)
			}
			for _, sample := range samples {
				if got := queryIDs(t, items, sample.filter, options); !reflect.DeepEqual(got, sample.want) {
					t.Fatalf("filter=%v got=%v want=%v", sample.filter, got, sample.want)
				}
				if explain, err := items.Explain(context.Background(), sample.filter); err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "a_b" {
					t.Fatalf("filter=%v explain=%+v err=%v", sample.filter, explain, err)
				}
			}
			if explain, err := items.Explain(context.Background(), Filter{"b": int64(1)}); err != nil || explain.Stage != "COLLSCAN" {
				t.Fatalf("non-prefix explain=%+v err=%v", explain, err)
			}
		})
	}
}

func TestUniqueCompoundIndexMaintainedAcrossCRUD(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	items := db.Collection("items")
	first, err := items.InsertOne(context.Background(), Document{"tenant": String("a"), "slug": String("one")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := items.InsertOne(context.Background(), Document{"tenant": String("a"), "slug": String("two")})
	if err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "tenant_slug", []IndexField{
		{Field: "tenant", Order: 1}, {Field: "slug", Order: 1},
	}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := items.InsertOne(context.Background(), Document{"tenant": String("missing-suffix")}); err != nil {
			t.Fatalf("partial tuples must not conflict in a unique index: %v", err)
		}
	}
	if _, err := items.InsertOne(context.Background(), Document{"tenant": String("b"), "slug": String("one")}); err != nil {
		t.Fatalf("same suffix in another tuple must be allowed: %v", err)
	}
	if _, err := items.InsertOne(context.Background(), Document{"tenant": String("a"), "slug": String("one")}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate tuple error = %v", err)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": second}, Update{"$set": map[string]any{"slug": "one"}}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate tuple update error = %v", err)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": second}, Update{"$set": map[string]any{"slug": "three"}}); err != nil {
		t.Fatal(err)
	}
	if got := queryIDs(t, items, Filter{"tenant": "a", "slug": "two"}, QueryOptions{}); len(got) != 0 {
		t.Fatalf("old tuple remained indexed: %v", got)
	}
	if got := queryIDs(t, items, Filter{"tenant": "a", "slug": "three"}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{second}) {
		t.Fatalf("updated tuple IDs = %v", got)
	}
	if _, err := items.DeleteOne(context.Background(), Filter{"_id": first}); err != nil {
		t.Fatal(err)
	}
	if _, err := items.InsertOne(context.Background(), Document{"tenant": String("a"), "slug": String("one")}); err != nil {
		t.Fatalf("deleted tuple was not released: %v", err)
	}
}

func TestCompoundIndexPreservesMissingSuffixMatchesAndRejectsNonScalars(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	items := db.Collection("items")
	missing, err := items.InsertOne(context.Background(), Document{"a": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "a_b", []IndexField{{Field: "a", Order: 1}, {Field: "b", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := queryIDs(t, items, Filter{"a": int64(1)}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{missing}) {
		t.Fatalf("left-prefix query lost missing suffix document: %v (missing=%s)", got, missing)
	}
	if _, err := items.InsertOne(context.Background(), Document{"a": Int(1), "b": Array(Int(2))}); !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("non-scalar insert error = %v", err)
	}
	valid, err := items.InsertOne(context.Background(), Document{"a": Int(1), "b": Int(2)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": valid}, Update{"$set": map[string]any{"b": []any{2}}}); !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("non-scalar update error = %v", err)
	}
	if got := queryIDs(t, items, Filter{"a": int64(1), "b": int64(2)}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{valid}) {
		t.Fatalf("failed update changed index/document atomically: %v", got)
	}
}

func TestCompoundIndexDefinitionValidationAndOwnership(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	items := db.Collection("items")
	invalid := []struct {
		name   string
		fields []IndexField
	}{
		{"duplicate", []IndexField{{Field: "a", Order: 1}, {Field: "a", Order: -1}}},
		{"direction", []IndexField{{Field: "a", Order: 0}}},
		{"too_many", []IndexField{{Field: "a", Order: 1}, {Field: "b", Order: 1}, {Field: "c", Order: 1}, {Field: "d", Order: 1}, {Field: "e", Order: 1}}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if err := items.CreateIndex(context.Background(), test.name, test.fields, IndexOptions{}); !errors.Is(err, ErrInvalidIndex) {
				t.Fatalf("error = %v", err)
			}
		})
	}

	fields := []IndexField{{Field: "a", Order: 1}, {Field: "b", Order: -1}}
	if err := items.CreateIndex(context.Background(), "owned", fields, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	fields[0].Field, fields[1].Order = "mutated", 1
	definition := db.collections["items"].indexes["owned"].definition
	if !reflect.DeepEqual(definition.Fields, []IndexField{{Field: "a", Order: 1}, {Field: "b", Order: -1}}) {
		t.Fatalf("definition retained caller-owned slice: %+v", definition)
	}
}

func TestCompoundIndexRejectsOversizedCanonicalTupleAcrossBuildAndInsert(t *testing.T) {
	oversized := String(strings.Repeat("x", maxCanonicalSecondaryKeyLen))
	for _, existing := range []bool{true, false} {
		t.Run(fmt.Sprintf("existing_%t", existing), func(t *testing.T) {
			db := New()
			defer db.Close()
			items := db.Collection("items")
			if existing {
				if _, err := items.InsertOne(context.Background(), Document{"a": oversized, "b": Int(1)}); err != nil {
					t.Fatal(err)
				}
			}
			err := items.CreateIndex(context.Background(), "a_b", []IndexField{{Field: "a", Order: 1}, {Field: "b", Order: 1}}, IndexOptions{})
			if existing {
				if !errors.Is(err, ErrInvalidIndex) {
					t.Fatalf("build error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := items.InsertOne(context.Background(), Document{"a": oversized, "b": Int(1)}); !errors.Is(err, ErrInvalidIndex) {
				t.Fatalf("insert error = %v", err)
			}
		})
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
