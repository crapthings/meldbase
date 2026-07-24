package database

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
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

func TestIndexedInAndEligibleOrMatchCollectionScanWithResiduals(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(t *testing.T) *DB {
			return New()
		},
		"durable": func(t *testing.T) *DB {
			db, err := Open(filepath.Join(t.TempDir(), "in-or.meld2"))
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
			for _, document := range []Document{
				{"bucket": Int(1), "state": String("active"), "label": String("first")},
				{"bucket": Int(2), "state": String("active"), "label": String("second")},
				{"bucket": Int(2), "state": String("held"), "label": String("third")},
				{"bucket": Int(3), "state": String("hidden"), "label": String("fourth")},
				{"bucket": Int(1), "state": String("hidden"), "label": String("fifth")},
			} {
				if _, err := items.InsertOne(context.Background(), document); err != nil {
					t.Fatal(err)
				}
			}
			samples := []Filter{
				{"bucket": map[string]any{"$in": []any{int64(2)}}},
				{"bucket": map[string]any{"$in": []any{int64(1), int64(3), int64(1)}}, "state": "active"},
				{"$or": []Filter{{"bucket": int64(1), "state": "active"}, {"bucket": int64(3)}}},
			}
			wants := make([][]DocumentID, len(samples))
			for index, filter := range samples {
				wants[index] = queryIDs(t, items, filter, QueryOptions{Sort: []SortField{{Path: "label", Direction: 1}}})
			}
			if err := items.CreateIndex(context.Background(), "by_bucket_state", []IndexField{{Field: "bucket", Order: 1}, {Field: "state", Order: 1}}, IndexOptions{}); err != nil {
				t.Fatal(err)
			}
			for index, filter := range samples {
				got := queryIDs(t, items, filter, QueryOptions{Sort: []SortField{{Path: "label", Direction: 1}}})
				if !reflect.DeepEqual(got, wants[index]) {
					t.Fatalf("filter=%v got=%v want=%v", filter, got, wants[index])
				}
				explain, err := items.Explain(context.Background(), filter)
				if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "by_bucket_state" || explain.KeysExamined == 0 {
					t.Fatalf("filter=%v explain=%+v err=%v", filter, explain, err)
				}
			}
		})
	}
}

func TestExplainQueryReportsCompiledOptionsBoundsAndSortCompatibleIndex(t *testing.T) {
	db := New()
	defer db.Close()
	items := db.Collection("items")
	for _, document := range []Document{
		{"a": Int(1), "z": Int(3), "other": Int(1)},
		{"a": Int(1), "z": Int(1), "other": Int(3)},
		{"a": Int(1), "z": Int(2), "other": Int(2)},
		{"a": Int(2), "z": Int(0), "other": Int(0)},
	} {
		if _, err := items.InsertOne(context.Background(), document); err != nil {
			t.Fatal(err)
		}
	}
	// The non-sort-compatible name comes first lexically. Plan ranking must
	// still choose by_a_z for the equality prefix plus z ascending order.
	if err := items.CreateIndex(context.Background(), "by_a_other", []IndexField{{Field: "a", Order: 1}, {Field: "other", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_a_z", []IndexField{{Field: "a", Order: 1}, {Field: "z", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	limit := 2
	query, err := CompileQuery(Filter{"a": int64(1)}, QueryOptions{Sort: []SortField{{Path: "z", Direction: 1}}, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	explain, err := items.ExplainQuery(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if explain.Stage != "IXSCAN" || explain.IndexName != "by_a_z" || !explain.ResidualPredicate || !explain.SortRequired || !explain.SortIndexCompatible {
		t.Fatalf("explain=%+v", explain)
	}
	if len(explain.Bounds) != 1 || explain.Bounds[0].Path != "a" || len(explain.Bounds[0].Values) != 1 {
		t.Fatalf("bounds=%+v", explain.Bounds)
	}
	value, ok := explain.Bounds[0].Values[0].Int64()
	if !ok || value != 1 {
		t.Fatalf("bound value=%v", explain.Bounds[0].Values[0])
	}
	if explain.EstimatedDocuments != 3 || explain.EstimatedKeys != 3 || explain.DocumentsExamined != 3 || explain.KeysExamined != 3 {
		t.Fatalf("scan accounting=%+v", explain)
	}
	if explain.CandidatesRetained != 2 || explain.SortBytes <= 0 {
		t.Fatalf("sort accounting=%+v", explain)
	}
	if _, err := items.ExplainWithOptions(context.Background(), Filter{"a": int64(1)}, QueryOptions{Sort: []SortField{{Path: "z", Direction: 1}}, Limit: &limit}); err != nil {
		t.Fatal(err)
	}
	if _, err := items.ExplainQuery(context.Background(), QuerySpec{}); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("zero query error=%v", err)
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
	first, err := items.InsertOne(context.Background(), Document{"workspace": String("a"), "slug": String("one")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := items.InsertOne(context.Background(), Document{"workspace": String("a"), "slug": String("two")})
	if err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "workspace_slug", []IndexField{
		{Field: "workspace", Order: 1}, {Field: "slug", Order: 1},
	}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := items.InsertOne(context.Background(), Document{"workspace": String("missing-suffix")}); err != nil {
			t.Fatalf("partial tuples must not conflict in a unique index: %v", err)
		}
	}
	if _, err := items.InsertOne(context.Background(), Document{"workspace": String("b"), "slug": String("one")}); err != nil {
		t.Fatalf("same suffix in another tuple must be allowed: %v", err)
	}
	if _, err := items.InsertOne(context.Background(), Document{"workspace": String("a"), "slug": String("one")}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate tuple error = %v", err)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": second}, Update{"$set": map[string]any{"slug": "one"}}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate tuple update error = %v", err)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": second}, Update{"$set": map[string]any{"slug": "three"}}); err != nil {
		t.Fatal(err)
	}
	if got := queryIDs(t, items, Filter{"workspace": "a", "slug": "two"}, QueryOptions{}); len(got) != 0 {
		t.Fatalf("old tuple remained indexed: %v", got)
	}
	if got := queryIDs(t, items, Filter{"workspace": "a", "slug": "three"}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{second}) {
		t.Fatalf("updated tuple IDs = %v", got)
	}
	if _, err := items.DeleteOne(context.Background(), Filter{"_id": first}); err != nil {
		t.Fatal(err)
	}
	if _, err := items.InsertOne(context.Background(), Document{"workspace": String("a"), "slug": String("one")}); err != nil {
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
	query, _ := CompileQuery(Filter{"n": map[string]any{"$gt": int64(5), "$lte": int64(9)}}, QueryOptions{})
	_, explain, err := values.plan(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if explain.Stage != "IXSCAN" || explain.KeysExamined != 4 || explain.DocumentsExamined != 4 {
		t.Fatalf("exclusive lower bound explain = %+v", explain)
	}
	query, _ = CompileQuery(Filter{"n": map[string]any{"$gte": int64(5), "$lt": int64(10)}}, QueryOptions{})
	_, explain, err = values.plan(context.Background(), query)
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

func TestOrUnionPlansPrimaryRangeCompoundAndDifferentIndexes(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(t *testing.T) *DB { return New() },
		"durable": func(t *testing.T) *DB {
			db, err := Open(filepath.Join(t.TempDir(), "or-union.meld2"))
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
			documents := make([]Document, 100)
			for index := range documents {
				documents[index] = Document{
					"bucket": Int(int64(index % 10)),
					"serial": Int(int64(index)),
				}
				if index == 50 {
					documents[index]["special"] = String("only")
				}
			}
			ids, err := items.InsertMany(context.Background(), documents)
			if err != nil {
				t.Fatal(err)
			}
			for _, index := range []struct {
				name   string
				fields []IndexField
			}{
				{"by_bucket", []IndexField{{Field: "bucket", Order: 1}}},
				{"by_serial", []IndexField{{Field: "serial", Order: 1}}},
				{"bucket_serial", []IndexField{{Field: "bucket", Order: 1}, {Field: "serial", Order: 1}}},
			} {
				if err := items.CreateIndex(context.Background(), index.name, index.fields, IndexOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			rangeIDs := make([]DocumentID, 0, 20)
			bucketIDs := make([]DocumentID, 0, 10)
			for index, id := range ids {
				if index%10 == 0 || index%10 == 9 {
					rangeIDs = append(rangeIDs, id)
				}
				if index%10 == 1 {
					bucketIDs = append(bucketIDs, id)
				}
			}
			differentIDs := append([]DocumentID(nil), bucketIDs...)
			differentIDs = append(differentIDs, ids[99])
			overlapIDs := append([]DocumentID(nil), bucketIDs...)
			unindexedIDs := append([]DocumentID(nil), bucketIDs...)
			unindexedIDs = append(unindexedIDs[:5], append([]DocumentID{ids[50]}, unindexedIDs[5:]...)...)

			tests := []struct {
				name         string
				filter       Filter
				want         []DocumentID
				stage        string
				maxExamined  int64
				minIndexName int
			}{
				{
					name: "same field range union",
					filter: Filter{"$or": []Filter{
						{"bucket": map[string]any{"$lt": int64(1)}},
						{"bucket": map[string]any{"$gt": int64(8)}},
					}},
					want: rangeIDs, stage: "IXSCAN", maxExamined: 20, minIndexName: 1,
				},
				{
					name: "different secondary indexes",
					filter: Filter{"$or": []Filter{
						{"bucket": int64(1)},
						{"serial": int64(99)},
					}},
					want: differentIDs, stage: "IXSCAN", maxExamined: 11, minIndexName: 2,
				},
				{
					name: "primary key or",
					filter: Filter{"$or": []Filter{
						{"_id": ids[80]},
						{"_id": ids[2]},
					}},
					want: []DocumentID{ids[2], ids[80]}, stage: "ID_LOOKUP", maxExamined: 2, minIndexName: 1,
				},
				{
					name:   "primary key in",
					filter: Filter{"_id": map[string]any{"$in": []any{ids[80], ids[2], ids[80]}}},
					want:   []DocumentID{ids[2], ids[80]}, stage: "ID_LOOKUP", maxExamined: 2, minIndexName: 1,
				},
				{
					name: "branch specific compound bounds",
					filter: Filter{"$or": []Filter{
						{"bucket": int64(1), "serial": int64(11)},
						{"bucket": int64(3), "serial": int64(73)},
					}},
					want: []DocumentID{ids[11], ids[73]}, stage: "IXSCAN", maxExamined: 2, minIndexName: 1,
				},
				{
					name: "deduplicated cross index document",
					filter: Filter{"$or": []Filter{
						{"bucket": int64(1)},
						{"serial": int64(11)},
					}},
					want: overlapIDs, stage: "IXSCAN", maxExamined: 10, minIndexName: 2,
				},
				{
					name: "unindexed branch falls back",
					filter: Filter{"$or": []Filter{
						{"bucket": int64(1)},
						{"special": "only"},
					}},
					want: unindexedIDs, stage: "COLLSCAN", maxExamined: 100,
				},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					explain, err := items.Explain(context.Background(), test.filter)
					if err != nil {
						t.Fatal(err)
					}
					if explain.Stage != test.stage || explain.DocumentsExamined > test.maxExamined {
						t.Fatalf("explain=%+v", explain)
					}
					if len(explain.IndexNames) < test.minIndexName {
						t.Fatalf("index names=%v want at least %d", explain.IndexNames, test.minIndexName)
					}
					before := db.Stats().Queries
					got := queryIDs(t, items, test.filter, QueryOptions{})
					after := db.Stats().Queries
					if !reflect.DeepEqual(got, test.want) {
						t.Fatalf("ids=%v want=%v", got, test.want)
					}
					if examined := int64(after.DocumentsExamined - before.DocumentsExamined); examined != explain.DocumentsExamined {
						t.Fatalf("Find examined=%d Explain examined=%d", examined, explain.DocumentsExamined)
					}
					switch test.stage {
					case "COLLSCAN":
						if after.CollectionScans-before.CollectionScans != 1 {
							t.Fatalf("query stats=%+v before=%+v", after, before)
						}
					case "IXSCAN":
						if after.IndexScans-before.IndexScans != 1 {
							t.Fatalf("query stats=%+v before=%+v", after, before)
						}
					case "ID_LOOKUP":
						if after.IDLookups-before.IDLookups != 1 {
							t.Fatalf("query stats=%+v before=%+v", after, before)
						}
					}
				})
			}
		})
	}
}

func TestIndexedOrUnionMatchesCollectionScanAcrossBackends(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(t *testing.T) *DB { return New() },
		"durable": func(t *testing.T) *DB {
			db, err := Open(filepath.Join(t.TempDir(), "or-differential.meld2"))
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
			random := rand.New(rand.NewPCG(0x0a11ce, 0x0b71ee))
			permutation := random.Perm(120)
			documents := make([]Document, len(permutation))
			for position, number := range permutation {
				documents[position] = Document{
					"a": Int(int64(number % 7)), "b": Int(int64(number % 11)),
					"n": Int(int64(number)), "active": Bool(number%3 != 0),
				}
				if number == 113 {
					documents[position]["special"] = String("only")
				}
			}
			ids, err := items.InsertMany(context.Background(), documents)
			if err != nil {
				t.Fatal(err)
			}
			limitFive, limitThree := 5, 3
			samples := []struct {
				filter  Filter
				options QueryOptions
				indexed bool
			}{
				{
					filter: Filter{"$or": []Filter{
						{"a": map[string]any{"$lte": int64(1)}},
						{"a": map[string]any{"$gte": int64(5)}},
					}},
					options: QueryOptions{Sort: []SortField{{Path: "n", Direction: -1}}, Skip: 2, Limit: &limitFive},
					indexed: true,
				},
				{
					filter:  Filter{"$or": []Filter{{"a": int64(2)}, {"b": int64(9)}}},
					options: QueryOptions{Limit: &limitThree}, indexed: true,
				},
				{
					filter: Filter{"$or": []Filter{
						{"a": int64(1), "b": int64(3)},
						{"a": int64(4), "b": int64(7)},
					}},
					options: QueryOptions{Sort: []SortField{{Path: "b", Direction: 1}}}, indexed: true,
				},
				{
					filter: Filter{
						"active": true,
						"$or":    []Filter{{"a": int64(1)}, {"b": int64(2)}},
					},
					options: QueryOptions{}, indexed: true,
				},
				{
					filter:  Filter{"$or": []Filter{{"_id": ids[97]}, {"a": int64(6)}}},
					options: QueryOptions{}, indexed: true,
				},
				{
					filter:  Filter{"$or": []Filter{{"a": int64(1)}, {"special": "only"}}},
					options: QueryOptions{}, indexed: false,
				},
			}
			wants := make([][]DocumentID, len(samples))
			for index, sample := range samples {
				wants[index] = queryIDs(t, items, sample.filter, sample.options)
			}
			for _, index := range []struct {
				name   string
				fields []IndexField
			}{
				{"by_a", []IndexField{{Field: "a", Order: 1}}},
				{"by_b", []IndexField{{Field: "b", Order: 1}}},
				{"by_a_b", []IndexField{{Field: "a", Order: 1}, {Field: "b", Order: 1}}},
			} {
				if err := items.CreateIndex(context.Background(), index.name, index.fields, IndexOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			for index, sample := range samples {
				got := queryIDs(t, items, sample.filter, sample.options)
				if !reflect.DeepEqual(got, wants[index]) {
					t.Fatalf("sample=%d filter=%v got=%v want=%v", index, sample.filter, got, wants[index])
				}
				explain, err := items.ExplainWithOptions(context.Background(), sample.filter, sample.options)
				if err != nil {
					t.Fatal(err)
				}
				if sample.indexed && explain.Stage != "IXSCAN" {
					t.Fatalf("sample=%d explain=%+v", index, explain)
				}
				if !sample.indexed && explain.Stage != "COLLSCAN" {
					t.Fatalf("sample=%d explain=%+v", index, explain)
				}
			}
		})
	}
}

func TestDurableOrUnionMergesExactSpansForInsertionOrderLimit(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "or-limit.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	buckets := []int64{3, 1, 3, 1, 2, 1}
	documents := make([]Document, len(buckets))
	for index, bucket := range buckets {
		documents[index] = Document{"bucket": Int(bucket), "serial": Int(int64(index))}
	}
	ids, err := items.InsertMany(context.Background(), documents)
	if err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_bucket", []IndexField{{Field: "bucket", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_serial", []IndexField{{Field: "serial", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	filter := Filter{"$or": []Filter{{"bucket": int64(1)}, {"bucket": int64(3)}}}
	one := 1
	explain, err := items.ExplainWithOptions(context.Background(), filter, QueryOptions{Limit: &one})
	if err != nil {
		t.Fatal(err)
	}
	if explain.Stage != "IXSCAN" || explain.DocumentsExamined != 1 || explain.KeysExamined != 2 || explain.CandidatesRetained != 1 {
		t.Fatalf("limit explain=%+v", explain)
	}
	if got := queryIDs(t, items, filter, QueryOptions{Limit: &one}); !reflect.DeepEqual(got, []DocumentID{ids[0]}) {
		t.Fatalf("limit ids=%v", got)
	}

	two := 2
	explain, err = items.ExplainWithOptions(context.Background(), filter, QueryOptions{Skip: 1, Limit: &two})
	if err != nil {
		t.Fatal(err)
	}
	if explain.DocumentsExamined != 3 || explain.KeysExamined != 4 || explain.CandidatesRetained != 3 {
		t.Fatalf("skip/limit explain=%+v", explain)
	}
	if got := queryIDs(t, items, filter, QueryOptions{Skip: 1, Limit: &two}); !reflect.DeepEqual(got, []DocumentID{ids[1], ids[2]}) {
		t.Fatalf("skip/limit ids=%v", got)
	}

	overlap := Filter{"$or": []Filter{{"bucket": int64(3)}, {"serial": int64(0)}}}
	explain, err = items.ExplainWithOptions(context.Background(), overlap, QueryOptions{Limit: &one})
	if err != nil {
		t.Fatal(err)
	}
	if len(explain.IndexNames) != 2 || explain.DocumentsExamined != 1 || explain.KeysExamined != 2 {
		t.Fatalf("cross-index limit explain=%+v", explain)
	}
	if got := queryIDs(t, items, overlap, QueryOptions{Limit: &one}); !reflect.DeepEqual(got, []DocumentID{ids[0]}) {
		t.Fatalf("cross-index limit ids=%v", got)
	}

	explain, err = items.ExplainWithOptions(context.Background(), filter, QueryOptions{
		Sort:  []SortField{{Path: "serial", Direction: -1}},
		Limit: &one,
	})
	if err != nil {
		t.Fatal(err)
	}
	if explain.DocumentsExamined != 5 || explain.KeysExamined != 5 {
		t.Fatalf("sorted limit explain=%+v", explain)
	}
	if got := queryIDs(t, items, filter, QueryOptions{
		Sort: []SortField{{Path: "serial", Direction: -1}}, Limit: &one,
	}); !reflect.DeepEqual(got, []DocumentID{ids[5]}) {
		t.Fatalf("sorted limit ids=%v", got)
	}
}

func TestMemoryOrUnionLimitStopsAfterInsertionOrderWindow(t *testing.T) {
	db, err := NewWithOptions(DatabaseOptions{
		ResourceLimits: ResourceLimits{MaxQueryDocumentsExamined: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	documents := make([]Document, 100)
	for index := range documents {
		documents[index] = Document{"bucket": Int(int64(index % 10))}
	}
	ids, err := items.InsertMany(context.Background(), documents)
	if err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_bucket", []IndexField{{Field: "bucket", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	one := 1
	filter := Filter{"$or": []Filter{{"bucket": int64(1)}, {"bucket": int64(3)}}}
	explain, err := items.ExplainWithOptions(context.Background(), filter, QueryOptions{Limit: &one})
	if err != nil {
		t.Fatal(err)
	}
	if explain.Stage != "IXSCAN" || explain.DocumentsExamined != 1 || explain.KeysExamined != 20 || explain.CandidatesRetained != 1 {
		t.Fatalf("explain=%+v", explain)
	}
	if got := queryIDs(t, items, filter, QueryOptions{Limit: &one}); !reflect.DeepEqual(got, []DocumentID{ids[1]}) {
		t.Fatalf("ids=%v", got)
	}
}

func TestDurableSelectiveOrUnionRespectsDocumentBudget(t *testing.T) {
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "or-budget.meld2"), OpenOptions{
		ResourceLimits: ResourceLimits{MaxQueryDocumentsExamined: 25},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	documents := make([]Document, 100)
	for index := range documents {
		documents[index] = Document{"bucket": Int(int64(index % 10))}
	}
	if _, err := items.InsertMany(context.Background(), documents); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_bucket", []IndexField{{Field: "bucket", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	filter := Filter{"$or": []Filter{{"bucket": int64(1)}, {"bucket": int64(3)}}}
	cursor, err := items.Find(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	if documents, err := cursor.All(context.Background()); err != nil || len(documents) != 20 {
		t.Fatalf("indexed union documents=%d err=%v", len(documents), err)
	}
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := items.SnapshotQuery(context.Background(), query)
	if err != nil || len(snapshot.Documents) != 20 {
		t.Fatalf("snapshot documents=%d err=%v", len(snapshot.Documents), err)
	}
	update, err := items.UpdateMany(context.Background(), filter, Update{"$set": map[string]any{"selected": true}})
	if err != nil || update.MatchedCount != 20 || update.ModifiedCount != 20 {
		t.Fatalf("update=%+v err=%v", update, err)
	}
	cursor, err = items.Find(context.Background(), Filter{"unindexed": true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.All(context.Background()); !errors.Is(err, ErrQueryBudget) {
		t.Fatalf("collection scan error=%v", err)
	}
}

func TestExplainReportsOrSourcesDedupFallbackAndEarlyStop(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(t *testing.T) *DB { return New() },
		"durable": func(t *testing.T) *DB {
			db, err := Open(filepath.Join(t.TempDir(), "or-explain.meld2"))
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
			if _, err := items.InsertMany(context.Background(), []Document{
				{"a": Int(1), "b": Int(1)},
				{"a": Int(1), "b": Int(2)},
				{"a": Int(2), "b": Int(3)},
				{"a": Int(3), "special": String("only")},
			}); err != nil {
				t.Fatal(err)
			}
			for _, definition := range []struct {
				name, path string
			}{{"by_a", "a"}, {"by_b", "b"}} {
				if err := items.CreateIndex(context.Background(), definition.name, []IndexField{{Field: definition.path, Order: 1}}, IndexOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			union := Filter{"$or": []Filter{{"a": int64(1)}, {"b": int64(1)}}}
			explain, err := items.Explain(context.Background(), union)
			if err != nil {
				t.Fatal(err)
			}
			if explain.Stage != "IXSCAN" || explain.PlanReason != "multi_index_union" ||
				explain.KeysExamined != 3 || explain.CandidateIDs != 3 ||
				explain.UniqueCandidateIDs != 2 || explain.DuplicateCandidateIDs != 1 ||
				explain.DocumentsExamined != 2 || len(explain.Sources) != 2 {
				t.Fatalf("union explain=%+v", explain)
			}
			if explain.Budget.KeysUsed != 3 || explain.Budget.DocumentsUsed != 2 ||
				explain.Budget.KeysLimit == 0 || explain.Budget.DocumentsLimit == 0 {
				t.Fatalf("union budget=%+v", explain.Budget)
			}
			if explain.EarlyStopEligible || explain.EarlyStopReason != "limit_not_set" ||
				!hasExplainAdvice(explain, "high_union_overlap") {
				t.Fatalf("union early/advice=%+v advice=%+v", explain, explain.Advice)
			}
			var sourceKeys int64
			for _, source := range explain.Sources {
				sourceKeys += source.KeysExamined
				if source.IndexName == "" || source.Spans != 1 || source.ExactSpans != 1 {
					t.Fatalf("source=%+v", source)
				}
			}
			if sourceKeys != explain.KeysExamined {
				t.Fatalf("source keys=%d explain keys=%d", sourceKeys, explain.KeysExamined)
			}

			one := 1
			limited, err := items.ExplainWithOptions(context.Background(), union, QueryOptions{Limit: &one})
			if err != nil {
				t.Fatal(err)
			}
			if !limited.EarlyStopEligible || !limited.EarlyStopped || limited.DocumentsExamined != 1 ||
				limited.EarlyStopScope == "" {
				t.Fatalf("limited explain=%+v", limited)
			}
			before := db.Stats().Queries
			cursor, err := items.Find(context.Background(), union, QueryOptions{Limit: &one})
			if err != nil {
				t.Fatal(err)
			}
			if documents, err := cursor.All(context.Background()); err != nil || len(documents) != 1 {
				t.Fatalf("limited documents=%d err=%v", len(documents), err)
			}
			after := db.Stats().Queries
			if after.KeysExamined-before.KeysExamined != uint64(limited.KeysExamined) ||
				after.CandidateIDs-before.CandidateIDs != uint64(limited.CandidateIDs) ||
				after.UniqueCandidateIDs-before.UniqueCandidateIDs != uint64(limited.UniqueCandidateIDs) ||
				after.DuplicateCandidateIDs-before.DuplicateCandidateIDs != uint64(limited.DuplicateCandidateIDs) ||
				after.EarlyStops-before.EarlyStops != 1 {
				t.Fatalf("query stats before=%+v after=%+v explain=%+v", before, after, limited)
			}

			sorted, err := items.ExplainWithOptions(context.Background(), union, QueryOptions{
				Sort: []SortField{{Path: "score", Direction: 1}}, Limit: &one,
			})
			if err != nil {
				t.Fatal(err)
			}
			if sorted.EarlyStopEligible || sorted.EarlyStopReason != "sort_required" ||
				!hasExplainAdvice(sorted, "consider_sort_index") ||
				!hasExplainAdvice(sorted, "limit_requires_full_scan") {
				t.Fatalf("sorted explain=%+v", sorted)
			}

			rangeScan, err := items.ExplainWithOptions(context.Background(), Filter{
				"a": map[string]any{"$gte": int64(1)},
			}, QueryOptions{Limit: &one})
			if err != nil {
				t.Fatal(err)
			}
			if rangeScan.Stage != "IXSCAN" || rangeScan.EarlyStopEligible ||
				rangeScan.EarlyStopReason != "range_scan" ||
				!hasExplainAdvice(rangeScan, "limit_requires_full_scan") {
				t.Fatalf("range explain=%+v", rangeScan)
			}

			fallback := Filter{"$or": []Filter{{"a": int64(1)}, {"special": "only"}}}
			collectionScan, err := items.ExplainWithOptions(context.Background(), fallback, QueryOptions{Limit: &one})
			if err != nil {
				t.Fatal(err)
			}
			if collectionScan.Stage != "COLLSCAN" || collectionScan.FallbackReason != "unindexed_or_branch" ||
				!reflect.DeepEqual(collectionScan.UnindexedPaths, []string{"special"}) ||
				!hasExplainAdvice(collectionScan, "consider_filter_index") ||
				!collectionScan.EarlyStopEligible || !collectionScan.EarlyStopped ||
				collectionScan.DocumentsExamined != 1 {
				t.Fatalf("fallback explain=%+v", collectionScan)
			}
		})
	}
}

func TestNestedAndUsesSiblingIndexWhenOrBranchIsUnindexed(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(t *testing.T) *DB { return New() },
		"durable": func(t *testing.T) *DB {
			db, err := Open(filepath.Join(t.TempDir(), "nested-and-or.meld2"))
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
			if _, err := items.InsertMany(context.Background(), []Document{
				{"a": Int(1), "b": Int(1)},
				{"a": Int(2), "b": Int(1), "special": String("only")},
				{"a": Int(1), "b": Int(2)},
				{"a": Int(3), "b": Int(3)},
			}); err != nil {
				t.Fatal(err)
			}
			for _, definition := range []struct {
				name, path string
			}{{"by_a", "a"}, {"by_b", "b"}} {
				if err := items.CreateIndex(context.Background(), definition.name, []IndexField{{Field: definition.path, Order: 1}}, IndexOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			filter := Filter{"$and": []Filter{
				{"$or": []Filter{{"a": int64(1)}, {"special": "only"}}},
				{"b": int64(1)},
			}}
			explain, err := items.Explain(context.Background(), filter)
			if err != nil {
				t.Fatal(err)
			}
			if explain.Stage != "IXSCAN" || explain.IndexName != "by_b" ||
				explain.PlanReason != "secondary_index" || explain.FallbackReason != "" ||
				explain.KeysExamined != 2 || explain.DocumentsExamined != 2 ||
				len(explain.IndexableConjunctPaths) != 0 {
				t.Fatalf("nested AND/OR explain=%+v", explain)
			}
			cursor, err := items.Find(context.Background(), filter)
			if err != nil {
				t.Fatal(err)
			}
			documents, err := cursor.All(context.Background())
			if err != nil || len(documents) != 2 {
				t.Fatalf("nested AND/OR documents=%d err=%v", len(documents), err)
			}
		})
	}
}

func TestExplainReportsAndSingleSourceAmplificationAndCompoundControl(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(t *testing.T) *DB { return New() },
		"durable": func(t *testing.T) *DB {
			db, err := Open(filepath.Join(t.TempDir(), "and-explain.meld2"))
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
			documents := make([]Document, 256)
			for index := range documents {
				status := "closed"
				if (index/8)%8 == 0 {
					status = "open"
				}
				documents[index] = Document{
					"workspaceId": String(fmt.Sprintf("workspace-%02d", index%8)),
					"status":      String(status),
				}
			}
			if _, err := items.InsertMany(context.Background(), documents); err != nil {
				t.Fatal(err)
			}
			for _, definition := range []struct {
				name, path string
			}{{"by_status", "status"}, {"by_workspace", "workspaceId"}} {
				if err := items.CreateIndex(context.Background(), definition.name, []IndexField{{Field: definition.path, Order: 1}}, IndexOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			filter := Filter{"workspaceId": "workspace-01", "status": "open"}
			singleSource, err := items.Explain(context.Background(), filter)
			if err != nil {
				t.Fatal(err)
			}
			expectedPaths := []string{"status", "workspaceId"}
			if singleSource.Stage != "IXSCAN" || singleSource.IndexName != "by_status" ||
				singleSource.KeysExamined != 32 || singleSource.DocumentsExamined != 32 ||
				singleSource.CandidatesRetained != 4 || !singleSource.CompoundIndexOpportunity ||
				!reflect.DeepEqual(singleSource.IndexableConjunctPaths, expectedPaths) ||
				!hasExplainAdvice(singleSource, "consider_compound_index") {
				t.Fatalf("single-source AND explain=%+v advice=%+v", singleSource, singleSource.Advice)
			}
			var compoundAdvice ExplainAdvice
			for _, advice := range singleSource.Advice {
				if advice.Code == "consider_compound_index" {
					compoundAdvice = advice
				}
			}
			if !reflect.DeepEqual(compoundAdvice.Paths, expectedPaths) {
				t.Fatalf("compound advice=%+v", compoundAdvice)
			}
			one := 1
			limited, err := items.ExplainWithOptions(context.Background(), filter, QueryOptions{Limit: &one})
			if err != nil {
				t.Fatal(err)
			}
			if !limited.CompoundIndexOpportunity ||
				!reflect.DeepEqual(limited.IndexableConjunctPaths, expectedPaths) ||
				hasExplainAdvice(limited, "consider_compound_index") {
				t.Fatalf("limited AND advice should remain structural only: %+v advice=%+v", limited, limited.Advice)
			}

			if err := items.CreateIndex(context.Background(), "by_workspace_status", []IndexField{
				{Field: "workspaceId", Order: 1},
				{Field: "status", Order: 1},
			}, IndexOptions{}); err != nil {
				t.Fatal(err)
			}
			compound, err := items.Explain(context.Background(), filter)
			if err != nil {
				t.Fatal(err)
			}
			if compound.Stage != "IXSCAN" || compound.IndexName != "by_workspace_status" ||
				compound.KeysExamined != 4 || compound.DocumentsExamined != 4 ||
				compound.CandidatesRetained != 4 || compound.CompoundIndexOpportunity ||
				!reflect.DeepEqual(compound.IndexableConjunctPaths, expectedPaths) ||
				hasExplainAdvice(compound, "consider_compound_index") {
				t.Fatalf("compound AND explain=%+v advice=%+v", compound, compound.Advice)
			}
		})
	}
}

func hasExplainAdvice(explain ExplainResult, code string) bool {
	for _, advice := range explain.Advice {
		if advice.Code == code {
			return true
		}
	}
	return false
}

func TestExplainCollectionFallbackReasonsAreStructured(t *testing.T) {
	db := New()
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"a": Int(1)}); err != nil {
		t.Fatal(err)
	}

	missingIndex, err := items.Explain(context.Background(), Filter{"a": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if missingIndex.Stage != "COLLSCAN" || missingIndex.PlanReason != "collection_scan" ||
		missingIndex.FallbackReason != "no_secondary_indexes" ||
		!reflect.DeepEqual(missingIndex.UnindexedPaths, []string{"a"}) ||
		!hasExplainAdvice(missingIndex, "consider_filter_index") {
		t.Fatalf("missing index explain=%+v", missingIndex)
	}

	unfiltered, err := items.Explain(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if unfiltered.FallbackReason != "unfiltered" || len(unfiltered.UnindexedPaths) != 0 ||
		hasExplainAdvice(unfiltered, "consider_filter_index") {
		t.Fatalf("unfiltered explain=%+v", unfiltered)
	}

	if err := items.CreateIndex(context.Background(), "by_a", []IndexField{{Field: "a", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	unsupported, err := items.Explain(context.Background(), Filter{"a": map[string]any{"$ne": int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if unsupported.FallbackReason != "no_usable_index" || len(unsupported.UnindexedPaths) != 0 {
		t.Fatalf("unsupported predicate explain=%+v", unsupported)
	}
}
