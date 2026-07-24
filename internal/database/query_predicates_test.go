package database

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSizeAndTypePredicatesAcrossMemoryAndDurableExecution(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(*testing.T) *DB { return New() },
		"durable": func(t *testing.T) *DB {
			db, err := Open(filepath.Join(t.TempDir(), "query-predicates.meld2"))
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
			ids, err := items.InsertMany(context.Background(), []Document{
				{"scope": String("target"), "items": Array(Int(1), Int(2)), "value": Int(1)},
				{"scope": String("target"), "items": Array(), "value": Array()},
				{"scope": String("target"), "items": Array(Int(1), Int(2)), "value": Float(1)},
				{"scope": String("other"), "items": Array(Int(1), Int(2)), "value": Int(1)},
				{"scope": String("target"), "items": Object(Document{}), "value": Null()},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := items.CreateIndex(context.Background(), "by_scope", []IndexField{{Field: "scope", Order: 1}}, IndexOptions{}); err != nil {
				t.Fatal(err)
			}

			filter := Filter{
				"scope": "target",
				"items": map[string]any{"$size": 2},
				"value": map[string]any{"$type": []string{"array", "int64"}},
			}
			if got := queryIDs(t, items, filter, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{ids[0]}) {
				t.Fatalf("query ids=%v want=%v", got, []DocumentID{ids[0]})
			}
			explain, err := items.Explain(context.Background(), filter)
			if err != nil {
				t.Fatal(err)
			}
			if explain.Stage != "IXSCAN" || explain.IndexName != "by_scope" || !explain.ResidualPredicate {
				t.Fatalf("explain=%+v", explain)
			}
			if !reflect.DeepEqual(explain.UnindexedPaths, []string(nil)) {
				t.Fatalf("residual predicates were presented as B-tree opportunities: %+v", explain.UnindexedPaths)
			}
			residualOnly, err := items.Explain(context.Background(), Filter{
				"items": map[string]any{"$size": 2},
				"value": map[string]any{"$type": "int64"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if residualOnly.Stage != "COLLSCAN" || len(residualOnly.UnindexedPaths) != 0 {
				t.Fatalf("residual-only explain=%+v", residualOnly)
			}
			for _, advice := range residualOnly.Advice {
				if advice.Code == "consider_filter_index" {
					t.Fatalf("residual-only predicate produced B-tree advice: %+v", residualOnly.Advice)
				}
			}

			updated, err := items.UpdateMany(context.Background(), Filter{
				"scope": "target",
				"value": map[string]any{"$type": "float64"},
			}, Update{"$set": map[string]any{"updated": true}})
			if err != nil || updated.MatchedCount != 1 || updated.ModifiedCount != 1 {
				t.Fatalf("update=%+v err=%v", updated, err)
			}
			deleted, err := items.DeleteMany(context.Background(), Filter{
				"scope": "target",
				"items": map[string]any{"$size": 0},
			})
			if err != nil || deleted.DeletedCount != 1 {
				t.Fatalf("delete=%+v err=%v", deleted, err)
			}

			query, err := CompileQuery(filter, QueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			subscription, err := items.SubscribeQuery(context.Background(), query, 2)
			if err != nil {
				t.Fatal(err)
			}
			defer subscription.Close()
			if initial := receiveSnapshot(t, subscription.Snapshots); len(initial.Documents) != 1 {
				t.Fatalf("initial reactive documents=%d", len(initial.Documents))
			}
			if _, err := items.InsertOne(context.Background(), Document{
				"scope": String("target"), "items": Array(Int(3), Int(4)), "value": Int(2),
			}); err != nil {
				t.Fatal(err)
			}
			if next := receiveSnapshot(t, subscription.Snapshots); len(next.Documents) != 2 {
				t.Fatalf("updated reactive documents=%d", len(next.Documents))
			}
		})
	}
}

func TestAllPredicateAcrossMemoryAndDurableExecution(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(*testing.T) *DB { return New() },
		"durable": func(t *testing.T) *DB {
			db, err := Open(filepath.Join(t.TempDir(), "query-all.meld2"))
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
			ids, err := items.InsertMany(context.Background(), []Document{
				{"scope": String("target"), "tags": Array(String("one"), String("two"), String("three"))},
				{"scope": String("target"), "tags": Array(String("one"), String("two"))},
				{"scope": String("target"), "tags": Array(String("one"), String("three"))},
				{"scope": String("target"), "tags": String("one")},
				{"scope": String("target")},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := items.CreateIndex(context.Background(), "by_scope", []IndexField{{Field: "scope", Order: 1}}, IndexOptions{}); err != nil {
				t.Fatal(err)
			}
			filter := Filter{"scope": "target", "tags": map[string]any{"$all": []any{"one", "two", "one"}}}
			if got := queryIDs(t, items, filter, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{ids[0], ids[1]}) {
				t.Fatalf("query ids=%v want=%v", got, []DocumentID{ids[0], ids[1]})
			}
			explain, err := items.Explain(context.Background(), filter)
			if err != nil || explain.Stage != "IXSCAN" || !explain.ResidualPredicate {
				t.Fatalf("explain=%+v err=%v", explain, err)
			}
			if capabilities, want := mustQuery(t, filter).FilterCapabilities(), []FilterCapability{{Path: "scope", Operator: "eq"}, {Path: "tags", Operator: "all"}}; !reflect.DeepEqual(capabilities, want) {
				t.Fatalf("capabilities=%+v want=%+v", capabilities, want)
			}
			if _, err := items.UpdateMany(context.Background(), Filter{"tags": map[string]any{"$all": []any{"one", "three"}}}, Update{"$set": map[string]any{"all": true}}); err != nil {
				t.Fatal(err)
			}
			if got := queryIDs(t, items, Filter{"all": true}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{ids[0], ids[2]}) {
				t.Fatalf("updated ids=%v want=%v", got, []DocumentID{ids[0], ids[2]})
			}
		})
	}
}

func TestElemMatchKeepsScalarAndObjectConditionsOnOneArrayElement(t *testing.T) {
	constructors := map[string]func(*testing.T) *DB{
		"memory": func(*testing.T) *DB { return New() },
		"durable": func(t *testing.T) *DB {
			db, err := Open(filepath.Join(t.TempDir(), "query-elem-match.meld2"))
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
			ids, err := items.InsertMany(context.Background(), []Document{
				{"scope": String("target"), "scores": Array(Int(80), Int(93), Int(101)), "parts": Array(Object(Document{"kind": String("a"), "qty": Int(1)}), Object(Document{"kind": String("b"), "qty": Int(5)}))},
				{"scope": String("target"), "scores": Array(Int(85), Int(100)), "parts": Array(Object(Document{"kind": String("a"), "qty": Int(5)}))},
				{"scope": String("target"), "scores": Array(Int(91)), "parts": Array(Object(Document{"kind": String("a"), "qty": Int(2)}), Object(Document{"kind": String("b"), "qty": Int(9)}))},
				{"scope": String("target"), "scores": String("93"), "parts": Array()},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := items.CreateIndex(context.Background(), "by_scope", []IndexField{{Field: "scope", Order: 1}}, IndexOptions{}); err != nil {
				t.Fatal(err)
			}

			scalar := Filter{"scope": "target", "scores": map[string]any{"$elemMatch": map[string]any{"$gte": int64(90), "$lt": int64(100)}}}
			if got := queryIDs(t, items, scalar, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{ids[0], ids[2]}) {
				t.Fatalf("scalar ids=%v want=%v", got, []DocumentID{ids[0], ids[2]})
			}
			object := Filter{"scope": "target", "parts": map[string]any{"$elemMatch": map[string]any{"kind": "a", "qty": map[string]any{"$gte": int64(5)}}}}
			if got := queryIDs(t, items, object, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{ids[1]}) {
				t.Fatalf("object ids=%v want=%v", got, []DocumentID{ids[1]})
			}
			explain, err := items.Explain(context.Background(), object)
			if err != nil || explain.Stage != "IXSCAN" || !explain.ResidualPredicate {
				t.Fatalf("explain=%+v err=%v", explain, err)
			}
			query := mustQuery(t, scalar)
			if got, want := query.FilterCapabilities(), []FilterCapability{{Path: "scope", Operator: "eq"}, {Path: "scores", Operator: "elem_match"}}; !reflect.DeepEqual(got, want) {
				t.Fatalf("capabilities=%+v want=%+v", got, want)
			}
			if _, err := items.UpdateMany(context.Background(), object, Update{"$set": map[string]any{"same": true}}); err != nil {
				t.Fatal(err)
			}
			if got := queryIDs(t, items, Filter{"same": true}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{ids[1]}) {
				t.Fatalf("updated ids=%v want=%v", got, []DocumentID{ids[1]})
			}
		})
	}
}

func mustQuery(t *testing.T, filter Filter) QuerySpec {
	t.Helper()
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return query
}
