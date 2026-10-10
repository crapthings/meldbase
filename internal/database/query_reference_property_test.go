package database

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"testing"
)

// TestRandomIndexedCRUDQueriesMatchReferenceModel checks indexed queries after randomized document edits against an independent model in memory and durable databases.
func TestRandomIndexedCRUDQueriesMatchReferenceModel(t *testing.T) {
	type model struct {
		id       DocumentID
		bucket   int64
		state    string
		priority int64
		alive    bool
	}
	states := []string{"new", "open", "held", "done", "archived"}
	for _, durable := range []bool{false, true} {
		backend := "memory"
		if durable {
			backend = "durable"
		}
		for seed := uint64(1); seed <= 4; seed++ {
			t.Run(fmt.Sprintf("%s/seed-%d", backend, seed), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "reference-model.meld2")
				var db *DB
				var err error
				if durable {
					db, err = Open(path)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					db = New()
				}
				defer func() { _ = db.Close() }()
				items := db.Collection("items")
				random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
				models := make([]model, 48)
				documents := make([]Document, len(models))
				for index := range models {
					models[index].bucket = int64(random.Uint64() % 9)
					models[index].state = states[random.Uint64()%uint64(len(states))]
					models[index].priority = int64(random.Uint64() % 31)
					models[index].alive = true
					documents[index] = Document{
						"bucket":   Int(models[index].bucket),
						"state":    String(models[index].state),
						"priority": Int(models[index].priority),
					}
				}
				ids, err := items.InsertMany(context.Background(), documents)
				if err != nil {
					t.Fatal(err)
				}
				for index := range models {
					models[index].id = ids[index]
				}
				for _, definition := range []struct{ name, field string }{
					{"by_bucket", "bucket"}, {"by_state", "state"}, {"by_priority", "priority"},
				} {
					if err := items.CreateIndex(context.Background(), definition.name, []IndexField{{Field: definition.field, Order: 1}}, IndexOptions{}); err != nil {
						t.Fatal(err)
					}
				}

				aliveIndexes := func() []int {
					indexes := make([]int, 0, len(models))
					for index, value := range models {
						if value.alive {
							indexes = append(indexes, index)
						}
					}
					return indexes
				}
				assertQuery := func(filter Filter, matches func(model) bool) {
					t.Helper()
					want := make([]DocumentID, 0)
					for _, value := range models {
						if value.alive && matches(value) {
							want = append(want, value.id)
						}
					}
					got := queryIDs(t, items, filter, QueryOptions{})
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("filter=%v got=%v want=%v", filter, got, want)
					}
					explain, err := items.Explain(context.Background(), filter)
					if err != nil || explain.Stage != "IXSCAN" {
						t.Fatalf("filter=%v explain=%+v err=%v", filter, explain, err)
					}
				}
				assertRandomQueries := func() {
					bucket := int64(random.Uint64() % 9)
					state := states[random.Uint64()%uint64(len(states))]
					lower := int64(random.Uint64() % 24)
					upper := lower + int64(random.Uint64()%8)
					priority := int64(random.Uint64() % 31)
					assertQuery(Filter{"bucket": bucket}, func(value model) bool { return value.bucket == bucket })
					assertQuery(Filter{"state": state}, func(value model) bool { return value.state == state })
					assertQuery(Filter{"priority": map[string]any{"$gte": lower, "$lte": upper}}, func(value model) bool {
						return value.priority >= lower && value.priority <= upper
					})
					assertQuery(Filter{"bucket": bucket, "state": state}, func(value model) bool {
						return value.bucket == bucket && value.state == state
					})
					assertQuery(Filter{"$or": []Filter{{"bucket": bucket}, {"priority": priority}}}, func(value model) bool {
						return value.bucket == bucket || value.priority == priority
					})
				}

				for operation := 0; operation < 140; operation++ {
					alive := aliveIndexes()
					switch random.Uint64() % 3 {
					case 0:
						if len(alive) == 0 {
							continue
						}
						index := alive[random.Uint64()%uint64(len(alive))]
						bucket := int64(random.Uint64() % 9)
						state := states[random.Uint64()%uint64(len(states))]
						priority := int64(random.Uint64() % 31)
						if _, err := items.UpdateOne(context.Background(), Filter{"_id": models[index].id}, Update{"$set": map[string]any{
							"bucket": bucket, "state": state, "priority": priority,
						}}); err != nil {
							t.Fatalf("operation=%d update=%v", operation, err)
						}
						models[index].bucket, models[index].state, models[index].priority = bucket, state, priority
					case 1:
						if len(alive) == 0 {
							continue
						}
						index := alive[random.Uint64()%uint64(len(alive))]
						result, err := items.DeleteOne(context.Background(), Filter{"_id": models[index].id})
						if err != nil || result.DeletedCount != 1 {
							t.Fatalf("operation=%d delete=%+v err=%v", operation, result, err)
						}
						models[index].alive = false
					default:
						value := model{
							bucket: int64(random.Uint64() % 9), state: states[random.Uint64()%uint64(len(states))],
							priority: int64(random.Uint64() % 31), alive: true,
						}
						id, err := items.InsertOne(context.Background(), Document{
							"bucket": Int(value.bucket), "state": String(value.state), "priority": Int(value.priority),
						})
						if err != nil {
							t.Fatalf("operation=%d insert=%v", operation, err)
						}
						value.id = id
						models = append(models, value)
					}
					assertRandomQueries()
					if durable && operation == 69 {
						if err := db.Close(); err != nil {
							t.Fatal(err)
						}
						db, err = Open(path)
						if err != nil {
							t.Fatal(err)
						}
						items = db.Collection("items")
						assertRandomQueries()
					}
				}
			})
		}
	}
}
