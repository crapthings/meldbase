package meldbase

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestCollectionCRUDAndQuerySemantics(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	users := db.Collection("users")
	ids := make([]DocumentID, 3)
	documents := []Document{
		{"name": String("Ada"), "age": Int(17), "profile": Object(Document{"city": String("Shanghai")}), "tags": Array(String("new")), "score": Int(1)},
		{"name": String("Lin"), "age": Int(24), "profile": Object(Document{"city": String("Hangzhou")}), "tags": Array(String("active"), String("new")), "score": Int(1)},
		{"name": String("Sam"), "age": Int(30), "profile": Object(Document{"city": String("Shanghai")}), "tags": Array(), "score": Int(1)},
	}
	for i, document := range documents {
		id, err := users.InsertOne(context.Background(), document)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	limit := 2
	cursor, err := users.Find(context.Background(), Filter{
		"age": map[string]any{"$gte": int64(18)},
		"$or": []Filter{{"profile.city": "Shanghai"}, {"tags": "active"}},
	}, QueryOptions{Sort: []SortField{{Path: "age", Direction: -1}}, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	result, err := cursor.All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	gotNames := []string{}
	for _, document := range result {
		name, _ := document["name"].StringValue()
		gotNames = append(gotNames, name)
	}
	if !reflect.DeepEqual(gotNames, []string{"Sam", "Lin"}) {
		t.Fatalf("names = %v", gotNames)
	}

	update, err := users.UpdateMany(context.Background(), Filter{"age": map[string]any{"$gte": int64(18)}}, Update{
		"$inc": map[string]any{"score": int64(2)}, "$set": map[string]any{"status": "adult"}, "$push": map[string]any{"tags": "checked"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if update.MatchedCount != 2 || update.ModifiedCount != 2 {
		t.Fatalf("update = %+v", update)
	}
	found, err := users.FindOne(context.Background(), Filter{"_id": ids[1]})
	if err != nil {
		t.Fatal(err)
	}
	if score, _ := found["score"].Int64(); score != 3 {
		t.Fatalf("score = %d", score)
	}
	if status, _ := found["status"].StringValue(); status != "adult" {
		t.Fatalf("status = %q", status)
	}

	deleted, err := users.DeleteMany(context.Background(), Filter{"status": "adult"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted.DeletedCount != 2 {
		t.Fatalf("deleted = %d", deleted.DeletedCount)
	}
	remaining, err := users.Find(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	all, err := remaining.All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("remaining = %d", len(all))
	}
}

func TestStorageIsolationAndIDRules(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	collection := db.Collection("items")
	document := Document{"nested": Object(Document{"value": Int(1)})}
	id, err := collection.InsertOne(context.Background(), document)
	if err != nil {
		t.Fatal(err)
	}
	document["nested"] = Null()
	found, err := collection.FindOne(context.Background(), Filter{"_id": id.String()})
	if err != nil {
		t.Fatal(err)
	}
	if found["nested"].kind != ObjectKind {
		t.Fatal("caller mutation leaked into storage")
	}
	found["nested"] = Null()
	again, err := collection.FindOne(context.Background(), Filter{"_id": id})
	if err != nil {
		t.Fatal(err)
	}
	if again["nested"].kind != ObjectKind {
		t.Fatal("result mutation leaked into storage")
	}
	if _, err := collection.InsertOne(context.Background(), Document{"_id": ID(id)}); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := collection.InsertOne(context.Background(), Document{"_id": String(id.String())}); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("string id error = %v", err)
	}
}

func TestUpdateManyIsAtomicAndRejectsAmbiguousPaths(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	collection := db.Collection("items")
	if _, err := collection.InsertOne(context.Background(), Document{"n": Int(math.MaxInt64)}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.InsertOne(context.Background(), Document{"n": Int(1)}); err != nil {
		t.Fatal(err)
	}
	_, err := collection.UpdateMany(context.Background(), Filter{}, Update{"$inc": map[string]any{"n": int64(1)}})
	if !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("overflow error = %v", err)
	}
	cursor, _ := collection.Find(context.Background(), Filter{}, QueryOptions{Sort: []SortField{{Path: "n", Direction: 1}}})
	documents, _ := cursor.All(context.Background())
	first, _ := documents[0]["n"].Int64()
	if first != 1 {
		t.Fatalf("atomic update changed second document: %d", first)
	}
	_, err = collection.UpdateMany(context.Background(), Filter{}, Update{"$set": map[string]any{"profile": map[string]any{}, "profile.city": "x"}})
	if !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("path conflict error = %v", err)
	}
}

func TestMutationAffectedLimitRejectsAtomically(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	collection := db.Collection("items")
	for i := int64(0); i < 2; i++ {
		if _, err := collection.InsertOne(context.Background(), Document{"n": Int(i)}); err != nil {
			t.Fatal(err)
		}
	}
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := CompileUpdate(Update{"$set": map[string]any{"changed": true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.UpdateManyQueryLimited(context.Background(), query, mutation, 1); !errors.Is(err, ErrMutationLimit) {
		t.Fatalf("update limit error = %v", err)
	}
	documents, err := collection.FindQuery(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	all, err := documents.All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, document := range all {
		if _, changed := document["changed"]; changed {
			t.Fatal("rejected update changed a document")
		}
	}
	if _, err := collection.DeleteManyQueryLimited(context.Background(), query, 1); !errors.Is(err, ErrMutationLimit) {
		t.Fatalf("delete limit error = %v", err)
	}
	documents, _ = collection.FindQuery(context.Background(), query)
	all, _ = documents.All(context.Background())
	if len(all) != 2 {
		t.Fatalf("rejected delete left %d documents", len(all))
	}
}

func TestInsertManyIsAtomicAcrossIDsIndexesAndChangeFeed(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	collection := db.Collection("items")
	if err := collection.CreateIndex(context.Background(), "items_email", []IndexField{{Field: "email", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batches, _, err := db.WatchChanges(ctx, "items", 2)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := collection.InsertMany(context.Background(), []Document{
		{"email": String("a@example.com")},
		{"email": String("b@example.com")},
	})
	if err != nil || len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("insert ids=%v err=%v", ids, err)
	}
	batch := <-batches
	if len(batch.Changes) != 2 || batch.Changes[0].Operation != InsertOperation || batch.Changes[1].Operation != InsertOperation {
		t.Fatalf("batch = %+v", batch)
	}
	if _, err := collection.InsertMany(context.Background(), []Document{
		{"email": String("c@example.com")},
		{"email": String("c@example.com")},
	}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate batch error = %v", err)
	}
	cursor, err := collection.Find(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	documents, err := cursor.All(context.Background())
	if err != nil || len(documents) != 2 {
		t.Fatalf("documents=%d err=%v", len(documents), err)
	}
}

func TestChangeFeedPublishesOrderedAtomicBatches(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	collection := db.Collection("items")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batches, done, err := db.WatchChanges(ctx, "items", 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if _, err := collection.InsertOne(context.Background(), Document{"n": Int(i)}); err != nil {
			t.Fatal(err)
		}
	}
	first := <-batches
	second := <-batches
	if first.Token != 1 || second.Token != 2 {
		t.Fatalf("tokens = %d, %d", first.Token, second.Token)
	}
	if len(first.Changes) != 1 || len(second.Changes) != 1 {
		t.Fatal("insert batches malformed")
	}
	if _, err := collection.UpdateMany(context.Background(), Filter{}, Update{"$set": map[string]any{"ready": true}}); err != nil {
		t.Fatal(err)
	}
	update := <-batches
	if update.Token != 3 || len(update.Changes) != 2 {
		t.Fatalf("update batch = %+v", update)
	}
	for _, change := range update.Changes {
		if change.Operation != UpdateOperation {
			t.Fatalf("operation = %s", change.Operation)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("done error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("watcher did not close")
	}
}

func TestClosedAndCancelledOperationsFail(t *testing.T) {
	db := New()
	collection := db.Collection("items")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := collection.InsertOne(ctx, Document{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Find(context.Background(), Filter{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed error = %v", err)
	}
	if _, err := db.Collection("bad/name").InsertOne(context.Background(), Document{}); !errors.Is(err, ErrInvalidCollection) {
		t.Fatalf("collection error = %v", err)
	}
}

func TestCompileQueryRejectsNonFiniteAndUnsafeValues(t *testing.T) {
	if _, err := CompileQuery(Filter{"x": math.NaN()}, QueryOptions{}); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("NaN error = %v", err)
	}
	if _, err := CompileQuery(Filter{"x": map[string]any{"constructor": true}}, QueryOptions{}); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("unsafe object error = %v", err)
	}
}
