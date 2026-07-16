package meldbase

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestSharedReactiveViewReusesOneQueryAndIsolatesSnapshots(t *testing.T) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"active": Bool(false), "name": String("original")})
	if err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{"active": true}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	const count = 100
	subscriptions := make([]*QuerySubscription, count)
	for index := range subscriptions {
		subscriptions[index], err = collection.SubscribeQuery(context.Background(), query, 2)
		if err != nil {
			t.Fatal(err)
		}
		if initial := receiveSnapshot(t, subscriptions[index].Snapshots); len(initial.Documents) != 0 || initial.Token != 1 {
			t.Fatalf("initial %d = %+v", index, initial)
		}
	}
	stats := db.Stats()
	if stats.Realtime.SharedViews != 1 || stats.Realtime.QuerySubscribers != count || stats.Realtime.SharedViewReuses != count-1 {
		t.Fatalf("shared registration stats = %+v", stats.Realtime)
	}
	if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"active": true}}); err != nil {
		t.Fatal(err)
	}
	results := make([]QuerySnapshot, count)
	for index, subscription := range subscriptions {
		results[index] = receiveSnapshot(t, subscription.Snapshots)
		if results[index].Token != 2 || len(results[index].Documents) != 1 {
			t.Fatalf("result %d = %+v", index, results[index])
		}
	}
	// Public snapshots contain mutable maps. Mutating one subscriber's result
	// must never modify the shared view or another subscriber's result.
	results[0].Documents[0]["name"] = String("mutated")
	name, _ := results[1].Documents[0]["name"].StringValue()
	if name != "original" {
		t.Fatalf("subscriber snapshots alias: %q", name)
	}
	stats = db.Stats()
	if stats.Realtime.QueryRecomputes != 1 || stats.Realtime.WatcherDeliveries != 1 {
		t.Fatalf("fan-out performed per-subscriber work: %+v", stats.Realtime)
	}
	for _, subscription := range subscriptions {
		subscription.Close()
	}
	waitForRealtimeStats(t, db, func(stats RealtimeStats) bool {
		return stats.SharedViews == 0 && stats.QuerySubscribers == 0
	})
}

func TestSharedReactiveViewDropsOnlySlowSubscriber(t *testing.T) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"active": Bool(false)})
	if err != nil {
		t.Fatal(err)
	}
	query, _ := CompileQuery(Filter{"active": true}, QueryOptions{})
	slow, err := collection.SubscribeQuery(context.Background(), query, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	fast, err := collection.SubscribeQuery(context.Background(), query, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	_ = receiveSnapshot(t, fast.Snapshots)

	if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"active": true}}); err != nil {
		t.Fatal(err)
	}
	updated := receiveSnapshot(t, fast.Snapshots)
	if len(updated.Documents) != 1 {
		t.Fatalf("fast subscriber update = %+v", updated)
	}
	select {
	case err := <-slow.Errors:
		if !errors.Is(err, ErrSlowConsumer) {
			t.Fatalf("slow subscriber error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow subscriber remained attached")
	}
	waitForRealtimeStats(t, db, func(stats RealtimeStats) bool {
		return stats.SharedViews == 1 && stats.QuerySubscribers == 1 && stats.SlowConsumers == 1
	})
}

func TestSharedSubscriberNeverRegressesSnapshotToken(t *testing.T) {
	subscriber := &sharedQuerySubscriber{
		snapshots: make(chan QuerySnapshot, 2), errors: make(chan error, 1), lastToken: 5,
	}
	if subscriber.deliver(QuerySnapshot{Token: 4}, nil) != reactiveDeliverySkipped || len(subscriber.snapshots) != 0 {
		t.Fatal("stale snapshot was delivered")
	}
	if subscriber.deliver(QuerySnapshot{Token: 6}, nil) != reactiveDeliverySent || len(subscriber.snapshots) != 1 {
		t.Fatal("new snapshot was not delivered")
	}
	if snapshot := <-subscriber.snapshots; snapshot.Token != 6 {
		t.Fatalf("snapshot token = %d", snapshot.Token)
	}
}

func TestIncrementalReactiveViewsMatchFullQueryModel(t *testing.T) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	documents := make([]Document, 120)
	for index := range documents {
		documents[index] = Document{
			"group": Bool(index%3 == 0), "score": Int(int64(index % 17)), "name": String(fmt.Sprintf("item-%03d", index)),
		}
	}
	ids, err := collection.InsertMany(context.Background(), documents)
	if err != nil {
		t.Fatal(err)
	}
	one, seven := 1, 7
	queries := []QuerySpec{}
	for _, definition := range []struct {
		filter  Filter
		options QueryOptions
	}{
		{filter: Filter{"group": true}},
		{filter: Filter{}, options: QueryOptions{Sort: []SortField{{Path: "score", Direction: 1}, {Path: "name", Direction: -1}}, Skip: 3, Limit: &one}},
		{filter: Filter{"group": false}, options: QueryOptions{Sort: []SortField{{Path: "score", Direction: -1}}, Skip: 2, Limit: &seven}},
	} {
		query, err := CompileQuery(definition.filter, definition.options)
		if err != nil {
			t.Fatal(err)
		}
		queries = append(queries, query)
	}
	subscriptions := make([]*QuerySubscription, len(queries))
	expected := make([]QuerySnapshot, len(queries))
	for index, query := range queries {
		subscriptions[index], err = collection.SubscribeQuery(context.Background(), query, 8)
		if err != nil {
			t.Fatal(err)
		}
		defer subscriptions[index].Close()
		expected[index] = receiveSnapshot(t, subscriptions[index].Snapshots)
	}

	random := rand.New(rand.NewSource(20260715))
	for operation := 0; operation < 400; operation++ {
		switch random.Intn(5) {
		case 0:
			id, err := collection.InsertOne(context.Background(), Document{
				"group": Bool(random.Intn(2) == 0), "score": Int(int64(random.Intn(23))),
				"name": String(fmt.Sprintf("new-%03d", operation)),
			})
			if err != nil {
				t.Fatalf("insert %d: %v", operation, err)
			}
			ids = append(ids, id)
		case 1:
			if len(ids) > 0 {
				position := random.Intn(len(ids))
				if _, err := collection.DeleteOne(context.Background(), Filter{"_id": ids[position]}); err != nil {
					t.Fatalf("delete %d: %v", operation, err)
				}
				ids = append(ids[:position], ids[position+1:]...)
			}
		default:
			if len(ids) > 0 {
				id := ids[random.Intn(len(ids))]
				if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{
					"group": random.Intn(2) == 0, "score": int64(random.Intn(23)),
					"name": fmt.Sprintf("updated-%03d-%02d", operation, random.Intn(11)),
				}}); err != nil {
					t.Fatalf("update %d: %v", operation, err)
				}
			}
		}
		for index, query := range queries {
			full, err := collection.SnapshotQuery(context.Background(), query)
			if err != nil {
				t.Fatal(err)
			}
			if documentSlicesEqual(expected[index].Documents, full.Documents) {
				continue
			}
			actual := receiveSnapshot(t, subscriptions[index].Snapshots)
			if actual.Token != full.Token || !documentSlicesEqual(actual.Documents, full.Documents) {
				t.Fatalf("operation=%d query=%d token=%d/%d incremental=%+v full=%+v", operation, index, actual.Token, full.Token, actual.Documents, full.Documents)
			}
			expected[index] = full
		}
	}
	stats := db.Stats().Realtime
	if stats.IncrementalBatches == 0 || stats.IncrementalViewUpdates == 0 || stats.FullViewRecomputes != 0 {
		t.Fatalf("incremental stats = %+v", stats)
	}
}

func TestReactiveQueueOverflowFallsBackToFullRecompute(t *testing.T) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	query, _ := CompileQuery(Filter{}, QueryOptions{})
	subscription, err := collection.SubscribeQuery(context.Background(), query, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	_ = receiveSnapshot(t, subscription.Snapshots)
	db.reactive.mu.Lock()
	db.reactive.maxChanges = 1
	db.reactive.mu.Unlock()
	if _, err := collection.InsertMany(context.Background(), []Document{{"n": Int(1)}, {"n": Int(2)}}); err != nil {
		t.Fatal(err)
	}
	snapshot := receiveSnapshot(t, subscription.Snapshots)
	if len(snapshot.Documents) != 2 {
		t.Fatalf("fallback snapshot = %+v", snapshot)
	}
	stats := db.Stats().Realtime
	if stats.QueueOverflows != 1 || stats.FullViewRecomputes != 1 {
		t.Fatalf("overflow stats = %+v", stats)
	}
}

func waitForRealtimeStats(t *testing.T, db *DB, predicate func(RealtimeStats) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if predicate(db.Stats().Realtime) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("realtime stats did not converge: %+v", db.Stats().Realtime)
}
