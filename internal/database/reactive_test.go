package database

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReactiveQueryInitialRelevantAndIrrelevantChanges(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	collection := db.Collection("todos")
	if _, err := collection.InsertOne(context.Background(), Document{"done": Bool(false), "title": String("one")}); err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{"done": false}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subscription, err := collection.SubscribeQuery(ctx, query, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	initial := receiveSnapshot(t, subscription.Snapshots)
	if len(initial.Documents) != 1 || initial.Token != 1 {
		t.Fatalf("initial = %+v", initial)
	}
	if _, err := collection.InsertOne(context.Background(), Document{"done": Bool(true), "title": String("irrelevant")}); err != nil {
		t.Fatal(err)
	}
	select {
	case snapshot := <-subscription.Snapshots:
		t.Fatalf("irrelevant change emitted %+v", snapshot)
	case <-time.After(30 * time.Millisecond):
	}
	if _, err := collection.InsertOne(context.Background(), Document{"done": Bool(false), "title": String("two")}); err != nil {
		t.Fatal(err)
	}
	next := receiveSnapshot(t, subscription.Snapshots)
	if next.Token != 3 || len(next.Documents) != 2 {
		t.Fatalf("next = %+v", next)
	}
}

func TestReactiveQueryFailsBoundedSlowConsumer(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	collection := db.Collection("todos")
	query, _ := CompileQuery(Filter{}, QueryOptions{})
	subscription, err := collection.SubscribeQuery(context.Background(), query, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	// Leave the initial snapshot unread, filling the one-item output queue.
	if _, err := collection.InsertOne(context.Background(), Document{"done": Bool(false)}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-subscription.Errors:
		if !errors.Is(err, ErrSlowConsumer) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow consumer was not terminated")
	}
}

func TestAuthorizationConstraintRunsBeforeCallerPagination(t *testing.T) {
	documents := []Document{
		{"_id": String("a"), "workspace": String("other"), "rank": Int(1)},
		{"_id": String("b"), "workspace": String("mine"), "rank": Int(2)},
	}
	one := 1
	caller, _ := CompileQuery(Filter{}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: 1}}, Limit: &one})
	policy, _ := CompileQuery(Filter{"workspace": "mine"}, QueryOptions{})
	result := caller.Constrain(policy).Execute(documents)
	if len(result) != 1 {
		t.Fatalf("result length = %d", len(result))
	}
	id, _ := result[0]["_id"].StringValue()
	if id != "b" {
		t.Fatalf("policy applied after pagination, got %q", id)
	}
}

func receiveSnapshot(t *testing.T, snapshots <-chan QuerySnapshot) QuerySnapshot {
	t.Helper()
	select {
	case snapshot, ok := <-snapshots:
		if !ok {
			t.Fatal("snapshot channel closed")
		}
		return snapshot
	case <-time.After(time.Second):
		t.Fatal("snapshot timeout")
		return QuerySnapshot{}
	}
}
