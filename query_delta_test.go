package meldbase

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestQueryDeltaDeterministicallyTransformsOrderedResults(t *testing.T) {
	old := deltaDocuments(1, 2, 3, 4, 5)
	next := []Document{
		deltaDocument(3, "changed"), deltaDocument(6, "added"), deltaDocument(2, "same"), deltaDocument(5, "same"),
	}
	payload, err := buildSharedQueryDelta(old, next, 9)
	if err != nil {
		t.Fatal(err)
	}
	delta := cloneSharedQueryDelta(payload, 7)
	applied, err := ApplyQueryDelta(QuerySnapshot{Token: 7, Documents: old}, delta)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Token != 9 || !documentSlicesEqual(applied.Documents, next) {
		t.Fatalf("applied=%+v next=%+v operations=%+v", applied, next, delta.Operations)
	}
	again, err := buildSharedQueryDelta(old, next, 9)
	if err != nil || !reflect.DeepEqual(payload.operations, again.operations) {
		t.Fatalf("delta is not deterministic: err=%v first=%+v second=%+v", err, payload.operations, again.operations)
	}
	// Public operations are deep clones and cannot mutate the shared payload.
	for index := range delta.Operations {
		if delta.Operations[index].Document != nil {
			delta.Operations[index].Document["value"] = String("mutated")
			if value, _ := payload.operations[index].Document["value"].StringValue(); value == "mutated" {
				t.Fatal("public delta aliases shared document")
			}
			break
		}
	}
}

func TestQueryDeltaRandomizedPermutationAddRemoveAndChange(t *testing.T) {
	random := rand.New(rand.NewSource(20260715))
	current := deltaDocuments(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	token := uint64(1)
	for iteration := 0; iteration < 1_000; iteration++ {
		pool := make([]int, 0, 20)
		for id := 1; id <= 20; id++ {
			if random.Intn(3) != 0 {
				pool = append(pool, id)
			}
		}
		random.Shuffle(len(pool), func(left, right int) { pool[left], pool[right] = pool[right], pool[left] })
		next := make([]Document, len(pool))
		for index, id := range pool {
			value := "same"
			if random.Intn(4) == 0 {
				value = "changed"
			}
			next[index] = deltaDocument(id, value)
		}
		payload, err := buildSharedQueryDelta(current, next, token+1)
		if err != nil {
			t.Fatalf("iteration %d: %v", iteration, err)
		}
		if len(payload.operations) == 0 {
			if !documentSlicesEqual(current, next) {
				t.Fatalf("iteration %d produced empty delta for changed results", iteration)
			}
			continue
		}
		applied, err := ApplyQueryDelta(QuerySnapshot{Token: token, Documents: current}, cloneSharedQueryDelta(payload, token))
		if err != nil || !documentSlicesEqual(applied.Documents, next) {
			t.Fatalf("iteration %d err=%v operations=%+v", iteration, err, payload.operations)
		}
		current, token = next, token+1
	}
}

func TestApplyQueryDeltaRejectsAmbiguousOrInvalidOperations(t *testing.T) {
	base := QuerySnapshot{Token: 5, Documents: deltaDocuments(1, 2)}
	validAdd := QueryDeltaOperation{Kind: QueryDeltaAdd, DocumentID: deterministicDocumentID(2), Document: deltaDocument(3, "same")}
	tests := []QueryDelta{
		{FromToken: 4, Token: 6, Operations: []QueryDeltaOperation{validAdd}},
		{FromToken: 5, Token: 5, Operations: []QueryDeltaOperation{validAdd}},
		{FromToken: 5, Token: 6},
		{FromToken: 5, Token: 6, Operations: []QueryDeltaOperation{{Kind: QueryDeltaRemove, DocumentID: deterministicDocumentID(9)}}},
		{FromToken: 5, Token: 6, Operations: []QueryDeltaOperation{{Kind: QueryDeltaAdd, DocumentID: deterministicDocumentID(0), Document: deltaDocument(2, "same")}}},
		{FromToken: 5, Token: 6, Operations: []QueryDeltaOperation{{Kind: QueryDeltaMove, DocumentID: deterministicDocumentID(1)}}},
		{FromToken: 5, Token: 6, Operations: []QueryDeltaOperation{{Kind: QueryDeltaChange, DocumentID: deterministicDocumentID(0), Document: deltaDocument(2, "same")}}},
	}
	for index, delta := range tests {
		if _, err := ApplyQueryDelta(base, delta); !errors.Is(err, ErrInvalidDelta) {
			t.Fatalf("case %d error=%v", index, err)
		}
	}
}

func TestSharedViewGeneratesOneDeltaForMultipleSubscribers(t *testing.T) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	documents := make([]Document, 5)
	for index := range documents {
		documents[index] = Document{"score": Int(int64(index + 1)), "value": String("original")}
	}
	ids, err := collection.InsertMany(context.Background(), documents)
	if err != nil {
		t.Fatal(err)
	}
	limit := 3
	query, err := CompileQuery(Filter{}, QueryOptions{Sort: []SortField{{Path: "score", Direction: 1}}, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	first, err := collection.SubscribeQueryDeltas(context.Background(), query, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := collection.SubscribeQueryDeltas(context.Background(), query, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	compatibility, err := collection.SubscribeQuery(context.Background(), query, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer compatibility.Close()
	compatibilityState := receiveSnapshot(t, compatibility.Snapshots)
	firstState, secondState := first.Initial, second.Initial
	if !documentSlicesEqual(firstState.Documents, compatibilityState.Documents) || !documentSlicesEqual(secondState.Documents, compatibilityState.Documents) {
		t.Fatal("delta initial snapshots differ from compatibility view")
	}

	mutations := []func() error{
		func() error {
			_, err := collection.UpdateOne(context.Background(), Filter{"_id": ids[4]}, Update{"$set": map[string]any{"score": int64(-1)}})
			return err
		},
		func() error {
			_, err := collection.UpdateOne(context.Background(), Filter{"_id": ids[0]}, Update{"$set": map[string]any{"score": int64(100)}})
			return err
		},
		func() error {
			_, err := collection.UpdateOne(context.Background(), Filter{"_id": ids[1]}, Update{"$set": map[string]any{"value": "changed"}})
			return err
		},
		func() error {
			_, err := collection.DeleteOne(context.Background(), Filter{"_id": ids[4]})
			return err
		},
	}
	for iteration, mutate := range mutations {
		if err := mutate(); err != nil {
			t.Fatalf("mutation %d: %v", iteration, err)
		}
		firstDelta := receiveQueryDelta(t, first.Deltas)
		secondDelta := receiveQueryDelta(t, second.Deltas)
		firstState, err = ApplyQueryDelta(firstState, firstDelta)
		if err != nil {
			t.Fatalf("first delta %d: %v", iteration, err)
		}
		secondState, err = ApplyQueryDelta(secondState, secondDelta)
		if err != nil {
			t.Fatalf("second delta %d: %v", iteration, err)
		}
		compatibilityState = receiveSnapshot(t, compatibility.Snapshots)
		full, err := collection.SnapshotQuery(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		if !documentSlicesEqual(firstState.Documents, full.Documents) || !documentSlicesEqual(secondState.Documents, full.Documents) ||
			!documentSlicesEqual(compatibilityState.Documents, full.Documents) {
			t.Fatalf("mutation %d diverged from full query", iteration)
		}
		if iteration == 0 {
			for index := range firstDelta.Operations {
				if firstDelta.Operations[index].Document != nil {
					firstDelta.Operations[index].Document["value"] = String("mutated")
					for _, operation := range secondDelta.Operations {
						if operation.DocumentID == firstDelta.Operations[index].DocumentID && operation.Document != nil {
							value, _ := operation.Document["value"].StringValue()
							if value == "mutated" {
								t.Fatal("delta subscribers share mutable documents")
							}
						}
					}
					break
				}
			}
		}
	}
	stats := db.Stats().Realtime
	if stats.SharedViews != 1 || stats.QuerySubscribers != 3 || stats.SharedDeltas != uint64(len(mutations)) ||
		stats.DeltaDeliveries != uint64(2*len(mutations)) || stats.DeltaOperations == 0 {
		t.Fatalf("delta sharing stats = %+v", stats)
	}
}

func receiveQueryDelta(t *testing.T, deltas <-chan QueryDelta) QueryDelta {
	t.Helper()
	select {
	case delta, ok := <-deltas:
		if !ok {
			t.Fatal("delta channel closed")
		}
		return delta
	case <-time.After(time.Second):
		t.Fatal("delta timeout")
		return QueryDelta{}
	}
}

func deltaDocuments(ids ...int) []Document {
	result := make([]Document, len(ids))
	for index, id := range ids {
		result[index] = deltaDocument(id, "same")
	}
	return result
}

func deltaDocument(id int, value string) Document {
	return Document{"_id": ID(deterministicDocumentID(id - 1)), "value": String(value)}
}
