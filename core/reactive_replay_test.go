package meldbase

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	storage "github.com/crapthings/meldbase/internal/storage"
)

func TestHistoricalReplayUsesSharedReactiveTransitions(t *testing.T) {
	file, _, err := storage.Open(filepath.Join(t.TempDir(), "reactive-replay.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	a, b, c, other := DocumentID{2}, DocumentID{1}, DocumentID{3}, DocumentID{9}
	doc := func(id DocumentID, score int64, active bool) []byte {
		encoded, err := encodeStoredDocument(Document{"_id": ID(id), "score": Int(score), "active": Bool(active)})
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	apply := func(sequence byte, mutations ...storage.DocumentMutation) {
		t.Helper()
		transactionID := [16]byte{sequence}
		got, err := file.ApplyDocumentTransaction(storage.DocumentTransaction{TransactionID: transactionID, Mutations: mutations})
		if err != nil || got != uint64(sequence) {
			t.Fatalf("sequence=%d got=%d err=%v", sequence, got, err)
		}
	}
	apply(1,
		storage.DocumentMutation{Collection: "items", DocumentID: [16]byte(a), Operation: storage.DocumentInsert, Document: doc(a, 10, true)},
		storage.DocumentMutation{Collection: "items", DocumentID: [16]byte(b), Operation: storage.DocumentInsert, Document: doc(b, 10, true)},
	)
	apply(2, storage.DocumentMutation{Collection: "items", DocumentID: [16]byte(b), Operation: storage.DocumentUpdate, Document: doc(b, 5, true)})
	apply(3, storage.DocumentMutation{Collection: "other", DocumentID: [16]byte(other), Operation: storage.DocumentInsert, Document: doc(other, 1, true)})
	apply(4, storage.DocumentMutation{Collection: "items", DocumentID: [16]byte(c), Operation: storage.DocumentInsert, Document: doc(c, 7, true)})
	apply(5, storage.DocumentMutation{Collection: "items", DocumentID: [16]byte(a), Operation: storage.DocumentUpdate, Document: doc(a, 1, true)})
	apply(6, storage.DocumentMutation{Collection: "items", DocumentID: [16]byte(b), Operation: storage.DocumentDelete})

	limit := 2
	query, err := CompileQuery(Filter{"active": true}, QueryOptions{Sort: []SortField{{Path: "score", Direction: 1}}, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, stream, err := file.OpenSnapshotAndStreamAt(0)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	defer stream.Close()
	view, err := newReactiveReplayView(snapshot, "items", query)
	if err != nil {
		t.Fatal(err)
	}
	if initial := view.Snapshot(); initial.Token != 0 || len(initial.Documents) != 0 {
		t.Fatalf("initial=%+v", initial)
	}

	wantIDs := map[uint64][]DocumentID{
		1: {a, b}, // equal sort keys retain insertion order, not primary-key order
		2: {b, a},
		3: {b, a}, // unrelated collection advances sequence without a delta
		4: {b, c},
		5: {a, b},
		6: {a, c},
	}
	previous := view.Snapshot()
	for sequence := uint64(1); sequence <= 6; sequence++ {
		batch, err := stream.Next(context.Background())
		if err != nil || batch.Sequence != sequence {
			t.Fatalf("sequence=%d batch=%+v err=%v", sequence, batch, err)
		}
		next, delta, err := view.ApplyCommit(stream, batch)
		if err != nil {
			t.Fatalf("sequence=%d: %v", sequence, err)
		}
		assertSnapshotIDs(t, next, wantIDs[sequence])
		if sequence == 3 {
			if delta != nil {
				t.Fatalf("unrelated commit produced delta=%+v", delta)
			}
		} else {
			if delta == nil {
				t.Fatalf("sequence=%d missing delta", sequence)
			}
			publicDelta := cloneSharedQueryDelta(delta, previous.Token)
			applied, err := ApplyQueryDelta(previous, publicDelta)
			if err != nil || !documentSlicesEqual(applied.Documents, next.Documents) || applied.Token != next.Token {
				t.Fatalf("sequence=%d applied=%+v next=%+v err=%v", sequence, applied, next, err)
			}
		}
		previous = next
	}
}

func TestHistoricalSnapshotRestoresInsertionTieBreaker(t *testing.T) {
	file, _, err := storage.Open(filepath.Join(t.TempDir(), "historical-order.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	first, second := DocumentID{2}, DocumentID{1}
	encode := func(id DocumentID) []byte {
		value, err := encodeStoredDocument(Document{"_id": ID(id), "same": Int(1)})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if _, err := file.ApplyDocumentTransaction(storage.DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []storage.DocumentMutation{
		{Collection: "items", DocumentID: [16]byte(first), Operation: storage.DocumentInsert, Document: encode(first)},
		{Collection: "items", DocumentID: [16]byte(second), Operation: storage.DocumentInsert, Document: encode(second)},
	}}); err != nil {
		t.Fatal(err)
	}
	snapshot, stream, err := file.OpenSnapshotAndStreamAt(1)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	defer stream.Close()
	query, err := CompileQuery(Filter{}, QueryOptions{Sort: []SortField{{Path: "same", Direction: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	view, err := newReactiveReplayView(snapshot, "items", query)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotIDs(t, view.Snapshot(), []DocumentID{first, second})
}

func TestReplayRejectsBatchAtomically(t *testing.T) {
	file, _, err := storage.Open(filepath.Join(t.TempDir(), "replay-atomic.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := DocumentID{1}
	encoded, err := encodeStoredDocument(Document{"_id": ID(id), "active": Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(storage.DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []storage.DocumentMutation{{
		Collection: "items", DocumentID: [16]byte(id), Operation: storage.DocumentInsert, Document: encoded,
	}}}); err != nil {
		t.Fatal(err)
	}
	snapshot, stream, err := file.OpenSnapshotAndStreamAt(0)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	defer stream.Close()
	query, _ := CompileQuery(Filter{}, QueryOptions{})
	view, err := newReactiveReplayView(snapshot, "items", query)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	corrupt := batch
	corrupt.Changes = append([]storage.CommitChange(nil), batch.Changes...)
	corrupt.Changes = append(corrupt.Changes, batch.Changes[len(batch.Changes)-1])
	if _, _, err := view.ApplyCommit(stream, corrupt); err == nil {
		t.Fatal("duplicate change accepted")
	}
	if unchanged := view.Snapshot(); unchanged.Token != 0 || len(unchanged.Documents) != 0 || view.collectionID != 0 || view.order.next != 0 {
		t.Fatalf("failed batch mutated view=%+v collectionID=%d next=%d", unchanged, view.collectionID, view.order.next)
	}
	next, delta, err := view.ApplyCommit(stream, batch)
	if err != nil || delta == nil {
		t.Fatalf("valid retry next=%+v delta=%+v err=%v", next, delta, err)
	}
	assertSnapshotIDs(t, next, []DocumentID{id})
}

func TestQueryReplaySourceSkipsInvisibleSequencesAndReportsRetention(t *testing.T) {
	file, _, err := storage.Open(filepath.Join(t.TempDir(), "query-replay-source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	item, other := DocumentID{1}, DocumentID{2}
	encode := func(id DocumentID, title string) []byte {
		value, err := encodeStoredDocument(Document{"_id": ID(id), "title": String(title)})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	transactions := []storage.DocumentTransaction{
		{TransactionID: [16]byte{1}, Mutations: []storage.DocumentMutation{{Collection: "items", DocumentID: [16]byte(item), Operation: storage.DocumentInsert, Document: encode(item, "one")}}},
		{TransactionID: [16]byte{2}, Mutations: []storage.DocumentMutation{{Collection: "other", DocumentID: [16]byte(other), Operation: storage.DocumentInsert, Document: encode(other, "other")}}},
		{TransactionID: [16]byte{3}, Mutations: []storage.DocumentMutation{{Collection: "items", DocumentID: [16]byte(item), Operation: storage.DocumentUpdate, Document: encode(item, "three")}}},
	}
	for _, transaction := range transactions {
		if _, err := file.ApplyDocumentTransaction(transaction); err != nil {
			t.Fatal(err)
		}
	}
	query, _ := CompileQuery(Filter{}, QueryOptions{})
	source := &queryReplaySource{file: file}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	replay, err := source.OpenQueryReplay(ctx, "items", query, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	assertSnapshotIDs(t, replay.Initial, []DocumentID{item})
	select {
	case delta := <-replay.Deltas:
		if delta.FromToken != 1 || delta.Token != 3 || len(delta.Operations) != 1 || delta.Operations[0].Kind != QueryDeltaChange {
			t.Fatalf("delta=%+v", delta)
		}
	case err := <-replay.Errors:
		t.Fatalf("replay error=%v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	replay.Close()
	if _, err := file.RetainCommitsFrom(3, [16]byte{4}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.OpenQueryReplay(context.Background(), "items", query, 1, 1); !errors.Is(err, ErrHistoryLost) {
		t.Fatalf("history error=%v", err)
	}
}

func assertSnapshotIDs(t *testing.T, snapshot QuerySnapshot, want []DocumentID) {
	t.Helper()
	if len(snapshot.Documents) != len(want) {
		t.Fatalf("token=%d documents=%d want=%d", snapshot.Token, len(snapshot.Documents), len(want))
	}
	for index, document := range snapshot.Documents {
		id, ok := document.ID()
		if !ok || id != want[index] {
			t.Fatalf("token=%d index=%d id=%v ok=%t want=%v", snapshot.Token, index, id, ok, want[index])
		}
	}
}
