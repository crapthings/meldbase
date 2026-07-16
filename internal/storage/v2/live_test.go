package v2

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSnapshotAndLiveStreamHaveNoCommitGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	firstID, secondID := [16]byte{1}, [16]byte{2}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations:     []DocumentMutation{{Collection: "items", DocumentID: firstID, Operation: DocumentInsert, Document: []byte("snapshot")}},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, stream, err := file.OpenSnapshotAndStream()
	if err != nil || snapshot.Sequence() != 1 {
		t.Fatalf("snapshot sequence=%d err=%v", snapshot.Sequence(), err)
	}
	defer snapshot.Close()
	defer stream.Close()

	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t),
		Mutations:     []DocumentMutation{{Collection: "items", DocumentID: secondID, Operation: DocumentInsert, Document: []byte("after-snapshot")}},
	}); err != nil {
		t.Fatal(err)
	}
	// Snapshot remains fixed at N even though the live database is now N+1.
	if _, ok, err := snapshot.GetDocument("items", secondID); err != nil || ok {
		t.Fatalf("snapshot observed future document ok=%t err=%v", ok, err)
	}
	records, err := snapshot.ScanCollection("items", nil, nil, 0)
	if err != nil || len(records) != 1 || records[0].DocumentID != firstID {
		t.Fatalf("snapshot records=%+v err=%v", records, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	batch, err := stream.Next(ctx)
	if err != nil || batch.Sequence != 2 || batch.Changes[len(batch.Changes)-1].DocumentID != secondID {
		t.Fatalf("live batch=%+v err=%v", batch, err)
	}
}

func TestLiveStreamWaitCancellationCloseAndHistoryLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live-cancel.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("one")}},
	}); err != nil {
		t.Fatal(err)
	}
	stream, err := file.OpenLiveCommitStream(1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := stream.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, ErrCursorClosed) {
		t.Fatalf("closed error = %v", err)
	}
}

func TestOneCommitWakesAllLiveStreams(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live-fanout.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("one")}},
	}); err != nil {
		t.Fatal(err)
	}
	const count = 32
	streams := make([]*LiveCommitStream, count)
	for index := range streams {
		streams[index], err = file.OpenLiveCommitStream(1)
		if err != nil {
			t.Fatal(err)
		}
		defer streams[index].Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	errorsOut := make(chan error, count)
	var ready sync.WaitGroup
	ready.Add(count)
	start := make(chan struct{})
	for _, stream := range streams {
		go func(stream *LiveCommitStream) {
			ready.Done()
			<-start
			batch, err := stream.Next(ctx)
			if err == nil && batch.Sequence != 2 {
				err = errors.New("unexpected sequence")
			}
			errorsOut <- err
		}(stream)
	}
	ready.Wait()
	close(start)
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("two")}},
	}); err != nil {
		t.Fatal(err)
	}
	for range count {
		if err := <-errorsOut; err != nil {
			t.Fatal(err)
		}
	}
}

func TestHistoricalSnapshotAndStreamReconstructExactSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "historical-live.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	firstID, secondID := [16]byte{1}, [16]byte{2}
	transactions := []DocumentTransaction{
		{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{Collection: "items", DocumentID: firstID, Operation: DocumentInsert, Document: []byte("one")}}},
		{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{Collection: "items", DocumentID: firstID, Operation: DocumentUpdate, Document: []byte("two")}}},
		{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{Collection: "items", DocumentID: secondID, Operation: DocumentInsert, Document: []byte("second")}}},
	}
	for _, transaction := range transactions {
		if _, err := file.ApplyDocumentTransaction(transaction); err != nil {
			t.Fatal(err)
		}
	}

	snapshot, stream, err := file.OpenSnapshotAndStreamAt(1)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	defer stream.Close()
	if _, err := stream.ResolveChange(CommitChange{}); !errors.Is(err, ErrNoDeliveredCommit) {
		t.Fatalf("resolve before delivery error = %v", err)
	}
	if snapshot.Sequence() != 1 {
		t.Fatalf("snapshot sequence = %d", snapshot.Sequence())
	}
	first, ok, err := snapshot.GetDocument("items", firstID)
	if err != nil || !ok || string(first) != "one" {
		t.Fatalf("historical first=%q ok=%t err=%v", first, ok, err)
	}
	if _, ok, err := snapshot.GetDocument("items", secondID); err != nil || ok {
		t.Fatalf("historical future document ok=%t err=%v", ok, err)
	}
	meta, exists, err := snapshot.CollectionMeta("items")
	if err != nil || !exists || meta.ID == 0 {
		t.Fatalf("historical collection meta=%+v exists=%t err=%v", meta, exists, err)
	}
	for want := uint64(2); want <= 3; want++ {
		batch, err := stream.Next(context.Background())
		if err != nil || batch.Sequence != want || batch.CatalogRoot == 0 {
			t.Fatalf("replay want=%d batch=%+v err=%v", want, batch, err)
		}
		change := batch.Changes[0]
		if change.CollectionID != meta.ID {
			t.Fatalf("replay collection id=%d want=%d", change.CollectionID, meta.ID)
		}
		resolved, err := stream.ResolveChange(change)
		if err != nil {
			t.Fatal(err)
		}
		if want == 2 && (string(resolved.Before) != "one" || string(resolved.After) != "two") {
			t.Fatalf("resolved update before=%q after=%q", resolved.Before, resolved.After)
		}
		if want == 3 && (resolved.Before != nil || string(resolved.After) != "second") {
			t.Fatalf("resolved insert before=%q after=%q", resolved.Before, resolved.After)
		}
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, recoveredStream, err := reopened.OpenSnapshotAndStreamAt(1)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	defer recoveredStream.Close()
	value, ok, err := recovered.GetDocument("items", firstID)
	if err != nil || !ok || string(value) != "one" {
		t.Fatalf("reopened historical value=%q ok=%t err=%v", value, ok, err)
	}
}

func TestLiveReplayPinPreventsRetentionFromOvertakingConsumer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live-retention-pin.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{1}
	for sequence := 1; sequence <= 3; sequence++ {
		operation := DocumentUpdate
		if sequence == 1 {
			operation = DocumentInsert
		}
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
			TransactionID: randomTransactionID(t),
			Mutations:     []DocumentMutation{{Collection: "items", DocumentID: id, Operation: operation, Document: []byte{byte(sequence)}}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	stream, err := file.OpenLiveCommitStream(1)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if _, err := file.RetainCommitsFrom(3, randomTransactionID(t), time.Now()); !errors.Is(err, ErrHistoryPinned) {
		t.Fatalf("retention overtook replay cursor: %v", err)
	}
	if root, _ := file.DatabaseRoot(); root.CommitSequence != 3 {
		t.Fatalf("failed retention published sequence %d", root.CommitSequence)
	}
	batch, err := stream.Next(context.Background())
	if err != nil || batch.Sequence != 2 {
		t.Fatalf("first replay batch=%+v err=%v", batch, err)
	}
	if _, err := file.RetainCommitsFrom(3, randomTransactionID(t), time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []uint64{3, 4} {
		batch, err := stream.Next(context.Background())
		if err != nil || batch.Sequence != want {
			t.Fatalf("replay want=%d batch=%+v err=%v", want, batch, err)
		}
	}
	if _, _, err := file.OpenSnapshotAndStreamAt(2); !errors.Is(err, ErrHistoryLost) {
		t.Fatalf("expired historical snapshot error = %v", err)
	}
}
