package database

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentReadersObserveValidSnapshotsDuringSingleWriterCommits checks that concurrent readers see strictly ordered snapshots while one writer inserts indexed documents.
func TestConcurrentReadersObserveValidSnapshotsDuringSingleWriterCommits(t *testing.T) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	if err := collection.CreateIndex(context.Background(), "items_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errorsOut := make(chan error, 8)
	var readers sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				cursor, err := collection.Find(ctx, Filter{"n": map[string]any{"$gte": int64(0)}}, QueryOptions{Sort: []SortField{{Path: "n", Direction: 1}}})
				if err != nil {
					if ctx.Err() == nil {
						errorsOut <- err
					}
					return
				}
				documents, err := cursor.All(ctx)
				if err != nil {
					if ctx.Err() == nil {
						errorsOut <- err
					}
					return
				}
				var previous int64 = -1
				for _, document := range documents {
					number, ok := document["n"].Int64()
					if !ok || number <= previous {
						errorsOut <- fmt.Errorf("invalid ordered snapshot after %d: %d", previous, number)
						return
					}
					previous = number
				}
			}
		}()
	}
	for number := int64(0); number < 400; number++ {
		if _, err := collection.InsertOne(context.Background(), Document{"n": Int(number)}); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	readers.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
	cursor, err := collection.Find(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	documents, err := cursor.All(context.Background())
	if err != nil || len(documents) != 400 {
		t.Fatalf("documents=%d err=%v", len(documents), err)
	}
}

// TestConcurrentQueriesWritesAndRealtimeConvergeForMemoryAndDurable checks that concurrent queries, writes, and live updates produce identical results in both database modes and after reopening.
func TestConcurrentQueriesWritesAndRealtimeConvergeForMemoryAndDurable(t *testing.T) {
	var memoryDocuments []Document
	t.Run("memory", func(t *testing.T) {
		db := New()
		defer db.Close()
		memoryDocuments = runConcurrentQueryWriteRealtimeStress(t, db)
	})

	path := filepath.Join(t.TempDir(), "concurrent-query-write-realtime.meld2")
	var durableDocuments []Document
	t.Run("durable", func(t *testing.T) {
		db, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		durableDocuments = runConcurrentQueryWriteRealtimeStress(t, db)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})
	if !documentSlicesEqual(memoryDocuments, durableDocuments) {
		t.Fatalf("memory and durable final query results differ: memory=%+v durable=%+v", memoryDocuments, durableDocuments)
	}

	t.Run("durable reopen", func(t *testing.T) {
		db, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		query, err := CompileQuery(Filter{"tenant": "concurrency-stress"}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: 1}}})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := db.Collection("items").SnapshotQuery(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		if !documentSlicesEqual(snapshot.Documents, durableDocuments) {
			t.Fatalf("durable reopen results differ: got=%+v want=%+v", snapshot.Documents, durableDocuments)
		}
	})
}

// runConcurrentQueryWriteRealtimeStress runs concurrent readers and writers and checks that live query deltas reach the final database snapshot.
func runConcurrentQueryWriteRealtimeStress(t *testing.T, db *DB) []Document {
	t.Helper()
	const (
		readers          = 4
		writers          = 6
		insertsPerWriter = 24
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	items := db.Collection("items")
	if err := items.CreateIndex(ctx, "by_tenant", []IndexField{{Field: "tenant", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{"tenant": "concurrency-stress"}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	live, err := items.SubscribeQueryDeltas(ctx, query, writers*insertsPerWriter+1)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if len(live.Initial.Documents) != 0 {
		t.Fatalf("initial realtime documents=%+v", live.Initial.Documents)
	}

	readerStart := make(chan struct{})
	writerStart := make(chan struct{})
	readerStop := make(chan struct{})
	readersReady := make(chan struct{}, readers)
	errorsOut := make(chan error, readers+writers)
	var queryCount atomic.Uint64
	var readerGroup sync.WaitGroup
	for range readers {
		readerGroup.Add(1)
		go func() {
			defer readerGroup.Done()
			<-readerStart
			readersReady <- struct{}{}
			for {
				select {
				case <-readerStop:
					return
				default:
				}
				cursor, err := items.Find(ctx, Filter{"tenant": "concurrency-stress"}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: 1}}})
				if err != nil {
					errorsOut <- fmt.Errorf("concurrent find: %w", err)
					return
				}
				documents, err := cursor.All(ctx)
				if err != nil {
					errorsOut <- fmt.Errorf("concurrent cursor: %w", err)
					return
				}
				if err := validateConcurrentQueryDocuments(documents); err != nil {
					errorsOut <- err
					return
				}
				queryCount.Add(1)
			}
		}()
	}

	type liveResult struct {
		snapshot QuerySnapshot
		err      error
	}
	liveDone := make(chan liveResult, 1)
	go func() {
		state := live.Initial
		for range writers * insertsPerWriter {
			select {
			case delta := <-live.Deltas:
				next, err := ApplyQueryDelta(state, delta)
				if err != nil {
					liveDone <- liveResult{err: fmt.Errorf("apply realtime delta: %w", err)}
					return
				}
				state = next
			case err := <-live.Errors:
				liveDone <- liveResult{err: fmt.Errorf("realtime subscription: %w", err)}
				return
			case <-ctx.Done():
				liveDone <- liveResult{err: ctx.Err()}
				return
			}
		}
		liveDone <- liveResult{snapshot: state}
	}()

	var writerGroup sync.WaitGroup
	for writer := range writers {
		writer := writer
		writerGroup.Add(1)
		go func() {
			defer writerGroup.Done()
			<-writerStart
			for operation := range insertsPerWriter {
				rank := int64(writer*insertsPerWriter + operation)
				_, err := items.InsertOne(ctx, Document{
					"_id":    ID(DocumentID{0: byte(writer + 1), 8: byte(operation + 1)}),
					"tenant": String("concurrency-stress"),
					"rank":   Int(rank),
				})
				if err != nil {
					errorsOut <- fmt.Errorf("writer %d insert %d: %w", writer, operation, err)
					return
				}
			}
		}()
	}

	close(readerStart)
	for range readers {
		<-readersReady
	}
	close(writerStart)
	writerGroup.Wait()
	liveState := <-liveDone
	close(readerStop)
	readerGroup.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
	if liveState.err != nil {
		t.Fatal(liveState.err)
	}
	if queryCount.Load() == 0 {
		t.Fatal("concurrent readers did not execute a query")
	}

	final, err := items.SnapshotQuery(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Documents) != writers*insertsPerWriter {
		t.Fatalf("final documents=%d", len(final.Documents))
	}
	if !documentSlicesEqual(liveState.snapshot.Documents, final.Documents) || liveState.snapshot.Token != final.Token {
		t.Fatalf("realtime state token=%d documents=%+v; final token=%d documents=%+v", liveState.snapshot.Token, liveState.snapshot.Documents, final.Token, final.Documents)
	}
	return final.Documents
}

// validateConcurrentQueryDocuments checks that stress-test results contain the expected tenant and strictly increasing ranks.
func validateConcurrentQueryDocuments(documents []Document) error {
	var previous int64 = -1
	for _, document := range documents {
		tenant, ok := document["tenant"].StringValue()
		if !ok || tenant != "concurrency-stress" {
			return fmt.Errorf("invalid concurrent query tenant: %v", document["tenant"])
		}
		rank, ok := document["rank"].Int64()
		if !ok || rank <= previous {
			return fmt.Errorf("invalid concurrent query rank after %d: %v", previous, document["rank"])
		}
		previous = rank
	}
	return nil
}
