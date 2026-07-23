package database

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

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
