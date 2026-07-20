package meldbase

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkV2ConcurrentIndependentCommits is the storage baseline for the
// future CommitCoordinator. It deliberately uses independent documents: any
// contention it observes comes from the shared V2 commit path rather than a
// business-key conflict.
func BenchmarkV2ConcurrentIndependentCommits(b *testing.B) {
	for _, parallelism := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("parallelism_%d", parallelism), func(b *testing.B) {
			db, err := Open(filepath.Join(b.TempDir(), "commits.meld2"))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			collection := db.Collection("events")
			var sequence atomic.Uint64
			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(worker *testing.PB) {
				for worker.Next() {
					value := sequence.Add(1)
					if _, err := collection.InsertOne(context.Background(), Document{
						"writer": Int(int64(value)), "payload": String("commit-baseline"),
					}); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.StopTimer()
			stats := db.Stats()
			if got, want := stats.CommitSequence, uint64(b.N); got != want {
				b.Fatalf("commit sequence=%d want=%d", got, want)
			}
			if got, want := stats.Storage.CommittedTransactions, uint64(b.N); got != want {
				b.Fatalf("committed transactions=%d want=%d", got, want)
			}
		})
	}
}

// BenchmarkV2PublicInsertPair compares the established public synchronous
// InsertOne path with the public coordinator path. The grouped case uses the
// package-test admission hook only to ensure both independent callers land in
// one window; the measured operation still traverses document validation,
// queue admission, the V2 COW group publisher and result delivery.
func BenchmarkV2PublicInsertPair(b *testing.B) {
	for _, grouped := range []bool{false, true} {
		name := "sequential"
		if grouped {
			name = "coordinated_group"
		}
		b.Run(name, func(b *testing.B) {
			options := OpenOptions{}
			if grouped {
				options.CommitCoordinator = V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Second}
			}
			db, err := OpenWithOptions(filepath.Join(b.TempDir(), "public-pair.meld2"), options)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			collection := db.Collection("events")
			initialGeneration := uint64(0)
			if grouped {
				initialGeneration = db.commitCoordinator.store.file.Meta().Generation
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				firstID, secondID := DocumentID{15: 1}, DocumentID{15: 2}
				binary.LittleEndian.PutUint64(firstID[:8], uint64(iteration)+1)
				binary.LittleEndian.PutUint64(secondID[:8], uint64(iteration)+1)
				if !grouped {
					if _, err := collection.InsertOne(context.Background(), Document{"_id": ID(firstID), "payload": String("pair")}); err != nil {
						b.Fatal(err)
					}
					if _, err := collection.InsertOne(context.Background(), Document{"_id": ID(secondID), "payload": String("pair")}); err != nil {
						b.Fatal(err)
					}
					continue
				}
				entered, release := make(chan struct{}), make(chan struct{})
				db.commitCoordinator.testBeforeCoalesce = func() { close(entered); <-release }
				first, second := make(chan error, 1), make(chan error, 1)
				go func() {
					_, err := collection.InsertOne(context.Background(), Document{"_id": ID(firstID), "payload": String("pair")})
					first <- err
				}()
				select {
				case <-entered:
				case <-time.After(time.Second):
					b.Fatal("coordinator did not reach coalescing boundary")
				}
				go func() {
					_, err := collection.InsertOne(context.Background(), Document{"_id": ID(secondID), "payload": String("pair")})
					second <- err
				}()
				deadline := time.Now().Add(time.Second)
				for {
					db.commitCoordinator.mu.Lock()
					pending := len(db.commitCoordinator.queue)
					db.commitCoordinator.mu.Unlock()
					if pending == 1 {
						break
					}
					if time.Now().After(deadline) {
						b.Fatal("second insert was not admitted")
					}
					time.Sleep(time.Microsecond)
				}
				close(release)
				if err := <-first; err != nil {
					b.Fatal(err)
				}
				if err := <-second; err != nil {
					b.Fatal(err)
				}
				db.commitCoordinator.testBeforeCoalesce = nil
			}
			b.StopTimer()
			if got, want := db.Stats().CommitSequence, uint64(2*b.N); got != want {
				b.Fatalf("commit sequence=%d want=%d", got, want)
			}
			if grouped {
				if generation := db.commitCoordinator.store.file.Meta().Generation; generation != initialGeneration+uint64(b.N) {
					b.Fatalf("grouped generations=%d want=%d", generation, initialGeneration+uint64(b.N))
				}
				if stats := db.CommitCoordinatorStats(); stats.GroupedTransactions != uint64(2*b.N) {
					b.Fatalf("grouped transactions=%d want=%d", stats.GroupedTransactions, 2*b.N)
				}
			}
		})
	}
}

// BenchmarkV2PublicWriteTransactionPair measures the same physical-barrier
// comparison for two independently built optimistic point transactions. The
// grouped path must never rerun either callback after a conflict or
// cancellation boundary; this benchmark uses independent inserts so it exposes
// only the coordinator's intended persistence saving.
func BenchmarkV2PublicWriteTransactionPair(b *testing.B) {
	for _, grouped := range []bool{false, true} {
		name := "sequential"
		if grouped {
			name = "coordinated_group"
		}
		b.Run(name, func(b *testing.B) {
			options := OpenOptions{}
			if grouped {
				options.CommitCoordinator = V2CommitCoordinatorOptions{Enabled: true, MaxBatch: 2, MaxPending: 8, MaxDelay: time.Second}
			}
			db, err := OpenWithOptions(filepath.Join(b.TempDir(), "public-transaction-pair.meld2"), options)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			initialGeneration := uint64(0)
			if grouped {
				initialGeneration = db.commitCoordinator.store.file.Meta().Generation
			}
			run := func(id DocumentID, value int64) error {
				return db.RunWriteTransaction(context.Background(), func(tx *WriteTransaction) error {
					_, err := tx.InsertOne("events", Document{"_id": ID(id), "payload": String("transaction-pair"), "value": Int(value)})
					return err
				})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				firstID, secondID := DocumentID{14: 1}, DocumentID{14: 2}
				binary.LittleEndian.PutUint64(firstID[:8], uint64(iteration)+1)
				binary.LittleEndian.PutUint64(secondID[:8], uint64(iteration)+1)
				if !grouped {
					if err := run(firstID, 1); err != nil {
						b.Fatal(err)
					}
					if err := run(secondID, 2); err != nil {
						b.Fatal(err)
					}
					continue
				}
				entered, release := make(chan struct{}), make(chan struct{})
				db.commitCoordinator.testBeforeCoalesce = func() { close(entered); <-release }
				first, second := make(chan error, 1), make(chan error, 1)
				go func() { first <- run(firstID, 1) }()
				select {
				case <-entered:
				case <-time.After(time.Second):
					b.Fatal("transaction coordinator did not reach coalescing boundary")
				}
				go func() { second <- run(secondID, 2) }()
				deadline := time.Now().Add(time.Second)
				for {
					db.commitCoordinator.mu.Lock()
					pending := len(db.commitCoordinator.queue)
					db.commitCoordinator.mu.Unlock()
					if pending == 1 {
						break
					}
					if time.Now().After(deadline) {
						b.Fatal("second transaction was not admitted")
					}
					time.Sleep(time.Microsecond)
				}
				close(release)
				if err := <-first; err != nil {
					b.Fatal(err)
				}
				if err := <-second; err != nil {
					b.Fatal(err)
				}
				db.commitCoordinator.testBeforeCoalesce = nil
			}
			b.StopTimer()
			if got, want := db.Stats().CommitSequence, uint64(2*b.N); got != want {
				b.Fatalf("commit sequence=%d want=%d", got, want)
			}
			if grouped {
				if generation := db.commitCoordinator.store.file.Meta().Generation; generation != initialGeneration+uint64(b.N) {
					b.Fatalf("grouped generations=%d want=%d", generation, initialGeneration+uint64(b.N))
				}
			}
		})
	}
}

// BenchmarkV2SharedRealtimeFanout measures a durable write plus delivery to
// many subscribers of one canonical query. It separates the cost of one shared
// view transition from per-subscriber delta cloning/delivery.
func BenchmarkV2SharedRealtimeFanout(b *testing.B) {
	for _, subscribers := range []int{1, 100} {
		b.Run(fmt.Sprintf("subscribers_%d", subscribers), func(b *testing.B) {
			db, err := Open(filepath.Join(b.TempDir(), "realtime.meld2"))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			collection := db.Collection("items")
			id, err := collection.InsertOne(context.Background(), Document{"active": Bool(false), "payload": String("baseline")})
			if err != nil {
				b.Fatal(err)
			}
			query, err := CompileQuery(Filter{"active": true}, QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			live := make([]*QueryDeltaSubscription, 0, subscribers)
			for range subscribers {
				subscription, err := collection.SubscribeQueryDeltas(context.Background(), query, 2)
				if err != nil {
					b.Fatal(err)
				}
				live = append(live, subscription)
				defer subscription.Close()
			}
			if got := db.Stats().Realtime.SharedViews; got != 1 {
				b.Fatalf("shared views=%d want=1", got)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{
					"$set": map[string]any{"active": iteration%2 == 0},
				}); err != nil {
					b.Fatal(err)
				}
				for _, subscription := range live {
					select {
					case delta := <-subscription.Deltas:
						if len(delta.Operations) != 1 {
							b.Fatalf("delta operations=%d", len(delta.Operations))
						}
					case err := <-subscription.Errors:
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkV2ReactiveViewRebuild measures one immutable V2 collection scan
// shared by many canonical views. It is the cold/resync cost that must remain
// bounded and independent of a writer; each document's canonical byte size is
// intentionally charged once even when it matches every view.
func BenchmarkV2ReactiveViewRebuild(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "reactive-rebuild.meld2"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	documents := make([]Document, 256)
	for index := range documents {
		documents[index] = Document{
			"_id":  ID(DocumentID{0: byte(index >> 8), 1: byte(index), 15: 1}),
			"kind": String("event"), "payload": String("rebuild-baseline-payload"),
		}
	}
	if _, err := db.Collection("events").InsertMany(context.Background(), documents); err != nil {
		b.Fatal(err)
	}
	query, err := CompileQuery(Filter{"kind": "event"}, QueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	queries := make([]QuerySpec, 16)
	for index := range queries {
		queries[index] = query
	}
	token := db.Stats().CommitSequence
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		order := &reactiveCollectionOrder{positions: make(map[DocumentID]uint64)}
		states, err := buildStorageReactiveViewStates(db.querySource, token, "events", queries, order, maphash.MakeSeed(), db.resourceLimits)
		if err != nil || len(states) != len(queries) || len(states[0].snapshot.Documents) != len(documents) {
			b.Fatalf("states=%d documents=%d err=%v", len(states), len(states[0].snapshot.Documents), err)
		}
	}
}
