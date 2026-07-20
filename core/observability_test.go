package meldbase

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

// This relative gate runs only on the dedicated performance runner. It samples
// 1,000 times more frequently than the normal one-second admin interval while a
// synthetic writer repeatedly crosses the exact db.mu publication boundary.
// Absolute operations/second vary by host; only paired throughput matters.
func TestStatsSamplingPerformanceBudget(t *testing.T) {
	if os.Getenv("MELDBASE_PERF_GATE") == "" {
		t.Skip("set MELDBASE_PERF_GATE=1 on a dedicated performance runner")
	}
	const (
		rounds      = 7
		roundTime   = 250 * time.Millisecond
		batchWrites = 1024
	)
	measure := func(sample bool) uint64 {
		db := New()
		var samples atomic.Uint64
		stop, done := make(chan struct{}), make(chan struct{})
		if sample {
			go func() {
				defer close(done)
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						_ = db.Stats()
						samples.Add(1)
					}
				}
			}()
		}
		deadline := time.Now().Add(roundTime)
		var writes uint64
		for time.Now().Before(deadline) {
			for range batchWrites {
				db.mu.Lock()
				db.token++
				db.metrics.commits.Add(1)
				db.mu.Unlock()
				writes++
			}
			runtime.Gosched()
		}
		if sample {
			close(stop)
			<-done
			if samples.Load() == 0 {
				t.Fatal("Stats sampler did not run")
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return writes
	}
	baseline, sampled := make([]uint64, rounds), make([]uint64, rounds)
	for round := range rounds {
		if round%2 == 0 {
			baseline[round], sampled[round] = measure(false), measure(true)
		} else {
			sampled[round], baseline[round] = measure(true), measure(false)
		}
	}
	sort.Slice(baseline, func(left, right int) bool { return baseline[left] < baseline[right] })
	sort.Slice(sampled, func(left, right int) bool { return sampled[left] < sampled[right] })
	baselineMedian, sampledMedian := baseline[len(baseline)/2], sampled[len(sampled)/2]
	if sampledMedian < baselineMedian*95/100 {
		t.Fatalf("1ms Stats sampling writes=%d baseline=%d overhead=%.1f%%", sampledMedian, baselineMedian,
			(1-float64(sampledMedian)/float64(baselineMedian))*100)
	}
	t.Logf("1ms Stats sampling writes=%d baseline=%d overhead=%.1f%%", sampledMedian, baselineMedian,
		(1-float64(sampledMedian)/float64(baselineMedian))*100)
}

func TestStatsTrackCoreWorkWithoutUserData(t *testing.T) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")

	initial := db.Stats()
	if initial.Closed || initial.Durable || initial.CommitSequence != 0 || initial.Commits.Total != 0 {
		t.Fatalf("unexpected initial stats: %+v", initial)
	}
	if initial.StartedAt.IsZero() || initial.CapturedAt.Before(initial.StartedAt) || initial.Uptime < 0 {
		t.Fatalf("invalid timing stats: %+v", initial)
	}
	if initial.WritesDisabled || initial.Realtime.PendingBatchCapacity != maxPendingReactiveBatches ||
		initial.Realtime.PendingChangeCapacity != maxPendingReactiveChanges ||
		initial.Realtime.PendingByteCapacity != maxPendingReactiveBytes ||
		initial.Realtime.WatcherByteCapacity != maxPendingChangeWatchersBytes ||
		initial.Realtime.DispatchBatchCapacity != maxPendingChangeDispatchBatches ||
		initial.Realtime.DispatchChangeCapacity != maxPendingChangeDispatchChanges ||
		initial.Realtime.DispatchByteCapacity != maxPendingChangeDispatchBytes {
		t.Fatalf("initial health/capacity stats: %+v", initial)
	}

	if _, err := collection.InsertMany(context.Background(), []Document{
		{"n": Int(1), "group": String("a")},
		{"n": Int(2), "group": String("b")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := collection.CreateIndex(context.Background(), "items_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.FindOne(context.Background(), Filter{"n": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.FindOne(context.Background(), Filter{"group": "b"}); err != nil {
		t.Fatal(err)
	}

	stats := db.Stats()
	if stats.CommitSequence != 2 || stats.Collections != 1 || stats.Documents != 2 || stats.Indexes != 1 {
		t.Fatalf("state stats: %+v", stats)
	}
	if stats.Commits.Total != 2 || stats.Commits.Changes != 3 {
		t.Fatalf("commit stats: %+v", stats.Commits)
	}
	if stats.Queries.Total != 2 || stats.Queries.IndexScans != 1 || stats.Queries.CollectionScans != 1 {
		t.Fatalf("query stats: %+v", stats.Queries)
	}
	if stats.Queries.DocumentsReturned != 2 || stats.Queries.DocumentsExamined != 3 {
		t.Fatalf("query work stats: %+v", stats.Queries)
	}
	if stats.IndexBuilds.Active != 0 || stats.IndexBuilds.Attempts != 1 || stats.IndexBuilds.Completed != 1 ||
		stats.IndexBuilds.Failed != 0 || stats.IndexBuilds.LastEntries != 2 || stats.IndexBuilds.LastBytes == 0 ||
		stats.IndexBuilds.LastDuration <= 0 || stats.IndexBuilds.MaxDuration < stats.IndexBuilds.LastDuration {
		t.Fatalf("index build stats: %+v", stats.IndexBuilds)
	}
}

func TestStatsReportsFailStopWithoutExposingErrorText(t *testing.T) {
	db := New()
	defer db.Close()
	db.mu.Lock()
	db.fatalErr = ErrDurability
	db.mu.Unlock()
	if stats := db.Stats(); !stats.WritesDisabled || stats.Closed {
		t.Fatalf("fail-stop stats=%+v", stats)
	}
}

func TestStatsTrackReactiveLifecycle(t *testing.T) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"active": Bool(false)})
	if err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{"active": true}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	subscription, err := collection.SubscribeQuery(ctx, query, 2)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	<-subscription.Snapshots
	if active := db.Stats().ActiveChangeWatchers; active != 1 {
		t.Fatalf("active watchers = %d", active)
	}

	if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"active": true}}); err != nil {
		t.Fatal(err)
	}
	select {
	case snapshot := <-subscription.Snapshots:
		if len(snapshot.Documents) != 1 {
			t.Fatalf("snapshot documents = %d", len(snapshot.Documents))
		}
	case err := <-subscription.Errors:
		t.Fatal(err)
	}

	stats := db.Stats()
	if stats.Realtime.PublishedBatches != 2 || stats.Realtime.PublishedChanges != 2 {
		t.Fatalf("publish stats: %+v", stats.Realtime)
	}
	if stats.Realtime.WatcherDeliveries != 1 || stats.Realtime.InitialSnapshots != 1 || stats.Realtime.QueryRecomputes != 1 {
		t.Fatalf("reactive stats: %+v", stats.Realtime)
	}
	if stats.Realtime.SnapshotsEmitted != 2 || stats.Realtime.DocumentsEmitted != 1 {
		t.Fatalf("snapshot stats: %+v", stats.Realtime)
	}

	cancel()
	subscription.Close()
	deadline := time.Now().Add(time.Second)
	for db.Stats().ActiveChangeWatchers != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active := db.Stats().ActiveChangeWatchers; active != 0 {
		t.Fatalf("active watchers after cancel = %d", active)
	}
}

func TestStatsSnapshotIsAllocationFreeForMemoryAndV2(t *testing.T) {
	memory := New()
	defer memory.Close()
	if allocations := testing.AllocsPerRun(1_000, func() { _ = memory.Stats() }); allocations != 0 {
		t.Fatalf("memory Stats allocations=%v, want 0", allocations)
	}

	v2, err := Open(filepath.Join(t.TempDir(), "stats.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()
	if allocations := testing.AllocsPerRun(1_000, func() { _ = v2.Stats() }); allocations != 0 {
		t.Fatalf("V2 Stats allocations=%v, want 0", allocations)
	}
}

func TestStatsLogicalGaugesStayCommitConsistentDuringWrites(t *testing.T) {
	db := New()
	defer db.Close()
	collection := db.Collection("items")
	if err := collection.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	id := DocumentID{15: 1}
	errorsSeen := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for iteration := 0; iteration < 1_000; iteration++ {
			if _, err := collection.InsertOne(context.Background(), Document{"_id": ID(id), "value": Int(int64(iteration))}); err != nil {
				errorsSeen <- err
				return
			}
			if result, err := collection.DeleteOne(context.Background(), Filter{"_id": id}); err != nil || result.DeletedCount != 1 {
				if err == nil {
					err = ErrCorrupt
				}
				errorsSeen <- err
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			select {
			case err := <-errorsSeen:
				t.Fatal(err)
			default:
			}
			stats := db.Stats()
			if stats.Documents != 0 || stats.Collections != 1 || stats.Indexes != 1 {
				t.Fatalf("final logical gauges=%+v", stats)
			}
			return
		default:
			stats := db.Stats()
			wantDocuments := uint64(0)
			if stats.CommitSequence > 1 && stats.CommitSequence%2 == 0 {
				wantDocuments = 1
			}
			if stats.Documents != wantDocuments || stats.Collections != 1 || stats.Indexes != 1 {
				t.Fatalf("sequence=%d logical gauges collections=%d documents=%d indexes=%d want documents=%d",
					stats.CommitSequence, stats.Collections, stats.Documents, stats.Indexes, wantDocuments)
			}
		}
	}
}
