package admin

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
	meldserver "github.com/crapthings/meldbase/server"
)

type fakeSource struct {
	mu    sync.Mutex
	stats meldbase.DBStats
}

type fakeServerSource struct {
	mu    sync.Mutex
	stats meldserver.ServerStats
}

func (source *fakeServerSource) Stats() meldserver.ServerStats {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.stats
}

func (source *fakeServerSource) set(stats meldserver.ServerStats) {
	source.mu.Lock()
	source.stats = stats
	source.mu.Unlock()
}

func (source *fakeSource) Stats() meldbase.DBStats {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.stats
}

func (source *fakeSource) set(stats meldbase.DBStats) {
	source.mu.Lock()
	source.stats = stats
	source.mu.Unlock()
}

func newTestSampler(t *testing.T, source StatsSource, history, subscribers int) *Sampler {
	t.Helper()
	sampler, err := NewSampler(source, SamplerOptions{
		Interval: time.Hour, HistorySize: history, MaxSubscribers: subscribers,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sampler.Close() })
	return sampler
}

func TestSamplerDerivesRatesAndBoundsHistory(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	source := &fakeSource{stats: meldbase.DBStats{
		StartedAt: started, CapturedAt: started.Add(time.Second),
		Commits: meldbase.CommitStats{Total: 10, Changes: 20},
		Queries: meldbase.QueryStats{Total: 4, DocumentsExamined: 12, DocumentsReturned: 3},
	}}
	sampler := newTestSampler(t, source, 2, 2)

	second := source.Stats()
	second.CapturedAt = second.CapturedAt.Add(2 * time.Second)
	second.Commits.Total += 4
	second.Commits.Changes += 10
	second.Queries.Total += 6
	second.Queries.DocumentsExamined += 20
	second.Queries.DocumentsReturned += 8
	second.Queries.KeysExamined += 30
	second.Storage.PageCache.Hits = 9
	second.Storage.PageCache.Misses = 1
	source.set(second)
	sampler.capture()

	latest, ok := sampler.Latest()
	if !ok || latest.Sequence != 2 || !latest.Rates.Valid {
		t.Fatalf("latest=%+v ok=%t", latest, ok)
	}
	if latest.Rates.CommitsPerSecond != 2 || latest.Rates.ChangesPerSecond != 5 || latest.Rates.QueriesPerSecond != 3 {
		t.Fatalf("rates=%+v", latest.Rates)
	}
	if latest.Rates.PageCacheHitRatio != 0.9 {
		t.Fatalf("page cache hit ratio=%f", latest.Rates.PageCacheHitRatio)
	}
	if latest.Rates.KeysExaminedPerSecond != 15 {
		t.Fatalf("key rate=%f", latest.Rates.KeysExaminedPerSecond)
	}

	third := second
	third.CapturedAt = third.CapturedAt.Add(time.Second)
	third.Commits.Total++
	source.set(third)
	sampler.capture()
	history := sampler.History()
	if len(history) != 2 || history[0].Sequence != 2 || history[1].Sequence != 3 {
		t.Fatalf("bounded history=%+v", history)
	}

	reset := third
	reset.StartedAt = reset.StartedAt.Add(time.Minute)
	reset.CapturedAt = reset.CapturedAt.Add(time.Second)
	reset.Commits.Total = 0
	source.set(reset)
	sampler.capture()
	latest, _ = sampler.Latest()
	if latest.Rates.Valid {
		t.Fatalf("rates remained valid across session reset: %+v", latest.Rates)
	}
}

func TestSamplerOptionallyCapturesServerRPCStatsAndRates(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	database := &fakeSource{stats: meldbase.DBStats{StartedAt: started, CapturedAt: started.Add(time.Second)}}
	server := &fakeServerSource{stats: meldserver.ServerStats{
		StartedAt: started, CapturedAt: started.Add(time.Second), RPCRequests: 10, RPCFailed: 2, RPCRejected: 1, RPCBusy: 3,
	}}
	sampler, err := NewSampler(database, SamplerOptions{Interval: time.Hour, HistorySize: 2, MaxSubscribers: 1, Server: server})
	if err != nil {
		t.Fatal(err)
	}
	defer sampler.Close()
	nextDB := database.Stats()
	nextDB.CapturedAt = nextDB.CapturedAt.Add(2 * time.Second)
	database.set(nextDB)
	nextServer := server.Stats()
	nextServer.CapturedAt = nextServer.CapturedAt.Add(2 * time.Second)
	nextServer.RPCRequests += 8
	nextServer.RPCFailed += 2
	nextServer.RPCCanceled += 1
	nextServer.RPCRejected += 1
	nextServer.RPCBusy += 2
	server.set(nextServer)
	sampler.capture()
	latest, ok := sampler.Latest()
	if !ok || latest.Server == nil || latest.Server.RPCRequests != 18 {
		t.Fatalf("latest=%+v ok=%t", latest, ok)
	}
	if !latest.Rates.Valid || !latest.Rates.RPCRatesValid || latest.Rates.RPCRequestsPerSecond != 4 || latest.Rates.RPCFailuresPerSecond != 2 || latest.Rates.RPCBusyPerSecond != 1 {
		t.Fatalf("rates=%+v", latest.Rates)
	}
	restartedDB := database.Stats()
	restartedDB.CapturedAt = restartedDB.CapturedAt.Add(time.Second)
	database.set(restartedDB)
	restartedServer := meldserver.ServerStats{StartedAt: started.Add(time.Hour), CapturedAt: restartedDB.CapturedAt}
	server.set(restartedServer)
	sampler.capture()
	restarted, _ := sampler.Latest()
	if !restarted.Rates.Valid || restarted.Rates.RPCRatesValid {
		t.Fatalf("server restart incorrectly invalidated DB rates or retained RPC rates: %+v", restarted.Rates)
	}
}

func TestSamplerCoalescesSlowSubscribersWithoutBackpressure(t *testing.T) {
	started := time.Now()
	source := &fakeSource{stats: meldbase.DBStats{StartedAt: started, CapturedAt: started}}
	sampler := newTestSampler(t, source, 4, 1)
	subscription, err := sampler.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if _, err := sampler.Subscribe(context.Background()); !errors.Is(err, ErrSubscriberLimit) {
		t.Fatalf("second subscriber error=%v", err)
	}

	for index := 1; index <= 2; index++ {
		stats := source.Stats()
		stats.CapturedAt = stats.CapturedAt.Add(time.Second)
		stats.Commits.Total++
		source.set(stats)
		sampler.capture()
	}

	select {
	case sample := <-subscription.C:
		if sample.Sequence != 3 {
			t.Fatalf("coalesced sequence=%d", sample.Sequence)
		}
	default:
		t.Fatal("coalesced subscriber had no latest sample")
	}
	if status := sampler.Status(); status.DroppedDeliveries != 2 || status.Subscribers != 1 {
		t.Fatalf("sampler status=%+v", status)
	}
	latest, _ := sampler.Latest()
	if latest.Health.Telemetry != HealthDegraded || !latest.Health.Signals.TelemetryDeliveryDropped {
		t.Fatalf("slow admin consumer health=%+v", latest.Health)
	}

	subscription.Close()
	if status := sampler.Status(); status.Subscribers != 0 {
		t.Fatalf("subscribers after close=%d", status.Subscribers)
	}
}

func TestLatestValueDeliveryCannotDeadlockWithConcurrentConsumer(t *testing.T) {
	channel := make(chan Sample, 1)
	channel <- Sample{Sequence: 1}

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for sample := range channel {
			if sample.Sequence == 100_000 {
				return
			}
		}
	}()

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for sequence := uint64(2); sequence <= 100_000; sequence++ {
			offerLatest(channel, Sample{Sequence: sequence})
		}
	}()

	select {
	case <-producerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("latest-value producer blocked while consumer drained the channel")
	}
	select {
	case <-consumerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not observe the final latest value")
	}
}

func TestSamplerContextAndCloseLifecycle(t *testing.T) {
	source := &fakeSource{stats: meldbase.DBStats{StartedAt: time.Now(), CapturedAt: time.Now()}}
	sampler, err := NewSampler(source, SamplerOptions{Interval: time.Hour, HistorySize: 1, MaxSubscribers: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	subscription, err := sampler.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for sampler.Status().Subscribers != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if sampler.Status().Subscribers != 0 {
		t.Fatal("context cancellation did not remove subscriber")
	}
	if err := sampler.Close(); err != nil {
		t.Fatal(err)
	}
	for range subscription.C {
		// The initial latest-value sample may still be buffered when the
		// subscription closes; closure guarantees no future delivery.
	}
	if _, err := sampler.Subscribe(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("subscribe after close error=%v", err)
	}
	if err := sampler.Close(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSamplerCapture(b *testing.B) {
	now := time.Now()
	source := &fakeSource{stats: meldbase.DBStats{StartedAt: now, CapturedAt: now}}
	sampler, err := NewSampler(source, SamplerOptions{Interval: time.Hour, HistorySize: 1, MaxSubscribers: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer sampler.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		sampler.capture()
	}
}
