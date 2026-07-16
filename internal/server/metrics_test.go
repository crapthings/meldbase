package server

import (
	"context"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
)

func TestServerStatsSnapshotIsAllocationFreeAndContainsNoDynamicDimensions(t *testing.T) {
	handler := &Handler{startedAt: time.Now()}
	handler.metrics.connectionsAccepted.Store(3)
	handler.metrics.rpcRequests.Store(7)
	handler.metrics.rpcSucceeded.Store(5)
	handler.metrics.rpcIdempotencyUnknown.Store(2)
	handler.metrics.rpcAtomicCommits.Store(4)
	if allocations := testing.AllocsPerRun(1000, func() { _ = handler.Stats() }); allocations != 0 {
		t.Fatalf("Stats allocations=%f", allocations)
	}
	stats := handler.Stats()
	if stats.ConnectionsAccepted != 3 || stats.RPCRequests != 7 || stats.RPCSucceeded != 5 || stats.RPCIdempotencyUnknown != 2 || stats.RPCAtomicCommits != 4 {
		t.Fatalf("stats=%+v", stats)
	}
}

func BenchmarkServerStatsSnapshot(b *testing.B) {
	handler := &Handler{startedAt: time.Now()}
	handler.metrics.rpcRequests.Store(1_000_000)
	handler.metrics.rpcSucceeded.Store(999_000)
	b.ReportAllocs()
	for range b.N {
		_ = handler.Stats()
	}
}

func BenchmarkRPCMetricSpan(b *testing.B) {
	handler := &Handler{startedAt: time.Now()}
	method := RPCMethod(func(context.Context, Principal, []meldbase.Value) (meldbase.Value, error) {
		return meldbase.Null(), nil
	})
	b.ReportAllocs()
	for range b.N {
		span := handler.beginRPC(0, 32)
		result, err := invokeRPCMethod(context.Background(), method, Principal{}, nil)
		if err != nil || result.Kind() != meldbase.NullKind {
			b.Fatal(err)
		}
		handler.finishRPC(span, "success", 12)
	}
}
