package admin

import (
	"testing"
	"time"

	"github.com/crapthings/meldbase"
	meldserver "github.com/crapthings/meldbase/server"
)

func TestAssessHealthSeparatesStatePressureAndWindowEvents(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	previous := Sample{
		Stats: meldbase.DBStats{
			StartedAt: started,
			Realtime:  meldbase.RealtimeStats{PendingBatchCapacity: 100, PendingChangeCapacity: 1_000},
		},
		Sampler: SamplerStatus{DroppedDeliveries: 2},
	}
	current := previous
	current.Stats.Realtime.PendingBatches = 50
	current.Stats.Realtime.QueueOverflows = 1
	current.Stats.Realtime.SlowConsumers = 1
	current.Stats.Storage.RollbackAnchorFailures = 1
	current.Sampler.DroppedDeliveries = 3
	health := assessHealth(&previous, current)
	if health.Overall != HealthDegraded || health.Realtime != HealthDegraded || health.Durability != HealthDegraded ||
		health.Telemetry != HealthDegraded || health.Storage != HealthHealthy || health.Transport != HealthUnavailable {
		t.Fatalf("health=%+v", health)
	}
	if !health.Signals.ReactiveQueuePressure || !health.Signals.ReactiveQueueOverflow || !health.Signals.SlowConsumer ||
		!health.Signals.DurabilityFailure || !health.Signals.TelemetryDeliveryDropped {
		t.Fatalf("signals=%+v", health.Signals)
	}

	critical := current
	critical.Stats.Realtime.PendingBatches = 90
	health = assessHealth(&current, critical)
	if health.Overall != HealthCritical || health.Realtime != HealthCritical {
		t.Fatalf("critical pressure health=%+v", health)
	}
}

func TestAssessHealthTreatsFailStopAsCriticalAndFreeSpaceFallbackAsDegraded(t *testing.T) {
	current := Sample{Stats: meldbase.DBStats{
		WritesDisabled: true,
		Storage: meldbase.StorageStats{
			FreeSpaceLoadFailures: 1, PersistentFreeSpace: false, RetentionPressure: true, StorageQuotaExhausted: true,
		},
	}}
	health := assessHealth(nil, current)
	if health.Overall != HealthCritical || health.Database != HealthCritical || health.Durability != HealthCritical ||
		health.Storage != HealthDegraded || !health.Signals.WritesDisabled || !health.Signals.PersistentFreeSpaceDiscarded ||
		!health.Signals.CommitRetentionPressure || !health.Signals.StorageQuotaExhausted {
		t.Fatalf("health=%+v", health)
	}
	current.Stats.WritesDisabled = false
	current.Stats.Closed = true
	current.Stats.Storage.PersistentFreeSpace = true
	current.Stats.Storage.RetentionPressure = false
	current.Stats.Storage.StorageQuotaExhausted = false
	health = assessHealth(nil, current)
	if health.Database != HealthCritical || health.Storage != HealthHealthy || !health.Signals.DatabaseClosed {
		t.Fatalf("closed health=%+v", health)
	}
}

func TestAssessHealthReportsRecentStorageLimitRejection(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	previous := Sample{Stats: meldbase.DBStats{StartedAt: started, Storage: meldbase.StorageStats{StorageLimitRejections: 2}}}
	current := previous
	current.Stats.Storage.StorageLimitRejections++
	health := assessHealth(&previous, current)
	if health.Storage != HealthDegraded || health.Overall != HealthDegraded || !health.Signals.StorageLimitRejected {
		t.Fatalf("quota rejection health=%+v", health)
	}
}

func TestAssessHealthReportsCommitCoordinatorPressureAndRejection(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	previous := Sample{Stats: meldbase.DBStats{StartedAt: started, CommitCoordinator: meldbase.CommitCoordinatorStats{Enabled: true, PendingCapacity: 10, AdmissionRejected: 2}}}
	current := previous
	current.Stats.CommitCoordinator.Pending = 9
	current.Stats.CommitCoordinator.AdmissionRejected++
	health := assessHealth(&previous, current)
	if health.Database != HealthCritical || health.Overall != HealthCritical || !health.Signals.CommitCoordinatorPressure || !health.Signals.CommitCoordinatorRejected {
		t.Fatalf("coordinator pressure health=%+v", health)
	}
	current.Stats.CommitCoordinator.Pending = 5
	health = assessHealth(&previous, current)
	if health.Database != HealthDegraded || !health.Signals.CommitCoordinatorPressure {
		t.Fatalf("coordinator degraded health=%+v", health)
	}
}

func TestAssessHealthReportsRecentPrimaryWriteFenceRejection(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	previous := Sample{Stats: meldbase.DBStats{StartedAt: started, PrimaryWriteFence: meldbase.PrimaryWriteFenceStats{
		Configured: true, Enforced: true, Checks: 2, Rejected: 1,
	}}}
	current := previous
	current.Stats.PrimaryWriteFence.Checks++
	current.Stats.PrimaryWriteFence.Rejected++
	health := assessHealth(&previous, current)
	if health.Database != HealthDegraded || health.Overall != HealthDegraded || !health.Signals.PrimaryWriteFenceRejected {
		t.Fatalf("primary write fence health=%+v", health)
	}
}

func TestAssessHealthIncludesCentralDispatchPressure(t *testing.T) {
	current := Sample{Stats: meldbase.DBStats{Realtime: meldbase.RealtimeStats{
		DispatchPendingChanges: 9, DispatchChangeCapacity: 10,
	}}}
	health := assessHealth(nil, current)
	if health.Realtime != HealthCritical || health.Overall != HealthCritical || !health.Signals.ReactiveQueuePressure {
		t.Fatalf("dispatch pressure health=%+v", health)
	}
}

func TestAssessHealthIncludesCentralDispatchBytePressure(t *testing.T) {
	current := Sample{Stats: meldbase.DBStats{Realtime: meldbase.RealtimeStats{
		DispatchPendingBytes: 9, DispatchByteCapacity: 10,
	}}}
	health := assessHealth(nil, current)
	if health.Realtime != HealthCritical || health.Overall != HealthCritical || !health.Signals.ReactiveQueuePressure {
		t.Fatalf("dispatch byte pressure health=%+v", health)
	}
}

func TestAssessHealthIncludesReactiveHubBytePressure(t *testing.T) {
	current := Sample{Stats: meldbase.DBStats{Realtime: meldbase.RealtimeStats{
		PendingBytes: 9, PendingByteCapacity: 10,
	}}}
	health := assessHealth(nil, current)
	if health.Realtime != HealthCritical || health.Overall != HealthCritical || !health.Signals.ReactiveQueuePressure {
		t.Fatalf("reactive byte pressure health=%+v", health)
	}
}

func TestAssessHealthIncludesDirectWatcherBytePressure(t *testing.T) {
	current := Sample{Stats: meldbase.DBStats{Realtime: meldbase.RealtimeStats{
		WatcherPendingBytes: 9, WatcherByteCapacity: 10,
	}}}
	health := assessHealth(nil, current)
	if health.Realtime != HealthCritical || health.Overall != HealthCritical || !health.Signals.ReactiveQueuePressure {
		t.Fatalf("watcher byte pressure health=%+v", health)
	}
}

func TestAssessHealthReportsRecentRollbackAnchorFailure(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	previous := Sample{Stats: meldbase.DBStats{StartedAt: started}}
	current := previous
	current.Stats.Storage.RollbackAnchorFailures = 1
	current.Stats.Storage.RollbackAnchorStore.EndpointFailures = 1
	health := assessHealth(&previous, current)
	if health.Durability != HealthDegraded || health.Overall != HealthDegraded || !health.Signals.DurabilityFailure || !health.Signals.RollbackAnchorDegraded {
		t.Fatalf("rollback anchor health=%+v", health)
	}
}

func TestAssessHealthDegradesForPersistentIndexBuildOperatorStates(t *testing.T) {
	current := Sample{Stats: meldbase.DBStats{
		IndexBuilds: meldbase.IndexBuildStats{Persistent: 1, PersistentFailed: 1},
	}}
	health := assessHealth(nil, current)
	if health.Overall != HealthDegraded || health.Storage != HealthDegraded ||
		health.Database != HealthHealthy || health.Durability != HealthHealthy ||
		!health.Signals.IndexBuildFailed || health.Signals.IndexBuildRetentionPressure {
		t.Fatalf("failed build health=%+v", health)
	}

	current.Stats.IndexBuilds = meldbase.IndexBuildStats{Persistent: 1, RetentionLeaseActive: true, RetentionPressure: true}
	current.Stats.Storage.RetentionPressure = true
	health = assessHealth(nil, current)
	if health.Overall != HealthDegraded || health.Storage != HealthDegraded ||
		health.Database != HealthHealthy || health.Durability != HealthHealthy ||
		!health.Signals.CommitRetentionPressure || !health.Signals.IndexBuildRetentionPressure ||
		health.Signals.IndexBuildFailed {
		t.Fatalf("build retention health=%+v", health)
	}

	current.Stats.Storage.RetentionPressure = false
	current.Stats.IndexBuilds.RetentionPressure = false
	health = assessHealth(nil, current)
	if health.Overall != HealthHealthy || health.Signals.IndexBuildRetentionPressure {
		t.Fatalf("lease within retention budget health=%+v", health)
	}

	current.Stats.Storage.RetentionPressure = true
	health = assessHealth(nil, current)
	if health.Overall != HealthDegraded || !health.Signals.CommitRetentionPressure ||
		health.Signals.IndexBuildRetentionPressure {
		t.Fatalf("pressure owned by another replay pin health=%+v", health)
	}
}

func TestAssessHealthUsesSessionDeltasForTransportAndTransientEvents(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	previous := Sample{
		Stats:  meldbase.DBStats{StartedAt: started},
		Server: &meldserver.ServerStats{StartedAt: started, RPCBusy: 2, RPCIdempotencyUnknown: 1},
	}
	current := previous
	server := *previous.Server
	server.RPCBusy++
	server.RPCIdempotencyUnknown++
	server.RealtimeOutboundOverflows++
	server.Worker.ProtocolFailures++
	current.Server = &server
	health := assessHealth(&previous, current)
	if health.Transport != HealthDegraded || health.Overall != HealthDegraded || !health.Signals.TransportBusy ||
		!health.Signals.RPCOutcomeUnknown || !health.Signals.WorkerProtocolFailure {
		t.Fatalf("transport health=%+v", health)
	}

	restarted := current
	restarted.Stats.StartedAt = started.Add(time.Hour)
	restartedServer := *current.Server
	restartedServer.StartedAt = started.Add(time.Hour)
	restarted.Server = &restartedServer
	health = assessHealth(&current, restarted)
	if health.Realtime != HealthHealthy || health.Durability != HealthHealthy || health.Transport != HealthHealthy {
		t.Fatalf("session reset retained transient health=%+v", health)
	}
}

func TestHealthSeverityContract(t *testing.T) {
	for level, want := range map[HealthLevel]uint64{HealthHealthy: 0, HealthDegraded: 1, HealthCritical: 2} {
		if got, ok := level.Severity(); !ok || got != want {
			t.Fatalf("severity %q=%d/%t want=%d", level, got, ok, want)
		}
	}
	if _, ok := HealthUnavailable.Severity(); ok {
		t.Fatal("unavailable health exported as numeric severity")
	}
}

func TestAssessHealthDoesNotAllocate(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	previous := Sample{Stats: meldbase.DBStats{StartedAt: started}, Server: &meldserver.ServerStats{StartedAt: started}}
	current := previous
	current.Stats.Realtime = meldbase.RealtimeStats{PendingBatches: 700, PendingBatchCapacity: 1_024}
	server := *previous.Server
	current.Server = &server
	if allocations := testing.AllocsPerRun(1_000, func() { _ = assessHealth(&previous, current) }); allocations != 0 {
		t.Fatalf("health assessment allocations=%v, want 0", allocations)
	}
}

func BenchmarkAssessHealth(b *testing.B) {
	started := time.Unix(1_700_000_000, 0)
	previous := Sample{Stats: meldbase.DBStats{StartedAt: started}, Server: &meldserver.ServerStats{StartedAt: started}}
	current := previous
	current.Stats.Realtime = meldbase.RealtimeStats{PendingBatches: 700, PendingBatchCapacity: 1_024}
	server := *previous.Server
	server.RPCBusy = 1
	current.Server = &server
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		_ = assessHealth(&previous, current)
	}
}
