package meldotel

import (
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/crapthings/meldbase/admin"
	"github.com/crapthings/meldbase/core"
	meldserver "github.com/crapthings/meldbase/server"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type fakeSource struct {
	sample admin.Sample
	ok     bool
}

func (source *fakeSource) Latest() (admin.Sample, bool) { return source.sample, source.ok }

type capturedSink struct {
	ints   map[string]int64
	floats map[string]float64
}

type discardSink struct{}

func (discardSink) Int64(string, metric.Int64Observable, int64)       {}
func (discardSink) Float64(string, metric.Float64Observable, float64) {}

func (sink *capturedSink) Int64(name string, _ metric.Int64Observable, value int64) {
	sink.ints[name] = value
}
func (sink *capturedSink) Float64(name string, _ metric.Float64Observable, value float64) {
	sink.floats[name] = value
}

func TestAdapterMapsOnlyFixedAggregateMeasurements(t *testing.T) {
	source := &fakeSource{ok: true, sample: representativeSample()}
	adapter, err := New(source, Options{MeterProvider: noop.NewMeterProvider(), InstrumentationVersion: "test"})
	if err != nil {
		t.Fatal(err)
	}
	sink := &capturedSink{ints: make(map[string]int64), floats: make(map[string]float64)}
	adapter.observe(sink)
	for name, want := range map[string]int64{
		"meldbase.up": 1, "meldbase.commit.sequence": 17, "meldbase.document.count": 31,
		"meldbase.commit.coordinator.enabled": 1, "meldbase.commit.coordinator.pending": 3,
		"meldbase.commit.coordinator.pending_capacity": 8, "meldbase.commit.coordinator.admitted": 21,
		"meldbase.commit.coordinator.admission_rejected": 2, "meldbase.commit.coordinator.batch": 10,
		"meldbase.commit.coordinator.grouped_transaction": 20, "meldbase.commit.coordinator.outcome_unknown": 1,
		"meldbase.health.overall": 1, "meldbase.health.database": 0,
		"meldbase.health.realtime": 1, "meldbase.database.write_disabled": 0,
		"meldbase.recovery.performed": 1, "meldbase.recovery.fallback": 1,
		"meldbase.recovery.meta.checksum_valid": 2, "meldbase.recovery.meta.root_valid": 1,
		"meldbase.recovery.main_tail_removed": 17, "meldbase.recovery.wal_replayed": 3,
		"meldbase.primary_write_fence.configured": 1, "meldbase.primary_write_fence.enforced": 1,
		"meldbase.primary_write_fence.check": 9, "meldbase.primary_write_fence.rejection": 2,
		"meldbase.query.count": 11, "meldbase.realtime.queue.overflow": 2,
		"meldbase.realtime.dispatch.pending.batch": 2, "meldbase.realtime.dispatch.pending.change": 3,
		"meldbase.realtime.dispatch.pending.size": 4, "meldbase.realtime.dispatch.pending.batch_capacity": 1024,
		"meldbase.realtime.dispatch.pending.change_capacity": 8192, "meldbase.realtime.dispatch.pending.max_size": 67108864,
		"meldbase.realtime.pending.size": 6, "meldbase.realtime.pending.max_size": 67108864,
		"meldbase.realtime.watcher.pending.size": 7, "meldbase.realtime.watcher.pending.max_size": 134217728,
		"meldbase.write_transaction.active": 1, "meldbase.write_transaction.started": 10,
		"meldbase.write_transaction.committed": 4, "meldbase.write_transaction.noop": 2,
		"meldbase.write_transaction.conflict": 3, "meldbase.write_transaction.aborted": 1,
		"meldbase.resource.limit.rejection": 3, "meldbase.resource.document.max": 1024,
		"meldbase.resource.transaction.max": 4096, "meldbase.resource.transaction.change.max": 8,
		"meldbase.index.build.failed_persistent": 2, "meldbase.index.build.retention_lease.active": 1,
		"meldbase.index.build.retention.pressure": 1,
		"meldbase.wal.current.size":               4096, "meldbase.checkpoint.automatic": 3,
		"meldbase.storage.physical.page": 101, "meldbase.storage.transaction.rejected": 4,
		"meldbase.storage.generation": 20, "meldbase.storage.rollback.protected": 1,
		"meldbase.storage.rollback.anchor.sequence": 17, "meldbase.storage.rollback.anchor.lag": 1,
		"meldbase.storage.rollback.anchor.generation": 19, "meldbase.storage.rollback.anchor.generation_lag": 1,
		"meldbase.storage.rollback.anchor.failure": 2,
		"meldbase.storage.rollback.anchor.replica": 3, "meldbase.storage.rollback.anchor.quorum": 2,
		"meldbase.storage.rollback.anchor.store.load": 7, "meldbase.storage.rollback.anchor.store.advance": 6,
		"meldbase.storage.rollback.anchor.endpoint.failure": 5, "meldbase.storage.rollback.anchor.quorum.failure": 4,
		"meldbase.storage.rollback.anchor.conflict": 3, "meldbase.storage.rollback.anchor.authentication.failure": 2,
		"meldbase.storage.rollback.anchor.protocol.failure":      1,
		"meldbase.storage.rollback.anchor.configuration.failure": 8,
		"meldbase.storage.size":                                  8192, "meldbase.storage.max_size": 8000,
		"meldbase.storage.size_overage": 192, "meldbase.storage.quota.exhausted": 1, "meldbase.storage.limit.rejection": 5,
		"meldbase.storage.retained_commit": 14, "meldbase.storage.retention.max": 10,
		"meldbase.storage.retention.overage": 4, "meldbase.storage.retention.pressure": 1,
		"meldbase.storage.retained_commit.size": 1024, "meldbase.storage.retention.max_size": 900,
		"meldbase.storage.retention.size_overage": 124,
		"meldbase.storage.retention.pruned":       23, "meldbase.storage.retention.pressure_event": 2,
		"meldbase.backup.attempt": 3, "meldbase.backup.completed": 2,
		"meldbase.backup.failure": 1, "meldbase.backup.last.size": 1048576,
		"meldbase.server.connection.active": 2, "meldbase.rpc.request": 19,
		"meldbase.realtime.outbound.queue.overflow": 8,
		"meldbase.worker.policy.invalidation":       6, "meldbase.admin.delivery.dropped": 7,
	} {
		if got, exists := sink.ints[name]; !exists || got != want {
			t.Fatalf("%s=%d/%t want=%d", name, got, exists, want)
		}
	}
	for name, want := range map[string]float64{
		"meldbase.wal.append.time":                      0.009,
		"meldbase.storage.commit.max_duration":          0.004,
		"meldbase.storage.rollback.anchor.time":         0.006,
		"meldbase.storage.rollback.anchor.max_duration": 0.003,
		"meldbase.storage.rollback.anchor.timeout":      10,
		"meldbase.backup.last_duration":                 1.5,
		"meldbase.cache.page.hit_ratio":                 0.8,
		"meldbase.rpc.time":                             0.012,
	} {
		if got, exists := sink.floats[name]; !exists || math.Abs(got-want) > 1e-12 {
			t.Fatalf("%s=%g/%t want=%g", name, got, exists, want)
		}
	}
	if stats := adapter.Stats(); stats.Collections != 1 || stats.UnavailableSamples != 0 || stats.SaturatedMeasurements != 0 || stats.Closed {
		t.Fatalf("adapter stats=%+v", stats)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	adapter.observe(sink)
	if stats := adapter.Stats(); stats.Collections != 1 || !stats.Closed {
		t.Fatalf("closed adapter stats=%+v", stats)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAdapterMissingSampleAndUint64SaturationAreFailSafe(t *testing.T) {
	source := &fakeSource{}
	adapter, err := New(source, Options{MeterProvider: noop.NewMeterProvider()})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	sink := &capturedSink{ints: make(map[string]int64), floats: make(map[string]float64)}
	adapter.observe(sink)
	if len(sink.ints) != 1 || sink.ints["meldbase.up"] != 0 || adapter.Stats().UnavailableSamples != 1 {
		t.Fatalf("missing sample ints=%v stats=%+v", sink.ints, adapter.Stats())
	}

	source.ok = true
	source.sample = representativeSample()
	source.sample.Stats.Commits.Total = math.MaxUint64
	adapter.observe(sink)
	if sink.ints["meldbase.commit.count"] != math.MaxInt64 || adapter.Stats().SaturatedMeasurements != 1 {
		t.Fatalf("saturation value=%d stats=%+v", sink.ints["meldbase.commit.count"], adapter.Stats())
	}
}

func TestAdapterContractHasUniqueSafeNamesAndNoDynamicIdentity(t *testing.T) {
	namePattern := regexp.MustCompile(`^meldbase\.[a-z][a-z0-9_.]*$`)
	seen := map[string]struct{}{"meldbase.up": {}}
	check := func(name, description, unit string) {
		t.Helper()
		if !namePattern.MatchString(name) || description == "" || unit == "" {
			t.Fatalf("invalid instrument name=%q description=%q unit=%q", name, description, unit)
		}
		if _, duplicate := seen[name]; duplicate {
			t.Fatalf("duplicate instrument %q", name)
		}
		seen[name] = struct{}{}
		lower := strings.ToLower(name + " " + description)
		for _, forbidden := range []string{"collection.name", "document.id", "query.text", "principal", "tenant", "credential", "db.client"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("instrument %q contains forbidden identity %q", name, forbidden)
			}
		}
	}
	for _, descriptor := range intDescriptors {
		check(descriptor.name, descriptor.description, descriptor.unit)
	}
	for _, descriptor := range floatDescriptors {
		check(descriptor.name, descriptor.description, descriptor.unit)
	}
	if len(seen) < 50 {
		t.Fatalf("aggregate contract unexpectedly small: %d instruments", len(seen))
	}
	public := Instruments()
	if SchemaVersion != 12 || len(public) != len(seen) || public[0].Name != "meldbase.up" {
		t.Fatalf("public schema version=%d instruments=%d/%d first=%+v", SchemaVersion, len(public), len(seen), public[0])
	}
	public[0].Name = "mutated"
	if Instruments()[0].Name != "meldbase.up" {
		t.Fatal("caller mutated the shared instrument schema")
	}
}

func TestAdapterRequiresExplicitSourceAndMeterProvider(t *testing.T) {
	if _, err := New(nil, Options{MeterProvider: noop.NewMeterProvider()}); err == nil {
		t.Fatal("nil source accepted")
	}
	if _, err := New(&fakeSource{}, Options{}); err == nil {
		t.Fatal("nil MeterProvider accepted")
	}
}

func representativeSample() admin.Sample {
	return admin.Sample{
		Version: admin.SchemaVersion,
		Stats: meldbase.DBStats{
			CommitSequence: 17, Collections: 3, Documents: 31, Indexes: 4,
			Recovery: meldbase.RecoveryReport{
				SchemaVersion: 1, Engine: "v2", Recovered: true, FallbackToOlderRoot: true,
				ChecksumValidMetaSlots: 2, RootValidMetaSlots: 1, MainTailBytesRemoved: 17, WALRecordsReplayed: 3,
			},
			Commits: meldbase.CommitStats{Total: 9, Changes: 13},
			CommitCoordinator: meldbase.V2CommitCoordinatorStats{
				Enabled: true, Pending: 3, PendingCapacity: 8, Admitted: 21, AdmissionRejected: 2,
				Batches: 10, GroupedTransactions: 20, OutcomeUnknown: 1,
			},
			PrimaryWriteFence: meldbase.V2PrimaryWriteFenceStats{Configured: true, Enforced: true, Checks: 9, Rejected: 2},
			Transactions:      meldbase.WriteTransactionStats{Active: 1, Started: 10, Committed: 4, Noops: 2, Conflicts: 3, Aborted: 1},
			Queries:           meldbase.QueryStats{Total: 11, Failed: 1, ActiveCursors: 2, DocumentsExamined: 22, DocumentsReturned: 10},
			Realtime: meldbase.RealtimeStats{
				SharedViews: 3, QuerySubscribers: 4, QueueOverflows: 2, SlowConsumers: 1,
				PendingBytes: 6, PendingBatchCapacity: 1024, PendingChangeCapacity: 65536, PendingByteCapacity: 67108864,
				WatcherPendingBytes: 7, WatcherByteCapacity: 134217728,
				DispatchPendingBatches: 2, DispatchPendingChanges: 3, DispatchPendingBytes: 4,
				DispatchBatchCapacity: 1024, DispatchChangeCapacity: 8192, DispatchByteCapacity: 67108864,
			},
			Durability: meldbase.DurabilityStats{
				WALAppends: 8, WALCurrentBytes: 4096, WALCurrentCommits: 2, WALAppendNanos: 9_000_000,
				CheckpointAttempts: 4, CheckpointsCompleted: 3, AutomaticCheckpoints: 3,
			},
			Storage: meldbase.StorageStats{
				Generation: 20, CommitSequence: 18, RollbackProtected: true, RollbackAnchorSequence: 17, RollbackAnchorGeneration: 19,
				RollbackAnchorFailures: 2, RollbackAnchorTimeout: 10 * time.Second, RollbackAnchorNanos: 6_000_000, RollbackAnchorMaxLatency: 3 * time.Millisecond,
				RollbackAnchorStore: meldbase.RollbackAnchorStoreStatus{Replicas: 3, Quorum: 2, Loads: 7, Advances: 6, EndpointFailures: 5, QuorumFailures: 4, Conflicts: 3, AuthenticationFailures: 2, ProtocolFailures: 1, ConfigurationFailures: 8},
				PhysicalPages:       101, ReusablePages: 9, CommittedTransactions: 13, RejectedTransactions: 4,
				RetainedCommits: 14, CommitRetentionMax: 10, CommitRetentionOverage: 4,
				RetainedCommitBytes: 1024, CommitRetentionMaxBytes: 900, CommitRetentionByteOverage: 124,
				RetentionPrunedCommits: 23, RetentionPressureEvents: 2, RetentionPressure: true,
				StorageUsedBytes: 8192, StorageMaxBytes: 8000, StorageByteOverage: 192,
				StorageLimitRejections: 5, StorageQuotaExhausted: true,
				CommitNanos: 11_000_000, CommitMaxLatency: 4 * time.Millisecond,
			},
			Resources: meldbase.ResourceStats{Limits: meldbase.ResourceLimits{
				MaxDocumentBytes: 1024, MaxTransactionBytes: 4096, MaxTransactionChanges: 8,
				MaxIndexBuildEntries: 64, MaxIndexBuildBytes: 8192,
			}, Rejections: 3},
			IndexBuilds: meldbase.IndexBuildStats{Persistent: 3, PersistentFailed: 2, RetentionLeaseActive: true, RetentionPressure: true},
			Backup:      meldbase.BackupStats{Attempts: 3, Completed: 2, Failed: 1, LastBytes: 1 << 20, LastDuration: 1500 * time.Millisecond},
		},
		Health: admin.HealthStatus{
			Overall: admin.HealthDegraded, Database: admin.HealthHealthy, Durability: admin.HealthHealthy,
			Storage: admin.HealthHealthy, Realtime: admin.HealthDegraded, Telemetry: admin.HealthHealthy,
			Transport: admin.HealthUnavailable,
		},
		Rates:   admin.Rates{Valid: true, PageCacheHitRatio: 0.8, DocumentCacheHitRatio: 0.75},
		Sampler: admin.SamplerStatus{DroppedDeliveries: 7},
		Server: &meldserver.ServerStats{
			ActiveConnections: 2, RealtimeOutboundOverflows: 8, RPCRequests: 19, RPCActive: 1, RPCTotalNanos: 12_000_000,
			Worker: meldserver.WorkerHubStats{ConnectedWorkers: 1, PolicyInvalidations: 6},
		},
	}
}

func BenchmarkAdapterObserve(b *testing.B) {
	adapter, err := New(&fakeSource{ok: true, sample: representativeSample()}, Options{MeterProvider: noop.NewMeterProvider()})
	if err != nil {
		b.Fatal(err)
	}
	defer adapter.Close()
	sink := discardSink{}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		adapter.observe(sink)
	}
}
