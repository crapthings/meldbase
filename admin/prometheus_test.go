package admin

import (
	"math"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crapthings/meldbase/core"
	meldserver "github.com/crapthings/meldbase/server"
)

func TestMarshalPrometheusProducesCompleteLowCardinalityContract(t *testing.T) {
	sample := representativePrometheusSample()
	payload := MarshalPrometheus(sample)
	text := string(payload)
	if len(payload) == 0 || payload[len(payload)-1] != '\n' {
		t.Fatal("Prometheus payload must be non-empty and newline terminated")
	}
	for _, forbidden := range []string{"secret_engine", "private_collection", "private_field", "private-value", "NaN", "+Inf", "-Inf", "%!"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Prometheus payload contains %q: %s", forbidden, text)
		}
	}
	for _, expected := range []string{
		`meldbase_writes_disabled 1`,
		`meldbase_health_status{component="overall"} 2`,
		`meldbase_health_status{component="database"} 2`,
		`meldbase_health_status{component="realtime"} 1`,
		`meldbase_health_status{component="telemetry"} 0`,
		`meldbase_database_info{engine="v1"} 1`,
		`meldbase_recovery_performed 1`,
		`meldbase_recovery_fallback_to_older_root 1`,
		`meldbase_recovery_meta_slots{validation="checksum"} 2`,
		`meldbase_recovery_meta_slots{validation="root"} 1`,
		`meldbase_recovery_main_tail_bytes_removed 17`,
		`meldbase_recovery_wal_records_replayed 3`,
		`meldbase_recovery_wal_tail_bytes_removed 11`,
		`meldbase_query_plan_total{stage="collection_scan"} 3`,
		`meldbase_query_plan_total{stage="index_scan"} 4`,
		`meldbase_query_plan_total{stage="id_lookup"} 5`,
		`meldbase_write_transaction_active 1`,
		`meldbase_write_transaction_started_total 10`,
		`meldbase_write_transaction_outcomes_total{outcome="committed"} 4`,
		`meldbase_write_transaction_outcomes_total{outcome="noop"} 2`,
		`meldbase_write_transaction_outcomes_total{outcome="conflict"} 3`,
		`meldbase_write_transaction_outcomes_total{outcome="aborted"} 1`,
		`meldbase_commit_coordinator_enabled 1`,
		`meldbase_commit_coordinator_pending 3`,
		`meldbase_commit_coordinator_pending_capacity 8`,
		`meldbase_commit_coordinator_admitted_total 21`,
		`meldbase_commit_coordinator_admission_rejected_total 2`,
		`meldbase_commit_coordinator_batches_total 10`,
		`meldbase_commit_coordinator_grouped_transactions_total 20`,
		`meldbase_commit_coordinator_outcome_unknown_total 1`,
		`meldbase_primary_write_fence_configured 1`,
		`meldbase_primary_write_fence_enforced 1`,
		`meldbase_primary_write_fence_checks_total 9`,
		`meldbase_primary_write_fence_rejections_total 2`,
		`meldbase_storage_transactions_total{outcome="committed"} 13`,
		`meldbase_storage_generation 5`,
		`meldbase_storage_rollback_protected 1`,
		`meldbase_storage_rollback_anchor_sequence 1`,
		`meldbase_storage_rollback_anchor_generation 4`,
		`meldbase_storage_rollback_anchor_lag 1`,
		`meldbase_storage_rollback_anchor_generation_lag 1`,
		`meldbase_storage_rollback_anchor_failures_total 2`,
		`meldbase_storage_rollback_anchor_replicas 3`,
		`meldbase_storage_rollback_anchor_quorum 2`,
		`meldbase_storage_rollback_anchor_store_loads_total 7`,
		`meldbase_storage_rollback_anchor_store_advances_total 6`,
		`meldbase_storage_rollback_anchor_endpoint_failures_total 5`,
		`meldbase_storage_rollback_anchor_quorum_failures_total 4`,
		`meldbase_storage_rollback_anchor_conflicts_total 3`,
		`meldbase_storage_rollback_anchor_authentication_failures_total 2`,
		`meldbase_storage_rollback_anchor_protocol_failures_total 1`,
		`meldbase_storage_rollback_anchor_configuration_failures_total 8`,
		`meldbase_storage_rollback_anchor_timeout_seconds 10`,
		`meldbase_storage_rollback_anchor_duration_seconds_total 0.004`,
		`meldbase_storage_tree_splits_total 11`,
		`meldbase_storage_tree_merges_total 6`,
		`meldbase_storage_persistent_free_space 1`,
		`meldbase_storage_free_space_load_failures_total 2`,
		`meldbase_storage_free_space_candidate_checks_total 5`,
		`meldbase_storage_retained_commits 14`,
		`meldbase_storage_commit_retention_max 10`,
		`meldbase_storage_commit_retention_overage 4`,
		`meldbase_storage_commit_retention_pressure 1`,
		`meldbase_storage_retention_pruned_commits_total 23`,
		`meldbase_storage_retention_pressure_events_total 2`,
		`meldbase_index_build_failed_persistent 2`,
		`meldbase_index_build_retention_lease_active 1`,
		`meldbase_index_build_retention_pressure 1`,
		`meldbase_backup_attempts_total 3`,
		`meldbase_backup_completed_total 2`,
		`meldbase_backup_failures_total 1`,
		`meldbase_backup_last_bytes 1048576`,
		`meldbase_backup_last_duration_seconds 1.5`,
		`meldbase_realtime_pending_batch_capacity 1024`,
		`meldbase_realtime_pending_change_capacity 65536`,
		`meldbase_realtime_pending_bytes 6`,
		`meldbase_realtime_pending_byte_capacity 67108864`,
		`meldbase_realtime_watcher_pending_bytes 7`,
		`meldbase_realtime_watcher_byte_capacity 134217728`,
		`meldbase_realtime_dispatch_pending_batches 2`,
		`meldbase_realtime_dispatch_pending_changes 3`,
		`meldbase_realtime_dispatch_pending_bytes 4`,
		`meldbase_realtime_dispatch_batch_capacity 1024`,
		`meldbase_realtime_dispatch_change_capacity 8192`,
		`meldbase_realtime_dispatch_byte_capacity 67108864`,
		`meldbase_wal_current_bytes 8192`,
		`meldbase_wal_current_commits 2`,
		`meldbase_checkpoints_completed_total 5`,
		`meldbase_automatic_checkpoints_total 4`,
		`meldbase_diagnostics_operations_observed_total{kind="query"} 23`,
		`meldbase_rpc_requests_total 41`,
		`meldbase_rpc_active 2`,
		`meldbase_rpc_duration_seconds_total 0.007`,
		`meldbase_rpc_idempotency_replays_total 5`,
		`meldbase_rpc_idempotency_unknown_total 2`,
		`meldbase_rpc_atomic_commits_total 6`,
		`meldbase_rpc_atomic_rollbacks_total 3`,
		`meldbase_worker_connected 2`,
		`meldbase_worker_registered_publications 3`,
		`meldbase_worker_policy_evaluations_total 13`,
		`meldbase_worker_policy_denied_total 2`,
		`meldbase_worker_policy_invalidations_total 4`,
		`meldbase_worker_transaction_operations_total 17`,
		`meldbase_realtime_outbound_queue_overflows_total 8`,
		`meldbase_admin_dropped_deliveries_total 29`,
	} {
		if !strings.Contains(text, expected+"\n") {
			t.Fatalf("missing metric %q", expected)
		}
	}
	if strings.Contains(text, "meldbase_wal_append_duration_seconds_total -") {
		t.Fatal("uint64 nanosecond counter overflowed through time.Duration")
	}
	validatePrometheusText(t, text)
}

func TestMarshalPrometheusDoesNotReportRollbackLagWhenProtectionIsDisabled(t *testing.T) {
	payload := string(MarshalPrometheus(Sample{Stats: meldbase.DBStats{CommitSequence: 9}}))
	if !strings.Contains(payload, "meldbase_storage_rollback_anchor_lag 0\n") {
		t.Fatalf("unexpected rollback lag metric:\n%s", payload)
	}
}

func representativePrometheusSample() Sample {
	return Sample{
		Version: SchemaVersion,
		Stats: meldbase.DBStats{
			Uptime: 3*time.Second + 250*time.Millisecond, Durable: true, WritesDisabled: true,
			CommitSequence: 2, Collections: 3, Documents: 4, Indexes: 5, ActiveChangeWatchers: 6,
			Recovery: meldbase.RecoveryReport{
				SchemaVersion: 1, Engine: "v1", Recovered: true, FallbackToOlderRoot: true,
				ChecksumValidMetaSlots: 2, RootValidMetaSlots: 1, MainTailBytesRemoved: 17,
				WALRecordsReplayed: 3, WALTailBytesRemoved: 11,
			},
			Commits: meldbase.CommitStats{Total: 7, Changes: 8},
			CommitCoordinator: meldbase.V2CommitCoordinatorStats{
				Enabled: true, Pending: 3, PendingCapacity: 8, Admitted: 21, AdmissionRejected: 2,
				Batches: 10, GroupedTransactions: 20, OutcomeUnknown: 1,
			},
			PrimaryWriteFence: meldbase.V2PrimaryWriteFenceStats{Configured: true, Enforced: true, Checks: 9, Rejected: 2},
			Transactions:      meldbase.WriteTransactionStats{Active: 1, Started: 10, Committed: 4, Noops: 2, Conflicts: 3, Aborted: 1},
			Queries: meldbase.QueryStats{
				ActiveCursors: 2, Total: 17, Failed: 2, CollectionScans: 3, IndexScans: 4,
				IDLookups: 5, DocumentsExamined: 21, DocumentsReturned: 13,
			},
			Realtime: meldbase.RealtimeStats{
				SharedViews: 2, QuerySubscribers: 3, PendingBatches: 4, PendingChanges: 5, PendingBytes: 6,
				PendingBatchCapacity: 1024, PendingChangeCapacity: 65536, PendingByteCapacity: 67108864,
				WatcherPendingBytes: 7, WatcherByteCapacity: 134217728,
				DispatchPendingBatches: 2, DispatchPendingChanges: 3, DispatchPendingBytes: 4,
				DispatchBatchCapacity: 1024, DispatchChangeCapacity: 8192, DispatchByteCapacity: 67108864,
				QueueOverflows: 6, SlowConsumers: 7, IncrementalBatches: 8,
				FullViewRecomputes: 9, DeltaDeliveries: 10,
			},
			Durability: meldbase.DurabilityStats{
				WALAppends: 3, WALPayloadBytes: 4096, WALCurrentBytes: 8192, WALCurrentCommits: 2, WALAppendFailures: 1,
				WALAppendNanos: math.MaxUint64, WALAppendMaxLatency: 4 * time.Millisecond,
				CheckpointAttempts: 7, CheckpointsCompleted: 5, CheckpointFailures: 2, AutomaticCheckpoints: 4,
				CheckpointNanos: 12_000_000, CheckpointMaxLatency: 7 * time.Millisecond,
			},
			Storage: meldbase.StorageStats{
				Engine: "secret_engine", PageSize: 16_384, PhysicalPages: 101,
				Generation: 5, CommitSequence: 2, RollbackProtected: true, RollbackAnchorSequence: 1, RollbackAnchorGeneration: 4, RollbackAnchorFailures: 2,
				RollbackAnchorStore:   meldbase.RollbackAnchorStoreStatus{Replicas: 3, Quorum: 2, Loads: 7, Advances: 6, EndpointFailures: 5, QuorumFailures: 4, Conflicts: 3, AuthenticationFailures: 2, ProtocolFailures: 1, ConfigurationFailures: 8},
				RollbackAnchorTimeout: 10 * time.Second, RollbackAnchorNanos: 4_000_000, RollbackAnchorMaxLatency: 2 * time.Millisecond,
				OldestRetainedSequence: 12, ActiveReaders: 2, ActiveReplayLeases: 1, ReusablePages: 7,
				PageCache: meldbase.PageCacheStats{CapacityPages: 100, ResidentPages: 80, Hits: 50, Misses: 5, Evictions: 2},
				DocumentCache: meldbase.DocumentCacheStats{
					CapacityEntries: 90, CapacityBytes: 1 << 20, Entries: 70, Bytes: 4096,
					Hits: 40, Misses: 4, Evictions: 1,
				},
				CommitAttempts: 17, CommittedTransactions: 13, RejectedTransactions: 4, TreeSplits: 11, TreeMerges: 6,
				PersistentFreeSpace: true, FreeSpaceLoads: 3, FreeSpaceLoadFailures: 2, FreeSpacePublishes: 4, FreeSpaceCandidateChecks: 5,
				RetainedCommits: 14, CommitRetentionMax: 10, CommitRetentionOverage: 4,
				RetainedCommitBytes: 1024, CommitRetentionMaxBytes: 900, CommitRetentionByteOverage: 124,
				RetentionPrunedCommits: 23, RetentionPressureEvents: 2, RetentionPressure: true,
				StorageUsedBytes: 8192, StorageMaxBytes: 8000, StorageByteOverage: 192,
				StorageLimitRejections: 5, StorageQuotaExhausted: true,
				CommitNanos: 9_000_000, CommitMaxLatency: 3 * time.Millisecond,
			},
			Compaction: meldbase.CompactionStats{
				Active: 1, Attempts: 2, Completed: 1, Failed: 1,
				InputBytes: 1000, OutputBytes: 500, LastDuration: time.Second,
			},
			Reclamation: meldbase.ReclamationStats{
				Active: 1, Attempts: 2, Completed: 1, Failed: 1,
				LastReachable: 80, LastReclaimable: 20, LastDuration: 2 * time.Second,
			},
			Backup: meldbase.BackupStats{
				Attempts: 3, Completed: 2, Failed: 1, LastBytes: 1 << 20, LastDuration: 1500 * time.Millisecond,
			},
			Diagnostics: meldbase.DiagnosticStats{
				Enabled: true, Capacity: 32, Retained: 4, Recorded: 7, Overwritten: 3,
				QueriesObserved: 23, CommitsObserved: 11,
			},
			Resources: meldbase.ResourceStats{Limits: meldbase.ResourceLimits{
				MaxDocumentBytes: 1024, MaxTransactionBytes: 4096, MaxTransactionChanges: 8,
				MaxIndexBuildEntries: 64, MaxIndexBuildBytes: 8192,
			}, Rejections: 3},
			IndexBuilds: meldbase.IndexBuildStats{Persistent: 3, PersistentFailed: 2, RetentionLeaseActive: true, RetentionPressure: true},
		},
		Health: HealthStatus{
			Overall: HealthCritical, Database: HealthCritical, Durability: HealthCritical,
			Storage: HealthHealthy, Realtime: HealthDegraded, Telemetry: HealthHealthy, Transport: HealthUnavailable,
		},
		Server: &meldserver.ServerStats{
			ActiveConnections: 2, ConnectionsAccepted: 11, RealtimeOutboundOverflows: 8,
			RPCRequests: 41, RPCActive: 2, RPCSucceeded: 31, RPCFailed: 3, RPCCanceled: 2, RPCRejected: 4, RPCBusy: 1,
			RPCArguments: 50, RPCRequestBytes: 4096, RPCResultBytes: 2048,
			RPCTotalNanos: 7_000_000, RPCMaxLatency: 2 * time.Millisecond,
			RPCIdempotencyClaims: 7, RPCIdempotencyReplays: 5, RPCIdempotencyConflicts: 1,
			RPCIdempotencyInProgress: 3, RPCIdempotencyUnknown: 2, RPCIdempotencyFailures: 4,
			RPCAtomicCommits: 6, RPCAtomicRollbacks: 3, RPCAtomicNoopCompletions: 2,
			Worker: meldserver.WorkerHubStats{
				ConnectedWorkers: 2, RegisteredMethods: 5, RegisteredPublications: 3,
				CallsStarted: 19, CallsActive: 1, CallsBusy: 2, ProtocolFailures: 1, TransactionOps: 17,
				PolicyEvaluations: 13, PolicyActive: 1, PolicyDenied: 2, PolicyFailed: 1, PolicyBusy: 1, PolicyInvalidations: 4,
			},
		},
		Sampler: SamplerStatus{Samples: 31, Subscribers: 2, DroppedDeliveries: 29, HistorySamples: 30},
	}
}

func validatePrometheusText(t *testing.T, payload string) {
	t.Helper()
	commentPattern := regexp.MustCompile(`^# (HELP|TYPE) (meldbase_[a-z0-9_]+) (.+)$`)
	samplePattern := regexp.MustCompile(`^(meldbase_[a-z0-9_]+)(\{[a-z_]+="[a-z0-9_]+"\})? (-?[0-9]+(?:\.[0-9]+)?(?:e[+-]?[0-9]+)?)$`)
	helpSeen := map[string]bool{}
	typeSeen := map[string]string{}
	for number, line := range strings.Split(strings.TrimSuffix(payload, "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			matches := commentPattern.FindStringSubmatch(line)
			if matches == nil {
				t.Fatalf("invalid metadata line %d: %q", number+1, line)
			}
			name := matches[2]
			if matches[1] == "HELP" {
				if helpSeen[name] {
					t.Fatalf("duplicate HELP for %s", name)
				}
				helpSeen[name] = true
			} else {
				if typeSeen[name] != "" || (matches[3] != "counter" && matches[3] != "gauge") {
					t.Fatalf("invalid TYPE for %s: %q", name, matches[3])
				}
				typeSeen[name] = matches[3]
				if matches[3] == "counter" && !strings.HasSuffix(name, "_total") {
					t.Fatalf("counter lacks _total suffix: %s", name)
				}
			}
			continue
		}
		matches := samplePattern.FindStringSubmatch(line)
		if matches == nil {
			t.Fatalf("invalid sample line %d: %q", number+1, line)
		}
		name := matches[1]
		if !helpSeen[name] || typeSeen[name] == "" {
			t.Fatalf("sample precedes HELP/TYPE for %s", name)
		}
	}
	if len(helpSeen) == 0 || len(helpSeen) != len(typeSeen) {
		t.Fatalf("metadata family mismatch help=%d type=%d", len(helpSeen), len(typeSeen))
	}
}

func BenchmarkMarshalPrometheus(b *testing.B) {
	sample := representativePrometheusSample()
	outputBytes := len(MarshalPrometheus(sample))
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_ = MarshalPrometheus(sample)
	}
	b.StopTimer()
	b.ReportMetric(float64(outputBytes), "output-B")
}

func TestMarshalPrometheusAllocationAndCapacityBudget(t *testing.T) {
	sample := representativePrometheusSample()
	output := MarshalPrometheus(sample)
	if len(output) == 0 || len(output) > prometheusInitialCapacity {
		t.Fatalf("rendered bytes=%d initial capacity=%d", len(output), prometheusInitialCapacity)
	}
	// Go 1.23 materializes more strconv temporaries than the current toolchain,
	// and race instrumentation adds one more (51 versus 33 in this fixture).
	// Keep one cross-mode budget that still catches a regression above the
	// observed fixed-schema renderer.
	if allocations := testing.AllocsPerRun(1_000, func() { _ = MarshalPrometheus(sample) }); allocations > 51 {
		t.Fatalf("Prometheus render allocations=%v, budget=51", allocations)
	}
}

func TestMarshalPrometheusIsDeterministicUnderConcurrency(t *testing.T) {
	sample := representativePrometheusSample()
	expected := string(MarshalPrometheus(sample))
	var workers sync.WaitGroup
	errors := make(chan string, 32)
	for worker := 0; worker < 32; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 100; iteration++ {
				if actual := string(MarshalPrometheus(sample)); actual != expected {
					errors <- actual
					return
				}
			}
		}()
	}
	workers.Wait()
	close(errors)
	if actual, ok := <-errors; ok {
		t.Fatalf("concurrent output changed: %q", actual)
	}
}
