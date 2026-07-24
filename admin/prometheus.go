package admin

import (
	"bytes"
	"strconv"
	"time"

	"github.com/crapthings/meldbase"
)

const PrometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

// The fixed schema currently renders below this size even with the optional
// transport families. Pre-growing once avoids a late buffer growth and copy
// on every scrape. Keep BenchmarkMarshalPrometheus's output-B metric and tests
// visible when adding families so this remains an optimization, never a limit.
const prometheusInitialCapacity = 48 << 10

type prometheusSample struct {
	labels  string
	value   string
	number  uint64
	numeric bool
}

// MarshalPrometheus renders one sampled database state using the Prometheus
// text exposition format 0.0.4. It emits only fixed metric names and fixed-enum
// labels; no application-controlled string can enter the output.
func MarshalPrometheus(sample Sample) []byte {
	stats := sample.Stats
	var output bytes.Buffer
	output.Grow(prometheusInitialCapacity)
	writePrometheusFamily(&output, "meldbase_up", "Whether the sampled database is open.", "gauge", gaugeBool(!stats.Closed))
	writePrometheusFamily(&output, "meldbase_writes_disabled", "Whether a fail-stop durability error has disabled database writes.", "gauge", gaugeBool(stats.WritesDisabled))
	writePrometheusFamily(&output, "meldbase_health_status", "Derived component health: 0 healthy, 1 degraded, 2 critical.", "gauge", healthPrometheusSamples(sample.Health)...)
	writePrometheusFamily(&output, "meldbase_database_info", "Static information about the sampled database engine.", "gauge", prometheusSample{labels: `{engine="` + safeEngine(stats) + `"}`, value: "1"})
	writePrometheusFamily(&output, "meldbase_recovery_performed", "Whether startup selected a fallback, removed a provable tail, or degraded an optional accelerator.", "gauge", gaugeBool(stats.Recovery.Recovered))
	writePrometheusFamily(&output, "meldbase_recovery_fallback_to_older_root", "Whether startup selected an older valid Meta root after rejecting a newer root.", "gauge", gaugeBool(stats.Recovery.FallbackToOlderRoot))
	writePrometheusFamily(&output, "meldbase_recovery_meta_redundancy_degraded", "Whether fewer redundant Meta roots than expected survived startup validation.", "gauge", gaugeBool(stats.Recovery.MetaRedundancyDegraded))
	writePrometheusFamily(&output, "meldbase_recovery_acceleration_degraded", "Whether startup discarded an optional acceleration structure.", "gauge", gaugeBool(stats.Recovery.AccelerationDegraded))
	writePrometheusFamily(&output, "meldbase_recovery_meta_slots", "Meta slots validated at startup by fixed validation level.", "gauge",
		labeledUint(`{validation="checksum"}`, uint64(stats.Recovery.ChecksumValidMetaSlots)),
		labeledUint(`{validation="root"}`, uint64(stats.Recovery.RootValidMetaSlots)),
	)
	writePrometheusFamily(&output, "meldbase_recovery_main_tail_bytes_removed", "Provably incomplete main-file tail bytes removed at startup.", "gauge", gaugeUint(stats.Recovery.MainTailBytesRemoved))
	writePrometheusFamily(&output, "meldbase_uptime_seconds", "Database process-session uptime in seconds.", "gauge", gaugeDuration(stats.Uptime))
	writePrometheusFamily(&output, "meldbase_commit_sequence", "Current durable or in-memory commit sequence.", "gauge", gaugeUint(stats.CommitSequence))
	writePrometheusFamily(&output, "meldbase_collections", "Current number of collections.", "gauge", gaugeUint(stats.Collections))
	writePrometheusFamily(&output, "meldbase_documents", "Current number of documents.", "gauge", gaugeUint(stats.Documents))
	writePrometheusFamily(&output, "meldbase_indexes", "Current number of secondary indexes.", "gauge", gaugeUint(stats.Indexes))
	writePrometheusFamily(&output, "meldbase_index_build_active", "Index builds currently active.", "gauge", gaugeUint(stats.IndexBuilds.Active))
	writePrometheusFamily(&output, "meldbase_index_build_persistent", "Durable unfinished index builds.", "gauge", gaugeUint(stats.IndexBuilds.Persistent))
	writePrometheusFamily(&output, "meldbase_index_build_scanning", "Durable index builds scanning their source snapshot.", "gauge", gaugeUint(stats.IndexBuilds.Scanning))
	writePrometheusFamily(&output, "meldbase_index_build_catching_up", "Durable index builds replaying retained commits.", "gauge", gaugeUint(stats.IndexBuilds.CatchingUp))
	writePrometheusFamily(&output, "meldbase_index_build_ready", "Durable index builds ready for atomic publication.", "gauge", gaugeUint(stats.IndexBuilds.Ready))
	writePrometheusFamily(&output, "meldbase_index_build_failed_persistent", "Durable index builds stopped in a terminal failed state.", "gauge", gaugeUint(stats.IndexBuilds.PersistentFailed))
	writePrometheusFamily(&output, "meldbase_index_build_retention_lease_active", "Whether an unfinished durable index build currently pins Commit Log history.", "gauge", gaugeBool(stats.IndexBuilds.RetentionLeaseActive))
	writePrometheusFamily(&output, "meldbase_index_build_retention_pressure", "Whether a durable index-build watermark is preventing Commit Log retention from meeting its configured budget.", "gauge", gaugeBool(stats.IndexBuilds.RetentionPressure))
	writePrometheusFamily(&output, "meldbase_index_build_persistent_entries", "Entries in unfinished durable index shadows.", "gauge", gaugeUint(stats.IndexBuilds.PersistentEntries))
	writePrometheusFamily(&output, "meldbase_index_build_persistent_bytes", "Canonical Secondary bytes in unfinished durable index shadows.", "gauge", gaugeUint(stats.IndexBuilds.PersistentBytes))
	writePrometheusFamily(&output, "meldbase_index_build_scheduler_runs_total", "Background index-build scheduler time quanta started.", "counter", counterUint(stats.IndexBuilds.SchedulerRuns))
	writePrometheusFamily(&output, "meldbase_index_build_scheduler_yields_total", "Background index-build scheduler time quanta yielded at their deadline.", "counter", counterUint(stats.IndexBuilds.SchedulerYields))
	writePrometheusFamily(&output, "meldbase_index_build_scheduler_failures_total", "Background index builds durably failed or encountered an unexpected failure.", "counter", counterUint(stats.IndexBuilds.SchedulerFailures))
	writePrometheusFamily(&output, "meldbase_index_build_attempts_total", "Index builds attempted.", "counter", counterUint(stats.IndexBuilds.Attempts))
	writePrometheusFamily(&output, "meldbase_index_build_completed_total", "Index builds completed.", "counter", counterUint(stats.IndexBuilds.Completed))
	writePrometheusFamily(&output, "meldbase_index_build_failed_total", "Index builds failed.", "counter", counterUint(stats.IndexBuilds.Failed))
	writePrometheusFamily(&output, "meldbase_index_build_retries_total", "Optimistic index-build snapshot retries.", "counter", counterUint(stats.IndexBuilds.Retries))
	writePrometheusFamily(&output, "meldbase_index_build_conflicts_total", "Index-build snapshot conflicts.", "counter", counterUint(stats.IndexBuilds.Conflicts))
	writePrometheusFamily(&output, "meldbase_index_build_last_entries", "Entries admitted by the last index build.", "gauge", gaugeUint(stats.IndexBuilds.LastEntries))
	writePrometheusFamily(&output, "meldbase_index_build_last_bytes", "Canonical Secondary bytes admitted by the last index build.", "gauge", gaugeUint(stats.IndexBuilds.LastBytes))
	writePrometheusFamily(&output, "meldbase_index_build_last_duration_seconds", "Duration of the last index build in seconds.", "gauge", gaugeDuration(stats.IndexBuilds.LastDuration))
	writePrometheusFamily(&output, "meldbase_index_build_max_duration_seconds", "Maximum index-build duration in this process session.", "gauge", gaugeDuration(stats.IndexBuilds.MaxDuration))
	writePrometheusFamily(&output, "meldbase_active_change_watchers", "Current number of change and query subscribers.", "gauge", gaugeUint(stats.ActiveChangeWatchers))

	writePrometheusFamily(&output, "meldbase_commits_total", "Committed database transactions in this process session.", "counter", counterUint(stats.Commits.Total))
	writePrometheusFamily(&output, "meldbase_commit_changes_total", "Logical changes in committed transactions in this process session.", "counter", counterUint(stats.Commits.Changes))
	writePrometheusFamily(&output, "meldbase_commit_coordinator_enabled", "Whether the optional commit coordinator is enabled.", "gauge", gaugeBool(stats.CommitCoordinator.Enabled))
	writePrometheusFamily(&output, "meldbase_commit_coordinator_pending", "Current requests waiting for commit-coordinator admission.", "gauge", gaugeUint(stats.CommitCoordinator.Pending))
	writePrometheusFamily(&output, "meldbase_commit_coordinator_pending_capacity", "Fixed commit-coordinator pending-request capacity.", "gauge", gaugeUint(stats.CommitCoordinator.PendingCapacity))
	writePrometheusFamily(&output, "meldbase_commit_coordinator_admitted_total", "Requests admitted by the commit coordinator.", "counter", counterUint(stats.CommitCoordinator.Admitted))
	writePrometheusFamily(&output, "meldbase_commit_coordinator_admission_rejected_total", "Requests rejected because the commit-coordinator queue was full.", "counter", counterUint(stats.CommitCoordinator.AdmissionRejected))
	writePrometheusFamily(&output, "meldbase_commit_coordinator_batches_total", "Commit-coordinator batches processed.", "counter", counterUint(stats.CommitCoordinator.Batches))
	writePrometheusFamily(&output, "meldbase_commit_coordinator_grouped_transactions_total", "Logical requests processed in multi-member commit-coordinator batches.", "counter", counterUint(stats.CommitCoordinator.GroupedTransactions))
	writePrometheusFamily(&output, "meldbase_commit_coordinator_outcome_unknown_total", "Admitted requests whose caller canceled before its durable outcome was known.", "counter", counterUint(stats.CommitCoordinator.OutcomeUnknown))
	writePrometheusFamily(&output, "meldbase_primary_write_fence_configured", "Whether an external primary-write fence was configured at open.", "gauge", gaugeBool(stats.PrimaryWriteFence.Configured))
	writePrometheusFamily(&output, "meldbase_primary_write_fence_enforced", "Whether the external primary-write fence currently guards local writes.", "gauge", gaugeBool(stats.PrimaryWriteFence.Enforced))
	writePrometheusFamily(&output, "meldbase_primary_write_fence_checks_total", "Primary-write fence checks before logical primary commits.", "counter", counterUint(stats.PrimaryWriteFence.Checks))
	writePrometheusFamily(&output, "meldbase_primary_write_fence_rejections_total", "Primary-write fence checks that rejected a local write.", "counter", counterUint(stats.PrimaryWriteFence.Rejected))
	writePrometheusFamily(&output, "meldbase_write_transaction_active", "Public optimistic write transaction callbacks currently active.", "gauge", gaugeUint(stats.Transactions.Active))
	writePrometheusFamily(&output, "meldbase_write_transaction_started_total", "Public optimistic write transaction callbacks started.", "counter", counterUint(stats.Transactions.Started))
	writePrometheusFamily(&output, "meldbase_write_transaction_outcomes_total", "Public optimistic write transaction callbacks by fixed terminal outcome.", "counter",
		labeledUint(`{outcome="committed"}`, stats.Transactions.Committed),
		labeledUint(`{outcome="noop"}`, stats.Transactions.Noops),
		labeledUint(`{outcome="conflict"}`, stats.Transactions.Conflicts),
		labeledUint(`{outcome="aborted"}`, stats.Transactions.Aborted),
	)
	writePrometheusFamily(&output, "meldbase_resource_limit_rejections_total", "Operations rejected by database resource admission limits.", "counter", counterUint(stats.Resources.Rejections))
	writePrometheusFamily(&output, "meldbase_resource_max_document_bytes", "Configured maximum canonical bytes per document.", "gauge", gaugeUint(stats.Resources.Limits.MaxDocumentBytes))
	writePrometheusFamily(&output, "meldbase_resource_max_transaction_bytes", "Configured maximum canonical document bytes per transaction.", "gauge", gaugeUint(stats.Resources.Limits.MaxTransactionBytes))
	writePrometheusFamily(&output, "meldbase_resource_max_transaction_changes", "Configured maximum logical changes per transaction.", "gauge", gaugeUint(stats.Resources.Limits.MaxTransactionChanges))
	writePrometheusFamily(&output, "meldbase_resource_max_index_build_entries", "Configured maximum entries per index build.", "gauge", gaugeUint(stats.Resources.Limits.MaxIndexBuildEntries))
	writePrometheusFamily(&output, "meldbase_resource_max_index_build_bytes", "Configured maximum canonical Secondary bytes per index build.", "gauge", gaugeUint(stats.Resources.Limits.MaxIndexBuildBytes))

	writePrometheusFamily(&output, "meldbase_queries_total", "Completed public queries in this process session.", "counter", counterUint(stats.Queries.Total))
	writePrometheusFamily(&output, "meldbase_query_failures_total", "Failed public queries in this process session.", "counter", counterUint(stats.Queries.Failed))
	writePrometheusFamily(&output, "meldbase_query_plan_total", "Completed public queries by fixed planner stage.", "counter",
		labeledUint(`{stage="collection_scan"}`, stats.Queries.CollectionScans),
		labeledUint(`{stage="index_scan"}`, stats.Queries.IndexScans),
		labeledUint(`{stage="id_lookup"}`, stats.Queries.IDLookups),
	)
	writePrometheusFamily(&output, "meldbase_query_documents_examined_total", "Documents examined by completed public queries.", "counter", counterUint(stats.Queries.DocumentsExamined))
	writePrometheusFamily(&output, "meldbase_query_documents_returned_total", "Documents returned by completed public queries.", "counter", counterUint(stats.Queries.DocumentsReturned))
	writePrometheusFamily(&output, "meldbase_query_keys_examined_total", "Physical primary or secondary index keys admitted by public query budgets, including failed queries.", "counter", counterUint(stats.Queries.KeysExamined))
	writePrometheusFamily(&output, "meldbase_query_candidate_ids_total", "Index candidate document IDs processed by public query deduplication.", "counter", counterUint(stats.Queries.CandidateIDs))
	writePrometheusFamily(&output, "meldbase_query_unique_candidate_ids_total", "Unique index candidate document IDs processed by public queries.", "counter", counterUint(stats.Queries.UniqueCandidateIDs))
	writePrometheusFamily(&output, "meldbase_query_duplicate_candidate_ids_total", "Duplicate index candidate document IDs discarded by public queries.", "counter", counterUint(stats.Queries.DuplicateCandidateIDs))
	writePrometheusFamily(&output, "meldbase_query_candidates_retained_total", "Final retained query candidates accumulated across public query executions.", "counter", counterUint(stats.Queries.CandidatesRetained))
	writePrometheusFamily(&output, "meldbase_query_sort_bytes_total", "Final retained sort-candidate bytes accumulated across public query executions.", "counter", counterUint(stats.Queries.SortBytes))
	writePrometheusFamily(&output, "meldbase_query_early_stops_total", "Public queries that stopped proven-order scanning after satisfying their result window.", "counter", counterUint(stats.Queries.EarlyStops))
	writePrometheusFamily(&output, "meldbase_query_budget_pressure_events_total", "Public queries whose most-used execution-budget dimension reached at least 80 percent.", "counter", counterUint(stats.Queries.BudgetPressureEvents))
	writePrometheusFamily(&output, "meldbase_query_budget_rejections_total", "Public queries rejected by a query execution budget.", "counter", counterUint(stats.Queries.BudgetRejections))
	writePrometheusFamily(&output, "meldbase_query_active_cursors", "Current number of active lazy query cursors.", "gauge", gaugeUint(stats.Queries.ActiveCursors))

	writePrometheusFamily(&output, "meldbase_realtime_shared_views", "Current number of shared reactive query views.", "gauge", gaugeUint(stats.Realtime.SharedViews))
	writePrometheusFamily(&output, "meldbase_realtime_query_subscribers", "Current number of reactive query subscribers.", "gauge", gaugeUint(stats.Realtime.QuerySubscribers))
	writePrometheusFamily(&output, "meldbase_realtime_pending_batches", "Current number of pending reactive commit batches.", "gauge", gaugeUint(stats.Realtime.PendingBatches))
	writePrometheusFamily(&output, "meldbase_realtime_pending_changes", "Current number of pending reactive logical changes.", "gauge", gaugeUint(stats.Realtime.PendingChanges))
	writePrometheusFamily(&output, "meldbase_realtime_pending_bytes", "Current canonical document-image bytes pending in the reactive hub.", "gauge", gaugeUint(stats.Realtime.PendingBytes))
	writePrometheusFamily(&output, "meldbase_realtime_pending_batch_capacity", "Fixed reactive pending-batch capacity.", "gauge", gaugeUint(stats.Realtime.PendingBatchCapacity))
	writePrometheusFamily(&output, "meldbase_realtime_pending_change_capacity", "Fixed reactive pending-change capacity.", "gauge", gaugeUint(stats.Realtime.PendingChangeCapacity))
	writePrometheusFamily(&output, "meldbase_realtime_pending_byte_capacity", "Fixed reactive canonical document-image byte capacity.", "gauge", gaugeUint(stats.Realtime.PendingByteCapacity))
	writePrometheusFamily(&output, "meldbase_realtime_watcher_pending_bytes", "Current canonical document-image bytes pending across direct Go change watchers.", "gauge", gaugeUint(stats.Realtime.WatcherPendingBytes))
	writePrometheusFamily(&output, "meldbase_realtime_watcher_byte_capacity", "Fixed aggregate canonical document-image byte capacity across direct Go change watchers.", "gauge", gaugeUint(stats.Realtime.WatcherByteCapacity))
	writePrometheusFamily(&output, "meldbase_realtime_dispatch_pending_batches", "Current batches waiting in the central change dispatcher.", "gauge", gaugeUint(stats.Realtime.DispatchPendingBatches))
	writePrometheusFamily(&output, "meldbase_realtime_dispatch_pending_changes", "Current logical changes waiting in the central change dispatcher.", "gauge", gaugeUint(stats.Realtime.DispatchPendingChanges))
	writePrometheusFamily(&output, "meldbase_realtime_dispatch_pending_bytes", "Current canonical document-image bytes waiting in the central change dispatcher.", "gauge", gaugeUint(stats.Realtime.DispatchPendingBytes))
	writePrometheusFamily(&output, "meldbase_realtime_dispatch_batch_capacity", "Fixed central change-dispatcher batch capacity.", "gauge", gaugeUint(stats.Realtime.DispatchBatchCapacity))
	writePrometheusFamily(&output, "meldbase_realtime_dispatch_change_capacity", "Fixed central change-dispatcher change capacity.", "gauge", gaugeUint(stats.Realtime.DispatchChangeCapacity))
	writePrometheusFamily(&output, "meldbase_realtime_dispatch_byte_capacity", "Fixed central change-dispatcher canonical document-image byte capacity.", "gauge", gaugeUint(stats.Realtime.DispatchByteCapacity))
	writePrometheusFamily(&output, "meldbase_realtime_queue_overflows_total", "Reactive queue overflow fallbacks in this process session.", "counter", counterUint(stats.Realtime.QueueOverflows))
	writePrometheusFamily(&output, "meldbase_realtime_slow_consumers_total", "Slow business-data consumers disconnected in this process session.", "counter", counterUint(stats.Realtime.SlowConsumers))
	writePrometheusFamily(&output, "meldbase_realtime_incremental_batches_total", "Commit batches applied incrementally to reactive views.", "counter", counterUint(stats.Realtime.IncrementalBatches))
	writePrometheusFamily(&output, "meldbase_realtime_full_recomputes_total", "Full reactive view recomputations in this process session.", "counter", counterUint(stats.Realtime.FullViewRecomputes))
	writePrometheusFamily(&output, "meldbase_realtime_delta_deliveries_total", "Reactive delta deliveries in this process session.", "counter", counterUint(stats.Realtime.DeltaDeliveries))

	if server := sample.Server; server != nil {
		writePrometheusFamily(&output, "meldbase_server_active_connections", "Current authenticated realtime WebSocket connections.", "gauge", gaugeUint(server.ActiveConnections))
		writePrometheusFamily(&output, "meldbase_server_connections_accepted_total", "Authenticated realtime WebSocket connections accepted in this server session.", "counter", counterUint(server.ConnectionsAccepted))
		writePrometheusFamily(&output, "meldbase_realtime_outbound_queue_overflows_total", "Realtime connections closed because their outbound frame or byte budget was exceeded.", "counter", counterUint(server.RealtimeOutboundOverflows))
		writePrometheusFamily(&output, "meldbase_rpc_requests_total", "Valid HTTP and WebSocket RPC call envelopes received.", "counter", counterUint(server.RPCRequests))
		writePrometheusFamily(&output, "meldbase_rpc_active", "Current executing RPC method handlers.", "gauge", gaugeUint(server.RPCActive))
		writePrometheusFamily(&output, "meldbase_rpc_succeeded_total", "RPC method executions completed successfully.", "counter", counterUint(server.RPCSucceeded))
		writePrometheusFamily(&output, "meldbase_rpc_failed_total", "RPC method executions that returned an application/internal failure.", "counter", counterUint(server.RPCFailed))
		writePrometheusFamily(&output, "meldbase_rpc_canceled_total", "RPC method executions canceled by their transport context.", "counter", counterUint(server.RPCCanceled))
		writePrometheusFamily(&output, "meldbase_rpc_rejected_total", "RPC calls rejected before method execution.", "counter", counterUint(server.RPCRejected))
		writePrometheusFamily(&output, "meldbase_rpc_busy_total", "RPC calls rejected by global or per-connection concurrency budgets.", "counter", counterUint(server.RPCBusy))
		writePrometheusFamily(&output, "meldbase_rpc_request_bytes_total", "RPC request envelope bytes passed to executing handlers.", "counter", counterUint(server.RPCRequestBytes))
		writePrometheusFamily(&output, "meldbase_rpc_result_bytes_total", "Typed RPC result bytes returned by successful handlers.", "counter", counterUint(server.RPCResultBytes))
		writePrometheusFamily(&output, "meldbase_rpc_duration_seconds_total", "Accumulated RPC method execution duration.", "counter", counterNanos(server.RPCTotalNanos))
		writePrometheusFamily(&output, "meldbase_rpc_max_duration_seconds", "Maximum RPC method execution duration in this server session.", "gauge", gaugeDuration(server.RPCMaxLatency))
		writePrometheusFamily(&output, "meldbase_rpc_idempotency_claims_total", "Durable RPC idempotency claims that received execution ownership.", "counter", counterUint(server.RPCIdempotencyClaims))
		writePrometheusFamily(&output, "meldbase_rpc_idempotency_replays_total", "RPC terminal responses served from durable idempotency records.", "counter", counterUint(server.RPCIdempotencyReplays))
		writePrometheusFamily(&output, "meldbase_rpc_idempotency_conflicts_total", "RPC idempotency keys reused with another request fingerprint.", "counter", counterUint(server.RPCIdempotencyConflicts))
		writePrometheusFamily(&output, "meldbase_rpc_idempotency_in_progress_total", "Duplicate RPC calls rejected while the owning execution is active.", "counter", counterUint(server.RPCIdempotencyInProgress))
		writePrometheusFamily(&output, "meldbase_rpc_idempotency_unknown_total", "RPC calls whose durable outcome cannot be proven.", "counter", counterUint(server.RPCIdempotencyUnknown))
		writePrometheusFamily(&output, "meldbase_rpc_idempotency_failures_total", "RPC idempotency storage or validation failures.", "counter", counterUint(server.RPCIdempotencyFailures))
		writePrometheusFamily(&output, "meldbase_rpc_atomic_commits_total", "Transactional RPC business/result commits published atomically.", "counter", counterUint(server.RPCAtomicCommits))
		writePrometheusFamily(&output, "meldbase_rpc_atomic_rollbacks_total", "Transactional RPC handler executions rolled back before publication.", "counter", counterUint(server.RPCAtomicRollbacks))
		writePrometheusFamily(&output, "meldbase_rpc_atomic_noop_completions_total", "Successful transactional RPC calls with no business mutation.", "counter", counterUint(server.RPCAtomicNoopCompletions))
		writePrometheusFamily(&output, "meldbase_worker_connected", "Current authenticated trusted Workers.", "gauge", gaugeUint(server.Worker.ConnectedWorkers))
		writePrometheusFamily(&output, "meldbase_worker_registered_methods", "Current dynamically registered worker methods.", "gauge", gaugeUint(server.Worker.RegisteredMethods))
		writePrometheusFamily(&output, "meldbase_worker_registered_read_policies", "Current dynamically registered worker read policies.", "gauge", gaugeUint(server.Worker.RegisteredReadPolicies))
		writePrometheusFamily(&output, "meldbase_worker_calls_total", "Worker method calls started in this process session.", "counter", counterUint(server.Worker.CallsStarted))
		writePrometheusFamily(&output, "meldbase_worker_calls_active", "Current in-flight worker method calls.", "gauge", gaugeUint(server.Worker.CallsActive))
		writePrometheusFamily(&output, "meldbase_worker_calls_busy_total", "Worker calls rejected by per-worker pending budgets.", "counter", counterUint(server.Worker.CallsBusy))
		writePrometheusFamily(&output, "meldbase_worker_protocol_failures_total", "Worker control sessions closed for protocol failures.", "counter", counterUint(server.Worker.ProtocolFailures))
		writePrometheusFamily(&output, "meldbase_worker_transaction_operations_total", "Transactional point operations executed for workers.", "counter", counterUint(server.Worker.TransactionOps))
		writePrometheusFamily(&output, "meldbase_worker_policy_evaluations_total", "Worker read-policy evaluations started.", "counter", counterUint(server.Worker.PolicyEvaluations))
		writePrometheusFamily(&output, "meldbase_worker_policy_evaluations_active", "Current in-flight worker read-policy evaluations.", "gauge", gaugeUint(server.Worker.PolicyActive))
		writePrometheusFamily(&output, "meldbase_worker_policy_denied_total", "Worker read-policy evaluations denied by application logic.", "counter", counterUint(server.Worker.PolicyDenied))
		writePrometheusFamily(&output, "meldbase_worker_policy_failures_total", "Worker read-policy evaluations that failed internally.", "counter", counterUint(server.Worker.PolicyFailed))
		writePrometheusFamily(&output, "meldbase_worker_policy_busy_total", "Worker read-policy evaluations rejected by pending budgets.", "counter", counterUint(server.Worker.PolicyBusy))
		writePrometheusFamily(&output, "meldbase_worker_policy_invalidations_total", "Durably committed worker read-policy generation changes.", "counter", counterUint(server.Worker.PolicyInvalidations))
	}

	writePrometheusFamily(&output, "meldbase_storage_page_size_bytes", "Storage page size in bytes.", "gauge", gaugeUint(stats.Storage.PageSize))
	writePrometheusFamily(&output, "meldbase_storage_generation", "Current physical publication generation.", "gauge", gaugeUint(stats.Storage.Generation))
	writePrometheusFamily(&output, "meldbase_storage_rollback_protected", "Whether acknowledged commits are gated by an external rollback anchor.", "gauge", gaugeBool(stats.Storage.RollbackProtected))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_sequence", "Last rollback-anchor sequence durably read back in this process.", "gauge", gaugeUint(stats.Storage.RollbackAnchorSequence))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_generation", "Last rollback-anchor generation durably read back in this process.", "gauge", gaugeUint(stats.Storage.RollbackAnchorGeneration))
	rollbackLag := uint64(0)
	if stats.Storage.RollbackProtected && stats.Storage.CommitSequence > stats.Storage.RollbackAnchorSequence {
		rollbackLag = stats.Storage.CommitSequence - stats.Storage.RollbackAnchorSequence
	}
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_lag", "Logical commits by which the database is ahead of its rollback anchor.", "gauge", gaugeUint(rollbackLag))
	rollbackGenerationLag := uint64(0)
	if stats.Storage.RollbackProtected && stats.Storage.Generation > stats.Storage.RollbackAnchorGeneration {
		rollbackGenerationLag = stats.Storage.Generation - stats.Storage.RollbackAnchorGeneration
	}
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_generation_lag", "Physical generations by which the database is ahead of its rollback anchor.", "gauge", gaugeUint(rollbackGenerationLag))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_failures_total", "Rollback-anchor durable save or read-back failures.", "counter", counterUint(stats.Storage.RollbackAnchorFailures))
	anchorStore := stats.Storage.RollbackAnchorStore
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_replicas", "Configured rollback-anchor replicas.", "gauge", gaugeUint(anchorStore.Replicas))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_quorum", "Rollback-anchor replicas required for a majority.", "gauge", gaugeUint(anchorStore.Quorum))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_store_loads_total", "Rollback-anchor store load operations.", "counter", counterUint(anchorStore.Loads))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_store_advances_total", "Rollback-anchor store advance operations.", "counter", counterUint(anchorStore.Advances))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_endpoint_failures_total", "Rollback-anchor endpoint failures observed before an operation completed.", "counter", counterUint(anchorStore.EndpointFailures))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_quorum_failures_total", "Rollback-anchor operations that failed to reach quorum.", "counter", counterUint(anchorStore.QuorumFailures))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_conflicts_total", "Rollback-anchor incomparable-history or identity conflicts.", "counter", counterUint(anchorStore.Conflicts))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_authentication_failures_total", "Rollback-anchor authentication failures.", "counter", counterUint(anchorStore.AuthenticationFailures))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_protocol_failures_total", "Rollback-anchor protocol validation failures.", "counter", counterUint(anchorStore.ProtocolFailures))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_configuration_failures_total", "Rollback-anchor static configuration or member identity failures.", "counter", counterUint(anchorStore.ConfigurationFailures))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_timeout_seconds", "Configured deadline for each rollback-anchor interaction.", "gauge", gaugeDuration(stats.Storage.RollbackAnchorTimeout))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_duration_seconds_total", "Accumulated synchronous rollback-anchor update duration.", "counter", counterNanos(stats.Storage.RollbackAnchorNanos))
	writePrometheusFamily(&output, "meldbase_storage_rollback_anchor_max_duration_seconds", "Maximum synchronous rollback-anchor update duration.", "gauge", gaugeDuration(stats.Storage.RollbackAnchorMaxLatency))
	writePrometheusFamily(&output, "meldbase_storage_physical_pages", "Current physical page high-water count.", "gauge", gaugeUint(stats.Storage.PhysicalPages))
	writePrometheusFamily(&output, "meldbase_storage_used_bytes", "Current physical file high-water bytes.", "gauge", gaugeUint(stats.Storage.StorageUsedBytes))
	writePrometheusFamily(&output, "meldbase_storage_max_bytes", "Configured physical file high-water quota.", "gauge", gaugeUint(stats.Storage.StorageMaxBytes))
	writePrometheusFamily(&output, "meldbase_storage_byte_overage", "Existing physical bytes above the configured quota.", "gauge", gaugeUint(stats.Storage.StorageByteOverage))
	writePrometheusFamily(&output, "meldbase_storage_quota_exhausted", "Whether storage has neither append capacity nor reusable pages.", "gauge", gaugeBool(stats.Storage.StorageQuotaExhausted))
	writePrometheusFamily(&output, "meldbase_storage_limit_rejections_total", "Transactions rejected before I/O by the physical storage quota.", "counter", counterUint(stats.Storage.StorageLimitRejections))
	writePrometheusFamily(&output, "meldbase_storage_reusable_pages", "Current process-local reusable page count.", "gauge", gaugeUint(stats.Storage.ReusablePages))
	writePrometheusFamily(&output, "meldbase_storage_tree_splits_total", "B+Tree node splits published in this process session.", "counter", counterUint(stats.Storage.TreeSplits))
	writePrometheusFamily(&output, "meldbase_storage_tree_merges_total", "B+Tree sibling merges published in this process session.", "counter", counterUint(stats.Storage.TreeMerges))
	writePrometheusFamily(&output, "meldbase_storage_persistent_free_space", "Whether a validated persistent free-space snapshot is active.", "gauge", gaugeBool(stats.Storage.PersistentFreeSpace))
	writePrometheusFamily(&output, "meldbase_storage_free_space_loads_total", "Persistent free-space snapshot load attempts in this process session.", "counter", counterUint(stats.Storage.FreeSpaceLoads))
	writePrometheusFamily(&output, "meldbase_storage_free_space_load_failures_total", "Invalid optional free-space snapshots discarded in this process session.", "counter", counterUint(stats.Storage.FreeSpaceLoadFailures))
	writePrometheusFamily(&output, "meldbase_storage_free_space_publications_total", "Physical free-space maintenance generations published in this process session.", "counter", counterUint(stats.Storage.FreeSpacePublishes))
	writePrometheusFamily(&output, "meldbase_storage_free_space_candidate_checks_total", "Candidate page headers checked while restoring persistent free space.", "counter", counterUint(stats.Storage.FreeSpaceCandidateChecks))
	writePrometheusFamily(&output, "meldbase_storage_oldest_retained_sequence", "Oldest currently retained replay sequence.", "gauge", gaugeUint(stats.Storage.OldestRetainedSequence))
	writePrometheusFamily(&output, "meldbase_storage_retained_commits", "Current logical commits retained for replay.", "gauge", gaugeUint(stats.Storage.RetainedCommits))
	writePrometheusFamily(&output, "meldbase_storage_commit_retention_max", "Configured normal Commit Log window.", "gauge", gaugeUint(stats.Storage.CommitRetentionMax))
	writePrometheusFamily(&output, "meldbase_storage_commit_retention_overage", "Retained commits above the configured window.", "gauge", gaugeUint(stats.Storage.CommitRetentionOverage))
	writePrometheusFamily(&output, "meldbase_storage_retained_commit_bytes", "Canonical logical bytes currently retained in the Commit Log.", "gauge", gaugeUint(stats.Storage.RetainedCommitBytes))
	writePrometheusFamily(&output, "meldbase_storage_commit_retention_max_bytes", "Configured normal Commit Log logical-byte budget.", "gauge", gaugeUint(stats.Storage.CommitRetentionMaxBytes))
	writePrometheusFamily(&output, "meldbase_storage_commit_retention_byte_overage", "Retained logical bytes above the configured budget.", "gauge", gaugeUint(stats.Storage.CommitRetentionByteOverage))
	writePrometheusFamily(&output, "meldbase_storage_commit_retention_pressure", "Whether the configured count or byte retention budget is currently unsatisfied.", "gauge", gaugeBool(stats.Storage.RetentionPressure))
	writePrometheusFamily(&output, "meldbase_storage_retention_pruned_commits_total", "Commit Log entries pruned by successful publications in this session.", "counter", counterUint(stats.Storage.RetentionPrunedCommits))
	writePrometheusFamily(&output, "meldbase_storage_retention_pressure_events_total", "Successful commits whose desired retention watermark was blocked by replay pins.", "counter", counterUint(stats.Storage.RetentionPressureEvents))
	writePrometheusFamily(&output, "meldbase_storage_active_readers", "Current number of pinned storage readers.", "gauge", gaugeUint(stats.Storage.ActiveReaders))
	writePrometheusFamily(&output, "meldbase_storage_active_replay_leases", "Current number of pinned replay leases.", "gauge", gaugeUint(stats.Storage.ActiveReplayLeases))
	writePrometheusFamily(&output, "meldbase_storage_commit_attempts_total", "Storage commit attempts in this process session.", "counter", counterUint(stats.Storage.CommitAttempts))
	writePrometheusFamily(&output, "meldbase_storage_transactions_total", "Storage transactions by fixed outcome in this process session.", "counter",
		labeledUint(`{outcome="committed"}`, stats.Storage.CommittedTransactions),
		labeledUint(`{outcome="rejected"}`, stats.Storage.RejectedTransactions),
	)
	writePrometheusFamily(&output, "meldbase_storage_commit_duration_seconds_total", "Accumulated storage commit duration in seconds.", "counter", counterNanos(stats.Storage.CommitNanos))
	writePrometheusFamily(&output, "meldbase_storage_commit_max_duration_seconds", "Maximum storage commit duration observed in this process session.", "gauge", gaugeDuration(stats.Storage.CommitMaxLatency))

	writePrometheusCache(&output, "page", "capacity_pages", "resident_pages", stats.Storage.PageCache.CapacityPages, stats.Storage.PageCache.ResidentPages, stats.Storage.PageCache.Hits, stats.Storage.PageCache.Misses, stats.Storage.PageCache.Evictions)
	writePrometheusCache(&output, "document", "capacity_entries", "entries", stats.Storage.DocumentCache.CapacityEntries, stats.Storage.DocumentCache.Entries, stats.Storage.DocumentCache.Hits, stats.Storage.DocumentCache.Misses, stats.Storage.DocumentCache.Evictions)
	writePrometheusFamily(&output, "meldbase_document_cache_capacity_bytes", "Configured decoded-document cache capacity in bytes.", "gauge", gaugeUint(stats.Storage.DocumentCache.CapacityBytes))
	writePrometheusFamily(&output, "meldbase_document_cache_bytes", "Current conservative decoded-document cache size in bytes.", "gauge", gaugeUint(stats.Storage.DocumentCache.Bytes))

	writePrometheusFamily(&output, "meldbase_compaction_active", "Current number of active logical compactions.", "gauge", gaugeUint(stats.Compaction.Active))
	writePrometheusFamily(&output, "meldbase_compaction_attempts_total", "Logical compaction attempts in this process session.", "counter", counterUint(stats.Compaction.Attempts))
	writePrometheusFamily(&output, "meldbase_compaction_completed_total", "Completed logical compactions in this process session.", "counter", counterUint(stats.Compaction.Completed))
	writePrometheusFamily(&output, "meldbase_compaction_failures_total", "Failed logical compactions in this process session.", "counter", counterUint(stats.Compaction.Failed))
	writePrometheusFamily(&output, "meldbase_compaction_last_input_bytes", "Input bytes observed by the last compaction attempt.", "gauge", gaugeUint(stats.Compaction.InputBytes))
	writePrometheusFamily(&output, "meldbase_compaction_last_output_bytes", "Output bytes produced by the last completed compaction.", "gauge", gaugeUint(stats.Compaction.OutputBytes))
	writePrometheusFamily(&output, "meldbase_compaction_last_duration_seconds", "Duration of the last compaction attempt in seconds.", "gauge", gaugeDuration(stats.Compaction.LastDuration))

	writePrometheusFamily(&output, "meldbase_reclamation_active", "Current number of active page-reclamation scans.", "gauge", gaugeUint(stats.Reclamation.Active))
	writePrometheusFamily(&output, "meldbase_reclamation_attempts_total", "Page-reclamation attempts in this process session.", "counter", counterUint(stats.Reclamation.Attempts))
	writePrometheusFamily(&output, "meldbase_reclamation_scans_total", "Complete page-graph scans, including optimistic retries.", "counter", counterUint(stats.Reclamation.Scans))
	writePrometheusFamily(&output, "meldbase_reclamation_conflicts_total", "Online reclamations discarded after exhausting commit-conflict retries.", "counter", counterUint(stats.Reclamation.Conflicts))
	writePrometheusFamily(&output, "meldbase_reclamation_completed_total", "Completed page-reclamation scans in this process session.", "counter", counterUint(stats.Reclamation.Completed))
	writePrometheusFamily(&output, "meldbase_reclamation_failures_total", "Failed page-reclamation scans in this process session.", "counter", counterUint(stats.Reclamation.Failed))
	writePrometheusFamily(&output, "meldbase_reclamation_last_reachable_pages", "Reachable pages found by the last completed reclamation scan.", "gauge", gaugeUint(stats.Reclamation.LastReachable))
	writePrometheusFamily(&output, "meldbase_reclamation_last_reclaimable_pages", "Reclaimable pages found by the last completed reclamation scan.", "gauge", gaugeUint(stats.Reclamation.LastReclaimable))
	writePrometheusFamily(&output, "meldbase_reclamation_last_duration_seconds", "Duration of the last reclamation attempt in seconds.", "gauge", gaugeDuration(stats.Reclamation.LastDuration))
	writePrometheusFamily(&output, "meldbase_reclamation_last_attempts", "Graph scans used by the last reclamation operation.", "gauge", gaugeUint(stats.Reclamation.LastAttempts))
	writePrometheusFamily(&output, "meldbase_reclamation_last_online", "Whether the last reclamation used optimistic online mode.", "gauge", gaugeBool(stats.Reclamation.LastOnline))

	writePrometheusFamily(&output, "meldbase_backup_active", "Current number of active physical backups.", "gauge", gaugeUint(stats.Backup.Active))
	writePrometheusFamily(&output, "meldbase_backup_attempts_total", "Physical backup attempts in this process session.", "counter", counterUint(stats.Backup.Attempts))
	writePrometheusFamily(&output, "meldbase_backup_completed_total", "Completed physical backups in this process session.", "counter", counterUint(stats.Backup.Completed))
	writePrometheusFamily(&output, "meldbase_backup_failures_total", "Failed physical backups in this process session.", "counter", counterUint(stats.Backup.Failed))
	writePrometheusFamily(&output, "meldbase_backup_last_bytes", "Bytes copied by the last physical backup attempt.", "gauge", gaugeUint(stats.Backup.LastBytes))
	writePrometheusFamily(&output, "meldbase_backup_last_duration_seconds", "Duration of the last physical backup attempt in seconds.", "gauge", gaugeDuration(stats.Backup.LastDuration))

	writePrometheusFamily(&output, "meldbase_diagnostics_enabled", "Whether a bounded diagnostic session is currently enabled.", "gauge", gaugeBool(stats.Diagnostics.Enabled))
	writePrometheusFamily(&output, "meldbase_diagnostics_capacity", "Configured diagnostic event-ring capacity.", "gauge", gaugeUint(stats.Diagnostics.Capacity))
	writePrometheusFamily(&output, "meldbase_diagnostics_retained", "Current number of retained diagnostic events.", "gauge", gaugeUint(stats.Diagnostics.Retained))
	writePrometheusFamily(&output, "meldbase_diagnostics_recorded_total", "Diagnostic events recorded by the current diagnostic session.", "counter", counterUint(stats.Diagnostics.Recorded))
	writePrometheusFamily(&output, "meldbase_diagnostics_overwritten_total", "Diagnostic events overwritten by the current diagnostic session.", "counter", counterUint(stats.Diagnostics.Overwritten))
	writePrometheusFamily(&output, "meldbase_diagnostics_operations_observed_total", "Operations timed by the current diagnostic session by fixed kind.", "counter",
		labeledUint(`{kind="query"}`, stats.Diagnostics.QueriesObserved),
		labeledUint(`{kind="commit"}`, stats.Diagnostics.CommitsObserved),
	)

	writePrometheusFamily(&output, "meldbase_admin_samples_total", "Admin samples captured by this sampler.", "counter", counterUint(sample.Sampler.Samples))
	writePrometheusFamily(&output, "meldbase_admin_subscribers", "Current number of admin sampler subscribers.", "gauge", gaugeUint(sample.Sampler.Subscribers))
	writePrometheusFamily(&output, "meldbase_admin_dropped_deliveries_total", "Admin samples overwritten for slow consumers.", "counter", counterUint(sample.Sampler.DroppedDeliveries))
	writePrometheusFamily(&output, "meldbase_admin_history_samples", "Current number of retained admin history samples.", "gauge", gaugeUint(sample.Sampler.HistorySamples))
	return output.Bytes()
}

func healthPrometheusSamples(health HealthStatus) []prometheusSample {
	components := []struct {
		name  string
		level HealthLevel
	}{
		{"overall", health.Overall}, {"database", health.Database}, {"durability", health.Durability},
		{"storage", health.Storage}, {"realtime", health.Realtime}, {"telemetry", health.Telemetry},
		{"transport", health.Transport},
	}
	result := make([]prometheusSample, 0, len(components))
	for _, component := range components {
		if severity, available := component.level.Severity(); available {
			result = append(result, labeledUint(`{component="`+component.name+`"}`, severity))
		}
	}
	return result
}

func writePrometheusCache(output *bytes.Buffer, kind, capacitySuffix, residentSuffix string, capacity, resident, hits, misses, evictions uint64) {
	prefix := "meldbase_" + kind + "_cache_"
	writePrometheusFamily(output, prefix+capacitySuffix, "Configured "+kind+" cache capacity.", "gauge", gaugeUint(capacity))
	writePrometheusFamily(output, prefix+residentSuffix, "Current resident "+kind+" cache size.", "gauge", gaugeUint(resident))
	writePrometheusFamily(output, prefix+"hits_total", "Successful "+kind+" cache lookups.", "counter", counterUint(hits))
	writePrometheusFamily(output, prefix+"misses_total", "Unsuccessful "+kind+" cache lookups.", "counter", counterUint(misses))
	writePrometheusFamily(output, prefix+"evictions_total", kindTitle(kind)+" cache evictions.", "counter", counterUint(evictions))
}

func writePrometheusFamily(output *bytes.Buffer, name, help, metricType string, samples ...prometheusSample) {
	output.WriteString("# HELP ")
	output.WriteString(name)
	output.WriteByte(' ')
	output.WriteString(help)
	output.WriteByte('\n')
	output.WriteString("# TYPE ")
	output.WriteString(name)
	output.WriteByte(' ')
	output.WriteString(metricType)
	output.WriteByte('\n')
	for _, sample := range samples {
		output.WriteString(name)
		output.WriteString(sample.labels)
		output.WriteByte(' ')
		if sample.numeric {
			encoded := strconv.AppendUint(output.AvailableBuffer(), sample.number, 10)
			_, _ = output.Write(encoded)
		} else {
			output.WriteString(sample.value)
		}
		output.WriteByte('\n')
	}
}

func counterUint(value uint64) prometheusSample {
	return prometheusSample{number: value, numeric: true}
}
func gaugeUint(value uint64) prometheusSample { return prometheusSample{number: value, numeric: true} }

func labeledUint(labels string, value uint64) prometheusSample {
	return prometheusSample{labels: labels, number: value, numeric: true}
}

func gaugeBool(value bool) prometheusSample {
	if value {
		return prometheusSample{number: 1, numeric: true}
	}
	return prometheusSample{numeric: true}
}

func counterNanos(value uint64) prometheusSample {
	return prometheusSample{value: strconv.FormatFloat(float64(value)/float64(time.Second), 'g', -1, 64)}
}

func gaugeDuration(value time.Duration) prometheusSample {
	return prometheusSample{value: formatSeconds(value)}
}

func formatSeconds(value time.Duration) string {
	return strconv.FormatFloat(float64(value)/float64(time.Second), 'g', -1, 64)
}

func safeEngine(stats meldbase.DBStats) string {
	switch stats.Storage.Engine {
	case "current":
		return "current"
	case "memory":
		return "memory"
	default:
		if stats.Durable {
			return "current"
		}
		return "memory"
	}
}

func kindTitle(kind string) string {
	if kind == "page" {
		return "Page"
	}
	return "Document"
}
