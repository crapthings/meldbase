package meldbase

import (
	"sync"
	"sync/atomic"
	"time"
)

// DBStats is a point-in-time, allocation-bounded view of database health.
// Counters are process-lifetime values and reset when the database is reopened.
// Persistent state such as CommitSequence is read from the database itself.
//
// Stats deliberately exposes no user values, document IDs, query parameters, or
// callbacks. It is safe for an admin sampler to call periodically, but it is not
// intended to be called on every database operation.
type DBStats struct {
	CapturedAt           time.Time     `json:"capturedAt"`
	StartedAt            time.Time     `json:"startedAt"`
	Uptime               time.Duration `json:"uptimeNanos"`
	Closed               bool          `json:"closed"`
	WritesDisabled       bool          `json:"writesDisabled"`
	Durable              bool          `json:"durable"`
	CommitSequence       uint64        `json:"commitSequence"`
	Collections          uint64        `json:"collections"`
	Documents            uint64        `json:"documents"`
	Indexes              uint64        `json:"indexes"`
	ActiveChangeWatchers uint64        `json:"activeChangeWatchers"`

	Commits           CommitStats            `json:"commits"`
	Transactions      WriteTransactionStats  `json:"writeTransactions"`
	Queries           QueryStats             `json:"queries"`
	Realtime          RealtimeStats          `json:"realtime"`
	CommitCoordinator CommitCoordinatorStats `json:"commitCoordinator"`
	PrimaryWriteFence PrimaryWriteFenceStats `json:"primaryWriteFence"`
	Durability        DurabilityStats        `json:"durability"`
	Storage           StorageStats           `json:"storage"`
	Compaction        CompactionStats        `json:"compaction"`
	Reclamation       ReclamationStats       `json:"reclamation"`
	Backup            BackupStats            `json:"backup"`
	Diagnostics       DiagnosticStats        `json:"diagnostics"`
	Recovery          RecoveryReport         `json:"recovery"`
	Resources         ResourceStats          `json:"resources"`
	IndexBuilds       IndexBuildStats        `json:"indexBuilds"`
}

type ResourceStats struct {
	Limits     ResourceLimits `json:"limits"`
	Rejections uint64         `json:"rejections"`
}

type IndexBuildStats struct {
	Active               uint64        `json:"active"`
	Persistent           uint64        `json:"persistent"`
	Scanning             uint64        `json:"scanning"`
	CatchingUp           uint64        `json:"catchingUp"`
	Ready                uint64        `json:"ready"`
	PersistentFailed     uint64        `json:"persistentFailed"`
	RetentionLeaseActive bool          `json:"retentionLeaseActive"`
	RetentionPressure    bool          `json:"retentionPressure"`
	PersistentEntries    uint64        `json:"persistentEntries"`
	PersistentBytes      uint64        `json:"persistentBytes"`
	SchedulerRuns        uint64        `json:"schedulerRuns"`
	SchedulerYields      uint64        `json:"schedulerYields"`
	SchedulerFailures    uint64        `json:"schedulerFailures"`
	Attempts             uint64        `json:"attempts"`
	Completed            uint64        `json:"completed"`
	Failed               uint64        `json:"failed"`
	Retries              uint64        `json:"retries"`
	Conflicts            uint64        `json:"conflicts"`
	LastEntries          uint64        `json:"lastEntries"`
	LastBytes            uint64        `json:"lastBytes"`
	LastDuration         time.Duration `json:"lastDurationNanos"`
	MaxDuration          time.Duration `json:"maxDurationNanos"`

	// oldestActiveAppliedSequence is copied from the immutable persistent
	// aggregate. It is deliberately not exported: callers need the bounded
	// lease state, not an index-build-specific replay watermark.
	oldestActiveAppliedSequence uint64
}

type CommitStats struct {
	Total   uint64 `json:"total"`
	Changes uint64 `json:"changes"`
}

// DurabilityStats is retained in the admin wire contract. Current-format
// databases do not use a WAL or checkpoints, so every field is zero.
type DurabilityStats struct {
	WALAppends           uint64        `json:"walAppends"`
	WALPayloadBytes      uint64        `json:"walPayloadBytes"`
	WALCurrentBytes      uint64        `json:"walCurrentBytes"`
	WALCurrentCommits    uint64        `json:"walCurrentCommits"`
	WALAppendFailures    uint64        `json:"walAppendFailures"`
	WALAppendNanos       uint64        `json:"walAppendNanos"`
	WALAppendMaxLatency  time.Duration `json:"walAppendMaxLatencyNanos"`
	CheckpointAttempts   uint64        `json:"checkpointAttempts"`
	CheckpointsCompleted uint64        `json:"checkpointsCompleted"`
	CheckpointFailures   uint64        `json:"checkpointFailures"`
	AutomaticCheckpoints uint64        `json:"automaticCheckpoints"`
	CheckpointNanos      uint64        `json:"checkpointNanos"`
	CheckpointMaxLatency time.Duration `json:"checkpointMaxLatencyNanos"`
}

// CommitCoordinatorStats is a fixed-cardinality snapshot of the optional
//
//	write-admission scheduler. It is included in DBStats and the versioned
//
// admin schema, so applications can alert on admission pressure without
// inspecting a mutable queue or adding application labels.
type CommitCoordinatorStats struct {
	Enabled             bool   `json:"enabled"`
	Pending             uint64 `json:"pending"`
	PendingCapacity     uint64 `json:"pendingCapacity"`
	Admitted            uint64 `json:"admitted"`
	AdmissionRejected   uint64 `json:"admissionRejected"`
	Batches             uint64 `json:"batches"`
	GroupedTransactions uint64 `json:"groupedTransactions"`
	OutcomeUnknown      uint64 `json:"outcomeUnknown"`
}

// PrimaryWriteFenceStats is a fixed-cardinality view of the optional
// external primary-write guard. Configured means a guard was supplied at open;
// Enforced is false while a read-only follower applies validated source
// history. Checks and Rejected count only actual primary write admissions.
// No lease, epoch, endpoint, database ID, or controller detail is exposed.
type PrimaryWriteFenceStats struct {
	Configured bool   `json:"configured"`
	Enforced   bool   `json:"enforced"`
	Checks     uint64 `json:"checks"`
	Rejected   uint64 `json:"rejected"`
}

// WriteTransactionStats describes public optimistic point transactions. Every
// started callback reaches exactly one terminal counter. These aggregates do
// not contain collection, document, actor, or callback identifiers.
type WriteTransactionStats struct {
	Active    uint64 `json:"active"`
	Started   uint64 `json:"started"`
	Committed uint64 `json:"committed"`
	Noops     uint64 `json:"noops"`
	Conflicts uint64 `json:"conflicts"`
	Aborted   uint64 `json:"aborted"`
}

type QueryStats struct {
	ActiveCursors     uint64 `json:"activeCursors"`
	Total             uint64 `json:"total"`
	Failed            uint64 `json:"failed"`
	CollectionScans   uint64 `json:"collectionScans"`
	IndexScans        uint64 `json:"indexScans"`
	IDLookups         uint64 `json:"idLookups"`
	DocumentsExamined uint64 `json:"documentsExamined"`
	DocumentsReturned uint64 `json:"documentsReturned"`
}

type RealtimeStats struct {
	SharedViews            uint64 `json:"sharedViews"`
	QuerySubscribers       uint64 `json:"querySubscribers"`
	SharedViewReuses       uint64 `json:"sharedViewReuses"`
	IncrementalBatches     uint64 `json:"incrementalBatches"`
	IncrementalViewUpdates uint64 `json:"incrementalViewUpdates"`
	FullViewRecomputes     uint64 `json:"fullViewRecomputes"`
	QueueOverflows         uint64 `json:"queueOverflows"`
	PendingBatches         uint64 `json:"pendingBatches"`
	PendingChanges         uint64 `json:"pendingChanges"`
	PendingBytes           uint64 `json:"pendingBytes"`
	PendingBatchCapacity   uint64 `json:"pendingBatchCapacity"`
	PendingChangeCapacity  uint64 `json:"pendingChangeCapacity"`
	PendingByteCapacity    uint64 `json:"pendingByteCapacity"`
	WatcherPendingBytes    uint64 `json:"watcherPendingBytes"`
	WatcherByteCapacity    uint64 `json:"watcherByteCapacity"`
	DispatchPendingBatches uint64 `json:"dispatchPendingBatches"`
	DispatchPendingChanges uint64 `json:"dispatchPendingChanges"`
	DispatchPendingBytes   uint64 `json:"dispatchPendingBytes"`
	DispatchBatchCapacity  uint64 `json:"dispatchBatchCapacity"`
	DispatchChangeCapacity uint64 `json:"dispatchChangeCapacity"`
	DispatchByteCapacity   uint64 `json:"dispatchByteCapacity"`
	SharedDeltas           uint64 `json:"sharedDeltas"`
	DeltaDeliveries        uint64 `json:"deltaDeliveries"`
	DeltaOperations        uint64 `json:"deltaOperations"`
	PublishedBatches       uint64 `json:"publishedBatches"`
	PublishedChanges       uint64 `json:"publishedChanges"`
	WatcherDeliveries      uint64 `json:"watcherDeliveries"`
	InitialSnapshots       uint64 `json:"initialSnapshots"`
	QueryRecomputes        uint64 `json:"queryRecomputes"`
	SnapshotsEmitted       uint64 `json:"snapshotsEmitted"`
	DocumentsEmitted       uint64 `json:"documentsEmitted"`
	SlowConsumers          uint64 `json:"slowConsumers"`
}

// StorageStats describes the selected physical backend. Session counters reset
// on reopen; physical state and cache counters come from the backend itself.
type StorageStats struct {
	Engine                     string                    `json:"engine"`
	RollbackProtected          bool                      `json:"rollbackProtected"`
	RollbackAnchorSequence     uint64                    `json:"rollbackAnchorSequence"`
	RollbackAnchorGeneration   uint64                    `json:"rollbackAnchorGeneration"`
	RollbackAnchorFailures     uint64                    `json:"rollbackAnchorFailures"`
	RollbackAnchorTimeout      time.Duration             `json:"rollbackAnchorTimeoutNanos"`
	RollbackAnchorNanos        uint64                    `json:"rollbackAnchorNanos"`
	RollbackAnchorMaxLatency   time.Duration             `json:"rollbackAnchorMaxLatencyNanos"`
	RollbackAnchorStore        RollbackAnchorStoreStatus `json:"rollbackAnchorStore"`
	PageSize                   uint64                    `json:"pageSize"`
	Generation                 uint64                    `json:"generation"`
	PhysicalPages              uint64                    `json:"physicalPages"`
	CommitSequence             uint64                    `json:"commitSequence"`
	OldestRetainedSequence     uint64                    `json:"oldestRetainedSequence"`
	RetainedCommits            uint64                    `json:"retainedCommits"`
	CommitRetentionMax         uint64                    `json:"commitRetentionMax"`
	CommitRetentionOverage     uint64                    `json:"commitRetentionOverage"`
	RetainedCommitBytes        uint64                    `json:"retainedCommitBytes"`
	CommitRetentionMaxBytes    uint64                    `json:"commitRetentionMaxBytes"`
	CommitRetentionByteOverage uint64                    `json:"commitRetentionByteOverage"`
	RetentionPrunedCommits     uint64                    `json:"retentionPrunedCommits"`
	RetentionPressureEvents    uint64                    `json:"retentionPressureEvents"`
	RetentionPressure          bool                      `json:"retentionPressure"`
	StorageUsedBytes           uint64                    `json:"storageUsedBytes"`
	StorageMaxBytes            uint64                    `json:"storageMaxBytes"`
	StorageByteOverage         uint64                    `json:"storageByteOverage"`
	StorageLimitRejections     uint64                    `json:"storageLimitRejections"`
	StorageQuotaExhausted      bool                      `json:"storageQuotaExhausted"`
	ActiveReaders              uint64                    `json:"activeReaders"`
	ActiveReplayLeases         uint64                    `json:"activeReplayLeases"`
	Documents                  uint64                    `json:"documents"`
	Collections                uint64                    `json:"collections"`
	ReusablePages              uint64                    `json:"reusablePages"`
	TreeSplits                 uint64                    `json:"treeSplits"`
	TreeMerges                 uint64                    `json:"treeMerges"`
	PersistentFreeSpace        bool                      `json:"persistentFreeSpace"`
	FreeSpaceLoads             uint64                    `json:"freeSpaceLoads"`
	FreeSpaceLoadFailures      uint64                    `json:"freeSpaceLoadFailures"`
	FreeSpacePublishes         uint64                    `json:"freeSpacePublishes"`
	FreeSpaceCandidateChecks   uint64                    `json:"freeSpaceCandidateChecks"`
	PageCache                  PageCacheStats            `json:"pageCache"`
	DocumentCache              DocumentCacheStats        `json:"documentCache"`
	CommitAttempts             uint64                    `json:"commitAttempts"`
	CommittedTransactions      uint64                    `json:"committedTransactions"`
	RejectedTransactions       uint64                    `json:"rejectedTransactions"`
	CommitNanos                uint64                    `json:"commitNanos"`
	CommitMaxLatency           time.Duration             `json:"commitMaxLatencyNanos"`
}

type PageCacheStats struct {
	CapacityPages uint64 `json:"capacityPages"`
	ResidentPages uint64 `json:"residentPages"`
	Hits          uint64 `json:"hits"`
	Misses        uint64 `json:"misses"`
	Evictions     uint64 `json:"evictions"`
}

type DocumentCacheStats struct {
	CapacityEntries uint64 `json:"capacityEntries"`
	CapacityBytes   uint64 `json:"capacityBytes"`
	Entries         uint64 `json:"entries"`
	Bytes           uint64 `json:"bytes"`
	Hits            uint64 `json:"hits"`
	Misses          uint64 `json:"misses"`
	Evictions       uint64 `json:"evictions"`
}

type CompactionStats struct {
	Active       uint64        `json:"active"`
	Attempts     uint64        `json:"attempts"`
	Completed    uint64        `json:"completed"`
	Failed       uint64        `json:"failed"`
	InputBytes   uint64        `json:"inputBytes"`
	OutputBytes  uint64        `json:"outputBytes"`
	LastDuration time.Duration `json:"lastDurationNanos"`
}

type ReclamationStats struct {
	Active          uint64        `json:"active"`
	Attempts        uint64        `json:"attempts"`
	Scans           uint64        `json:"scans"`
	Conflicts       uint64        `json:"conflicts"`
	Completed       uint64        `json:"completed"`
	Failed          uint64        `json:"failed"`
	LastAttempts    uint64        `json:"lastAttempts"`
	LastOnline      bool          `json:"lastOnline"`
	LastReachable   uint64        `json:"lastReachable"`
	LastReclaimable uint64        `json:"lastReclaimable"`
	LastDuration    time.Duration `json:"lastDurationNanos"`
}

type BackupStats struct {
	Active       uint64        `json:"active"`
	Attempts     uint64        `json:"attempts"`
	Completed    uint64        `json:"completed"`
	Failed       uint64        `json:"failed"`
	LastBytes    uint64        `json:"lastBytes"`
	LastDuration time.Duration `json:"lastDurationNanos"`
}

type dbMetrics struct {
	collections, documents, indexes                      atomic.Uint64
	commits, commitChanges                               atomic.Uint64
	writeTransactionsActive, writeTransactionsStarted    atomic.Uint64
	writeTransactionsCommitted, writeTransactionsNoops   atomic.Uint64
	writeTransactionsConflicts, writeTransactionsAborted atomic.Uint64

	queries, queryFailures, activeCursors  atomic.Uint64
	collectionScans, indexScans, idLookups atomic.Uint64
	documentsExamined, documentsReturned   atomic.Uint64

	publishedBatches, publishedChanges, watcherDeliveries atomic.Uint64
	initialSnapshots, queryRecomputes, snapshotsEmitted   atomic.Uint64
	documentsEmitted, slowConsumers                       atomic.Uint64
	sharedViews, querySubscribers, sharedViewReuses       atomic.Uint64
	incrementalBatches, incrementalViewUpdates            atomic.Uint64
	fullViewRecomputes, reactiveQueueOverflows            atomic.Uint64
	pendingReactiveBatches, pendingReactiveChanges        atomic.Uint64
	pendingReactiveBytes                                  atomic.Uint64
	sharedDeltas, deltaDeliveries, deltaOperations        atomic.Uint64

	durableCommitAttempts, durableCommittedTransactions atomic.Uint64
	durableRejectedTransactions, durableCommitNanos     atomic.Uint64
	durableCommitMaxNanos                               atomic.Uint64
	primaryWriteFenceChecks, primaryWriteFenceRejected  atomic.Uint64

	compactionActive, compactionAttempts, compactionCompleted     atomic.Uint64
	compactionFailed, compactionInputBytes, compactionOutputBytes atomic.Uint64
	compactionLastNanos                                           atomic.Uint64

	reclamationActive, reclamationAttempts, reclamationCompleted    atomic.Uint64
	reclamationFailed, reclamationReachable, reclamationReclaimable atomic.Uint64
	reclamationScans, reclamationConflicts, reclamationLastAttempts atomic.Uint64
	reclamationLastNanos                                            atomic.Uint64
	reclamationLastOnline                                           atomic.Bool

	backupActive, backupAttempts, backupCompleted      atomic.Uint64
	backupFailed, backupLastBytes, backupLastNanos     atomic.Uint64
	resourceLimitRejections                            atomic.Uint64
	indexBuildActive, indexBuildAttempts               atomic.Uint64
	indexBuildCompleted, indexBuildFailed              atomic.Uint64
	indexBuildRetries, indexBuildConflicts             atomic.Uint64
	indexBuildSchedulerRuns, indexBuildSchedulerYields atomic.Uint64
	indexBuildSchedulerFailures                        atomic.Uint64
	indexBuildMaxNanos                                 atomic.Uint64
	indexBuildLastMu                                   sync.Mutex
	indexBuildLastEntries, indexBuildLastBytes         uint64
	indexBuildLastNanos                                uint64
}

func (db *DB) Stats() DBStats {
	now := time.Now()
	stats := DBStats{CapturedAt: now}
	if db == nil {
		return stats
	}

	var coordinator *commitCoordinator
	var dispatcher *changeDispatcher
	var dispatch changeDispatcherStats
	db.mu.RLock()
	stats.StartedAt = db.startedAt
	stats.Recovery = db.recovery
	stats.Resources.Limits = db.resourceLimits
	stats.Closed = db.closed
	stats.WritesDisabled = db.fatalErr != nil
	stats.Durable = db.durability != nil
	stats.CommitSequence = db.token
	stats.Collections = db.metrics.collections.Load()
	stats.Documents = db.metrics.documents.Load()
	stats.Indexes = db.metrics.indexes.Load()
	stats.PrimaryWriteFence.Configured = db.primaryWriteFence != nil
	stats.PrimaryWriteFence.Enforced = stats.PrimaryWriteFence.Configured && !db.replicaReadOnly
	coordinator = db.commitCoordinator
	dispatcher = db.dispatcher
	if provider, ok := db.durability.(storageStatsBackend); ok {
		stats.Storage = provider.storageDBStats()
		if stats.Storage.Engine == "current" {
			stats.Documents = stats.Storage.Documents
			stats.Collections = stats.Storage.Collections
		}
	}
	if provider, ok := db.durability.(indexBuildStatsBackend); ok {
		persistent := provider.indexBuildDBStats()
		stats.IndexBuilds.Persistent = persistent.Persistent
		stats.IndexBuilds.Scanning = persistent.Scanning
		stats.IndexBuilds.CatchingUp = persistent.CatchingUp
		stats.IndexBuilds.Ready = persistent.Ready
		stats.IndexBuilds.PersistentFailed = persistent.PersistentFailed
		stats.IndexBuilds.oldestActiveAppliedSequence = persistent.oldestActiveAppliedSequence
		stats.IndexBuilds.RetentionLeaseActive = persistent.oldestActiveAppliedSequence != 0 &&
			persistent.oldestActiveAppliedSequence < stats.CommitSequence
		stats.IndexBuilds.RetentionPressure = stats.IndexBuilds.RetentionLeaseActive &&
			stats.Storage.RetentionPressure && stats.Storage.OldestRetainedSequence != 0 &&
			persistent.oldestActiveAppliedSequence+1 == stats.Storage.OldestRetainedSequence
		stats.IndexBuilds.PersistentEntries = persistent.PersistentEntries
		stats.IndexBuilds.PersistentBytes = persistent.PersistentBytes
	}
	db.mu.RUnlock()
	if coordinator != nil {
		stats.CommitCoordinator = coordinator.stats()
	}
	if dispatcher != nil {
		dispatch = dispatcher.stats()
	}
	if !stats.StartedAt.IsZero() {
		stats.Uptime = now.Sub(stats.StartedAt)
	}

	var watcherPendingBytes uint64
	db.feedMu.Lock()
	stats.ActiveChangeWatchers = uint64(len(db.watchers))
	watcherPendingBytes = db.pendingWatcherBytes
	db.feedMu.Unlock()
	stats.ActiveChangeWatchers += db.metrics.querySubscribers.Load()

	stats.Commits = CommitStats{
		Total:   db.metrics.commits.Load(),
		Changes: db.metrics.commitChanges.Load(),
	}
	stats.Transactions = WriteTransactionStats{
		Active: db.metrics.writeTransactionsActive.Load(), Started: db.metrics.writeTransactionsStarted.Load(),
		Committed: db.metrics.writeTransactionsCommitted.Load(), Noops: db.metrics.writeTransactionsNoops.Load(),
		Conflicts: db.metrics.writeTransactionsConflicts.Load(), Aborted: db.metrics.writeTransactionsAborted.Load(),
	}
	stats.Resources.Rejections = db.metrics.resourceLimitRejections.Load()
	stats.PrimaryWriteFence.Checks = db.metrics.primaryWriteFenceChecks.Load()
	stats.PrimaryWriteFence.Rejected = db.metrics.primaryWriteFenceRejected.Load()
	db.metrics.indexBuildLastMu.Lock()
	lastIndexBuildEntries, lastIndexBuildBytes := db.metrics.indexBuildLastEntries, db.metrics.indexBuildLastBytes
	lastIndexBuildNanos := db.metrics.indexBuildLastNanos
	db.metrics.indexBuildLastMu.Unlock()
	persistentIndexBuilds := stats.IndexBuilds
	stats.IndexBuilds = IndexBuildStats{
		Active: db.metrics.indexBuildActive.Load(), Attempts: db.metrics.indexBuildAttempts.Load(),
		Completed: db.metrics.indexBuildCompleted.Load(), Failed: db.metrics.indexBuildFailed.Load(),
		Retries: db.metrics.indexBuildRetries.Load(), Conflicts: db.metrics.indexBuildConflicts.Load(),
		LastEntries: lastIndexBuildEntries, LastBytes: lastIndexBuildBytes,
		LastDuration: time.Duration(lastIndexBuildNanos),
		MaxDuration:  time.Duration(db.metrics.indexBuildMaxNanos.Load()),
		Persistent:   persistentIndexBuilds.Persistent, Scanning: persistentIndexBuilds.Scanning,
		CatchingUp: persistentIndexBuilds.CatchingUp, Ready: persistentIndexBuilds.Ready,
		PersistentFailed:     persistentIndexBuilds.PersistentFailed,
		RetentionLeaseActive: persistentIndexBuilds.RetentionLeaseActive,
		RetentionPressure:    persistentIndexBuilds.RetentionPressure,
		PersistentEntries:    persistentIndexBuilds.PersistentEntries, PersistentBytes: persistentIndexBuilds.PersistentBytes,
		SchedulerRuns: db.metrics.indexBuildSchedulerRuns.Load(), SchedulerYields: db.metrics.indexBuildSchedulerYields.Load(),
		SchedulerFailures: db.metrics.indexBuildSchedulerFailures.Load(),
	}
	stats.Queries = QueryStats{
		ActiveCursors:     db.metrics.activeCursors.Load(),
		Total:             db.metrics.queries.Load(),
		Failed:            db.metrics.queryFailures.Load(),
		CollectionScans:   db.metrics.collectionScans.Load(),
		IndexScans:        db.metrics.indexScans.Load(),
		IDLookups:         db.metrics.idLookups.Load(),
		DocumentsExamined: db.metrics.documentsExamined.Load(),
		DocumentsReturned: db.metrics.documentsReturned.Load(),
	}
	stats.Realtime = RealtimeStats{
		SharedViews:            db.metrics.sharedViews.Load(),
		QuerySubscribers:       db.metrics.querySubscribers.Load(),
		SharedViewReuses:       db.metrics.sharedViewReuses.Load(),
		IncrementalBatches:     db.metrics.incrementalBatches.Load(),
		IncrementalViewUpdates: db.metrics.incrementalViewUpdates.Load(),
		FullViewRecomputes:     db.metrics.fullViewRecomputes.Load(),
		QueueOverflows:         db.metrics.reactiveQueueOverflows.Load(),
		PendingBatches:         db.metrics.pendingReactiveBatches.Load(),
		PendingChanges:         db.metrics.pendingReactiveChanges.Load(),
		PendingBytes:           db.metrics.pendingReactiveBytes.Load(),
		PendingBatchCapacity:   maxPendingReactiveBatches,
		PendingChangeCapacity:  maxPendingReactiveChanges,
		PendingByteCapacity:    maxPendingReactiveBytes,
		WatcherPendingBytes:    watcherPendingBytes,
		WatcherByteCapacity:    maxPendingChangeWatchersBytes,
		DispatchPendingBatches: dispatch.pendingBatches,
		DispatchPendingChanges: dispatch.pendingChanges,
		DispatchPendingBytes:   dispatch.pendingBytes,
		DispatchBatchCapacity:  dispatch.batchCapacity,
		DispatchChangeCapacity: dispatch.changeCapacity,
		DispatchByteCapacity:   dispatch.byteCapacity,
		SharedDeltas:           db.metrics.sharedDeltas.Load(),
		DeltaDeliveries:        db.metrics.deltaDeliveries.Load(),
		DeltaOperations:        db.metrics.deltaOperations.Load(),
		PublishedBatches:       db.metrics.publishedBatches.Load(),
		PublishedChanges:       db.metrics.publishedChanges.Load(),
		WatcherDeliveries:      db.metrics.watcherDeliveries.Load(),
		InitialSnapshots:       db.metrics.initialSnapshots.Load(),
		QueryRecomputes:        db.metrics.queryRecomputes.Load(),
		SnapshotsEmitted:       db.metrics.snapshotsEmitted.Load(),
		DocumentsEmitted:       db.metrics.documentsEmitted.Load(),
		SlowConsumers:          db.metrics.slowConsumers.Load(),
	}
	stats.Compaction = CompactionStats{
		Active: db.metrics.compactionActive.Load(), Attempts: db.metrics.compactionAttempts.Load(),
		Completed: db.metrics.compactionCompleted.Load(), Failed: db.metrics.compactionFailed.Load(),
		InputBytes: db.metrics.compactionInputBytes.Load(), OutputBytes: db.metrics.compactionOutputBytes.Load(),
		LastDuration: time.Duration(db.metrics.compactionLastNanos.Load()),
	}
	stats.Reclamation = ReclamationStats{
		Active: db.metrics.reclamationActive.Load(), Attempts: db.metrics.reclamationAttempts.Load(),
		Scans: db.metrics.reclamationScans.Load(), Conflicts: db.metrics.reclamationConflicts.Load(),
		Completed: db.metrics.reclamationCompleted.Load(), Failed: db.metrics.reclamationFailed.Load(),
		LastAttempts: db.metrics.reclamationLastAttempts.Load(), LastOnline: db.metrics.reclamationLastOnline.Load(),
		LastReachable: db.metrics.reclamationReachable.Load(), LastReclaimable: db.metrics.reclamationReclaimable.Load(),
		LastDuration: time.Duration(db.metrics.reclamationLastNanos.Load()),
	}
	stats.Backup = BackupStats{
		Active: db.metrics.backupActive.Load(), Attempts: db.metrics.backupAttempts.Load(),
		Completed: db.metrics.backupCompleted.Load(), Failed: db.metrics.backupFailed.Load(),
		LastBytes: db.metrics.backupLastBytes.Load(), LastDuration: time.Duration(db.metrics.backupLastNanos.Load()),
	}
	if diagnostics := db.diagnostics.Load(); diagnostics != nil {
		stats.Diagnostics = diagnostics.Stats()
	}
	stats.Storage.CommitAttempts = db.metrics.durableCommitAttempts.Load()
	stats.Storage.CommittedTransactions = db.metrics.durableCommittedTransactions.Load()
	stats.Storage.RejectedTransactions = db.metrics.durableRejectedTransactions.Load()
	stats.Storage.CommitNanos = db.metrics.durableCommitNanos.Load()
	stats.Storage.CommitMaxLatency = time.Duration(db.metrics.durableCommitMaxNanos.Load())
	return stats
}

func (db *DB) recordLiveCommit(batch ChangeBatch) {
	db.recordCommittedBatch(batch)
	// Every production caller holds db.mu for the successful publication. Keep
	// logical gauges on that same visibility boundary so Stats observes either
	// the complete old commit or the complete new one without scanning catalogs.
	db.metrics.collections.Store(uint64(len(db.collections)))
	for _, change := range batch.Changes {
		switch change.Operation {
		case InsertOperation:
			db.metrics.documents.Add(1)
		case DeleteOperation:
			db.metrics.documents.Add(^uint64(0))
		case CreateIndexOperation:
			db.metrics.indexes.Add(1)
		}
	}
	db.metrics.commits.Add(1)
	db.metrics.commitChanges.Add(uint64(len(batch.Changes)))
}

// initializeLogicalStats performs the one allowed catalog walk while a DB is
// still private to its constructor. Live commits maintain the gauges in O(1),
// and Stats never repeats this work.  keeps documents outside the compatibility
// mirror, so its authoritative persistent count is supplied explicitly.
func (db *DB) initializeLogicalStats(persistentDocuments *uint64) {
	if db == nil {
		return
	}
	var documents, indexes uint64
	for _, collection := range db.collections {
		documents += uint64(len(collection.documents))
		indexes += uint64(len(collection.indexes))
	}
	if persistentDocuments != nil {
		documents = *persistentDocuments
	}
	db.metrics.collections.Store(uint64(len(db.collections)))
	db.metrics.documents.Store(documents)
	db.metrics.indexes.Store(indexes)
}

func (db *DB) recordQuery(explain ExplainResult, returned int, err error, diagnostic diagnosticSpan) {
	db.metrics.queries.Add(1)
	if err != nil {
		db.metrics.queryFailures.Add(1)
		db.finishQueryDiagnostic(diagnostic, explain, returned, err)
		return
	}
	switch explain.Stage {
	case "COLLSCAN":
		db.metrics.collectionScans.Add(1)
	case "IXSCAN":
		db.metrics.indexScans.Add(1)
	case "ID_LOOKUP":
		db.metrics.idLookups.Add(1)
	}
	if explain.DocumentsExamined > 0 {
		db.metrics.documentsExamined.Add(uint64(explain.DocumentsExamined))
	}
	if returned > 0 {
		db.metrics.documentsReturned.Add(uint64(returned))
	}
	db.finishQueryDiagnostic(diagnostic, explain, returned, err)
}

func updateAtomicMax(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}
