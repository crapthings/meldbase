package database

import "errors"

var (
	// ErrClosed reports that the database is closed.
	ErrClosed = errors.New("meldbase: database is closed")
	// ErrInvalidDocument reports an invalid document.
	ErrInvalidDocument = errors.New("meldbase: invalid document")
	// ErrInvalidFilter reports an invalid filter.
	ErrInvalidFilter = errors.New("meldbase: invalid filter")
	// ErrInvalidUpdate reports an invalid update.
	ErrInvalidUpdate = errors.New("meldbase: invalid update")
	// ErrMutationLimit reports that a mutation exceeded its affected-row limit.
	ErrMutationLimit = errors.New("meldbase: mutation affected-row limit exceeded")
	// ErrWriteConflict reports that the write transaction snapshot conflicted.
	ErrWriteConflict = errors.New("meldbase: write transaction snapshot conflicted")
	// ErrWriteTransactionUnsupported reports that write transactions require a durable database.
	ErrWriteTransactionUnsupported = errors.New("meldbase: write transactions require a durable database")
	// ErrNotFound reports that the document was not found.
	ErrNotFound = errors.New("meldbase: document not found")
	// ErrDuplicateID reports a duplicate document ID.
	ErrDuplicateID = errors.New("meldbase: duplicate document id")
	// ErrInvalidCollection reports an invalid collection name.
	ErrInvalidCollection = errors.New("meldbase: invalid collection")
	// ErrImmutableID reports that _id is immutable.
	ErrImmutableID = errors.New("meldbase: _id is immutable")
	// ErrSlowConsumer reports that the change consumer is too slow.
	ErrSlowConsumer = errors.New("meldbase: change consumer is too slow")
	// ErrCorrupt reports database corruption.
	ErrCorrupt = errors.New("meldbase: corrupt database")
	// ErrUnsupportedFormat reports an unsupported storage format or required feature.
	ErrUnsupportedFormat = errors.New("meldbase: unsupported storage format or required feature")
	// ErrDuplicateKey reports a duplicate index key.
	ErrDuplicateKey = errors.New("meldbase: duplicate index key")
	// ErrInvalidIndex reports an invalid index definition.
	ErrInvalidIndex = errors.New("meldbase: invalid index")
	// ErrCompoundIndexUnsupported reports that compound or descending indexes are unsupported.
	ErrCompoundIndexUnsupported = errors.New("meldbase: compound or descending indexes are unsupported")
	// ErrDurability reports a durability failure that disables writes.
	ErrDurability = errors.New("meldbase: durability failure; writes are disabled")
	// ErrInvalidDelta reports an invalid query delta.
	ErrInvalidDelta = errors.New("meldbase: invalid query delta")
	// ErrHistoryLost reports that the requested history is no longer retained.
	ErrHistoryLost = errors.New("meldbase: requested history is no longer retained")
	// ErrDestinationExists reports that the destination already exists.
	ErrDestinationExists = errors.New("meldbase: destination already exists")
	// ErrCompactionUnsupported reports that compaction requires an open durable database.
	ErrCompactionUnsupported = errors.New("meldbase: compaction requires an open durable database")
	// ErrCompactionDestinationExists reports that the compaction destination already exists.
	ErrCompactionDestinationExists = errors.New("meldbase: compaction destination already exists")
	// ErrReclamationUnsupported reports that page reclamation requires an open durable database.
	ErrReclamationUnsupported = errors.New("meldbase: page reclamation requires an open durable database")
	// ErrInvalidReclamationOptions reports invalid reclamation options.
	ErrInvalidReclamationOptions = errors.New("meldbase: invalid reclamation options")
	// ErrReclamationConflict reports that the online reclamation conflicted with concurrent writes.
	ErrReclamationConflict = errors.New("meldbase: online reclamation conflicted with concurrent writes")
	// ErrBackupUnsupported reports that physical backup requires an open durable database.
	ErrBackupUnsupported = errors.New("meldbase: physical backup requires an open durable database")
	// ErrBackupDestinationExists reports that the backup destination already exists or is the source.
	ErrBackupDestinationExists = errors.New("meldbase: backup destination already exists or is the source")
	// ErrLogicalArchiveUnsupported reports that logical archive requires an open durable database.
	ErrLogicalArchiveUnsupported = errors.New("meldbase: logical archive requires an open durable database")
	// ErrLogicalArchiveDestinationExists reports that the logical archive destination already exists or is the source.
	ErrLogicalArchiveDestinationExists = errors.New("meldbase: logical archive destination already exists or is the source")
	// ErrVerificationUnsupported reports that verification requires an existing database.
	ErrVerificationUnsupported = errors.New("meldbase: verification requires an existing database")
	// ErrDatabaseLocked reports that the database is locked by another process.
	ErrDatabaseLocked = errors.New("meldbase: database is locked by another process")
	// ErrRollbackDetected reports that the database was rolled back.
	ErrRollbackDetected = errors.New("meldbase: database rollback detected")
	// ErrDatabaseIdentity reports an unexpected database identity.
	ErrDatabaseIdentity = errors.New("meldbase: unexpected database identity")
	// ErrInsecureFileMode reports that the database file permissions are not owner-private.
	ErrInsecureFileMode = errors.New("meldbase: database file permissions are not owner-private")
	// ErrRollbackAnchorRequired reports that the rollback anchor is missing; explicit initialization is required.
	ErrRollbackAnchorRequired = errors.New("meldbase: rollback anchor is missing; explicit initialization is required")
	// ErrRollbackAnchor reports a failure to persist the rollback anchor.
	ErrRollbackAnchor = errors.New("meldbase: rollback anchor durability failure")
	// ErrInvalidRollbackProtection reports invalid rollback protection options.
	ErrInvalidRollbackProtection = errors.New("meldbase: invalid rollback protection options")
	// ErrRecoveryRequired reports that the selected policy requires startup recovery.
	ErrRecoveryRequired = errors.New("meldbase: startup recovery required by the selected policy")
	// ErrInvalidResourceLimits reports invalid resource limits.
	ErrInvalidResourceLimits = errors.New("meldbase: invalid resource limits")
	// ErrResourceLimit reports that an operation exceeded a resource limit.
	ErrResourceLimit = errors.New("meldbase: resource limit exceeded")
	// ErrQueryBudget reports that the query exceeded its execution budget.
	ErrQueryBudget = errors.New("meldbase: query execution budget exceeded")
	// ErrIndexBuildUnsupported reports that resumable index builds require a durable database.
	ErrIndexBuildUnsupported = errors.New("meldbase: resumable index builds require a durable database")
	// ErrIndexBuildNotFound reports that the index build was not found.
	ErrIndexBuildNotFound = errors.New("meldbase: index build not found")
	// ErrIndexBuildExists reports that the index build already exists.
	ErrIndexBuildExists = errors.New("meldbase: index build already exists")
	// ErrIndexBuildFailed reports that the index build is in a terminal failed state.
	ErrIndexBuildFailed = errors.New("meldbase: index build is in a terminal failed state")
	// ErrInvalidIndexBuildSchedulerOptions reports invalid index build scheduler options.
	ErrInvalidIndexBuildSchedulerOptions = errors.New("meldbase: invalid index build scheduler options")
	// ErrIndexBuildSchedulerRunning reports that the index build scheduler is already running.
	ErrIndexBuildSchedulerRunning = errors.New("meldbase: index build scheduler already running")
	// ErrInvalidCommitCoordinatorOptions reports invalid commit coordinator options.
	ErrInvalidCommitCoordinatorOptions = errors.New("meldbase: invalid commit coordinator options")
	// ErrInvalidReplayDeliveryTimeout reports invalid replay delivery timeout.
	ErrInvalidReplayDeliveryTimeout = errors.New("meldbase: invalid replay delivery timeout")
	// ErrDurableConsumerUnsupported reports that durable change consumers require a durable database.
	ErrDurableConsumerUnsupported = errors.New("meldbase: durable change consumers require a durable database")
	// ErrDurableConsumerExists reports that the durable change consumer already exists.
	ErrDurableConsumerExists = errors.New("meldbase: durable change consumer already exists")
	// ErrDurableConsumerNotFound reports that the durable change consumer was not found.
	ErrDurableConsumerNotFound = errors.New("meldbase: durable change consumer not found")
	// ErrReplicaReadOnly reports that the follower is read-only.
	ErrReplicaReadOnly = errors.New("meldbase: follower is read-only")
	// ErrReplicaSequence reports that the follower change token is not the next sequence.
	ErrReplicaSequence = errors.New("meldbase: follower change token is not the next sequence")
	// ErrReplicaProtocol reports an invalid replication protocol frame.
	ErrReplicaProtocol = errors.New("meldbase: invalid replication protocol frame")
	// ErrReplicaSourceActive reports that the replication source consumer already has an active session.
	ErrReplicaSourceActive = errors.New("meldbase: replication source consumer already has an active session")
	// ErrReplicaPromotionAuthority reports that the follower promotion requires an external fencing authority.
	ErrReplicaPromotionAuthority = errors.New("meldbase: follower promotion requires an external fencing authority")
	// ErrReplicaPromotionFence reports that the follower promotion fence does not match local state.
	ErrReplicaPromotionFence = errors.New("meldbase: follower promotion fence does not match local state")
	// ErrReplicaPromotionWriteFence reports that the follower promotion requires a configured primary write fence.
	ErrReplicaPromotionWriteFence = errors.New("meldbase: follower promotion requires a configured primary write fence")
	// ErrReplicaPromoted reports that the follower has been promoted and no longer accepts replication.
	ErrReplicaPromoted = errors.New("meldbase: follower has been promoted and no longer accepts replication")
	// ErrPrimaryWriteFence reports that the primary write fence rejected the commit.
	ErrPrimaryWriteFence = errors.New("meldbase: primary write fence rejected the commit")
	// ErrCommitOutcomeUnknown reports that an admitted write has no confirmed result.
	//
	// Callers must reconcile by the returned document IDs instead of retrying blindly.
	ErrCommitOutcomeUnknown = errors.New("meldbase: commit outcome is unknown; do not retry blindly")
)
