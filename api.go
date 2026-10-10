package meldbase

import (
	"context"
	"io"
	"time"

	database "github.com/crapthings/meldbase/internal/database"
)

// ArchiveBootstrap binds an exact verified physical snapshot to the
// durable database change feed that was pinned before that snapshot began.
//
// A receiver must persist and verify Backup, then drain and Ack every batch up
// through SnapshotToken without applying it (the snapshot already contains
// those effects). It can then apply and Ack later batches in order. This avoids
// the bootstrap/tail gap without inventing a second, weaker history contract.
type ArchiveBootstrap = database.ArchiveBootstrap

// OperationalState is a minimal, allocation-free serving-state snapshot.
//
// A
// fail-stop durability error preserves reads from the last committed state but
// disables writes; a closed database is neither readable nor writable.
type OperationalState = database.OperationalState

// BackupResult records the identity, size, and checksum of a physical backup.
type BackupResult = database.BackupResult

// PhysicalBackupImportOptions limits the bytes accepted from a physical backup stream.
//
// Zero selects the normal file limit.
// Deployments with a deliberately larger database must set MaxBytes
// explicitly on the receiving side; a sender never chooses that authority.
type PhysicalBackupImportOptions = database.PhysicalBackupImportOptions

// ImportPhysicalBackup verifies a physical backup stream and publishes it at a new path.
//
// It writes a private temporary file, checks the claimed byte count and
// SHA-256 while streaming, verifies the stored page structure and index contents,
// then publishes without overwriting an existing file and syncs the directory.
//
// source is intentionally transport-neutral. A WebSocket, HTTP response, QUIC
// stream, or removable-media reader may supply it, but transport cancellation
// must close or honor ctx itself: a generic io.Reader cannot be interrupted
// while blocked in Read. The destination is never opened as a writable DB by
// this function; callers normally open the successfully imported file through
// OpenFollower before applying a replication tail.
func ImportPhysicalBackup(ctx context.Context, source io.Reader, destination string, expected BackupResult, options PhysicalBackupImportOptions) (BackupResult, error) {
	return database.ImportPhysicalBackup(ctx, source, destination, expected, options)
}

// Operation identifies a document or catalog change.
type Operation = database.Operation

// InsertOperation marks a document insertion.
const InsertOperation = database.InsertOperation

// UpdateOperation marks a document update.
const UpdateOperation = database.UpdateOperation

// DeleteOperation marks a document deletion.
const DeleteOperation = database.DeleteOperation

// CreateCollectionOperation marks collection creation in the durable database change feed.
//
// Ordinary collection creation remains implicit for CRUD callers.
const CreateCollectionOperation = database.CreateCollectionOperation

// CreateIndexOperation marks publication of a new index.
const CreateIndexOperation = database.CreateIndexOperation

// ReplaceIndexOperation marks replacement of an existing index.
const ReplaceIndexOperation = database.ReplaceIndexOperation

// IndexDefinition describes the ordered fields and uniqueness of an index.
type IndexDefinition = database.IndexDefinition

// Change records a document or catalog edit at a commit.
type Change = database.Change

// ChangeBatch groups changes under one ordered commit token.
type ChangeBatch = database.ChangeBatch

// DB owns an in-memory or durable document database.
type DB = database.DB

// New creates an in-memory database with default limits.
func New() *DB {
	return database.New()
}

// NewWithOptions creates an in-memory database with explicit resource limits.
func NewWithOptions(options DatabaseOptions) (*DB, error) {
	return database.NewWithOptions(options)
}

// Collection provides document operations for one named collection.
type Collection = database.Collection

// DeleteResult reports how many documents were deleted.
type DeleteResult = database.DeleteResult

// Cursor reads query results and releases their resources when closed.
type Cursor = database.Cursor

// ErrDiagnosticsActive reports that a diagnostics session is already active.
var ErrDiagnosticsActive = database.ErrDiagnosticsActive

// DiagnosticKind identifies the operation recorded by a diagnostic event.
type DiagnosticKind = database.DiagnosticKind

// DiagnosticQuery marks a query diagnostic.
const DiagnosticQuery = database.DiagnosticQuery

// DiagnosticCommit marks a commit diagnostic.
const DiagnosticCommit = database.DiagnosticCommit

// DiagnosticOutcome identifies how a recorded operation ended.
type DiagnosticOutcome = database.DiagnosticOutcome

// DiagnosticSuccess marks a successful operation.
const DiagnosticSuccess = database.DiagnosticSuccess

// DiagnosticFailure marks a failed operation.
const DiagnosticFailure = database.DiagnosticFailure

// DiagnosticCanceled marks a canceled operation.
const DiagnosticCanceled = database.DiagnosticCanceled

// DiagnosticsOptions configures bounded diagnostic recording and event selection.
//
// Defaults retain 256 events and record failures, queries taking at least 50ms,
// and commits taking at least 100ms.
// RecordAll is intended only for short development sessions. SampleEvery adds a
// deterministic one-in-N sample of otherwise fast successful operations.
type DiagnosticsOptions = database.DiagnosticsOptions

// DiagnosticEvent records one operation's timing, outcome, and work counters.
type DiagnosticEvent = database.DiagnosticEvent

// DiagnosticStats reports diagnostics activity and retained event counts.
type DiagnosticStats = database.DiagnosticStats

// DiagnosticSnapshot captures a diagnostics session's counters and retained events.
type DiagnosticSnapshot = database.DiagnosticSnapshot

// Diagnostics owns a bounded ring of recorded operation events.
//
// Close disables timing and recording but keeps retained events readable.
type Diagnostics = database.Diagnostics

// Document maps field names to typed database values.
type Document = database.Document

// NewDocument converts Go fields into a document and rejects invalid names or values.
func NewDocument(fields map[string]any) (Document, error) {
	return database.NewDocument(fields)
}

// Open creates or opens a Meldbase durable database in the current format.
func Open(path string) (*DB, error) {
	return database.Open(path)
}

// OpenWithOptions creates or opens a durable database with explicit storage and resource options.
func OpenWithOptions(path string, options OpenOptions) (*DB, error) {
	return database.OpenWithOptions(path, options)
}

// DurableChangeBatch contains one collection's document changes at a commit token.
//
// Changes is empty when another collection or private catalog
// change advanced the durable position; callers must still Ack that Token after
// processing it so the checkpoint can advance without pinning history forever.
//
// This is deliberately a document-change feed, not a full replication protocol:
// it does not expose private System records, index definitions, collection
// lifecycle or raw storage bytes.
type DurableChangeBatch = database.DurableChangeBatch

// DurableChangeSubscription delivers ordered collection changes with a saved acknowledgement.
//
// Ack must be called only after the
// consumer's external side effect for that token is durable.
type DurableChangeSubscription = database.DurableChangeSubscription

// DurableDatabaseChangeBatch contains public document and catalog changes from one commit.
//
// Archives and single-writer followers must acknowledge Token only after the
// externally applied effect is durable.
//
// Private System records, raw pages, index-build progress and retention
// control records are deliberately excluded. A batch can therefore be empty
// when a private record advanced a retained position; it must still be Acked.
type DurableDatabaseChangeBatch = database.DurableDatabaseChangeBatch

// DurableDatabaseChangeSubscription delivers public database changes with a saved acknowledgement.
//
// It exposes collection creation,
// index publication and document changes in exact Commit Log order. It does
// not itself copy a bootstrap snapshot or apply changes to a follower; those
// transport and ownership contracts are intentionally separate.
type DurableDatabaseChangeSubscription = database.DurableDatabaseChangeSubscription

// ErrClosed reports that the database is closed.
var ErrClosed = database.ErrClosed

// ErrInvalidDocument reports an invalid document.
var ErrInvalidDocument = database.ErrInvalidDocument

// ErrInvalidFilter reports an invalid filter.
var ErrInvalidFilter = database.ErrInvalidFilter

// ErrInvalidUpdate reports an invalid update.
var ErrInvalidUpdate = database.ErrInvalidUpdate

// ErrMutationLimit reports that a mutation exceeded its affected-row limit.
var ErrMutationLimit = database.ErrMutationLimit

// ErrWriteConflict reports that the write transaction snapshot conflicted.
var ErrWriteConflict = database.ErrWriteConflict

// ErrWriteTransactionUnsupported reports that write transactions require a durable database.
var ErrWriteTransactionUnsupported = database.ErrWriteTransactionUnsupported

// ErrNotFound reports that the document was not found.
var ErrNotFound = database.ErrNotFound

// ErrDuplicateID reports a duplicate document ID.
var ErrDuplicateID = database.ErrDuplicateID

// ErrInvalidCollection reports an invalid collection name.
var ErrInvalidCollection = database.ErrInvalidCollection

// ErrImmutableID reports that _id is immutable.
var ErrImmutableID = database.ErrImmutableID

// ErrSlowConsumer reports that the change consumer is too slow.
var ErrSlowConsumer = database.ErrSlowConsumer

// ErrCorrupt reports database corruption.
var ErrCorrupt = database.ErrCorrupt

// ErrUnsupportedFormat reports an unsupported storage format or required feature.
var ErrUnsupportedFormat = database.ErrUnsupportedFormat

// ErrDuplicateKey reports a duplicate index key.
var ErrDuplicateKey = database.ErrDuplicateKey

// ErrInvalidIndex reports an invalid index definition.
var ErrInvalidIndex = database.ErrInvalidIndex

// ErrCompoundIndexUnsupported reports that compound or descending indexes are unsupported.
var ErrCompoundIndexUnsupported = database.ErrCompoundIndexUnsupported

// ErrDurability reports a durability failure that disables writes.
var ErrDurability = database.ErrDurability

// ErrInvalidDelta reports an invalid query delta.
var ErrInvalidDelta = database.ErrInvalidDelta

// ErrHistoryLost reports that the requested history is no longer retained.
var ErrHistoryLost = database.ErrHistoryLost

// ErrDestinationExists reports that the destination already exists.
var ErrDestinationExists = database.ErrDestinationExists

// ErrCompactionUnsupported reports that compaction requires an open durable database.
var ErrCompactionUnsupported = database.ErrCompactionUnsupported

// ErrCompactionDestinationExists reports that the compaction destination already exists.
var ErrCompactionDestinationExists = database.ErrCompactionDestinationExists

// ErrReclamationUnsupported reports that page reclamation requires an open durable database.
var ErrReclamationUnsupported = database.ErrReclamationUnsupported

// ErrInvalidReclamationOptions reports invalid reclamation options.
var ErrInvalidReclamationOptions = database.ErrInvalidReclamationOptions

// ErrReclamationConflict reports that the online reclamation conflicted with concurrent writes.
var ErrReclamationConflict = database.ErrReclamationConflict

// ErrBackupUnsupported reports that physical backup requires an open durable database.
var ErrBackupUnsupported = database.ErrBackupUnsupported

// ErrBackupDestinationExists reports that the backup destination already exists or is the source.
var ErrBackupDestinationExists = database.ErrBackupDestinationExists

// ErrLogicalArchiveUnsupported reports that logical archive requires an open durable database.
var ErrLogicalArchiveUnsupported = database.ErrLogicalArchiveUnsupported

// ErrLogicalArchiveDestinationExists reports that the logical archive destination already exists or is the source.
var ErrLogicalArchiveDestinationExists = database.ErrLogicalArchiveDestinationExists

// ErrVerificationUnsupported reports that verification requires an existing database.
var ErrVerificationUnsupported = database.ErrVerificationUnsupported

// ErrDatabaseLocked reports that the database is locked by another process.
var ErrDatabaseLocked = database.ErrDatabaseLocked

// ErrRollbackDetected reports that the database was rolled back.
var ErrRollbackDetected = database.ErrRollbackDetected

// ErrDatabaseIdentity reports an unexpected database identity.
var ErrDatabaseIdentity = database.ErrDatabaseIdentity

// ErrInsecureFileMode reports that the database file permissions are not owner-private.
var ErrInsecureFileMode = database.ErrInsecureFileMode

// ErrRollbackAnchorRequired reports that the rollback anchor is missing; explicit initialization is required.
var ErrRollbackAnchorRequired = database.ErrRollbackAnchorRequired

// ErrRollbackAnchor reports a failure to persist the rollback anchor.
var ErrRollbackAnchor = database.ErrRollbackAnchor

// ErrInvalidRollbackProtection reports invalid rollback protection options.
var ErrInvalidRollbackProtection = database.ErrInvalidRollbackProtection

// ErrRecoveryRequired reports that the selected policy requires startup recovery.
var ErrRecoveryRequired = database.ErrRecoveryRequired

// ErrInvalidResourceLimits reports invalid resource limits.
var ErrInvalidResourceLimits = database.ErrInvalidResourceLimits

// ErrResourceLimit reports that an operation exceeded a resource limit.
var ErrResourceLimit = database.ErrResourceLimit

// ErrQueryBudget reports that one query exceeded an execution-work budget.
//
// Callers can use errors.Is to distinguish this from invalid query input.
var ErrQueryBudget = database.ErrQueryBudget

// ErrIndexBuildUnsupported reports that resumable index builds require a durable database.
var ErrIndexBuildUnsupported = database.ErrIndexBuildUnsupported

// ErrIndexBuildNotFound reports that the index build was not found.
var ErrIndexBuildNotFound = database.ErrIndexBuildNotFound

// ErrIndexBuildExists reports that the index build already exists.
var ErrIndexBuildExists = database.ErrIndexBuildExists

// ErrIndexBuildFailed reports that the index build is in a terminal failed state.
var ErrIndexBuildFailed = database.ErrIndexBuildFailed

// ErrInvalidIndexBuildSchedulerOptions reports invalid index build scheduler options.
var ErrInvalidIndexBuildSchedulerOptions = database.ErrInvalidIndexBuildSchedulerOptions

// ErrIndexBuildSchedulerRunning reports that the index build scheduler is already running.
var ErrIndexBuildSchedulerRunning = database.ErrIndexBuildSchedulerRunning

// ErrInvalidCommitCoordinatorOptions reports invalid commit coordinator options.
var ErrInvalidCommitCoordinatorOptions = database.ErrInvalidCommitCoordinatorOptions

// ErrInvalidReplayDeliveryTimeout reports invalid replay delivery timeout.
var ErrInvalidReplayDeliveryTimeout = database.ErrInvalidReplayDeliveryTimeout

// ErrDurableConsumerUnsupported reports that durable change consumers require a durable database.
var ErrDurableConsumerUnsupported = database.ErrDurableConsumerUnsupported

// ErrDurableConsumerExists reports that the durable change consumer already exists.
var ErrDurableConsumerExists = database.ErrDurableConsumerExists

// ErrDurableConsumerNotFound reports that the durable change consumer was not found.
var ErrDurableConsumerNotFound = database.ErrDurableConsumerNotFound

// ErrReplicaReadOnly reports that the follower is read-only.
var ErrReplicaReadOnly = database.ErrReplicaReadOnly

// ErrReplicaSequence reports that the follower change token is not the next sequence.
var ErrReplicaSequence = database.ErrReplicaSequence

// ErrReplicaProtocol reports an invalid replication protocol frame.
var ErrReplicaProtocol = database.ErrReplicaProtocol

// ErrReplicaSourceActive reports that the replication source consumer already has an active session.
var ErrReplicaSourceActive = database.ErrReplicaSourceActive

// ErrReplicaPromotionAuthority reports that the follower promotion requires an external fencing authority.
var ErrReplicaPromotionAuthority = database.ErrReplicaPromotionAuthority

// ErrReplicaPromotionFence reports that the follower promotion fence does not match local state.
var ErrReplicaPromotionFence = database.ErrReplicaPromotionFence

// ErrReplicaPromotionWriteFence reports that the follower promotion requires a configured primary write fence.
var ErrReplicaPromotionWriteFence = database.ErrReplicaPromotionWriteFence

// ErrReplicaPromoted reports that the follower has been promoted and no longer accepts replication.
var ErrReplicaPromoted = database.ErrReplicaPromoted

// ErrPrimaryWriteFence reports that the primary write fence rejected the commit.
var ErrPrimaryWriteFence = database.ErrPrimaryWriteFence

// ErrCommitOutcomeUnknown means cancellation or a lost caller connection
// raced an already-admitted durable write.
//
// Callers must reconcile by the
// returned document ID(s), rather than retrying business logic blindly.
var ErrCommitOutcomeUnknown = database.ErrCommitOutcomeUnknown

// Filter maps document fields and query operators to their conditions.
type Filter = database.Filter

// QueryOptions configures query sorting and pagination.
type QueryOptions = database.QueryOptions

// CompileQuery validates a filter and options to produce a reusable query.
func CompileQuery(filter Filter, options QueryOptions) (QuerySpec, error) {
	return database.CompileQuery(filter, options)
}

// Follower owns a read-only local database advanced by validated source change batches.
//
// Transport authentication and snapshot transfer are managed outside this type;
// promotion requires the external authority and write guard described below.
type Follower = database.Follower

// FollowerPromotionRequest identifies the database version requesting permission to become writable.
type FollowerPromotionRequest = database.FollowerPromotionRequest

// FollowerPromotionFence proves that the former primary lost write authority for this database version.
//
// Epoch is non-empty and
// deliberately opaque to Meldbase: a controller may use it as an epoch ID or
// a compact signed lease certificate (as integrations/primarylease does).
// Meldbase does not invent a local substitute for that distributed safety
// decision.
type FollowerPromotionFence = database.FollowerPromotionFence

// FollowerPromotionAuthority grants promotion after durably revoking the former primary's write authority.
//
// Implementations normally revoke a primary lease through a quorum
// controller or external consensus store.
type FollowerPromotionAuthority = database.FollowerPromotionAuthority

// FollowerPromotionFenceBinder connects a promotion certificate to the local write protection guard.
//
// The binder may update caller-owned local lease/epoch state, but must not enable
// writes until it has accepted the exact fence. It runs on the promotion
// control path, outside the DB writer lock; unlike ValidatePrimaryWrite it
// may coordinate with the controller if the implementation needs to.
//
// A promoted follower requires this interface in addition to
// PrimaryWriteFence. Otherwise an unrelated always-allow guard could make a
// one-time promotion certificate appear to grant permanent write authority.
type FollowerPromotionFenceBinder = database.FollowerPromotionFenceBinder

// OpenFollower opens a database copy as a read-only replica.
//
// Normal public mutations on DB return ErrReplicaReadOnly; use Apply to advance the
// next source token. The returned DB remains fully queryable and reactive.
func OpenFollower(path string, options OpenOptions) (*Follower, error) {
	return database.OpenFollower(path, options)
}

// IndexBuildSchedulerOptions configures polling, time slices, and build concurrency.
//
// The scheduler must be started explicitly; each task yields durable progress
// between bounded time slices so document writes and other builds can proceed.
type IndexBuildSchedulerOptions = database.IndexBuildSchedulerOptions

// IndexBuildSchedulerStats reports background index-build activity and outcomes.
type IndexBuildSchedulerStats = database.IndexBuildSchedulerStats

// IndexBuildScheduler runs resumable index builds in bounded background tasks.
type IndexBuildScheduler = database.IndexBuildScheduler

// IndexBuildID identifies one durable, resumable Storage  index build.
type IndexBuildID = database.IndexBuildID

// ParseIndexBuildID parses a hexadecimal index-build identifier.
func ParseIndexBuildID(value string) (IndexBuildID, error) {
	return database.ParseIndexBuildID(value)
}

// IndexBuildPhase identifies the current stage of a resumable index build.
type IndexBuildPhase = database.IndexBuildPhase

// IndexBuildFailure identifies why an index build stopped permanently.
type IndexBuildFailure = database.IndexBuildFailure

// IndexBuildPhaseScan marks a build scanning its initial document snapshot.
const IndexBuildPhaseScan = database.IndexBuildPhaseScan

// IndexBuildPhaseCatchUp marks a build applying changes after its initial snapshot.
const IndexBuildPhaseCatchUp = database.IndexBuildPhaseCatchUp

// IndexBuildPhaseReady marks a build ready for publication.
const IndexBuildPhaseReady = database.IndexBuildPhaseReady

// IndexBuildPhaseFailed marks a build that stopped with a terminal failure.
const IndexBuildPhaseFailed = database.IndexBuildPhaseFailed

// IndexBuildFailureNone marks the absence of an index-build failure.
const IndexBuildFailureNone = database.IndexBuildFailureNone

// IndexBuildFailureUniqueConflict marks a duplicate key found while building a unique index.
const IndexBuildFailureUniqueConflict = database.IndexBuildFailureUniqueConflict

// IndexBuildFailureResourceLimit marks an index build that exceeded a resource limit.
const IndexBuildFailureResourceLimit = database.IndexBuildFailureResourceLimit

// IndexBuildFailureHistoryLost marks a build whose required change history was lost.
const IndexBuildFailureHistoryLost = database.IndexBuildFailureHistoryLost

// IndexBuildFailureCanceled marks a canceled index build.
const IndexBuildFailureCanceled = database.IndexBuildFailureCanceled

// IndexBuildFailureInvalidIndex marks a build with invalid index data.
const IndexBuildFailureInvalidIndex = database.IndexBuildFailureInvalidIndex

// IndexBuildStatus reports the stored progress and outcome of a resumable index build.
//
// EntryCount and CanonicalBytes describe the pending index tree, not transient
// Go heap usage.
type IndexBuildStatus = database.IndexBuildStatus

// IndexCatalogEntry describes a published index without exposing document data.
//
// Index management remains a deployment concern; this catalog supports CLIs
// and protected operator views.
type IndexCatalogEntry = database.IndexCatalogEntry

// IndexField selects a field and its sort direction within an index.
//
// Order must be 1 (ascending) or -1 (descending); fields are evaluated left to right.
type IndexField = database.IndexField

// IndexOptions controls complete-tuple uniqueness.
type IndexOptions = database.IndexOptions

// ExplainResult describes the query plan and the work measured during execution.
//
// Estimated fields are
// conservative candidate estimates when the selected backend can provide them;
// DocumentsExamined and KeysExamined are the actual completed scan counts.
// ResidualPredicate means the complete compiled predicate is rechecked after
// index admission, including when the index appears to cover every condition.
type ExplainResult = database.ExplainResult

// ExplainBound describes the values that restrict one indexed field.
//
// Values is used for equality and membership unions; range
// bounds use Lower and Upper. A nil range endpoint is unbounded.
type ExplainBound = database.ExplainBound

// ExplainAccessSource records the query work performed through one index or primary key path.
//
// CandidateIDs counts document IDs processed by the
// deduplicator. Read-ahead needed for an ordered union remains visible in
// KeysExamined even when execution stops before those IDs are consumed.
type ExplainAccessSource = database.ExplainAccessSource

// ExplainBudget is the final resource-budget snapshot for one execution.
//
// Pressure and Exceeded are fixed reason codes naming one of "documents",
// "keys", "candidates", "sort_bytes", "skip", or "predicate_steps".
type ExplainBudget = database.ExplainBudget

// ExplainAdvice records a possible query improvement to check against the workload.
//
// It is not an instruction to create an index. Paths and Sort contain schema facts but
// never query values. Callers should validate selectivity against workload
// measurements.
type ExplainAdvice = database.ExplainAdvice

// LogicalArchiveResult records archive counts, byte size, and checksum.
//
// SHA256
// covers every JSONL record before the final end record; the end record stores
// the same digest so an importer can reject truncation or alteration.
type LogicalArchiveResult = database.LogicalArchiveResult

// LogicalArchiveImportOptions limits the size of an archive accepted for import.
//
// Zero MaxBytes selects the normal storage-file limit; the receiver owns this cap.
type LogicalArchiveImportOptions = database.LogicalArchiveImportOptions

// ImportLogicalArchive creates a new database from a validated logical archive.
//
// Import and offline verification run in a private temporary database before
// atomic publication, so a malformed archive leaves no destination database.
func ImportLogicalArchive(ctx context.Context, source io.Reader, destination string, options LogicalArchiveImportOptions) (result LogicalArchiveResult, resultErr error) {
	return database.ImportLogicalArchive(ctx, source, destination, options)
}

// MaintenanceOptions sets the schedule and limits for background page reclamation.
//
// Each loop uses online optimistic reclamation and never overlaps its own runs.
type MaintenanceOptions = database.MaintenanceOptions

// MaintenanceStats counts background page reclamation runs and records their outcomes.
type MaintenanceStats = database.MaintenanceStats

// Maintenance runs background tasks that make unused database pages reusable.
//
// Stop waits for an active scan to observe cancellation; closing the database
// also cancels the loop.
type Maintenance = database.Maintenance

// DecodeMutationSpecJSON validates and decodes a data-only mutation within the supplied limits.
func DecodeMutationSpecJSON(data []byte, limits QueryLimits) (MutationSpec, error) {
	return database.DecodeMutationSpecJSON(data, limits)
}

// DBStats is a point-in-time, allocation-bounded view of database health.
//
// Counters are process-lifetime values and reset when the database is reopened.
// Persistent state such as CommitSequence is read from the database itself.
//
// Stats deliberately exposes no user values, document IDs, query parameters, or
// callbacks. It is safe for an admin sampler to call periodically, but it is not
// intended to be called on every database operation.
type DBStats = database.DBStats

// ResourceStats reports configured resource limits and rejection counts.
type ResourceStats = database.ResourceStats

// IndexBuildStats reports index-build progress, resource use, and outcomes.
type IndexBuildStats = database.IndexBuildStats

// CommitStats counts committed batches and their changes.
type CommitStats = database.CommitStats

// DurabilityStats preserves legacy durability counters in the admin response format.
//
// Current-format databases do not use a WAL or checkpoints, so every field is zero.
type DurabilityStats = database.DurabilityStats

// CommitCoordinatorStats reports queued writes, admission decisions, and commit groups.
//
// DBStats and the versioned admin schema expose these bounded counters so
// applications can alert on queue pressure without inspecting the queue.
type CommitCoordinatorStats = database.CommitCoordinatorStats

// PrimaryWriteFenceStats reports whether write protection is active and counts its checks and rejections.
//
// Configured means a guard was supplied at open;
// Enforced is false while a read-only follower applies validated source
// history. Checks and Rejected count only actual primary write admissions.
// No lease, epoch, endpoint, database ID, or controller detail is exposed.
type PrimaryWriteFenceStats = database.PrimaryWriteFenceStats

// WriteTransactionStats counts active transactions and their outcomes.
//
// Every started callback reaches exactly one terminal counter. These aggregates do
// not contain collection, document, actor, or callback identifiers.
type WriteTransactionStats = database.WriteTransactionStats

// QueryStats counts query activity and execution work during the current process session.
//
// Physical key and candidate counters include work completed before a failed query; the
// older document/return counters retain their successful-query contract.
type QueryStats = database.QueryStats

// RealtimeStats reports live query activity, queued work, and overloads.
type RealtimeStats = database.RealtimeStats

// StorageStats describes the selected physical backend.
//
// Session counters reset
// on reopen; physical state and cache counters come from the backend itself.
type StorageStats = database.StorageStats

// PageCacheStats reports page-cache occupancy, hits, misses, and evictions.
type PageCacheStats = database.PageCacheStats

// DocumentCacheStats reports document-cache occupancy, hits, misses, and evictions.
type DocumentCacheStats = database.DocumentCacheStats

// CompactionStats reports compaction activity, byte counts, and outcomes.
type CompactionStats = database.CompactionStats

// ReclamationStats reports page-reclamation activity and outcomes.
type ReclamationStats = database.ReclamationStats

// BackupStats reports backup activity, sizes, and outcomes.
type BackupStats = database.BackupStats

// QueryLimits bounds the size and complexity of accepted queries.
type QueryLimits = database.QueryLimits

// DefaultQueryLimits provides the default query input limits.
var DefaultQueryLimits = database.DefaultQueryLimits

// SortField specifies a document path and its ascending or descending sort direction.
type SortField = database.SortField

// QuerySpec holds a compiled filter with sorting and pagination.
type QuerySpec = database.QuerySpec

// QueryDeltaOperationKind identifies a change to an ordered query result.
type QueryDeltaOperationKind = database.QueryDeltaOperationKind

// QueryDeltaRemove removes a document from an ordered result.
const QueryDeltaRemove = database.QueryDeltaRemove

// QueryDeltaAdd inserts a document before an anchor.
const QueryDeltaAdd = database.QueryDeltaAdd

// QueryDeltaMove moves a document before an anchor.
const QueryDeltaMove = database.QueryDeltaMove

// QueryDeltaChange replaces a document without changing its position.
const QueryDeltaChange = database.QueryDeltaChange

// QueryDeltaOperation mutates an ordered query result.
//
// A zero BeforeID means
// the end of the result; database document IDs are never zero.
type QueryDeltaOperation = database.QueryDeltaOperation

// QueryDelta transforms exactly FromToken into Token.
//
// Operations are ordered:
// removals first, followed by reverse-order add/move anchors and document
// changes. Applying them in slice order is deterministic.
type QueryDelta = database.QueryDelta

// ApplyQueryDelta strictly validates and applies an ordered delta without
// mutating the input snapshot.
func ApplyQueryDelta(snapshot QuerySnapshot, delta QueryDelta) (QuerySnapshot, error) {
	return database.ApplyQueryDelta(snapshot, delta)
}

// QueryReplaySource atomically reconstructs a query at afterToken and tails
// later ordered revisions.
//
// Initial.Token must equal afterToken. Implementations
// return ErrHistoryLost when retention can no longer satisfy that contract.
type QueryReplaySource = database.QueryReplaySource

// QueryReplaySubscription provides a historical query snapshot and its later changes.
type QueryReplaySubscription = database.QueryReplaySubscription

// DecodeQuerySpecJSON validates and decodes a data-only query within the supplied limits.
func DecodeQuerySpecJSON(data []byte, limits QueryLimits) (QuerySpec, error) {
	return database.DecodeQuerySpecJSON(data, limits)
}

// MarshalQuerySpecJSON validates a query and encodes its canonical JSON representation.
//
// The encoded query is used for transport fingerprints and cross-language conformance.
func MarshalQuerySpecJSON(query QuerySpec) ([]byte, error) {
	return database.MarshalQuerySpecJSON(query)
}

// ValidateStrictJSON rejects oversized, trailing, deeply nested, and
// duplicate-key JSON before a transport decodes it into structs or maps.
func ValidateStrictJSON(data []byte, maxBytes int) error {
	return database.ValidateStrictJSON(data, maxBytes)
}

// MarshalWireValue encodes a typed value as wire JSON.
func MarshalWireValue(value Value) ([]byte, error) {
	return database.MarshalWireValue(value)
}

// UnmarshalWireValue decodes a typed JSON value within the supplied query limits.
//
// It applies the same nesting, item count, and size limits as query operands.
// Protocol arguments such as RPC inputs contain data only; decoding never runs code.
func UnmarshalWireValue(data []byte, limits QueryLimits) (Value, error) {
	return database.UnmarshalWireValue(data, limits)
}

// MarshalWireDocument encodes a document as typed wire JSON.
func MarshalWireDocument(document Document) ([]byte, error) {
	return database.MarshalWireDocument(document)
}

// UnmarshalWireDocument decodes a typed wire document with a required document ID.
func UnmarshalWireDocument(data []byte, limits QueryLimits) (Document, error) {
	return database.UnmarshalWireDocument(data, limits)
}

// UnmarshalWireInputDocument decodes a typed wire document whose ID may be omitted.
func UnmarshalWireInputDocument(data []byte, limits QueryLimits) (Document, error) {
	return database.UnmarshalWireInputDocument(data, limits)
}

// QuerySnapshot pairs an ordered query result with its commit token.
type QuerySnapshot = database.QuerySnapshot

// QuerySubscription provides full query snapshots as the result changes.
type QuerySubscription = database.QuerySubscription

// QueryDeltaSubscription returns one safe initial snapshot and then ordered
// deltas.
//
// It is the preferred core stream for transports and reactive clients;
// QuerySubscription remains the full-snapshot compatibility adapter.
type QueryDeltaSubscription = database.QueryDeltaSubscription

// ReclaimResult reports reachable and reusable pages after reclamation.
type ReclaimResult = database.ReclaimResult

// ReclaimOptions configures page-reclamation mode, retries, and persistence.
//
// Online scans a duplicate read handle without the storage writer lock and installs its result
// only if the Meta generation is unchanged. MaxAttempts bounds complete graph
// rescans after concurrent commits; zero selects three attempts.
type ReclaimOptions = database.ReclaimOptions

// RecoveryMode selects whether startup may perform bounded recovery actions.
//
// Zero selects automatic recovery; RecoveryReport describes completed actions.
type RecoveryMode = database.RecoveryMode

// RecoveryAutomatic allows bounded automatic recovery when opening a database.
const RecoveryAutomatic = database.RecoveryAutomatic

// RecoveryRequireClean rejects opening a database that needs recovery.
const RecoveryRequireClean = database.RecoveryRequireClean

// OpenOptions configures the current durable storage format.
type OpenOptions = database.OpenOptions

// PrimaryWriteFence checks whether an external authority still permits a local write.
//
// Its implementation normally checks an atomically
// refreshed lease epoch and expiry, not the network. Returning an error rejects
// the whole logical commit before storage mutation; it never poisons the
// database or advances a token.
//
// Implementations must not call back into DB and must return promptly: the
// check runs while the writer has admitted a commit. Election, renewal,
// certificate rotation and old-primary revocation remain external concerns.
type PrimaryWriteFence = database.PrimaryWriteFence

// PrimaryWriteFenceRequest identifies the database and next commit requiring write authority.
//
// A lease implementation must
// reject when its external authority/epoch/expiry no longer permits that write.
type PrimaryWriteFenceRequest = database.PrimaryWriteFenceRequest

// CommitCoordinatorOptions sets the queue and batch limits for optional grouped writes.
//
// Grouping is disabled by default.
//
// A coordinator group publishes one stored database version but retains one
// logical commit token for every admitted write request, including prepared
// public write transactions. Index builds and other maintenance operations
// remain exclusive commits. When rollback protection is configured, the
// coordinator advances the external anchor after the group's stored version
// is durable and before acknowledging any member.
type CommitCoordinatorOptions = database.CommitCoordinatorOptions

// DefaultCommitCoordinatorMaxBatch sets the default maximum number of writes in a commit group.
const DefaultCommitCoordinatorMaxBatch = database.DefaultCommitCoordinatorMaxBatch

// DefaultCommitCoordinatorMaxPending sets the default maximum number of queued writes.
const DefaultCommitCoordinatorMaxPending = database.DefaultCommitCoordinatorMaxPending

// DefaultCommitCoordinatorMaxDelay sets the default wait for grouping queued writes.
const DefaultCommitCoordinatorMaxDelay = database.DefaultCommitCoordinatorMaxDelay

// RollbackAnchor records trusted minimum commit and generation values outside the database.
//
// A server must never accept the same identity below either an acknowledged
// logical commit sequence or physical maintenance generation after restart.
// The coordinates are independently monotonic: one group may advance several
// logical sequences while publishing a single physical generation.
type RollbackAnchor = database.RollbackAnchor

// RollbackAnchorStore loads and durably advances a database's trusted rollback boundary.
//
// Advance must not return until the anchor is persistent and
// must reject identity changes or regression of either monotonic coordinate.
// Implementations must be safe for concurrent callers and honor cancellation.
// An Advance error does not prove that state was unchanged: persistence may
// have completed before a response, deadline or cancellation was observed.
type RollbackAnchorStore = database.RollbackAnchorStore

// RollbackAnchorStoreStatus reports anchor backend counters without exposing database identities.
//
// These diagnostic counters never participate in recovery.
type RollbackAnchorStoreStatus = database.RollbackAnchorStoreStatus

// RollbackAnchorStatusProvider exposes anchor store counters without acquiring locks.
type RollbackAnchorStatusProvider = database.RollbackAnchorStatusProvider

// RollbackProtection configures identity and progress checks that reject database rollback.
//
// AnchorStore should live on an independently trusted device or remote
// quorum; placing it beside the database cannot detect whole-device rollback.
// InitializeAnchor explicitly trusts the database currently at Path when the
// store is empty and should only be used during provisioning or audited restore.
type RollbackProtection = database.RollbackProtection

// DefaultRollbackAnchorOperationTimeout limits how long a rollback anchor operation may delay a commit response.
const DefaultRollbackAnchorOperationTimeout = database.DefaultRollbackAnchorOperationTimeout

// StorageLimits caps the physical database file size.
//
// Zero selects DefaultMaxFileBytes; the limit must be a multiple of the 16 KiB page size.
type StorageLimits = database.StorageLimits

// PageSize sets the byte size of each storage page.
const PageSize = database.PageSize

// DefaultMaxFileBytes sets the default physical database file size limit.
const DefaultMaxFileBytes = database.DefaultMaxFileBytes

// CompactionOptions sets storage and resource limits for a rewritten database.
//
// ResourceLimits govern transient index construction as well as the reopened
// destination handle; zero fields select production defaults.
type CompactionOptions = database.CompactionOptions

// CommitRetentionPolicy limits retained history by commit count and encoded byte size.
//
// Zero fields select production defaults.
// Active replay leases may temporarily exceed either budget rather than losing
// history under a reader.
type CommitRetentionPolicy = database.CommitRetentionPolicy

// DefaultCommitRetentionMaxCommits sets the default retained commit count limit.
const DefaultCommitRetentionMaxCommits = database.DefaultCommitRetentionMaxCommits

// DefaultCommitRetentionMaxBytes sets the default retained commit payload limit.
const DefaultCommitRetentionMaxBytes = database.DefaultCommitRetentionMaxBytes

// DefaultReplayDeliveryTimeout limits how long replay waits to deliver into a full buffer.
//
// A delivery timeout releases the retained-history lease.
const DefaultReplayDeliveryTimeout = database.DefaultReplayDeliveryTimeout

// RecoveryReport records completed startup recovery actions without exposing sensitive data.
//
// It reports only actions that were completed before
// Open returned successfully; corruption and unsupported formats still fail
// Open instead of being described as recovered.
type RecoveryReport = database.RecoveryReport

// ReplicationSourceLease reserves a replica consumer for one session in this process.
//
// It prevents sessions from racing the same checkpoint and does not replace
// external primary election or follower-promotion fencing.
type ReplicationSourceLease = database.ReplicationSourceLease

// ReplicationSourceSession sends committed database changes to one authenticated receiver.
//
// Only one batch may await acknowledgement at a time, limiting queued work
// and preventing acknowledgement of an unseen source token.
type ReplicationSourceSession = database.ReplicationSourceSession

// NewReplicationSourceSession creates a replication session from a named database change feed.
//
// The caller authenticates the receiver and must close the session when
// that authenticated connection ends.
func NewReplicationSourceSession(db *DB, subscription *DurableDatabaseChangeSubscription, limits ReplicationFrameLimits) (*ReplicationSourceSession, error) {
	return database.NewReplicationSourceSession(db, subscription, limits)
}

// ReplicationProtocolVersion identifies the protocol for replicating commits between trusted servers.
//
// This version is separate from the browser realtime subscription protocol.
const ReplicationProtocolVersion = database.ReplicationProtocolVersion

// DefaultReplicationMaxFrameBytes sets the default replication frame byte limit.
const DefaultReplicationMaxFrameBytes = database.DefaultReplicationMaxFrameBytes

// ReplicationFrameLimits caps the size of a decompressed replication message.
//
// The
// default accommodates the configured 64 MiB canonical transaction limit plus
// JSON/base64 overhead, while still rejecting unbounded peer allocation.
type ReplicationFrameLimits = database.ReplicationFrameLimits

// ReplicationFrame carries a handshake, change batch, acknowledgement, or resync request.
//
// A transport must
// authenticate both peers (for example with mTLS) before it accepts frames;
// DatabaseID binds every frame to one durable source identity.
type ReplicationFrame = database.ReplicationFrame

// ReplicationHelloFrame identifies a replication handshake frame.
const ReplicationHelloFrame = database.ReplicationHelloFrame

// ReplicationBatchFrame identifies a replication change-batch frame.
const ReplicationBatchFrame = database.ReplicationBatchFrame

// ReplicationAckFrame identifies a durable replication acknowledgment.
const ReplicationAckFrame = database.ReplicationAckFrame

// ReplicationResyncFrame identifies a request to rebuild a replica from a snapshot.
const ReplicationResyncFrame = database.ReplicationResyncFrame

// MarshalReplicationFrame validates and encodes a replication message as bounded JSON.
//
// Document images use base64 of the storage-independent typed document codec.
// It is suitable for WebSocket binary/text messages, QUIC streams or framed
// RPC, but it does not provide authentication or encryption itself.
func MarshalReplicationFrame(frame ReplicationFrame, limits ReplicationFrameLimits) ([]byte, error) {
	return database.MarshalReplicationFrame(frame, limits)
}

// UnmarshalReplicationFrame validates and decodes a bounded JSON replication message.
//
// It rejects unknown fields, duplicate keys, malformed base64, invalid typed
// documents, and non-canonical identities before the follower receives a batch.
func UnmarshalReplicationFrame(data []byte, limits ReplicationFrameLimits) (ReplicationFrame, error) {
	return database.UnmarshalReplicationFrame(data, limits)
}

// DefaultMaxDocumentBytes sets the default canonical document byte limit.
const DefaultMaxDocumentBytes = database.DefaultMaxDocumentBytes

// DefaultMaxTransactionBytes sets the default canonical transaction byte limit.
const DefaultMaxTransactionBytes = database.DefaultMaxTransactionBytes

// DefaultMaxTransactionChanges sets the default transaction change count limit.
const DefaultMaxTransactionChanges = database.DefaultMaxTransactionChanges

// DefaultMaxIndexBuildEntries sets the default index-build entry count limit.
const DefaultMaxIndexBuildEntries = database.DefaultMaxIndexBuildEntries

// DefaultMaxIndexBuildBytes sets the default index-build byte limit.
const DefaultMaxIndexBuildBytes = database.DefaultMaxIndexBuildBytes

// DefaultMaxReactiveViewDocuments sets the default number of document versions retained by a live query.
//
// Views retain matching versions for incremental ordering and updates,
// not merely the page currently emitted to a subscriber.
const DefaultMaxReactiveViewDocuments = database.DefaultMaxReactiveViewDocuments

// DefaultMaxReactiveViewBytes sets the default retained live query byte limit.
const DefaultMaxReactiveViewBytes = database.DefaultMaxReactiveViewBytes

// DefaultMaxQueryDocumentsExamined sets the default examined document limit per query.
const DefaultMaxQueryDocumentsExamined = database.DefaultMaxQueryDocumentsExamined

// DefaultMaxQueryKeysExamined sets the default examined index key limit per query.
const DefaultMaxQueryKeysExamined = database.DefaultMaxQueryKeysExamined

// DefaultMaxQueryCandidates sets the default retained query candidate limit.
const DefaultMaxQueryCandidates = database.DefaultMaxQueryCandidates

// DefaultMaxQuerySortBytes sets the default retained sort data byte limit.
const DefaultMaxQuerySortBytes = database.DefaultMaxQuerySortBytes

// DefaultMaxQuerySkip sets the default query offset limit.
const DefaultMaxQuerySkip = database.DefaultMaxQuerySkip

// ResourceLimits bounds work admitted by writes, index maintenance, and query execution.
//
// Zero values select production defaults; limits cannot be disabled accidentally. Byte
// limits use the canonical typed binary representation, independent of Go heap
// layout, JSON spelling, storage generation, or transport compression.
type ResourceLimits = database.ResourceLimits

// DatabaseOptions configures an in-memory database.
type DatabaseOptions = database.DatabaseOptions

// NewFileRollbackAnchorStore creates a file-backed store for trusted database version limits.
//
// Updates replace the anchor atomically and reject invalid saved state.
// The parent directory must exist on storage trusted independently from the database.
func NewFileRollbackAnchorStore(path string) (RollbackAnchorStore, error) {
	return database.NewFileRollbackAnchorStore(path)
}

// StorageFormatInfo reports file format metadata and compatibility with this reader.
//
// ReaderCompatible says this binary understands the reported revision and all
// required feature bits; callers must still Open the database before use.
type StorageFormatInfo = database.StorageFormatInfo

// StorageFormat identifies the supported file format or an absent database.
//
// Unknown denotes
// a missing or zero-length path, not an unrecognized non-empty file.
type StorageFormat = database.StorageFormat

// StorageFormatUnknown identifies a missing or empty database file.
const StorageFormatUnknown = database.StorageFormatUnknown

// StorageFormatCurrent identifies the supported database file format.
const StorageFormatCurrent = database.StorageFormatCurrent

// DetectStorageFormat distinguishes an absent database from a recognized database format.
//
// Old database files deliberately fail
// closed: this build contains no legacy reader or automatic migration path.
func DetectStorageFormat(path string) (StorageFormat, error) {
	return database.DetectStorageFormat(path)
}

// InspectStorageFormat validates current Meta checksums and reports its newest
// readable envelope without opening the database for mutation.
func InspectStorageFormat(path string) (StorageFormatInfo, error) {
	return database.InspectStorageFormat(path)
}

// Update maps update operators to their requested field changes.
type Update = database.Update

// UpdateResult reports how many documents matched and changed.
type UpdateResult = database.UpdateResult

// MutationSpec holds a compiled document update.
type MutationSpec = database.MutationSpec

// CompileUpdate validates update operators and produces a reusable mutation.
func CompileUpdate(update Update) (MutationSpec, error) {
	return database.CompileUpdate(update)
}

// Kind identifies the type stored in a Value.
type Kind = database.Kind

// NullKind identifies a null value.
const NullKind = database.NullKind

// BoolKind identifies a boolean value.
const BoolKind = database.BoolKind

// Int64Kind identifies a signed 64-bit integer.
const Int64Kind = database.Int64Kind

// Float64Kind identifies a 64-bit float.
const Float64Kind = database.Float64Kind

// StringKind identifies a string.
const StringKind = database.StringKind

// BinaryKind identifies binary data.
const BinaryKind = database.BinaryKind

// TimeKind identifies a timestamp.
const TimeKind = database.TimeKind

// ArrayKind identifies an array of values.
const ArrayKind = database.ArrayKind

// ObjectKind identifies a document object.
const ObjectKind = database.ObjectKind

// IDKind identifies a document identifier.
const IDKind = database.IDKind

// DocumentID is a 16-byte document identifier.
type DocumentID = database.DocumentID

// NewDocumentID creates an identifier from cryptographically random bytes.
func NewDocumentID() (DocumentID, error) {
	return database.NewDocumentID()
}

// ParseDocumentID reads a 16-byte identifier from a hexadecimal string.
func ParseDocumentID(s string) (DocumentID, error) {
	return database.ParseDocumentID(s)
}

// Value stores typed database data behind a private representation.
//
// Private fields prevent callers from pairing a type with an incompatible payload.
type Value = database.Value

// Null creates a null value.
func Null() Value {
	return database.Null()
}

// Bool creates a boolean value.
func Bool(v bool) Value {
	return database.Bool(v)
}

// Int creates a signed 64-bit integer value.
func Int(v int64) Value {
	return database.Int(v)
}

// Float creates a 64-bit floating-point value.
func Float(v float64) Value {
	return database.Float(v)
}

// String creates a string value.
func String(v string) Value {
	return database.String(v)
}

// Binary creates a binary value from a copy of the bytes.
func Binary(v []byte) Value {
	return database.Binary(v)
}

// Time stores millisecond precision, matching JavaScript Date and the wire
// contract.
//
// Precision is normalized at construction rather than silently lost
// during transport.
func Time(v time.Time) Value {
	return database.Time(v)
}

// ID creates a document identifier value.
func ID(v DocumentID) Value {
	return database.ID(v)
}

// Array creates an array value from copies of its elements.
func Array(v ...Value) Value {
	return database.Array(v...)
}

// Object creates an object value from a copy of the document.
func Object(v Document) Value {
	return database.Object(v)
}

// ValueOf converts a supported Go value into a database value with copied nested data.
func ValueOf(x any) (Value, error) {
	return database.ValueOf(x)
}

// VerificationReport records the results of a read-only page and index audit.
//
// ReclaimablePages is informational; verification never installs a free pool or publishes
// maintenance metadata.
type VerificationReport = database.VerificationReport

// VerifyFile checks an existing database's protected pages and index contents without changing it.
//
// It takes a non-blocking shared advisory lock, so an active writer fails with
// ErrDatabaseLocked. It never creates, truncates, repairs, reclaims, or advances
// the database. Meta inspection alone is cheaper; this method walks every page
// protected by both valid Meta roots, recomputes published and provable shadow
// Secondary keys from canonical Primary documents in both directions, and
// hashes the file. Legacy caught-up builds lacking an applied CatalogRoot remain
// readable but report IndexBuildContentsVerified=false.
func VerifyFile(ctx context.Context, path string) (VerificationReport, error) {
	return database.VerifyFile(ctx, path)
}

// WriteTransaction stages reads and writes against a snapshot for an atomic commit.
//
// Commit validation detects conflicting changes, and returned values are independent copies.
//
// A transaction is active only during its handler callback. Handlers must not
// retain it or call normal DB/Collection methods from inside the callback.
type WriteTransaction = database.WriteTransaction
