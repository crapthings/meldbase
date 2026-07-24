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

// OperationalState is a minimal, allocation-free serving-state snapshot. A
// fail-stop durability error preserves reads from the last committed state but
// disables writes; a closed database is neither readable nor writable.
type OperationalState = database.OperationalState

type BackupResult = database.BackupResult

// PhysicalBackupImportOptions bounds an untrusted physical-backup stream
// before it can consume local disk. Zero selects the normal file limit.
// Deployments with a deliberately larger database must set MaxBytes
// explicitly on the receiving side; a sender never chooses that authority.
type PhysicalBackupImportOptions = database.PhysicalBackupImportOptions

// ImportPhysicalBackup receives one exact Backup artifact into a new local
// path. It writes a private temporary file, checks the claimed byte count and
// SHA-256 while streaming, runs the complete offline  graph/index verifier,
// then publishes with the same no-overwrite link-and-directory-sync commit
// point as backup and migration.
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

type Operation = database.Operation

const InsertOperation = database.InsertOperation

const UpdateOperation = database.UpdateOperation

const DeleteOperation = database.DeleteOperation

// CreateCollectionOperation is emitted only by the durable database change
// feed. Ordinary collection creation remains implicit for CRUD callers.
const CreateCollectionOperation = database.CreateCollectionOperation

const CreateIndexOperation = database.CreateIndexOperation

type IndexDefinition = database.IndexDefinition

type Change = database.Change

type ChangeBatch = database.ChangeBatch

type DB = database.DB

func New() *DB {
	return database.New()
}

// NewWithOptions creates an in-memory database with explicit resource limits.
func NewWithOptions(options DatabaseOptions) (*DB, error) {
	return database.NewWithOptions(options)
}

type Collection = database.Collection

type DeleteResult = database.DeleteResult

type Cursor = database.Cursor

var ErrDiagnosticsActive = database.ErrDiagnosticsActive

type DiagnosticKind = database.DiagnosticKind

const DiagnosticQuery = database.DiagnosticQuery

const DiagnosticCommit = database.DiagnosticCommit

type DiagnosticOutcome = database.DiagnosticOutcome

const DiagnosticSuccess = database.DiagnosticSuccess

const DiagnosticFailure = database.DiagnosticFailure

const DiagnosticCanceled = database.DiagnosticCanceled

// DiagnosticsOptions controls opt-in detailed events. Defaults retain 256
// events and record failed, >=50ms queries and >=100ms durable commits. Setting
// RecordAll is intended only for short development sessions. SampleEvery adds a
// deterministic one-in-N sample of otherwise fast successful operations.
type DiagnosticsOptions = database.DiagnosticsOptions

type DiagnosticEvent = database.DiagnosticEvent

type DiagnosticStats = database.DiagnosticStats

type DiagnosticSnapshot = database.DiagnosticSnapshot

// Diagnostics owns a fixed-capacity event ring. Close disables future timing
// and recording but keeps the retained snapshot readable by its owner.
type Diagnostics = database.Diagnostics

type Document = database.Document

func NewDocument(fields map[string]any) (Document, error) {
	return database.NewDocument(fields)
}

// Open creates or opens a Meldbase durable database in the current format.
func Open(path string) (*DB, error) {
	return database.Open(path)
}

func OpenWithOptions(path string, options OpenOptions) (*DB, error) {
	return database.OpenWithOptions(path, options)
}

// DurableChangeBatch is one globally ordered Commit Log position projected to
// one collection. Changes is empty when another collection or private catalog
// change advanced the durable position; callers must still Ack that Token after
// processing it so the checkpoint can advance without pinning history forever.
//
// This is deliberately a document-change feed, not a full replication protocol:
// it does not expose private System records, index definitions, collection
// lifecycle or raw storage bytes.
type DurableChangeBatch = database.DurableChangeBatch

// DurableChangeSubscription is a pull/acknowledge bridge over a  durable
// checkpoint. Batches remain ordered. Ack must be called only after the
// consumer's external side effect for that token is durable.
type DurableChangeSubscription = database.DurableChangeSubscription

// DurableDatabaseChangeBatch is one globally ordered  Commit Log position
// projected into public document and catalog events. It is the semantic source
// for archive and single-writer-follower protocols: callers must Ack only
// after the externally applied effect for Token is durable.
//
// Private System records, raw pages, index-build progress and retention
// control records are deliberately excluded. A batch can therefore be empty
// when a private record advanced a retained position; it must still be Acked.
type DurableDatabaseChangeBatch = database.DurableDatabaseChangeBatch

// DurableDatabaseChangeSubscription is a crash-resumable pull/acknowledge
// feed over the complete public database. It exposes collection creation,
// index publication and document changes in exact Commit Log order. It does
// not itself copy a bootstrap snapshot or apply changes to a follower; those
// transport and ownership contracts are intentionally separate.
type DurableDatabaseChangeSubscription = database.DurableDatabaseChangeSubscription

var ErrClosed = database.ErrClosed

var ErrInvalidDocument = database.ErrInvalidDocument

var ErrInvalidFilter = database.ErrInvalidFilter

var ErrInvalidUpdate = database.ErrInvalidUpdate

var ErrMutationLimit = database.ErrMutationLimit

var ErrWriteConflict = database.ErrWriteConflict

var ErrWriteTransactionUnsupported = database.ErrWriteTransactionUnsupported

var ErrNotFound = database.ErrNotFound

var ErrDuplicateID = database.ErrDuplicateID

var ErrInvalidCollection = database.ErrInvalidCollection

var ErrImmutableID = database.ErrImmutableID

var ErrSlowConsumer = database.ErrSlowConsumer

var ErrCorrupt = database.ErrCorrupt

var ErrUnsupportedFormat = database.ErrUnsupportedFormat

var ErrDuplicateKey = database.ErrDuplicateKey

var ErrInvalidIndex = database.ErrInvalidIndex

var ErrCompoundIndexUnsupported = database.ErrCompoundIndexUnsupported

var ErrDurability = database.ErrDurability

var ErrInvalidDelta = database.ErrInvalidDelta

var ErrHistoryLost = database.ErrHistoryLost

var ErrDestinationExists = database.ErrDestinationExists

var ErrCompactionUnsupported = database.ErrCompactionUnsupported

var ErrCompactionDestinationExists = database.ErrCompactionDestinationExists

var ErrReclamationUnsupported = database.ErrReclamationUnsupported

var ErrInvalidReclamationOptions = database.ErrInvalidReclamationOptions

var ErrReclamationConflict = database.ErrReclamationConflict

var ErrBackupUnsupported = database.ErrBackupUnsupported

var ErrBackupDestinationExists = database.ErrBackupDestinationExists

var ErrLogicalArchiveUnsupported = database.ErrLogicalArchiveUnsupported

var ErrLogicalArchiveDestinationExists = database.ErrLogicalArchiveDestinationExists

var ErrVerificationUnsupported = database.ErrVerificationUnsupported

var ErrDatabaseLocked = database.ErrDatabaseLocked

var ErrRollbackDetected = database.ErrRollbackDetected

var ErrDatabaseIdentity = database.ErrDatabaseIdentity

var ErrInsecureFileMode = database.ErrInsecureFileMode

var ErrRollbackAnchorRequired = database.ErrRollbackAnchorRequired

var ErrRollbackAnchor = database.ErrRollbackAnchor

var ErrInvalidRollbackProtection = database.ErrInvalidRollbackProtection

var ErrRecoveryRequired = database.ErrRecoveryRequired

var ErrInvalidResourceLimits = database.ErrInvalidResourceLimits

var ErrResourceLimit = database.ErrResourceLimit

// ErrQueryBudget reports that one query exceeded an execution-work budget.
// Callers can use errors.Is to distinguish this from invalid query input.
var ErrQueryBudget = database.ErrQueryBudget

var ErrIndexBuildUnsupported = database.ErrIndexBuildUnsupported

var ErrIndexBuildNotFound = database.ErrIndexBuildNotFound

var ErrIndexBuildExists = database.ErrIndexBuildExists

var ErrIndexBuildFailed = database.ErrIndexBuildFailed

var ErrInvalidIndexBuildSchedulerOptions = database.ErrInvalidIndexBuildSchedulerOptions

var ErrIndexBuildSchedulerRunning = database.ErrIndexBuildSchedulerRunning

var ErrInvalidCommitCoordinatorOptions = database.ErrInvalidCommitCoordinatorOptions

var ErrInvalidReplayDeliveryTimeout = database.ErrInvalidReplayDeliveryTimeout

var ErrDurableConsumerUnsupported = database.ErrDurableConsumerUnsupported

var ErrDurableConsumerExists = database.ErrDurableConsumerExists

var ErrDurableConsumerNotFound = database.ErrDurableConsumerNotFound

var ErrReplicaReadOnly = database.ErrReplicaReadOnly

var ErrReplicaSequence = database.ErrReplicaSequence

var ErrReplicaProtocol = database.ErrReplicaProtocol

var ErrReplicaSourceActive = database.ErrReplicaSourceActive

var ErrReplicaPromotionAuthority = database.ErrReplicaPromotionAuthority

var ErrReplicaPromotionFence = database.ErrReplicaPromotionFence

var ErrReplicaPromotionWriteFence = database.ErrReplicaPromotionWriteFence

var ErrReplicaPromoted = database.ErrReplicaPromoted

var ErrPrimaryWriteFence = database.ErrPrimaryWriteFence

// ErrCommitOutcomeUnknown means cancellation or a lost caller connection
// raced an already-admitted durable write. Callers must reconcile by the
// returned document ID(s), rather than retrying business logic blindly.
var ErrCommitOutcomeUnknown = database.ErrCommitOutcomeUnknown

type Filter = database.Filter

type QueryOptions = database.QueryOptions

func CompileQuery(filter Filter, options QueryOptions) (QuerySpec, error) {
	return database.CompileQuery(filter, options)
}

// Follower owns a local, read-only database that advances only through
// validated DurableDatabaseChangeBatch values. It is the local application
// half of a future remote replication protocol; transport authentication,
// snapshot transfer and promotion deliberately remain outside this type.
type Follower = database.Follower

// FollowerPromotionRequest is the exact local state an external fencing
// system must certify before this process can become writable primary.
type FollowerPromotionRequest = database.FollowerPromotionRequest

// FollowerPromotionFence is a controller-issued, non-empty epoch proving the
// old primary's write authority was fenced for this database/token. Epoch is
// deliberately opaque to Meldbase: a controller may use it as an epoch ID or
// a compact signed lease certificate (as integrations/primarylease does).
// Meldbase does not invent a local substitute for that distributed safety
// decision.
type FollowerPromotionFence = database.FollowerPromotionFence

// FollowerPromotionAuthority must make its returned fence durable before it
// returns. Implementations normally revoke a primary lease through a quorum
// controller or external consensus store.
type FollowerPromotionAuthority = database.FollowerPromotionAuthority

// FollowerPromotionFenceBinder binds one controller-issued promotion fence to
// the local primary-write guard before a follower becomes writable. The
// binder may update caller-owned local lease/epoch state, but must not enable
// writes until it has accepted the exact fence. It runs on the promotion
// control path, outside the DB writer lock; unlike ValidatePrimaryWrite it
// may coordinate with the controller if the implementation needs to.
//
// A promoted follower requires this interface in addition to
// PrimaryWriteFence. Otherwise an unrelated always-allow guard could make a
// one-time promotion certificate appear to grant permanent write authority.
type FollowerPromotionFenceBinder = database.FollowerPromotionFenceBinder

// OpenFollower opens a physical archive/bootstrap copy as a replica. Normal
// public mutations on DB return ErrReplicaReadOnly; use Apply to advance the
// next source token. The returned DB remains fully queryable and reactive.
func OpenFollower(path string, options OpenOptions) (*Follower, error) {
	return database.OpenFollower(path, options)
}

// IndexBuildSchedulerOptions configures an explicit default-off runner. Each
// task receives a bounded time quantum, then yields durable progress so CRUD and
// other builds can proceed between quanta.
type IndexBuildSchedulerOptions = database.IndexBuildSchedulerOptions

type IndexBuildSchedulerStats = database.IndexBuildSchedulerStats

type IndexBuildScheduler = database.IndexBuildScheduler

// IndexBuildID identifies one durable, resumable Storage  index build.
type IndexBuildID = database.IndexBuildID

func ParseIndexBuildID(value string) (IndexBuildID, error) {
	return database.ParseIndexBuildID(value)
}

type IndexBuildPhase = database.IndexBuildPhase

type IndexBuildFailure = database.IndexBuildFailure

const IndexBuildPhaseScan = database.IndexBuildPhaseScan

const IndexBuildPhaseCatchUp = database.IndexBuildPhaseCatchUp

const IndexBuildPhaseReady = database.IndexBuildPhaseReady

const IndexBuildPhaseFailed = database.IndexBuildPhaseFailed

const IndexBuildFailureNone = database.IndexBuildFailureNone

const IndexBuildFailureUniqueConflict = database.IndexBuildFailureUniqueConflict

const IndexBuildFailureResourceLimit = database.IndexBuildFailureResourceLimit

const IndexBuildFailureHistoryLost = database.IndexBuildFailureHistoryLost

const IndexBuildFailureCanceled = database.IndexBuildFailureCanceled

const IndexBuildFailureInvalidIndex = database.IndexBuildFailureInvalidIndex

// IndexBuildStatus is durable progress. EntryCount and CanonicalBytes describe
// the current private Secondary tree, not transient Go heap usage.
type IndexBuildStatus = database.IndexBuildStatus

// IndexCatalogEntry is an immutable operator-facing description of one
// published index. It contains no document keys, values, or cardinalities.
// Index management remains a deployment concern; this is intentionally a
// read-only catalog for CLIs and protected operator surfaces.
type IndexCatalogEntry = database.IndexCatalogEntry

// IndexField is one ordered component of an index definition. Order must be 1
// (ascending) or -1 (descending); fields are evaluated left to right.
type IndexField = database.IndexField

// IndexOptions controls complete-tuple uniqueness.
type IndexOptions = database.IndexOptions

type ExplainResult = database.ExplainResult

type ExplainBound = database.ExplainBound

type ExplainAccessSource = database.ExplainAccessSource

type ExplainBudget = database.ExplainBudget

type ExplainAdvice = database.ExplainAdvice

// LogicalArchiveResult is the portable, data-only archive receipt. SHA256
// covers every JSONL record before the final end record; the end record stores
// the same digest so an importer can reject truncation or alteration.
type LogicalArchiveResult = database.LogicalArchiveResult

// LogicalArchiveImportOptions bounds an untrusted logical archive. Zero
// MaxBytes selects the normal storage-file limit; the receiver owns this cap.
type LogicalArchiveImportOptions = database.LogicalArchiveImportOptions

// ImportLogicalArchive validates and applies a portable archive into a private
// temporary database, verifies that database offline, then atomically publishes
// it at destination. A malformed archive never leaves a destination database.
func ImportLogicalArchive(ctx context.Context, source io.Reader, destination string, options LogicalArchiveImportOptions) (result LogicalArchiveResult, resultErr error) {
	return database.ImportLogicalArchive(ctx, source, destination, options)
}

// MaintenanceOptions configures an explicit default-off maintenance loop.
// Every run uses online optimistic reclamation; runs never overlap.
type MaintenanceOptions = database.MaintenanceOptions

type MaintenanceStats = database.MaintenanceStats

// Maintenance owns one background reclamation loop. Stop is idempotent and
// waits for an active scan to observe cancellation. Closing the DB also stops
// the loop through the DB lifecycle channel.
type Maintenance = database.Maintenance

func DecodeMutationSpecJSON(data []byte, limits QueryLimits) (MutationSpec, error) {
	return database.DecodeMutationSpecJSON(data, limits)
}

// DBStats is a point-in-time, allocation-bounded view of database health.
// Counters are process-lifetime values and reset when the database is reopened.
// Persistent state such as CommitSequence is read from the database itself.
//
// Stats deliberately exposes no user values, document IDs, query parameters, or
// callbacks. It is safe for an admin sampler to call periodically, but it is not
// intended to be called on every database operation.
type DBStats = database.DBStats

type ResourceStats = database.ResourceStats

type IndexBuildStats = database.IndexBuildStats

type CommitStats = database.CommitStats

// DurabilityStats is retained in the admin wire contract. Current-format
// databases do not use a WAL or checkpoints, so every field is zero.
type DurabilityStats = database.DurabilityStats

// CommitCoordinatorStats is a fixed-cardinality snapshot of the optional
//
//	write-admission scheduler. It is included in DBStats and the versioned
//
// admin schema, so applications can alert on admission pressure without
// inspecting a mutable queue or adding application labels.
type CommitCoordinatorStats = database.CommitCoordinatorStats

// PrimaryWriteFenceStats is a fixed-cardinality view of the optional
// external primary-write guard. Configured means a guard was supplied at open;
// Enforced is false while a read-only follower applies validated source
// history. Checks and Rejected count only actual primary write admissions.
// No lease, epoch, endpoint, database ID, or controller detail is exposed.
type PrimaryWriteFenceStats = database.PrimaryWriteFenceStats

// WriteTransactionStats describes public optimistic point transactions. Every
// started callback reaches exactly one terminal counter. These aggregates do
// not contain collection, document, actor, or callback identifiers.
type WriteTransactionStats = database.WriteTransactionStats

type QueryStats = database.QueryStats

type RealtimeStats = database.RealtimeStats

// StorageStats describes the selected physical backend. Session counters reset
// on reopen; physical state and cache counters come from the backend itself.
type StorageStats = database.StorageStats

type PageCacheStats = database.PageCacheStats

type DocumentCacheStats = database.DocumentCacheStats

type CompactionStats = database.CompactionStats

type ReclamationStats = database.ReclamationStats

type BackupStats = database.BackupStats

type QueryLimits = database.QueryLimits

var DefaultQueryLimits = database.DefaultQueryLimits

type SortField = database.SortField

type QuerySpec = database.QuerySpec

type QueryDeltaOperationKind = database.QueryDeltaOperationKind

const QueryDeltaRemove = database.QueryDeltaRemove

const QueryDeltaAdd = database.QueryDeltaAdd

const QueryDeltaMove = database.QueryDeltaMove

const QueryDeltaChange = database.QueryDeltaChange

// QueryDeltaOperation mutates an ordered query result. A zero BeforeID means
// the end of the result; database document IDs are never zero.
type QueryDeltaOperation = database.QueryDeltaOperation

// QueryDelta transforms exactly FromToken into Token. Operations are ordered:
// removals first, followed by reverse-order add/move anchors and document
// changes. Applying them in slice order is deterministic.
type QueryDelta = database.QueryDelta

// ApplyQueryDelta strictly validates and applies an ordered delta without
// mutating the input snapshot.
func ApplyQueryDelta(snapshot QuerySnapshot, delta QueryDelta) (QuerySnapshot, error) {
	return database.ApplyQueryDelta(snapshot, delta)
}

// QueryReplaySource atomically reconstructs a query at afterToken and tails
// later ordered revisions. Initial.Token must equal afterToken. Implementations
// return ErrHistoryLost when retention can no longer satisfy that contract.
type QueryReplaySource = database.QueryReplaySource

type QueryReplaySubscription = database.QueryReplaySubscription

func DecodeQuerySpecJSON(data []byte, limits QueryLimits) (QuerySpec, error) {
	return database.DecodeQuerySpecJSON(data, limits)
}

// MarshalQuerySpecJSON emits the canonical, data-only wire representation used
// for transport fingerprints and cross-language conformance.
func MarshalQuerySpecJSON(query QuerySpec) ([]byte, error) {
	return database.MarshalQuerySpecJSON(query)
}

// ValidateStrictJSON rejects oversized, trailing, deeply nested, and
// duplicate-key JSON before a transport decodes it into structs or maps.
func ValidateStrictJSON(data []byte, maxBytes int) error {
	return database.ValidateStrictJSON(data, maxBytes)
}

func MarshalWireValue(value Value) ([]byte, error) {
	return database.MarshalWireValue(value)
}

// UnmarshalWireValue decodes one closed, typed wire value using the same
// depth/item/byte limits as query operands. It is suitable for data-only
// protocol arguments such as RPC; it never evaluates source or callbacks.
func UnmarshalWireValue(data []byte, limits QueryLimits) (Value, error) {
	return database.UnmarshalWireValue(data, limits)
}

func MarshalWireDocument(document Document) ([]byte, error) {
	return database.MarshalWireDocument(document)
}

func UnmarshalWireDocument(data []byte, limits QueryLimits) (Document, error) {
	return database.UnmarshalWireDocument(data, limits)
}

func UnmarshalWireInputDocument(data []byte, limits QueryLimits) (Document, error) {
	return database.UnmarshalWireInputDocument(data, limits)
}

type QuerySnapshot = database.QuerySnapshot

type QuerySubscription = database.QuerySubscription

// QueryDeltaSubscription returns one safe initial snapshot and then ordered
// deltas. It is the preferred core stream for transports and reactive clients;
// QuerySubscription remains the full-snapshot compatibility adapter.
type QueryDeltaSubscription = database.QueryDeltaSubscription

type ReclaimResult = database.ReclaimResult

// ReclaimOptions controls explicit page reclamation. Online scans a duplicate
// read handle without holding the storage writer lock and installs its result
// only if the Meta generation is unchanged. MaxAttempts bounds complete graph
// rescans after concurrent commits; zero selects three attempts.
type ReclaimOptions = database.ReclaimOptions

// RecoveryMode controls whether Open may perform only the bounded recovery
// actions described by RecoveryReport. Zero selects the normal automatic mode.
type RecoveryMode = database.RecoveryMode

const RecoveryAutomatic = database.RecoveryAutomatic

const RecoveryRequireClean = database.RecoveryRequireClean

// OpenOptions configures the current durable storage format.
type OpenOptions = database.OpenOptions

// PrimaryWriteFence is the local enforcement hook for an external primary
// election/fencing system. Its implementation normally checks an atomically
// refreshed lease epoch and expiry, not the network. Returning an error rejects
// the whole logical commit before storage mutation; it never poisons the
// database or advances a token.
//
// Implementations must not call back into DB and must return promptly: the
// check runs while the writer has admitted a commit. Election, renewal,
// certificate rotation and old-primary revocation remain external concerns.
type PrimaryWriteFence = database.PrimaryWriteFence

// PrimaryWriteFenceRequest binds a proposed primary mutation to this database
// identity and exact next logical commit sequence. A lease implementation must
// reject when its external authority/epoch/expiry no longer permits that write.
type PrimaryWriteFenceRequest = database.PrimaryWriteFenceRequest

// CommitCoordinatorOptions controls optional group commit for ordinary
// InsertMany, filter Update and filter Delete operations. It is disabled by
// default, so opening an existing database never changes write scheduling
// unexpectedly.
//
// A coordinator group has one physical  Meta publication but retains one
// logical commit token for every admitted write request. Public write
// transactions, atomic RPC, index builds and other maintenance operations
// remain exclusive commits. When rollback protection is configured, the
// coordinator advances the external anchor only after the group's final Meta
// publication is durable and before acknowledging any member.
type CommitCoordinatorOptions = database.CommitCoordinatorOptions

const DefaultCommitCoordinatorMaxBatch = database.DefaultCommitCoordinatorMaxBatch

const DefaultCommitCoordinatorMaxPending = database.DefaultCommitCoordinatorMaxPending

const DefaultCommitCoordinatorMaxDelay = database.DefaultCommitCoordinatorMaxDelay

// RollbackAnchor is trusted state retained outside the database device. A
// server must never accept the same identity below either an acknowledged
// logical commit sequence or physical maintenance generation after restart.
// The coordinates are independently monotonic: one group may advance several
// logical sequences while publishing a single physical generation.
type RollbackAnchor = database.RollbackAnchor

// RollbackAnchorStore durably loads and atomically advances one database's
// monotonic anchor. Advance must not return until the anchor is persistent and
// must reject identity changes or regression of either monotonic coordinate.
// Implementations must be safe for concurrent callers and honor cancellation.
// An Advance error does not prove that state was unchanged: persistence may
// have completed before a response, deadline or cancellation was observed.
type RollbackAnchorStore = database.RollbackAnchorStore

// RollbackAnchorStoreStatus is a bounded, identity-free process-session view of
// an anchor backend. Counters are diagnostic and never participate in recovery.
type RollbackAnchorStoreStatus = database.RollbackAnchorStoreStatus

// RollbackAnchorStatusProvider is an optional lock-free observability contract
// for RollbackAnchorStore implementations.
type RollbackAnchorStatusProvider = database.RollbackAnchorStatusProvider

// RollbackProtection configures fail-closed database identity and sequence
// checks. AnchorStore should live on an independently trusted device or remote
// quorum; placing it beside the database cannot detect whole-device rollback.
// InitializeAnchor explicitly trusts the database currently at Path when the
// store is empty and should only be used during provisioning or audited restore.
type RollbackProtection = database.RollbackProtection

// DefaultRollbackAnchorOperationTimeout prevents a failed remote trust service
// from indefinitely holding database publication acknowledgement.
const DefaultRollbackAnchorOperationTimeout = database.DefaultRollbackAnchorOperationTimeout

// StorageLimits bounds the physical single-file high-water mark. Zero selects
// DefaultMaxFileBytes. The value must be a 16 KiB page multiple.
type StorageLimits = database.StorageLimits

const PageSize = database.PageSize

const DefaultMaxFileBytes = database.DefaultMaxFileBytes

// CompactionOptions configures newly written replacement or compaction files.
// ResourceLimits govern transient index construction as well as the reopened
// destination handle; zero fields select production defaults.
type CompactionOptions = database.CompactionOptions

// CommitRetentionPolicy bounds logical Commit Log history by both commit
// count and canonical encoded bytes. Zero fields select production defaults.
// Active replay leases may temporarily exceed either budget rather than losing
// history under a reader.
type CommitRetentionPolicy = database.CommitRetentionPolicy

const DefaultCommitRetentionMaxCommits = database.DefaultCommitRetentionMaxCommits

const DefaultCommitRetentionMaxBytes = database.DefaultCommitRetentionMaxBytes

// DefaultReplayDeliveryTimeout bounds how long a replay source can wait
// for a full caller buffer before it releases the retained-history lease.
const DefaultReplayDeliveryTimeout = database.DefaultReplayDeliveryTimeout

// RecoveryReport is an immutable, non-sensitive receipt for decisions made
// while opening a database. It reports only actions that were completed before
// Open returned successfully; corruption and unsupported formats still fail
// Open instead of being described as recovered.
type RecoveryReport = database.RecoveryReport

// ReplicationSourceLease gives one authenticated source-side replica identity
// exclusive process-local ownership of its durable consumer. A lease prevents
// duplicate concurrent transports from racing one checkpoint; it does not
// establish distributed primary authority or replace follower-promotion
// fencing.
type ReplicationSourceLease = database.ReplicationSourceLease

// ReplicationSourceSession is the primary-side state machine for one
// authenticated peer. It deliberately permits one unacknowledged batch at a
// time: this is both bounded flow control and the proof that a durable ACK can
// never skip an unseen source token.
type ReplicationSourceSession = database.ReplicationSourceSession

// NewReplicationSourceSession binds an existing named durable database feed to
// one source identity. The caller owns peer authentication and must close the
// session when that authenticated connection ends.
func NewReplicationSourceSession(db *DB, subscription *DurableDatabaseChangeSubscription, limits ReplicationFrameLimits) (*ReplicationSourceSession, error) {
	return database.NewReplicationSourceSession(db, subscription, limits)
}

// ReplicationProtocolVersion is intentionally separate from the browser
// realtime protocol. It transports durable database positions between trusted
// servers, not end-user query subscriptions.
const ReplicationProtocolVersion = database.ReplicationProtocolVersion

const DefaultReplicationMaxFrameBytes = database.DefaultReplicationMaxFrameBytes

// ReplicationFrameLimits bounds one already-decompressed protocol frame. The
// default accommodates the configured 64 MiB canonical transaction limit plus
// JSON/base64 overhead, while still rejecting unbounded peer allocation.
type ReplicationFrameLimits = database.ReplicationFrameLimits

// ReplicationFrame is a transport-neutral protocol envelope. A transport must
// authenticate both peers (for example with mTLS) before it accepts frames;
// DatabaseID binds every frame to one durable source identity.
type ReplicationFrame = database.ReplicationFrame

const ReplicationHelloFrame = database.ReplicationHelloFrame

const ReplicationBatchFrame = database.ReplicationBatchFrame

const ReplicationAckFrame = database.ReplicationAckFrame

const ReplicationResyncFrame = database.ReplicationResyncFrame

// MarshalReplicationFrame returns a strict JSON frame with canonical document
// images encoded as base64 of the storage-independent typed document codec.
// It is suitable for WebSocket binary/text messages, QUIC streams or framed
// RPC, but it does not provide authentication or encryption itself.
func MarshalReplicationFrame(frame ReplicationFrame, limits ReplicationFrameLimits) ([]byte, error) {
	return database.MarshalReplicationFrame(frame, limits)
}

// UnmarshalReplicationFrame rejects unknown fields, duplicate JSON keys,
// malformed base64, invalid typed documents and non-canonical identities before
// a receiver reaches the follower state machine.
func UnmarshalReplicationFrame(data []byte, limits ReplicationFrameLimits) (ReplicationFrame, error) {
	return database.UnmarshalReplicationFrame(data, limits)
}

const DefaultMaxDocumentBytes = database.DefaultMaxDocumentBytes

const DefaultMaxTransactionBytes = database.DefaultMaxTransactionBytes

const DefaultMaxTransactionChanges = database.DefaultMaxTransactionChanges

const DefaultMaxIndexBuildEntries = database.DefaultMaxIndexBuildEntries

const DefaultMaxIndexBuildBytes = database.DefaultMaxIndexBuildBytes

// Reactive views retain matching document versions for incremental ordering
// and updates, not merely the page currently emitted to a subscriber.
const DefaultMaxReactiveViewDocuments = database.DefaultMaxReactiveViewDocuments

const DefaultMaxReactiveViewBytes = database.DefaultMaxReactiveViewBytes

const DefaultMaxQueryDocumentsExamined = database.DefaultMaxQueryDocumentsExamined

const DefaultMaxQueryKeysExamined = database.DefaultMaxQueryKeysExamined

const DefaultMaxQueryCandidates = database.DefaultMaxQueryCandidates

const DefaultMaxQuerySortBytes = database.DefaultMaxQuerySortBytes

const DefaultMaxQuerySkip = database.DefaultMaxQuerySkip

// ResourceLimits bounds work admitted by writes, index maintenance, and query execution. Zero values
// select production defaults; limits cannot be disabled accidentally. Byte
// limits use the canonical typed binary representation, independent of Go heap
// layout, JSON spelling, storage generation, or transport compression.
type ResourceLimits = database.ResourceLimits

// DatabaseOptions configures an in-memory database.
type DatabaseOptions = database.DatabaseOptions

// NewFileRollbackAnchorStore returns a fail-closed, atomically replaced anchor
// file. The parent directory must already exist. For rollback protection, that
// directory must be backed by storage trusted independently from the database.
func NewFileRollbackAnchorStore(path string) (RollbackAnchorStore, error) {
	return database.NewFileRollbackAnchorStore(path)
}

// StorageFormatInfo is a read-only negotiation view, not a full graph audit.
// ReaderCompatible says this binary understands the reported revision and all
// required feature bits; callers must still Open the database before use.
type StorageFormatInfo = database.StorageFormatInfo

// StorageFormat identifies the sole supported on-disk engine. Unknown denotes
// a missing or zero-length path, not an unrecognized non-empty file.
type StorageFormat = database.StorageFormat

const StorageFormatUnknown = database.StorageFormatUnknown

const StorageFormatCurrent = database.StorageFormatCurrent

// DetectStorageFormat performs only enough inspection to distinguish a new
// path from the current database format. Old database files deliberately fail
// closed: this build contains no legacy reader or automatic migration path.
func DetectStorageFormat(path string) (StorageFormat, error) {
	return database.DetectStorageFormat(path)
}

// InspectStorageFormat validates current Meta checksums and reports its newest
// readable envelope without opening the database for mutation.
func InspectStorageFormat(path string) (StorageFormatInfo, error) {
	return database.InspectStorageFormat(path)
}

type Update = database.Update

type UpdateResult = database.UpdateResult

type MutationSpec = database.MutationSpec

func CompileUpdate(update Update) (MutationSpec, error) {
	return database.CompileUpdate(update)
}

type Kind = database.Kind

const NullKind = database.NullKind

const BoolKind = database.BoolKind

const Int64Kind = database.Int64Kind

const Float64Kind = database.Float64Kind

const StringKind = database.StringKind

const BinaryKind = database.BinaryKind

const TimeKind = database.TimeKind

const ArrayKind = database.ArrayKind

const ObjectKind = database.ObjectKind

const IDKind = database.IDKind

type DocumentID = database.DocumentID

func NewDocumentID() (DocumentID, error) {
	return database.NewDocumentID()
}

func ParseDocumentID(s string) (DocumentID, error) {
	return database.ParseDocumentID(s)
}

// Value is a closed tagged value. Its representation is private so callers
// cannot construct a tag/payload mismatch.
type Value = database.Value

func Null() Value {
	return database.Null()
}

func Bool(v bool) Value {
	return database.Bool(v)
}

func Int(v int64) Value {
	return database.Int(v)
}

func Float(v float64) Value {
	return database.Float(v)
}

func String(v string) Value {
	return database.String(v)
}

func Binary(v []byte) Value {
	return database.Binary(v)
}

// Time stores millisecond precision, matching JavaScript Date and the wire
// contract. Precision is normalized at construction rather than silently lost
// during transport.
func Time(v time.Time) Value {
	return database.Time(v)
}

func ID(v DocumentID) Value {
	return database.ID(v)
}

func Array(v ...Value) Value {
	return database.Array(v...)
}

func Object(v Document) Value {
	return database.Object(v)
}

func ValueOf(x any) (Value, error) {
	return database.ValueOf(x)
}

// VerificationReport is a schema-versioned receipt for a full, read-only
// protected-page graph and published-index semantic audit. ReclaimablePages is
// informational; verification never installs a free pool or publishes
// maintenance metadata.
type VerificationReport = database.VerificationReport

// VerifyFile performs an offline, non-mutating audit of an existing file.
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

// WriteTransaction is a short-lived snapshot write view. It provides point
// operations with optimistic serializable commit validation. Values returned
// from it are isolated clones.
//
// A transaction is active only during its handler callback. Handlers must not
// retain it or call normal DB/Collection methods from inside the callback.
type WriteTransaction = database.WriteTransaction
