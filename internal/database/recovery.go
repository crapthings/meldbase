package database

import (
	"context"
	"fmt"
	"time"
)

// RecoveryMode selects whether startup may perform bounded recovery actions.
//
// Zero selects automatic recovery; RecoveryReport describes completed actions.
type RecoveryMode uint8

const (
	// RecoveryAutomatic allows bounded automatic recovery when opening a database.
	RecoveryAutomatic RecoveryMode = iota
	// RecoveryRequireClean rejects opening a database that needs recovery.
	RecoveryRequireClean
)

// OpenOptions configures the current durable storage format.
type OpenOptions struct {
	Recovery              RecoveryMode
	CommitRetention       CommitRetentionPolicy
	ReplayDeliveryTimeout time.Duration
	CommitCoordinator     CommitCoordinatorOptions
	ResourceLimits        ResourceLimits
	StorageLimits         StorageLimits
	RollbackProtection    RollbackProtection
	// RequireGraphAudit rejects a database at startup when any page
	// protected by the current or fallback Meta root is structurally invalid.
	// It is intentionally opt-in because audit cost grows with database size.
	// This does not replace the offline semantic index verifier.
	RequireGraphAudit bool
	// RequirePrivateFileMode rejects a database file with group/world permission
	// bits instead of silently changing operator-owned permissions.
	RequirePrivateFileMode bool
	// PrimaryWriteFence optionally proves that this local database still holds
	// external primary authority before every business commit. It is not
	// consulted by a read-only follower applying an already validated source
	// batch. The guard must be local, non-blocking and safe for concurrent use;
	// controller I/O/lease renewal belongs outside Meldbase's writer lock.
	PrimaryWriteFence PrimaryWriteFence
	// Follower marks this local open as a replica. Normal application writes
	// fail with ErrReplicaReadOnly; only Follower.Apply may advance it.
	Follower bool
}

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
type PrimaryWriteFence interface {
	ValidatePrimaryWrite(PrimaryWriteFenceRequest) error
}

// PrimaryWriteFenceRequest identifies the database and next commit requiring write authority.
//
// A lease implementation must
// reject when its external authority/epoch/expiry no longer permits that write.
type PrimaryWriteFenceRequest struct {
	DatabaseID         [16]byte
	NextCommitSequence uint64
}

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
type CommitCoordinatorOptions struct {
	Enabled    bool
	MaxBatch   int
	MaxPending int
	MaxDelay   time.Duration
}

const (
	// DefaultCommitCoordinatorMaxBatch sets the default maximum number of writes in a commit group.
	DefaultCommitCoordinatorMaxBatch = 32
	// DefaultCommitCoordinatorMaxPending sets the default maximum number of queued writes.
	DefaultCommitCoordinatorMaxPending = 1024
)

// DefaultCommitCoordinatorMaxDelay sets the default wait for grouping queued writes.
const DefaultCommitCoordinatorMaxDelay = time.Millisecond

// normalizeCommitCoordinatorOptions fills enabled coordinator defaults and rejects invalid bounds.
func normalizeCommitCoordinatorOptions(options CommitCoordinatorOptions) (CommitCoordinatorOptions, error) {
	if !options.Enabled {
		return CommitCoordinatorOptions{}, nil
	}
	if options.MaxBatch == 0 {
		options.MaxBatch = DefaultCommitCoordinatorMaxBatch
	}
	if options.MaxPending == 0 {
		options.MaxPending = DefaultCommitCoordinatorMaxPending
	}
	if options.MaxDelay == 0 {
		options.MaxDelay = DefaultCommitCoordinatorMaxDelay
	}
	if options.MaxBatch < 2 || options.MaxBatch > 256 || options.MaxPending < options.MaxBatch ||
		options.MaxPending > 65_536 || options.MaxDelay < 0 || options.MaxDelay > time.Second {
		return CommitCoordinatorOptions{}, ErrInvalidCommitCoordinatorOptions
	}
	return options, nil
}

// RollbackAnchor records trusted minimum commit and generation values outside the database.
//
// A server must never accept the same identity below either an acknowledged
// logical commit sequence or physical maintenance generation after restart.
// The coordinates are independently monotonic: one group may advance several
// logical sequences while publishing a single physical generation.
type RollbackAnchor struct {
	DatabaseID            [16]byte
	MinimumCommitSequence uint64
	MinimumGeneration     uint64
}

// RollbackAnchorStore loads and durably advances a database's trusted rollback boundary.
//
// Advance must not return until the anchor is persistent and
// must reject identity changes or regression of either monotonic coordinate.
// Implementations must be safe for concurrent callers and honor cancellation.
// An Advance error does not prove that state was unchanged: persistence may
// have completed before a response, deadline or cancellation was observed.
type RollbackAnchorStore interface {
	Load(context.Context) (RollbackAnchor, bool, error)
	Advance(context.Context, RollbackAnchor) error
}

// RollbackAnchorStoreStatus reports anchor backend counters without exposing database identities.
//
// These diagnostic counters never participate in recovery.
type RollbackAnchorStoreStatus struct {
	Replicas               uint64 `json:"replicas"`
	Quorum                 uint64 `json:"quorum"`
	Loads                  uint64 `json:"loads"`
	Advances               uint64 `json:"advances"`
	EndpointFailures       uint64 `json:"endpointFailures"`
	QuorumFailures         uint64 `json:"quorumFailures"`
	Conflicts              uint64 `json:"conflicts"`
	AuthenticationFailures uint64 `json:"authenticationFailures"`
	ProtocolFailures       uint64 `json:"protocolFailures"`
	ConfigurationFailures  uint64 `json:"configurationFailures"`
}

// RollbackAnchorStatusProvider exposes anchor store counters without acquiring locks.
type RollbackAnchorStatusProvider interface {
	RollbackAnchorStatus() RollbackAnchorStoreStatus
}

// RollbackProtection configures identity and progress checks that reject database rollback.
//
// AnchorStore should live on an independently trusted device or remote
// quorum; placing it beside the database cannot detect whole-device rollback.
// InitializeAnchor explicitly trusts the database currently at Path when the
// store is empty and should only be used during provisioning or audited restore.
type RollbackProtection struct {
	ExpectedDatabaseID    [16]byte
	MinimumCommitSequence uint64
	MinimumGeneration     uint64
	AnchorStore           RollbackAnchorStore
	InitializeAnchor      bool
	// OperationTimeout bounds each Load/Advance/read-back interaction. Zero
	// selects DefaultRollbackAnchorOperationTimeout.
	OperationTimeout time.Duration
}

// DefaultRollbackAnchorOperationTimeout limits how long a rollback anchor operation may delay a commit response.
const DefaultRollbackAnchorOperationTimeout = 10 * time.Second

// StorageLimits caps the physical database file size.
//
// Zero selects DefaultMaxFileBytes; the limit must be a multiple of the 16 KiB page size.
type StorageLimits struct{ MaxFileBytes uint64 }

const (
	// PageSize sets the byte size of each storage page.
	PageSize uint64 = 16 << 10
	// DefaultMaxFileBytes sets the default physical database file size limit.
	DefaultMaxFileBytes uint64 = 8 << 30
)

// CompactionOptions sets storage and resource limits for a rewritten database.
//
// ResourceLimits govern transient index construction as well as the reopened
// destination handle; zero fields select production defaults.
type CompactionOptions struct {
	StorageLimits  StorageLimits
	ResourceLimits ResourceLimits
}

// CommitRetentionPolicy limits retained history by commit count and encoded byte size.
//
// Zero fields select production defaults.
// Active replay leases may temporarily exceed either budget rather than losing
// history under a reader.
type CommitRetentionPolicy struct {
	MaxCommits uint64
	MaxBytes   uint64
}

const (
	// DefaultCommitRetentionMaxCommits sets the default retained commit count limit.
	DefaultCommitRetentionMaxCommits uint64 = 10_000
	// DefaultCommitRetentionMaxBytes sets the default retained commit payload limit.
	DefaultCommitRetentionMaxBytes uint64 = 256 << 20
	// DefaultReplayDeliveryTimeout limits how long replay waits to deliver into a full buffer.
	//
	// A delivery timeout releases the retained-history lease.
	DefaultReplayDeliveryTimeout = 5 * time.Second
)

// normalizeReplayDeliveryTimeout selects the default timeout or validates the supplied duration.
func normalizeReplayDeliveryTimeout(timeout time.Duration) (time.Duration, error) {
	if timeout == 0 {
		return DefaultReplayDeliveryTimeout, nil
	}
	if timeout < time.Millisecond || timeout > time.Minute {
		return 0, ErrInvalidReplayDeliveryTimeout
	}
	return timeout, nil
}

// validateRecoveryMode rejects unknown startup recovery policies.
func validateRecoveryMode(mode RecoveryMode) error {
	if mode != RecoveryAutomatic && mode != RecoveryRequireClean {
		return fmt.Errorf("meldbase: invalid recovery mode %d", mode)
	}
	return nil
}

// RecoveryReport records completed startup recovery actions without exposing sensitive data.
//
// It reports only actions that were completed before
// Open returned successfully; corruption and unsupported formats still fail
// Open instead of being described as recovered.
type RecoveryReport struct {
	SchemaVersion          int    `json:"schemaVersion"`
	Engine                 string `json:"engine"`
	Created                bool   `json:"created"`
	Recovered              bool   `json:"recovered"`
	CommitSequenceBefore   uint64 `json:"commitSequenceBefore"`
	CommitSequenceAfter    uint64 `json:"commitSequenceAfter"`
	SelectedMetaSlot       uint8  `json:"selectedMetaSlot"`
	ChecksumValidMetaSlots uint8  `json:"checksumValidMetaSlots"`
	RootValidMetaSlots     uint8  `json:"rootValidMetaSlots"`
	MetaRedundancyDegraded bool   `json:"metaRedundancyDegraded"`
	FallbackToOlderRoot    bool   `json:"fallbackToOlderRoot"`
	MainTailBytesRemoved   uint64 `json:"mainTailBytesRemoved"`
	WALRecordsReplayed     uint64 `json:"walRecordsReplayed"`
	WALTailBytesRemoved    uint64 `json:"walTailBytesRemoved"`
	AccelerationDegraded   bool   `json:"accelerationDegraded"`
}

// RecoveryReport returns the receipt captured by the successful constructor.
//
// It performs no I/O and never changes after the DB is opened.
func (db *DB) RecoveryReport() RecoveryReport {
	if db == nil {
		return RecoveryReport{SchemaVersion: 1}
	}
	return db.recovery
}

// finalizeRecoveryReport sets the report version and summarizes completed recovery actions.
func finalizeRecoveryReport(report RecoveryReport) RecoveryReport {
	report.SchemaVersion = 1
	report.Recovered = report.FallbackToOlderRoot || report.MetaRedundancyDegraded || report.MainTailBytesRemoved != 0 ||
		report.AccelerationDegraded
	return report
}
