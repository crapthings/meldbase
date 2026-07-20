package meldbase

import (
	"context"
	"fmt"
	"time"
)

// RecoveryMode controls whether Open may perform only the bounded recovery
// actions described by RecoveryReport. Zero selects the normal automatic mode.
type RecoveryMode uint8

const (
	RecoveryAutomatic RecoveryMode = iota
	RecoveryRequireClean
)

// OpenOptions configures format-neutral Open. V1Checkpoint is ignored for V2;
// V2 retention/replay/storage fields are ignored for V1, while
// V2RollbackProtection is rejected for V1 so a requested safety boundary is
// never silently absent.
type OpenOptions struct {
	Recovery                RecoveryMode
	V1Checkpoint            V1CheckpointPolicy
	V2CommitRetention       V2CommitRetentionPolicy
	V2ReplayDeliveryTimeout time.Duration
	V2CommitCoordinator     V2CommitCoordinatorOptions
	ResourceLimits          ResourceLimits
	V2StorageLimits         V2StorageLimits
	V2RollbackProtection    V2RollbackProtection
	// V2RequireGraphAudit performs a structural full-graph audit before a V2
	// open succeeds. It is ignored for V1, whose recovery has a distinct WAL
	// validation contract.
	V2RequireGraphAudit bool
	// V2RequirePrivateFileMode rejects an existing V2 database that grants
	// group or other Unix permission bits. It is ignored for V1.
	V2RequirePrivateFileMode bool
	// V2PrimaryWriteFence is forwarded only when Open selects V2. Open rejects
	// an existing legacy V1 file when this boundary is requested, so a caller
	// cannot silently lose primary-fence enforcement through format detection.
	V2PrimaryWriteFence V2PrimaryWriteFence
}

// V2Options configures explicitly selected Storage V2 opening.
type V2Options struct {
	Recovery              RecoveryMode
	CommitRetention       V2CommitRetentionPolicy
	ReplayDeliveryTimeout time.Duration
	CommitCoordinator     V2CommitCoordinatorOptions
	ResourceLimits        ResourceLimits
	StorageLimits         V2StorageLimits
	RollbackProtection    V2RollbackProtection
	// RequireGraphAudit rejects a V2 database at startup when any page
	// protected by the current or fallback Meta root is structurally invalid.
	// It is intentionally opt-in because audit cost grows with database size.
	// This does not replace the offline semantic index verifier.
	RequireGraphAudit bool
	// RequirePrivateFileMode rejects a V2 file with group/world permission
	// bits instead of silently changing operator-owned permissions.
	RequirePrivateFileMode bool
	// PrimaryWriteFence optionally proves that this local database still holds
	// external primary authority before every business V2 commit. It is not
	// consulted by a read-only follower applying an already validated source
	// batch. The guard must be local, non-blocking and safe for concurrent use;
	// controller I/O/lease renewal belongs outside Meldbase's writer lock.
	PrimaryWriteFence V2PrimaryWriteFence
	// Follower marks this local open as a replica. Normal application writes
	// fail with ErrReplicaReadOnly; only V2Follower.Apply may advance it.
	Follower bool
}

// V2PrimaryWriteFence is the local enforcement hook for an external primary
// election/fencing system. Its implementation normally checks an atomically
// refreshed lease epoch and expiry, not the network. Returning an error rejects
// the whole logical commit before V2 storage mutation; it never poisons the
// database or advances a token.
//
// Implementations must not call back into DB and must return promptly: the
// check runs while the V2 writer has admitted a commit. Election, renewal,
// certificate rotation and old-primary revocation remain external concerns.
type V2PrimaryWriteFence interface {
	ValidateV2PrimaryWrite(PrimaryWriteFenceRequest) error
}

// PrimaryWriteFenceRequest binds a proposed primary mutation to this database
// identity and exact next logical commit sequence. A lease implementation must
// reject when its external authority/epoch/expiry no longer permits that write.
type PrimaryWriteFenceRequest struct {
	DatabaseID         [16]byte
	NextCommitSequence uint64
}

// V2CommitCoordinatorOptions controls optional group commit for ordinary V2
// InsertMany, filter Update and filter Delete operations. It is disabled by
// default, so opening an existing database never changes write scheduling
// unexpectedly.
//
// A coordinator group has one physical V2 Meta publication but retains one
// logical commit token for every admitted write request. Public write
// transactions, atomic RPC, index builds and other maintenance operations
// remain exclusive commits. When rollback protection is configured, the
// coordinator advances the external anchor only after the group's final Meta
// publication is durable and before acknowledging any member.
type V2CommitCoordinatorOptions struct {
	Enabled    bool
	MaxBatch   int
	MaxPending int
	MaxDelay   time.Duration
}

const (
	DefaultV2CommitCoordinatorMaxBatch   = 32
	DefaultV2CommitCoordinatorMaxPending = 1024
)

const DefaultV2CommitCoordinatorMaxDelay = time.Millisecond

func normalizeV2CommitCoordinatorOptions(options V2CommitCoordinatorOptions) (V2CommitCoordinatorOptions, error) {
	if !options.Enabled {
		return V2CommitCoordinatorOptions{}, nil
	}
	if options.MaxBatch == 0 {
		options.MaxBatch = DefaultV2CommitCoordinatorMaxBatch
	}
	if options.MaxPending == 0 {
		options.MaxPending = DefaultV2CommitCoordinatorMaxPending
	}
	if options.MaxDelay == 0 {
		options.MaxDelay = DefaultV2CommitCoordinatorMaxDelay
	}
	if options.MaxBatch < 2 || options.MaxBatch > 256 || options.MaxPending < options.MaxBatch ||
		options.MaxPending > 65_536 || options.MaxDelay < 0 || options.MaxDelay > time.Second {
		return V2CommitCoordinatorOptions{}, ErrInvalidCommitCoordinatorOptions
	}
	return options, nil
}

// RollbackAnchor is trusted state retained outside the database device. A
// server must never accept the same identity below either an acknowledged
// logical commit sequence or physical maintenance generation after restart.
// The coordinates are independently monotonic: one group may advance several
// logical sequences while publishing a single physical generation.
type RollbackAnchor struct {
	DatabaseID            [16]byte
	MinimumCommitSequence uint64
	MinimumGeneration     uint64
}

// RollbackAnchorStore durably loads and atomically advances one database's
// monotonic anchor. Advance must not return until the anchor is persistent and
// must reject identity changes or regression of either monotonic coordinate.
// Implementations must be safe for concurrent callers and honor cancellation.
// An Advance error does not prove that state was unchanged: persistence may
// have completed before a response, deadline or cancellation was observed.
type RollbackAnchorStore interface {
	Load(context.Context) (RollbackAnchor, bool, error)
	Advance(context.Context, RollbackAnchor) error
}

// RollbackAnchorStoreStatus is a bounded, identity-free process-session view of
// an anchor backend. Counters are diagnostic and never participate in recovery.
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

// RollbackAnchorStatusProvider is an optional lock-free observability contract
// for RollbackAnchorStore implementations.
type RollbackAnchorStatusProvider interface {
	RollbackAnchorStatus() RollbackAnchorStoreStatus
}

// V2RollbackProtection configures fail-closed database identity and sequence
// checks. AnchorStore should live on an independently trusted device or remote
// quorum; placing it beside the database cannot detect whole-device rollback.
// InitializeAnchor explicitly trusts the database currently at Path when the
// store is empty and should only be used during provisioning or audited restore.
type V2RollbackProtection struct {
	ExpectedDatabaseID    [16]byte
	MinimumCommitSequence uint64
	MinimumGeneration     uint64
	AnchorStore           RollbackAnchorStore
	InitializeAnchor      bool
	// OperationTimeout bounds each Load/Advance/read-back interaction. Zero
	// selects DefaultRollbackAnchorOperationTimeout.
	OperationTimeout time.Duration
}

// DefaultRollbackAnchorOperationTimeout prevents a failed remote trust service
// from indefinitely holding database publication acknowledgement.
const DefaultRollbackAnchorOperationTimeout = 10 * time.Second

// V2StorageLimits bounds the physical single-file high-water mark. Zero selects
// DefaultV2MaxFileBytes. The value must be a 16 KiB V2 page multiple.
type V2StorageLimits struct{ MaxFileBytes uint64 }

const (
	V2PageSize            uint64 = 16 << 10
	DefaultV2MaxFileBytes uint64 = 8 << 30
)

// V2DestinationOptions configures newly written migration or compaction files.
// ResourceLimits govern transient index construction as well as the reopened
// destination handle; zero fields select production defaults.
type V2DestinationOptions struct {
	StorageLimits  V2StorageLimits
	ResourceLimits ResourceLimits
}

// V2CommitRetentionPolicy bounds logical Commit Log history by both commit
// count and canonical encoded bytes. Zero fields select production defaults.
// Active replay leases may temporarily exceed either budget rather than losing
// history under a reader.
type V2CommitRetentionPolicy struct {
	MaxCommits uint64
	MaxBytes   uint64
}

const (
	DefaultV2CommitRetentionMaxCommits uint64 = 10_000
	DefaultV2CommitRetentionMaxBytes   uint64 = 256 << 20
	// DefaultV2ReplayDeliveryTimeout bounds how long a replay source can wait
	// for a full caller buffer before it releases the retained-history lease.
	DefaultV2ReplayDeliveryTimeout = 5 * time.Second
)

func normalizeV2ReplayDeliveryTimeout(timeout time.Duration) (time.Duration, error) {
	if timeout == 0 {
		return DefaultV2ReplayDeliveryTimeout, nil
	}
	if timeout < time.Millisecond || timeout > time.Minute {
		return 0, ErrInvalidReplayDeliveryTimeout
	}
	return timeout, nil
}

func validateRecoveryMode(mode RecoveryMode) error {
	if mode != RecoveryAutomatic && mode != RecoveryRequireClean {
		return fmt.Errorf("meldbase: invalid recovery mode %d", mode)
	}
	return nil
}

// RecoveryReport is an immutable, non-sensitive receipt for decisions made
// while opening a database. It reports only actions that were completed before
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
// It performs no I/O and never changes after the DB is opened.
func (db *DB) RecoveryReport() RecoveryReport {
	if db == nil {
		return RecoveryReport{SchemaVersion: 1}
	}
	return db.recovery
}

func finalizeRecoveryReport(report RecoveryReport) RecoveryReport {
	report.SchemaVersion = 1
	report.Recovered = report.FallbackToOlderRoot || report.MetaRedundancyDegraded || report.MainTailBytesRemoved != 0 ||
		report.WALRecordsReplayed != 0 || report.WALTailBytesRemoved != 0 || report.AccelerationDegraded
	return report
}
