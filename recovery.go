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
// V2 retention/storage fields are ignored for V1, while V2RollbackProtection
// is rejected for V1 so a requested safety boundary is never silently absent.
type OpenOptions struct {
	Recovery             RecoveryMode
	V1Checkpoint         V1CheckpointPolicy
	V2CommitRetention    V2CommitRetentionPolicy
	ResourceLimits       ResourceLimits
	V2StorageLimits      V2StorageLimits
	V2RollbackProtection V2RollbackProtection
}

// V2Options configures explicitly selected Storage V2 opening.
type V2Options struct {
	Recovery           RecoveryMode
	CommitRetention    V2CommitRetentionPolicy
	ResourceLimits     ResourceLimits
	StorageLimits      V2StorageLimits
	RollbackProtection V2RollbackProtection
}

// RollbackAnchor is trusted state retained outside the database device. A
// server must never accept the same identity below either an acknowledged
// logical commit sequence or physical maintenance generation after restart.
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
)

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
