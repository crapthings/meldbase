package meldbase

import "fmt"

// RecoveryMode controls whether Open may perform only the bounded recovery
// actions described by RecoveryReport. Zero selects the normal automatic mode.
type RecoveryMode uint8

const (
	RecoveryAutomatic RecoveryMode = iota
	RecoveryRequireClean
)

// OpenOptions configures format-neutral Open. V1Checkpoint is ignored for V2;
// V2 retention/storage fields are ignored for V1.
type OpenOptions struct {
	Recovery          RecoveryMode
	V1Checkpoint      V1CheckpointPolicy
	V2CommitRetention V2CommitRetentionPolicy
	ResourceLimits    ResourceLimits
	V2StorageLimits   V2StorageLimits
}

// V2Options configures explicitly selected Storage V2 opening.
type V2Options struct {
	Recovery        RecoveryMode
	CommitRetention V2CommitRetentionPolicy
	ResourceLimits  ResourceLimits
	StorageLimits   V2StorageLimits
}

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
