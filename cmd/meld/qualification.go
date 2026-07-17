package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	soakqualification "github.com/crapthings/meldbase/internal/qualification"
)

const qualificationReceiptMaxBytes = 1 << 20

type qualificationSoakPhase = soakqualification.SoakPhaseReceipt
type qualificationSoakReceipt = soakqualification.SoakReceipt

type qualificationDestructiveRecord struct {
	SchemaVersion            uint32    `json:"schemaVersion"`
	SourceRevision           string    `json:"sourceRevision"`
	PlatformClass            string    `json:"platformClass"`
	GOOS                     string    `json:"goos"`
	GOARCH                   string    `json:"goarch"`
	Device                   uint64    `json:"device"`
	FilesystemType           string    `json:"filesystemType"`
	FilesystemName           string    `json:"filesystemName"`
	BlockSize                uint64    `json:"blockSize"`
	StartedAt                time.Time `json:"startedAt"`
	FinishedAt               time.Time `json:"finishedAt"`
	DurabilityReceiptSHA256  string    `json:"durabilityReceiptSha256"`
	SoakReceiptSHA256        string    `json:"soakReceiptSha256"`
	CapacityExhaustion       bool      `json:"capacityExhaustion"`
	ProcessKill              bool      `json:"processKill"`
	PowerCut                 bool      `json:"powerCut"`
	PublicationBoundaries    bool      `json:"publicationBoundariesCovered"`
	LockReacquisition        bool      `json:"lockReacquisition"`
	OldOrNewStateOnly        bool      `json:"oldOrNewStateOnly"`
	OfflineVerification      bool      `json:"offlineVerification"`
	KernelAndMountRecorded   bool      `json:"kernelAndMountRecorded"`
	ControllerPolicyRecorded bool      `json:"controllerPolicyRecorded"`
	HostAndOperatorRecorded  bool      `json:"hostAndOperatorRecorded"`
	SecuredArtifactsSHA256   string    `json:"securedArtifactsSha256"`
}

type qualificationCheckResult struct {
	SchemaVersion           uint32 `json:"schemaVersion"`
	SourceRevision          string `json:"sourceRevision"`
	EvidenceLevel           uint8  `json:"evidenceLevel"`
	ProductionQualified     bool   `json:"productionQualified"`
	GOOS                    string `json:"goos"`
	GOARCH                  string `json:"goarch"`
	Device                  uint64 `json:"device"`
	FilesystemType          string `json:"filesystemType"`
	FilesystemName          string `json:"filesystemName"`
	BlockSize               uint64 `json:"blockSize"`
	DurabilityReceiptSHA256 string `json:"durabilityReceiptSha256"`
	SoakReceiptSHA256       string `json:"soakReceiptSha256"`
	DestructiveRecordSHA256 string `json:"destructiveRecordSha256,omitempty"`
	Passed                  bool   `json:"passed"`
}

func runQualificationCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	durabilityPath := flags.String("durability-receipt", "", "schema-2 durability-check receipt from the target volume")
	soakPath := flags.String("soak-receipt", "", "schema-4 release-soak receipt from the same target volume")
	destructivePath := flags.String("destructive-record", "", "optional schema-1 secured destructive-test record")
	sourceRevision := flags.String("source-revision", "", "required 40- or 64-hex release revision")
	requireLevel := flags.Int("require-level", 3, "minimum evidence level: 3 or 4")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *durabilityPath == "" || *soakPath == "" || !validDurabilitySourceRevision(*sourceRevision) {
		return errors.New("qualification-check requires --durability-receipt, --soak-receipt and a 40- or 64-hex --source-revision")
	}
	if *requireLevel != 3 && *requireLevel != 4 {
		return errors.New("qualification-check --require-level must be 3 or 4")
	}

	var durability durabilityCheckResult
	durabilityRaw, err := readQualificationReceipt(*durabilityPath, &durability)
	if err != nil {
		return fmt.Errorf("durability receipt: %w", err)
	}
	if err := validateQualificationDurability(durability, *sourceRevision); err != nil {
		return fmt.Errorf("durability receipt: %w", err)
	}
	var soak qualificationSoakReceipt
	soakRaw, err := readQualificationReceipt(*soakPath, &soak)
	if err != nil {
		return fmt.Errorf("soak receipt: %w", err)
	}
	if err := validateQualificationSoak(soak, durability, *sourceRevision); err != nil {
		return fmt.Errorf("soak receipt: %w", err)
	}

	durabilityHash, soakHash := qualificationSHA256(durabilityRaw), qualificationSHA256(soakRaw)
	result := qualificationCheckResult{
		SchemaVersion: 1, SourceRevision: *sourceRevision, EvidenceLevel: 3,
		GOOS: durability.GOOS, GOARCH: durability.GOARCH, Device: durability.Device,
		FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		DurabilityReceiptSHA256: durabilityHash, SoakReceiptSHA256: soakHash, Passed: true,
	}
	if *destructivePath != "" {
		var destructive qualificationDestructiveRecord
		raw, err := readQualificationReceipt(*destructivePath, &destructive)
		if err != nil {
			return fmt.Errorf("destructive record: %w", err)
		}
		if err := validateQualificationDestructive(destructive, result); err != nil {
			return fmt.Errorf("destructive record: %w", err)
		}
		result.EvidenceLevel = 4
		result.ProductionQualified = true
		result.DestructiveRecordSHA256 = qualificationSHA256(raw)
	}
	if int(result.EvidenceLevel) < *requireLevel {
		return fmt.Errorf("qualification evidence level %d does not satisfy required level %d", result.EvidenceLevel, *requireLevel)
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func readQualificationReceipt(path string, target any) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > qualificationReceiptMaxBytes {
		return nil, errors.New("receipt must be a nonempty regular file no larger than 1 MiB")
	}
	raw, err := io.ReadAll(io.LimitReader(file, qualificationReceiptMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > qualificationReceiptMaxBytes {
		return nil, errors.New("receipt grew beyond 1 MiB while being read")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("receipt contains more than one JSON value")
		}
		return nil, err
	}
	return raw, nil
}

func validateQualificationDurability(receipt durabilityCheckResult, revision string) error {
	if receipt.SchemaVersion != 2 || receipt.SourceRevision != revision || receipt.BuildRevision != revision || receipt.BuildModified || !receipt.Passed {
		return errors.New("requires a passing clean schema-2 receipt bound to the release revision")
	}
	if receipt.Directory == "" || receipt.GOOS == "" || receipt.GOARCH == "" || receipt.GoVersion == "" || receipt.Device == 0 ||
		receipt.FilesystemType == "" || receipt.FilesystemName == "" || receipt.BlockSize == 0 || receipt.TotalBytes == 0 ||
		receipt.AvailableBytes > receipt.TotalBytes || receipt.Duration <= 0 ||
		!qualificationTimingValid(receipt.StartedAt, receipt.FinishedAt, receipt.Duration) {
		return errors.New("invalid target, runtime, capacity or timing evidence")
	}
	if !qualificationSafeName(receipt.GOOS, 32) || !qualificationSafeName(receipt.GOARCH, 32) ||
		!qualificationSafeName(receipt.FilesystemType, 128) || !qualificationSafeName(receipt.FilesystemName, 128) {
		return errors.New("invalid platform or filesystem identity")
	}
	wantChecks := []string{
		"create-private-probe-directory", "file-write-and-fsync", "parent-directory-fsync-after-create", "probe-directory-fsync",
		"exclusive-advisory-lock-and-close-release", "atomic-no-overwrite-link", "same-directory-rename-and-fsync",
		"meldbase-create-indexed-commit-reopen", "meldbase-offline-full-verification", "cleanup-and-parent-fsync",
	}
	if len(receipt.Checks) != len(wantChecks) {
		return errors.New("incomplete fixed capability check set")
	}
	for index, check := range receipt.Checks {
		if check.Name != wantChecks[index] || !check.Passed || check.Duration <= 0 || check.Error != "" {
			return fmt.Errorf("capability check %d is invalid", index+1)
		}
	}
	proof := receipt.Database
	if proof == nil || proof.VerificationSchema != 3 || proof.FormatRevision != 3 || proof.CommitSequence != 3 || proof.FileBytes == 0 ||
		proof.PhysicalPages == 0 || proof.ReachablePages == 0 || proof.ReachablePages > proof.PhysicalPages ||
		!proof.IndexVerified || !proof.FreeSpaceValid || !qualificationHexDigest(proof.SHA256) {
		return errors.New("invalid indexed database verification proof")
	}
	return nil
}

func validateQualificationSoak(receipt qualificationSoakReceipt, durability durabilityCheckResult, revision string) error {
	if receipt.SchemaVersion != 4 || receipt.FormatRevision != 3 || receipt.Engine != "v2" || receipt.Profile != "release" ||
		!receipt.RaceEnabled || receipt.SourceRevision != revision || receipt.BuildRevision != revision || receipt.BuildModified {
		return errors.New("requires a race-enabled clean schema-4 release receipt whose binary matches the release revision")
	}
	if receipt.GOOS != durability.GOOS || receipt.GOARCH != durability.GOARCH || receipt.GoVersion != durability.GoVersion || receipt.Device != durability.Device ||
		receipt.FilesystemType != durability.FilesystemType || receipt.FilesystemName != durability.FilesystemName || receipt.BlockSize != durability.BlockSize {
		return errors.New("runtime or target volume identity does not match durability receipt")
	}
	if receipt.GoVersion == "" || !qualificationTimingValid(receipt.StartedAt, receipt.FinishedAt, receipt.ActualDuration) ||
		receipt.RequestedSeconds < 4*60*60 || receipt.RequestedSeconds > 6*60*60 ||
		receipt.ConcurrentDuration > receipt.ActualDuration ||
		receipt.ActualDuration < time.Duration(receipt.RequestedSeconds)*time.Second ||
		receipt.Documents < 10_000 || receipt.Documents > 1_000_000 || receipt.RequestedReopens < 12 || receipt.RequestedReopens > 1_000 ||
		receipt.CompletedReopens != receipt.RequestedReopens ||
		len(receipt.Phases) != receipt.RequestedReopens {
		return errors.New("release duration, document, reopen or timing floor not met")
	}
	var writes, reads, batches, attempts, conflicts uint64
	var previousSequence uint64
	var phaseDuration, concurrentDuration time.Duration
	for index, phase := range receipt.Phases {
		if phase.Ordinal != index+1 || phase.Duration <= 0 || phase.ConcurrentDuration <= 0 || phase.ConcurrentDuration > phase.Duration ||
			phase.Writes == 0 || phase.SnapshotReads == 0 ||
			phase.IndexBuildBatches == 0 || phase.ReclamationAttempts == 0 || phase.CommitSequence <= previousSequence || phase.PhysicalPages == 0 ||
			phase.IndexBuildPhase < 1 || phase.IndexBuildPhase > 3 {
			return fmt.Errorf("phase %d is incomplete or out of order", index+1)
		}
		previousSequence = phase.CommitSequence
		if !qualificationAddCounter(&writes, phase.Writes) || !qualificationAddCounter(&reads, phase.SnapshotReads) ||
			!qualificationAddCounter(&batches, phase.IndexBuildBatches) || !qualificationAddCounter(&attempts, phase.ReclamationAttempts) ||
			!qualificationAddCounter(&conflicts, phase.ReclamationConflicts) || phase.Duration > time.Duration(1<<63-1)-phaseDuration ||
			phase.ConcurrentDuration > time.Duration(1<<63-1)-concurrentDuration {
			return fmt.Errorf("phase %d counters or duration overflow", index+1)
		}
		phaseDuration += phase.Duration
		concurrentDuration += phase.ConcurrentDuration
	}
	if receipt.Writes != writes || receipt.SnapshotReads != reads || receipt.IndexBuildBatches != batches ||
		receipt.ReclamationAttempts != attempts || receipt.ReclamationConflicts != conflicts || conflicts == 0 ||
		receipt.ConcurrentDuration != concurrentDuration || concurrentDuration < time.Duration(receipt.RequestedSeconds)*time.Second ||
		phaseDuration > receipt.ActualDuration {
		return errors.New("aggregate worker evidence does not match phase totals")
	}
	if receipt.FinalCommitSequence < previousSequence || receipt.FinalFileBytes == 0 || receipt.FinalPhysicalPages == 0 ||
		receipt.FinalReachablePages == 0 || receipt.FinalReachablePages > receipt.FinalPhysicalPages ||
		!qualificationHexDigest(receipt.FinalFileSHA256) || !receipt.PersistentFreeSpace ||
		!receipt.FreeSpaceValid || !receipt.SemanticIndexes || !receipt.SemanticIndexBuilds || !receipt.FinalIndexBuildAbsent {
		return errors.New("final graph, index-build or FreeSpace verification is incomplete")
	}
	return nil
}

func validateQualificationDestructive(record qualificationDestructiveRecord, packet qualificationCheckResult) error {
	if record.SchemaVersion != 1 || record.SourceRevision != packet.SourceRevision || !qualificationSafeName(record.PlatformClass, 128) ||
		record.GOOS != packet.GOOS || record.GOARCH != packet.GOARCH || record.Device != packet.Device ||
		record.FilesystemType != packet.FilesystemType || record.FilesystemName != packet.FilesystemName || record.BlockSize != packet.BlockSize ||
		record.StartedAt.IsZero() || !record.FinishedAt.After(record.StartedAt) ||
		record.DurabilityReceiptSHA256 != packet.DurabilityReceiptSHA256 || record.SoakReceiptSHA256 != packet.SoakReceiptSHA256 {
		return errors.New("record identity, target volume or receipt bindings do not match")
	}
	if !record.CapacityExhaustion || !record.ProcessKill || !record.PowerCut || !record.PublicationBoundaries ||
		!record.LockReacquisition || !record.OldOrNewStateOnly || !record.OfflineVerification ||
		!record.KernelAndMountRecorded || !record.ControllerPolicyRecorded || !record.HostAndOperatorRecorded ||
		!qualificationHexDigest(record.SecuredArtifactsSHA256) {
		return errors.New("destructive capacity, kill, power-cut, recovery or artifact evidence is incomplete")
	}
	if !qualificationProductionFilesystem(record.FilesystemName) {
		return errors.New("filesystem class is not in the production qualification matrix")
	}
	return nil
}

func qualificationSHA256(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func qualificationHexDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func qualificationSafeName(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') && character != '.' && character != '_' && character != '-' {
			return false
		}
	}
	return true
}

func qualificationProductionFilesystem(name string) bool {
	switch name {
	case "ext-family", "xfs", "btrfs", "apfs":
		return true
	default:
		return false
	}
}

func qualificationAddCounter(total *uint64, value uint64) bool {
	if value > ^uint64(0)-*total {
		return false
	}
	*total += value
	return true
}

func qualificationTimingValid(started, finished time.Time, duration time.Duration) bool {
	if started.IsZero() || !finished.After(started) || duration <= 0 {
		return false
	}
	drift := finished.Sub(started) - duration
	if drift < 0 {
		drift = -drift
	}
	return drift <= 5*time.Second
}
