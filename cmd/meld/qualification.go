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
	SchemaVersion               uint32                          `json:"schemaVersion"`
	SourceRevision              string                          `json:"sourceRevision"`
	PlatformClass               string                          `json:"platformClass"`
	GOOS                        string                          `json:"goos"`
	GOARCH                      string                          `json:"goarch"`
	Device                      uint64                          `json:"device"`
	FilesystemType              string                          `json:"filesystemType"`
	FilesystemName              string                          `json:"filesystemName"`
	BlockSize                   uint64                          `json:"blockSize"`
	StartedAt                   time.Time                       `json:"startedAt"`
	FinishedAt                  time.Time                       `json:"finishedAt"`
	DurabilityReceiptSHA256     string                          `json:"durabilityReceiptSha256"`
	SoakReceiptSHA256           string                          `json:"soakReceiptSha256"`
	ProcessReceiptSHA256        string                          `json:"processReceiptSha256"`
	CapacityReceiptSHA256       string                          `json:"capacityReceiptSha256"`
	CorruptionReceiptSHA256     string                          `json:"corruptionReceiptSha256"`
	PowerReceiptSHA256          []string                        `json:"powerReceiptSha256"`
	Trials                      []qualificationDestructiveTrial `json:"trials"`
	Infrastructure              qualificationInfrastructure     `json:"infrastructure"`
	SecuredArtifactsIndexSHA256 string                          `json:"securedArtifactsIndexSha256"`
	SessionPlanSHA256           string                          `json:"sessionPlanSha256"`
	SessionHeadEventSHA256      string                          `json:"sessionHeadEventSha256"`
	SessionExecutableSHA256     string                          `json:"sessionExecutableSha256"`
}

type qualificationDestructiveTrial struct {
	ID                   string    `json:"id"`
	Kind                 string    `json:"kind"`
	PublicationBoundary  string    `json:"publicationBoundary"`
	TriggerPoint         string    `json:"triggerPoint"`
	StartedAt            time.Time `json:"startedAt"`
	FinishedAt           time.Time `json:"finishedAt"`
	OldCommitSequence    uint64    `json:"oldCommitSequence"`
	NewCommitSequence    uint64    `json:"newCommitSequence"`
	RecoveredSequence    uint64    `json:"recoveredCommitSequence"`
	Outcome              string    `json:"outcome"`
	OldStateSHA256       string    `json:"oldStateSha256"`
	NewStateSHA256       string    `json:"newStateSha256"`
	RecoveredStateSHA256 string    `json:"recoveredStateSha256"`
	LockReacquired       bool      `json:"lockReacquired"`
	OfflineVerified      bool      `json:"offlineVerified"`
	DatabaseSHA256       string    `json:"databaseSha256"`
	ArtifactsSHA256      string    `json:"artifactsSha256"`
}

type qualificationInfrastructure struct {
	EnvironmentRecordSHA256 string `json:"environmentRecordSha256"`
	KernelAndMountSHA256    string `json:"kernelAndMountSha256"`
	ControllerPolicySHA256  string `json:"controllerPolicySha256"`
	HostAndOperatorSHA256   string `json:"hostAndOperatorSha256"`
	ControllerMethod        string `json:"controllerMethod"`
}

const (
	qualificationTrialCapacity         = "capacity-exhaustion"
	qualificationTrialProcess          = "process-kill"
	qualificationTrialPower            = "power-cut"
	qualificationAsyncBoundary         = "asynchronous"
	qualificationMinimumBoundaryTrials = 3
	qualificationMinimumProcessTrials  = 20
)

var qualificationPublicationBoundaries = [...]string{
	"after-page-write",
	"before-data-sync",
	"after-data-sync",
	"after-meta-write",
	"after-meta-sync",
}

type qualificationCheckResult struct {
	SchemaVersion                 uint32   `json:"schemaVersion"`
	SourceRevision                string   `json:"sourceRevision"`
	EvidenceLevel                 uint8    `json:"evidenceLevel"`
	StorageQualified              bool     `json:"storageQualified"`
	RollbackProtectionQualified   bool     `json:"rollbackProtectionQualified"`
	ProductionQualified           bool     `json:"productionQualified"`
	GOOS                          string   `json:"goos"`
	GOARCH                        string   `json:"goarch"`
	Device                        uint64   `json:"device"`
	FilesystemType                string   `json:"filesystemType"`
	FilesystemName                string   `json:"filesystemName"`
	BlockSize                     uint64   `json:"blockSize"`
	DurabilityReceiptSHA256       string   `json:"durabilityReceiptSha256"`
	SoakReceiptSHA256             string   `json:"soakReceiptSha256"`
	DestructiveRecordSHA256       string   `json:"destructiveRecordSha256,omitempty"`
	AnchorPublicKeySHA256         string   `json:"anchorPublicKeySha256,omitempty"`
	AnchorPhaseReceiptSHA256      []string `json:"anchorPhaseReceiptSha256,omitempty"`
	AnchorHistoryReceiptSHA256    string   `json:"anchorHistoryReceiptSha256,omitempty"`
	AnchorHistoryControllerSHA256 string   `json:"anchorHistoryControllerSha256,omitempty"`
	AnchorRunID                   string   `json:"anchorRunId,omitempty"`
	AnchorHistoryRunID            string   `json:"anchorHistoryRunId,omitempty"`
	AnchorConfigurationID         string   `json:"anchorConfigurationId,omitempty"`
	AnchorHistoryConfigurationID  string   `json:"anchorHistoryConfigurationId,omitempty"`
	Passed                        bool     `json:"passed"`
}

func runQualificationCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	durabilityPath := flags.String("durability-receipt", "", "schema-2 durability-check receipt from the target volume")
	soakPath := flags.String("soak-receipt", "", "schema-4 release-soak receipt from the same target volume")
	destructivePath := flags.String("destructive-record", "", "optional schema-6 secured destructive-test manifest")
	environmentPath := flags.String("environment-record", "", "machine-generated environment evidence required with a destructive record")
	artifactsRootPath := flags.String("artifacts-root", "", "complete secured artifact directory required with a destructive record")
	artifactsIndexPath := flags.String("artifacts-index", "", "machine-generated artifact index required with a destructive record")
	anchorPublicKeyPath := flags.String("anchor-public-key", "", "Ed25519 key that verifies all rollback-anchor qualification evidence")
	anchorHistoryReceiptPath := flags.String("anchor-history-receipt", "", "signed multi-agent history qualification receipt")
	anchorHistoryPath := flags.String("anchor-history", "", "exact schema-3 multi-agent controller history")
	var anchorPhasePaths anchorReceiptFlags
	flags.Var(&anchorPhasePaths, "anchor-phase-receipt", "ordered signed anchor phase receipt; repeat exactly five times")
	sourceRevision := flags.String("source-revision", "", "required 40- or 64-hex release revision")
	requireLevel := flags.Int("require-level", 3, "minimum evidence level: 3, 4 or 5")
	releaseSigningKeyPath := flags.String("release-signing-key", "", "private Ed25519 key for a signed Level 5 release packet")
	outputPath := flags.String("out", "", "new exclusive durable signed Level 5 release packet")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *durabilityPath == "" || *soakPath == "" || !validDurabilitySourceRevision(*sourceRevision) {
		return errors.New("qualification-check requires --durability-receipt, --soak-receipt and a 40- or 64-hex --source-revision")
	}
	if *requireLevel != 3 && *requireLevel != 4 && *requireLevel != 5 {
		return errors.New("qualification-check --require-level must be 3, 4 or 5")
	}
	if (*releaseSigningKeyPath == "") != (*outputPath == "") {
		return errors.New("qualification-check release signing requires both --release-signing-key and --out")
	}
	hasDestructive, hasEnvironment := *destructivePath != "", *environmentPath != ""
	hasArtifactsRoot, hasArtifactsIndex := *artifactsRootPath != "", *artifactsIndexPath != ""
	if hasDestructive != hasEnvironment || hasDestructive != hasArtifactsRoot || hasDestructive != hasArtifactsIndex {
		return errors.New("qualification-check destructive evidence requires --destructive-record, --environment-record, --artifacts-root and --artifacts-index together")
	}
	result, err := buildQualificationCheckResult(qualificationCheckInputs{
		durabilityPath: *durabilityPath, soakPath: *soakPath, destructivePath: *destructivePath,
		environmentPath: *environmentPath, artifactsRootPath: *artifactsRootPath, artifactsIndexPath: *artifactsIndexPath,
		anchorPublicKeyPath: *anchorPublicKeyPath, anchorPhasePaths: anchorPhasePaths,
		anchorHistoryReceiptPath: *anchorHistoryReceiptPath, anchorHistoryPath: *anchorHistoryPath, sourceRevision: *sourceRevision,
	})
	if err != nil {
		return err
	}
	if int(result.EvidenceLevel) < *requireLevel {
		return fmt.Errorf("qualification evidence level %d does not satisfy required level %d", result.EvidenceLevel, *requireLevel)
	}
	if *releaseSigningKeyPath != "" {
		if *requireLevel != 5 || result.EvidenceLevel != 5 {
			return errors.New("signed release packets require --require-level 5 and complete Level 5 evidence")
		}
		packet, err := newQualificationReleasePacket(result, *releaseSigningKeyPath)
		if err != nil {
			return err
		}
		if err := writeJSONExclusiveDurable(*outputPath, packet); err != nil {
			return err
		}
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(packet)
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

type qualificationCheckInputs struct {
	durabilityPath           string
	soakPath                 string
	destructivePath          string
	environmentPath          string
	artifactsRootPath        string
	artifactsIndexPath       string
	anchorPublicKeyPath      string
	anchorPhasePaths         []string
	anchorHistoryReceiptPath string
	anchorHistoryPath        string
	sourceRevision           string
}

func buildQualificationCheckResult(inputs qualificationCheckInputs) (qualificationCheckResult, error) {
	if inputs.durabilityPath == "" || inputs.soakPath == "" || !validDurabilitySourceRevision(inputs.sourceRevision) {
		return qualificationCheckResult{}, errors.New("qualification evidence requires durability, soak and a valid source revision")
	}
	var durability durabilityCheckResult
	durabilityRaw, err := readQualificationReceipt(inputs.durabilityPath, &durability)
	if err != nil {
		return qualificationCheckResult{}, fmt.Errorf("durability receipt: %w", err)
	}
	if err := validateQualificationDurability(durability, inputs.sourceRevision); err != nil {
		return qualificationCheckResult{}, fmt.Errorf("durability receipt: %w", err)
	}
	var soak qualificationSoakReceipt
	soakRaw, err := readQualificationReceipt(inputs.soakPath, &soak)
	if err != nil {
		return qualificationCheckResult{}, fmt.Errorf("soak receipt: %w", err)
	}
	if err := validateQualificationSoak(soak, durability, inputs.sourceRevision); err != nil {
		return qualificationCheckResult{}, fmt.Errorf("soak receipt: %w", err)
	}

	durabilityHash, soakHash := qualificationSHA256(durabilityRaw), qualificationSHA256(soakRaw)
	result := qualificationCheckResult{
		SchemaVersion: 2, SourceRevision: inputs.sourceRevision, EvidenceLevel: 3,
		GOOS: durability.GOOS, GOARCH: durability.GOARCH, Device: durability.Device,
		FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		DurabilityReceiptSHA256: durabilityHash, SoakReceiptSHA256: soakHash, Passed: true,
	}
	if inputs.destructivePath != "" {
		if inputs.environmentPath == "" || inputs.artifactsRootPath == "" || inputs.artifactsIndexPath == "" {
			return qualificationCheckResult{}, errors.New("destructive qualification requires environment evidence and the complete secured artifact root and index")
		}
		var destructive qualificationDestructiveRecord
		raw, err := readQualificationReceipt(inputs.destructivePath, &destructive)
		if err != nil {
			return qualificationCheckResult{}, fmt.Errorf("destructive record: %w", err)
		}
		if err := validateQualificationDestructive(destructive, result); err != nil {
			return qualificationCheckResult{}, fmt.Errorf("destructive record: %w", err)
		}
		artifacts, err := verifyQualificationArtifactIndex(inputs.artifactsRootPath, inputs.artifactsIndexPath, inputs.sourceRevision)
		if err != nil {
			return qualificationCheckResult{}, fmt.Errorf("secured artifacts: %w", err)
		}
		if qualificationSHA256(artifacts.Raw) != destructive.SecuredArtifactsIndexSHA256 {
			return qualificationCheckResult{}, errors.New("secured artifact index differs from destructive record binding")
		}
		var environment qualificationEnvironmentEvidence
		environmentRaw, err := readQualificationReceipt(inputs.environmentPath, &environment)
		if err != nil {
			return qualificationCheckResult{}, fmt.Errorf("qualification environment: %w", err)
		}
		if err := validateQualificationEnvironmentBinding(environment, environmentRaw, artifacts, durability, destructive, inputs.sourceRevision, inputs.environmentPath); err != nil {
			return qualificationCheckResult{}, fmt.Errorf("qualification environment: %w", err)
		}
		if err := verifyQualificationDestructiveOriginalEvidence(qualificationDestructiveRecomputeInputs{
			Record: destructive, Artifacts: artifacts, Durability: durability, DurabilityRaw: durabilityRaw, SoakRaw: soakRaw,
			Environment: environment, EnvironmentRaw: environmentRaw, EnvironmentPath: inputs.environmentPath, Revision: inputs.sourceRevision,
		}); err != nil {
			return qualificationCheckResult{}, fmt.Errorf("destructive original evidence: %w", err)
		}
		result.EvidenceLevel = 4
		result.StorageQualified = true
		result.DestructiveRecordSHA256 = qualificationSHA256(raw)
	}
	hasAnchorEvidence := inputs.anchorPublicKeyPath != "" || inputs.anchorHistoryReceiptPath != "" || inputs.anchorHistoryPath != "" || len(inputs.anchorPhasePaths) != 0
	if hasAnchorEvidence {
		if inputs.destructivePath == "" {
			return qualificationCheckResult{}, errors.New("rollback-anchor qualification cannot raise evidence without a destructive storage record")
		}
		bindings, err := validateQualificationAnchorEvidence(
			inputs.anchorPublicKeyPath, inputs.anchorPhasePaths, inputs.anchorHistoryReceiptPath, inputs.anchorHistoryPath, inputs.sourceRevision,
		)
		if err != nil {
			return qualificationCheckResult{}, fmt.Errorf("rollback-anchor evidence: %w", err)
		}
		result.EvidenceLevel = 5
		result.RollbackProtectionQualified = true
		result.ProductionQualified = true
		result.AnchorPublicKeySHA256 = bindings.PublicKeySHA256
		result.AnchorPhaseReceiptSHA256 = bindings.PhaseReceiptSHA256
		result.AnchorHistoryReceiptSHA256 = bindings.HistoryReceiptSHA256
		result.AnchorHistoryControllerSHA256 = bindings.HistoryControllerSHA256
		result.AnchorRunID = bindings.RunID
		result.AnchorHistoryRunID = bindings.HistoryRunID
		result.AnchorConfigurationID = bindings.ConfigurationID
		result.AnchorHistoryConfigurationID = bindings.HistoryConfigurationID
	}
	return result, nil
}

type qualificationAnchorBindings struct {
	PublicKeySHA256         string
	PhaseReceiptSHA256      []string
	HistoryReceiptSHA256    string
	HistoryControllerSHA256 string
	RunID                   string
	HistoryRunID            string
	ConfigurationID         string
	HistoryConfigurationID  string
}

func validateQualificationAnchorEvidence(publicKeyPath string, phasePaths []string, historyReceiptPath, historyPath, revision string) (qualificationAnchorBindings, error) {
	if publicKeyPath == "" || len(phasePaths) != len(anchorQualificationPhases) || historyReceiptPath == "" || historyPath == "" {
		return qualificationAnchorBindings{}, errors.New("Level 5 requires one public key, five ordered phase receipts, one history receipt and its controller history")
	}
	publicKey, err := loadAnchorQualificationPublicKey(publicKeyPath)
	if err != nil {
		return qualificationAnchorBindings{}, err
	}
	chain, err := verifyAnchorQualificationChain(phasePaths, publicKey, true)
	if err != nil {
		return qualificationAnchorBindings{}, err
	}
	history, err := verifyAnchorHistoryQualificationFiles(historyReceiptPath, historyPath, publicKey)
	if err != nil {
		return qualificationAnchorBindings{}, err
	}
	finalPhase := chain.Receipts[len(chain.Receipts)-1]
	if finalPhase.SourceRevision != revision || finalPhase.BuildRevision != revision || finalPhase.BuildModified {
		return qualificationAnchorBindings{}, errors.New("anchor phase chain is not bound to the clean release revision")
	}
	if history.Receipt.SourceRevision != revision || history.Receipt.BuildRevision != revision || history.Receipt.BuildModified {
		return qualificationAnchorBindings{}, errors.New("anchor history receipt is not bound to the clean release revision")
	}
	if history.Controller.SourceRevision != revision || history.Controller.BuildRevision != revision || history.Controller.BuildModified {
		return qualificationAnchorBindings{}, errors.New("anchor history controller is not bound to the clean release revision")
	}
	for _, fragment := range history.Controller.Fragments {
		if fragment.SourceRevision != revision || fragment.BuildRevision != revision || fragment.BuildModified {
			return qualificationAnchorBindings{}, fmt.Errorf("anchor history agent %q is not bound to the clean release revision", fragment.Request.AgentID)
		}
	}
	if chain.RunID == history.Receipt.RunID || chain.ConfigurationID == history.Receipt.ConfigurationID {
		return qualificationAnchorBindings{}, errors.New("phase and concurrent-history qualifications must use separate runs and disposable anchor configurations")
	}
	for _, phase := range chain.Receipts {
		if phase.ExternalEvidenceSHA256 == history.Receipt.ExternalEvidenceSHA256 {
			return qualificationAnchorBindings{}, errors.New("history qualification reuses phase external evidence")
		}
	}
	return qualificationAnchorBindings{
		PublicKeySHA256: qualificationSHA256(publicKey), PhaseReceiptSHA256: append([]string(nil), chain.ReceiptSHA256...),
		HistoryReceiptSHA256: history.ReceiptSHA256, HistoryControllerSHA256: history.ControllerSHA256,
		RunID: chain.RunID, HistoryRunID: history.Receipt.RunID, ConfigurationID: chain.ConfigurationID, HistoryConfigurationID: history.Receipt.ConfigurationID,
	}, nil
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
	if record.SchemaVersion != 6 || record.SourceRevision != packet.SourceRevision || !qualificationSafeName(record.PlatformClass, 128) ||
		record.GOOS != packet.GOOS || record.GOARCH != packet.GOARCH || record.Device != packet.Device ||
		record.FilesystemType != packet.FilesystemType || record.FilesystemName != packet.FilesystemName || record.BlockSize != packet.BlockSize ||
		record.StartedAt.IsZero() || !record.FinishedAt.After(record.StartedAt) ||
		record.DurabilityReceiptSHA256 != packet.DurabilityReceiptSHA256 || record.SoakReceiptSHA256 != packet.SoakReceiptSHA256 {
		return errors.New("record identity, target volume or receipt bindings do not match")
	}
	if !qualificationProductionFilesystem(record.FilesystemName) {
		return errors.New("filesystem class is not in the production qualification matrix")
	}
	if !qualificationHexDigest(record.SecuredArtifactsIndexSHA256) || !qualificationHexDigest(record.SessionPlanSHA256) ||
		!qualificationHexDigest(record.SessionHeadEventSHA256) || !qualificationHexDigest(record.SessionExecutableSHA256) ||
		!qualificationHexDigest(record.ProcessReceiptSHA256) || !qualificationHexDigest(record.CapacityReceiptSHA256) || !qualificationHexDigest(record.CorruptionReceiptSHA256) ||
		len(record.PowerReceiptSHA256) == 0 ||
		!qualificationHexDigest(record.Infrastructure.EnvironmentRecordSHA256) ||
		!qualificationHexDigest(record.Infrastructure.KernelAndMountSHA256) ||
		!qualificationHexDigest(record.Infrastructure.ControllerPolicySHA256) ||
		!qualificationHexDigest(record.Infrastructure.HostAndOperatorSHA256) || !qualificationPowerMethod(record.Infrastructure.ControllerMethod) {
		return errors.New("secured infrastructure or aggregate artifact evidence is incomplete")
	}
	for index, digest := range record.PowerReceiptSHA256 {
		if !qualificationHexDigest(digest) {
			return fmt.Errorf("power receipt hash %d is invalid", index+1)
		}
	}
	if err := validateQualificationDestructiveTrials(record); err != nil {
		return err
	}
	return nil
}

func validateQualificationDestructiveTrials(record qualificationDestructiveRecord) error {
	if len(record.Trials) == 0 || len(record.Trials) > 10_000 {
		return errors.New("destructive trial set must contain between 1 and 10000 trials")
	}
	seenIDs := make(map[string]struct{}, len(record.Trials))
	coverage := make(map[string]map[string]int, 3)
	processTriggers := make(map[string]int, 2)
	for index, trial := range record.Trials {
		if !qualificationSafeName(trial.ID, 128) {
			return fmt.Errorf("destructive trial %d has an invalid id", index+1)
		}
		if _, exists := seenIDs[trial.ID]; exists {
			return fmt.Errorf("destructive trial %d duplicates id %q", index+1, trial.ID)
		}
		seenIDs[trial.ID] = struct{}{}
		if trial.Kind != qualificationTrialCapacity && trial.Kind != qualificationTrialProcess && trial.Kind != qualificationTrialPower {
			return fmt.Errorf("destructive trial %s has an unknown kind", trial.ID)
		}
		if !qualificationBoundaryAllowed(trial.Kind, trial.PublicationBoundary) {
			return fmt.Errorf("destructive trial %s has an invalid publication boundary", trial.ID)
		}
		if !qualificationSafeName(trial.TriggerPoint, 128) {
			return fmt.Errorf("destructive trial %s has an invalid trigger point", trial.ID)
		}
		switch trial.Kind {
		case qualificationTrialCapacity:
			if trial.TriggerPoint != "real-enospc-at-boundary" {
				return fmt.Errorf("destructive trial %s is not bound to a real ENOSPC trigger", trial.ID)
			}
		case qualificationTrialProcess:
			if trial.TriggerPoint != "oracle-prepared" && trial.TriggerPoint != "oracle-committed" {
				return fmt.Errorf("destructive trial %s has an invalid process-kill trigger", trial.ID)
			}
			processTriggers[trial.TriggerPoint]++
		case qualificationTrialPower:
			if trial.TriggerPoint != "external-power-cut" {
				return fmt.Errorf("destructive trial %s is not bound to an external power-cut trigger", trial.ID)
			}
		}
		if trial.StartedAt.Before(record.StartedAt) || trial.FinishedAt.After(record.FinishedAt) || !trial.FinishedAt.After(trial.StartedAt) {
			return fmt.Errorf("destructive trial %s has invalid timing", trial.ID)
		}
		if trial.OldCommitSequence == 0 || trial.NewCommitSequence != trial.OldCommitSequence+1 ||
			(trial.RecoveredSequence != trial.OldCommitSequence && trial.RecoveredSequence != trial.NewCommitSequence) {
			return fmt.Errorf("destructive trial %s did not recover exactly the old or new generation", trial.ID)
		}
		wantOutcome := "old"
		if trial.RecoveredSequence == trial.NewCommitSequence {
			wantOutcome = "new"
		}
		if trial.Outcome != wantOutcome {
			return fmt.Errorf("destructive trial %s outcome does not match its recovered sequence", trial.ID)
		}
		if !qualificationHexDigest(trial.OldStateSHA256) || !qualificationHexDigest(trial.NewStateSHA256) ||
			!qualificationHexDigest(trial.RecoveredStateSHA256) || trial.OldStateSHA256 == trial.NewStateSHA256 {
			return fmt.Errorf("destructive trial %s lacks distinct old/new state proofs", trial.ID)
		}
		wantState := trial.OldStateSHA256
		if wantOutcome == "new" {
			wantState = trial.NewStateSHA256
		}
		if trial.RecoveredStateSHA256 != wantState {
			return fmt.Errorf("destructive trial %s recovered state does not match its outcome", trial.ID)
		}
		if !trial.LockReacquired || !trial.OfflineVerified || !qualificationHexDigest(trial.DatabaseSHA256) ||
			!qualificationHexDigest(trial.ArtifactsSHA256) {
			return fmt.Errorf("destructive trial %s lacks lock, offline verification or artifact evidence", trial.ID)
		}
		if coverage[trial.Kind] == nil {
			coverage[trial.Kind] = make(map[string]int)
		}
		coverage[trial.Kind][trial.PublicationBoundary]++
	}
	for _, boundary := range qualificationPublicationBoundaries {
		if coverage[qualificationTrialCapacity][boundary] < qualificationMinimumBoundaryTrials {
			return fmt.Errorf("capacity-exhaustion evidence requires at least %d trials at %s", qualificationMinimumBoundaryTrials, boundary)
		}
		if coverage[qualificationTrialPower][boundary] < qualificationMinimumBoundaryTrials {
			return fmt.Errorf("power-cut evidence requires at least %d trials at %s", qualificationMinimumBoundaryTrials, boundary)
		}
	}
	if coverage[qualificationTrialProcess][qualificationAsyncBoundary] < qualificationMinimumProcessTrials {
		return fmt.Errorf("process-kill evidence requires at least %d asynchronous trials", qualificationMinimumProcessTrials)
	}
	if processTriggers["oracle-prepared"] < qualificationMinimumProcessTrials/2 ||
		processTriggers["oracle-committed"] < qualificationMinimumProcessTrials/2 {
		return errors.New("process-kill evidence requires at least 10 prepared and 10 committed trigger trials")
	}
	return nil
}

func qualificationBoundaryAllowed(kind, boundary string) bool {
	if kind == qualificationTrialProcess {
		return boundary == qualificationAsyncBoundary
	}
	for _, candidate := range qualificationPublicationBoundaries {
		if boundary == candidate {
			return true
		}
	}
	return false
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
