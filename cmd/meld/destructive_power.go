package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

const (
	destructivePowerMarkerSchema  uint32 = 1
	destructivePowerEventSchema   uint32 = 1
	destructivePowerReceiptSchema uint32 = 1
)

var destructiveBootIDFn = destructiveBootID

type destructivePowerMarker struct {
	SchemaVersion     uint32    `json:"schemaVersion"`
	TrialID           string    `json:"trialId"`
	Boundary          string    `json:"boundary"`
	SourceRevision    string    `json:"sourceRevision,omitempty"`
	BuildRevision     string    `json:"buildRevision,omitempty"`
	BuildModified     bool      `json:"buildModified"`
	GOOS              string    `json:"goos"`
	GOARCH            string    `json:"goarch"`
	Device            uint64    `json:"device"`
	FilesystemType    string    `json:"filesystemType"`
	FilesystemName    string    `json:"filesystemName"`
	BlockSize         uint64    `json:"blockSize"`
	DatabaseRelative  string    `json:"databaseRelative"`
	BootIDBefore      string    `json:"bootIdBefore"`
	StartedAt         time.Time `json:"startedAt"`
	ReachedAt         time.Time `json:"reachedAt"`
	OldCommitSequence uint64    `json:"oldCommitSequence"`
	NewCommitSequence uint64    `json:"newCommitSequence"`
	OldStateSHA256    string    `json:"oldStateSha256"`
	NewStateSHA256    string    `json:"newStateSha256"`
}

type destructivePowerControllerEvent struct {
	SchemaVersion         uint32    `json:"schemaVersion"`
	TrialID               string    `json:"trialId"`
	Method                string    `json:"method"`
	MarkerSHA256          string    `json:"markerSha256"`
	BootIDBefore          string    `json:"bootIdBefore"`
	MarkerObservedAt      time.Time `json:"markerObservedAt"`
	CutRequestedAt        time.Time `json:"cutRequestedAt"`
	PowerRestoredAt       time.Time `json:"powerRestoredAt"`
	ControllerProofSHA256 string    `json:"controllerProofSha256"`
	ControllerRunID       string    `json:"controllerRunId,omitempty"`
	ControllerPublicKey   string    `json:"controllerPublicKey,omitempty"`
	Signature             string    `json:"signature,omitempty"`
}

type destructivePowerEvidence struct {
	TrialID                        string `json:"trialId"`
	Boundary                       string `json:"boundary"`
	Method                         string `json:"method"`
	BootIDBefore                   string `json:"bootIdBefore"`
	BootIDAfter                    string `json:"bootIdAfter"`
	MarkerSHA256                   string `json:"markerSha256"`
	ControllerSHA256               string `json:"controllerEventSha256"`
	ControllerProofSHA256          string `json:"controllerProofSha256"`
	ControllerPublicKeySHA256      string `json:"controllerPublicKeySha256,omitempty"`
	ControllerTargetIdentitySHA256 string `json:"controllerTargetIdentitySha256,omitempty"`
	MarkerArtifact                 string `json:"markerArtifact"`
	ControllerArtifact             string `json:"controllerEventArtifact"`
	ControllerProofArtifact        string `json:"controllerProofArtifact"`
	DatabaseArtifact               string `json:"databaseArtifact"`
	MetaGeneration                 uint64 `json:"metaGeneration"`
	CommitSequence                 uint64 `json:"commitSequence"`
	ValidMetaSlots                 int    `json:"validMetaSlots"`
	PhysicalPages                  uint64 `json:"physicalPages"`
	ReachablePages                 uint64 `json:"reachablePages"`
	FreeSpaceValid                 bool   `json:"freeSpaceValid"`
}

type destructivePowerReceipt struct {
	SchemaVersion  uint32                        `json:"schemaVersion"`
	SourceRevision string                        `json:"sourceRevision,omitempty"`
	BuildRevision  string                        `json:"buildRevision,omitempty"`
	BuildModified  bool                          `json:"buildModified"`
	GOOS           string                        `json:"goos"`
	GOARCH         string                        `json:"goarch"`
	GoVersion      string                        `json:"goVersion"`
	Device         uint64                        `json:"device"`
	FilesystemType string                        `json:"filesystemType"`
	FilesystemName string                        `json:"filesystemName"`
	BlockSize      uint64                        `json:"blockSize"`
	Trial          qualificationDestructiveTrial `json:"trial"`
	Evidence       destructivePowerEvidence      `json:"powerEvidence"`
}

type destructivePowerCheckResult struct {
	SchemaVersion uint32 `json:"schemaVersion"`
	ReceiptSHA256 string `json:"receiptSha256"`
	TrialID       string `json:"trialId"`
	Boundary      string `json:"boundary"`
	Outcome       string `json:"outcome"`
	Passed        bool   `json:"passed"`
}

type destructivePowerMatrixResult struct {
	SchemaVersion    uint32         `json:"schemaVersion"`
	ControllerMethod string         `json:"controllerMethod"`
	SourceRevision   string         `json:"sourceRevision,omitempty"`
	BuildRevision    string         `json:"buildRevision,omitempty"`
	BuildModified    bool           `json:"buildModified"`
	GOOS             string         `json:"goos"`
	GOARCH           string         `json:"goarch"`
	Device           uint64         `json:"device"`
	FilesystemType   string         `json:"filesystemType"`
	FilesystemName   string         `json:"filesystemName"`
	BlockSize        uint64         `json:"blockSize"`
	ReceiptCount     int            `json:"receiptCount"`
	Coverage         map[string]int `json:"coverage"`
	OldOutcomes      int            `json:"oldOutcomes"`
	NewOutcomes      int            `json:"newOutcomes"`
	ReceiptSHA256    []string       `json:"receiptSha256"`
	AggregateSHA256  string         `json:"aggregateSha256"`
	Passed           bool           `json:"passed"`
}

func runDestructivePowerReceiptCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-power-receipt-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	receiptPath := flags.String("receipt", "", "power recovery receipt whose retained artifacts will be reopened")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *receiptPath == "" {
		return errors.New("destructive-power-receipt-check requires --receipt")
	}
	var receipt destructivePowerReceipt
	raw, err := readQualificationReceipt(*receiptPath, &receipt)
	if err != nil {
		return err
	}
	if err := validateDestructivePowerReceipt(receipt); err != nil {
		return err
	}
	result := destructivePowerCheckResult{
		SchemaVersion: 1, ReceiptSHA256: qualificationSHA256(raw), TrialID: receipt.Trial.ID,
		Boundary: receipt.Trial.PublicationBoundary, Outcome: receipt.Trial.Outcome, Passed: true,
	}
	encoder := json.NewEncoder(stdout)
	return encoder.Encode(result)
}

func runDestructivePowerMatrixCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-power-matrix-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var receiptPaths destructivePowerReceiptFlags
	flags.Var(&receiptPaths, "receipt", "power recovery receipt; repeat for the complete matrix")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(receiptPaths) < qualificationMinimumBoundaryTrials*len(qualificationPublicationBoundaries) || len(receiptPaths) > 1_000 {
		return fmt.Errorf("destructive-power-matrix-check requires between %d and 1000 --receipt values", qualificationMinimumBoundaryTrials*len(qualificationPublicationBoundaries))
	}
	result := destructivePowerMatrixResult{SchemaVersion: 1, Coverage: make(map[string]int, len(qualificationPublicationBoundaries))}
	seenTrials := make(map[string]struct{}, len(receiptPaths))
	seenReceipts := make(map[string]struct{}, len(receiptPaths))
	seenBootTransitions := make(map[string]struct{}, len(receiptPaths))
	aggregate := sha256.New()
	var baseline destructivePowerReceipt
	for index, path := range receiptPaths {
		var receipt destructivePowerReceipt
		raw, err := readQualificationReceipt(path, &receipt)
		if err != nil {
			return fmt.Errorf("power receipt %d: %w", index+1, err)
		}
		if err := validateDestructivePowerReceipt(receipt); err != nil {
			return fmt.Errorf("power receipt %d: %w", index+1, err)
		}
		digest := qualificationSHA256(raw)
		if _, exists := seenReceipts[digest]; exists {
			return fmt.Errorf("power receipt %d duplicates an exact receipt", index+1)
		}
		seenReceipts[digest] = struct{}{}
		if _, exists := seenTrials[receipt.Trial.ID]; exists {
			return fmt.Errorf("power receipt %d duplicates trial %q", index+1, receipt.Trial.ID)
		}
		seenTrials[receipt.Trial.ID] = struct{}{}
		bootTransition := receipt.Evidence.BootIDBefore + "\x00" + receipt.Evidence.BootIDAfter
		if _, exists := seenBootTransitions[bootTransition]; exists {
			return fmt.Errorf("power receipt %d duplicates a boot transition", index+1)
		}
		seenBootTransitions[bootTransition] = struct{}{}
		if index == 0 {
			baseline = receipt
			result.ControllerMethod = receipt.Evidence.Method
			result.SourceRevision, result.BuildRevision, result.BuildModified = receipt.SourceRevision, receipt.BuildRevision, receipt.BuildModified
			result.GOOS, result.GOARCH, result.Device = receipt.GOOS, receipt.GOARCH, receipt.Device
			result.FilesystemType, result.FilesystemName, result.BlockSize = receipt.FilesystemType, receipt.FilesystemName, receipt.BlockSize
		} else if receipt.Evidence.Method != baseline.Evidence.Method {
			return fmt.Errorf("power receipt %d uses controller method %q instead of %q", index+1, receipt.Evidence.Method, baseline.Evidence.Method)
		} else if receipt.SourceRevision != baseline.SourceRevision || receipt.BuildRevision != baseline.BuildRevision ||
			receipt.BuildModified != baseline.BuildModified || receipt.GOOS != baseline.GOOS || receipt.GOARCH != baseline.GOARCH ||
			receipt.GoVersion != baseline.GoVersion || receipt.Device != baseline.Device || receipt.FilesystemType != baseline.FilesystemType ||
			receipt.FilesystemName != baseline.FilesystemName || receipt.BlockSize != baseline.BlockSize || receipt.Evidence.ControllerPublicKeySHA256 != baseline.Evidence.ControllerPublicKeySHA256 || receipt.Evidence.ControllerTargetIdentitySHA256 != baseline.Evidence.ControllerTargetIdentitySHA256 {
			return fmt.Errorf("power receipt %d does not share the matrix build, runtime and volume identity", index+1)
		}
		result.Coverage[receipt.Trial.PublicationBoundary]++
		if receipt.Trial.Outcome == "old" {
			result.OldOutcomes++
		} else {
			result.NewOutcomes++
		}
		result.ReceiptSHA256 = append(result.ReceiptSHA256, digest)
		decoded, _ := hex.DecodeString(digest)
		_, _ = aggregate.Write(decoded)
	}
	for _, boundary := range qualificationPublicationBoundaries {
		if result.Coverage[boundary] < qualificationMinimumBoundaryTrials {
			return fmt.Errorf("power matrix requires at least %d trials at %s", qualificationMinimumBoundaryTrials, boundary)
		}
	}
	result.ReceiptCount = len(receiptPaths)
	result.AggregateSHA256 = hex.EncodeToString(aggregate.Sum(nil))
	result.Passed = true
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func runDestructivePowerPrepare(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-power-prepare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", "", "root of the disposable independently mounted target volume")
	controlDirectory := flags.String("control-dir", "", "existing marker directory on a different device")
	markerPath := flags.String("marker", "", "new marker path directly inside --control-dir")
	trialID := flags.String("trial-id", "", "unique secured trial identifier")
	boundaryName := flags.String("boundary", "", "publication boundary")
	token := flags.String("destructive-token", "", "exact token emitted by destructive-volume-check")
	sourceRevision := flags.String("source-revision", "", "optional 40- or 64-hex source revision")
	requireClean := flags.Bool("require-clean-source", false, "require a clean binary matching --source-revision")
	if err := flags.Parse(args); err != nil {
		return err
	}
	boundary, boundaryOK := destructiveQualificationBoundary(*boundaryName)
	if *directory == "" || *controlDirectory == "" || *markerPath == "" || !qualificationSafeName(*trialID, 64) || !boundaryOK {
		return errors.New("destructive-power-prepare requires --dir, --control-dir, --marker, a safe --trial-id and valid --boundary")
	}
	if *sourceRevision != "" && !validDurabilitySourceRevision(*sourceRevision) {
		return errors.New("destructive-power-prepare --source-revision must be 40 or 64 hexadecimal characters")
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if *requireClean && (*sourceRevision == "" || buildRevision != *sourceRevision || buildModified) {
		return errors.New("destructive-power-prepare clean source verification failed")
	}
	facts, err := inspectDestructiveVolume(*directory, *controlDirectory)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolumeFacts(facts); err != nil {
		return err
	}
	if subtle.ConstantTimeCompare([]byte(*token), []byte(destructiveVolumeToken(facts))) != 1 {
		return errors.New("destructive power token mismatch; run destructive-volume-check again")
	}
	cleanMarker, err := filepath.Abs(filepath.Clean(*markerPath))
	if err != nil || filepath.Dir(cleanMarker) != facts.controlDirectory {
		return errors.New("power marker must be a new file directly inside --control-dir")
	}
	if _, err := os.Lstat(cleanMarker); err == nil {
		return errors.New("power marker already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	bootID, err := destructiveBootIDFn()
	if err != nil {
		return err
	}
	relativeDirectory := ".meldbase-power-" + *trialID
	targetTrial := filepath.Join(facts.directory, relativeDirectory)
	if err := os.Mkdir(targetTrial, 0o700); err != nil {
		return err
	}
	if err := syncProbeDirectory(facts.directory); err != nil {
		return err
	}
	databasePath := filepath.Join(targetTrial, "power.meld")
	oldState, newState := []byte("old"), []byte("new")
	if err := seedDestructiveCapacityDatabase(databasePath, oldState); err != nil {
		return err
	}
	started := time.Now().UTC()
	reached := false
	file, _, _, err := storagev2.OpenForQualification(databasePath, storagev2.OpenOptions{}, func(current storagev2.QualificationBoundary) error {
		if reached || current != boundary {
			return nil
		}
		reached = true
		marker := destructivePowerMarker{
			SchemaVersion: destructivePowerMarkerSchema, TrialID: *trialID, Boundary: string(current),
			SourceRevision: *sourceRevision, BuildRevision: buildRevision, BuildModified: buildModified,
			GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Device: facts.device, FilesystemType: facts.filesystemType,
			FilesystemName: facts.filesystemName, BlockSize: facts.blockSize,
			DatabaseRelative: filepath.Join(relativeDirectory, "power.meld"), BootIDBefore: bootID,
			StartedAt: started, ReachedAt: time.Now().UTC(), OldCommitSequence: 1, NewCommitSequence: 2,
			OldStateSHA256: bytesSHA256(oldState), NewStateSHA256: bytesSHA256(newState),
		}
		if err := writeJSONExclusiveDurable(cleanMarker, marker); err != nil {
			return err
		}
		waitForDestructiveKill()
		return nil
	})
	if err != nil {
		return err
	}
	defer file.Close()
	id := [16]byte{15: 1}
	_, err = file.ApplyDocumentTransaction(storagev2.DocumentTransaction{TransactionID: [16]byte{2}, Mutations: []storagev2.DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: storagev2.DocumentUpdate, Document: newState,
	}}})
	if err != nil {
		return err
	}
	if !reached {
		return errors.New("selected power publication boundary was not reached")
	}
	return errors.New("power qualification worker resumed without an external reset")
}

func runDestructivePowerRecover(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-power-recover", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", "", "root of the disposable target volume after reboot")
	controlDirectory := flags.String("control-dir", "", "control/evidence directory on a different device")
	markerPath := flags.String("marker", "", "prepare marker")
	controllerPath := flags.String("controller-event", "", "external hard-reset controller event")
	controllerProofPath := flags.String("controller-proof", "", "secured controller transcript or hardware log")
	controllerPublicKeyPath := flags.String("controller-public-key", "", "trusted physical controller-agent public key")
	output := flags.String("out", "", "new power recovery receipt directly inside --control-dir")
	token := flags.String("destructive-token", "", "exact token emitted before the cut")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *directory == "" || *controlDirectory == "" || *markerPath == "" || *controllerPath == "" || *controllerProofPath == "" || *output == "" {
		return errors.New("destructive-power-recover requires --dir, --control-dir, --marker, --controller-event, --controller-proof and --out")
	}
	facts, err := inspectDestructiveVolume(*directory, *controlDirectory)
	if err != nil {
		return err
	}
	recoveryFacts := facts
	recoveryFacts.empty = true
	if err := validateDestructiveVolumeFacts(recoveryFacts); err != nil {
		return err
	}
	if subtle.ConstantTimeCompare([]byte(*token), []byte(destructiveVolumeToken(facts))) != 1 {
		return errors.New("destructive power token mismatch; target geometry changed")
	}
	markerPathClean, controllerPathClean, controllerProofPathClean, outputClean, err := validateDestructivePowerRecoveryPaths(
		facts.controlDirectory, *markerPath, *controllerPath, *controllerProofPath, *output,
	)
	if err != nil {
		return err
	}
	var marker destructivePowerMarker
	markerRaw, err := readQualificationReceipt(markerPathClean, &marker)
	if err != nil {
		return fmt.Errorf("power marker: %w", err)
	}
	var controller destructivePowerControllerEvent
	controllerRaw, err := readQualificationReceipt(controllerPathClean, &controller)
	if err != nil {
		return fmt.Errorf("power controller event: %w", err)
	}
	controllerProofRaw, err := os.ReadFile(controllerProofPathClean)
	if err != nil {
		return fmt.Errorf("power controller proof: %w", err)
	}
	if len(controllerProofRaw) == 0 || len(controllerProofRaw) > qualificationReceiptMaxBytes || qualificationSHA256(controllerProofRaw) != controller.ControllerProofSHA256 {
		return errors.New("power controller proof is empty, oversized or does not match the controller event")
	}
	if controller.Method == "qemu-system-reset" {
		if err := validateDestructiveQMPProof(controllerProofRaw, controller, markerRaw); err != nil {
			return fmt.Errorf("power QMP proof: %w", err)
		}
	} else if controller.Method == "qemu-host-sigkill" {
		if err := validateDestructiveQMPKillProof(controllerProofRaw, controller, markerRaw); err != nil {
			return fmt.Errorf("power QEMU process-kill proof: %w", err)
		}
	} else {
		if *controllerPublicKeyPath == "" {
			return errors.New("physical power recovery requires --controller-public-key")
		}
		publicKey, err := loadAnchorQualificationPublicKey(*controllerPublicKeyPath)
		if err != nil {
			return fmt.Errorf("physical power controller public key: %w", err)
		}
		if err := verifyDestructivePhysicalPowerEvidence(controllerProofRaw, controller, markerRaw, publicKey); err != nil {
			return fmt.Errorf("physical power controller proof: %w", err)
		}
	}
	currentBootID, err := destructiveBootIDFn()
	if err != nil {
		return err
	}
	if err := validateDestructivePowerInputs(marker, markerRaw, controller, facts, currentBootID); err != nil {
		return err
	}
	databasePath := filepath.Join(facts.directory, marker.DatabaseRelative)
	if err := validatePowerTargetEntries(facts.directory, filepath.Dir(marker.DatabaseRelative)); err != nil {
		return err
	}
	artifactDirectory, err := os.MkdirTemp(facts.controlDirectory, ".meldbase-power-evidence-")
	if err != nil {
		return err
	}
	databaseArtifact := filepath.Join(artifactDirectory, "crash-image.meld")
	if err := copyFileExclusiveDurable(databaseArtifact, databasePath); err != nil {
		return err
	}
	verified, err := storagev2.VerifyPathContextWithIndexAudit(context.Background(), databasePath, func(storagev2.IndexMeta, [16]byte, []byte) ([]byte, bool, error) {
		return nil, false, errors.New("power fixture unexpectedly contains an index")
	})
	if err != nil {
		return err
	}
	recovered, meta, err := recoverDestructiveCapacityDatabase(databasePath)
	if err != nil {
		return err
	}
	if meta != verified.Meta || meta.CommitSequence != marker.OldCommitSequence && meta.CommitSequence != marker.NewCommitSequence {
		return errors.New("power recovery selected an invalid generation")
	}
	recoveredHash := bytesSHA256(recovered)
	outcome := "old"
	wantHash := marker.OldStateSHA256
	if meta.CommitSequence == marker.NewCommitSequence {
		outcome, wantHash = "new", marker.NewStateSHA256
	}
	if recoveredHash != wantHash {
		return errors.New("power recovery sequence and logical state disagree")
	}
	markerHash, controllerHash := qualificationSHA256(markerRaw), qualificationSHA256(controllerRaw)
	evidence := destructivePowerEvidence{
		TrialID: marker.TrialID, Boundary: marker.Boundary, Method: controller.Method,
		BootIDBefore: marker.BootIDBefore, BootIDAfter: currentBootID, MarkerSHA256: markerHash,
		ControllerSHA256: controllerHash, ControllerProofSHA256: controller.ControllerProofSHA256,
		MarkerArtifact: markerPathClean, ControllerArtifact: controllerPathClean, ControllerProofArtifact: controllerProofPathClean,
		DatabaseArtifact: databaseArtifact,
		MetaGeneration:   meta.Generation, CommitSequence: meta.CommitSequence, ValidMetaSlots: verified.ValidMetaSlots,
		PhysicalPages: verified.PhysicalPages, ReachablePages: verified.ReachablePages, FreeSpaceValid: verified.FreeSpaceValid,
	}
	if qualificationPhysicalPowerMethod(controller.Method) {
		publicKey, err := loadAnchorQualificationPublicKey(*controllerPublicKeyPath)
		if err != nil {
			return err
		}
		evidence.ControllerPublicKeySHA256 = qualificationSHA256(publicKey)
		var proof destructivePowerControllerProof
		if err := decodeOneStrictJSON(controllerProofRaw, &proof); err != nil {
			return err
		}
		evidence.ControllerTargetIdentitySHA256 = proof.Response.TargetIdentitySHA256
	}
	evidenceRaw, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	artifactHash := sha256.New()
	_, _ = artifactHash.Write(markerRaw)
	_, _ = artifactHash.Write(controllerRaw)
	_, _ = artifactHash.Write(controllerProofRaw)
	_, _ = artifactHash.Write(evidenceRaw)
	_, _ = artifactHash.Write(verified.SHA256[:])
	trial := qualificationDestructiveTrial{
		ID: marker.TrialID, Kind: qualificationTrialPower, PublicationBoundary: marker.Boundary, TriggerPoint: "external-power-cut",
		StartedAt: marker.StartedAt, FinishedAt: time.Now().UTC(), OldCommitSequence: marker.OldCommitSequence,
		NewCommitSequence: marker.NewCommitSequence, RecoveredSequence: meta.CommitSequence, Outcome: outcome,
		OldStateSHA256: marker.OldStateSHA256, NewStateSHA256: marker.NewStateSHA256, RecoveredStateSHA256: recoveredHash,
		LockReacquired: true, OfflineVerified: verified.SemanticIndexesVerified && verified.SemanticIndexBuildsVerified && verified.FreeSpaceValid,
		DatabaseSHA256: hex.EncodeToString(verified.SHA256[:]), ArtifactsSHA256: hex.EncodeToString(artifactHash.Sum(nil)),
	}
	receipt := destructivePowerReceipt{
		SchemaVersion: destructivePowerReceiptSchema, SourceRevision: marker.SourceRevision,
		BuildRevision: marker.BuildRevision, BuildModified: marker.BuildModified, GOOS: marker.GOOS, GOARCH: marker.GOARCH,
		GoVersion: runtime.Version(), Device: marker.Device, FilesystemType: marker.FilesystemType,
		FilesystemName: marker.FilesystemName, BlockSize: marker.BlockSize, Trial: trial, Evidence: evidence,
	}
	if !trial.OfflineVerified {
		return errors.New("power recovery did not pass complete offline verification")
	}
	if err := writeJSONExclusiveDurable(outputClean, receipt); err != nil {
		return err
	}
	if err := errors.Join(os.RemoveAll(filepath.Join(facts.directory, filepath.Dir(marker.DatabaseRelative))), syncProbeDirectory(facts.directory)); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(receipt)
}

func validateDestructivePowerRecoveryPaths(controlDirectory, markerPath, controllerPath, proofPath, outputPath string) (string, string, string, string, error) {
	inputs := make([]string, 3)
	for index, path := range []string{markerPath, controllerPath, proofPath} {
		absolute, err := existingRegularAbsolutePath(path)
		if err != nil || filepath.Dir(absolute) != controlDirectory {
			return "", "", "", "", errors.New("power marker, controller event and proof must be existing regular direct children of --control-dir")
		}
		inputs[index] = absolute
	}
	output, err := newAbsolutePath(outputPath)
	if err != nil || filepath.Dir(output) != controlDirectory {
		return "", "", "", "", errors.New("power recovery output must be a new regular direct child of --control-dir")
	}
	seen := map[string]struct{}{output: {}}
	for _, path := range inputs {
		if _, duplicate := seen[path]; duplicate {
			return "", "", "", "", errors.New("power marker, controller event, proof and recovery output must use distinct paths")
		}
		seen[path] = struct{}{}
	}
	return inputs[0], inputs[1], inputs[2], output, nil
}

func validateDestructivePowerInputs(marker destructivePowerMarker, markerRaw []byte, event destructivePowerControllerEvent, facts destructiveVolumeFacts, currentBootID string) error {
	buildRevision, buildModified := durabilityBuildIdentity()
	if marker.SchemaVersion != destructivePowerMarkerSchema || !qualificationSafeName(marker.TrialID, 64) ||
		!qualificationBoundaryAllowed(qualificationTrialPower, marker.Boundary) || marker.GOOS != runtime.GOOS || marker.GOARCH != runtime.GOARCH ||
		marker.Device != facts.device || marker.FilesystemType != facts.filesystemType || marker.FilesystemName != facts.filesystemName ||
		marker.BlockSize != facts.blockSize || !qualificationSafeName(marker.BootIDBefore, 64) || !qualificationSafeName(currentBootID, 64) || currentBootID == marker.BootIDBefore ||
		marker.StartedAt.IsZero() || marker.ReachedAt.Before(marker.StartedAt) || marker.OldCommitSequence != 1 || marker.NewCommitSequence != 2 ||
		!qualificationHexDigest(marker.OldStateSHA256) || !qualificationHexDigest(marker.NewStateSHA256) || marker.OldStateSHA256 == marker.NewStateSHA256 ||
		marker.BuildRevision != buildRevision || marker.BuildModified != buildModified {
		return errors.New("power marker identity, boot, build, volume or state contract is invalid")
	}
	if !validPowerDatabaseRelative(marker.DatabaseRelative, marker.TrialID) {
		return errors.New("power marker database path is invalid")
	}
	expectedSchema := destructivePowerEventSchema
	if qualificationPhysicalPowerMethod(event.Method) {
		expectedSchema = destructivePhysicalPowerEventSchema
	}
	if event.SchemaVersion != expectedSchema || event.TrialID != marker.TrialID ||
		event.MarkerSHA256 != qualificationSHA256(markerRaw) || event.BootIDBefore != marker.BootIDBefore ||
		!qualificationPowerMethod(event.Method) || event.MarkerObservedAt.IsZero() || event.CutRequestedAt.Before(event.MarkerObservedAt) ||
		!event.PowerRestoredAt.After(event.CutRequestedAt) || !qualificationHexDigest(event.ControllerProofSHA256) {
		return errors.New("external power controller event is incomplete or does not bind the marker")
	}
	if qualificationPhysicalPowerMethod(event.Method) {
		if !anchorQualificationHex(event.ControllerRunID, 16) || event.ControllerPublicKey == "" || event.Signature == "" {
			return errors.New("physical power controller event lacks signed controller-agent identity")
		}
	} else if event.ControllerRunID != "" || event.ControllerPublicKey != "" || event.Signature != "" {
		return errors.New("QEMU power event unexpectedly contains physical controller attestation")
	}
	return nil
}

func qualificationPowerMethod(value string) bool {
	switch value {
	case "qemu-system-reset", "qemu-host-sigkill", "hypervisor-hard-reset", "ipmi-chassis-power-cycle", "pdu-power-cycle", "redfish-computer-system-power-cycle":
		return true
	default:
		return false
	}
}

func validPowerDatabaseRelative(value, trialID string) bool {
	want := filepath.Join(".meldbase-power-"+trialID, "power.meld")
	return value == want && !filepath.IsAbs(value) && !strings.Contains(value, "..")
}

func validatePowerTargetEntries(target, expectedDirectory string) error {
	entries, err := os.ReadDir(target)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "lost+found" && entry.IsDir() || entry.Name() == expectedDirectory && entry.IsDir() {
			continue
		}
		return fmt.Errorf("unexpected file %q on power recovery target", entry.Name())
	}
	return nil
}
