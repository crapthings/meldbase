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
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

const destructiveENOSPCReceiptSchema uint32 = 1

var (
	fillDestructiveVolumeFn     = fillDestructiveVolume
	destructiveAvailableBytesFn = destructiveAvailableBytes
)

type destructiveENOSPCReceipt struct {
	SchemaVersion      uint32                             `json:"schemaVersion"`
	SourceRevision     string                             `json:"sourceRevision,omitempty"`
	BuildRevision      string                             `json:"buildRevision,omitempty"`
	BuildModified      bool                               `json:"buildModified"`
	GOOS               string                             `json:"goos"`
	GOARCH             string                             `json:"goarch"`
	GoVersion          string                             `json:"goVersion"`
	Device             uint64                             `json:"device"`
	FilesystemType     string                             `json:"filesystemType"`
	FilesystemName     string                             `json:"filesystemName"`
	BlockSize          uint64                             `json:"blockSize"`
	StartedAt          time.Time                          `json:"startedAt"`
	FinishedAt         time.Time                          `json:"finishedAt"`
	TrialsPerBoundary  int                                `json:"trialsPerBoundary"`
	Trials             []qualificationDestructiveTrial    `json:"trials"`
	CapacityEvidence   []destructiveCapacityTrialEvidence `json:"capacityEvidence"`
	ArtifactsDirectory string                             `json:"artifactsDirectory"`
}

type destructiveCapacityMarker struct {
	SchemaVersion uint32    `json:"schemaVersion"`
	Boundary      string    `json:"boundary"`
	PID           int       `json:"pid"`
	ReachedAt     time.Time `json:"reachedAt"`
}

type destructiveCapacityTrialEvidence struct {
	TrialID                string `json:"trialId"`
	Boundary               string `json:"boundary"`
	DatabaseArtifact       string `json:"databaseArtifact"`
	MarkerArtifact         string `json:"markerArtifact"`
	MarkerSHA256           string `json:"markerSha256"`
	AllocatedBytes         uint64 `json:"allocatedBytes"`
	AvailableBytesBefore   uint64 `json:"availableBytesBefore"`
	AvailableBytesAtENOSPC uint64 `json:"availableBytesAtEnospc"`
	AvailableBytesAfter    uint64 `json:"availableBytesAfterCleanup"`
	ENOSPCOperation        string `json:"enospcOperation"`
	MetaGeneration         uint64 `json:"metaGeneration"`
	CommitSequence         uint64 `json:"commitSequence"`
	ValidMetaSlots         int    `json:"validMetaSlots"`
	PhysicalPages          uint64 `json:"physicalPages"`
	ReachablePages         uint64 `json:"reachablePages"`
	FreeSpaceValid         bool   `json:"freeSpaceValid"`
}

func runDestructiveENOSPCCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-enospc-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", "", "root of the disposable independently mounted target volume")
	controlDirectory := flags.String("control-dir", "", "existing evidence directory on a different device")
	output := flags.String("out", "", "new receipt path directly inside --control-dir")
	token := flags.String("destructive-token", "", "exact token emitted by destructive-volume-check")
	trialsPerBoundary := flags.Int("trials-per-boundary", qualificationMinimumBoundaryTrials, "real ENOSPC trials at each publication boundary (minimum 3)")
	sourceRevision := flags.String("source-revision", "", "optional 40- or 64-hex source revision")
	requireClean := flags.Bool("require-clean-source", false, "require a clean binary matching --source-revision")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *directory == "" || *controlDirectory == "" || *output == "" {
		return errors.New("destructive-enospc-check requires --dir, --control-dir and --out")
	}
	if *trialsPerBoundary < qualificationMinimumBoundaryTrials || *trialsPerBoundary > 100 {
		return fmt.Errorf("destructive-enospc-check --trials-per-boundary must be between %d and 100", qualificationMinimumBoundaryTrials)
	}
	if *sourceRevision != "" && !validDurabilitySourceRevision(*sourceRevision) {
		return errors.New("destructive-enospc-check --source-revision must be 40 or 64 hexadecimal characters")
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if *requireClean && (*sourceRevision == "" || buildRevision != *sourceRevision || buildModified) {
		return errors.New("destructive-enospc-check clean source verification failed")
	}
	facts, err := inspectDestructiveVolume(*directory, *controlDirectory)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolumeFacts(facts); err != nil {
		return err
	}
	wantToken := destructiveVolumeToken(facts)
	if subtle.ConstantTimeCompare([]byte(*token), []byte(wantToken)) != 1 {
		return fmt.Errorf("destructive token mismatch; run destructive-volume-check again and pass %q", wantToken)
	}
	cleanOutput, err := filepath.Abs(filepath.Clean(*output))
	if err != nil {
		return err
	}
	if filepath.Clean(filepath.Dir(cleanOutput)) != facts.controlDirectory {
		return errors.New("destructive ENOSPC receipt must be written directly inside --control-dir")
	}
	if _, err := os.Lstat(cleanOutput); err == nil {
		return fmt.Errorf("destructive ENOSPC receipt already exists: %s", cleanOutput)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	artifactsDirectory, err := os.MkdirTemp(facts.controlDirectory, ".meldbase-enospc-evidence-")
	if err != nil {
		return err
	}
	receipt := destructiveENOSPCReceipt{
		SchemaVersion: destructiveENOSPCReceiptSchema, SourceRevision: *sourceRevision,
		BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		Device: facts.device, FilesystemType: facts.filesystemType, FilesystemName: facts.filesystemName, BlockSize: facts.blockSize,
		StartedAt: time.Now().UTC(), TrialsPerBoundary: *trialsPerBoundary, ArtifactsDirectory: artifactsDirectory,
	}
	ordinal := 0
	for _, boundary := range qualificationPublicationBoundaries {
		for repetition := 1; repetition <= *trialsPerBoundary; repetition++ {
			ordinal++
			trial, evidence, err := runDestructiveCapacityTrial(facts, executable, artifactsDirectory, boundary, ordinal, stderr)
			if err != nil {
				return fmt.Errorf("capacity trial %s repetition %d: %w", boundary, repetition, err)
			}
			receipt.Trials = append(receipt.Trials, trial)
			receipt.CapacityEvidence = append(receipt.CapacityEvidence, evidence)
		}
	}
	receipt.FinishedAt = time.Now().UTC()
	if err := validateDestructiveENOSPCReceipt(receipt); err != nil {
		return err
	}
	if err := writeDestructiveENOSPCReceipt(cleanOutput, receipt); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(receipt)
}

func validateDestructiveENOSPCReceipt(receipt destructiveENOSPCReceipt) error {
	return validateDestructiveENOSPCReceiptWithCanonicalEvidence(receipt, receipt.CapacityEvidence)
}

func validateDestructiveENOSPCReceiptWithCanonicalEvidence(receipt destructiveENOSPCReceipt, canonicalEvidence []destructiveCapacityTrialEvidence) error {
	wantTrials := receipt.TrialsPerBoundary * len(qualificationPublicationBoundaries)
	if receipt.SchemaVersion != destructiveENOSPCReceiptSchema || receipt.GOOS != "linux" || receipt.Device == 0 ||
		receipt.BlockSize == 0 || receipt.TrialsPerBoundary < qualificationMinimumBoundaryTrials ||
		len(receipt.Trials) != wantTrials || len(receipt.CapacityEvidence) != wantTrials || len(canonicalEvidence) != wantTrials ||
		receipt.StartedAt.IsZero() || !receipt.FinishedAt.After(receipt.StartedAt) {
		return errors.New("destructive ENOSPC receipt identity, timing or trial count is invalid")
	}
	coverage := make(map[string]int, len(qualificationPublicationBoundaries))
	seen := make(map[string]struct{}, wantTrials)
	for index, trial := range receipt.Trials {
		evidence := receipt.CapacityEvidence[index]
		if _, exists := seen[trial.ID]; exists {
			return fmt.Errorf("destructive ENOSPC trial %d duplicates id %q", index+1, trial.ID)
		}
		seen[trial.ID] = struct{}{}
		if trial.Kind != qualificationTrialCapacity || trial.TriggerPoint != "real-enospc-at-boundary" ||
			evidence.TrialID != trial.ID || evidence.Boundary != trial.PublicationBoundary || evidence.ENOSPCOperation != "fallocate" ||
			evidence.AllocatedBytes == 0 || evidence.AvailableBytesBefore == 0 || evidence.AvailableBytesAtENOSPC >= receipt.BlockSize ||
			evidence.AvailableBytesAfter == 0 || evidence.CommitSequence != trial.RecoveredSequence || evidence.ValidMetaSlots < 1 ||
			evidence.PhysicalPages == 0 || evidence.ReachablePages == 0 || evidence.ReachablePages > evidence.PhysicalPages || !evidence.FreeSpaceValid {
			return fmt.Errorf("destructive ENOSPC trial %d has incomplete capacity or recovery evidence", index+1)
		}
		if !qualificationBoundaryAllowed(qualificationTrialCapacity, trial.PublicationBoundary) {
			return fmt.Errorf("destructive ENOSPC trial %d has an invalid boundary", index+1)
		}
		if !trial.LockReacquired || !trial.OfflineVerified || !qualificationHexDigest(trial.DatabaseSHA256) ||
			!qualificationHexDigest(trial.ArtifactsSHA256) || !qualificationHexDigest(evidence.MarkerSHA256) {
			return fmt.Errorf("destructive ENOSPC trial %d lacks hashes or verification", index+1)
		}
		markerRaw, err := os.ReadFile(evidence.MarkerArtifact)
		if err != nil || qualificationSHA256(markerRaw) != evidence.MarkerSHA256 {
			return fmt.Errorf("destructive ENOSPC trial %d boundary marker is missing or mismatched", index+1)
		}
		var marker destructiveCapacityMarker
		if err := json.Unmarshal(markerRaw, &marker); err != nil || marker.SchemaVersion != 1 ||
			marker.Boundary != trial.PublicationBoundary || marker.PID <= 0 || marker.ReachedAt.IsZero() {
			return fmt.Errorf("destructive ENOSPC trial %d boundary marker does not reproduce the trial", index+1)
		}
		digest, err := hashRegularFile(evidence.DatabaseArtifact, 1<<30)
		if err != nil {
			return fmt.Errorf("destructive ENOSPC trial %d database artifact: %w", index+1, err)
		}
		if digest != trial.DatabaseSHA256 {
			return fmt.Errorf("destructive ENOSPC trial %d database artifact hash mismatch", index+1)
		}
		artifactVerified, err := verifyRawDestructiveArtifact(evidence.DatabaseArtifact)
		if err != nil || hex.EncodeToString(artifactVerified.SHA256[:]) != trial.DatabaseSHA256 ||
			artifactVerified.Meta.CommitSequence != trial.RecoveredSequence || artifactVerified.Meta.Generation != evidence.MetaGeneration ||
			artifactVerified.ValidMetaSlots != evidence.ValidMetaSlots || artifactVerified.PhysicalPages != evidence.PhysicalPages ||
			artifactVerified.ReachablePages != evidence.ReachablePages || artifactVerified.FreeSpaceValid != evidence.FreeSpaceValid {
			return fmt.Errorf("destructive ENOSPC trial %d crash image does not reproduce its offline proof", index+1)
		}
		evidenceRaw, err := json.Marshal(canonicalEvidence[index])
		if err != nil {
			return err
		}
		aggregate := sha256.New()
		_, _ = aggregate.Write(markerRaw)
		_, _ = aggregate.Write(evidenceRaw)
		_, _ = aggregate.Write(artifactVerified.SHA256[:])
		if hex.EncodeToString(aggregate.Sum(nil)) != trial.ArtifactsSHA256 {
			return fmt.Errorf("destructive ENOSPC trial %d aggregate artifact hash is mismatched", index+1)
		}
		coverage[trial.PublicationBoundary]++
	}
	for _, boundary := range qualificationPublicationBoundaries {
		if coverage[boundary] < receipt.TrialsPerBoundary {
			return fmt.Errorf("destructive ENOSPC receipt lacks %s coverage", boundary)
		}
	}
	return nil
}

func verifyRawDestructiveArtifact(path string) (storagev2.VerificationResult, error) {
	return storagev2.VerifyPathContextWithIndexAudit(context.Background(), path, func(storagev2.IndexMeta, [16]byte, []byte) ([]byte, bool, error) {
		return nil, false, errors.New("destructive fixture unexpectedly contains an index")
	})
}

func runDestructiveENOSPCWorker(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-enospc-worker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("db", "", "qualification database")
	markerPath := flags.String("marker", "", "boundary marker on the control device")
	boundaryName := flags.String("boundary", "", "publication boundary")
	if err := flags.Parse(args); err != nil {
		return err
	}
	boundary, ok := destructiveQualificationBoundary(*boundaryName)
	if *databasePath == "" || *markerPath == "" || !ok {
		return errors.New("destructive-enospc-worker requires --db, --marker and a valid --boundary")
	}
	reached := false
	file, _, _, err := storagev2.OpenForQualification(*databasePath, storagev2.OpenOptions{}, func(current storagev2.QualificationBoundary) error {
		if reached || current != boundary {
			return nil
		}
		reached = true
		marker := destructiveCapacityMarker{SchemaVersion: 1, Boundary: string(current), PID: os.Getpid(), ReachedAt: time.Now().UTC()}
		if err := writeJSONExclusiveDurable(*markerPath, marker); err != nil {
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
		Collection: "items", DocumentID: id, Operation: storagev2.DocumentUpdate, Document: []byte("new"),
	}}})
	if err != nil {
		return err
	}
	if !reached {
		return errors.New("selected destructive publication boundary was not reached")
	}
	return errors.New("destructive ENOSPC worker unexpectedly resumed after boundary")
}

func runDestructiveCapacityTrial(facts destructiveVolumeFacts, executable, artifactsDirectory, boundary string, ordinal int, stderr io.Writer) (trial qualificationDestructiveTrial, evidence destructiveCapacityTrialEvidence, resultErr error) {
	targetTrial, err := os.MkdirTemp(facts.directory, fmt.Sprintf(".meldbase-enospc-%04d-", ordinal))
	if err != nil {
		return trial, evidence, err
	}
	defer func() {
		cleanupErr := errors.Join(os.RemoveAll(targetTrial), syncProbeDirectory(facts.directory))
		resultErr = errors.Join(resultErr, cleanupErr)
	}()
	artifactTrial, err := os.MkdirTemp(artifactsDirectory, fmt.Sprintf("trial-%04d-", ordinal))
	if err != nil {
		return trial, evidence, err
	}
	databasePath := filepath.Join(targetTrial, "capacity.meld")
	markerPath := filepath.Join(artifactTrial, "boundary.json")
	oldState, newState := []byte("old"), []byte("new")
	if err := seedDestructiveCapacityDatabase(databasePath, oldState); err != nil {
		return trial, evidence, err
	}
	started := time.Now().UTC()
	command := exec.Command(executable, "destructive-enospc-worker", "--db", databasePath, "--marker", markerPath, "--boundary", boundary)
	command.Stdout, command.Stderr = io.Discard, stderr
	if err := command.Start(); err != nil {
		return trial, evidence, err
	}
	workerReaped := false
	defer func() {
		if !workerReaped {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	markerRaw, err := waitForCapacityMarker(markerPath, boundary, 30*time.Second)
	if err != nil {
		return trial, evidence, err
	}
	fill, err := fillDestructiveVolumeFn(facts.directory, facts.blockSize)
	if err != nil {
		return trial, evidence, err
	}
	defer func() {
		if fill.Path != "" {
			_ = os.Remove(fill.Path)
		}
	}()
	killErr := command.Process.Signal(os.Kill)
	waitErr := command.Wait()
	workerReaped = true
	if killErr != nil || !destructiveProcessWasKilled(waitErr) {
		return trial, evidence, errors.Join(killErr, fmt.Errorf("capacity worker termination: %w", waitErr))
	}
	if err := os.Remove(fill.Path); err != nil {
		return trial, evidence, err
	}
	fill.Path = ""
	if err := syncProbeDirectory(facts.directory); err != nil {
		return trial, evidence, err
	}
	afterCleanup, err := destructiveAvailableBytesFn(facts.directory)
	if err != nil {
		return trial, evidence, err
	}
	databaseArtifact := filepath.Join(artifactTrial, "crash-image.meld")
	if err := copyFileExclusiveDurable(databaseArtifact, databasePath); err != nil {
		return trial, evidence, err
	}
	verified, err := storagev2.VerifyPathContextWithIndexAudit(context.Background(), databasePath, func(storagev2.IndexMeta, [16]byte, []byte) ([]byte, bool, error) {
		return nil, false, errors.New("capacity fixture unexpectedly contains an index")
	})
	if err != nil {
		return trial, evidence, err
	}
	recovered, meta, err := recoverDestructiveCapacityDatabase(databasePath)
	if err != nil {
		return trial, evidence, err
	}
	if meta != verified.Meta {
		return trial, evidence, errors.New("normal open selected a different generation than offline verification")
	}
	if string(recovered) != string(oldState) && string(recovered) != string(newState) {
		return trial, evidence, fmt.Errorf("recovered unknown capacity state %q", recovered)
	}
	recoveredSequence := meta.CommitSequence
	if recoveredSequence != 1 && recoveredSequence != 2 {
		return trial, evidence, fmt.Errorf("recovered capacity sequence %d is not old or new", recoveredSequence)
	}
	if string(recovered) == string(oldState) && recoveredSequence != 1 || string(recovered) == string(newState) && recoveredSequence != 2 {
		return trial, evidence, errors.New("capacity sequence and logical state disagree")
	}
	markerDigest := sha256.Sum256(markerRaw)
	trialID := fmt.Sprintf("capacity-%s-%04d-%s", boundary, ordinal, hex.EncodeToString(markerDigest[:4]))
	evidence = destructiveCapacityTrialEvidence{
		TrialID: trialID, Boundary: boundary, DatabaseArtifact: databaseArtifact, MarkerArtifact: markerPath,
		MarkerSHA256: hex.EncodeToString(markerDigest[:]), AllocatedBytes: fill.AllocatedBytes,
		AvailableBytesBefore: fill.AvailableBytesBefore, AvailableBytesAtENOSPC: fill.AvailableBytesAtENOSPC,
		AvailableBytesAfter: afterCleanup, ENOSPCOperation: fill.ENOSPCOperation,
		MetaGeneration: meta.Generation, CommitSequence: meta.CommitSequence, ValidMetaSlots: verified.ValidMetaSlots,
		PhysicalPages: verified.PhysicalPages, ReachablePages: verified.ReachablePages, FreeSpaceValid: verified.FreeSpaceValid,
	}
	evidenceRaw, err := json.Marshal(evidence)
	if err != nil {
		return trial, evidence, err
	}
	artifactHash := sha256.New()
	_, _ = artifactHash.Write(markerRaw)
	_, _ = artifactHash.Write(evidenceRaw)
	_, _ = artifactHash.Write(verified.SHA256[:])
	outcome := "old"
	if recoveredSequence == 2 {
		outcome = "new"
	}
	trial = qualificationDestructiveTrial{
		ID: trialID, Kind: qualificationTrialCapacity, PublicationBoundary: boundary, TriggerPoint: "real-enospc-at-boundary",
		StartedAt: started, FinishedAt: time.Now().UTC(), OldCommitSequence: 1, NewCommitSequence: 2,
		RecoveredSequence: recoveredSequence, Outcome: outcome,
		OldStateSHA256: bytesSHA256(oldState), NewStateSHA256: bytesSHA256(newState), RecoveredStateSHA256: bytesSHA256(recovered),
		LockReacquired: true, OfflineVerified: verified.SemanticIndexesVerified && verified.SemanticIndexBuildsVerified && verified.FreeSpaceValid,
		DatabaseSHA256: hex.EncodeToString(verified.SHA256[:]), ArtifactsSHA256: hex.EncodeToString(artifactHash.Sum(nil)),
	}
	if !trial.OfflineVerified || fill.ENOSPCOperation == "" {
		return trial, evidence, errors.New("capacity trial lacks real ENOSPC or complete offline verification")
	}
	return trial, evidence, nil
}

func seedDestructiveCapacityDatabase(path string, value []byte) error {
	file, _, err := storagev2.Open(path)
	if err != nil {
		return err
	}
	id := [16]byte{15: 1}
	_, commitErr := file.ApplyDocumentTransaction(storagev2.DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []storagev2.DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: storagev2.DocumentInsert, Document: value,
	}}})
	return errors.Join(commitErr, file.Close())
}

func recoverDestructiveCapacityDatabase(path string) ([]byte, storagev2.Meta, error) {
	file, meta, err := storagev2.Open(path)
	if err != nil {
		return nil, storagev2.Meta{}, err
	}
	id := [16]byte{15: 1}
	value, exists, readErr := file.GetDocument("items", id)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || !exists {
		return nil, storagev2.Meta{}, errors.Join(readErr, closeErr, errors.New("capacity fixture document missing"))
	}
	return value, meta, nil
}

func destructiveQualificationBoundary(value string) (storagev2.QualificationBoundary, bool) {
	for _, boundary := range []storagev2.QualificationBoundary{
		storagev2.QualificationAfterPageWrite, storagev2.QualificationBeforeDataSync, storagev2.QualificationAfterDataSync,
		storagev2.QualificationAfterMetaWrite, storagev2.QualificationAfterMetaSync,
	} {
		if string(boundary) == value {
			return boundary, true
		}
	}
	return "", false
}

func waitForCapacityMarker(path, boundary string, timeout time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		if err != nil {
			return nil, err
		}
		var marker destructiveCapacityMarker
		if err := json.Unmarshal(raw, &marker); err != nil {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		if marker.SchemaVersion != 1 || marker.Boundary != boundary || marker.PID <= 0 || marker.ReachedAt.IsZero() {
			return nil, errors.New("capacity boundary marker identity is invalid")
		}
		return raw, nil
	}
	return nil, errors.New("timed out waiting for capacity publication boundary")
}

func writeJSONExclusiveDurable(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(raw)
	if writeErr == nil && written != len(raw) {
		writeErr = io.ErrShortWrite
	}
	writeErr = errors.Join(writeErr, file.Sync(), file.Close())
	if writeErr != nil {
		return writeErr
	}
	return syncProbeDirectory(filepath.Dir(path))
}

func copyFileExclusiveDurable(destination, source string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	return errors.Join(copyErr, output.Sync(), output.Close(), syncProbeDirectory(filepath.Dir(destination)))
}

func bytesSHA256(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func hashRegularFile(path string, maximum int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return "", errors.New("artifact must be a bounded nonempty regular file")
	}
	hash := sha256.New()
	if copied, err := io.Copy(hash, io.LimitReader(file, maximum+1)); err != nil {
		return "", err
	} else if copied != info.Size() {
		return "", errors.New("artifact changed while hashing")
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func writeDestructiveENOSPCReceipt(path string, receipt destructiveENOSPCReceipt) error {
	return writeJSONExclusiveDurable(path, receipt)
}
