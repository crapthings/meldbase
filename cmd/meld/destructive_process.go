package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
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
	"strings"
	"syscall"
	"time"

	"github.com/crapthings/meldbase"
)

const destructiveProcessReceiptSchema uint32 = 2

var errDestructiveOracleIncomplete = errors.New("destructive oracle is incomplete")

type destructiveProcessReceipt struct {
	SchemaVersion      uint32                          `json:"schemaVersion"`
	SourceRevision     string                          `json:"sourceRevision,omitempty"`
	BuildRevision      string                          `json:"buildRevision,omitempty"`
	BuildModified      bool                            `json:"buildModified"`
	GOOS               string                          `json:"goos"`
	GOARCH             string                          `json:"goarch"`
	GoVersion          string                          `json:"goVersion"`
	Device             uint64                          `json:"device"`
	FilesystemType     string                          `json:"filesystemType"`
	FilesystemName     string                          `json:"filesystemName"`
	BlockSize          uint64                          `json:"blockSize"`
	ArtifactsDirectory string                          `json:"artifactsDirectory"`
	StartedAt          time.Time                       `json:"startedAt"`
	FinishedAt         time.Time                       `json:"finishedAt"`
	RequestedTrials    int                             `json:"requestedTrials"`
	CompletedTrials    int                             `json:"completedTrials"`
	TrialDirectories   []string                        `json:"trialDirectories"`
	Trials             []qualificationDestructiveTrial `json:"trials"`
	Verifications      []meldbase.VerificationReport   `json:"verifications"`
}

type destructiveLedgerRecord struct {
	Phase   string `json:"phase"`
	Counter uint64 `json:"counter"`
}

func runDestructiveProcessCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-process-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", "", "existing directory on the disposable target volume")
	output := flags.String("out", "", "new path for the machine-readable process-kill receipt")
	artifactsDirectoryFlag := flags.String("artifacts-dir", "", "existing directory for retained crash images (defaults to receipt parent)")
	trials := flags.Int("trials", qualificationMinimumProcessTrials, "number of independent SIGKILL trials (minimum 20)")
	sourceRevision := flags.String("source-revision", "", "optional 40- or 64-hex source revision")
	requireClean := flags.Bool("require-clean-source", false, "require a clean binary matching --source-revision")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *directory == "" || *output == "" {
		return errors.New("destructive-process-check requires --dir and --out")
	}
	if *trials < qualificationMinimumProcessTrials || *trials > 1_000 {
		return fmt.Errorf("destructive-process-check --trials must be between %d and 1000", qualificationMinimumProcessTrials)
	}
	if *sourceRevision != "" && !validDurabilitySourceRevision(*sourceRevision) {
		return errors.New("destructive-process-check --source-revision must be 40 or 64 hexadecimal characters")
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if *requireClean && (*sourceRevision == "" || buildRevision != *sourceRevision || buildModified) {
		return errors.New("destructive-process-check clean source verification failed")
	}
	target, err := filepath.Abs(filepath.Clean(*directory))
	if err != nil {
		return err
	}
	info, err := os.Stat(target)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("destructive-process-check --dir must be a directory")
	}
	volume, err := storageSoakVolume(target, info)
	if err != nil {
		return err
	}
	cleanOutput, err := filepath.Abs(filepath.Clean(*output))
	if err != nil {
		return err
	}
	if _, err := os.Lstat(cleanOutput); err == nil {
		return fmt.Errorf("destructive process receipt already exists: %s", cleanOutput)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	artifactsRoot := *artifactsDirectoryFlag
	if artifactsRoot == "" {
		artifactsRoot = filepath.Dir(cleanOutput)
	}
	artifactsRoot, _, err = resolvedDirectory(artifactsRoot)
	if err != nil {
		return err
	}
	if strings.HasPrefix(artifactsRoot+string(os.PathSeparator), target+string(os.PathSeparator)) {
		return errors.New("destructive process artifacts directory must be outside the target directory")
	}
	artifactsDirectory, err := os.MkdirTemp(artifactsRoot, ".meldbase-process-evidence-")
	if err != nil {
		return err
	}

	executable, err := os.Executable()
	if err != nil {
		return err
	}
	started := time.Now().UTC()
	receipt := destructiveProcessReceipt{
		SchemaVersion: destructiveProcessReceiptSchema, SourceRevision: *sourceRevision,
		BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		Device: volume.Device, FilesystemType: volume.FilesystemType, FilesystemName: volume.FilesystemName, BlockSize: volume.BlockSize,
		ArtifactsDirectory: artifactsDirectory, StartedAt: started, RequestedTrials: *trials,
	}
	for ordinal := 1; ordinal <= *trials; ordinal++ {
		trialDirectory, trial, verification, err := runDestructiveProcessTrial(target, artifactsDirectory, executable, ordinal, stderr)
		if err != nil {
			return fmt.Errorf("destructive process trial %d: %w", ordinal, err)
		}
		receipt.TrialDirectories = append(receipt.TrialDirectories, trialDirectory)
		receipt.Trials = append(receipt.Trials, trial)
		receipt.Verifications = append(receipt.Verifications, verification)
		receipt.CompletedTrials++
	}
	receipt.FinishedAt = time.Now().UTC()
	if err := writeDestructiveReceipt(cleanOutput, receipt); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(receipt)
}

func runDestructiveProcessTrial(target, artifactsRoot, executable string, ordinal int, stderr io.Writer) (artifactTrial string, trial qualificationDestructiveTrial, verification meldbase.VerificationReport, resultErr error) {
	trialDirectory, err := os.MkdirTemp(target, fmt.Sprintf(".meldbase-process-kill-%04d-", ordinal))
	if err != nil {
		return "", trial, verification, err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(trialDirectory), syncProbeDirectory(target)) }()
	artifactTrial, err = os.MkdirTemp(artifactsRoot, fmt.Sprintf("trial-%04d-", ordinal))
	if err != nil {
		return artifactTrial, trial, verification, err
	}
	databasePath := filepath.Join(trialDirectory, "process-kill.meld")
	ledgerPath := filepath.Join(trialDirectory, "oracle.jsonl")
	started := time.Now().UTC()
	initialSequence, err := seedDestructiveProcessDatabase(databasePath)
	if err != nil {
		return artifactTrial, trial, verification, err
	}
	pauseAfter := "prepared"
	if ordinal%2 == 0 {
		pauseAfter = "committed"
	}
	command := exec.Command(executable, "destructive-process-worker", "--db", databasePath, "--ledger", ledgerPath, "--pause-after", pauseAfter)
	command.Stdout = io.Discard
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		return artifactTrial, trial, verification, err
	}
	killErr := waitForDestructivePhase(ledgerPath, pauseAfter, 10*time.Second)
	if killErr == nil {
		killErr = command.Process.Signal(os.Kill)
	}
	waitErr := command.Wait()
	if killErr != nil {
		return artifactTrial, trial, verification, errors.Join(killErr, waitErr)
	}
	if !destructiveProcessWasKilled(waitErr) {
		return artifactTrial, trial, verification, fmt.Errorf("destructive worker did not terminate from SIGKILL: %w", waitErr)
	}

	confirmed, prepared, ledgerRaw, err := readDestructiveLedger(ledgerPath)
	if err != nil {
		return artifactTrial, trial, verification, err
	}
	if prepared < confirmed || prepared > confirmed+1 {
		return artifactTrial, trial, verification, fmt.Errorf("destructive oracle is not sequential: confirmed=%d prepared=%d", confirmed, prepared)
	}
	if err := copyFileExclusiveDurable(filepath.Join(artifactTrial, "crash-image.meld"), databasePath); err != nil {
		return artifactTrial, trial, verification, err
	}
	if err := copyFileExclusiveDurable(filepath.Join(artifactTrial, "oracle.jsonl"), ledgerPath); err != nil {
		return artifactTrial, trial, verification, err
	}
	verification, err = meldbase.VerifyFile(context.Background(), databasePath)
	if err != nil {
		return artifactTrial, trial, verification, err
	}
	recoveredCounter, lockReacquired, err := recoverDestructiveCounter(databasePath)
	if err != nil {
		return artifactTrial, trial, verification, err
	}
	oldSequence := initialSequence + confirmed
	newSequence := oldSequence + 1
	if recoveredCounter != confirmed && recoveredCounter != prepared {
		return artifactTrial, trial, verification, fmt.Errorf("recovered counter %d is neither confirmed %d nor prepared %d", recoveredCounter, confirmed, prepared)
	}
	recoveredSequence := initialSequence + recoveredCounter
	if recoveredSequence != oldSequence && recoveredSequence != newSequence {
		return artifactTrial, trial, verification, fmt.Errorf("recovered sequence %d is outside old/new [%d,%d]", recoveredSequence, oldSequence, newSequence)
	}
	if verification.CommitSequence != recoveredSequence {
		return artifactTrial, trial, verification, fmt.Errorf("offline sequence %d does not match recovered sequence %d", verification.CommitSequence, recoveredSequence)
	}
	artifactDigest := sha256.Sum256(ledgerRaw)
	identityHash := sha256.New()
	_, _ = identityHash.Write(ledgerRaw)
	_, _ = identityHash.Write([]byte(verification.SHA256))
	_, _ = identityHash.Write([]byte(started.Format(time.RFC3339Nano)))
	identityDigest := identityHash.Sum(nil)
	outcome := "old"
	if recoveredSequence == newSequence {
		outcome = "new"
	}
	trial = qualificationDestructiveTrial{
		ID: "process-kill-" + strings.ToLower(hex.EncodeToString(identityDigest[:8])), Kind: qualificationTrialProcess,
		PublicationBoundary: qualificationAsyncBoundary, TriggerPoint: "oracle-" + pauseAfter,
		StartedAt: started, FinishedAt: time.Now().UTC(),
		OldCommitSequence: oldSequence, NewCommitSequence: newSequence, RecoveredSequence: recoveredSequence, Outcome: outcome,
		OldStateSHA256: destructiveCounterStateSHA256(confirmed), NewStateSHA256: destructiveCounterStateSHA256(confirmed + 1),
		RecoveredStateSHA256: destructiveCounterStateSHA256(recoveredCounter), LockReacquired: lockReacquired,
		OfflineVerified: verification.Verified && verification.IndexContentsVerified && verification.IndexBuildContentsVerified,
		DatabaseSHA256:  verification.SHA256, ArtifactsSHA256: hex.EncodeToString(artifactDigest[:]),
	}
	if !trial.OfflineVerified {
		return artifactTrial, trial, verification, errors.New("destructive process database did not pass complete offline verification")
	}
	return artifactTrial, trial, verification, nil
}

func destructiveCounterStateSHA256(counter uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("counter:%d\n", counter)))
	return hex.EncodeToString(digest[:])
}

func runDestructiveProcessWorker(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-process-worker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("db", "", "database path")
	ledgerPath := flags.String("ledger", "", "append-only oracle ledger")
	pauseAfter := flags.String("pause-after", "", "qualification controller phase: prepared or committed")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *databasePath == "" || *ledgerPath == "" || (*pauseAfter != "prepared" && *pauseAfter != "committed") {
		return errors.New("destructive-process-worker requires --db, --ledger and --pause-after=prepared|committed")
	}
	ledger, err := os.OpenFile(*ledgerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer ledger.Close()
	db, err := meldbase.Open(*databasePath)
	if err != nil {
		return err
	}
	defer db.Close()
	items := db.Collection("items")
	for counter := uint64(1); ; counter++ {
		if err := appendDestructiveLedger(ledger, destructiveLedgerRecord{Phase: "prepared", Counter: counter}); err != nil {
			return err
		}
		if *pauseAfter == "prepared" {
			waitForDestructiveKill()
		}
		if counter > uint64(^uint64(0)>>1) {
			return errors.New("destructive worker counter exhausted")
		}
		result, err := items.UpdateOne(context.Background(), meldbase.Filter{"name": "counter"}, meldbase.Update{"$set": map[string]any{"value": int64(counter)}})
		if err != nil {
			return err
		}
		if result.MatchedCount != 1 || result.ModifiedCount != 1 {
			return fmt.Errorf("destructive worker update result=%+v", result)
		}
		if err := appendDestructiveLedger(ledger, destructiveLedgerRecord{Phase: "committed", Counter: counter}); err != nil {
			return err
		}
		if *pauseAfter == "committed" {
			waitForDestructiveKill()
		}
	}
}

func waitForDestructiveKill() {
	for {
		time.Sleep(time.Hour)
	}
}

func seedDestructiveProcessDatabase(path string) (uint64, error) {
	db, err := meldbase.Open(path)
	if err != nil {
		return 0, err
	}
	items := db.Collection("items")
	_, insertErr := items.InsertOne(context.Background(), meldbase.Document{"name": meldbase.String("counter"), "value": meldbase.Int(0)})
	closeErr := db.Close()
	if insertErr != nil || closeErr != nil {
		return 0, errors.Join(insertErr, closeErr)
	}
	report, err := meldbase.VerifyFile(context.Background(), path)
	if err != nil {
		return 0, err
	}
	return report.CommitSequence, nil
}

func waitForDestructivePhase(path, phase string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		confirmed, prepared, _, err := readDestructiveLedger(path)
		if err == nil {
			if phase == "prepared" && prepared >= 1 || phase == "committed" && confirmed >= 1 {
				return nil
			}
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errDestructiveOracleIncomplete) {
			return err
		}
		time.Sleep(2 * time.Millisecond)
	}
	return errors.New("timed out waiting for destructive worker progress")
}

func appendDestructiveLedger(file *os.File, record destructiveLedgerRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if written, err := file.Write(encoded); err != nil {
		return err
	} else if written != len(encoded) {
		return io.ErrShortWrite
	}
	return file.Sync()
}

func readDestructiveLedger(path string) (confirmed, prepared uint64, raw []byte, resultErr error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	raw, err = io.ReadAll(io.LimitReader(file, 16<<20))
	if err != nil {
		return 0, 0, nil, err
	}
	if len(raw) == 16<<20 {
		return 0, 0, nil, errors.New("destructive oracle ledger exceeds 16 MiB")
	}
	completeEnd := bytes.LastIndexByte(raw, '\n')
	if completeEnd < 0 {
		return 0, 0, nil, fmt.Errorf("%w: no complete record", errDestructiveOracleIncomplete)
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw[:completeEnd+1]))
	for scanner.Scan() {
		var record destructiveLedgerRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return 0, 0, nil, err
		}
		switch record.Phase {
		case "prepared":
			if record.Counter != confirmed+1 || record.Counter < prepared {
				return 0, 0, nil, errors.New("destructive oracle prepared records are out of order")
			}
			prepared = record.Counter
		case "committed":
			if record.Counter != prepared || record.Counter != confirmed+1 {
				return 0, 0, nil, errors.New("destructive oracle committed records are out of order")
			}
			confirmed = record.Counter
		default:
			return 0, 0, nil, errors.New("destructive oracle contains an unknown phase")
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, nil, err
	}
	if prepared == 0 {
		return 0, 0, nil, fmt.Errorf("%w: no prepared operation", errDestructiveOracleIncomplete)
	}
	return confirmed, prepared, raw, nil
}

func recoverDestructiveCounter(path string) (uint64, bool, error) {
	db, err := meldbase.Open(path)
	if err != nil {
		return 0, false, err
	}
	document, findErr := db.Collection("items").FindOne(context.Background(), meldbase.Filter{"name": "counter"})
	closeErr := db.Close()
	if findErr != nil || closeErr != nil {
		return 0, true, errors.Join(findErr, closeErr)
	}
	value, ok := document["value"].Int64()
	if !ok || value < 0 {
		return 0, true, errors.New("recovered destructive counter is not a non-negative integer")
	}
	return uint64(value), true, nil
}

func destructiveProcessWasKilled(err error) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ProcessState == nil {
		return false
	}
	status, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

func writeDestructiveReceipt(path string, receipt destructiveProcessReceipt) error {
	raw, err := json.MarshalIndent(receipt, "", "  ")
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
	if writeErr == nil {
		directory, err := os.Open(filepath.Dir(path))
		if err != nil {
			writeErr = err
		} else {
			writeErr = errors.Join(directory.Sync(), directory.Close())
		}
	}
	if writeErr != nil {
		removeErr := os.Remove(path)
		return errors.Join(fmt.Errorf("publish destructive process receipt: %w", writeErr), removeErr)
	}
	return nil
}
