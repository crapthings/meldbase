package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

const destructiveFlushEIOResultSchema uint32 = 1

type destructiveFlushEIOReady struct {
	SchemaVersion  uint32    `json:"schemaVersion"`
	BuildRevision  string    `json:"buildRevision,omitempty"`
	BuildModified  bool      `json:"buildModified"`
	GOOS           string    `json:"goos"`
	GOARCH         string    `json:"goarch"`
	GoVersion      string    `json:"goVersion"`
	ReadyAt        time.Time `json:"readyAt"`
	BootID         string    `json:"bootId"`
	DatabaseSHA256 string    `json:"databaseSha256"`
	CommitSequence uint64    `json:"commitSequence"`
}

type destructiveFlushEIOArmed struct {
	SchemaVersion uint32    `json:"schemaVersion"`
	ReadySHA256   string    `json:"readySha256"`
	QMPArmSHA256  string    `json:"qmpArmSha256"`
	ArmedAt       time.Time `json:"armedAt"`
}

type destructiveFlushEIOFaultResult struct {
	SchemaVersion      uint32    `json:"schemaVersion"`
	BuildRevision      string    `json:"buildRevision,omitempty"`
	BuildModified      bool      `json:"buildModified"`
	GOOS               string    `json:"goos"`
	GOARCH             string    `json:"goarch"`
	GoVersion          string    `json:"goVersion"`
	StartedAt          time.Time `json:"startedAt"`
	FinishedAt         time.Time `json:"finishedAt"`
	BootID             string    `json:"bootId"`
	ReadySHA256        string    `json:"readySha256"`
	ArmedSHA256        string    `json:"armedSha256"`
	BeforeSHA256       string    `json:"beforeSha256"`
	BeforeSequence     uint64    `json:"beforeSequence"`
	FirstErrorIsEIO    bool      `json:"firstErrorIsEio"`
	PoisonedErrorIsEIO bool      `json:"poisonedErrorIsEio"`
	ReadAfterError     bool      `json:"readAfterError"`
	Passed             bool      `json:"passed"`
}

type destructiveFlushEIOWorkerResult struct {
	SchemaVersion       uint32    `json:"schemaVersion"`
	BuildRevision       string    `json:"buildRevision,omitempty"`
	BuildModified       bool      `json:"buildModified"`
	GOOS                string    `json:"goos"`
	GOARCH              string    `json:"goarch"`
	GoVersion           string    `json:"goVersion"`
	StartedAt           time.Time `json:"startedAt"`
	FinishedAt          time.Time `json:"finishedAt"`
	FaultBootID         string    `json:"faultBootId"`
	RecoveryBootID      string    `json:"recoveryBootId"`
	FaultSHA256         string    `json:"faultSha256"`
	ProofSHA256         string    `json:"proofSha256"`
	RecoveryPlanSHA256  string    `json:"recoveryPlanSha256"`
	RecoveryReadySHA256 string    `json:"recoveryReadySha256"`
	DatabaseArtifact    string    `json:"databaseArtifact"`
	BeforeSHA256        string    `json:"beforeSha256"`
	AfterSHA256         string    `json:"afterSha256"`
	BeforeSequence      uint64    `json:"beforeSequence"`
	RecoveredSequence   uint64    `json:"recoveredSequence"`
	OfflineVerified     bool      `json:"offlineVerified"`
	FreeSpaceValid      bool      `json:"freeSpaceValid"`
	PersistentFreeSpace bool      `json:"persistentFreeSpace"`
	Passed              bool      `json:"passed"`
}

func runDestructiveFlushEIOWorker(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-flush-eio-worker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("database", "", "existing seeded V2 database on the injected block device")
	readyPath := flags.String("ready", "", "new durable guest-ready receipt on the independent control device")
	armedPath := flags.String("armed", "", "durable host-armed receipt on the independent control device")
	outputPath := flags.String("out", "", "new fault-stage result on the independent control device")
	timeout := flags.Duration("timeout", 2*time.Minute, "maximum time to wait for host arming")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *databasePath == "" || *readyPath == "" || *armedPath == "" || *outputPath == "" || *timeout < 10*time.Second || *timeout > 30*time.Minute {
		return errors.New("destructive-flush-eio-worker requires database, ready, armed, out and a timeout from 10s to 30m")
	}
	database, err := existingRegularAbsolutePath(*databasePath)
	if err != nil {
		return err
	}
	control := make([]string, 3)
	for index, value := range []string{*readyPath, *armedPath, *outputPath} {
		control[index], err = filepath.Abs(filepath.Clean(value))
		if err != nil {
			return err
		}
	}
	readyClean, armedClean, outputClean := control[0], control[1], control[2]
	seen := map[string]struct{}{database: {}}
	for _, path := range control {
		if filepath.Dir(path) != filepath.Dir(readyClean) || filepath.Dir(path) == filepath.Dir(database) {
			return errors.New("flush EIO control artifacts must share an independent directory")
		}
		if _, duplicate := seen[path]; duplicate {
			return errors.New("flush EIO worker paths must be distinct")
		}
		seen[path] = struct{}{}
	}
	for _, path := range []string{readyClean, outputClean} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("flush EIO output already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	before, err := verifyDestructiveEIODatabase(database)
	if err != nil || !before.PersistentFreeSpace || !before.FreeSpaceValid {
		return errors.Join(err, errors.New("flush EIO source is not a verified reusable-page fixture"))
	}
	bootID, err := destructiveBootIDFn()
	if err != nil {
		return err
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	ready := destructiveFlushEIOReady{1, buildRevision, buildModified, runtime.GOOS, runtime.GOARCH, runtime.Version(), time.Now().UTC(), bootID, fmt.Sprintf("%x", before.SHA256), before.Meta.CommitSequence}
	if err := validateDestructiveFlushEIOReady(ready); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(readyClean, ready); err != nil {
		return err
	}
	readyRaw, err := os.ReadFile(readyClean)
	if err != nil {
		return err
	}
	readySHA := qualificationSHA256(readyRaw)
	deadline := time.Now().Add(*timeout)
	var armed destructiveFlushEIOArmed
	var armedRaw []byte
	for time.Now().Before(deadline) {
		armed = destructiveFlushEIOArmed{}
		armedRaw, err = readQualificationReceipt(armedClean, &armed)
		if err == nil && validateDestructiveFlushEIOArmed(armed, readySHA, ready.ReadyAt) == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(armedRaw) == 0 || validateDestructiveFlushEIOArmed(armed, readySHA, ready.ReadyAt) != nil {
		return errors.New("timed out waiting for a valid flush EIO armed receipt")
	}
	started := time.Now().UTC()
	file, meta, err := storagev2.Open(database)
	if err != nil {
		return err
	}
	stable, exists, err := file.GetDocument("items", destructiveEIODocumentID)
	if err != nil || !exists || string(stable) != "stable-16" {
		_ = file.Close()
		return errors.Join(err, errors.New("flush EIO stable document is missing"))
	}
	firstErr := applyDestructiveEIOUpdate(file, 0xf2, "must-not-commit-flush-1")
	poisonedErr := applyDestructiveEIOUpdate(file, 0xf3, "must-not-commit-flush-2")
	afterRead, afterExists, readErr := file.GetDocument("items", destructiveEIODocumentID)
	closeErr := file.Close()
	if !errors.Is(firstErr, syscall.EIO) || !errors.Is(poisonedErr, syscall.EIO) || readErr != nil || !afterExists || string(afterRead) != "stable-16" || closeErr != nil {
		return errors.Join(firstErr, poisonedErr, readErr, closeErr, errors.New("flush EIO worker did not observe fail-stop behavior"))
	}
	fault := destructiveFlushEIOFaultResult{
		SchemaVersion: 1, BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		StartedAt: started, FinishedAt: time.Now().UTC(), BootID: bootID, ReadySHA256: readySHA, ArmedSHA256: qualificationSHA256(armedRaw),
		BeforeSHA256: fmt.Sprintf("%x", before.SHA256), BeforeSequence: meta.CommitSequence,
		FirstErrorIsEIO: true, PoisonedErrorIsEIO: true, ReadAfterError: true, Passed: true,
	}
	if err := validateDestructiveFlushEIOFaultResult(fault); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(outputClean, fault); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(fault)
}

func validateDestructiveFlushEIOReady(ready destructiveFlushEIOReady) error {
	if ready.SchemaVersion != 1 || (ready.BuildRevision != "" && !validDurabilitySourceRevision(ready.BuildRevision)) || ready.GOOS == "" || ready.GOARCH == "" || ready.GoVersion == "" || ready.ReadyAt.IsZero() || !qualificationSafeName(ready.BootID, 64) || !qualificationHexDigest(ready.DatabaseSHA256) || ready.CommitSequence == 0 {
		return errors.New("flush EIO ready receipt is incomplete")
	}
	return nil
}

func validateDestructiveFlushEIOArmed(armed destructiveFlushEIOArmed, readySHA string, readyAt time.Time) error {
	if armed.SchemaVersion != 1 || armed.ReadySHA256 != readySHA || !qualificationHexDigest(armed.QMPArmSHA256) || armed.ArmedAt.IsZero() || armed.ArmedAt.Before(readyAt) {
		return errors.New("flush EIO armed receipt is incomplete or not bound to ready")
	}
	return nil
}

func validateDestructiveFlushEIOFaultResult(result destructiveFlushEIOFaultResult) error {
	if result.SchemaVersion != 1 || (result.BuildRevision != "" && !validDurabilitySourceRevision(result.BuildRevision)) || result.GOOS == "" || result.GOARCH == "" || result.GoVersion == "" || result.StartedAt.IsZero() || !result.FinishedAt.After(result.StartedAt) || !qualificationSafeName(result.BootID, 64) || !qualificationHexDigest(result.ReadySHA256) || !qualificationHexDigest(result.ArmedSHA256) || !qualificationHexDigest(result.BeforeSHA256) || result.BeforeSequence == 0 || !result.FirstErrorIsEIO || !result.PoisonedErrorIsEIO || !result.ReadAfterError || !result.Passed {
		return errors.New("flush EIO fault result is incomplete")
	}
	return nil
}

func runDestructiveFlushEIORecovery(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-flush-eio-recovery", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("database", "", "seeded database on the freshly mounted recovery device")
	faultPath := flags.String("fault", "", "durable fault-stage result")
	proofPath := flags.String("proof", "", "durable QMP fault proof")
	planPath := flags.String("plan", "", "durable stopped-image recovery plan")
	readyPath := flags.String("recovery-ready", "", "fresh-boot raw-device receipt")
	artifactPath := flags.String("artifact", "", "new recovered database copy")
	outputPath := flags.String("out", "", "new recovery result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *databasePath == "" || *faultPath == "" || *proofPath == "" || *planPath == "" || *readyPath == "" || *artifactPath == "" || *outputPath == "" {
		return errors.New("destructive-flush-eio-recovery requires database, fault, proof, plan, recovery-ready, artifact and out")
	}
	database, err := existingRegularAbsolutePath(*databasePath)
	if err != nil {
		return err
	}
	var fault destructiveFlushEIOFaultResult
	faultRaw, err := readQualificationReceipt(*faultPath, &fault)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIOFaultResult(fault); err != nil {
		return err
	}
	var proof destructiveQMPFlushEIOProof
	proofRaw, err := readQualificationReceipt(*proofPath, &proof)
	if err != nil {
		return err
	}
	if err := validateQMPFlushEIOProofStructure(proof); err != nil {
		return err
	}
	if proof.FaultSHA256 != qualificationSHA256(faultRaw) || fault.ReadySHA256 != proof.ReadySHA256 || fault.ArmedSHA256 != proof.ArmedSHA256 {
		return errors.New("flush EIO recovery inputs are not one fault transition")
	}
	var plan destructiveFlushEIORecoveryPlan
	planRaw, err := readQualificationReceipt(*planPath, &plan)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIORecoveryPlan(plan, faultRaw, proofRaw, proof); err != nil {
		return err
	}
	var recoveryReady destructiveFlushEIORecoveryReady
	recoveryReadyRaw, err := readQualificationReceipt(*readyPath, &recoveryReady)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIORecoveryReady(recoveryReady, planRaw, plan, fault); err != nil {
		return err
	}
	artifact, err := filepath.Abs(filepath.Clean(*artifactPath))
	if err != nil {
		return err
	}
	output, err := filepath.Abs(filepath.Clean(*outputPath))
	if err != nil {
		return err
	}
	if artifact == output || filepath.Dir(artifact) != filepath.Dir(output) || filepath.Dir(artifact) == filepath.Dir(database) {
		return errors.New("flush EIO recovery outputs must be distinct on the independent control device")
	}
	for _, path := range []string{artifact, output} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("flush EIO recovery output already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	started := time.Now().UTC()
	bootID, err := destructiveBootIDFn()
	if err != nil {
		return err
	}
	if bootID != recoveryReady.BootID {
		return errors.New("flush EIO recovery process is not running in the raw-device preflight boot")
	}
	after, err := verifyDestructiveEIODatabase(database)
	if err != nil || after.Meta.CommitSequence != fault.BeforeSequence || !after.PersistentFreeSpace || !after.FreeSpaceValid {
		return errors.Join(err, errors.New("flush EIO post-reboot recovery did not preserve the old verified generation"))
	}
	if err := copyFileExclusiveDurable(artifact, database); err != nil {
		return err
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	result := destructiveFlushEIOWorkerResult{
		SchemaVersion: 1, BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		StartedAt: started, FinishedAt: time.Now().UTC(), FaultBootID: fault.BootID, RecoveryBootID: bootID,
		FaultSHA256: qualificationSHA256(faultRaw), ProofSHA256: qualificationSHA256(proofRaw), RecoveryPlanSHA256: qualificationSHA256(planRaw), RecoveryReadySHA256: qualificationSHA256(recoveryReadyRaw), DatabaseArtifact: artifact,
		BeforeSHA256: fault.BeforeSHA256, AfterSHA256: fmt.Sprintf("%x", after.SHA256), BeforeSequence: fault.BeforeSequence, RecoveredSequence: after.Meta.CommitSequence,
		OfflineVerified: true, FreeSpaceValid: after.FreeSpaceValid, PersistentFreeSpace: after.PersistentFreeSpace, Passed: true,
	}
	if err := validateDestructiveFlushEIOWorkerResult(result); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(output, result); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(result)
}

func validateDestructiveFlushEIOWorkerResult(result destructiveFlushEIOWorkerResult) error {
	if result.SchemaVersion != destructiveFlushEIOResultSchema || (result.BuildRevision != "" && !validDurabilitySourceRevision(result.BuildRevision)) || result.GOOS == "" || result.GOARCH == "" || result.GoVersion == "" || result.StartedAt.IsZero() || !result.FinishedAt.After(result.StartedAt) || !qualificationSafeName(result.FaultBootID, 64) || !qualificationSafeName(result.RecoveryBootID, 64) || result.FaultBootID == result.RecoveryBootID || !qualificationHexDigest(result.FaultSHA256) || !qualificationHexDigest(result.ProofSHA256) || !qualificationHexDigest(result.RecoveryPlanSHA256) || !qualificationHexDigest(result.RecoveryReadySHA256) || !filepath.IsAbs(result.DatabaseArtifact) || !qualificationHexDigest(result.BeforeSHA256) || !qualificationHexDigest(result.AfterSHA256) || result.BeforeSequence == 0 || result.RecoveredSequence != result.BeforeSequence || !result.OfflineVerified || !result.FreeSpaceValid || !result.PersistentFreeSpace || !result.Passed {
		return errors.New("flush EIO worker result is incomplete")
	}
	return nil
}

func runDestructiveFlushEIOResultCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-flush-eio-result-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	resultPath := flags.String("result", "", "guest flush EIO result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *resultPath == "" {
		return errors.New("destructive-flush-eio-result-check requires --result")
	}
	var result destructiveFlushEIOWorkerResult
	raw, err := readQualificationReceipt(*resultPath, &result)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIOWorkerResult(result); err != nil {
		return err
	}
	verified, err := verifyDestructiveEIODatabase(result.DatabaseArtifact)
	if err != nil || fmt.Sprintf("%x", verified.SHA256) != result.AfterSHA256 || verified.Meta.CommitSequence != result.RecoveredSequence || !verified.PersistentFreeSpace || !verified.FreeSpaceValid {
		return errors.Join(err, errors.New("flush EIO result database is missing or mismatched"))
	}
	return json.NewEncoder(stdout).Encode(struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		ResultSHA256  string `json:"resultSha256"`
		Passed        bool   `json:"passed"`
	}{1, qualificationSHA256(raw), true})
}
