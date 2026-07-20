package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

const destructiveVolatileLossSchema uint32 = 1

var destructiveVolatileLossDocumentID = [16]byte{15: 0xa1}

type destructiveVolatileLossSeed struct {
	SchemaVersion  uint32 `json:"schemaVersion"`
	BuildRevision  string `json:"buildRevision,omitempty"`
	BuildModified  bool   `json:"buildModified"`
	GOOS           string `json:"goos"`
	GOARCH         string `json:"goarch"`
	GoVersion      string `json:"goVersion"`
	DatabaseSHA256 string `json:"databaseSha256"`
	CommitSequence uint64 `json:"commitSequence"`
	StateSHA256    string `json:"stateSha256"`
	Passed         bool   `json:"passed"`
}

type destructiveVolatileLossMarker struct {
	SchemaVersion              uint32    `json:"schemaVersion"`
	BuildRevision              string    `json:"buildRevision,omitempty"`
	BuildModified              bool      `json:"buildModified"`
	GOOS                       string    `json:"goos"`
	GOARCH                     string    `json:"goarch"`
	GoVersion                  string    `json:"goVersion"`
	BootID                     string    `json:"bootId"`
	StartedAt                  time.Time `json:"startedAt"`
	CommitAcknowledgedAt       time.Time `json:"commitAcknowledgedAt"`
	BeforeSHA256               string    `json:"beforeSha256"`
	AcknowledgedSHA256         string    `json:"acknowledgedSha256"`
	OldCommitSequence          uint64    `json:"oldCommitSequence"`
	AcknowledgedCommitSequence uint64    `json:"acknowledgedCommitSequence"`
	OldStateSHA256             string    `json:"oldStateSha256"`
	AcknowledgedStateSHA256    string    `json:"acknowledgedStateSha256"`
}

type destructiveVolatileLossResult struct {
	SchemaVersion              uint32    `json:"schemaVersion"`
	BuildRevision              string    `json:"buildRevision,omitempty"`
	BuildModified              bool      `json:"buildModified"`
	GOOS                       string    `json:"goos"`
	GOARCH                     string    `json:"goarch"`
	GoVersion                  string    `json:"goVersion"`
	BootIDBefore               string    `json:"bootIdBefore"`
	BootIDAfter                string    `json:"bootIdAfter"`
	RecoveredAt                time.Time `json:"recoveredAt"`
	MarkerSHA256               string    `json:"markerSha256"`
	ControllerSHA256           string    `json:"controllerSha256"`
	DatabaseArtifact           string    `json:"databaseArtifact"`
	RecoveredSHA256            string    `json:"recoveredSha256"`
	AcknowledgedCommitSequence uint64    `json:"acknowledgedCommitSequence"`
	RecoveredCommitSequence    uint64    `json:"recoveredCommitSequence"`
	AcknowledgedStateSHA256    string    `json:"acknowledgedStateSha256"`
	RecoveredStateSHA256       string    `json:"recoveredStateSha256"`
	AcknowledgedCommitLost     bool      `json:"acknowledgedCommitLost"`
	MonotonicFloorRejected     bool      `json:"monotonicFloorRejected"`
	UnsafeStorageDetected      bool      `json:"unsafeStorageDetected"`
	NegativeControlPassed      bool      `json:"negativeControlPassed"`
}

type destructiveVolatileLossRecoveryReady struct {
	SchemaVersion uint32    `json:"schemaVersion"`
	BuildRevision string    `json:"buildRevision,omitempty"`
	BuildModified bool      `json:"buildModified"`
	GOOS          string    `json:"goos"`
	GOARCH        string    `json:"goarch"`
	GoVersion     string    `json:"goVersion"`
	BootIDBefore  string    `json:"bootIdBefore"`
	BootIDAfter   string    `json:"bootIdAfter"`
	ReadyAt       time.Time `json:"readyAt"`
	MarkerSHA256  string    `json:"markerSha256"`
}

func runDestructiveVolatileLossSeed(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-volatile-loss-seed", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("database", "", "new database in the durable base image")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *databasePath == "" {
		return errors.New("destructive-volatile-loss-seed requires --database")
	}
	database, err := newAbsolutePath(*databasePath)
	if err != nil {
		return err
	}
	file, _, err := storagev2.Open(database)
	if err != nil {
		return err
	}
	oldState := []byte("durable-old")
	_, applyErr := file.ApplyDocumentTransaction(storagev2.DocumentTransaction{TransactionID: [16]byte{15: 1}, Mutations: []storagev2.DocumentMutation{{Collection: "items", DocumentID: destructiveVolatileLossDocumentID, Operation: storagev2.DocumentInsert, Document: oldState}}})
	if err := errors.Join(applyErr, file.Close()); err != nil {
		return err
	}
	verified, err := verifyDestructiveVolatileLossDatabase(database)
	if err != nil {
		return err
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	result := destructiveVolatileLossSeed{SchemaVersion: 1, BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(), DatabaseSHA256: fmt.Sprintf("%x", verified.SHA256), CommitSequence: verified.Meta.CommitSequence, StateSHA256: bytesSHA256(oldState), Passed: true}
	if err := validateDestructiveVolatileLossSeed(result); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func runDestructiveVolatileLossUpdate(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-volatile-loss-update", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("database", "", "database through the volatile snapshot device")
	markerPath := flags.String("marker", "", "new durable acknowledgement marker on independent control storage")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *databasePath == "" || *markerPath == "" {
		return errors.New("destructive-volatile-loss-update requires --database and --marker")
	}
	database, err := existingRegularAbsolutePath(*databasePath)
	if err != nil {
		return err
	}
	marker, err := newAbsolutePath(*markerPath)
	if err != nil || filepath.Dir(marker) == filepath.Dir(database) {
		return errors.Join(err, errors.New("volatile-loss marker must be on independent storage"))
	}
	before, err := verifyDestructiveVolatileLossDatabase(database)
	if err != nil || before.Meta.CommitSequence != 1 {
		return errors.Join(err, errors.New("volatile-loss source is not sequence 1"))
	}
	bootID, err := destructiveBootIDFn()
	if err != nil {
		return err
	}
	started := time.Now().UTC()
	file, meta, err := storagev2.Open(database)
	if err != nil {
		return err
	}
	old, exists, err := file.GetDocument("items", destructiveVolatileLossDocumentID)
	if err != nil || !exists || string(old) != "durable-old" {
		_ = file.Close()
		return errors.Join(err, errors.New("volatile-loss old state is missing"))
	}
	newState := []byte("acknowledged-new")
	sequence, applyErr := file.ApplyDocumentTransaction(storagev2.DocumentTransaction{TransactionID: [16]byte{15: 2}, Mutations: []storagev2.DocumentMutation{{Collection: "items", DocumentID: destructiveVolatileLossDocumentID, Operation: storagev2.DocumentUpdate, Document: newState}}})
	closeErr := file.Close()
	if applyErr != nil || closeErr != nil || sequence != meta.CommitSequence+1 {
		return errors.Join(applyErr, closeErr, errors.New("volatile-loss update was not acknowledged"))
	}
	after, err := verifyDestructiveVolatileLossDatabase(database)
	if err != nil || after.Meta.CommitSequence != sequence {
		return errors.Join(err, errors.New("acknowledged volatile generation did not verify before the cut"))
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	receipt := destructiveVolatileLossMarker{SchemaVersion: 1, BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(), BootID: bootID, StartedAt: started, CommitAcknowledgedAt: time.Now().UTC(), BeforeSHA256: fmt.Sprintf("%x", before.SHA256), AcknowledgedSHA256: fmt.Sprintf("%x", after.SHA256), OldCommitSequence: before.Meta.CommitSequence, AcknowledgedCommitSequence: after.Meta.CommitSequence, OldStateSHA256: bytesSHA256(old), AcknowledgedStateSHA256: bytesSHA256(newState)}
	if err := validateDestructiveVolatileLossMarker(receipt); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(marker, receipt); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(receipt); err != nil {
		return err
	}
	waitForDestructiveKill()
	return errors.New("volatile-loss worker resumed without SIGKILL")
}

func runDestructiveVolatileLossRecover(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-volatile-loss-recover", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("database", "", "database after reboot without the volatile snapshot")
	markerPath := flags.String("marker", "", "acknowledged-commit marker")
	controllerPath := flags.String("controller", "", "volatile-loss controller event")
	proofPath := flags.String("proof", "", "QMP volatile-loss controller proof")
	artifactPath := flags.String("artifact", "", "new recovered database artifact")
	outputPath := flags.String("out", "", "new negative-control result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *databasePath == "" || *markerPath == "" || *controllerPath == "" || *proofPath == "" || *artifactPath == "" || *outputPath == "" {
		return errors.New("destructive-volatile-loss-recover requires database, marker, controller, proof, artifact and out")
	}
	database, err := existingRegularAbsolutePath(*databasePath)
	if err != nil {
		return err
	}
	paths := make([]string, 5)
	for index, value := range []string{*markerPath, *controllerPath, *proofPath, *artifactPath, *outputPath} {
		paths[index], err = filepath.Abs(filepath.Clean(value))
		if err != nil {
			return err
		}
	}
	markerClean, controllerClean, proofClean, artifactClean, outputClean := paths[0], paths[1], paths[2], paths[3], paths[4]
	for _, path := range paths {
		if filepath.Dir(path) != filepath.Dir(markerClean) || filepath.Dir(path) == filepath.Dir(database) {
			return errors.New("volatile-loss recovery artifacts must share independent control storage")
		}
	}
	var marker destructiveVolatileLossMarker
	markerRaw, err := readQualificationReceipt(markerClean, &marker)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolatileLossMarker(marker); err != nil {
		return err
	}
	var controller destructiveVolatileLossControllerEvent
	controllerRaw, err := readQualificationReceipt(controllerClean, &controller)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolatileLossControllerEvent(controller, qualificationSHA256(markerRaw)); err != nil {
		return err
	}
	var proof destructiveQMPVolatileLossProof
	proofRaw, err := readQualificationReceipt(proofClean, &proof)
	if err != nil {
		return err
	}
	if err := validateQMPVolatileLossProofStructure(proof); err != nil {
		return err
	}
	if controller.ProofSHA256 != qualificationSHA256(proofRaw) || proof.MarkerSHA256 != qualificationSHA256(markerRaw) || proof.RecoveryReadySHA256 != controller.RecoveryReadySHA256 {
		return errors.New("volatile-loss controller event does not bind its QMP proof")
	}
	bootID, err := destructiveBootIDFn()
	if err != nil {
		return err
	}
	if bootID == marker.BootID || bootID != controller.BootIDAfter {
		return errors.New("volatile-loss recovery did not occur in the controller-proven replacement boot")
	}
	verified, err := verifyDestructiveVolatileLossDatabase(database)
	if err != nil {
		return err
	}
	rejected, _, _, floorErr := storagev2.OpenWithOptions(database, storagev2.OpenOptions{MinimumCommitSequence: marker.AcknowledgedCommitSequence})
	if rejected != nil {
		_ = rejected.Close()
	}
	if !errors.Is(floorErr, storagev2.ErrStaleSnapshot) {
		return errors.Join(floorErr, errors.New("volatile-loss stale generation bypassed the monotonic floor"))
	}
	file, meta, err := storagev2.Open(database)
	if err != nil {
		return err
	}
	value, exists, readErr := file.GetDocument("items", destructiveVolatileLossDocumentID)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || !exists || meta.CommitSequence != marker.OldCommitSequence || string(value) != "durable-old" {
		return errors.Join(readErr, closeErr, errors.New("volatile-loss negative control did not recover the durable old state"))
	}
	if err := copyFileExclusiveDurable(artifactClean, database); err != nil {
		return err
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	result := destructiveVolatileLossResult{SchemaVersion: 1, BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(), BootIDBefore: marker.BootID, BootIDAfter: bootID, RecoveredAt: time.Now().UTC(), MarkerSHA256: qualificationSHA256(markerRaw), ControllerSHA256: qualificationSHA256(controllerRaw), DatabaseArtifact: artifactClean, RecoveredSHA256: fmt.Sprintf("%x", verified.SHA256), AcknowledgedCommitSequence: marker.AcknowledgedCommitSequence, RecoveredCommitSequence: meta.CommitSequence, AcknowledgedStateSHA256: marker.AcknowledgedStateSHA256, RecoveredStateSHA256: bytesSHA256(value), AcknowledgedCommitLost: true, MonotonicFloorRejected: true, UnsafeStorageDetected: true, NegativeControlPassed: true}
	if err := validateDestructiveVolatileLossResult(result); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(outputClean, result); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func runDestructiveVolatileLossRecoveryReady(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-volatile-loss-recovery-ready", flag.ContinueOnError)
	flags.SetOutput(stderr)
	markerPath := flags.String("marker", "", "acknowledged-commit marker")
	outputPath := flags.String("out", "", "new replacement-boot ready receipt")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *markerPath == "" || *outputPath == "" {
		return errors.New("destructive-volatile-loss-recovery-ready requires marker and out")
	}
	var marker destructiveVolatileLossMarker
	markerRaw, err := readQualificationReceipt(*markerPath, &marker)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolatileLossMarker(marker); err != nil {
		return err
	}
	bootID, err := destructiveBootIDFn()
	if err != nil {
		return err
	}
	if bootID == marker.BootID {
		return errors.New("volatile-loss replacement boot reused the original boot ID")
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	ready := destructiveVolatileLossRecoveryReady{SchemaVersion: 1, BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(), BootIDBefore: marker.BootID, BootIDAfter: bootID, ReadyAt: time.Now().UTC(), MarkerSHA256: qualificationSHA256(markerRaw)}
	if err := validateDestructiveVolatileLossRecoveryReady(ready); err != nil {
		return err
	}
	output, err := newAbsolutePath(*outputPath)
	if err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(output, ready); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(ready)
}

func verifyDestructiveVolatileLossDatabase(path string) (storagev2.VerificationResult, error) {
	return storagev2.VerifyPathContextWithIndexAudit(context.Background(), path, func(storagev2.IndexMeta, [16]byte, []byte) ([]byte, bool, error) {
		return nil, false, errors.New("volatile-loss fixture unexpectedly contains an index")
	})
}

func validateDestructiveVolatileLossSeed(seed destructiveVolatileLossSeed) error {
	if seed.SchemaVersion != 1 || (seed.BuildRevision != "" && !validDurabilitySourceRevision(seed.BuildRevision)) || seed.GOOS == "" || seed.GOARCH == "" || seed.GoVersion == "" || !qualificationHexDigest(seed.DatabaseSHA256) || seed.CommitSequence != 1 || !qualificationHexDigest(seed.StateSHA256) || !seed.Passed {
		return errors.New("volatile-loss seed is incomplete")
	}
	return nil
}

func validateDestructiveVolatileLossMarker(marker destructiveVolatileLossMarker) error {
	if marker.SchemaVersion != destructiveVolatileLossSchema || (marker.BuildRevision != "" && !validDurabilitySourceRevision(marker.BuildRevision)) || marker.GOOS == "" || marker.GOARCH == "" || marker.GoVersion == "" || !qualificationSafeName(marker.BootID, 64) || marker.StartedAt.IsZero() || !marker.CommitAcknowledgedAt.After(marker.StartedAt) || !qualificationHexDigest(marker.BeforeSHA256) || !qualificationHexDigest(marker.AcknowledgedSHA256) || marker.BeforeSHA256 == marker.AcknowledgedSHA256 || marker.OldCommitSequence != 1 || marker.AcknowledgedCommitSequence != 2 || !qualificationHexDigest(marker.OldStateSHA256) || !qualificationHexDigest(marker.AcknowledgedStateSHA256) || marker.OldStateSHA256 == marker.AcknowledgedStateSHA256 {
		return errors.New("volatile-loss marker is incomplete")
	}
	return nil
}

func validateDestructiveVolatileLossResult(result destructiveVolatileLossResult) error {
	if result.SchemaVersion != 1 || (result.BuildRevision != "" && !validDurabilitySourceRevision(result.BuildRevision)) || result.GOOS == "" || result.GOARCH == "" || result.GoVersion == "" || !qualificationSafeName(result.BootIDBefore, 64) || !qualificationSafeName(result.BootIDAfter, 64) || result.BootIDBefore == result.BootIDAfter || result.RecoveredAt.IsZero() || !qualificationHexDigest(result.MarkerSHA256) || !qualificationHexDigest(result.ControllerSHA256) || !filepath.IsAbs(result.DatabaseArtifact) || !qualificationHexDigest(result.RecoveredSHA256) || result.AcknowledgedCommitSequence != 2 || result.RecoveredCommitSequence != 1 || !qualificationHexDigest(result.AcknowledgedStateSHA256) || !qualificationHexDigest(result.RecoveredStateSHA256) || result.AcknowledgedStateSHA256 == result.RecoveredStateSHA256 || !result.AcknowledgedCommitLost || !result.MonotonicFloorRejected || !result.UnsafeStorageDetected || !result.NegativeControlPassed {
		return errors.New("volatile-loss result is incomplete")
	}
	return nil
}

func validateDestructiveVolatileLossRecoveryReady(ready destructiveVolatileLossRecoveryReady) error {
	if ready.SchemaVersion != 1 || (ready.BuildRevision != "" && !validDurabilitySourceRevision(ready.BuildRevision)) || ready.GOOS == "" || ready.GOARCH == "" || ready.GoVersion == "" || !qualificationSafeName(ready.BootIDBefore, 64) || !qualificationSafeName(ready.BootIDAfter, 64) || ready.BootIDBefore == ready.BootIDAfter || ready.ReadyAt.IsZero() || !qualificationHexDigest(ready.MarkerSHA256) {
		return errors.New("volatile-loss recovery-ready receipt is incomplete")
	}
	return nil
}
