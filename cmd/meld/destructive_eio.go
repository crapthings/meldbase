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
	"syscall"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

const destructiveEIOResultSchema uint32 = 1

var destructiveEIODocumentID = [16]byte{15: 0xe1}

type destructiveEIOSeedResult struct {
	SchemaVersion  uint32 `json:"schemaVersion"`
	BuildRevision  string `json:"buildRevision,omitempty"`
	BuildModified  bool   `json:"buildModified"`
	GOOS           string `json:"goos"`
	GOARCH         string `json:"goarch"`
	GoVersion      string `json:"goVersion"`
	DatabaseSHA256 string `json:"databaseSha256"`
	CommitSequence uint64 `json:"commitSequence"`
	PhysicalPages  uint64 `json:"physicalPages"`
	ReachablePages uint64 `json:"reachablePages"`
	Reclaimable    uint64 `json:"reclaimablePages"`
	ReusablePages  uint64 `json:"reusablePages"`
	Passed         bool   `json:"passed"`
}

type destructiveEIOWorkerResult struct {
	SchemaVersion       uint32    `json:"schemaVersion"`
	BuildRevision       string    `json:"buildRevision,omitempty"`
	BuildModified       bool      `json:"buildModified"`
	GOOS                string    `json:"goos"`
	GOARCH              string    `json:"goarch"`
	GoVersion           string    `json:"goVersion"`
	StartedAt           time.Time `json:"startedAt"`
	FinishedAt          time.Time `json:"finishedAt"`
	BootID              string    `json:"bootId"`
	DatabaseArtifact    string    `json:"databaseArtifact"`
	BeforeSHA256        string    `json:"beforeSha256"`
	AfterSHA256         string    `json:"afterSha256"`
	BeforeSequence      uint64    `json:"beforeSequence"`
	RecoveredSequence   uint64    `json:"recoveredSequence"`
	FirstErrorIsEIO     bool      `json:"firstErrorIsEio"`
	PoisonedErrorIsEIO  bool      `json:"poisonedErrorIsEio"`
	ReadAfterError      bool      `json:"readAfterError"`
	OfflineVerified     bool      `json:"offlineVerified"`
	IndexVerified       bool      `json:"indexVerified"`
	FreeSpaceValid      bool      `json:"freeSpaceValid"`
	PersistentFreeSpace bool      `json:"persistentFreeSpace"`
	Passed              bool      `json:"passed"`
}

func runDestructiveEIOSeed(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-eio-seed", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("database", "", "new V2 database prepared with reusable physical pages")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *databasePath == "" {
		return errors.New("destructive-eio-seed requires --database")
	}
	database, err := newAbsolutePath(*databasePath)
	if err != nil {
		return err
	}
	file, _, err := storagev2.Open(database)
	if err != nil {
		return err
	}
	closeWith := func(operationErr error) error { return errors.Join(operationErr, file.Close()) }
	value := []byte("stable-00")
	if _, err := file.ApplyDocumentTransaction(storagev2.DocumentTransaction{TransactionID: [16]byte{15: 1}, Mutations: []storagev2.DocumentMutation{{
		Collection: "items", DocumentID: destructiveEIODocumentID, Operation: storagev2.DocumentInsert, Document: value,
	}}}); err != nil {
		return closeWith(err)
	}
	for ordinal := byte(2); ordinal <= 16; ordinal++ {
		value = []byte(fmt.Sprintf("stable-%02d", ordinal))
		if _, err := file.ApplyDocumentTransaction(storagev2.DocumentTransaction{TransactionID: [16]byte{15: ordinal}, Mutations: []storagev2.DocumentMutation{{
			Collection: "items", DocumentID: destructiveEIODocumentID, Operation: storagev2.DocumentUpdate, Document: value,
		}}}); err != nil {
			return closeWith(err)
		}
	}
	stats, err := file.ReclaimPages()
	if err != nil || stats.ReclaimablePages == 0 {
		return closeWith(errors.Join(err, errors.New("destructive EIO seed produced no reclaimable pages")))
	}
	if err := file.PersistFreeSpace(); err != nil {
		return closeWith(err)
	}
	meta := file.Meta()
	storageStats := file.StorageStats()
	if storageStats.ReusablePages == 0 || !storageStats.PersistentFreeSpace {
		return closeWith(errors.New("destructive EIO seed did not persist a reusable page pool"))
	}
	if err := file.Close(); err != nil {
		return err
	}
	verified, err := verifyDestructiveEIODatabase(database)
	if err != nil || verified.Meta.CommitSequence != meta.CommitSequence || !verified.PersistentFreeSpace || !verified.FreeSpaceValid {
		return errors.Join(err, errors.New("destructive EIO seed failed offline verification"))
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	result := destructiveEIOSeedResult{
		SchemaVersion: 1, BuildRevision: buildRevision, BuildModified: buildModified,
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		DatabaseSHA256: fmt.Sprintf("%x", verified.SHA256), CommitSequence: verified.Meta.CommitSequence,
		PhysicalPages: verified.PhysicalPages, ReachablePages: verified.ReachablePages, Reclaimable: verified.ReclaimablePages,
		ReusablePages: storageStats.ReusablePages, Passed: true,
	}
	if err := validateDestructiveEIOSeedResult(result); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func validateDestructiveEIOSeedResult(result destructiveEIOSeedResult) error {
	if result.SchemaVersion != 1 || result.GOOS == "" || result.GOARCH == "" || result.GoVersion == "" ||
		(result.BuildRevision != "" && !validDurabilitySourceRevision(result.BuildRevision)) || !qualificationHexDigest(result.DatabaseSHA256) ||
		result.CommitSequence == 0 || result.PhysicalPages < 2 || result.ReachablePages == 0 || result.ReachablePages > result.PhysicalPages ||
		result.Reclaimable == 0 || result.ReusablePages == 0 || result.ReusablePages > result.Reclaimable || !result.Passed {
		return errors.New("destructive EIO seed result is incomplete")
	}
	return nil
}

func runDestructiveEIOWorker(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-eio-worker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("database", "", "existing seeded V2 database on the injected block device")
	outputPath := flags.String("out", "", "new result path on the independent control device")
	artifactPath := flags.String("artifact", "", "new recovered database copy on the independent control device")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *databasePath == "" || *outputPath == "" || *artifactPath == "" {
		return errors.New("destructive-eio-worker requires --database, --artifact and --out")
	}
	database, err := existingRegularAbsolutePath(*databasePath)
	if err != nil {
		return err
	}
	output, err := newAbsolutePath(*outputPath)
	if err != nil {
		return err
	}
	artifact, err := newAbsolutePath(*artifactPath)
	if err != nil || artifact == output || filepath.Dir(artifact) != filepath.Dir(output) || filepath.Dir(artifact) == filepath.Dir(database) {
		return errors.Join(err, errors.New("destructive EIO artifact must be a distinct new path"))
	}
	before, err := verifyDestructiveEIODatabase(database)
	if err != nil || !before.PersistentFreeSpace || !before.FreeSpaceValid {
		return errors.Join(err, errors.New("destructive EIO worker source is not a verified reusable-page fixture"))
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
	stable, exists, err := file.GetDocument("items", destructiveEIODocumentID)
	if err != nil || !exists || string(stable) != "stable-16" {
		_ = file.Close()
		return errors.Join(err, errors.New("destructive EIO stable document is missing"))
	}
	firstErr := applyDestructiveEIOUpdate(file, 0xe2, "must-not-commit-1")
	poisonedErr := applyDestructiveEIOUpdate(file, 0xe3, "must-not-commit-2")
	afterRead, afterExists, readErr := file.GetDocument("items", destructiveEIODocumentID)
	closeErr := file.Close()
	if !errors.Is(firstErr, syscall.EIO) || !errors.Is(poisonedErr, syscall.EIO) || readErr != nil || !afterExists || string(afterRead) != "stable-16" || closeErr != nil {
		return errors.Join(firstErr, poisonedErr, readErr, closeErr, errors.New("destructive EIO worker did not observe fail-stop write behavior"))
	}
	after, err := verifyDestructiveEIODatabase(database)
	if err != nil || after.Meta.CommitSequence != meta.CommitSequence || after.Meta.DatabaseID != before.Meta.DatabaseID ||
		!after.PersistentFreeSpace || !after.FreeSpaceValid {
		return errors.Join(err, errors.New("destructive EIO recovery did not preserve the old verified generation"))
	}
	if err := copyFileExclusiveDurable(artifact, database); err != nil {
		return err
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	result := destructiveEIOWorkerResult{
		SchemaVersion: destructiveEIOResultSchema, BuildRevision: buildRevision, BuildModified: buildModified,
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		StartedAt: started, FinishedAt: time.Now().UTC(), BootID: bootID,
		DatabaseArtifact: artifact, BeforeSHA256: fmt.Sprintf("%x", before.SHA256), AfterSHA256: fmt.Sprintf("%x", after.SHA256),
		BeforeSequence: meta.CommitSequence, RecoveredSequence: after.Meta.CommitSequence,
		FirstErrorIsEIO: true, PoisonedErrorIsEIO: true, ReadAfterError: true, OfflineVerified: true,
		IndexVerified: true, FreeSpaceValid: after.FreeSpaceValid, PersistentFreeSpace: after.PersistentFreeSpace, Passed: true,
	}
	if err := validateDestructiveEIOWorkerResult(result); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(output, result); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(result)
}

func applyDestructiveEIOUpdate(file *storagev2.File, transactionByte byte, value string) error {
	_, err := file.ApplyDocumentTransaction(storagev2.DocumentTransaction{TransactionID: [16]byte{15: transactionByte}, Mutations: []storagev2.DocumentMutation{{
		Collection: "items", DocumentID: destructiveEIODocumentID, Operation: storagev2.DocumentUpdate, Document: []byte(value),
	}}})
	return err
}

func verifyDestructiveEIODatabase(path string) (storagev2.VerificationResult, error) {
	return storagev2.VerifyPathContextWithIndexAudit(context.Background(), path, func(storagev2.IndexMeta, [16]byte, []byte) ([]byte, bool, error) {
		return nil, false, errors.New("destructive EIO fixture unexpectedly contains an index")
	})
}

func validateDestructiveEIOWorkerResult(result destructiveEIOWorkerResult) error {
	if result.SchemaVersion != destructiveEIOResultSchema || result.GOOS == "" || result.GOARCH == "" || result.GoVersion == "" ||
		(result.BuildRevision != "" && !validDurabilitySourceRevision(result.BuildRevision)) ||
		result.StartedAt.IsZero() || !result.FinishedAt.After(result.StartedAt) ||
		!qualificationSafeName(result.BootID, 64) || !filepath.IsAbs(result.DatabaseArtifact) ||
		!qualificationHexDigest(result.BeforeSHA256) || !qualificationHexDigest(result.AfterSHA256) || result.BeforeSequence == 0 ||
		result.RecoveredSequence != result.BeforeSequence || !result.FirstErrorIsEIO || !result.PoisonedErrorIsEIO ||
		!result.ReadAfterError || !result.OfflineVerified || !result.IndexVerified || !result.FreeSpaceValid || !result.PersistentFreeSpace || !result.Passed {
		return errors.New("destructive EIO worker result is incomplete")
	}
	return nil
}

func runDestructiveEIOResultCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-eio-result-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	resultPath := flags.String("result", "", "guest EIO result whose retained database will be reopened")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *resultPath == "" {
		return errors.New("destructive-eio-result-check requires --result")
	}
	var result destructiveEIOWorkerResult
	raw, err := readQualificationReceipt(*resultPath, &result)
	if err != nil {
		return err
	}
	if err := validateDestructiveEIOWorkerResult(result); err != nil {
		return err
	}
	verified, err := verifyDestructiveEIODatabase(result.DatabaseArtifact)
	if err != nil || fmt.Sprintf("%x", verified.SHA256) != result.AfterSHA256 || verified.Meta.CommitSequence != result.RecoveredSequence ||
		!verified.PersistentFreeSpace || !verified.FreeSpaceValid {
		return errors.Join(err, errors.New("destructive EIO result database is missing or mismatched"))
	}
	packet := struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		ResultSHA256  string `json:"resultSha256"`
		Passed        bool   `json:"passed"`
	}{SchemaVersion: 1, ResultSHA256: qualificationSHA256(raw), Passed: true}
	return json.NewEncoder(stdout).Encode(packet)
}
