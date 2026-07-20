package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/crapthings/meldbase/core"
	meldserver "github.com/crapthings/meldbase/server"
)

func TestDurabilityCheckExercisesTargetDirectoryAndCleansUp(t *testing.T) {
	directory := t.TempDir()
	revision := "0123456789abcdef0123456789abcdef01234567"
	var stdout, stderr bytes.Buffer
	if err := run([]string{"durability-check", "--dir", directory, "--source-revision", revision}, &stdout, &stderr); err != nil {
		t.Fatalf("durability check error=%v stderr=%s output=%s", err, stderr.String(), stdout.String())
	}
	var result durabilityCheckResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.SchemaVersion != 2 || result.SourceRevision != revision || !result.Passed || result.Directory != directory ||
		result.Duration <= 0 || result.FinishedAt.Before(result.StartedAt) || result.GOOS == "" || result.GOARCH == "" || result.GoVersion == "" ||
		result.FilesystemType == "" || result.FilesystemName == "" || result.BlockSize == 0 || result.TotalBytes == 0 || result.AvailableBytes > result.TotalBytes {
		t.Fatalf("result=%+v", result)
	}
	want := []string{
		"create-private-probe-directory", "file-write-and-fsync", "parent-directory-fsync-after-create", "probe-directory-fsync",
		"exclusive-advisory-lock-and-close-release", "atomic-no-overwrite-link", "same-directory-rename-and-fsync",
		"meldbase-create-indexed-commit-reopen", "meldbase-offline-full-verification", "cleanup-and-parent-fsync",
	}
	if len(result.Checks) != len(want) {
		t.Fatalf("checks=%+v", result.Checks)
	}
	for index, check := range result.Checks {
		if check.Name != want[index] || !check.Passed || check.Duration <= 0 || check.Error != "" {
			t.Fatalf("check[%d]=%+v", index, check)
		}
	}
	if proof := result.Database; proof == nil || proof.VerificationSchema != 3 || proof.FormatRevision != 3 || proof.CommitSequence != 3 ||
		proof.FileBytes == 0 || proof.PhysicalPages == 0 || proof.ReachablePages == 0 || !proof.IndexVerified || !proof.FreeSpaceValid || len(proof.SHA256) != 64 {
		t.Fatalf("database proof=%+v", proof)
	}
	leftovers, err := filepath.Glob(filepath.Join(directory, ".meldbase-durability-probe-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("probe leftovers=%v err=%v", leftovers, err)
	}
}

func TestDurabilityCheckRejectsInvalidSourceRevision(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"durability-check", "--dir", t.TempDir(), "--source-revision", "not-a-revision"}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "source-revision") {
		t.Fatalf("error=%v", err)
	}
}

func TestDurabilityCheckPublishesNoOverwriteDurableReceipt(t *testing.T) {
	directory := t.TempDir()
	receiptPath := filepath.Join(t.TempDir(), "durability.json")
	var stdout, stderr bytes.Buffer
	if err := run([]string{"durability-check", "--dir", directory, "--out", receiptPath}, &stdout, &stderr); err != nil {
		t.Fatalf("durability check error=%v stderr=%s", err, stderr.String())
	}
	var fromOutput, fromFile durabilityCheckResult
	if err := json.Unmarshal(stdout.Bytes(), &fromOutput); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fromFile); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fromOutput, fromFile) {
		t.Fatalf("stdout and durable receipt differ")
	}
	if err := run([]string{"durability-check", "--dir", directory, "--out", receiptPath}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Fatalf("no-overwrite error=%v", err)
	}
}

func TestDurabilityCheckCleanSourceRequiresClaimedRevision(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"durability-check", "--dir", t.TempDir(), "--require-clean-source"}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "clean source verification") {
		t.Fatalf("error=%v", err)
	}
}

func TestCheckedFilesystemBytesRejectsInvalidGeometry(t *testing.T) {
	if value, err := checkedFilesystemBytes(8, 4096); err != nil || value != 32768 {
		t.Fatalf("value=%d err=%v", value, err)
	}
	if _, err := checkedFilesystemBytes(1, 0); err == nil {
		t.Fatal("zero block size accepted")
	}
	if _, err := checkedFilesystemBytes(^uint64(0), 2); err == nil {
		t.Fatal("capacity overflow accepted")
	}
}

func TestDurabilityCheckRejectsNonDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err := run([]string{"durability-check", "--dir", path}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "must be a directory") {
		t.Fatalf("error=%v", err)
	}
}

func TestInspectCommandReportsCompatibleFormatAsJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inspect.meld")
	db, err := meldbase.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"inspect", "--db", path, "--require-compatible"}, &stdout, &stderr); err != nil {
		t.Fatalf("inspect error=%v stderr=%s", err, stderr.String())
	}
	var info meldbase.StorageFormatInfo
	if err := json.Unmarshal(stdout.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.Format != meldbase.StorageFormatCurrent || info.Revision != 3 || !info.ReaderCompatible || info.DatabaseIDHex == "" {
		t.Fatalf("inspect info=%+v", info)
	}
}

func TestIndexBuildCommandStartListResumeLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index-build.meld2")
	db, err := meldbase.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	for value := int64(1); value <= 3; value++ {
		if _, err := items.InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(value), "tenant": meldbase.String("a")}); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := run([]string{"index-build", "start", "--db", path, "--collection", "items", "--name", "by_value", "--field", "value:-1", "--field", "tenant:1", "--unique"}, &stdout, &stderr); err != nil {
		t.Fatalf("start=%v stderr=%s", err, stderr.String())
	}
	var started indexBuildCommandResult
	if err := json.Unmarshal(stdout.Bytes(), &started); err != nil || started.SchemaVersion != 1 || started.Action != "start" ||
		started.BuildID == nil || started.Build == nil || started.Build.ID != *started.BuildID || started.Build.Phase != meldbase.IndexBuildPhaseScan ||
		!reflect.DeepEqual(started.Build.Fields, []meldbase.IndexField{{Field: "value", Order: -1}, {Field: "tenant", Order: 1}}) {
		t.Fatalf("started=%+v err=%v output=%s", started, err, stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := run([]string{"index-build", "list", "--db", path}, &stdout, &stderr); err != nil {
		t.Fatalf("list=%v stderr=%s", err, stderr.String())
	}
	var listed indexBuildCommandResult
	if err := json.Unmarshal(stdout.Bytes(), &listed); err != nil || listed.Action != "list" || listed.Builds == nil ||
		len(*listed.Builds) != 1 || (*listed.Builds)[0].ID != *started.BuildID {
		t.Fatalf("listed=%+v err=%v output=%s", listed, err, stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := run([]string{"index-build", "resume", "--db", path, "--id", started.BuildID.String(), "--timeout", "10s"}, &stdout, &stderr); err != nil {
		t.Fatalf("resume=%v stderr=%s", err, stderr.String())
	}
	var resumed indexBuildCommandResult
	if err := json.Unmarshal(stdout.Bytes(), &resumed); err != nil || resumed.Action != "resume" || !resumed.Published ||
		resumed.BuildID == nil || *resumed.BuildID != *started.BuildID {
		t.Fatalf("resumed=%+v err=%v output=%s", resumed, err, stdout.String())
	}

	published, err := meldbase.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	explain, explainErr := published.Collection("items").Explain(context.Background(), meldbase.Filter{"value": int64(3)})
	closeErr := published.Close()
	if explainErr != nil || closeErr != nil || explain.Stage != "IXSCAN" || explain.IndexName != "by_value" {
		t.Fatalf("explain=%+v explainErr=%v closeErr=%v", explain, explainErr, closeErr)
	}

	stdout.Reset()
	if err := run([]string{"index-build", "list", "--db", path}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	listed = indexBuildCommandResult{}
	if err := json.Unmarshal(stdout.Bytes(), &listed); err != nil || listed.Builds == nil || len(*listed.Builds) != 0 {
		t.Fatalf("empty list=%+v err=%v output=%s", listed, err, stdout.String())
	}
}

func TestIndexBuildCommandLeavesUniqueConflictInspectableThenAborts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index-build-conflict.meld2")
	db, err := meldbase.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	for range 2 {
		if _, err := items.InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"index-build", "start", "--db", path, "--collection", "items", "--name", "unique_value", "--field", "value", "--unique"}, &output, &output); err != nil {
		t.Fatal(err)
	}
	var started indexBuildCommandResult
	if err := json.Unmarshal(output.Bytes(), &started); err != nil || started.BuildID == nil {
		t.Fatalf("started=%+v err=%v", started, err)
	}
	output.Reset()
	err = run([]string{"index-build", "resume", "--db", path, "--id", started.BuildID.String()}, &output, &output)
	if !errors.Is(err, meldbase.ErrDuplicateKey) || output.Len() != 0 {
		t.Fatalf("resume conflict=%v output=%s", err, output.String())
	}
	if err := run([]string{"index-build", "list", "--db", path}, &output, &output); err != nil {
		t.Fatal(err)
	}
	var listed indexBuildCommandResult
	if err := json.Unmarshal(output.Bytes(), &listed); err != nil || listed.Builds == nil || len(*listed.Builds) != 1 ||
		(*listed.Builds)[0].Phase != meldbase.IndexBuildPhaseReady {
		t.Fatalf("listed=%+v err=%v output=%s", listed, err, output.String())
	}
	output.Reset()
	if err := run([]string{"index-build", "abort", "--db", path, "--id", started.BuildID.String()}, &output, &output); err != nil {
		t.Fatal(err)
	}
	var aborted indexBuildCommandResult
	if err := json.Unmarshal(output.Bytes(), &aborted); err != nil || !aborted.Aborted || aborted.BuildID == nil || *aborted.BuildID != *started.BuildID {
		t.Fatalf("aborted=%+v err=%v output=%s", aborted, err, output.String())
	}
}

func TestIndexBuildCommandRejectsUnsafeArguments(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"index-build"}, &output, &output); err == nil {
		t.Fatal("missing action accepted")
	}
	if err := run([]string{"index-build", "resume", "--db", "missing", "--id", "bad"}, &output, &output); !errors.Is(err, meldbase.ErrIndexBuildNotFound) {
		t.Fatalf("malformed id=%v", err)
	}
	if err := run([]string{"index-build", "abort", "--db", "missing", "--id", strings.Repeat("01", 16), "--timeout", "-1s"}, &output, &output); err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("negative timeout=%v", err)
	}
}

func TestInspectCommandCompatibilityGateOutputsBeforeFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.meld")
	var output bytes.Buffer
	err := run([]string{"inspect", "--db", path, "--require-compatible"}, &output, &output)
	if !errors.Is(err, meldbase.ErrUnsupportedFormat) {
		t.Fatalf("inspect error=%v", err)
	}
	var info meldbase.StorageFormatInfo
	if jsonErr := json.Unmarshal(output.Bytes(), &info); jsonErr != nil || info.Format != meldbase.StorageFormatUnknown || info.ReaderCompatible {
		t.Fatalf("inspect info=%+v jsonErr=%v", info, jsonErr)
	}
}

func TestVerifyCommandProducesFullReadOnlyAuditJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify.meld2")
	db, err := meldbase.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"verify", "--db", path, "--timeout", "10s"}, &stdout, &stderr); err != nil {
		t.Fatalf("verify error=%v stderr=%s", err, stderr.String())
	}
	var report meldbase.VerificationReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if !report.Verified || report.SchemaVersion != 3 || !report.IndexContentsVerified || !report.IndexBuildContentsVerified || report.Format != meldbase.StorageFormatCurrent ||
		report.ReachablePages == 0 || report.SHA256 == "" {
		t.Fatalf("verify report=%+v", report)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("verify command mutated database: err=%v", err)
	}
}

func TestVerifyCommandReportsActiveWriterLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.meld2")
	db, err := meldbase.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var output bytes.Buffer
	if err := run([]string{"verify", "--db", path}, &output, &output); !errors.Is(err, meldbase.ErrDatabaseLocked) {
		t.Fatalf("locked verify error=%v", err)
	}
}

func TestBackupCommandPublishesVerifiedRestoreArtifactJSON(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.meld2")
	destination := filepath.Join(directory, "backup.meld2")
	db, err := meldbase.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(7)}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	identity := db.DatabaseIdentity()
	sequence := db.Stats().CommitSequence
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := run([]string{"backup", "--db", source, "--out", destination, "--timeout", "10s"}, &stdout, &stderr); err != nil {
		t.Fatalf("backup error=%v stderr=%s", err, stderr.String())
	}
	var result backupCommandResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.SchemaVersion != 1 || result.ArtifactKind != physicalRestoreArtifact || result.Bytes == 0 ||
		result.Pages == 0 || result.CommitSequence != sequence || result.DatabaseIDHex == "" || result.SHA256 == "" {
		t.Fatalf("backup result=%+v", result)
	}
	backup, err := meldbase.Open(destination)
	if err != nil {
		t.Fatal(err)
	}
	if backup.DatabaseIdentity() != identity || backup.Stats().CommitSequence != sequence {
		_ = backup.Close()
		t.Fatalf("backup identity/sequence=%x/%d", backup.DatabaseIdentity(), backup.Stats().CommitSequence)
	}
	if err := backup.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBackupCommandFailsClosedForExistingDestination(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.meld2")
	destination := filepath.Join(directory, "owner")
	db, err := meldbase.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"backup", "--db", source, "--out", destination}, &output, &output); !errors.Is(err, meldbase.ErrBackupDestinationExists) {
		t.Fatalf("existing destination error=%v", err)
	}
	if contents, err := os.ReadFile(destination); err != nil || string(contents) != "owner" {
		t.Fatalf("destination=%q err=%v", contents, err)
	}

}

func TestRestoreCommandImportsVerifiedPhysicalBackup(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.meld2")
	artifact := filepath.Join(directory, "backup.meld2")
	receiptPath := filepath.Join(directory, "backup.json")
	restored := filepath.Join(directory, "restored.meld2")
	db, err := meldbase.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(7)}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	identity := db.DatabaseIdentity()
	sequence := db.Stats().CommitSequence
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var backupOutput, stderr bytes.Buffer
	if err := run([]string{"backup", "--db", source, "--out", artifact}, &backupOutput, &stderr); err != nil {
		t.Fatalf("backup error=%v stderr=%s", err, stderr.String())
	}
	if err := os.WriteFile(receiptPath, backupOutput.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	var restoreOutput bytes.Buffer
	if err := run([]string{"restore", "--in", artifact, "--receipt", receiptPath, "--out", restored, "--timeout", "10s"}, &restoreOutput, &stderr); err != nil {
		t.Fatalf("restore error=%v stderr=%s", err, stderr.String())
	}
	var result backupCommandResult
	if err := json.Unmarshal(restoreOutput.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.SchemaVersion != 1 || result.ArtifactKind != physicalRestoreArtifact || result.CommitSequence != sequence || result.DatabaseIDHex == "" {
		t.Fatalf("restore result=%+v", result)
	}
	reopened, err := meldbase.Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.DatabaseIdentity() != identity || reopened.Stats().CommitSequence != sequence {
		t.Fatalf("restored identity/sequence=%x/%d", reopened.DatabaseIdentity(), reopened.Stats().CommitSequence)
	}
	resultSet, err := reopened.Collection("items").Find(context.Background(), meldbase.Filter{"value": 7})
	if err != nil {
		t.Fatal(err)
	}
	items, err := resultSet.All(context.Background())
	if err != nil || len(items) != 1 {
		t.Fatalf("restored items=%v err=%v", items, err)
	}
}

func TestRestoreCommandFailsClosedForExistingDestinationAndInvalidReceipt(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.meld2")
	artifact := filepath.Join(directory, "backup.meld2")
	receiptPath := filepath.Join(directory, "backup.json")
	destination := filepath.Join(directory, "destination.meld2")
	db, err := meldbase.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var backupOutput, output bytes.Buffer
	if err := run([]string{"backup", "--db", source, "--out", artifact}, &backupOutput, &output); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(receiptPath, backupOutput.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"restore", "--in", artifact, "--receipt", receiptPath, "--out", destination}, &output, &output); !errors.Is(err, meldbase.ErrBackupDestinationExists) {
		t.Fatalf("existing destination error=%v", err)
	}
	if contents, err := os.ReadFile(destination); err != nil || string(contents) != "owner" {
		t.Fatalf("destination=%q err=%v", contents, err)
	}
	if err := os.WriteFile(receiptPath, []byte(`{"schemaVersion":1,"artifactKind":"wrong"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"restore", "--in", artifact, "--receipt", receiptPath, "--out", filepath.Join(directory, "unused.meld2")}, &output, &output); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("invalid receipt error=%v", err)
	}
	if err := os.WriteFile(receiptPath, backupOutput.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"restore", "--in", artifact, "--receipt", receiptPath, "--out", artifact}, &output, &output); err == nil || !strings.Contains(err.Error(), "different paths") {
		t.Fatalf("matching source/destination error=%v", err)
	}
}

func TestDemoExercisesDurabilityIndexUpdateAndReactiveQuery(t *testing.T) {
	var stdout, stderr bytes.Buffer
	path := filepath.Join(t.TempDir(), "demo.meld")
	if err := run([]string{"demo", "--db", path}, &stdout, &stderr); err != nil {
		t.Fatalf("demo error = %v stderr=%s", err, stderr.String())
	}
	for _, expected := range []string{
		"Inserted user:", "Created index: users_email", "Found users: 1",
		"Received snapshot: 1 document", "Received snapshot: 2 documents",
		"Database reopened successfully", "Recovery check passed",
	} {
		if !strings.Contains(stdout.String(), expected) {
			t.Fatalf("demo output missing %q:\n%s", expected, stdout.String())
		}
	}
}

func TestServeRequiresExplicitUnsafeDevelopmentMode(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"serve", "--db", filepath.Join(t.TempDir(), "data.meld")}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "--dev-no-auth") {
		t.Fatalf("serve error = %v", err)
	}
}

func TestCollectionAccessConfigurationHasNoLegacyShorthand(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"serve", "--db", filepath.Join(t.TempDir(), "data.meld"), "--workspace-collections", "tasks"}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("legacy serve shorthand error=%v output=%s", err, output.String())
	}
	output.Reset()
	err = run([]string{"init", "--dir", filepath.Join(t.TempDir(), "bundle"), "--workspace-collections", "tasks"}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("legacy init shorthand error=%v output=%s", err, output.String())
	}
}

func TestServeRejectsInvalidCollectionAccessManifestBeforeOpeningDatabase(t *testing.T) {
	directory := t.TempDir()
	secretPath := filepath.Join(directory, "jwt.secret")
	if err := os.WriteFile(secretPath, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(directory, "access-policy.json")
	if err := os.WriteFile(manifestPath, []byte(`{"version":1,"workspaceField":"workspaceId","collections":[{"collection":"tasks","mode":"unknown"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err := run([]string{
		"serve", "--db", filepath.Join(directory, "app.meld"), "--jwt-hs256-secret-file", secretPath,
		"--jwt-issuer", "identity", "--jwt-audience", "app", "--access-policy-file", manifestPath,
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "collection access manifest") {
		t.Fatalf("invalid manifest error=%v", err)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "app.meld")); !os.IsNotExist(statErr) {
		t.Fatalf("invalid manifest opened database: %v", statErr)
	}
}

func TestAccessPolicyExplainUsesTheServerAuthorizer(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "access-policy.json")
	if err := os.WriteFile(manifestPath, []byte(`{
		"version":1,
		"workspaceField":"workspaceId",
		"collections":[
			{"collection":"notes","mode":"owner","ownerField":"ownerId","fields":{"queryPaths":["title"],"resultFields":["title"],"inputFields":["title"],"updatePaths":["title"]}},
			{"collection":"payroll","mode":"rpc_only"}
		]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{
		"access-policy", "explain", "--file", manifestPath,
		"--subject", "user-a", "--workspace", "team-a", "--collection", "notes",
	}, &output, &output); err != nil {
		t.Fatal(err)
	}
	var owner struct {
		Mode  string `json:"mode"`
		Query struct {
			Allowed    bool            `json:"allowed"`
			Constraint json.RawMessage `json:"constraint"`
			QueryPaths string          `json:"queryPaths"`
		} `json:"query"`
		Insert struct {
			Allowed      bool              `json:"allowed"`
			InputFields  string            `json:"inputFields"`
			ServerFields map[string]string `json:"serverFields"`
		} `json:"insert"`
		Update struct {
			Allowed           bool     `json:"allowed"`
			UpdatePaths       string   `json:"updatePaths"`
			DeniedUpdatePaths []string `json:"deniedUpdatePaths"`
		} `json:"update"`
	}
	if err := json.Unmarshal(output.Bytes(), &owner); err != nil {
		t.Fatal(err)
	}
	if owner.Mode != "owner" || !owner.Query.Allowed || !strings.Contains(string(owner.Query.Constraint), "workspaceId") ||
		!strings.Contains(string(owner.Query.Constraint), "ownerId") || !owner.Insert.Allowed ||
		owner.Insert.ServerFields["workspaceId"] != "team-a" || owner.Insert.ServerFields["ownerId"] != "user-a" ||
		owner.Query.QueryPaths != "[title]" || owner.Insert.InputFields != "[ownerId title workspaceId]" ||
		!owner.Update.Allowed || owner.Update.UpdatePaths != "[title]" ||
		!reflect.DeepEqual(owner.Update.DeniedUpdatePaths, []string{"ownerId", "workspaceId"}) {
		t.Fatalf("owner explanation=%s", output.String())
	}

	output.Reset()
	if err := run([]string{
		"access-policy", "explain", "--file", manifestPath,
		"--subject", "user-a", "--workspace", "team-a", "--collection", "payroll",
	}, &output, &output); err != nil {
		t.Fatal(err)
	}
	var rpcOnly struct {
		Query  struct{ Allowed bool } `json:"query"`
		Insert struct{ Allowed bool } `json:"insert"`
		Update struct{ Allowed bool } `json:"update"`
		Delete struct{ Allowed bool } `json:"delete"`
	}
	if err := json.Unmarshal(output.Bytes(), &rpcOnly); err != nil {
		t.Fatal(err)
	}
	if rpcOnly.Query.Allowed || rpcOnly.Insert.Allowed || rpcOnly.Update.Allowed || rpcOnly.Delete.Allowed {
		t.Fatalf("rpc-only explanation=%s", output.String())
	}
}

func TestInitCreatesPrivateSingleNodeBundle(t *testing.T) {
	root := filepath.Join(t.TempDir(), "meldbase-local")
	var output bytes.Buffer
	if err := run([]string{"init", "--dir", root, "--jwt-issuer", "https://identity.example/", "--jwt-audience", "app-api", "--collections", "projects,tasks"}, &output, &output); err != nil {
		t.Fatalf("init error=%v output=%s", err, output.String())
	}
	for _, path := range []string{
		filepath.Join(root, "data"), filepath.Join(root, "backups"), filepath.Join(root, "rehearsals"),
		filepath.Join(root, "config"), filepath.Join(root, "secrets"),
	} {
		info, err := os.Stat(path)
		if err != nil || !info.IsDir() {
			t.Fatalf("directory %s info=%v err=%v", path, info, err)
		}
	}
	configPath := filepath.Join(root, "config", "meldbase.env")
	config, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"MELDBASE_DB='" + filepath.Join(root, "data", "app.meld") + "'",
		"MELDBASE_JWT_ISSUER='https://identity.example/'", "MELDBASE_JWT_AUDIENCE='app-api'",
		"MELDBASE_ACCESS_POLICY_FILE='" + filepath.Join(root, "config", "access-policy.json") + "'", "MELDBASE_ADMIN_TOKEN='",
		"MELDBASE_HTTP_ORIGINS='http://localhost:5173,http://127.0.0.1:5173,http://[::1]:5173'",
		"MELDBASE_REALTIME_ORIGIN_PATTERNS='localhost:*,127.0.0.1:*,[[]::1]:*'",
	} {
		if !strings.Contains(string(config), expected) {
			t.Fatalf("config missing %q:\n%s", expected, config)
		}
	}
	accessPolicyPath := filepath.Join(root, "config", "access-policy.json")
	for _, path := range []string{configPath, accessPolicyPath, filepath.Join(root, "secrets", "jwt-hs256.secret")} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("mode for %s = %o, want 600", path, info.Mode().Perm())
		}
	}
	accessPolicy, err := os.ReadFile(accessPolicyPath)
	if err != nil || !strings.Contains(string(accessPolicy), `"$schema": "`+meldserver.CollectionAccessManifestSchemaURL+`"`) || !strings.Contains(string(accessPolicy), `"workspaceField": "workspaceId"`) ||
		!strings.Contains(string(accessPolicy), `"collection": "projects"`) || !strings.Contains(string(accessPolicy), `"mode": "collaborative"`) {
		t.Fatalf("access policy=%s err=%v", accessPolicy, err)
	}
	var policyValidation bytes.Buffer
	if err := run([]string{"access-policy", "validate", "--file", accessPolicyPath}, &policyValidation, &policyValidation); err != nil ||
		!strings.Contains(policyValidation.String(), `"$schema": "`+meldserver.CollectionAccessManifestSchemaURL+`"`) {
		t.Fatalf("generated access policy validation=%s err=%v", policyValidation.String(), err)
	}
	secret, err := os.ReadFile(filepath.Join(root, "secrets", "jwt-hs256.secret"))
	if err != nil || len(strings.TrimSpace(string(secret))) < 32 {
		t.Fatalf("secret bytes=%d err=%v", len(secret), err)
	}
	launcher, err := os.ReadFile(filepath.Join(root, "start.sh"))
	if err != nil || !strings.Contains(string(launcher), "--admin-metrics") || !strings.Contains(string(launcher), "--access-policy-file") {
		t.Fatalf("launcher=%q err=%v", launcher, err)
	}
	if !strings.Contains(output.String(), "Start it with: "+filepath.Join(root, "start.sh")) ||
		!strings.Contains(output.String(), "Admin token: "+configPath) || strings.Contains(output.String(), string(secret)) {
		t.Fatalf("unexpected init output: %s", output.String())
	}
}

func TestInitBundleLauncherUsesGeneratedManifest(t *testing.T) {
	root := filepath.Join(t.TempDir(), "meldbase-local")
	var output bytes.Buffer
	if err := run([]string{
		"init", "--dir", root,
		"--public-realtime-url", "wss://api.example/v1/realtime",
		"--http-origins", "https://app.example",
		"--realtime-origin-patterns", "https://app.example",
	}, &output, &output); err != nil {
		t.Fatalf("init error=%v output=%s", err, output.String())
	}
	argumentsPath := filepath.Join(t.TempDir(), "arguments")
	fakeBinary := filepath.Join(t.TempDir(), "fake-meld")
	if err := os.WriteFile(fakeBinary, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$MELDBASE_ARGUMENTS_FILE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(filepath.Join(root, "start.sh"))
	command.Env = append(os.Environ(), "MELDBASE_BIN="+fakeBinary, "MELDBASE_ARGUMENTS_FILE="+argumentsPath)
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("bundle launcher: %v\n%s", err, raw)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"serve", "--db", filepath.Join(root, "data", "app.meld"), "--addr", "127.0.0.1:8080",
		"--jwt-hs256-secret-file", filepath.Join(root, "secrets", "jwt-hs256.secret"),
		"--jwt-issuer", "meldbase-local", "--jwt-audience", "meldbase-api",
		"--access-policy-file", filepath.Join(root, "config", "access-policy.json"),
		"--http-origins", "https://app.example",
		"--realtime-origin-patterns", "https://app.example",
		"--admin-addr", "127.0.0.1:9091", "--admin-diagnostics", "--admin-metrics",
		"--public-realtime-url", "wss://api.example/v1/realtime",
	}
	if got := strings.Split(strings.TrimSpace(string(arguments)), "\n"); !reflect.DeepEqual(got, want) {
		t.Fatalf("bundle arguments=%q want=%q", got, want)
	}
}

func TestInitRefusesExistingOrUnsafeBundleConfiguration(t *testing.T) {
	root := filepath.Join(t.TempDir(), "meldbase-local")
	if err := os.Mkdir(root, 0o750); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err := run([]string{"init", "--dir", root}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing init error=%v", err)
	}
	err = run([]string{"init", "--dir", filepath.Join(t.TempDir(), "unsafe"), "--addr", ":8080"}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("unsafe address error=%v", err)
	}
}

func TestServeWithoutWorkerControlStartsAndStopsCleanly(t *testing.T) {
	const childEnvironment = "MELDBASE_TEST_SERVE_WITHOUT_WORKER_CHILD"
	if os.Getenv(childEnvironment) == "1" {
		err := run([]string{
			"serve", "--db", os.Getenv("MELDBASE_TEST_SERVE_DB"), "--addr", "127.0.0.1:0", "--dev-no-auth",
		}, os.Stdout, os.Stderr)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	command := exec.Command(os.Args[0], "-test.run=^TestServeWithoutWorkerControlStartsAndStopsCleanly$")
	command.Env = append(os.Environ(), childEnvironment+"=1", "MELDBASE_TEST_SERVE_DB="+filepath.Join(t.TempDir(), "data.meld2"))
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("serve exited before shutdown signal: %v\n%s", err, output.String())
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("serve without worker control: %v\n%s", err, output.String())
	}
}

func TestServeRollbackAnchorFlagsFailClosed(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{
		"serve", "--db", filepath.Join(t.TempDir(), "data.meld"), "--dev-no-auth", "--rollback-anchor-init",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "--rollback-anchor-init requires --rollback-anchor") {
		t.Fatalf("anchor initialization error=%v", err)
	}
	directory := t.TempDir()
	err = run([]string{
		"serve", "--db", filepath.Join(directory, "data.meld"), "--dev-no-auth",
		"--rollback-anchor", filepath.Join(directory, "data.anchor"), "--rollback-anchor-timeout", "0s",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "--rollback-anchor-timeout must be positive") {
		t.Fatalf("anchor timeout error=%v", err)
	}
	err = run([]string{
		"serve", "--db", filepath.Join(directory, "remote.meld"), "--dev-no-auth",
		"--rollback-anchor", filepath.Join(directory, "data.anchor"), "--rollback-anchor-cluster", "cluster-a",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("mixed local/remote anchor error=%v", err)
	}
	err = run([]string{
		"serve", "--db", filepath.Join(directory, "remote.meld"), "--dev-no-auth",
		"--rollback-anchor-cluster", "cluster-a",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "remote rollback anchor requires") {
		t.Fatalf("partial remote anchor error=%v", err)
	}
}

func TestDefaultRealtimeURL(t *testing.T) {
	if got := defaultRealtimeURL(":8080"); got != "ws://localhost:8080/v1/realtime" {
		t.Fatalf("url = %q", got)
	}
}

func TestDefaultBrowserOriginsIncludeEscapedIPv6RealtimePattern(t *testing.T) {
	if got := defaultHTTPOrigins(); !reflect.DeepEqual(got, []string{"http://localhost:5173", "http://127.0.0.1:5173", "http://[::1]:5173"}) {
		t.Fatalf("HTTP origins=%q", got)
	}
	if got := defaultRealtimeOriginPatterns(); !reflect.DeepEqual(got, []string{"localhost:*", "127.0.0.1:*", "[[]::1]:*"}) {
		t.Fatalf("realtime patterns=%q", got)
	}
	if got := configuredOriginList("https://app.example, https://admin.example", defaultHTTPOrigins()); !reflect.DeepEqual(got, []string{"https://app.example", "https://admin.example"}) {
		t.Fatalf("configured origins=%q", got)
	}
}

func TestAdminAddressMustBeExplicitlyLoopback(t *testing.T) {
	for _, address := range []string{"127.0.0.1:9091", "localhost:9091", "[::1]:9091"} {
		if !isLoopbackAddress(address) {
			t.Fatalf("rejected loopback address %q", address)
		}
	}
	for _, address := range []string{":9091", "0.0.0.0:9091", "example.com:9091", "bad"} {
		if isLoopbackAddress(address) {
			t.Fatalf("accepted non-loopback address %q", address)
		}
	}
}

func TestAdminDiagnosticsRequireAdminListener(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{
		"serve", "--db", filepath.Join(t.TempDir(), "data.meld"), "--dev-no-auth", "--admin-diagnostics",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "--admin-addr") {
		t.Fatalf("diagnostics without admin listener error=%v", err)
	}
}

func TestAdminMetricsRequireAdminListener(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{
		"serve", "--db", filepath.Join(t.TempDir(), "data.meld"), "--dev-no-auth", "--admin-metrics",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "--admin-addr") {
		t.Fatalf("metrics without admin listener error=%v", err)
	}
}

func TestWorkerControlRequiresLoopbackAndDedicatedToken(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{
		"serve", "--db", filepath.Join(t.TempDir(), "data.meld"), "--dev-no-auth", "--worker-addr", ":9092",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("non-loopback worker control error=%v", err)
	}
	t.Setenv("MELDBASE_WORKER_TOKEN", "")
	err = run([]string{
		"serve", "--db", filepath.Join(t.TempDir(), "data.meld"), "--dev-no-auth", "--worker-addr", "127.0.0.1:9092",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "MELDBASE_WORKER_TOKEN") {
		t.Fatalf("missing worker token error=%v", err)
	}
}

func TestWorkerPublicationsRequireControlListener(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{
		"serve", "--db", filepath.Join(t.TempDir(), "data.meld"), "--dev-no-auth", "--worker-publications", "orders",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "--worker-addr") {
		t.Fatalf("worker publications without listener error=%v", err)
	}
}
