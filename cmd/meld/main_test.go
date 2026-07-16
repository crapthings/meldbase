package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/crapthings/meldbase"
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
	db, err := meldbase.OpenV2(path)
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
	if info.Format != meldbase.StorageFormatV2 || info.Revision != 3 || !info.ReaderCompatible || info.DatabaseIDHex == "" {
		t.Fatalf("inspect info=%+v", info)
	}
}

func TestIndexBuildCommandStartListResumeLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index-build.meld2")
	db, err := meldbase.OpenV2(path)
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

	published, err := meldbase.OpenV2(path)
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
	db, err := meldbase.OpenV2(path)
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

func TestIndexBuildCommandRejectsUnsafeArgumentsAndNonV2(t *testing.T) {
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
	v1Path := filepath.Join(t.TempDir(), "legacy.meld")
	v1, err := meldbase.OpenV1(v1Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"index-build", "list", "--db", v1Path}, &output, &output); !errors.Is(err, meldbase.ErrIndexBuildUnsupported) {
		t.Fatalf("V1 list=%v", err)
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
	db, err := meldbase.OpenV2(path)
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
	var report meldbase.V2VerificationReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if !report.Verified || report.SchemaVersion != 3 || !report.IndexContentsVerified || !report.IndexBuildContentsVerified || report.Format != meldbase.StorageFormatV2 ||
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
	db, err := meldbase.OpenV2(path)
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
	db, err := meldbase.OpenV2(source)
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
	if result.SchemaVersion != 1 || result.ArtifactKind != physicalV2RestoreArtifact || result.Bytes == 0 ||
		result.Pages == 0 || result.CommitSequence != sequence || result.DatabaseIDHex == "" || result.SHA256 == "" {
		t.Fatalf("backup result=%+v", result)
	}
	backup, err := meldbase.OpenV2(destination)
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

func TestBackupCommandFailsClosedForExistingDestinationAndNonV2Source(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.meld2")
	destination := filepath.Join(directory, "owner")
	db, err := meldbase.OpenV2(source)
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

	v1Path := filepath.Join(directory, "legacy.meld")
	v1, err := meldbase.OpenV1(v1Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"backup", "--db", v1Path, "--out", filepath.Join(directory, "invalid")}, &output, &output); !errors.Is(err, meldbase.ErrBackupUnsupported) {
		t.Fatalf("V1 backup error=%v", err)
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

func TestDefaultRealtimeURL(t *testing.T) {
	if got := defaultRealtimeURL(":8080"); got != "ws://localhost:8080/v1/realtime" {
		t.Fatalf("url = %q", got)
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
