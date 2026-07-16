package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/crapthings/meldbase/internal/qualification"
)

func TestStorageSoakCommandPublishesExclusiveSchemaFourReceipt(t *testing.T) {
	target := t.TempDir()
	receiptPath := filepath.Join(t.TempDir(), "storage-soak.json")
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"storage-soak", "--dir", target, "--out", receiptPath, "--profile", "custom",
		"--seconds", "1", "--documents", "100", "--reopens", "1",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("storage soak=%v stderr=%s", err, stderr.String())
	}
	raw, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, stdout.Bytes()) {
		t.Fatalf("receipt and stdout differ\nreceipt=%s\nstdout=%s", raw, stdout.Bytes())
	}
	for _, stage := range []string{
		"stage=started", "stage=phase_running", "stage=phase_verifying", "stage=phase_verified",
		"stage=shadow_verifying", "stage=shadow_verified", "stage=final_verifying", "stage=complete",
	} {
		if !strings.Contains(stderr.String(), stage) {
			t.Fatalf("missing %q in progress log: %s", stage, stderr.String())
		}
	}
	if strings.Contains(stderr.String(), target) || strings.Contains(stderr.String(), receiptPath) {
		t.Fatalf("progress log exposed a path: %s", stderr.String())
	}
	var receipt qualification.SoakReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil || receipt.SchemaVersion != 4 || receipt.Profile != "custom" ||
		receipt.CompletedReopens != 1 || receipt.Device == 0 || receipt.FilesystemType == "" || receipt.FilesystemName == "" ||
		!receipt.SemanticIndexes || !receipt.SemanticIndexBuilds || !receipt.FinalIndexBuildAbsent {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	info, err := os.Stat(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("receipt mode=%v", info.Mode())
	}
	leftovers, err := filepath.Glob(filepath.Join(target, ".meldbase-storage-soak-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("leftovers=%v err=%v", leftovers, err)
	}

	stdout.Reset()
	stderr.Reset()
	err = run([]string{
		"storage-soak", "--dir", target, "--out", receiptPath, "--profile", "custom",
		"--seconds", "1", "--documents", "100", "--reopens", "1",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "already exists") || stdout.Len() != 0 {
		t.Fatalf("overwrite error=%v stdout=%s", err, stdout.String())
	}
}

func TestStorageSoakReleaseRequiresCleanStampedRaceBinaryBeforeWork(t *testing.T) {
	target := t.TempDir()
	receiptPath := filepath.Join(t.TempDir(), "release.json")
	var output bytes.Buffer
	err := run([]string{
		"storage-soak", "--dir", target, "--out", receiptPath, "--profile", "release",
		"--seconds", "14400", "--documents", "10000", "--reopens", "12",
		"--source-revision", qualificationTestRevision, "--require-clean-source",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "clean source verification failed") {
		t.Fatalf("error=%v output=%s", err, output.String())
	}
	if _, err := os.Stat(receiptPath); !os.IsNotExist(err) {
		t.Fatalf("release receipt unexpectedly exists: %v", err)
	}
}
