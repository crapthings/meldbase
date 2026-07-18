package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDestructiveProcessCheckKillsWorkerAndProducesVerifiableTrial(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL and advisory-lock qualification is Unix-only")
	}
	directory := t.TempDir()
	executable := filepath.Join(directory, "meld-test")
	build := exec.Command("go", "build", "-o", executable, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build destructive executable: %v\n%s", err, output)
	}
	target := filepath.Join(directory, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	receiptPath := filepath.Join(directory, "process-receipt.json")
	command := exec.Command(executable, "destructive-process-check", "--dir", target, "--out", receiptPath)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("destructive process check: %v\nstderr=%s\nstdout=%s", err, stderr.String(), stdout.String())
	}
	var receipt destructiveProcessReceipt
	if err := json.Unmarshal(stdout.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.RequestedTrials != qualificationMinimumProcessTrials || receipt.CompletedTrials != qualificationMinimumProcessTrials ||
		len(receipt.Trials) != qualificationMinimumProcessTrials || len(receipt.TrialDirectories) != qualificationMinimumProcessTrials ||
		len(receipt.Verifications) != qualificationMinimumProcessTrials || !receipt.FinishedAt.After(receipt.StartedAt) {
		t.Fatalf("receipt counts=%+v", receipt)
	}
	seen := make(map[string]struct{}, len(receipt.Trials))
	for index, trial := range receipt.Trials {
		verification := receipt.Verifications[index]
		if _, exists := seen[trial.ID]; exists {
			t.Fatalf("duplicate trial id %q", trial.ID)
		}
		seen[trial.ID] = struct{}{}
		for _, name := range []string{"crash-image.meld", "oracle.jsonl"} {
			if _, err := os.Stat(filepath.Join(receipt.TrialDirectories[index], name)); err != nil {
				t.Fatalf("trial artifact %s: %v", name, err)
			}
		}
		wantTrigger := "oracle-prepared"
		if index%2 == 1 {
			wantTrigger = "oracle-committed"
		}
		if receipt.SchemaVersion != destructiveProcessReceiptSchema || receipt.GOOS != runtime.GOOS || receipt.Device == 0 ||
			receipt.FilesystemName == "" || receipt.BlockSize == 0 || receipt.TrialDirectories[index] == "" ||
			trial.Kind != qualificationTrialProcess || trial.PublicationBoundary != qualificationAsyncBoundary || trial.TriggerPoint != wantTrigger ||
			trial.NewCommitSequence != trial.OldCommitSequence+1 ||
			(trial.RecoveredSequence != trial.OldCommitSequence && trial.RecoveredSequence != trial.NewCommitSequence) ||
			!qualificationHexDigest(trial.OldStateSHA256) || !qualificationHexDigest(trial.NewStateSHA256) ||
			!qualificationHexDigest(trial.RecoveredStateSHA256) ||
			!trial.LockReacquired || !trial.OfflineVerified || !qualificationHexDigest(trial.DatabaseSHA256) ||
			!qualificationHexDigest(trial.ArtifactsSHA256) || verification.CommitSequence != trial.RecoveredSequence ||
			verification.SHA256 != trial.DatabaseSHA256 {
			t.Fatalf("receipt=%+v", receipt)
		}
	}
	if entries, err := os.ReadDir(target); err != nil || len(entries) != 0 {
		t.Fatalf("target leftovers=%v err=%v", entries, err)
	}
	stored, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	var storedReceipt destructiveProcessReceipt
	if err := json.Unmarshal(stored, &storedReceipt); err != nil || len(storedReceipt.Trials) != qualificationMinimumProcessTrials ||
		storedReceipt.Trials[0].ID != receipt.Trials[0].ID {
		t.Fatalf("stored receipt=%+v err=%v", storedReceipt, err)
	}
	second := exec.Command(executable, "destructive-process-check", "--dir", target, "--out", receiptPath)
	if output, err := second.CombinedOutput(); err == nil || !strings.Contains(string(output), "already exists") {
		t.Fatalf("second invocation err=%v output=%s", err, output)
	}
}

func TestDestructiveProcessCheckRejectsInsufficientTrialCount(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"destructive-process-check", "--dir", t.TempDir(), "--out", filepath.Join(t.TempDir(), "receipt.json"), "--trials", "19"}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "between 20 and 1000") {
		t.Fatalf("error=%v output=%s", err, output.String())
	}
}

func TestReadDestructiveLedgerIgnoresOnlyIncompleteTrailingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oracle.jsonl")
	raw := []byte("{\"phase\":\"prepared\",\"counter\":1}\n{\"phase\":\"committed\",\"counter\":1}\n{\"phase\":")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	confirmed, prepared, actual, err := readDestructiveLedger(path)
	if err != nil || confirmed != 1 || prepared != 1 || !bytes.Equal(actual, raw) {
		t.Fatalf("confirmed=%d prepared=%d raw=%q err=%v", confirmed, prepared, actual, err)
	}
	if err := os.WriteFile(path, []byte("{\"phase\":\"invalid\",\"counter\":1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := readDestructiveLedger(path); err == nil {
		t.Fatal("complete malformed oracle record accepted")
	}
}
