package main

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestDestructiveCapacityControllerCoversEveryPublicationBoundary(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "meld-test")
	build := exec.Command("go", "build", "-o", executable, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build destructive executable: %v\n%s", err, output)
	}
	target := filepath.Join(directory, "target")
	artifacts := filepath.Join(directory, "artifacts")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(artifacts, 0o700); err != nil {
		t.Fatal(err)
	}
	originalFill, originalAvailable := fillDestructiveVolumeFn, destructiveAvailableBytesFn
	fillDestructiveVolumeFn = func(directory string, blockSize uint64) (destructiveFillResult, error) {
		path := filepath.Join(directory, ".simulated-controller-fill")
		if err := os.WriteFile(path, []byte("simulation-only"), 0o600); err != nil {
			return destructiveFillResult{}, err
		}
		return destructiveFillResult{
			Path: path, AllocatedBytes: 64 << 20, AvailableBytesBefore: 128 << 20,
			AvailableBytesAtENOSPC: 0, ENOSPCOperation: "simulated-test-only",
		}, nil
	}
	destructiveAvailableBytesFn = func(string) (uint64, error) { return 128 << 20, nil }
	defer func() {
		fillDestructiveVolumeFn, destructiveAvailableBytesFn = originalFill, originalAvailable
	}()
	facts := destructiveVolumeFacts{directory: target, blockSize: 4096}
	var trials []qualificationDestructiveTrial
	var evidenceSet []destructiveCapacityTrialEvidence
	ordinal := 0
	started := time.Now().UTC()
	for _, boundary := range qualificationPublicationBoundaries {
		for range qualificationMinimumBoundaryTrials {
			ordinal++
			trial, evidence, err := runDestructiveCapacityTrial(facts, executable, artifacts, boundary, ordinal, io.Discard)
			if err != nil {
				t.Fatalf("boundary %s: %v", boundary, err)
			}
			if trial.Kind != qualificationTrialCapacity || trial.PublicationBoundary != boundary ||
				trial.TriggerPoint != "real-enospc-at-boundary" || trial.NewCommitSequence != trial.OldCommitSequence+1 ||
				(trial.RecoveredSequence != trial.OldCommitSequence && trial.RecoveredSequence != trial.NewCommitSequence) ||
				!trial.LockReacquired || !trial.OfflineVerified || !qualificationHexDigest(trial.DatabaseSHA256) ||
				!qualificationHexDigest(trial.ArtifactsSHA256) || evidence.Boundary != boundary ||
				evidence.ENOSPCOperation != "simulated-test-only" || evidence.DatabaseArtifact == "" || evidence.MarkerArtifact == "" {
				t.Fatalf("trial=%+v evidence=%+v", trial, evidence)
			}
			if _, err := os.Stat(evidence.DatabaseArtifact); err != nil {
				t.Fatal(err)
			}
			trials = append(trials, trial)
			evidenceSet = append(evidenceSet, evidence)
			entries, err := os.ReadDir(target)
			if err != nil || len(entries) != 0 {
				t.Fatalf("target leftovers=%v err=%v", entries, err)
			}
		}
	}
	receipt := destructiveENOSPCReceipt{
		SchemaVersion: destructiveENOSPCReceiptSchema, GOOS: "linux", Device: 42, BlockSize: 4096,
		StartedAt: started, FinishedAt: time.Now().UTC(), TrialsPerBoundary: qualificationMinimumBoundaryTrials,
		Trials: trials, CapacityEvidence: evidenceSet,
	}
	if err := validateDestructiveENOSPCReceipt(receipt); err == nil {
		t.Fatal("simulated ENOSPC controller evidence was accepted as a real receipt")
	}
}

func TestDestructiveQualificationBoundaryMappingFailsClosed(t *testing.T) {
	if _, ok := destructiveQualificationBoundary("unknown"); ok {
		t.Fatal("unknown publication boundary accepted")
	}
	for _, boundary := range qualificationPublicationBoundaries {
		if _, ok := destructiveQualificationBoundary(boundary); !ok {
			t.Fatalf("known boundary %q rejected", boundary)
		}
	}
}
