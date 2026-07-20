package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestValidateDestructivePowerInputsRequiresNewBootAndBoundHardReset(t *testing.T) {
	buildRevision, buildModified := durabilityBuildIdentity()
	started := time.Now().UTC().Add(-2 * time.Minute)
	facts := destructiveVolumeFacts{
		device: 42, filesystemType: "0xef53", filesystemName: "ext-family", blockSize: 4096,
	}
	marker := destructivePowerMarker{
		SchemaVersion: destructivePowerMarkerSchema, TrialID: "power-trial-001", Boundary: "after-data-sync",
		BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		Device: facts.device, FilesystemType: facts.filesystemType, FilesystemName: facts.filesystemName, BlockSize: facts.blockSize,
		DatabaseRelative: filepath.Join(".meldbase-power-power-trial-001", "power.meld"),
		BootIDBefore:     "11111111-1111-1111-1111-111111111111", StartedAt: started, ReachedAt: started.Add(time.Minute),
		OldCommitSequence: 1, NewCommitSequence: 2, OldStateSHA256: strings.Repeat("10", 32), NewStateSHA256: strings.Repeat("20", 32),
	}
	markerRaw, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	event := destructivePowerControllerEvent{
		SchemaVersion: destructivePowerEventSchema, TrialID: marker.TrialID, Method: "qemu-system-reset",
		MarkerSHA256: qualificationSHA256(markerRaw), BootIDBefore: marker.BootIDBefore,
		MarkerObservedAt: marker.ReachedAt.Add(time.Second), CutRequestedAt: marker.ReachedAt.Add(2 * time.Second),
		PowerRestoredAt:       marker.ReachedAt.Add(30 * time.Second),
		ControllerProofSHA256: strings.Repeat("30", 32),
	}
	newBoot := "22222222-2222-2222-2222-222222222222"
	if err := validateDestructivePowerInputs(marker, markerRaw, event, facts, newBoot); err != nil {
		t.Fatal(err)
	}
	if err := validateDestructivePowerInputs(marker, markerRaw, event, facts, marker.BootIDBefore); err == nil || !strings.Contains(err.Error(), "boot") {
		t.Fatalf("same boot error=%v", err)
	}
	event.Method = "graceful-reboot"
	if err := validateDestructivePowerInputs(marker, markerRaw, event, facts, newBoot); err == nil || !strings.Contains(err.Error(), "controller event") {
		t.Fatalf("graceful event error=%v", err)
	}
	event.Method = "qemu-system-reset"
	event.MarkerSHA256 = strings.Repeat("ff", 32)
	if err := validateDestructivePowerInputs(marker, markerRaw, event, facts, newBoot); err == nil {
		t.Fatal("controller event bound to a different marker was accepted")
	}
}

func TestValidatePowerTargetEntriesAllowsOnlyExpectedTrial(t *testing.T) {
	target := t.TempDir()
	if err := os.Mkdir(filepath.Join(target, "lost+found"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(target, ".meldbase-power-trial"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := validatePowerTargetEntries(target, ".meldbase-power-trial"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "unrelated"), []byte("do not delete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validatePowerTargetEntries(target, ".meldbase-power-trial"); err == nil || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("error=%v", err)
	}
}

func TestPowerDatabaseRelativeFailsClosed(t *testing.T) {
	if !validPowerDatabaseRelative(filepath.Join(".meldbase-power-safe", "power.meld"), "safe") {
		t.Fatal("valid power database path rejected")
	}
	for _, path := range []string{"/absolute/power.meld", "../escape/power.meld", filepath.Join(".meldbase-power-other", "power.meld")} {
		if validPowerDatabaseRelative(path, "safe") {
			t.Fatalf("unsafe path %q accepted", path)
		}
	}
}

func TestDestructivePowerRecoveryPathsFailBeforeRecoveryMutation(t *testing.T) {
	control := t.TempDir()
	paths := []string{filepath.Join(control, "marker.json"), filepath.Join(control, "controller.json"), filepath.Join(control, "proof.json")}
	for _, path := range paths {
		if err := os.WriteFile(path, []byte("evidence"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(control, "recovery.json")
	marker, controller, proof, gotOutput, err := validateDestructivePowerRecoveryPaths(control, paths[0], paths[1], paths[2], output)
	if err != nil || marker != paths[0] || controller != paths[1] || proof != paths[2] || gotOutput != output {
		t.Fatalf("valid paths: %q %q %q %q err=%v", marker, controller, proof, gotOutput, err)
	}
	if _, _, _, _, err := validateDestructivePowerRecoveryPaths(control, paths[0], paths[1], paths[2], paths[0]); err == nil {
		t.Fatal("existing/output alias was accepted")
	}
	if _, _, _, _, err := validateDestructivePowerRecoveryPaths(control, paths[0], paths[0], paths[2], output); err == nil {
		t.Fatal("duplicate evidence input was accepted")
	}
	symlink := filepath.Join(control, "proof-link.json")
	if err := os.Symlink(paths[2], symlink); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := validateDestructivePowerRecoveryPaths(control, paths[0], paths[1], symlink, output); err == nil {
		t.Fatal("symlink proof was accepted")
	}
	if _, _, _, _, err := validateDestructivePowerRecoveryPaths(control, paths[0], paths[1], paths[2], filepath.Join(t.TempDir(), "recovery.json")); err == nil {
		t.Fatal("output outside control directory was accepted")
	}
}
