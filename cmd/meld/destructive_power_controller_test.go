package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func physicalControllerEvidenceFixture(t *testing.T) (destructivePowerMarker, []byte, destructivePowerControllerProof, []byte, destructivePowerControllerEvent, ed25519.PublicKey) {
	t.Helper()
	marker := destructivePowerMarker{SchemaVersion: 1, SourceRevision: qualificationTestRevision, BuildRevision: qualificationTestRevision, GOOS: "linux", GOARCH: "amd64", TrialID: "power-01-01", Boundary: "after-page-write", DatabaseRelative: ".meldbase-power-power-01-01/power.meld", BootIDBefore: "boot-before", StartedAt: time.Now().UTC().Add(-time.Minute), ReachedAt: time.Now().UTC().Add(-50 * time.Second), OldCommitSequence: 1, NewCommitSequence: 2, OldStateSHA256: strings.Repeat("1", 64), NewStateSHA256: strings.Repeat("2", 64)}
	markerRaw, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	request := destructivePowerAdapterRequest{SchemaVersion: 1, ControllerRunID: strings.Repeat("a", 32), TrialID: marker.TrialID, Method: "ipmi-chassis-power-cycle", MarkerSHA256: qualificationSHA256(markerRaw), BootIDBefore: marker.BootIDBefore, TargetIdentitySHA256: strings.Repeat("3", 64), RequestedAt: marker.ReachedAt.Add(time.Second)}
	hardwareEvidence := []byte("retained hardware evidence")
	response := destructivePowerAdapterResponse{SchemaVersion: 1, ControllerRunID: request.ControllerRunID, TrialID: request.TrialID, Method: request.Method, OperationID: "ipmi-operation-1", TargetIdentitySHA256: strings.Repeat("3", 64), AcceptedAt: request.RequestedAt.Add(time.Second), PowerLostAt: request.RequestedAt.Add(2 * time.Second), PowerRestoredAt: request.RequestedAt.Add(3 * time.Second), HardwareEvidenceSHA256: qualificationSHA256(hardwareEvidence), HardwareEvidenceBase64: base64.StdEncoding.EncodeToString(hardwareEvidence), Success: true}
	proof := destructivePowerControllerProof{SchemaVersion: 1, SourceRevision: qualificationTestRevision, BuildRevision: qualificationTestRevision, GOOS: "linux", GOARCH: "amd64", GoVersion: "go-test", AdapterSHA256: strings.Repeat("5", 64), AdapterStderrSHA256: qualificationSHA256(nil), StartedAt: request.RequestedAt, FinishedAt: request.RequestedAt.Add(4 * time.Second), Request: request, Response: response}
	proofRaw, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	event := destructivePowerControllerEvent{SchemaVersion: 2, TrialID: marker.TrialID, Method: request.Method, MarkerSHA256: request.MarkerSHA256, BootIDBefore: marker.BootIDBefore, MarkerObservedAt: request.RequestedAt, CutRequestedAt: response.AcceptedAt, PowerRestoredAt: response.PowerRestoredAt, ControllerProofSHA256: qualificationSHA256(proofRaw), ControllerRunID: request.ControllerRunID, ControllerPublicKey: base64.StdEncoding.EncodeToString(publicKey)}
	if err := signDestructivePowerControllerEvent(&event, privateKey); err != nil {
		t.Fatal(err)
	}
	return marker, markerRaw, proof, proofRaw, event, publicKey
}

func TestPhysicalPowerControllerPathsFailBeforeHardwareAction(t *testing.T) {
	directory := t.TempDir()
	marker := filepath.Join(directory, "marker.json")
	if err := os.WriteFile(marker, []byte("marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	proof := filepath.Join(directory, "proof.json")
	event := filepath.Join(directory, "event.json")
	gotMarker, gotProof, gotEvent, err := validateDestructivePowerControllerPaths(marker, proof, event)
	if err != nil || gotMarker != marker || gotProof != proof || gotEvent != event {
		t.Fatalf("valid paths: marker=%q proof=%q event=%q err=%v", gotMarker, gotProof, gotEvent, err)
	}
	if _, _, _, err := validateDestructivePowerControllerPaths(marker, proof, proof); err == nil {
		t.Fatal("shared proof and event path was accepted")
	}
	if _, _, _, err := validateDestructivePowerControllerPaths(marker, proof, filepath.Join(t.TempDir(), "event.json")); err == nil {
		t.Fatal("event outside the marker control directory was accepted")
	}
	if err := os.WriteFile(proof, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := validateDestructivePowerControllerPaths(marker, proof, event); err == nil {
		t.Fatal("pre-existing proof path was accepted")
	}
	symlink := filepath.Join(directory, "marker-link.json")
	if err := os.Symlink(marker, symlink); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := validateDestructivePowerControllerPaths(symlink, filepath.Join(directory, "new-proof.json"), event); err == nil {
		t.Fatal("symlink marker was accepted")
	}
}

func TestPhysicalPowerAdapterMeasurementUsesOpenedExecutable(t *testing.T) {
	directory := t.TempDir()
	adapter := filepath.Join(directory, "adapter")
	original := []byte("#!/bin/sh\nexit 0\n")
	if err := os.WriteFile(adapter, original, 0o700); err != nil {
		t.Fatal(err)
	}
	file, digest, err := openDestructivePowerAdapter(adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if digest != qualificationSHA256(original) {
		t.Fatalf("adapter digest=%q", digest)
	}
	if err := os.Rename(adapter, adapter+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\nexit 99\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	opened, err := io.ReadAll(file)
	if err != nil || !bytes.Equal(opened, original) {
		t.Fatalf("opened adapter changed: %q err=%v", opened, err)
	}
	symlink := filepath.Join(directory, "adapter-link")
	if err := os.Symlink(adapter, symlink); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openDestructivePowerAdapter(symlink); err == nil {
		t.Fatal("symlink adapter was accepted")
	}
}

func TestPhysicalPowerControllerRunExecutesMeasuredDescriptorOnLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux /proc file-descriptor execution contract")
	}
	directory := t.TempDir()
	adapter := os.Getenv("MELDBASE_TEST_POWER_ADAPTER")
	if adapter == "" {
		adapter = filepath.Join(directory, "adapter")
		command := exec.Command("go", "build", "-o", adapter, "./testdata/poweradapter")
		if raw, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build adapter: %v\n%s", err, raw)
		}
	}
	marker, _, _, _, _, _ := physicalControllerEvidenceFixture(t)
	markerPath := filepath.Join(directory, "marker.json")
	if err := writeJSONExclusiveDurable(markerPath, marker); err != nil {
		t.Fatal(err)
	}
	privatePath := filepath.Join(directory, "controller.key")
	if err := writeAnchorQualificationKey(privatePath, qualificationPhysicalControllerTestPrivateKey(), 0o600); err != nil {
		t.Fatal(err)
	}
	oldIdentity := destructivePowerControllerBuildIdentity
	destructivePowerControllerBuildIdentity = func() (string, bool) { return qualificationTestRevision, false }
	t.Cleanup(func() { destructivePowerControllerBuildIdentity = oldIdentity })
	proofPath, eventPath := filepath.Join(directory, "proof.json"), filepath.Join(directory, "event.json")
	if err := runDestructivePowerControllerRun([]string{
		"--marker", markerPath, "--method", "ipmi-chassis-power-cycle",
		"--target-identity-sha256", strings.Repeat("3", 64), "--adapter", adapter,
		"--signing-key", privatePath, "--proof", proofPath, "--out", eventPath,
		"--source-revision", qualificationTestRevision, "--timeout", "30s",
	}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var proof destructivePowerControllerProof
	if _, err := readQualificationReceipt(proofPath, &proof); err != nil || proof.AdapterSHA256 == "" {
		t.Fatalf("proof=%+v err=%v", proof, err)
	}
	var event destructivePowerControllerEvent
	if _, err := readQualificationReceipt(eventPath, &event); err != nil || event.ControllerProofSHA256 == "" || event.Signature == "" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
}

func TestPhysicalPowerControllerEvidenceRejectsForgeryAndDowngrade(t *testing.T) {
	marker, markerRaw, proof, proofRaw, event, publicKey := physicalControllerEvidenceFixture(t)
	if err := verifyDestructivePhysicalPowerEvidence(proofRaw, event, markerRaw, publicKey); err != nil {
		t.Fatalf("valid evidence: %v", err)
	}

	changed := event
	changed.PowerRestoredAt = changed.PowerRestoredAt.Add(time.Second)
	if err := verifyDestructivePhysicalPowerEvidence(proofRaw, changed, markerRaw, publicKey); err == nil {
		t.Fatal("modified signed event was accepted")
	}

	wrongPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyDestructivePhysicalPowerEvidence(proofRaw, event, markerRaw, wrongPublic); err == nil {
		t.Fatal("different trusted controller key was accepted")
	}

	proof.Response.ControllerRunID = strings.Repeat("b", 32)
	if err := validateDestructivePowerControllerProof(proof, marker, markerRaw); err == nil {
		t.Fatal("adapter response from another controller run was accepted")
	}

	legacy := event
	legacy.SchemaVersion = destructivePowerEventSchema
	legacy.ControllerRunID, legacy.ControllerPublicKey, legacy.Signature = "", "", ""
	if err := verifyDestructivePhysicalPowerEvidence(proofRaw, legacy, markerRaw, publicKey); err == nil {
		t.Fatal("legacy unsigned physical event was accepted")
	}
}

func TestPhysicalPowerAdapterExchangeRejectsUnsafeResponses(t *testing.T) {
	_, _, proof, _, _, _ := physicalControllerEvidenceFixture(t)
	request, response := proof.Request, proof.Response
	if err := validateDestructivePowerAdapterExchange(request, response); err != nil {
		t.Fatal(err)
	}
	response.Success = false
	if err := validateDestructivePowerAdapterExchange(request, response); err == nil {
		t.Fatal("unsuccessful adapter response was accepted")
	}
	response = proof.Response
	response.PowerLostAt = response.AcceptedAt.Add(-time.Nanosecond)
	if err := validateDestructivePowerAdapterExchange(request, response); err == nil {
		t.Fatal("power loss before adapter acceptance was accepted")
	}
	response = proof.Response
	response.TargetIdentitySHA256 = strings.Repeat("6", 64)
	if err := validateDestructivePowerAdapterExchange(request, response); err == nil {
		t.Fatal("response for a different physical target was accepted")
	}
	response = proof.Response
	response.HardwareEvidenceSHA256 = ""
	if err := validateDestructivePowerAdapterExchange(request, response); err == nil {
		t.Fatal("response without hardware evidence was accepted")
	}
	response = proof.Response
	response.HardwareEvidenceBase64 = base64.StdEncoding.EncodeToString([]byte("substituted evidence"))
	if err := validateDestructivePowerAdapterExchange(request, response); err == nil {
		t.Fatal("hardware evidence content with a different digest was accepted")
	}
}

func TestPhysicalPowerControllerRejectsStaleRevisionMarker(t *testing.T) {
	marker, _, _, _, _, _ := physicalControllerEvidenceFixture(t)
	if err := validateDestructivePowerMarkerForController(marker, qualificationTestRevision); err != nil {
		t.Fatal(err)
	}
	marker.SourceRevision = strings.Repeat("f", 40)
	if err := validateDestructivePowerMarkerForController(marker, qualificationTestRevision); err == nil {
		t.Fatal("marker from another source revision was accepted")
	}
	marker.SourceRevision = qualificationTestRevision
	marker.BuildModified = true
	if err := validateDestructivePowerMarkerForController(marker, qualificationTestRevision); err == nil {
		t.Fatal("marker from a dirty build was accepted")
	}
}

func TestRedfishPowerCycleIsAFirstClassPhysicalMethod(t *testing.T) {
	if !qualificationPhysicalPowerMethod("redfish-computer-system-power-cycle") || !qualificationPowerMethod("redfish-computer-system-power-cycle") {
		t.Fatal("Redfish ComputerSystem power cycle is not recognized across physical power validation")
	}
}
