package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestQualificationSessionPowerPrepareDerivesImmutableNextStep(t *testing.T) {
	root, planPath := qualificationSessionAtFirstPowerStep(t)
	oldPrepare := qualificationSessionPowerPrepareFn
	t.Cleanup(func() { qualificationSessionPowerPrepareFn = oldPrepare })
	var got []string
	qualificationSessionPowerPrepareFn = func(args []string, _ io.Writer) error {
		got = append([]string(nil), args...)
		return nil
	}
	if err := runQualificationSessionPowerPrepare([]string{"--plan", planPath}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	plan := readQualificationSessionTestPlan(t, planPath)
	want := []string{
		"--dir", plan.Volume.Directory, "--control-dir", root,
		"--marker", filepath.Join(root, "power-01-01-marker.json"),
		"--trial-id", "power-01-01", "--boundary", "after-page-write",
		"--destructive-token", plan.Volume.DestructiveToken,
		"--source-revision", qualificationTestRevision, "--require-clean-source",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("prepare args=%q want=%q", got, want)
	}
	if err := os.WriteFile(filepath.Join(root, "power-01-01-controller.json"), []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runQualificationSessionPowerPrepare([]string{"--plan", planPath}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing output error=%v", err)
	}
}

func TestQualificationSessionPowerStatusReportsOnlyValidNextPhase(t *testing.T) {
	root, planPath := qualificationSessionAtFirstPowerStep(t)
	var output bytes.Buffer
	if err := runQualificationSessionPowerStatus([]string{"--plan", planPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var status qualificationSessionPowerStatus
	if err := json.Unmarshal(output.Bytes(), &status); err != nil || status.Phase != "prepare" || status.Step.PowerTrialID != "power-01-01" {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	marker, _, _, _, _, _ := physicalControllerEvidenceFixture(t)
	if err := writeJSONExclusiveDurable(filepath.Join(root, "power-01-01-marker.json"), marker); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := runQualificationSessionPowerStatus([]string{"--plan", planPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &status); err != nil || status.Phase != "controller" {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	if err := os.WriteFile(filepath.Join(root, "power-01-01-controller-proof.json"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runQualificationSessionPowerStatus([]string{"--plan", planPath}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "partial") {
		t.Fatalf("partial controller publication error=%v", err)
	}
}

func TestQualificationSessionPowerRecoverPreflightsPlanAndRecords(t *testing.T) {
	root, planPath := qualificationSessionAtFirstPowerStep(t)
	privateKey := qualificationPhysicalControllerTestPrivateKey()
	publicPath := filepath.Join(root, "controller.pub")
	if err := writeAnchorQualificationKey(publicPath, privateKey.Public().(ed25519.PublicKey), 0o600); err != nil {
		t.Fatal(err)
	}
	proofPath := filepath.Join(root, "power-01-01-controller-proof.json")
	proof := destructivePowerControllerProof{Response: destructivePowerAdapterResponse{TargetIdentitySHA256: strings.Repeat("71", 32)}}
	if err := writeJSONExclusiveDurable(proofPath, proof); err != nil {
		t.Fatal(err)
	}

	oldRecover, oldRecord := qualificationSessionPowerRecoverFn, qualificationSessionPowerRecordFn
	t.Cleanup(func() {
		qualificationSessionPowerRecoverFn, qualificationSessionPowerRecordFn = oldRecover, oldRecord
	})
	var recoverArgs, recordArgs []string
	qualificationSessionPowerRecoverFn = func(args []string, _ io.Writer, _ io.Writer) error {
		recoverArgs = append([]string(nil), args...)
		for index := range args {
			if args[index] == "--out" && index+1 < len(args) {
				return os.WriteFile(args[index+1], []byte("receipt"), 0o600)
			}
		}
		return nil
	}
	qualificationSessionPowerRecordFn = func(args []string, _ io.Writer, _ io.Writer) error {
		recordArgs = append([]string(nil), args...)
		return nil
	}
	if err := runQualificationSessionPowerRecover([]string{"--plan", planPath, "--controller-public-key", publicPath}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(recoverArgs, filepath.Join(root, "power-01-01-recovery.json")) || !slices.Equal(recordArgs, []string{"--plan", planPath, "--kind", "power", "--receipt", filepath.Join(root, "power-01-01-recovery.json")}) {
		t.Fatalf("recover=%q record=%q", recoverArgs, recordArgs)
	}

	wrongKeyPath := filepath.Join(root, "wrong-controller.pub")
	wrongPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeAnchorQualificationKey(wrongKeyPath, wrongPublic, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runQualificationSessionPowerRecover([]string{"--plan", planPath, "--controller-public-key", wrongKeyPath}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "immutable session plan") {
		t.Fatalf("wrong key error=%v", err)
	}
}

func qualificationSessionAtFirstPowerStep(t *testing.T) (string, string) {
	t.Helper()
	root, planPath := newQualificationSessionTestFixture(t)
	plan := readQualificationSessionTestPlan(t, planPath)
	for _, step := range plan.Steps[:5] {
		receipt := filepath.Join(root, step.ID+".json")
		if err := os.WriteFile(receipt, []byte(`{"step":"`+step.ID+`"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := runQualificationSessionRecord([]string{"--plan", planPath, "--kind", step.Kind, "--receipt", receipt}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
	}
	return root, planPath
}
