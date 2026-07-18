package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQualificationSessionOrdersRecordsAndSealsFixedCampaign(t *testing.T) {
	root, planPath := newQualificationSessionTestFixture(t)
	plan := readQualificationSessionTestPlan(t, planPath)

	var status bytes.Buffer
	if err := runQualificationSessionStatus([]string{"--plan", planPath}, &status, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var initial qualificationSessionStatus
	if err := json.Unmarshal(status.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	if initial.Completed != 0 || initial.Total != 20 || initial.Next == nil || initial.Next.Kind != "durability" || initial.ReadyToSeal {
		t.Fatalf("unexpected initial status: %+v", initial)
	}
	if err := runQualificationSessionSeal([]string{"--plan", planPath, "--out", filepath.Join(t.TempDir(), "early.json")}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("incomplete seal error=%v", err)
	}

	receiptsDirectory := filepath.Join(root, "receipts")
	if err := os.Mkdir(receiptsDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, step := range plan.Steps {
		receiptPath := filepath.Join(receiptsDirectory, step.ID+".json")
		if err := os.WriteFile(receiptPath, []byte(`{"step":"`+step.ID+`"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := runQualificationSessionRecord([]string{"--plan", planPath, "--kind", step.Kind, "--receipt", receiptPath}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
			t.Fatalf("record %s: %v", step.ID, err)
		}
	}

	status.Reset()
	if err := runQualificationSessionStatus([]string{"--plan", planPath}, &status, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var complete qualificationSessionStatus
	if err := json.Unmarshal(status.Bytes(), &complete); err != nil {
		t.Fatal(err)
	}
	if complete.Completed != 20 || !complete.ReadyToSeal || complete.Next != nil {
		t.Fatalf("unexpected complete status: %+v", complete)
	}

	indexPath := filepath.Join(t.TempDir(), "artifacts-index.json")
	var sealed bytes.Buffer
	if err := runQualificationSessionSeal([]string{"--plan", planPath, "--out", indexPath}, &sealed, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyQualificationArtifactIndex(root, indexPath, qualificationTestRevision); err != nil {
		t.Fatalf("sealed index rejected: %v", err)
	}
	var result struct {
		IndexSHA256 string `json:"indexSha256"`
		Sealed      bool   `json:"sealed"`
	}
	if err := json.Unmarshal(sealed.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	indexSHA256, err := hashRegularFile(indexPath, qualificationReceiptMaxBytes)
	if err != nil || !result.Sealed || result.IndexSHA256 != indexSHA256 {
		t.Fatalf("seal result=%+v digest=%q err=%v", result, indexSHA256, err)
	}
}

func TestQualificationSessionRejectsOrderDuplicateAndJournalReceipt(t *testing.T) {
	root, planPath := newQualificationSessionTestFixture(t)
	plan := readQualificationSessionTestPlan(t, planPath)
	receipt := filepath.Join(root, "first.json")
	if err := os.WriteFile(receipt, []byte(`{"step":"first"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runQualificationSessionRecord([]string{"--plan", planPath, "--kind", "power", "--receipt", receipt}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "next step") {
		t.Fatalf("out-of-order error=%v", err)
	}
	if err := runQualificationSessionRecord([]string{"--plan", planPath, "--kind", plan.Steps[0].Kind, "--receipt", receipt}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := runQualificationSessionRecord([]string{"--plan", planPath, "--kind", plan.Steps[1].Kind, "--receipt", receipt}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "repeats") {
		t.Fatalf("duplicate receipt error=%v", err)
	}
	if err := runQualificationSessionRecord([]string{"--plan", planPath, "--kind", plan.Steps[1].Kind, "--receipt", planPath}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "journal directory") {
		t.Fatalf("journal receipt error=%v", err)
	}
}

func TestQualificationSessionDetectsReceiptEventAndPlanTampering(t *testing.T) {
	t.Run("operator uid", func(t *testing.T) {
		_, planPath := newQualificationSessionTestFixture(t)
		qualificationSessionEffectiveUID = func() int { return 1001 }
		if err := runQualificationSessionStatus([]string{"--plan", planPath}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "command user") {
			t.Fatalf("operator drift error=%v", err)
		}
	})

	t.Run("retained executable", func(t *testing.T) {
		root, planPath := newQualificationSessionTestFixture(t)
		plan := readQualificationSessionTestPlan(t, planPath)
		executablePath := filepath.Join(root, filepath.FromSlash(plan.ExecutableRelativePath))
		if err := os.WriteFile(executablePath, []byte("replacement binary"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := runQualificationSessionStatus([]string{"--plan", planPath}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "retained executable") {
			t.Fatalf("executable tamper error=%v", err)
		}
	})

	t.Run("receipt", func(t *testing.T) {
		root, planPath := newQualificationSessionTestFixture(t)
		receipt := recordQualificationSessionTestFirst(t, root, planPath)
		if err := os.WriteFile(receipt, []byte(`{"step":"replaced"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := runQualificationSessionStatus([]string{"--plan", planPath}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "differs from its event") {
			t.Fatalf("receipt tamper error=%v", err)
		}
	})

	t.Run("event", func(t *testing.T) {
		root, planPath := newQualificationSessionTestFixture(t)
		recordQualificationSessionTestFirst(t, root, planPath)
		plan := readQualificationSessionTestPlan(t, planPath)
		eventPath := filepath.Join(root, qualificationSessionDirectory, "events", qualificationSessionEventFilename(plan.Steps[0]))
		var event qualificationSessionEvent
		if _, err := readQualificationReceipt(eventPath, &event); err != nil {
			t.Fatal(err)
		}
		event.Kind = "power"
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(eventPath, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := runQualificationSessionStatus([]string{"--plan", planPath}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "event 1 is invalid") {
			t.Fatalf("event tamper error=%v", err)
		}
	})

	t.Run("plan", func(t *testing.T) {
		root, planPath := newQualificationSessionTestFixture(t)
		recordQualificationSessionTestFirst(t, root, planPath)
		plan := readQualificationSessionTestPlan(t, planPath)
		plan.PlatformClass = "linux-ext4-sata"
		raw, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(planPath, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := runQualificationSessionStatus([]string{"--plan", planPath}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "event 1 is invalid") {
			t.Fatalf("plan tamper error=%v", err)
		}
	})
}

func newQualificationSessionTestFixture(t *testing.T) (string, string) {
	t.Helper()
	durability, _ := qualificationFixtures()
	root := t.TempDir()
	_, environmentPath, _ := qualificationEnvironmentFixture(t, root, durability, time.Now().UTC().Add(-time.Minute), "ipmi-chassis-power-cycle")

	oldIdentity := qualificationSessionBuildIdentity
	oldValidate := qualificationSessionValidateReceipt
	oldLoad := qualificationSessionLoadReceiptState
	oldRuntime := qualificationSessionRuntimeIdentity
	oldEffectiveUID := qualificationSessionEffectiveUID
	qualificationSessionBuildIdentity = func() (string, bool) { return qualificationTestRevision, false }
	qualificationSessionRuntimeIdentity = func() (string, string, string) { return durability.GOOS, durability.GOARCH, durability.GoVersion }
	qualificationSessionEffectiveUID = func() int { return 1000 }
	qualificationSessionValidateReceipt = func(_ qualificationSessionPlan, _ qualificationSessionStep, path string, _ *qualificationSessionState) ([]byte, time.Time, error) {
		raw, err := os.ReadFile(path)
		return raw, time.Time{}, err
	}
	qualificationSessionLoadReceiptState = func(_ qualificationSessionStep, _ string, _ *qualificationSessionState) error { return nil }
	t.Cleanup(func() {
		qualificationSessionBuildIdentity = oldIdentity
		qualificationSessionValidateReceipt = oldValidate
		qualificationSessionLoadReceiptState = oldLoad
		qualificationSessionRuntimeIdentity = oldRuntime
		qualificationSessionEffectiveUID = oldEffectiveUID
	})

	if err := runQualificationSessionInit([]string{
		"--artifacts-root", root, "--environment-record", environmentPath,
		"--source-revision", qualificationTestRevision, "--platform-class", "linux-ext4-nvme",
	}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	canonicalRoot, err := qualificationArtifactRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	return canonicalRoot, filepath.Join(canonicalRoot, qualificationSessionDirectory, "plan.json")
}

func readQualificationSessionTestPlan(t *testing.T, path string) qualificationSessionPlan {
	t.Helper()
	var plan qualificationSessionPlan
	if _, err := readQualificationReceipt(path, &plan); err != nil {
		t.Fatal(err)
	}
	return plan
}

func recordQualificationSessionTestFirst(t *testing.T, root, planPath string) string {
	t.Helper()
	receipt := filepath.Join(root, "first.json")
	if err := os.WriteFile(receipt, []byte(`{"step":"first"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runQualificationSessionRecord([]string{"--plan", planPath, "--kind", "durability", "--receipt", receipt}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	return receipt
}
