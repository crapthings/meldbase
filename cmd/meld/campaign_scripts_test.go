package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDestructiveQEMUMatrixDryRunPinsLegacyAndSessionTrials(t *testing.T) {
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(repository, "scripts", "destructive-qemu-matrix.sh")
	control := t.TempDir()
	inputs := make([]string, 4)
	for index := range inputs {
		inputs[index] = filepath.Join(t.TempDir(), "input")
		if err := os.WriteFile(inputs[index], []byte("fixture"), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	run := func(t *testing.T, session bool) string {
		t.Helper()
		arguments := []string{script, control, inputs[0], inputs[1], inputs[2], inputs[3]}
		command := exec.Command("bash", arguments...)
		command.Env = append(os.Environ(), "MELDBASE_QEMU_DRY_RUN=1", "MELDBASE_QEMU_CUT_MODE=host-sigkill")
		if session {
			plan := filepath.Join(control, "plan.json")
			if err := os.WriteFile(plan, []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}
			arguments = append(arguments, qualificationTestRevision)
			command = exec.Command("bash", arguments...)
			command.Env = append(os.Environ(), "MELDBASE_QEMU_DRY_RUN=1", "MELDBASE_QEMU_CUT_MODE=host-sigkill", "MELDBASE_QUALIFICATION_PLAN="+plan)
		}
		raw, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("dry run: %v\n%s", err, raw)
		}
		return strings.TrimSpace(string(raw))
	}

	legacy := strings.Split(run(t, false), "\n")
	if len(legacy) != 15 || !strings.Contains(legacy[0], "trial=matrix-01-after-page-write-1") ||
		!strings.Contains(legacy[14], "trial=matrix-15-after-meta-sync-3") {
		t.Fatalf("legacy plan=%q", legacy)
	}
	session := strings.Split(run(t, true), "\n")
	if len(session) != 15 || !strings.Contains(session[0], "trial=power-01-01 boundary=after-page-write") ||
		!strings.Contains(session[14], "trial=power-05-03 boundary=after-meta-sync") {
		t.Fatalf("session plan=%q", session)
	}
}

func TestQualificationLinuxFoundationDryRunPinsDestructiveOrder(t *testing.T) {
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	target := t.TempDir()
	binary := filepath.Join(t.TempDir(), "meld")
	operator := filepath.Join(root, "operator.json")
	controllerPublic := filepath.Join(root, "controller.pub")
	if err := os.WriteFile(binary, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(operator, []byte("approved"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(controllerPublic, []byte("controller-public-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", filepath.Join(repository, "scripts", "qualification-linux-foundation.sh"),
		root, target, binary, qualificationTestRevision, "linux-ext4-nvme", "redfish-computer-system-power-cycle", operator, controllerPublic, strings.Repeat("71", 32))
	command.Env = append(os.Environ(), "MELDBASE_QUALIFICATION_DRY_RUN=1")
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("dry run: %v\n%s", err, raw)
	}
	want := []string{"stage=environment", "stage=session-init", "stage=durability", "stage=soak", "stage=process", "stage=capacity", "stage=corruption"}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != len(want) {
		t.Fatalf("foundation plan=%q", lines)
	}
	for index := range want {
		if !strings.HasPrefix(lines[index], want[index]) {
			t.Fatalf("foundation plan[%d]=%q want prefix %q", index, lines[index], want[index])
		}
	}
	if !strings.Contains(lines[0], "attested=true") {
		t.Fatalf("foundation environment stage lacks physical controller attestation: %q", lines[0])
	}
}
