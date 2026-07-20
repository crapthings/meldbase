package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	meldserver "github.com/crapthings/meldbase/server"
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

func TestSingleNodeSystemdLauncherPinsLoopbackDevelopmentDefaults(t *testing.T) {
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	argumentsPath := filepath.Join(directory, "arguments")
	secretPath := filepath.Join(directory, "jwt.secret")
	policyPath := filepath.Join(directory, "access-policy.json")
	if err := os.WriteFile(secretPath, []byte(strings.Repeat("s", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policyPath, []byte(`{"version":1,"workspaceField":"workspaceId","collections":[{"collection":"projects","mode":"collaborative"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(directory, "fake-meld")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$MELDBASE_ARGUMENTS_FILE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(repository, "deploy", "single-node", "systemd", "meldbase-single-node")
	policyExample := filepath.Join(repository, "deploy", "single-node", "systemd", "access-policy.json.example")
	policyRaw, err := os.ReadFile(policyExample)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := meldserver.ParseCollectionAccessManifestJSON(policyRaw); err != nil {
		t.Fatalf("systemd access policy example: %v", err)
	}
	run := func(extra ...string) (string, error) {
		command := exec.Command("sh", launcher)
		command.Env = append(os.Environ(),
			"MELDBASE_BIN="+binary,
			"MELDBASE_DB=/var/lib/meldbase/data/test.meld",
			"MELDBASE_ADMIN_TOKEN="+strings.Repeat("a", 32),
			"MELDBASE_JWT_HS256_SECRET_FILE="+secretPath,
			"MELDBASE_JWT_ISSUER=https://identity.example.test/",
			"MELDBASE_JWT_AUDIENCE=meldbase-api",
			"MELDBASE_ACCESS_POLICY_FILE="+policyPath,
			"MELDBASE_ARGUMENTS_FILE="+argumentsPath,
		)
		command.Env = append(command.Env, extra...)
		raw, err := command.CombinedOutput()
		return string(raw), err
	}
	if output, err := run(); err != nil {
		t.Fatalf("launcher: %v\n%s", err, output)
	}
	raw, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"serve", "--db", "/var/lib/meldbase/data/test.meld", "--addr", "127.0.0.1:8080",
		"--jwt-hs256-secret-file", secretPath, "--jwt-issuer", "https://identity.example.test/", "--jwt-audience", "meldbase-api",
		"--access-policy-file", policyPath,
		"--admin-addr", "127.0.0.1:9091", "--admin-diagnostics", "--admin-metrics",
	}
	if got := strings.Split(strings.TrimSpace(string(raw)), "\n"); !reflect.DeepEqual(got, want) {
		t.Fatalf("arguments=%q want=%q", got, want)
	}
	if output, err := run("MELDBASE_ACCESS_POLICY_FILE="); err == nil || !strings.Contains(output, "must name a readable manifest") {
		t.Fatalf("missing policy should fail: err=%v output=%q", err, output)
	}
	if output, err := run("MELDBASE_ADDR=0.0.0.0:8080"); err == nil || !strings.Contains(output, "loopback") {
		t.Fatalf("public listener should fail: err=%v output=%q", err, output)
	}

	unit, err := os.ReadFile(filepath.Join(repository, "deploy", "single-node", "systemd", "meldbase.service"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"User=meldbase", "EnvironmentFile=/etc/meldbase/meldbase.env", "UMask=0077", "NoNewPrivileges=true",
		"ProtectSystem=strict", "ReadWritePaths=/var/lib/meldbase", "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6",
	} {
		if !strings.Contains(string(unit), required) {
			t.Fatalf("service is missing %q", required)
		}
	}
	environment, err := os.ReadFile(filepath.Join(repository, "deploy", "single-node", "systemd", "meldbase.env.example"))
	if err != nil || !strings.Contains(string(environment), "MELDBASE_ACCESS_POLICY_FILE=/etc/meldbase/access-policy.json") {
		t.Fatalf("environment example=%q err=%v", environment, err)
	}
}

func TestSingleNodeBackupRestoreDrillRetainsPrivateEvidenceInOrder(t *testing.T) {
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	database := filepath.Join(directory, "source.meld")
	if err := os.WriteFile(database, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(directory, "calls")
	binary := filepath.Join(directory, "fake-meld")
	fake := `#!/bin/sh
set -eu
printf '%s|' "$@" >> "$MELDBASE_CALLS"
printf '\n' >> "$MELDBASE_CALLS"
out=''
previous=''
for argument in "$@"; do
  if [ "$previous" = '--out' ]; then out="$argument"; break; fi
  previous="$argument"
done
case "$1" in
  backup|restore) : > "$out"; printf '{"version":1}\n' ;;
  *) printf '{"version":1}\n' ;;
esac
`
	if err := os.WriteFile(binary, []byte(fake), 0o700); err != nil {
		t.Fatal(err)
	}
	evidence := filepath.Join(directory, "rehearsal")
	command := exec.Command("bash", filepath.Join(repository, "scripts", "single-node-backup-restore-drill.sh"),
		"--meld", binary, "--db", database, "--out-dir", evidence, "--timeout", "7s", "--max-bytes", "123")
	command.Env = append(os.Environ(), "MELDBASE_CALLS="+calls)
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("drill: %v\n%s", err, raw)
	}
	if !strings.Contains(string(raw), "Backup and restore drill passed") {
		t.Fatalf("drill output=%q", raw)
	}
	info, err := os.Stat(evidence)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("evidence directory info=%v err=%v", info, err)
	}
	for _, name := range []string{
		"source-inspect.json", "source-verify.json", "physical-backup.meld", "backup-receipt.json",
		"restored.meld", "restore-receipt.json", "restored-inspect.json", "restored-verify.json",
	} {
		if info, err := os.Stat(filepath.Join(evidence, name)); err != nil || info.IsDir() {
			t.Fatalf("evidence %s info=%v err=%v", name, info, err)
		}
	}
	got, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	want := []string{
		"inspect|--db|" + database + "|--require-compatible|",
		"verify|--db|" + database + "|--timeout|7s|",
		"backup|--db|" + database + "|--out|" + filepath.Join(evidence, "physical-backup.meld") + "|--timeout|7s|",
		"restore|--in|" + filepath.Join(evidence, "physical-backup.meld") + "|--receipt|" + filepath.Join(evidence, "backup-receipt.json") + "|--out|" + filepath.Join(evidence, "restored.meld") + "|--timeout|7s|--max-bytes|123|",
		"inspect|--db|" + filepath.Join(evidence, "restored.meld") + "|--require-compatible|",
		"verify|--db|" + filepath.Join(evidence, "restored.meld") + "|--timeout|7s|",
	}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("drill calls=%q want=%q", lines, want)
	}
}
