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

const qualificationTestRevision = "0123456789abcdef0123456789abcdef01234567"

func TestQualificationCheckBuildsLevelThreePacketFromSameVolumeEvidence(t *testing.T) {
	durability, soak := qualificationFixtures()
	durabilityPath, _ := writeQualificationFixture(t, "durability.json", durability)
	soakPath, _ := writeQualificationFixture(t, "soak.json", soak)
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--source-revision", qualificationTestRevision,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("qualification check=%v stderr=%s", err, stderr.String())
	}
	var packet qualificationCheckResult
	if err := json.Unmarshal(stdout.Bytes(), &packet); err != nil {
		t.Fatal(err)
	}
	if packet.SchemaVersion != 1 || packet.SourceRevision != qualificationTestRevision || packet.EvidenceLevel != 3 ||
		packet.ProductionQualified || !packet.Passed || packet.GOOS != durability.GOOS || packet.Device != durability.Device ||
		!qualificationHexDigest(packet.DurabilityReceiptSHA256) || !qualificationHexDigest(packet.SoakReceiptSHA256) ||
		packet.DestructiveRecordSHA256 != "" {
		t.Fatalf("packet=%+v", packet)
	}
}

func TestQualificationCheckRequiresDestructiveRecordForLevelFour(t *testing.T) {
	durability, soak := qualificationFixtures()
	durabilityPath, _ := writeQualificationFixture(t, "durability.json", durability)
	soakPath, _ := writeQualificationFixture(t, "soak.json", soak)
	var output bytes.Buffer
	err := run([]string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--source-revision", qualificationTestRevision, "--require-level", "4",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "does not satisfy required level 4") {
		t.Fatalf("error=%v output=%s", err, output.String())
	}
}

func TestQualificationCheckAcceptsBoundCompleteDestructiveRecord(t *testing.T) {
	durability, soak := qualificationFixtures()
	durabilityPath, durabilityRaw := writeQualificationFixture(t, "durability.json", durability)
	soakPath, soakRaw := writeQualificationFixture(t, "soak.json", soak)
	record := qualificationDestructiveRecord{
		SchemaVersion: 1, SourceRevision: qualificationTestRevision, PlatformClass: "linux-ext4-nvme",
		GOOS: durability.GOOS, GOARCH: durability.GOARCH, Device: durability.Device,
		FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		StartedAt: durability.StartedAt, FinishedAt: durability.FinishedAt,
		DurabilityReceiptSHA256: qualificationSHA256(durabilityRaw), SoakReceiptSHA256: qualificationSHA256(soakRaw),
		CapacityExhaustion: true, ProcessKill: true, PowerCut: true, PublicationBoundaries: true,
		LockReacquisition: true, OldOrNewStateOnly: true, OfflineVerification: true,
		KernelAndMountRecorded: true, ControllerPolicyRecorded: true, HostAndOperatorRecorded: true,
		SecuredArtifactsSHA256: strings.Repeat("ab", 32),
	}
	recordPath, _ := writeQualificationFixture(t, "destructive.json", record)
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--destructive-record", recordPath, "--source-revision", qualificationTestRevision, "--require-level", "4",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("qualification check=%v stderr=%s", err, stderr.String())
	}
	var packet qualificationCheckResult
	if err := json.Unmarshal(stdout.Bytes(), &packet); err != nil || packet.EvidenceLevel != 4 || !packet.ProductionQualified ||
		!qualificationHexDigest(packet.DestructiveRecordSHA256) {
		t.Fatalf("packet=%+v err=%v", packet, err)
	}
}

func TestQualificationCheckRejectsWrongRevisionProfileVolumeAndBinding(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*durabilityCheckResult, *qualificationSoakReceipt)
		want   string
	}{
		{name: "wrong revision", mutate: func(_ *durabilityCheckResult, soak *qualificationSoakReceipt) {
			soak.SourceRevision = strings.Repeat("f", 40)
		}, want: "release revision"},
		{name: "wrong binary revision", mutate: func(_ *durabilityCheckResult, soak *qualificationSoakReceipt) {
			soak.BuildRevision = strings.Repeat("f", 40)
		}, want: "binary matches"},
		{name: "dirty binary", mutate: func(_ *durabilityCheckResult, soak *qualificationSoakReceipt) {
			soak.BuildModified = true
		}, want: "clean schema-4"},
		{name: "sentinel", mutate: func(_ *durabilityCheckResult, soak *qualificationSoakReceipt) {
			soak.Profile = "sentinel"
		}, want: "release receipt"},
		{name: "legacy soak schema", mutate: func(_ *durabilityCheckResult, soak *qualificationSoakReceipt) {
			soak.SchemaVersion = 3
		}, want: "schema-4"},
		{name: "different volume", mutate: func(_ *durabilityCheckResult, soak *qualificationSoakReceipt) {
			soak.Device++
		}, want: "target volume identity"},
		{name: "missing phase work", mutate: func(_ *durabilityCheckResult, soak *qualificationSoakReceipt) {
			soak.Phases[3].SnapshotReads = 0
		}, want: "phase 4"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			durability, soak := qualificationFixtures()
			test.mutate(&durability, &soak)
			durabilityPath, _ := writeQualificationFixture(t, "durability.json", durability)
			soakPath, _ := writeQualificationFixture(t, "soak.json", soak)
			var output bytes.Buffer
			err := run([]string{
				"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
				"--source-revision", qualificationTestRevision,
			}, &output, &output)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v output=%s", err, output.String())
			}
		})
	}
}

func TestQualificationCheckRejectsUnknownReceiptFields(t *testing.T) {
	durability, soak := qualificationFixtures()
	durabilityPath, _ := writeQualificationFixture(t, "durability.json", durability)
	soakRaw, err := json.Marshal(soak)
	if err != nil {
		t.Fatal(err)
	}
	soakRaw = bytes.Replace(soakRaw, []byte(`"schemaVersion":4`), []byte(`"schemaVersion":4,"unexpected":true`), 1)
	soakPath := filepath.Join(t.TempDir(), "soak.json")
	if err := os.WriteFile(soakPath, soakRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err = run([]string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--source-revision", qualificationTestRevision,
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error=%v output=%s", err, output.String())
	}
}

func TestQualificationProductionFilesystemIsExplicit(t *testing.T) {
	for _, name := range []string{"ext-family", "xfs", "btrfs", "apfs"} {
		if !qualificationProductionFilesystem(name) {
			t.Fatalf("supported filesystem %q rejected", name)
		}
	}
	for _, name := range []string{"", "tmpfs", "nfs", "overlayfs", "fuse", "unknown-1234"} {
		if qualificationProductionFilesystem(name) {
			t.Fatalf("unsupported filesystem %q accepted", name)
		}
	}
}

func TestQualificationTimingAllowsSmallWallClockDrift(t *testing.T) {
	started := time.Date(2026, 7, 17, 1, 0, 0, 0, time.UTC)
	if !qualificationTimingValid(started, started.Add(time.Minute+3*time.Second), time.Minute) {
		t.Fatal("small wall-clock drift rejected")
	}
	if qualificationTimingValid(started, started.Add(time.Minute+6*time.Second), time.Minute) {
		t.Fatal("large wall-clock drift accepted")
	}
	if qualificationTimingValid(time.Time{}, started, time.Minute) || qualificationTimingValid(started, started, time.Minute) {
		t.Fatal("invalid timestamps accepted")
	}
}

func qualificationFixtures() (durabilityCheckResult, qualificationSoakReceipt) {
	started := time.Date(2026, 7, 17, 1, 0, 0, 0, time.UTC)
	checks := make([]durabilityCheckRecord, 0, 10)
	for _, name := range []string{
		"create-private-probe-directory", "file-write-and-fsync", "parent-directory-fsync-after-create", "probe-directory-fsync",
		"exclusive-advisory-lock-and-close-release", "atomic-no-overwrite-link", "same-directory-rename-and-fsync",
		"meldbase-create-indexed-commit-reopen", "meldbase-offline-full-verification", "cleanup-and-parent-fsync",
	} {
		checks = append(checks, durabilityCheckRecord{Name: name, Passed: true, Duration: time.Millisecond})
	}
	durability := durabilityCheckResult{
		SchemaVersion: 2, SourceRevision: qualificationTestRevision, Directory: "/qualified/target",
		GOOS: "linux", GOARCH: "amd64", GoVersion: "go1.25.0", BuildRevision: qualificationTestRevision,
		Device: 42, FilesystemType: "0xef53", FilesystemName: "ext-family", BlockSize: 4096,
		TotalBytes: 1 << 40, AvailableBytes: 1 << 39, StartedAt: started, FinishedAt: started.Add(time.Second),
		Duration: time.Second, Passed: true, Checks: checks,
		Database: &durabilityDatabaseProof{
			VerificationSchema: 3, FormatRevision: 3, CommitSequence: 3, FileBytes: 4096,
			PhysicalPages: 4, ReachablePages: 3, IndexVerified: true, FreeSpaceValid: true, SHA256: strings.Repeat("01", 32),
		},
	}
	phases := make([]qualificationSoakPhase, 12)
	for index := range phases {
		phases[index] = qualificationSoakPhase{
			Ordinal: index + 1, Duration: 21 * time.Minute, ConcurrentDuration: 20 * time.Minute,
			Writes: 1, SnapshotReads: 1, IndexBuildBatches: 1,
			ReclamationAttempts: 1, CommitSequence: uint64(index + 10), PhysicalPages: 100, ReusablePages: 10, IndexBuildPhase: 1,
		}
	}
	phases[0].ReclamationConflicts = 1
	soak := qualificationSoakReceipt{
		SchemaVersion: 4, FormatRevision: 3, Engine: "v2", Profile: "release", RaceEnabled: true,
		GOOS: durability.GOOS, GOARCH: durability.GOARCH, GoVersion: durability.GoVersion,
		SourceRevision: qualificationTestRevision, BuildRevision: qualificationTestRevision,
		Device: durability.Device, FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		StartedAt: started, FinishedAt: started.Add(4*time.Hour + 15*time.Minute), RequestedSeconds: 4 * 60 * 60,
		ConcurrentDuration: 4 * time.Hour, ActualDuration: 4*time.Hour + 15*time.Minute,
		Documents: 10_000, RequestedReopens: 12, CompletedReopens: 12,
		Writes: 12, SnapshotReads: 12, IndexBuildBatches: 12, ReclamationAttempts: 12, ReclamationConflicts: 1,
		FinalCommitSequence: 30, FinalFileBytes: 1 << 20, FinalPhysicalPages: 200, FinalReachablePages: 150,
		FinalReclaimablePages: 25, FinalFileSHA256: strings.Repeat("02", 32), PersistentFreeSpace: true,
		FreeSpaceValid: true, SemanticIndexes: true, SemanticIndexBuilds: true, FinalIndexBuildAbsent: true, Phases: phases,
	}
	return durability, soak
}

func writeQualificationFixture(t *testing.T, name string, value any) (string, []byte) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, raw
}
