package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/crapthings/meldbase/integrations/anchorhttp"
	historyqualification "github.com/crapthings/meldbase/internal/qualification"
)

const qualificationTestRevision = "0123456789abcdef0123456789abcdef01234567"

func qualificationPhysicalControllerTestPrivateKey() ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
}

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
	if packet.SchemaVersion != 2 || packet.SourceRevision != qualificationTestRevision || packet.EvidenceLevel != 3 ||
		packet.StorageQualified || packet.RollbackProtectionQualified || packet.ProductionQualified || !packet.Passed || packet.GOOS != durability.GOOS || packet.Device != durability.Device ||
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
	withQualificationDestructiveRecomputeBypass(t)
	durability, soak := qualificationFixtures()
	durabilityPath, durabilityRaw := writeQualificationFixture(t, "durability.json", durability)
	soakPath, soakRaw := writeQualificationFixture(t, "soak.json", soak)
	record, artifacts := qualificationDestructiveFixture(t, durability, soak, durabilityRaw, soakRaw)
	recordPath, _ := writeQualificationFixture(t, "destructive.json", record)
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--destructive-record", recordPath, "--environment-record", artifacts.environmentPath,
		"--artifacts-root", artifacts.rootPath, "--artifacts-index", artifacts.indexPath,
		"--source-revision", qualificationTestRevision, "--require-level", "4",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("qualification check=%v stderr=%s", err, stderr.String())
	}
	var packet qualificationCheckResult
	if err := json.Unmarshal(stdout.Bytes(), &packet); err != nil || packet.EvidenceLevel != 4 || !packet.StorageQualified || packet.RollbackProtectionQualified || packet.ProductionQualified ||
		!qualificationHexDigest(packet.DestructiveRecordSHA256) {
		t.Fatalf("packet=%+v err=%v", packet, err)
	}
}

func TestQualificationCheckLevelFiveRequiresAndBindsCompleteRollbackProtectionEvidence(t *testing.T) {
	withQualificationDestructiveRecomputeBypass(t)
	durability, soak := qualificationFixtures()
	durabilityPath, durabilityRaw := writeQualificationFixture(t, "durability.json", durability)
	soakPath, soakRaw := writeQualificationFixture(t, "soak.json", soak)
	record, artifacts := qualificationDestructiveFixture(t, durability, soak, durabilityRaw, soakRaw)
	recordPath, _ := writeQualificationFixture(t, "destructive.json", record)
	anchor := qualificationAnchorFixture(t)
	arguments := qualificationLevelFiveArguments(durabilityPath, soakPath, recordPath, artifacts, anchor)
	var output bytes.Buffer
	if err := run(arguments, &output, &output); err != nil {
		t.Fatalf("Level 5 qualification=%v output=%s", err, output.String())
	}
	var packet qualificationCheckResult
	if err := json.Unmarshal(output.Bytes(), &packet); err != nil {
		t.Fatal(err)
	}
	if packet.SchemaVersion != 2 || packet.EvidenceLevel != 5 || !packet.StorageQualified || !packet.RollbackProtectionQualified || !packet.ProductionQualified || !packet.Passed ||
		len(packet.AnchorPhaseReceiptSHA256) != len(anchorQualificationPhases) || !qualificationHexDigest(packet.AnchorPublicKeySHA256) ||
		!qualificationHexDigest(packet.AnchorHistoryReceiptSHA256) || !qualificationHexDigest(packet.AnchorHistoryControllerSHA256) ||
		packet.AnchorRunID == packet.AnchorHistoryRunID || packet.AnchorConfigurationID == packet.AnchorHistoryConfigurationID {
		t.Fatalf("Level 5 packet=%+v", packet)
	}
	for index, path := range anchor.phasePaths {
		raw, err := os.ReadFile(path)
		if err != nil || packet.AnchorPhaseReceiptSHA256[index] != qualificationSHA256(raw) {
			t.Fatalf("phase binding %d=%q err=%v", index, packet.AnchorPhaseReceiptSHA256[index], err)
		}
	}
}

func TestQualificationCheckLevelFiveRejectsMissingMismatchedAndReusedAnchorEvidence(t *testing.T) {
	withQualificationDestructiveRecomputeBypass(t)
	durability, soak := qualificationFixtures()
	durabilityPath, durabilityRaw := writeQualificationFixture(t, "durability.json", durability)
	soakPath, soakRaw := writeQualificationFixture(t, "soak.json", soak)
	record, artifacts := qualificationDestructiveFixture(t, durability, soak, durabilityRaw, soakRaw)
	recordPath, _ := writeQualificationFixture(t, "destructive.json", record)
	base := []string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--destructive-record", recordPath, "--environment-record", artifacts.environmentPath,
		"--artifacts-root", artifacts.rootPath, "--artifacts-index", artifacts.indexPath,
		"--source-revision", qualificationTestRevision, "--require-level", "5",
	}
	var output bytes.Buffer
	if err := run(base, &output, &output); err == nil || !strings.Contains(err.Error(), "evidence level 4") {
		t.Fatalf("missing anchor evidence error=%v", err)
	}
	anchor := qualificationAnchorFixture(t)
	partial := append(append([]string(nil), base...), "--anchor-public-key", anchor.publicKeyPath)
	if err := run(partial, &output, &output); err == nil || !strings.Contains(err.Error(), "five ordered phase receipts") {
		t.Fatalf("partial anchor evidence error=%v", err)
	}
	wrongPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongPublicPath := filepath.Join(t.TempDir(), "wrong-anchor.pub")
	if err := writeAnchorQualificationKey(wrongPublicPath, wrongPublic, 0o644); err != nil {
		t.Fatal(err)
	}
	wrongKeyFixture := anchor
	wrongKeyFixture.publicKeyPath = wrongPublicPath
	if err := run(qualificationLevelFiveArguments(durabilityPath, soakPath, recordPath, artifacts, wrongKeyFixture), &output, &output); err == nil || !strings.Contains(err.Error(), "public key differs") {
		t.Fatalf("wrong anchor public key error=%v", err)
	}

	reused := anchor.historyReceipt
	reused.ExternalEvidenceSHA256 = anchor.phaseReceipts[0].ExternalEvidenceSHA256
	if err := signAnchorHistoryQualificationReceipt(&reused, anchor.privateKey); err != nil {
		t.Fatal(err)
	}
	reusedPath, _ := writeQualificationFixture(t, "history-reused-evidence.json", reused)
	reusedFixture := anchor
	reusedFixture.historyReceiptPath = reusedPath
	if err := run(qualificationLevelFiveArguments(durabilityPath, soakPath, recordPath, artifacts, reusedFixture), &output, &output); err == nil || !strings.Contains(err.Error(), "reuses phase external evidence") {
		t.Fatalf("reused external evidence error=%v", err)
	}

	wrongRevision := anchor.historyReceipt
	wrongRevision.SourceRevision = strings.Repeat("f", 40)
	wrongRevision.BuildRevision = wrongRevision.SourceRevision
	if err := signAnchorHistoryQualificationReceipt(&wrongRevision, anchor.privateKey); err != nil {
		t.Fatal(err)
	}
	wrongRevisionPath, _ := writeQualificationFixture(t, "history-wrong-revision.json", wrongRevision)
	wrongFixture := anchor
	wrongFixture.historyReceiptPath = wrongRevisionPath
	if err := run(qualificationLevelFiveArguments(durabilityPath, soakPath, recordPath, artifacts, wrongFixture), &output, &output); err == nil || !strings.Contains(err.Error(), "does not bind") {
		t.Fatalf("wrong history revision error=%v", err)
	}

	oldAgentController := anchor.historyController
	oldAgentController.Fragments = append([]anchorHistoryAgentFragment(nil), anchor.historyController.Fragments...)
	oldAgentController.Fragments[0].BuildRevision = strings.Repeat("6", 40)
	if err := signAnchorHistoryAgentFragment(&oldAgentController.Fragments[0], anchor.agentPrivateKeys[0]); err != nil {
		t.Fatal(err)
	}
	oldAgentControllerPath, oldAgentControllerRaw := writeQualificationFixture(t, "controller-old-agent.json", oldAgentController)
	oldAgentReceipt := anchor.historyReceipt
	oldAgentReceipt.ControllerSHA256 = qualificationSHA256(oldAgentControllerRaw)
	if err := signAnchorHistoryQualificationReceipt(&oldAgentReceipt, anchor.privateKey); err != nil {
		t.Fatal(err)
	}
	oldAgentReceiptPath, _ := writeQualificationFixture(t, "history-old-agent.json", oldAgentReceipt)
	oldAgentFixture := anchor
	oldAgentFixture.historyPath, oldAgentFixture.historyReceiptPath = oldAgentControllerPath, oldAgentReceiptPath
	if err := run(qualificationLevelFiveArguments(durabilityPath, soakPath, recordPath, artifacts, oldAgentFixture), &output, &output); err == nil || !strings.Contains(err.Error(), "history agent") {
		t.Fatalf("old history-agent build error=%v", err)
	}

	wrongPhaseRevision := anchor
	wrongPhaseRevision.phasePaths = rewriteQualificationPhaseChain(t, anchor, func(receipt *anchorQualificationReceipt) {
		receipt.SourceRevision = strings.Repeat("5", 40)
		receipt.BuildRevision = receipt.SourceRevision
	})
	if err := run(qualificationLevelFiveArguments(durabilityPath, soakPath, recordPath, artifacts, wrongPhaseRevision), &output, &output); err == nil || !strings.Contains(err.Error(), "phase chain is not bound") {
		t.Fatalf("wrong phase revision error=%v", err)
	}

	sameTrustResource := anchor
	sameTrustResource.phasePaths = rewriteQualificationPhaseChain(t, anchor, func(receipt *anchorQualificationReceipt) {
		receipt.RunID = anchor.historyReceipt.RunID
		receipt.ConfigurationID = anchor.historyReceipt.ConfigurationID
	})
	if err := run(qualificationLevelFiveArguments(durabilityPath, soakPath, recordPath, artifacts, sameTrustResource), &output, &output); err == nil || !strings.Contains(err.Error(), "separate runs") {
		t.Fatalf("reused anchor run/configuration error=%v", err)
	}

	reordered := qualificationLevelFiveArguments(durabilityPath, soakPath, recordPath, artifacts, anchor)
	first := indexOfArgumentValue(reordered, "--anchor-phase-receipt", 0)
	second := indexOfArgumentValue(reordered, "--anchor-phase-receipt", 1)
	reordered[first], reordered[second] = reordered[second], reordered[first]
	if err := run(reordered, &output, &output); err == nil || !strings.Contains(err.Error(), "out of phase order") {
		t.Fatalf("reordered phase chain error=%v", err)
	}
}

func TestQualificationCheckRejectsBooleanOnlyLegacyDestructiveRecord(t *testing.T) {
	durability, soak := qualificationFixtures()
	durabilityPath, durabilityRaw := writeQualificationFixture(t, "durability.json", durability)
	soakPath, soakRaw := writeQualificationFixture(t, "soak.json", soak)
	legacy := map[string]any{
		"schemaVersion": 1, "sourceRevision": qualificationTestRevision, "platformClass": "linux-ext4-nvme",
		"goos": durability.GOOS, "goarch": durability.GOARCH, "device": durability.Device,
		"filesystemType": durability.FilesystemType, "filesystemName": durability.FilesystemName, "blockSize": durability.BlockSize,
		"startedAt": durability.StartedAt, "finishedAt": durability.FinishedAt,
		"durabilityReceiptSha256": qualificationSHA256(durabilityRaw), "soakReceiptSha256": qualificationSHA256(soakRaw),
		"capacityExhaustion": true, "processKill": true, "powerCut": true, "publicationBoundariesCovered": true,
		"lockReacquisition": true, "oldOrNewStateOnly": true, "offlineVerification": true,
		"kernelAndMountRecorded": true, "controllerPolicyRecorded": true, "hostAndOperatorRecorded": true,
		"securedArtifactsSha256": strings.Repeat("ab", 32),
	}
	recordPath, _ := writeQualificationFixture(t, "legacy-destructive.json", legacy)
	artifacts := newQualificationArtifactTestFixture(t, durability, durability.StartedAt.Add(-time.Minute))
	var output bytes.Buffer
	err := run([]string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--destructive-record", recordPath, "--environment-record", artifacts.environmentPath,
		"--artifacts-root", artifacts.rootPath, "--artifacts-index", artifacts.indexPath,
		"--source-revision", qualificationTestRevision, "--require-level", "4",
	}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error=%v output=%s", err, output.String())
	}
}

func TestQualificationCheckRejectsMissingDestructiveBoundaryAndInvalidOutcome(t *testing.T) {
	durability, soak := qualificationFixtures()
	durabilityPath, durabilityRaw := writeQualificationFixture(t, "durability.json", durability)
	soakPath, soakRaw := writeQualificationFixture(t, "soak.json", soak)
	tests := []struct {
		name   string
		mutate func(*qualificationDestructiveRecord)
		want   string
	}{
		{name: "legacy schema without corruption binding", mutate: func(record *qualificationDestructiveRecord) {
			record.SchemaVersion = 2
		}, want: "record identity"},
		{name: "missing corruption receipt", mutate: func(record *qualificationDestructiveRecord) {
			record.CorruptionReceiptSHA256 = ""
		}, want: "aggregate artifact evidence"},
		{name: "missing power boundary", mutate: func(record *qualificationDestructiveRecord) {
			record.Trials = record.Trials[:len(record.Trials)-1]
		}, want: "power-cut evidence requires at least 3 trials at after-meta-sync"},
		{name: "self asserted outcome", mutate: func(record *qualificationDestructiveRecord) {
			record.Trials[0].Outcome = "new"
		}, want: "outcome does not match"},
		{name: "missing offline proof", mutate: func(record *qualificationDestructiveRecord) {
			record.Trials[0].OfflineVerified = false
		}, want: "lacks lock"},
		{name: "state does not match outcome", mutate: func(record *qualificationDestructiveRecord) {
			record.Trials[0].RecoveredStateSHA256 = record.Trials[0].NewStateSHA256
		}, want: "recovered state does not match"},
		{name: "simulated capacity trigger", mutate: func(record *qualificationDestructiveRecord) {
			record.Trials[0].TriggerPoint = "simulated-test-only"
		}, want: "not bound to a real ENOSPC"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record, artifacts := qualificationDestructiveFixture(t, durability, soak, durabilityRaw, soakRaw)
			test.mutate(&record)
			recordPath, _ := writeQualificationFixture(t, "destructive.json", record)
			var output bytes.Buffer
			err := run([]string{
				"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
				"--destructive-record", recordPath, "--environment-record", artifacts.environmentPath,
				"--artifacts-root", artifacts.rootPath, "--artifacts-index", artifacts.indexPath,
				"--source-revision", qualificationTestRevision, "--require-level", "4",
			}, &output, &output)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v output=%s", err, output.String())
			}
		})
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

func TestQualificationSoakAllowsBoundedClockDriftAndStableFinalSequence(t *testing.T) {
	durability, soak := qualificationFixtures()
	soak.FinishedAt = soak.FinishedAt.Add(500 * time.Millisecond)
	soak.FinalCommitSequence = soak.Phases[len(soak.Phases)-1].CommitSequence

	if err := validateQualificationSoak(soak, durability, qualificationTestRevision); err != nil {
		t.Fatalf("valid long-run receipt rejected: %v", err)
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
		Device: 2049, FilesystemType: "0xef53", FilesystemName: "ext-family", BlockSize: 4096,
		TotalBytes: 1 << 30, AvailableBytes: 1 << 29, StartedAt: started, FinishedAt: started.Add(time.Second),
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

func qualificationDestructiveFixture(t *testing.T, durability durabilityCheckResult, soak qualificationSoakReceipt, durabilityRaw, soakRaw []byte) (qualificationDestructiveRecord, qualificationArtifactTestFixture) {
	t.Helper()
	started := durability.StartedAt
	artifacts := newQualificationArtifactTestFixture(t, durability, started.Add(-time.Minute))
	finished := started.Add(2 * time.Hour)
	record := qualificationDestructiveRecord{
		SchemaVersion: 6, SourceRevision: qualificationTestRevision, PlatformClass: "linux-ext4-nvme",
		GOOS: durability.GOOS, GOARCH: durability.GOARCH, Device: durability.Device,
		FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		StartedAt: started, FinishedAt: finished,
		DurabilityReceiptSHA256: qualificationSHA256(durabilityRaw), SoakReceiptSHA256: qualificationSHA256(soakRaw),
		ProcessReceiptSHA256: strings.Repeat("41", 32), CapacityReceiptSHA256: strings.Repeat("42", 32),
		CorruptionReceiptSHA256: strings.Repeat("44", 32),
		PowerReceiptSHA256:      []string{strings.Repeat("43", 32)},
		Infrastructure: qualificationInfrastructure{
			EnvironmentRecordSHA256: artifacts.environmentSHA256, KernelAndMountSHA256: artifacts.kernelAndMountSHA256,
			ControllerPolicySHA256: artifacts.controllerSHA256, HostAndOperatorSHA256: artifacts.hostSHA256,
			ControllerMethod: artifacts.environment.Controller.Method,
		},
		SecuredArtifactsIndexSHA256: artifacts.indexSHA256,
		SessionPlanSHA256:           strings.Repeat("51", 32), SessionHeadEventSHA256: strings.Repeat("52", 32),
		SessionExecutableSHA256: strings.Repeat("53", 32),
	}
	ordinal := 0
	appendTrial := func(kind, boundary string) {
		ordinal++
		trialStarted := started.Add(time.Duration(ordinal) * time.Minute)
		trigger := "real-enospc-at-boundary"
		if kind == qualificationTrialProcess {
			trigger = "oracle-prepared"
			if ordinal%2 == 0 {
				trigger = "oracle-committed"
			}
		} else if kind == qualificationTrialPower {
			trigger = "external-power-cut"
		}
		record.Trials = append(record.Trials, qualificationDestructiveTrial{
			ID: fmt.Sprintf("trial-%02d", ordinal), Kind: kind, PublicationBoundary: boundary, TriggerPoint: trigger,
			StartedAt: trialStarted, FinishedAt: trialStarted.Add(time.Second),
			OldCommitSequence: uint64(ordinal), NewCommitSequence: uint64(ordinal + 1), RecoveredSequence: uint64(ordinal), Outcome: "old",
			OldStateSHA256: strings.Repeat("70", 32), NewStateSHA256: strings.Repeat("80", 32), RecoveredStateSHA256: strings.Repeat("70", 32),
			LockReacquired: true, OfflineVerified: true, DatabaseSHA256: strings.Repeat("50", 32), ArtifactsSHA256: strings.Repeat("60", 32),
		})
	}
	for _, boundary := range qualificationPublicationBoundaries {
		for range qualificationMinimumBoundaryTrials {
			appendTrial(qualificationTrialCapacity, boundary)
		}
	}
	for range qualificationMinimumProcessTrials {
		appendTrial(qualificationTrialProcess, qualificationAsyncBoundary)
	}
	for _, boundary := range qualificationPublicationBoundaries {
		for range qualificationMinimumBoundaryTrials {
			appendTrial(qualificationTrialPower, boundary)
		}
	}
	return record, artifacts
}

type qualificationAnchorTestFixture struct {
	publicKeyPath      string
	phasePaths         []string
	phaseReceipts      []anchorQualificationReceipt
	historyReceiptPath string
	historyPath        string
	historyReceipt     anchorHistoryQualificationReceipt
	historyController  anchorHistoryControllerRecord
	privateKey         ed25519.PrivateKey
	agentPrivateKeys   []ed25519.PrivateKey
}

func qualificationAnchorFixture(t *testing.T) qualificationAnchorTestFixture {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicKeyPath := filepath.Join(t.TempDir(), "anchor.pub")
	if err := writeAnchorQualificationKey(publicKeyPath, publicKey, 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	phasePaths := make([]string, len(anchorQualificationPhases))
	phaseReceipts := make([]anchorQualificationReceipt, len(anchorQualificationPhases))
	var previousRaw []byte
	for index, phase := range anchorQualificationPhases {
		receipt := syntheticAnchorQualificationReceipt(phase, now.Add(time.Duration(index*2)*time.Second), publicKey)
		receipt.SourceRevision = qualificationTestRevision
		receipt.BuildRevision = qualificationTestRevision
		if index > 0 {
			receipt.PreviousSHA256 = qualificationSHA256(previousRaw)
		}
		if err := signAnchorQualificationReceipt(&receipt, privateKey); err != nil {
			t.Fatal(err)
		}
		phasePaths[index], previousRaw = writeQualificationFixture(t, phase+".json", receipt)
		phaseReceipts[index] = receipt
	}

	target := anchorHistoryWireValue{
		Exists: true, DatabaseIDHex: strings.Repeat("02", 16), MinimumCommitSequence: 10, MinimumGeneration: 20,
	}
	controller := anchorHistoryControllerRecord{
		SchemaVersion: anchorHistoryControllerSchema, RunID: strings.Repeat("cd", 16), ConfigurationID: strings.Repeat("e", 64),
		Operations: []anchorHistoryWireOperation{
			{ID: "ambiguous-advance", Kind: "advance", Outcome: "failed", Invoke: 1, Return: 2, Value: target},
			{ID: "observed-load", Kind: "load", Outcome: "succeeded", Invoke: 3, Return: 4, Value: target},
		},
	}
	agentPrivateKeys := attachAnchorHistoryTestAgentEvidenceWithKeys(t, &controller)
	historyPath, controllerRaw := writeQualificationFixture(t, "controller-history.json", controller)
	operations := append(append([]anchorHistoryWireOperation(nil), controller.Operations...), anchorHistoryWireOperation{
		ID: anchorHistoryFinalLoadID, Kind: "load", Outcome: "succeeded", Invoke: 5, Return: 6, Value: target,
	})
	history, err := anchorHistoryFromWire(controller.Initial, operations)
	if err != nil {
		t.Fatal(err)
	}
	check, err := historyqualification.CheckAnchorHistory(history)
	if err != nil || !check.Linearizable {
		t.Fatalf("history check=%+v err=%v", check, err)
	}
	members := []anchorQualificationMember{
		{MemberID: "member-a", EndpointSHA256: strings.Repeat("a", 64), State: string(anchorhttp.ReplicaAvailable), Exists: true, DatabaseIDHex: target.DatabaseIDHex, MinimumCommitSequence: target.MinimumCommitSequence, MinimumGeneration: target.MinimumGeneration},
		{MemberID: "member-b", EndpointSHA256: strings.Repeat("b", 64), State: string(anchorhttp.ReplicaAvailable), Exists: true, DatabaseIDHex: target.DatabaseIDHex, MinimumCommitSequence: target.MinimumCommitSequence, MinimumGeneration: target.MinimumGeneration},
		{MemberID: "member-c", EndpointSHA256: strings.Repeat("c", 64), State: string(anchorhttp.ReplicaMissing)},
	}
	historyReceipt := anchorHistoryQualificationReceipt{
		SchemaVersion: anchorHistoryReceiptSchema, ProtocolVersion: anchorhttp.ProtocolVersion,
		RunID: controller.RunID, ControllerSHA256: qualificationSHA256(controllerRaw), ExternalEvidenceSHA256: strings.Repeat("9", 64),
		SourceRevision: qualificationTestRevision, BuildRevision: qualificationTestRevision,
		GOOS: "linux", GOARCH: "amd64", GoVersion: "go1.25.0", StartedAt: now.Add(20 * time.Second), FinishedAt: now.Add(21 * time.Second),
		ConfigurationID: controller.ConfigurationID, Replicas: 3, Quorum: 2, Members: members,
		Initial: controller.Initial, Operations: operations, Check: check, Passed: true, SigningPublicKey: base64PublicKey(publicKey),
	}
	if err := signAnchorHistoryQualificationReceipt(&historyReceipt, privateKey); err != nil {
		t.Fatal(err)
	}
	historyReceiptPath, _ := writeQualificationFixture(t, "history-receipt.json", historyReceipt)
	return qualificationAnchorTestFixture{
		publicKeyPath: publicKeyPath, phasePaths: phasePaths, phaseReceipts: phaseReceipts,
		historyReceiptPath: historyReceiptPath, historyPath: historyPath, historyReceipt: historyReceipt, privateKey: privateKey,
		historyController: controller, agentPrivateKeys: agentPrivateKeys,
	}
}

func qualificationLevelFiveArguments(durabilityPath, soakPath, destructivePath string, artifacts qualificationArtifactTestFixture, anchor qualificationAnchorTestFixture) []string {
	arguments := []string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--destructive-record", destructivePath, "--environment-record", artifacts.environmentPath,
		"--artifacts-root", artifacts.rootPath, "--artifacts-index", artifacts.indexPath,
		"--source-revision", qualificationTestRevision, "--require-level", "5",
		"--anchor-public-key", anchor.publicKeyPath,
	}
	for _, path := range anchor.phasePaths {
		arguments = append(arguments, "--anchor-phase-receipt", path)
	}
	return append(arguments, "--anchor-history-receipt", anchor.historyReceiptPath, "--anchor-history", anchor.historyPath)
}

type qualificationArtifactTestFixture struct {
	rootPath             string
	indexPath            string
	indexSHA256          string
	environmentPath      string
	environment          qualificationEnvironmentEvidence
	environmentSHA256    string
	kernelAndMountSHA256 string
	controllerSHA256     string
	hostSHA256           string
}

func newQualificationArtifactTestFixture(t *testing.T, durability durabilityCheckResult, capturedAt time.Time) qualificationArtifactTestFixture {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "evidence.bin"), []byte("secured qualification evidence\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	environment, environmentPath, environmentRaw := qualificationEnvironmentFixture(t, root, durability, capturedAt, "ipmi-chassis-power-cycle")
	index, err := buildQualificationArtifactIndex(root, qualificationTestRevision)
	if err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(t.TempDir(), "artifacts-index.json")
	if err := writeJSONExclusiveDurable(indexPath, index); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	kernelAndMountSHA, err := qualificationEnvironmentSectionSHA256(struct {
		Volume destructiveVolumeReceipt    `json:"volume"`
		Kernel qualificationKernelEvidence `json:"kernel"`
		Mount  qualificationMountEvidence  `json:"mount"`
	}{environment.Volume, environment.Kernel, environment.Mount})
	if err != nil {
		t.Fatal(err)
	}
	controllerSHA, err := qualificationEnvironmentSectionSHA256(environment.Controller)
	if err != nil {
		t.Fatal(err)
	}
	hostSHA, err := qualificationEnvironmentSectionSHA256(environment.HostOperator)
	if err != nil {
		t.Fatal(err)
	}
	return qualificationArtifactTestFixture{
		rootPath: root, indexPath: indexPath, indexSHA256: qualificationSHA256(raw), environmentPath: environmentPath,
		environment: environment, environmentSHA256: qualificationSHA256(environmentRaw), kernelAndMountSHA256: kernelAndMountSHA,
		controllerSHA256: controllerSHA, hostSHA256: hostSHA,
	}
}

func qualificationEnvironmentFixture(t *testing.T, root string, durability durabilityCheckResult, capturedAt time.Time, method string) (qualificationEnvironmentEvidence, string, []byte) {
	t.Helper()
	operatorPath := filepath.Join(root, "operator-authorization.json")
	operatorRaw := []byte(fmt.Sprintf("{\"change\":\"approved destructive qualification\",\"evidenceRoot\":%q}\n", root))
	if err := os.WriteFile(operatorPath, operatorRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	volumeFacts := destructiveVolumeFacts{
		directory: durability.Directory, device: durability.Device, controlDevice: durability.Device + 1,
		filesystemType: durability.FilesystemType, filesystemName: durability.FilesystemName, blockSize: durability.BlockSize,
		totalBytes: durability.TotalBytes, availableBytes: durability.AvailableBytes,
	}
	environment := qualificationEnvironmentEvidence{
		SchemaVersion: qualificationEnvironmentSchema, SourceRevision: qualificationTestRevision, BuildRevision: qualificationTestRevision,
		CapturedAt: capturedAt, GOOS: durability.GOOS, GOARCH: durability.GOARCH, GoVersion: durability.GoVersion,
		Volume: destructiveVolumeReceipt{
			SchemaVersion: destructiveVolumeSchema, Eligible: true, Directory: durability.Directory, GOOS: "linux", Device: durability.Device,
			ControlDevice: durability.Device + 1, FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName,
			BlockSize: durability.BlockSize, TotalBytes: durability.TotalBytes, AvailableBytes: durability.AvailableBytes,
			ControlAvailableBytes: 1 << 30, DestructiveToken: destructiveVolumeToken(volumeFacts),
		},
		Kernel: qualificationKernelEvidence{
			Sysname: "Linux", Release: "6.12.0-test", Version: "qualification kernel", Machine: "x86_64",
			BootIDSHA256: strings.Repeat("a1", 32), CommandLineSHA256: strings.Repeat("a2", 32), OSReleaseSHA256: strings.Repeat("a3", 32),
		},
		Mount: qualificationMountEvidence{
			MountID: 101, ParentID: 1, MajorMinor: "8:1", Root: "/", MountPoint: durability.Directory,
			MountOptions: []string{"rw"}, Filesystem: "ext4", MountSource: "/dev/sda1", SuperOptions: []string{"rw"},
		},
		Controller: qualificationControllerEvidence{
			Method: method, SysfsRoot: "/sys",
			AttestationPublicKeySHA256: func() string {
				if qualificationPhysicalPowerMethod(method) {
					return qualificationSHA256(qualificationPhysicalControllerTestPrivateKey().Public().(ed25519.PublicKey))
				}
				return ""
			}(),
			PowerTargetIdentitySHA256: func() string {
				if qualificationPhysicalPowerMethod(method) {
					return strings.Repeat("71", 32)
				}
				return ""
			}(),
			BlockDevices: []qualificationBlockDeviceEvidence{{
				MajorMinor: "8:1", SysfsPath: "/sys/devices/pci0000:00/block/sda/sda1", SizeSectors: 2_097_152,
				LogicalBlockBytes: 512, PhysicalBlockBytes: 4096, WriteCache: "write back", FUA: "1", Rotational: "0",
				Scheduler: "[none] mq-deadline", DiscardGranularity: "4096", StableWrites: "0",
				DeviceModelSHA256: strings.Repeat("b1", 32), DMUUIDSHA256: strings.Repeat("b2", 32),
			}},
		},
		HostOperator: qualificationHostOperatorEvidence{
			EffectiveUID: 1000, HostIdentitySHA256: strings.Repeat("c1", 32), OperatorEvidencePath: operatorPath,
			OperatorEvidenceBytes: uint64(len(operatorRaw)), OperatorEvidenceSHA256: qualificationSHA256(operatorRaw),
		},
	}
	environmentPath := filepath.Join(root, "qualification-environment.json")
	if err := writeJSONExclusiveDurable(environmentPath, environment); err != nil {
		t.Fatal(err)
	}
	environmentRaw, err := os.ReadFile(environmentPath)
	if err != nil {
		t.Fatal(err)
	}
	return environment, environmentPath, environmentRaw
}

func indexOfArgumentValue(arguments []string, name string, ordinal int) int {
	seen := 0
	for index := 0; index+1 < len(arguments); index++ {
		if arguments[index] != name {
			continue
		}
		if seen == ordinal {
			return index + 1
		}
		seen++
	}
	return -1
}

func rewriteQualificationPhaseChain(t *testing.T, fixture qualificationAnchorTestFixture, mutate func(*anchorQualificationReceipt)) []string {
	t.Helper()
	paths := make([]string, len(fixture.phaseReceipts))
	var previousRaw []byte
	for index, original := range fixture.phaseReceipts {
		receipt := original
		mutate(&receipt)
		if index == 0 {
			receipt.PreviousSHA256 = ""
		} else {
			receipt.PreviousSHA256 = qualificationSHA256(previousRaw)
		}
		if err := signAnchorQualificationReceipt(&receipt, fixture.privateKey); err != nil {
			t.Fatal(err)
		}
		paths[index], previousRaw = writeQualificationFixture(t, fmt.Sprintf("rewritten-phase-%d.json", index), receipt)
	}
	return paths
}

func withQualificationDestructiveRecomputeBypass(t *testing.T) {
	t.Helper()
	previous := verifyQualificationDestructiveOriginalEvidence
	verifyQualificationDestructiveOriginalEvidence = func(qualificationDestructiveRecomputeInputs) error { return nil }
	t.Cleanup(func() { verifyQualificationDestructiveOriginalEvidence = previous })
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
