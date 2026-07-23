package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
	storage "github.com/crapthings/meldbase/internal/storage"
)

func TestDestructiveManifestBuildImportsAndReverifiesMachineReceipts(t *testing.T) {
	directory := t.TempDir()
	templatePath := filepath.Join(directory, "template.meld")
	if err := seedDestructiveCapacityDatabase(templatePath, []byte("old")); err != nil {
		t.Fatal(err)
	}
	templateRaw, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatal(err)
	}
	publicVerification, err := meldbase.VerifyFile(context.Background(), templatePath)
	if err != nil {
		t.Fatal(err)
	}
	rawVerification, err := verifyRawDestructiveArtifact(templatePath)
	if err != nil {
		t.Fatal(err)
	}

	durability, soak := qualificationFixtures()
	durability.Directory = directory
	soakShift := durability.FinishedAt.Add(time.Minute).Sub(soak.StartedAt)
	soak.StartedAt = soak.StartedAt.Add(soakShift)
	soak.FinishedAt = soak.FinishedAt.Add(soakShift)
	durabilityPath, _ := writeQualificationFixture(t, "durability.json", durability)
	soakPath, _ := writeQualificationFixture(t, "soak.json", soak)
	fixture, _ := qualificationDestructiveFixture(t, durability, soak, nil, nil)
	processStarted := soak.FinishedAt.Add(time.Minute)
	processFinished := processStarted.Add(40 * time.Minute)
	capacityStarted := processFinished.Add(time.Minute)
	capacityFinished := capacityStarted.Add(30 * time.Minute)
	corruptionStarted := capacityFinished.Add(time.Minute)
	corruptionFinished := corruptionStarted.Add(time.Minute)
	powerStarted := corruptionFinished.Add(time.Minute)
	processOrdinal, capacityOrdinal, powerOrdinal := 0, 0, 0
	for index := range fixture.Trials {
		switch fixture.Trials[index].Kind {
		case qualificationTrialProcess:
			fixture.Trials[index].StartedAt = processStarted.Add(time.Duration(processOrdinal*2) * time.Minute)
			fixture.Trials[index].FinishedAt = fixture.Trials[index].StartedAt.Add(time.Minute)
			processOrdinal++
		case qualificationTrialCapacity:
			fixture.Trials[index].StartedAt = capacityStarted.Add(time.Duration(capacityOrdinal*2) * time.Minute)
			fixture.Trials[index].FinishedAt = fixture.Trials[index].StartedAt.Add(time.Minute)
			capacityOrdinal++
		case qualificationTrialPower:
			step := qualificationSessionSteps()[5+powerOrdinal]
			fixture.Trials[index].ID = step.PowerTrialID
			fixture.Trials[index].StartedAt = powerStarted.Add(time.Duration(powerOrdinal*2) * time.Minute)
			fixture.Trials[index].FinishedAt = fixture.Trials[index].StartedAt.Add(time.Minute)
			powerOrdinal++
		}
		fixture.Trials[index].OldCommitSequence = 1
		fixture.Trials[index].NewCommitSequence = 2
		fixture.Trials[index].RecoveredSequence = 1
		fixture.Trials[index].Outcome = "old"
		fixture.Trials[index].OldStateSHA256 = bytesSHA256([]byte("old"))
		fixture.Trials[index].NewStateSHA256 = bytesSHA256([]byte("new"))
		fixture.Trials[index].RecoveredStateSHA256 = bytesSHA256([]byte("old"))
		fixture.Trials[index].DatabaseSHA256 = publicVerification.SHA256
	}

	process := destructiveProcessReceipt{
		SchemaVersion: destructiveProcessReceiptSchema, SourceRevision: qualificationTestRevision,
		BuildRevision: qualificationTestRevision, GOOS: durability.GOOS, GOARCH: durability.GOARCH, GoVersion: durability.GoVersion,
		Device: durability.Device, FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		StartedAt: processStarted, FinishedAt: processFinished, RequestedTrials: qualificationMinimumProcessTrials,
		CompletedTrials: qualificationMinimumProcessTrials,
	}
	capacity := destructiveENOSPCReceipt{
		SchemaVersion: destructiveENOSPCReceiptSchema, SourceRevision: qualificationTestRevision,
		BuildRevision: qualificationTestRevision, GOOS: durability.GOOS, GOARCH: durability.GOARCH, GoVersion: durability.GoVersion,
		Device: durability.Device, FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		StartedAt: capacityStarted, FinishedAt: capacityFinished, TrialsPerBoundary: qualificationMinimumBoundaryTrials,
	}
	var powerReceipts []string
	var firstPowerTrial qualificationDestructiveTrial
	for _, sourceTrial := range fixture.Trials {
		trial := sourceTrial
		switch trial.Kind {
		case qualificationTrialProcess:
			trialDirectory := filepath.Join(directory, trial.ID)
			if err := os.Mkdir(trialDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(trialDirectory, "crash-image.meld"), templateRaw, 0o600); err != nil {
				t.Fatal(err)
			}
			oracle := []byte(fmt.Sprintf("{\"trial\":%q}\n", trial.ID))
			if err := os.WriteFile(filepath.Join(trialDirectory, "oracle.jsonl"), oracle, 0o600); err != nil {
				t.Fatal(err)
			}
			trial.ArtifactsSHA256 = bytesSHA256(oracle)
			process.Trials = append(process.Trials, trial)
			process.TrialDirectories = append(process.TrialDirectories, trialDirectory)
			process.Verifications = append(process.Verifications, publicVerification)
		case qualificationTrialCapacity:
			artifact := filepath.Join(directory, trial.ID+"-capacity.meld")
			if err := os.WriteFile(artifact, templateRaw, 0o600); err != nil {
				t.Fatal(err)
			}
			marker := destructiveCapacityMarker{SchemaVersion: 1, Boundary: trial.PublicationBoundary, PID: 1234, ReachedAt: trial.StartedAt}
			markerPath, markerRaw := writeQualificationFixture(t, trial.ID+"-capacity-marker.json", marker)
			evidence := destructiveCapacityTrialEvidence{
				TrialID: trial.ID, Boundary: trial.PublicationBoundary, DatabaseArtifact: artifact,
				MarkerArtifact: markerPath, MarkerSHA256: qualificationSHA256(markerRaw), AllocatedBytes: 64 << 20, AvailableBytesBefore: 64 << 20,
				AvailableBytesAtENOSPC: 0, AvailableBytesAfter: 64 << 20, ENOSPCOperation: "fallocate",
				MetaGeneration: rawVerification.Meta.Generation, CommitSequence: rawVerification.Meta.CommitSequence,
				ValidMetaSlots: rawVerification.ValidMetaSlots, PhysicalPages: rawVerification.PhysicalPages,
				ReachablePages: rawVerification.ReachablePages, FreeSpaceValid: rawVerification.FreeSpaceValid,
			}
			evidenceRaw, err := json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			aggregate := sha256.New()
			_, _ = aggregate.Write(markerRaw)
			_, _ = aggregate.Write(evidenceRaw)
			_, _ = aggregate.Write(rawVerification.SHA256[:])
			trial.ArtifactsSHA256 = hex.EncodeToString(aggregate.Sum(nil))
			capacity.Trials = append(capacity.Trials, trial)
			capacity.CapacityEvidence = append(capacity.CapacityEvidence, evidence)
		case qualificationTrialPower:
			if firstPowerTrial.ID == "" {
				firstPowerTrial = trial
			}
			powerPath := buildPowerReceiptFixture(t, directory, trial, durability, templateRaw, rawVerification, "ipmi-chassis-power-cycle", trial.ID)
			powerReceipts = append(powerReceipts, powerPath)
		}
	}
	process.ArtifactsDirectory = directory
	processPath, _ := writeQualificationFixture(t, "process.json", process)
	capacity.ArtifactsDirectory = directory
	capacityPath, _ := writeQualificationFixture(t, "capacity.json", capacity)
	corruptionResult, err := executeDestructiveCorruptionCampaign(context.Background(), templatePath, 4)
	if err != nil {
		t.Fatal(err)
	}
	corruption := destructiveCorruptionReceipt{
		SchemaVersion: destructiveCorruptionReceiptSchema, SourceRevision: qualificationTestRevision,
		BuildRevision: qualificationTestRevision, GOOS: durability.GOOS, GOARCH: durability.GOARCH, GoVersion: durability.GoVersion,
		DatabaseArtifact: templatePath, DatabaseSHA256: corruptionResult.Baseline.SHA256, Baseline: corruptionResult.Baseline,
		SampledPages: corruptionResult.SampledPages, OffsetsWithinPage: append([]uint64(nil), destructiveCorruptionOffsets...),
		MutationCount: corruptionResult.MutationCount, DetectedCount: corruptionResult.DetectedCount,
		ValidOutcomeCount: corruptionResult.ValidOutcomeCount, ValidOutcomeBySeq: corruptionResult.ValidOutcomeBySeq,
		StartedAt: corruptionStarted, FinishedAt: corruptionFinished, Passed: true,
	}
	corruptionPath, _ := writeQualificationFixture(t, "corruption.json", corruption)
	matrixArgs := []string{}
	for _, path := range powerReceipts {
		matrixArgs = append(matrixArgs, "--receipt", path)
	}
	var matrixOutput bytes.Buffer
	if err := runDestructivePowerMatrixCheck(matrixArgs, &matrixOutput, &matrixOutput); err != nil {
		t.Fatalf("power matrix=%v output=%s", err, matrixOutput.String())
	}
	var matrix destructivePowerMatrixResult
	if err := json.Unmarshal(matrixOutput.Bytes(), &matrix); err != nil || !matrix.Passed || matrix.ReceiptCount != 15 || len(matrix.Coverage) != 5 || matrix.ControllerMethod != "ipmi-chassis-power-cycle" {
		t.Fatalf("matrix=%+v err=%v", matrix, err)
	}
	duplicateTrial := firstPowerTrial
	duplicateTrial.ID = "power-duplicate-boot-transition"
	duplicatePath := buildPowerReceiptFixture(t, directory, duplicateTrial, durability, templateRaw, rawVerification, "ipmi-chassis-power-cycle", firstPowerTrial.ID)
	duplicateArgs := append([]string(nil), matrixArgs...)
	duplicateArgs[len(duplicateArgs)-1] = duplicatePath
	var rejected bytes.Buffer
	if err := runDestructivePowerMatrixCheck(duplicateArgs, &rejected, &rejected); err == nil || !strings.Contains(err.Error(), "boot transition") {
		t.Fatalf("duplicate boot transition error=%v output=%s", err, rejected.String())
	}
	mixedTrial := firstPowerTrial
	mixedTrial.ID = "power-mixed-controller-method"
	mixedPath := buildPowerReceiptFixture(t, directory, mixedTrial, durability, templateRaw, rawVerification, "pdu-power-cycle", mixedTrial.ID)
	mixedArgs := append([]string(nil), matrixArgs...)
	mixedArgs[len(mixedArgs)-1] = mixedPath
	rejected.Reset()
	if err := runDestructivePowerMatrixCheck(mixedArgs, &rejected, &rejected); err == nil || !strings.Contains(err.Error(), "controller method") {
		t.Fatalf("mixed controller method error=%v output=%s", err, rejected.String())
	}
	environment, environmentPath, _ := qualificationEnvironmentFixture(t, directory, durability, durability.StartedAt.Add(-time.Minute), "ipmi-chassis-power-cycle")
	artifactsRoot := filepath.Dir(directory)
	sessionReceipts := []string{durabilityPath, soakPath, processPath, capacityPath, corruptionPath}
	sessionReceipts = append(sessionReceipts, powerReceipts...)
	writeQualificationSessionArtifactFixture(t, artifactsRoot, environmentPath, environment, durability, "linux-ext4-test", sessionReceipts)
	artifactIndex, err := buildQualificationArtifactIndex(artifactsRoot, qualificationTestRevision)
	if err != nil {
		t.Fatal(err)
	}
	outputDirectory, err := os.MkdirTemp("", "meldbase-manifest-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(outputDirectory) })
	artifactIndexPath := filepath.Join(outputDirectory, "artifacts-index.json")
	if err := writeJSONExclusiveDurable(artifactIndexPath, artifactIndex); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(outputDirectory, "destructive-manifest.json")
	args := []string{
		"destructive-manifest-build", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--process-receipt", processPath, "--capacity-receipt", capacityPath, "--corruption-receipt", corruptionPath,
		"--source-revision", qualificationTestRevision, "--platform-class", "linux-ext4-test",
		"--artifacts-root", artifactsRoot, "--artifacts-index", artifactIndexPath,
		"--environment-record", environmentPath,
		"--out", manifestPath,
	}
	for _, path := range powerReceipts {
		args = append(args, "--power-receipt", path)
	}
	mixedManifestArgs := append([]string(nil), args...)
	mixedManifestArgs[len(mixedManifestArgs)-1] = mixedPath
	rejected.Reset()
	if err := run(mixedManifestArgs, &rejected, &rejected); err == nil || !strings.Contains(err.Error(), "controller method") {
		t.Fatalf("mixed manifest controller method error=%v output=%s", err, rejected.String())
	}
	var stdout, stderr bytes.Buffer
	if err := run(args, &stdout, &stderr); err != nil {
		t.Fatalf("manifest build=%v stderr=%s", err, stderr.String())
	}
	var manifest qualificationDestructiveRecord
	if err := json.Unmarshal(stdout.Bytes(), &manifest); err != nil || manifest.SchemaVersion != 6 || !qualificationHexDigest(manifest.CorruptionReceiptSHA256) ||
		!qualificationHexDigest(manifest.SecuredArtifactsIndexSHA256) || !qualificationHexDigest(manifest.SessionPlanSHA256) ||
		!qualificationHexDigest(manifest.SessionHeadEventSHA256) || !qualificationHexDigest(manifest.SessionExecutableSHA256) ||
		len(manifest.Trials) != 50 || len(manifest.PowerReceiptSHA256) != 15 {
		t.Fatalf("manifest=%+v err=%v", manifest, err)
	}
	t.Run("session attacks", func(t *testing.T) {
		t.Run("missing event", func(t *testing.T) {
			root := copyQualificationArtifactTree(t, artifactsRoot)
			last := qualificationSessionSteps()[len(qualificationSessionSteps())-1]
			if err := os.Remove(filepath.Join(root, qualificationSessionDirectory, "events", qualificationSessionEventFilename(last))); err != nil {
				t.Fatal(err)
			}
			indexed := qualificationVerifiedIndexFixture(t, root)
			if _, err := verifyQualificationSessionArtifactJournal(indexed, durability, environment, manifest); err == nil || !strings.Contains(err.Error(), "incomplete") {
				t.Fatalf("missing event error=%v", err)
			}
		})
		t.Run("rewritten head", func(t *testing.T) {
			root := copyQualificationArtifactTree(t, artifactsRoot)
			last := qualificationSessionSteps()[len(qualificationSessionSteps())-1]
			eventPath := filepath.Join(root, qualificationSessionDirectory, "events", qualificationSessionEventFilename(last))
			var event qualificationSessionEvent
			if _, err := readQualificationReceipt(eventPath, &event); err != nil {
				t.Fatal(err)
			}
			event.ReceiptSHA256 = manifest.PowerReceiptSHA256[0]
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(eventPath, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			indexed := qualificationVerifiedIndexFixture(t, root)
			if _, err := verifyQualificationSessionArtifactJournal(indexed, durability, environment, manifest); err == nil || !strings.Contains(err.Error(), "receipt binding") {
				t.Fatalf("rewritten head error=%v", err)
			}
		})
		t.Run("replaced executable", func(t *testing.T) {
			root := copyQualificationArtifactTree(t, artifactsRoot)
			if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(qualificationSessionExecutable)), []byte("different executable"), 0o700); err != nil {
				t.Fatal(err)
			}
			indexed := qualificationVerifiedIndexFixture(t, root)
			if _, err := verifyQualificationSessionArtifactJournal(indexed, durability, environment, manifest); err == nil || !strings.Contains(err.Error(), "executable") {
				t.Fatalf("replaced executable error=%v", err)
			}
		})
		t.Run("forged manifest head", func(t *testing.T) {
			forged := manifest
			forged.SessionHeadEventSHA256 = strings.Repeat("ee", 32)
			if _, err := verifyQualificationSessionArtifactJournal(qualificationVerifiedIndexFixture(t, artifactsRoot), durability, environment, forged); err == nil || !strings.Contains(err.Error(), "binding differs") {
				t.Fatalf("forged manifest head error=%v", err)
			}
		})
	})
	var packet bytes.Buffer
	if err := run([]string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--destructive-record", manifestPath, "--environment-record", environmentPath,
		"--artifacts-root", artifactsRoot, "--artifacts-index", artifactIndexPath,
		"--source-revision", qualificationTestRevision, "--require-level", "4",
	}, &packet, &packet); err != nil {
		t.Fatalf("level4=%v output=%s", err, packet.String())
	}
	var result qualificationCheckResult
	if err := json.Unmarshal(packet.Bytes(), &result); err != nil || result.EvidenceLevel != 4 || !result.StorageQualified || result.RollbackProtectionQualified || result.ProductionQualified {
		t.Fatalf("packet=%+v err=%v", result, err)
	}
	relocatedRoot := copyQualificationArtifactTree(t, artifactsRoot)
	relocated := func(path string) string {
		relative, err := filepath.Rel(artifactsRoot, path)
		if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			t.Fatalf("cannot relocate %q from %q: relative=%q error=%v", path, artifactsRoot, relative, err)
		}
		return filepath.Join(relocatedRoot, relative)
	}
	packet.Reset()
	if err := run([]string{
		"qualification-check", "--durability-receipt", relocated(durabilityPath), "--soak-receipt", relocated(soakPath),
		"--destructive-record", manifestPath, "--environment-record", relocated(environmentPath),
		"--artifacts-root", relocatedRoot, "--artifacts-index", artifactIndexPath,
		"--source-revision", qualificationTestRevision, "--require-level", "4",
	}, &packet, &packet); err != nil {
		t.Fatalf("relocated Level 4 verification=%v output=%s", err, packet.String())
	}
	qualificationArgs := []string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--environment-record", environmentPath, "--artifacts-root", artifactsRoot, "--artifacts-index", artifactIndexPath,
		"--source-revision", qualificationTestRevision, "--require-level", "4",
	}
	forgedTime := manifest
	forgedTime.StartedAt = forgedTime.StartedAt.Add(-time.Second)
	forgedTimePath := filepath.Join(outputDirectory, "forged-time-manifest.json")
	if err := writeJSONExclusiveDurable(forgedTimePath, forgedTime); err != nil {
		t.Fatal(err)
	}
	forgedArgs := append(append([]string(nil), qualificationArgs...), "--destructive-record", forgedTimePath)
	if err := run(forgedArgs, &packet, &packet); err == nil || !strings.Contains(err.Error(), "differs from recomputed original receipts") {
		t.Fatalf("forged manifest timing error=%v", err)
	}
	forgedOrder := manifest
	forgedOrder.Trials = append([]qualificationDestructiveTrial(nil), manifest.Trials...)
	forgedOrder.Trials[0], forgedOrder.Trials[1] = forgedOrder.Trials[1], forgedOrder.Trials[0]
	forgedOrderPath := filepath.Join(outputDirectory, "forged-order-manifest.json")
	if err := writeJSONExclusiveDurable(forgedOrderPath, forgedOrder); err != nil {
		t.Fatal(err)
	}
	forgedArgs = append(append([]string(nil), qualificationArgs...), "--destructive-record", forgedOrderPath)
	if err := run(forgedArgs, &packet, &packet); err == nil || !strings.Contains(err.Error(), "differs from recomputed original receipts") {
		t.Fatalf("forged manifest trial order error=%v", err)
	}
	forgedReceipt := manifest
	forgedReceipt.ProcessReceiptSHA256 = manifest.PowerReceiptSHA256[0]
	forgedReceiptPath := filepath.Join(outputDirectory, "forged-receipt-manifest.json")
	if err := writeJSONExclusiveDurable(forgedReceiptPath, forgedReceipt); err != nil {
		t.Fatal(err)
	}
	forgedArgs = append(append([]string(nil), qualificationArgs...), "--destructive-record", forgedReceiptPath)
	if err := run(forgedArgs, &packet, &packet); err == nil || !strings.Contains(err.Error(), "indexed process receipt") {
		t.Fatalf("substituted destructive receipt error=%v", err)
	}
}

func writeQualificationSessionArtifactFixture(t *testing.T, root, environmentPath string, environment qualificationEnvironmentEvidence,
	durability durabilityCheckResult, platformClass string, receiptPaths []string) {
	t.Helper()
	steps := qualificationSessionSteps()
	if len(receiptPaths) != len(steps) {
		t.Fatalf("session receipt count=%d want=%d", len(receiptPaths), len(steps))
	}
	sessionDirectory := filepath.Join(root, qualificationSessionDirectory)
	if err := os.Mkdir(sessionDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(sessionDirectory, "events"), 0o700); err != nil {
		t.Fatal(err)
	}
	executableRaw := []byte("exact retained qualification executable fixture\n")
	executablePath := filepath.Join(root, filepath.FromSlash(qualificationSessionExecutable))
	if err := os.WriteFile(executablePath, executableRaw, 0o700); err != nil {
		t.Fatal(err)
	}
	environmentRelative, err := filepath.Rel(root, environmentPath)
	if err != nil {
		t.Fatal(err)
	}
	plan := qualificationSessionPlan{
		SchemaVersion: qualificationSessionPlanSchema, SessionID: strings.Repeat("ab", 16),
		SourceRevision: qualificationTestRevision, BuildRevision: qualificationTestRevision,
		CreatedAt: environment.CapturedAt.Add(30 * time.Second), GOOS: durability.GOOS, GOARCH: durability.GOARCH,
		GoVersion: durability.GoVersion, PlatformClass: platformClass, ArtifactsRoot: root,
		ExecutableRelativePath: qualificationSessionExecutable, ExecutableBytes: uint64(len(executableRaw)),
		ExecutableSHA256: qualificationSHA256(executableRaw), EnvironmentRelativePath: filepath.ToSlash(environmentRelative),
		EnvironmentSHA256: environmentRecordDigest(t, environmentPath), Volume: environment.Volume,
		ControllerMethod: environment.Controller.Method, ControllerPublicKeySHA256: environment.Controller.AttestationPublicKeySHA256, ControllerTargetIdentitySHA256: environment.Controller.PowerTargetIdentitySHA256, Steps: steps,
	}
	planPath := filepath.Join(sessionDirectory, "plan.json")
	if err := writeJSONExclusiveDurable(planPath, plan); err != nil {
		t.Fatal(err)
	}
	planRaw, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	previousEventSHA256 := ""
	for index, step := range steps {
		receiptRaw, err := os.ReadFile(receiptPaths[index])
		if err != nil {
			t.Fatal(err)
		}
		relative, err := filepath.Rel(root, receiptPaths[index])
		if err != nil {
			t.Fatal(err)
		}
		_, finishedAt, err := qualificationSessionArtifactReceiptTime(step, receiptPaths[index])
		if err != nil {
			t.Fatal(err)
		}
		event := qualificationSessionEvent{
			SchemaVersion: qualificationSessionEventSchema, SessionID: plan.SessionID, PlanSHA256: qualificationSHA256(planRaw),
			Ordinal: step.Ordinal, StepID: step.ID, Kind: step.Kind, ReceiptRelativePath: filepath.ToSlash(relative),
			ReceiptSHA256: qualificationSHA256(receiptRaw), PreviousEventSHA256: previousEventSHA256,
			RecordedAt: finishedAt.Add(30 * time.Second),
		}
		eventPath := filepath.Join(sessionDirectory, "events", qualificationSessionEventFilename(step))
		if err := writeJSONExclusiveDurable(eventPath, event); err != nil {
			t.Fatal(err)
		}
		eventRaw, err := os.ReadFile(eventPath)
		if err != nil {
			t.Fatal(err)
		}
		previousEventSHA256 = qualificationSHA256(eventRaw)
	}
}

func environmentRecordDigest(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return qualificationSHA256(raw)
}

func qualificationVerifiedIndexFixture(t *testing.T, root string) verifiedQualificationArtifactIndex {
	t.Helper()
	index, err := buildQualificationArtifactIndex(root, qualificationTestRevision)
	if err != nil {
		t.Fatal(err)
	}
	entries := make(map[string]qualificationArtifactEntry, len(index.Entries))
	for _, entry := range index.Entries {
		entries[entry.Path] = entry
	}
	return verifiedQualificationArtifactIndex{Root: root, Index: index, Entries: entries}
}

func copyQualificationArtifactTree(t *testing.T, source string) string {
	t.Helper()
	destination, err := os.MkdirTemp("", "meldbase-relocated-artifacts-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(destination) })
	err = filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil || relative == "." {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.Mkdir(target, 0o700)
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() && entry.Type() != 0 {
			return fmt.Errorf("unexpected artifact type %q", path)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			_ = input.Close()
			return err
		}
		_, copyErr := io.Copy(output, input)
		return errors.Join(copyErr, input.Close(), output.Close())
	})
	if err != nil {
		t.Fatal(err)
	}
	return destination
}

func buildPowerReceiptFixture(t *testing.T, directory string, trial qualificationDestructiveTrial, durability durabilityCheckResult, databaseRaw []byte, verified storage.VerificationResult, method, bootNonce string) string {
	t.Helper()
	databasePath := filepath.Join(directory, trial.ID+"-power.meld")
	if err := os.WriteFile(databasePath, databaseRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	marker := destructivePowerMarker{
		SchemaVersion: destructivePowerMarkerSchema, TrialID: trial.ID, Boundary: trial.PublicationBoundary,
		SourceRevision: qualificationTestRevision, BuildRevision: qualificationTestRevision,
		GOOS: durability.GOOS, GOARCH: durability.GOARCH, Device: durability.Device,
		FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		DatabaseRelative: filepath.Join(".meldbase-power-"+trial.ID, "power.meld"),
		BootIDBefore:     bytesSHA256([]byte("before:" + bootNonce)), StartedAt: trial.StartedAt, ReachedAt: trial.StartedAt.Add(time.Second),
		OldCommitSequence: 1, NewCommitSequence: 2, OldStateSHA256: trial.OldStateSHA256, NewStateSHA256: trial.NewStateSHA256,
	}
	markerPath, markerRaw := writeQualificationFixture(t, trial.ID+"-marker.json", marker)
	privateKey := qualificationPhysicalControllerTestPrivateKey()
	publicKey := privateKey.Public().(ed25519.PublicKey)
	runID := bytesSHA256([]byte("run:" + bootNonce))[:32]
	requestedAt := marker.ReachedAt.Add(time.Second)
	request := destructivePowerAdapterRequest{SchemaVersion: 1, ControllerRunID: runID, TrialID: trial.ID, Method: method, MarkerSHA256: qualificationSHA256(markerRaw), BootIDBefore: marker.BootIDBefore, TargetIdentitySHA256: strings.Repeat("71", 32), RequestedAt: requestedAt}
	hardwareEvidence := []byte("hardware evidence for " + trial.ID)
	response := destructivePowerAdapterResponse{SchemaVersion: 1, ControllerRunID: runID, TrialID: trial.ID, Method: method, OperationID: "operation-" + trial.ID, TargetIdentitySHA256: strings.Repeat("71", 32), AcceptedAt: requestedAt.Add(time.Second), PowerLostAt: requestedAt.Add(2 * time.Second), PowerRestoredAt: requestedAt.Add(3 * time.Second), HardwareEvidenceSHA256: qualificationSHA256(hardwareEvidence), HardwareEvidenceBase64: base64.StdEncoding.EncodeToString(hardwareEvidence), Success: true}
	proof := destructivePowerControllerProof{SchemaVersion: 1, SourceRevision: qualificationTestRevision, BuildRevision: qualificationTestRevision, GOOS: "linux", GOARCH: "amd64", GoVersion: "go-test", AdapterSHA256: strings.Repeat("73", 32), AdapterStderrSHA256: qualificationSHA256(nil), StartedAt: requestedAt, FinishedAt: requestedAt.Add(4 * time.Second), Request: request, Response: response}
	controllerProofPath, controllerProofRaw := writeQualificationFixture(t, trial.ID+"-controller-proof.json", proof)
	controller := destructivePowerControllerEvent{
		SchemaVersion: destructivePhysicalPowerEventSchema, TrialID: trial.ID, Method: method,
		MarkerSHA256: qualificationSHA256(markerRaw), BootIDBefore: marker.BootIDBefore,
		MarkerObservedAt: request.RequestedAt, CutRequestedAt: response.AcceptedAt,
		PowerRestoredAt:       response.PowerRestoredAt,
		ControllerProofSHA256: qualificationSHA256(controllerProofRaw),
		ControllerRunID:       runID, ControllerPublicKey: base64.StdEncoding.EncodeToString(publicKey),
	}
	if err := signDestructivePowerControllerEvent(&controller, privateKey); err != nil {
		t.Fatal(err)
	}
	controllerPath, controllerRaw := writeQualificationFixture(t, trial.ID+"-controller.json", controller)
	evidence := destructivePowerEvidence{
		TrialID: trial.ID, Boundary: trial.PublicationBoundary, Method: controller.Method,
		BootIDBefore: marker.BootIDBefore, BootIDAfter: bytesSHA256([]byte("after:" + bootNonce)),
		MarkerSHA256: qualificationSHA256(markerRaw), ControllerSHA256: qualificationSHA256(controllerRaw),
		ControllerProofSHA256: qualificationSHA256(controllerProofRaw), MarkerArtifact: markerPath,
		ControllerPublicKeySHA256:      qualificationSHA256(publicKey),
		ControllerTargetIdentitySHA256: response.TargetIdentitySHA256,
		ControllerArtifact:             controllerPath, ControllerProofArtifact: controllerProofPath, DatabaseArtifact: databasePath,
		MetaGeneration: verified.Meta.Generation, CommitSequence: verified.Meta.CommitSequence,
		ValidMetaSlots: verified.ValidMetaSlots, PhysicalPages: verified.PhysicalPages,
		ReachablePages: verified.ReachablePages, FreeSpaceValid: verified.FreeSpaceValid,
	}
	evidenceRaw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	artifactHash := sha256.New()
	_, _ = artifactHash.Write(markerRaw)
	_, _ = artifactHash.Write(controllerRaw)
	_, _ = artifactHash.Write(controllerProofRaw)
	_, _ = artifactHash.Write(evidenceRaw)
	_, _ = artifactHash.Write(verified.SHA256[:])
	trial.ArtifactsSHA256 = hex.EncodeToString(artifactHash.Sum(nil))
	trial.DatabaseSHA256 = hex.EncodeToString(verified.SHA256[:])
	receipt := destructivePowerReceipt{
		SchemaVersion: destructivePowerReceiptSchema, SourceRevision: qualificationTestRevision,
		BuildRevision: qualificationTestRevision, GOOS: durability.GOOS, GOARCH: durability.GOARCH, GoVersion: durability.GoVersion,
		Device: durability.Device, FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName,
		BlockSize: durability.BlockSize, Trial: trial, Evidence: evidence,
	}
	path, _ := writeQualificationFixture(t, trial.ID+"-power.json", receipt)
	return path
}
