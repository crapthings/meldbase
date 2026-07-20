package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/crapthings/meldbase/core"
)

type destructivePowerReceiptFlags []string

func (values *destructivePowerReceiptFlags) String() string { return fmt.Sprint([]string(*values)) }
func (values *destructivePowerReceiptFlags) Set(value string) error {
	if value == "" {
		return errors.New("empty power receipt path")
	}
	*values = append(*values, value)
	return nil
}

func runDestructiveManifestBuild(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-manifest-build", flag.ContinueOnError)
	flags.SetOutput(stderr)
	durabilityPath := flags.String("durability-receipt", "", "schema-2 durability receipt")
	soakPath := flags.String("soak-receipt", "", "schema-4 release soak receipt")
	processPath := flags.String("process-receipt", "", "schema-2 process-kill receipt")
	capacityPath := flags.String("capacity-receipt", "", "schema-1 real ENOSPC receipt")
	corruptionPath := flags.String("corruption-receipt", "", "schema-1 reproducible silent-corruption receipt")
	var powerPaths destructivePowerReceiptFlags
	flags.Var(&powerPaths, "power-receipt", "schema-1 power recovery receipt; repeat for every trial")
	sourceRevision := flags.String("source-revision", "", "exact 40- or 64-hex release revision")
	platformClass := flags.String("platform-class", "", "bounded public platform class")
	artifactsRootPath := flags.String("artifacts-root", "", "quiescent complete secured artifact directory")
	artifactsIndexPath := flags.String("artifacts-index", "", "machine-generated schema-1 index for the artifact root")
	environmentRecordPath := flags.String("environment-record", "", "machine-generated qualification environment evidence inside the artifact root")
	output := flags.String("out", "", "new schema-6 destructive manifest path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *durabilityPath == "" || *soakPath == "" || *processPath == "" || *capacityPath == "" || *corruptionPath == "" || len(powerPaths) == 0 ||
		!validDurabilitySourceRevision(*sourceRevision) || !qualificationSafeName(*platformClass, 128) || *artifactsRootPath == "" || *artifactsIndexPath == "" ||
		*environmentRecordPath == "" || *output == "" {
		return errors.New("destructive-manifest-build requires all receipts, secured artifact root/index, environment record, source revision, platform class and --out")
	}
	var durability durabilityCheckResult
	durabilityRaw, err := readQualificationReceipt(*durabilityPath, &durability)
	if err != nil {
		return fmt.Errorf("durability receipt: %w", err)
	}
	if err := validateQualificationDurability(durability, *sourceRevision); err != nil {
		return fmt.Errorf("durability receipt: %w", err)
	}
	var soak qualificationSoakReceipt
	soakRaw, err := readQualificationReceipt(*soakPath, &soak)
	if err != nil {
		return fmt.Errorf("soak receipt: %w", err)
	}
	if err := validateQualificationSoak(soak, durability, *sourceRevision); err != nil {
		return fmt.Errorf("soak receipt: %w", err)
	}
	var process destructiveProcessReceipt
	processRaw, err := readQualificationReceipt(*processPath, &process)
	if err != nil {
		return fmt.Errorf("process receipt: %w", err)
	}
	if err := validateDestructiveProcessReceipt(process); err != nil {
		return fmt.Errorf("process receipt: %w", err)
	}
	var capacity destructiveENOSPCReceipt
	capacityRaw, err := readQualificationReceipt(*capacityPath, &capacity)
	if err != nil {
		return fmt.Errorf("capacity receipt: %w", err)
	}
	if err := validateDestructiveENOSPCReceipt(capacity); err != nil {
		return fmt.Errorf("capacity receipt: %w", err)
	}
	var corruption destructiveCorruptionReceipt
	corruptionRaw, err := readQualificationReceipt(*corruptionPath, &corruption)
	if err != nil {
		return fmt.Errorf("corruption receipt: %w", err)
	}
	if err := validateDestructiveCorruptionReceipt(corruption); err != nil {
		return fmt.Errorf("corruption receipt: %w", err)
	}
	if corruption.SourceRevision != *sourceRevision || corruption.BuildRevision != *sourceRevision || corruption.BuildModified ||
		corruption.GOOS != durability.GOOS || corruption.GOARCH != durability.GOARCH || corruption.GoVersion != durability.GoVersion {
		return errors.New("corruption receipt requires a clean exact-revision receipt from the same runtime")
	}
	if err := recheckDestructiveCorruptionReceipt(corruption); err != nil {
		return fmt.Errorf("corruption receipt: %w", err)
	}
	if err := validateDestructiveReceiptIdentity(*sourceRevision, durability, process.SourceRevision, process.BuildRevision, process.BuildModified,
		process.GOOS, process.GOARCH, process.GoVersion, process.Device, process.FilesystemType, process.FilesystemName, process.BlockSize); err != nil {
		return fmt.Errorf("process receipt: %w", err)
	}
	if err := validateDestructiveReceiptIdentity(*sourceRevision, durability, capacity.SourceRevision, capacity.BuildRevision, capacity.BuildModified,
		capacity.GOOS, capacity.GOARCH, capacity.GoVersion, capacity.Device, capacity.FilesystemType, capacity.FilesystemName, capacity.BlockSize); err != nil {
		return fmt.Errorf("capacity receipt: %w", err)
	}
	var environment qualificationEnvironmentEvidence
	environmentRaw, err := readQualificationReceipt(*environmentRecordPath, &environment)
	if err != nil {
		return fmt.Errorf("qualification environment: %w", err)
	}
	artifacts, err := verifyQualificationArtifactIndex(*artifactsRootPath, *artifactsIndexPath, *sourceRevision)
	if err != nil {
		return fmt.Errorf("secured artifacts: %w", err)
	}
	requiredArtifacts := []string{*durabilityPath, *soakPath, *processPath, *capacityPath, *corruptionPath, *environmentRecordPath,
		environment.HostOperator.OperatorEvidencePath, corruption.DatabaseArtifact}
	for _, directory := range process.TrialDirectories {
		requiredArtifacts = append(requiredArtifacts, filepath.Join(directory, "crash-image.meld"), filepath.Join(directory, "oracle.jsonl"))
	}
	for _, evidence := range capacity.CapacityEvidence {
		requiredArtifacts = append(requiredArtifacts, evidence.DatabaseArtifact, evidence.MarkerArtifact)
	}
	if err := requireQualificationArtifactPaths(artifacts, requiredArtifacts...); err != nil {
		return err
	}
	operatorEntry, err := qualificationArtifactEntryForPath(artifacts, environment.HostOperator.OperatorEvidencePath)
	if err != nil {
		return err
	}
	if operatorEntry.SHA256 != environment.HostOperator.OperatorEvidenceSHA256 || operatorEntry.Bytes != environment.HostOperator.OperatorEvidenceBytes {
		return errors.New("qualification environment operator evidence differs from the secured artifact index")
	}
	kernelAndMountSHA, err := qualificationEnvironmentSectionSHA256(struct {
		Volume destructiveVolumeReceipt    `json:"volume"`
		Kernel qualificationKernelEvidence `json:"kernel"`
		Mount  qualificationMountEvidence  `json:"mount"`
	}{environment.Volume, environment.Kernel, environment.Mount})
	if err != nil {
		return err
	}
	controllerSHA, err := qualificationEnvironmentSectionSHA256(environment.Controller)
	if err != nil {
		return err
	}
	hostSHA, err := qualificationEnvironmentSectionSHA256(environment.HostOperator)
	if err != nil {
		return err
	}
	record := qualificationDestructiveRecord{
		SchemaVersion: 6, SourceRevision: *sourceRevision, PlatformClass: *platformClass,
		GOOS: durability.GOOS, GOARCH: durability.GOARCH, Device: durability.Device,
		FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		DurabilityReceiptSHA256: qualificationSHA256(durabilityRaw), SoakReceiptSHA256: qualificationSHA256(soakRaw),
		ProcessReceiptSHA256: qualificationSHA256(processRaw), CapacityReceiptSHA256: qualificationSHA256(capacityRaw),
		CorruptionReceiptSHA256: qualificationSHA256(corruptionRaw),
		Infrastructure: qualificationInfrastructure{
			EnvironmentRecordSHA256: qualificationSHA256(environmentRaw), KernelAndMountSHA256: kernelAndMountSHA,
			ControllerPolicySHA256: controllerSHA, HostAndOperatorSHA256: hostSHA, ControllerMethod: environment.Controller.Method,
		},
		SecuredArtifactsIndexSHA256: qualificationSHA256(artifacts.Raw),
	}
	record.Trials = append(record.Trials, process.Trials...)
	record.Trials = append(record.Trials, capacity.Trials...)
	record.StartedAt, record.FinishedAt = process.StartedAt, process.FinishedAt
	if capacity.StartedAt.Before(record.StartedAt) {
		record.StartedAt = capacity.StartedAt
	}
	if capacity.FinishedAt.After(record.FinishedAt) {
		record.FinishedAt = capacity.FinishedAt
	}
	if corruption.StartedAt.Before(record.StartedAt) {
		record.StartedAt = corruption.StartedAt
	}
	if corruption.FinishedAt.After(record.FinishedAt) {
		record.FinishedAt = corruption.FinishedAt
	}
	var powerMethod string
	seenBootTransitions := make(map[string]struct{}, len(powerPaths))
	for index, path := range powerPaths {
		var power destructivePowerReceipt
		raw, err := readQualificationReceipt(path, &power)
		if err != nil {
			return fmt.Errorf("power receipt %d: %w", index+1, err)
		}
		if err := validateDestructivePowerReceipt(power); err != nil {
			return fmt.Errorf("power receipt %d: %w", index+1, err)
		}
		requiredArtifacts = append(requiredArtifacts, path, power.Evidence.MarkerArtifact, power.Evidence.ControllerArtifact,
			power.Evidence.ControllerProofArtifact, power.Evidence.DatabaseArtifact)
		if err := validateDestructiveReceiptIdentity(*sourceRevision, durability, power.SourceRevision, power.BuildRevision, power.BuildModified,
			power.GOOS, power.GOARCH, power.GoVersion, power.Device, power.FilesystemType, power.FilesystemName, power.BlockSize); err != nil {
			return fmt.Errorf("power receipt %d: %w", index+1, err)
		}
		if index == 0 {
			powerMethod = power.Evidence.Method
		} else if power.Evidence.Method != powerMethod {
			return fmt.Errorf("power receipt %d uses controller method %q instead of %q", index+1, power.Evidence.Method, powerMethod)
		}
		if power.Evidence.ControllerPublicKeySHA256 != environment.Controller.AttestationPublicKeySHA256 {
			return fmt.Errorf("power receipt %d controller attestation key differs from the environment", index+1)
		}
		if power.Evidence.ControllerTargetIdentitySHA256 != environment.Controller.PowerTargetIdentitySHA256 {
			return fmt.Errorf("power receipt %d controller target identity differs from the environment", index+1)
		}
		bootTransition := power.Evidence.BootIDBefore + "\x00" + power.Evidence.BootIDAfter
		if _, exists := seenBootTransitions[bootTransition]; exists {
			return fmt.Errorf("power receipt %d duplicates a boot transition", index+1)
		}
		seenBootTransitions[bootTransition] = struct{}{}
		record.PowerReceiptSHA256 = append(record.PowerReceiptSHA256, qualificationSHA256(raw))
		record.Trials = append(record.Trials, power.Trial)
		if power.Trial.StartedAt.Before(record.StartedAt) {
			record.StartedAt = power.Trial.StartedAt
		}
		if power.Trial.FinishedAt.After(record.FinishedAt) {
			record.FinishedAt = power.Trial.FinishedAt
		}
	}
	if err := validateQualificationEnvironmentAgainstDurability(environment, durability, *sourceRevision, powerMethod, record.StartedAt); err != nil {
		return fmt.Errorf("qualification environment: %w", err)
	}
	artifacts, err = verifyQualificationArtifactIndex(*artifactsRootPath, *artifactsIndexPath, *sourceRevision)
	if err != nil {
		return fmt.Errorf("secured artifacts changed during manifest assembly: %w", err)
	}
	if err := requireQualificationArtifactPaths(artifacts, requiredArtifacts...); err != nil {
		return err
	}
	record.SecuredArtifactsIndexSHA256 = qualificationSHA256(artifacts.Raw)
	session, err := verifyQualificationSessionArtifactJournal(artifacts, durability, environment, record)
	if err != nil {
		return fmt.Errorf("qualification session: %w", err)
	}
	record.SessionPlanSHA256 = session.PlanSHA256
	record.SessionHeadEventSHA256 = session.HeadEventSHA256
	record.SessionExecutableSHA256 = session.ExecutableSHA256
	packet := qualificationCheckResult{
		SourceRevision: *sourceRevision, GOOS: durability.GOOS, GOARCH: durability.GOARCH, Device: durability.Device,
		FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		DurabilityReceiptSHA256: record.DurabilityReceiptSHA256, SoakReceiptSHA256: record.SoakReceiptSHA256,
	}
	if err := validateQualificationDestructive(record, packet); err != nil {
		return err
	}
	recomputed, err := recomputeQualificationDestructiveRecord(qualificationDestructiveRecomputeInputs{
		Record: record, Artifacts: artifacts, Durability: durability, DurabilityRaw: durabilityRaw, SoakRaw: soakRaw,
		Environment: environment, EnvironmentRaw: environmentRaw, EnvironmentPath: *environmentRecordPath, Revision: *sourceRevision,
	})
	if err != nil {
		return fmt.Errorf("recompute destructive manifest before publication: %w", err)
	}
	if !reflect.DeepEqual(recomputed, record) {
		return errors.New("destructive manifest assembly differs from its original evidence recomputation")
	}
	cleanOutput, err := filepath.Abs(filepath.Clean(*output))
	if err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(cleanOutput, record); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(record)
}

func validateDestructiveReceiptIdentity(revision string, durability durabilityCheckResult, source, build string, modified bool,
	goos, goarch, goVersion string, device uint64, filesystemType, filesystemName string, blockSize uint64) error {
	if source != revision || build != revision || modified || goos != durability.GOOS || goarch != durability.GOARCH ||
		goVersion != durability.GoVersion || device != durability.Device || filesystemType != durability.FilesystemType ||
		filesystemName != durability.FilesystemName || blockSize != durability.BlockSize {
		return errors.New("requires a clean exact-revision receipt from the same runtime and target volume")
	}
	return nil
}

func validateDestructiveProcessReceipt(receipt destructiveProcessReceipt) error {
	if receipt.SchemaVersion != destructiveProcessReceiptSchema || receipt.RequestedTrials < qualificationMinimumProcessTrials ||
		receipt.CompletedTrials != receipt.RequestedTrials || len(receipt.Trials) != receipt.RequestedTrials ||
		len(receipt.Verifications) != receipt.RequestedTrials || len(receipt.TrialDirectories) != receipt.RequestedTrials ||
		receipt.StartedAt.IsZero() || !receipt.FinishedAt.After(receipt.StartedAt) || !filepath.IsAbs(receipt.ArtifactsDirectory) {
		return errors.New("process receipt identity, timing or trial count is invalid")
	}
	seen := make(map[string]struct{}, len(receipt.Trials))
	seenDirectories := make(map[string]struct{}, len(receipt.Trials))
	triggers := make(map[string]int, 2)
	for index, trial := range receipt.Trials {
		verification := receipt.Verifications[index]
		if _, exists := seen[trial.ID]; exists {
			return fmt.Errorf("process trial %d duplicates id %q", index+1, trial.ID)
		}
		seen[trial.ID] = struct{}{}
		directory := filepath.Clean(receipt.TrialDirectories[index])
		relative, err := filepath.Rel(filepath.Clean(receipt.ArtifactsDirectory), directory)
		if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("process trial %d artifact directory escapes its receipt root", index+1)
		}
		if _, exists := seenDirectories[directory]; exists {
			return fmt.Errorf("process trial %d reuses an artifact directory", index+1)
		}
		seenDirectories[directory] = struct{}{}
		if trial.Kind != qualificationTrialProcess || trial.PublicationBoundary != qualificationAsyncBoundary ||
			(trial.TriggerPoint != "oracle-prepared" && trial.TriggerPoint != "oracle-committed") ||
			trial.StartedAt.Before(receipt.StartedAt) || trial.FinishedAt.After(receipt.FinishedAt) ||
			verification.SchemaVersion != 3 || !verification.Verified || !verification.IndexContentsVerified ||
			!verification.IndexBuildContentsVerified || verification.CommitSequence != trial.RecoveredSequence || verification.SHA256 != trial.DatabaseSHA256 {
			return fmt.Errorf("process trial %d has incomplete recovery evidence", index+1)
		}
		triggers[trial.TriggerPoint]++
		databaseHash, err := hashRegularFile(filepath.Join(directory, "crash-image.meld"), 1<<30)
		if err != nil || databaseHash != trial.DatabaseSHA256 {
			return fmt.Errorf("process trial %d crash image is missing or mismatched", index+1)
		}
		actualVerification, err := meldbase.VerifyV2File(context.Background(), filepath.Join(directory, "crash-image.meld"))
		if err != nil || actualVerification != verification {
			return fmt.Errorf("process trial %d crash image does not reproduce its verification report", index+1)
		}
		oracleHash, err := hashRegularFile(filepath.Join(directory, "oracle.jsonl"), 16<<20)
		if err != nil || oracleHash != trial.ArtifactsSHA256 {
			return fmt.Errorf("process trial %d oracle is missing or mismatched", index+1)
		}
	}
	if triggers["oracle-prepared"] < qualificationMinimumProcessTrials/2 || triggers["oracle-committed"] < qualificationMinimumProcessTrials/2 {
		return errors.New("process receipt lacks prepared/committed trigger balance")
	}
	return nil
}

func validateDestructivePowerReceipt(receipt destructivePowerReceipt) error {
	return validateDestructivePowerReceiptWithCanonicalEvidence(receipt, receipt.Evidence)
}

func validateDestructivePowerReceiptWithCanonicalEvidence(receipt destructivePowerReceipt, canonicalEvidence destructivePowerEvidence) error {
	trial, evidence := receipt.Trial, receipt.Evidence
	if receipt.SchemaVersion != destructivePowerReceiptSchema || trial.Kind != qualificationTrialPower ||
		trial.TriggerPoint != "external-power-cut" || evidence.TrialID != trial.ID || evidence.Boundary != trial.PublicationBoundary ||
		evidence.CommitSequence != trial.RecoveredSequence || evidence.BootIDBefore == evidence.BootIDAfter ||
		!qualificationPowerMethod(evidence.Method) || !qualificationHexDigest(evidence.ControllerProofSHA256) ||
		evidence.ControllerProofSHA256 == "" || !trial.LockReacquired || !trial.OfflineVerified ||
		evidence.ValidMetaSlots < 1 || evidence.PhysicalPages == 0 || evidence.ReachablePages == 0 ||
		evidence.ReachablePages > evidence.PhysicalPages || !evidence.FreeSpaceValid {
		return errors.New("power receipt identity, reset or recovery evidence is incomplete")
	}
	for path, want := range map[string]string{
		evidence.MarkerArtifact: evidence.MarkerSHA256, evidence.ControllerArtifact: evidence.ControllerSHA256,
		evidence.ControllerProofArtifact: evidence.ControllerProofSHA256, evidence.DatabaseArtifact: trial.DatabaseSHA256,
	} {
		actual, err := hashRegularFile(path, 1<<30)
		if err != nil || actual != want {
			return fmt.Errorf("power artifact %q is missing or mismatched", path)
		}
	}
	var marker destructivePowerMarker
	markerRaw, err := readQualificationReceipt(evidence.MarkerArtifact, &marker)
	if err != nil || qualificationSHA256(markerRaw) != evidence.MarkerSHA256 || marker.TrialID != trial.ID || marker.Boundary != trial.PublicationBoundary {
		return errors.New("power marker artifact does not reproduce the trial")
	}
	var controller destructivePowerControllerEvent
	controllerRaw, err := readQualificationReceipt(evidence.ControllerArtifact, &controller)
	if err != nil || qualificationSHA256(controllerRaw) != evidence.ControllerSHA256 || controller.TrialID != trial.ID ||
		controller.MarkerSHA256 != evidence.MarkerSHA256 || controller.Method != evidence.Method ||
		controller.BootIDBefore != evidence.BootIDBefore || controller.MarkerObservedAt.IsZero() ||
		controller.CutRequestedAt.Before(controller.MarkerObservedAt) || !controller.PowerRestoredAt.After(controller.CutRequestedAt) ||
		controller.ControllerProofSHA256 != evidence.ControllerProofSHA256 {
		return errors.New("power controller artifact does not reproduce the trial")
	}
	controllerProofRaw, err := os.ReadFile(evidence.ControllerProofArtifact)
	if err != nil || qualificationSHA256(controllerProofRaw) != evidence.ControllerProofSHA256 {
		return errors.New("power controller proof artifact does not reproduce the trial")
	}
	if controller.Method == "qemu-system-reset" {
		if err := validateDestructiveQMPProof(controllerProofRaw, controller, markerRaw); err != nil {
			return fmt.Errorf("power QMP proof: %w", err)
		}
	} else if controller.Method == "qemu-host-sigkill" {
		if err := validateDestructiveQMPKillProof(controllerProofRaw, controller, markerRaw); err != nil {
			return fmt.Errorf("power QEMU process-kill proof: %w", err)
		}
	} else {
		publicRaw, decodeErr := base64.StdEncoding.Strict().DecodeString(controller.ControllerPublicKey)
		if decodeErr != nil || len(publicRaw) != ed25519.PublicKeySize || qualificationSHA256(publicRaw) != evidence.ControllerPublicKeySHA256 {
			return errors.New("physical power receipt controller key is malformed or differs from its evidence binding")
		}
		if err := verifyDestructivePhysicalPowerEvidence(controllerProofRaw, controller, markerRaw, ed25519.PublicKey(publicRaw)); err != nil {
			return fmt.Errorf("physical power controller proof: %w", err)
		}
		var physicalProof destructivePowerControllerProof
		if err := decodeOneStrictJSON(controllerProofRaw, &physicalProof); err != nil || physicalProof.Response.TargetIdentitySHA256 != evidence.ControllerTargetIdentitySHA256 {
			return errors.New("physical power controller target identity differs from the receipt")
		}
	}
	if qualificationPhysicalPowerMethod(controller.Method) != qualificationHexDigest(evidence.ControllerPublicKeySHA256) {
		return errors.New("power receipt physical controller key binding is missing or unexpected")
	}
	if qualificationPhysicalPowerMethod(controller.Method) != qualificationHexDigest(evidence.ControllerTargetIdentitySHA256) {
		return errors.New("power receipt physical controller target binding is missing or unexpected")
	}
	verified, err := verifyRawDestructiveArtifact(evidence.DatabaseArtifact)
	if err != nil || hexDigest(verified.SHA256) != trial.DatabaseSHA256 || verified.Meta.CommitSequence != trial.RecoveredSequence ||
		verified.Meta.Generation != evidence.MetaGeneration || verified.ValidMetaSlots != evidence.ValidMetaSlots ||
		verified.PhysicalPages != evidence.PhysicalPages || verified.ReachablePages != evidence.ReachablePages ||
		verified.FreeSpaceValid != evidence.FreeSpaceValid {
		return errors.New("power crash image does not reproduce its offline proof")
	}
	evidenceRaw, err := json.Marshal(canonicalEvidence)
	if err != nil {
		return err
	}
	artifactHash := sha256.New()
	_, _ = artifactHash.Write(markerRaw)
	_, _ = artifactHash.Write(controllerRaw)
	_, _ = artifactHash.Write(controllerProofRaw)
	_, _ = artifactHash.Write(evidenceRaw)
	_, _ = artifactHash.Write(verified.SHA256[:])
	if fmt.Sprintf("%x", artifactHash.Sum(nil)) != trial.ArtifactsSHA256 {
		return errors.New("power aggregate artifact hash does not match its components")
	}
	return nil
}

func hexDigest(value [32]byte) string {
	return fmt.Sprintf("%x", value[:])
}
