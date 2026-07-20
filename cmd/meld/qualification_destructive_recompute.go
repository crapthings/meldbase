package main

import (
	"errors"
	"fmt"
	"reflect"
)

type qualificationDestructiveRecomputeInputs struct {
	Record          qualificationDestructiveRecord
	Artifacts       verifiedQualificationArtifactIndex
	Durability      durabilityCheckResult
	DurabilityRaw   []byte
	SoakRaw         []byte
	Environment     qualificationEnvironmentEvidence
	EnvironmentRaw  []byte
	EnvironmentPath string
	Revision        string
}

var verifyQualificationDestructiveOriginalEvidence = func(inputs qualificationDestructiveRecomputeInputs) error {
	recomputed, err := recomputeQualificationDestructiveRecord(inputs)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(recomputed, inputs.Record) {
		return errors.New("destructive manifest differs from recomputed original receipts")
	}
	return nil
}

func recomputeQualificationDestructiveRecord(inputs qualificationDestructiveRecomputeInputs) (qualificationDestructiveRecord, error) {
	record, artifacts, durability := inputs.Record, inputs.Artifacts, inputs.Durability
	if qualificationSHA256(inputs.DurabilityRaw) != record.DurabilityReceiptSHA256 || qualificationSHA256(inputs.SoakRaw) != record.SoakReceiptSHA256 {
		return qualificationDestructiveRecord{}, errors.New("destructive manifest durability or soak binding differs")
	}
	durabilityPath, err := qualificationArtifactPathForDigest(artifacts, record.DurabilityReceiptSHA256)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed durability receipt: %w", err)
	}
	soakPath, err := qualificationArtifactPathForDigest(artifacts, record.SoakReceiptSHA256)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed soak receipt: %w", err)
	}
	processPath, err := qualificationArtifactPathForDigest(artifacts, record.ProcessReceiptSHA256)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed process receipt: %w", err)
	}
	capacityPath, err := qualificationArtifactPathForDigest(artifacts, record.CapacityReceiptSHA256)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed capacity receipt: %w", err)
	}
	corruptionPath, err := qualificationArtifactPathForDigest(artifacts, record.CorruptionReceiptSHA256)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed corruption receipt: %w", err)
	}

	var process destructiveProcessReceipt
	processRaw, err := readQualificationReceipt(processPath, &process)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed process receipt: %w", err)
	}
	if qualificationSHA256(processRaw) != record.ProcessReceiptSHA256 {
		return qualificationDestructiveRecord{}, errors.New("indexed process receipt digest changed while reading")
	}
	process, processArtifactPaths, err := rebaseQualificationProcessReceipt(process, artifacts)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed process receipt: %w", err)
	}
	if err := validateDestructiveProcessReceipt(process); err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed process receipt: %w", err)
	}
	if err := validateDestructiveReceiptIdentity(inputs.Revision, durability, process.SourceRevision, process.BuildRevision, process.BuildModified,
		process.GOOS, process.GOARCH, process.GoVersion, process.Device, process.FilesystemType, process.FilesystemName, process.BlockSize); err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed process receipt: %w", err)
	}

	var capacity destructiveENOSPCReceipt
	capacityRaw, err := readQualificationReceipt(capacityPath, &capacity)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed capacity receipt: %w", err)
	}
	if qualificationSHA256(capacityRaw) != record.CapacityReceiptSHA256 {
		return qualificationDestructiveRecord{}, errors.New("indexed capacity receipt digest changed while reading")
	}
	canonicalCapacityEvidence := append([]destructiveCapacityTrialEvidence(nil), capacity.CapacityEvidence...)
	capacity, capacityArtifactPaths, err := rebaseQualificationCapacityReceipt(capacity, artifacts)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed capacity receipt: %w", err)
	}
	if err := validateDestructiveENOSPCReceiptWithCanonicalEvidence(capacity, canonicalCapacityEvidence); err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed capacity receipt: %w", err)
	}
	if err := validateDestructiveReceiptIdentity(inputs.Revision, durability, capacity.SourceRevision, capacity.BuildRevision, capacity.BuildModified,
		capacity.GOOS, capacity.GOARCH, capacity.GoVersion, capacity.Device, capacity.FilesystemType, capacity.FilesystemName, capacity.BlockSize); err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed capacity receipt: %w", err)
	}

	var corruption destructiveCorruptionReceipt
	corruptionRaw, err := readQualificationReceipt(corruptionPath, &corruption)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed corruption receipt: %w", err)
	}
	if qualificationSHA256(corruptionRaw) != record.CorruptionReceiptSHA256 {
		return qualificationDestructiveRecord{}, errors.New("indexed corruption receipt digest changed while reading")
	}
	corruption, corruptionArtifactPaths, err := rebaseQualificationCorruptionReceipt(corruption, artifacts)
	if err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed corruption receipt: %w", err)
	}
	if err := validateDestructiveCorruptionReceipt(corruption); err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed corruption receipt: %w", err)
	}
	if corruption.SourceRevision != inputs.Revision || corruption.BuildRevision != inputs.Revision || corruption.BuildModified ||
		corruption.GOOS != durability.GOOS || corruption.GOARCH != durability.GOARCH || corruption.GoVersion != durability.GoVersion {
		return qualificationDestructiveRecord{}, errors.New("indexed corruption receipt is not from the clean campaign runtime")
	}
	if err := recheckDestructiveCorruptionReceipt(corruption); err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("indexed corruption receipt: %w", err)
	}

	kernelAndMountSHA, err := qualificationEnvironmentSectionSHA256(struct {
		Volume destructiveVolumeReceipt    `json:"volume"`
		Kernel qualificationKernelEvidence `json:"kernel"`
		Mount  qualificationMountEvidence  `json:"mount"`
	}{inputs.Environment.Volume, inputs.Environment.Kernel, inputs.Environment.Mount})
	if err != nil {
		return qualificationDestructiveRecord{}, err
	}
	controllerSHA, err := qualificationEnvironmentSectionSHA256(inputs.Environment.Controller)
	if err != nil {
		return qualificationDestructiveRecord{}, err
	}
	hostSHA, err := qualificationEnvironmentSectionSHA256(inputs.Environment.HostOperator)
	if err != nil {
		return qualificationDestructiveRecord{}, err
	}
	recomputed := qualificationDestructiveRecord{
		SchemaVersion: 6, SourceRevision: inputs.Revision, PlatformClass: record.PlatformClass,
		GOOS: durability.GOOS, GOARCH: durability.GOARCH, Device: durability.Device,
		FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		DurabilityReceiptSHA256: qualificationSHA256(inputs.DurabilityRaw), SoakReceiptSHA256: qualificationSHA256(inputs.SoakRaw),
		ProcessReceiptSHA256: qualificationSHA256(processRaw), CapacityReceiptSHA256: qualificationSHA256(capacityRaw),
		CorruptionReceiptSHA256: qualificationSHA256(corruptionRaw), SecuredArtifactsIndexSHA256: qualificationSHA256(artifacts.Raw),
		Infrastructure: qualificationInfrastructure{
			EnvironmentRecordSHA256: qualificationSHA256(inputs.EnvironmentRaw), KernelAndMountSHA256: kernelAndMountSHA,
			ControllerPolicySHA256: controllerSHA, HostAndOperatorSHA256: hostSHA, ControllerMethod: inputs.Environment.Controller.Method,
		},
		SessionPlanSHA256: record.SessionPlanSHA256, SessionHeadEventSHA256: record.SessionHeadEventSHA256,
		SessionExecutableSHA256: record.SessionExecutableSHA256,
	}
	recomputed.Trials = append(recomputed.Trials, process.Trials...)
	recomputed.Trials = append(recomputed.Trials, capacity.Trials...)
	recomputed.StartedAt, recomputed.FinishedAt = process.StartedAt, process.FinishedAt
	if capacity.StartedAt.Before(recomputed.StartedAt) {
		recomputed.StartedAt = capacity.StartedAt
	}
	if capacity.FinishedAt.After(recomputed.FinishedAt) {
		recomputed.FinishedAt = capacity.FinishedAt
	}
	if corruption.StartedAt.Before(recomputed.StartedAt) {
		recomputed.StartedAt = corruption.StartedAt
	}
	if corruption.FinishedAt.After(recomputed.FinishedAt) {
		recomputed.FinishedAt = corruption.FinishedAt
	}

	requiredArtifacts := []string{durabilityPath, soakPath, processPath, capacityPath, corruptionPath, inputs.EnvironmentPath}
	requiredArtifacts = append(requiredArtifacts, processArtifactPaths...)
	requiredArtifacts = append(requiredArtifacts, capacityArtifactPaths...)
	requiredArtifacts = append(requiredArtifacts, corruptionArtifactPaths...)
	seenBootTransitions := make(map[string]struct{}, len(record.PowerReceiptSHA256))
	seenPowerReceipts := make(map[string]struct{}, len(record.PowerReceiptSHA256))
	for ordinal, digest := range record.PowerReceiptSHA256 {
		if _, duplicate := seenPowerReceipts[digest]; duplicate {
			return qualificationDestructiveRecord{}, fmt.Errorf("power receipt %d repeats an exact digest", ordinal+1)
		}
		seenPowerReceipts[digest] = struct{}{}
		path, err := qualificationArtifactPathForDigest(artifacts, digest)
		if err != nil {
			return qualificationDestructiveRecord{}, fmt.Errorf("indexed power receipt %d: %w", ordinal+1, err)
		}
		var power destructivePowerReceipt
		raw, err := readQualificationReceipt(path, &power)
		if err != nil {
			return qualificationDestructiveRecord{}, fmt.Errorf("indexed power receipt %d: %w", ordinal+1, err)
		}
		if qualificationSHA256(raw) != digest {
			return qualificationDestructiveRecord{}, fmt.Errorf("indexed power receipt %d digest changed while reading", ordinal+1)
		}
		canonicalPowerEvidence := power.Evidence
		power, powerArtifactPaths, err := rebaseQualificationPowerReceipt(power, artifacts)
		if err != nil {
			return qualificationDestructiveRecord{}, fmt.Errorf("indexed power receipt %d: %w", ordinal+1, err)
		}
		if err := validateDestructivePowerReceiptWithCanonicalEvidence(power, canonicalPowerEvidence); err != nil {
			return qualificationDestructiveRecord{}, fmt.Errorf("indexed power receipt %d: %w", ordinal+1, err)
		}
		if err := validateDestructiveReceiptIdentity(inputs.Revision, durability, power.SourceRevision, power.BuildRevision, power.BuildModified,
			power.GOOS, power.GOARCH, power.GoVersion, power.Device, power.FilesystemType, power.FilesystemName, power.BlockSize); err != nil {
			return qualificationDestructiveRecord{}, fmt.Errorf("indexed power receipt %d: %w", ordinal+1, err)
		}
		if power.Evidence.Method != recomputed.Infrastructure.ControllerMethod {
			return qualificationDestructiveRecord{}, fmt.Errorf("indexed power receipt %d controller method differs from the environment", ordinal+1)
		}
		if power.Evidence.ControllerPublicKeySHA256 != inputs.Environment.Controller.AttestationPublicKeySHA256 {
			return qualificationDestructiveRecord{}, fmt.Errorf("indexed power receipt %d controller attestation key differs from the environment", ordinal+1)
		}
		if power.Evidence.ControllerTargetIdentitySHA256 != inputs.Environment.Controller.PowerTargetIdentitySHA256 {
			return qualificationDestructiveRecord{}, fmt.Errorf("indexed power receipt %d controller target identity differs from the environment", ordinal+1)
		}
		transition := power.Evidence.BootIDBefore + "\x00" + power.Evidence.BootIDAfter
		if _, duplicate := seenBootTransitions[transition]; duplicate {
			return qualificationDestructiveRecord{}, fmt.Errorf("indexed power receipt %d repeats a boot transition", ordinal+1)
		}
		seenBootTransitions[transition] = struct{}{}
		recomputed.PowerReceiptSHA256 = append(recomputed.PowerReceiptSHA256, digest)
		recomputed.Trials = append(recomputed.Trials, power.Trial)
		if power.Trial.StartedAt.Before(recomputed.StartedAt) {
			recomputed.StartedAt = power.Trial.StartedAt
		}
		if power.Trial.FinishedAt.After(recomputed.FinishedAt) {
			recomputed.FinishedAt = power.Trial.FinishedAt
		}
		requiredArtifacts = append(requiredArtifacts, path)
		requiredArtifacts = append(requiredArtifacts, powerArtifactPaths...)
	}
	if err := validateQualificationEnvironmentAgainstDurability(inputs.Environment, durability, inputs.Revision, recomputed.Infrastructure.ControllerMethod, recomputed.StartedAt); err != nil {
		return qualificationDestructiveRecord{}, err
	}
	if _, err := verifyQualificationSessionArtifactJournal(artifacts, durability, inputs.Environment, recomputed); err != nil {
		return qualificationDestructiveRecord{}, fmt.Errorf("qualification session: %w", err)
	}
	if err := requireQualificationArtifactPaths(artifacts, requiredArtifacts...); err != nil {
		return qualificationDestructiveRecord{}, err
	}
	actualIndex, err := buildQualificationArtifactIndex(artifacts.Root, inputs.Revision)
	if err != nil || !reflect.DeepEqual(actualIndex, artifacts.Index) {
		return qualificationDestructiveRecord{}, errors.New("secured artifact tree changed during destructive evidence recomputation")
	}
	packet := qualificationCheckResult{
		SourceRevision: inputs.Revision, GOOS: durability.GOOS, GOARCH: durability.GOARCH, Device: durability.Device,
		FilesystemType: durability.FilesystemType, FilesystemName: durability.FilesystemName, BlockSize: durability.BlockSize,
		DurabilityReceiptSHA256: recomputed.DurabilityReceiptSHA256, SoakReceiptSHA256: recomputed.SoakReceiptSHA256,
	}
	if err := validateQualificationDestructive(recomputed, packet); err != nil {
		return qualificationDestructiveRecord{}, err
	}
	return recomputed, nil
}
