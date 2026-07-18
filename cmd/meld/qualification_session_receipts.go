package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

func validateQualificationSessionReceipt(plan qualificationSessionPlan, step qualificationSessionStep, path string, state *qualificationSessionState) ([]byte, time.Time, error) {
	switch step.Kind {
	case "durability":
		var receipt durabilityCheckResult
		raw, err := readQualificationReceipt(path, &receipt)
		if err != nil {
			return nil, time.Time{}, err
		}
		if err := validateQualificationDurability(receipt, plan.SourceRevision); err != nil {
			return nil, time.Time{}, err
		}
		if receipt.Directory != plan.Volume.Directory || receipt.Device != plan.Volume.Device || receipt.FilesystemType != plan.Volume.FilesystemType ||
			receipt.FilesystemName != plan.Volume.FilesystemName || receipt.BlockSize != plan.Volume.BlockSize || receipt.GOOS != plan.GOOS ||
			receipt.GOARCH != plan.GOARCH || receipt.GoVersion != plan.GoVersion {
			return nil, time.Time{}, errors.New("session durability receipt differs from the planned runtime or target volume")
		}
		if err := qualificationSessionAdvanceTime(plan, state, receipt.StartedAt, receipt.FinishedAt); err != nil {
			return nil, time.Time{}, err
		}
		state.Durability = &receipt
		return raw, receipt.FinishedAt, nil
	case "soak":
		if state.Durability == nil {
			return nil, time.Time{}, errors.New("session soak requires the recorded durability receipt")
		}
		var receipt qualificationSoakReceipt
		raw, err := readQualificationReceipt(path, &receipt)
		if err != nil {
			return nil, time.Time{}, err
		}
		if err := validateQualificationSoak(receipt, *state.Durability, plan.SourceRevision); err != nil {
			return nil, time.Time{}, err
		}
		if err := qualificationSessionAdvanceTime(plan, state, receipt.StartedAt, receipt.FinishedAt); err != nil {
			return nil, time.Time{}, err
		}
		return raw, receipt.FinishedAt, nil
	case "process":
		if state.Durability == nil {
			return nil, time.Time{}, errors.New("session process receipt requires durability evidence")
		}
		var receipt destructiveProcessReceipt
		raw, err := readQualificationReceipt(path, &receipt)
		if err != nil {
			return nil, time.Time{}, err
		}
		if err := validateDestructiveProcessReceipt(receipt); err != nil {
			return nil, time.Time{}, err
		}
		if err := validateDestructiveReceiptIdentity(plan.SourceRevision, *state.Durability, receipt.SourceRevision, receipt.BuildRevision, receipt.BuildModified,
			receipt.GOOS, receipt.GOARCH, receipt.GoVersion, receipt.Device, receipt.FilesystemType, receipt.FilesystemName, receipt.BlockSize); err != nil {
			return nil, time.Time{}, err
		}
		paths := make([]string, 0, len(receipt.TrialDirectories)*2)
		for _, directory := range receipt.TrialDirectories {
			paths = append(paths, filepath.Join(directory, "crash-image.meld"), filepath.Join(directory, "oracle.jsonl"))
		}
		if err := qualificationSessionRequireArtifactPaths(plan.ArtifactsRoot, paths...); err != nil {
			return nil, time.Time{}, err
		}
		if err := qualificationSessionAdvanceTime(plan, state, receipt.StartedAt, receipt.FinishedAt); err != nil {
			return nil, time.Time{}, err
		}
		return raw, receipt.FinishedAt, nil
	case "capacity":
		if state.Durability == nil {
			return nil, time.Time{}, errors.New("session capacity receipt requires durability evidence")
		}
		var receipt destructiveENOSPCReceipt
		raw, err := readQualificationReceipt(path, &receipt)
		if err != nil {
			return nil, time.Time{}, err
		}
		if err := validateDestructiveENOSPCReceipt(receipt); err != nil {
			return nil, time.Time{}, err
		}
		if err := validateDestructiveReceiptIdentity(plan.SourceRevision, *state.Durability, receipt.SourceRevision, receipt.BuildRevision, receipt.BuildModified,
			receipt.GOOS, receipt.GOARCH, receipt.GoVersion, receipt.Device, receipt.FilesystemType, receipt.FilesystemName, receipt.BlockSize); err != nil {
			return nil, time.Time{}, err
		}
		paths := make([]string, 0, len(receipt.CapacityEvidence)*2)
		for _, evidence := range receipt.CapacityEvidence {
			paths = append(paths, evidence.DatabaseArtifact, evidence.MarkerArtifact)
		}
		if err := qualificationSessionRequireArtifactPaths(plan.ArtifactsRoot, paths...); err != nil {
			return nil, time.Time{}, err
		}
		if err := qualificationSessionAdvanceTime(plan, state, receipt.StartedAt, receipt.FinishedAt); err != nil {
			return nil, time.Time{}, err
		}
		return raw, receipt.FinishedAt, nil
	case "corruption":
		if state.Durability == nil {
			return nil, time.Time{}, errors.New("session corruption receipt requires durability evidence")
		}
		var receipt destructiveCorruptionReceipt
		raw, err := readQualificationReceipt(path, &receipt)
		if err != nil {
			return nil, time.Time{}, err
		}
		if err := validateDestructiveCorruptionReceipt(receipt); err != nil {
			return nil, time.Time{}, err
		}
		if receipt.SourceRevision != plan.SourceRevision || receipt.BuildRevision != plan.SourceRevision || receipt.BuildModified ||
			receipt.GOOS != plan.GOOS || receipt.GOARCH != plan.GOARCH || receipt.GoVersion != plan.GoVersion {
			return nil, time.Time{}, errors.New("session corruption receipt differs from the clean campaign runtime")
		}
		if err := recheckDestructiveCorruptionReceipt(receipt); err != nil {
			return nil, time.Time{}, err
		}
		if err := qualificationSessionRequireArtifactPaths(plan.ArtifactsRoot, receipt.DatabaseArtifact); err != nil {
			return nil, time.Time{}, err
		}
		if err := qualificationSessionAdvanceTime(plan, state, receipt.StartedAt, receipt.FinishedAt); err != nil {
			return nil, time.Time{}, err
		}
		return raw, receipt.FinishedAt, nil
	case "power":
		if state.Durability == nil {
			return nil, time.Time{}, errors.New("session power receipt requires durability evidence")
		}
		var receipt destructivePowerReceipt
		raw, err := readQualificationReceipt(path, &receipt)
		if err != nil {
			return nil, time.Time{}, err
		}
		if err := validateDestructivePowerReceipt(receipt); err != nil {
			return nil, time.Time{}, err
		}
		if err := validateDestructiveReceiptIdentity(plan.SourceRevision, *state.Durability, receipt.SourceRevision, receipt.BuildRevision, receipt.BuildModified,
			receipt.GOOS, receipt.GOARCH, receipt.GoVersion, receipt.Device, receipt.FilesystemType, receipt.FilesystemName, receipt.BlockSize); err != nil {
			return nil, time.Time{}, err
		}
		if receipt.Trial.ID != step.PowerTrialID || receipt.Trial.PublicationBoundary != step.PublicationBoundary || receipt.Evidence.Method != plan.ControllerMethod {
			return nil, time.Time{}, errors.New("session power receipt differs from the planned trial, boundary or controller")
		}
		if receipt.Evidence.ControllerPublicKeySHA256 != plan.ControllerPublicKeySHA256 {
			return nil, time.Time{}, errors.New("session power receipt controller attestation key differs from the plan")
		}
		if receipt.Evidence.ControllerTargetIdentitySHA256 != plan.ControllerTargetIdentitySHA256 {
			return nil, time.Time{}, errors.New("session power receipt controller target identity differs from the plan")
		}
		transition := receipt.Evidence.BootIDBefore + "\x00" + receipt.Evidence.BootIDAfter
		if _, duplicate := state.BootTransitions[transition]; duplicate {
			return nil, time.Time{}, errors.New("session power receipt repeats a boot transition")
		}
		if err := qualificationSessionRequireArtifactPaths(plan.ArtifactsRoot, receipt.Evidence.MarkerArtifact, receipt.Evidence.ControllerArtifact,
			receipt.Evidence.ControllerProofArtifact, receipt.Evidence.DatabaseArtifact); err != nil {
			return nil, time.Time{}, err
		}
		if err := qualificationSessionAdvanceTime(plan, state, receipt.Trial.StartedAt, receipt.Trial.FinishedAt); err != nil {
			return nil, time.Time{}, err
		}
		state.BootTransitions[transition] = struct{}{}
		return raw, receipt.Trial.FinishedAt, nil
	default:
		return nil, time.Time{}, fmt.Errorf("unknown qualification session step kind %q", step.Kind)
	}
}

func loadQualificationSessionReceiptState(step qualificationSessionStep, path string, state *qualificationSessionState) error {
	switch step.Kind {
	case "durability":
		var receipt durabilityCheckResult
		if _, err := readQualificationReceipt(path, &receipt); err != nil {
			return err
		}
		state.Durability = &receipt
		state.LastFinishedAt = receipt.FinishedAt
	case "soak":
		var receipt qualificationSoakReceipt
		if _, err := readQualificationReceipt(path, &receipt); err != nil {
			return err
		}
		state.LastFinishedAt = receipt.FinishedAt
	case "process":
		var receipt destructiveProcessReceipt
		if _, err := readQualificationReceipt(path, &receipt); err != nil {
			return err
		}
		state.LastFinishedAt = receipt.FinishedAt
	case "capacity":
		var receipt destructiveENOSPCReceipt
		if _, err := readQualificationReceipt(path, &receipt); err != nil {
			return err
		}
		state.LastFinishedAt = receipt.FinishedAt
	case "corruption":
		var receipt destructiveCorruptionReceipt
		if _, err := readQualificationReceipt(path, &receipt); err != nil {
			return err
		}
		state.LastFinishedAt = receipt.FinishedAt
	case "power":
		var receipt destructivePowerReceipt
		if _, err := readQualificationReceipt(path, &receipt); err != nil {
			return err
		}
		transition := receipt.Evidence.BootIDBefore + "\x00" + receipt.Evidence.BootIDAfter
		if _, duplicate := state.BootTransitions[transition]; duplicate {
			return errors.New("session journal repeats a power boot transition")
		}
		state.BootTransitions[transition] = struct{}{}
		state.LastFinishedAt = receipt.Trial.FinishedAt
	default:
		return fmt.Errorf("unknown qualification session step kind %q", step.Kind)
	}
	return nil
}

func qualificationSessionAdvanceTime(plan qualificationSessionPlan, state *qualificationSessionState, started, finished time.Time) error {
	if started.IsZero() || !finished.After(started) || started.Before(plan.CreatedAt) || (!state.LastFinishedAt.IsZero() && started.Before(state.LastFinishedAt)) {
		return errors.New("qualification session receipt timing is invalid or overlaps an earlier step")
	}
	state.LastFinishedAt = finished
	return nil
}

func qualificationSessionRequireArtifactPaths(root string, paths ...string) error {
	for _, path := range paths {
		absolute, err := qualificationArtifactCandidate(path)
		if err != nil || !qualificationPathWithin(root, absolute) || absolute == root {
			return fmt.Errorf("session artifact %q is missing, linked or outside the artifact root", path)
		}
	}
	return nil
}

func qualificationSessionReceiptRelativePath(root, path string) (string, error) {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == "." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("session receipt path escapes the artifact root")
	}
	return filepath.ToSlash(relative), nil
}
