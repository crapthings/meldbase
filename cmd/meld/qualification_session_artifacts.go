package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type qualificationSessionArtifactBinding struct {
	PlanSHA256       string
	HeadEventSHA256  string
	ExecutableSHA256 string
	ReceiptSHA256    []string
}

func verifyQualificationSessionArtifactJournal(artifacts verifiedQualificationArtifactIndex, durability durabilityCheckResult,
	environment qualificationEnvironmentEvidence, record qualificationDestructiveRecord) (qualificationSessionArtifactBinding, error) {
	var binding qualificationSessionArtifactBinding
	planRelative := qualificationSessionDirectory + "/plan.json"
	planEntry, exists := artifacts.Entries[planRelative]
	if !exists {
		return binding, errors.New("secured artifact tree has no qualification session plan")
	}
	planPath := filepath.Join(artifacts.Root, filepath.FromSlash(planRelative))
	var plan qualificationSessionPlan
	planRaw, err := readQualificationReceipt(planPath, &plan)
	if err != nil || qualificationSHA256(planRaw) != planEntry.SHA256 {
		return binding, errors.New("qualification session plan differs from the secured artifact index")
	}
	if err := validateQualificationSessionPlan(plan); err != nil {
		return binding, err
	}
	binding.PlanSHA256 = qualificationSHA256(planRaw)
	binding.ExecutableSHA256 = plan.ExecutableSHA256
	if plan.SourceRevision != record.SourceRevision || plan.PlatformClass != record.PlatformClass || plan.GOOS != durability.GOOS ||
		plan.GOARCH != durability.GOARCH || plan.GoVersion != durability.GoVersion || plan.ControllerMethod != record.Infrastructure.ControllerMethod || plan.ControllerPublicKeySHA256 != environment.Controller.AttestationPublicKeySHA256 || plan.ControllerTargetIdentitySHA256 != environment.Controller.PowerTargetIdentitySHA256 ||
		plan.Volume != environment.Volume || plan.EnvironmentSHA256 != record.Infrastructure.EnvironmentRecordSHA256 ||
		plan.CreatedAt.Before(environment.CapturedAt) || plan.CreatedAt.After(durability.StartedAt) {
		return binding, errors.New("qualification session plan differs from the manifest runtime, environment, volume or chronology")
	}
	executableEntry, exists := artifacts.Entries[plan.ExecutableRelativePath]
	if !exists || executableEntry.Bytes != plan.ExecutableBytes || executableEntry.SHA256 != plan.ExecutableSHA256 {
		return binding, errors.New("qualification session executable differs from the secured artifact index")
	}
	environmentEntry, exists := artifacts.Entries[plan.EnvironmentRelativePath]
	if !exists || environmentEntry.SHA256 != plan.EnvironmentSHA256 {
		return binding, errors.New("qualification session environment differs from the secured artifact index")
	}

	expectedReceiptSHA256 := []string{
		record.DurabilityReceiptSHA256, record.SoakReceiptSHA256, record.ProcessReceiptSHA256,
		record.CapacityReceiptSHA256, record.CorruptionReceiptSHA256,
	}
	expectedReceiptSHA256 = append(expectedReceiptSHA256, record.PowerReceiptSHA256...)
	if len(expectedReceiptSHA256) != len(plan.Steps) {
		return binding, errors.New("qualification session receipt count differs from the fixed campaign")
	}
	eventsPrefix := qualificationSessionDirectory + "/events/"
	eventEntries := 0
	for relative := range artifacts.Entries {
		if strings.HasPrefix(relative, eventsPrefix) {
			eventEntries++
		}
	}
	if eventEntries != len(plan.Steps) {
		return binding, errors.New("qualification session journal is incomplete or contains extra events")
	}

	previousEventSHA256 := ""
	previousRecordedAt := plan.CreatedAt
	previousFinishedAt := time.Time{}
	for index, step := range plan.Steps {
		eventRelative := eventsPrefix + qualificationSessionEventFilename(step)
		eventEntry, exists := artifacts.Entries[eventRelative]
		if !exists {
			return binding, fmt.Errorf("qualification session journal event %d is missing", index+1)
		}
		var event qualificationSessionEvent
		eventRaw, err := readQualificationReceipt(filepath.Join(artifacts.Root, filepath.FromSlash(eventRelative)), &event)
		if err != nil || qualificationSHA256(eventRaw) != eventEntry.SHA256 {
			return binding, fmt.Errorf("qualification session journal event %d differs from the secured artifact index", index+1)
		}
		if event.SchemaVersion != qualificationSessionEventSchema || event.SessionID != plan.SessionID || event.PlanSHA256 != binding.PlanSHA256 ||
			event.Ordinal != step.Ordinal || event.StepID != step.ID || event.Kind != step.Kind ||
			event.PreviousEventSHA256 != previousEventSHA256 || event.RecordedAt.Before(previousRecordedAt) ||
			validateQualificationArtifactPath(event.ReceiptRelativePath) != nil || event.ReceiptSHA256 != expectedReceiptSHA256[index] {
			return binding, fmt.Errorf("qualification session journal event %d identity, chain or receipt binding is invalid", index+1)
		}
		if event.ReceiptRelativePath == qualificationSessionDirectory || strings.HasPrefix(event.ReceiptRelativePath, qualificationSessionDirectory+"/") {
			return binding, fmt.Errorf("qualification session receipt %d is stored in the journal directory", index+1)
		}
		receiptEntry, exists := artifacts.Entries[event.ReceiptRelativePath]
		if !exists || receiptEntry.SHA256 != event.ReceiptSHA256 {
			return binding, fmt.Errorf("qualification session receipt %d differs from the secured artifact index", index+1)
		}
		startedAt, finishedAt, err := qualificationSessionArtifactReceiptTime(step, filepath.Join(artifacts.Root, filepath.FromSlash(event.ReceiptRelativePath)))
		if err != nil {
			return binding, fmt.Errorf("qualification session receipt %d: %w", index+1, err)
		}
		if startedAt.Before(plan.CreatedAt) || !finishedAt.After(startedAt) || (!previousFinishedAt.IsZero() && startedAt.Before(previousFinishedAt)) || event.RecordedAt.Before(finishedAt) {
			return binding, fmt.Errorf("qualification session receipt %d chronology is invalid", index+1)
		}
		previousEventSHA256 = qualificationSHA256(eventRaw)
		previousRecordedAt = event.RecordedAt
		previousFinishedAt = finishedAt
		binding.ReceiptSHA256 = append(binding.ReceiptSHA256, event.ReceiptSHA256)
	}
	binding.HeadEventSHA256 = previousEventSHA256
	if record.SessionPlanSHA256 != "" || record.SessionHeadEventSHA256 != "" || record.SessionExecutableSHA256 != "" {
		if record.SessionPlanSHA256 != binding.PlanSHA256 || record.SessionHeadEventSHA256 != binding.HeadEventSHA256 ||
			record.SessionExecutableSHA256 != binding.ExecutableSHA256 {
			return binding, errors.New("destructive manifest qualification session binding differs from the secured journal")
		}
	}
	return binding, nil
}

func qualificationSessionArtifactReceiptTime(step qualificationSessionStep, path string) (time.Time, time.Time, error) {
	switch step.Kind {
	case "durability":
		var receipt durabilityCheckResult
		_, err := readQualificationReceipt(path, &receipt)
		return receipt.StartedAt, receipt.FinishedAt, err
	case "soak":
		var receipt qualificationSoakReceipt
		_, err := readQualificationReceipt(path, &receipt)
		return receipt.StartedAt, receipt.FinishedAt, err
	case "process":
		var receipt destructiveProcessReceipt
		_, err := readQualificationReceipt(path, &receipt)
		return receipt.StartedAt, receipt.FinishedAt, err
	case "capacity":
		var receipt destructiveENOSPCReceipt
		_, err := readQualificationReceipt(path, &receipt)
		return receipt.StartedAt, receipt.FinishedAt, err
	case "corruption":
		var receipt destructiveCorruptionReceipt
		_, err := readQualificationReceipt(path, &receipt)
		return receipt.StartedAt, receipt.FinishedAt, err
	case "power":
		var receipt destructivePowerReceipt
		_, err := readQualificationReceipt(path, &receipt)
		if err == nil && (receipt.Trial.ID != step.PowerTrialID || receipt.Trial.PublicationBoundary != step.PublicationBoundary) {
			return time.Time{}, time.Time{}, errors.New("power trial differs from the fixed session matrix")
		}
		return receipt.Trial.StartedAt, receipt.Trial.FinishedAt, err
	default:
		return time.Time{}, time.Time{}, fmt.Errorf("unknown qualification session step %q", step.Kind)
	}
}
