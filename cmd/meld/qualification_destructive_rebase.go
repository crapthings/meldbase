package main

import (
	"errors"
	"fmt"
	"path/filepath"
)

func rebaseQualificationProcessReceipt(receipt destructiveProcessReceipt, artifacts verifiedQualificationArtifactIndex) (destructiveProcessReceipt, []string, error) {
	rebased := receipt
	rebased.ArtifactsDirectory = artifacts.Root
	rebased.TrialDirectories = make([]string, len(receipt.TrialDirectories))
	paths := make([]string, 0, len(receipt.TrialDirectories)*2)
	for index, oldDirectory := range receipt.TrialDirectories {
		database, err := qualificationArtifactPathForReference(artifacts, filepath.Join(oldDirectory, "crash-image.meld"), receipt.Trials[index].DatabaseSHA256)
		if err != nil {
			return destructiveProcessReceipt{}, nil, fmt.Errorf("process trial %d crash image rebase: %w", index+1, err)
		}
		oracle, err := qualificationArtifactPathForReference(artifacts, filepath.Join(oldDirectory, "oracle.jsonl"), receipt.Trials[index].ArtifactsSHA256)
		if err != nil {
			return destructiveProcessReceipt{}, nil, fmt.Errorf("process trial %d oracle rebase: %w", index+1, err)
		}
		if filepath.Base(database) != "crash-image.meld" || filepath.Base(oracle) != "oracle.jsonl" || filepath.Dir(database) != filepath.Dir(oracle) {
			return destructiveProcessReceipt{}, nil, fmt.Errorf("process trial %d relocated artifacts do not preserve their directory layout", index+1)
		}
		rebased.TrialDirectories[index] = filepath.Dir(database)
		paths = append(paths, database, oracle)
	}
	return rebased, paths, nil
}

func rebaseQualificationCapacityReceipt(receipt destructiveENOSPCReceipt, artifacts verifiedQualificationArtifactIndex) (destructiveENOSPCReceipt, []string, error) {
	rebased := receipt
	rebased.ArtifactsDirectory = artifacts.Root
	rebased.CapacityEvidence = append([]destructiveCapacityTrialEvidence(nil), receipt.CapacityEvidence...)
	paths := make([]string, 0, len(receipt.CapacityEvidence)*2)
	for index := range rebased.CapacityEvidence {
		evidence := &rebased.CapacityEvidence[index]
		database, err := qualificationArtifactPathForReference(artifacts, evidence.DatabaseArtifact, receipt.Trials[index].DatabaseSHA256)
		if err != nil {
			return destructiveENOSPCReceipt{}, nil, fmt.Errorf("capacity trial %d crash image rebase: %w", index+1, err)
		}
		marker, err := qualificationArtifactPathForReference(artifacts, evidence.MarkerArtifact, evidence.MarkerSHA256)
		if err != nil {
			return destructiveENOSPCReceipt{}, nil, fmt.Errorf("capacity trial %d marker rebase: %w", index+1, err)
		}
		evidence.DatabaseArtifact, evidence.MarkerArtifact = database, marker
		paths = append(paths, database, marker)
	}
	return rebased, paths, nil
}

func rebaseQualificationCorruptionReceipt(receipt destructiveCorruptionReceipt, artifacts verifiedQualificationArtifactIndex) (destructiveCorruptionReceipt, []string, error) {
	rebased := receipt
	database, err := qualificationArtifactPathForReference(artifacts, receipt.DatabaseArtifact, receipt.DatabaseSHA256)
	if err != nil {
		return destructiveCorruptionReceipt{}, nil, fmt.Errorf("corruption database rebase: %w", err)
	}
	rebased.DatabaseArtifact = database
	return rebased, []string{database}, nil
}

func rebaseQualificationPowerReceipt(receipt destructivePowerReceipt, artifacts verifiedQualificationArtifactIndex) (destructivePowerReceipt, []string, error) {
	rebased := receipt
	evidence := &rebased.Evidence
	var err error
	evidence.MarkerArtifact, err = qualificationArtifactPathForReference(artifacts, receipt.Evidence.MarkerArtifact, receipt.Evidence.MarkerSHA256)
	if err != nil {
		return destructivePowerReceipt{}, nil, fmt.Errorf("power marker rebase: %w", err)
	}
	evidence.ControllerArtifact, err = qualificationArtifactPathForReference(artifacts, receipt.Evidence.ControllerArtifact, receipt.Evidence.ControllerSHA256)
	if err != nil {
		return destructivePowerReceipt{}, nil, fmt.Errorf("power controller event rebase: %w", err)
	}
	evidence.ControllerProofArtifact, err = qualificationArtifactPathForReference(artifacts, receipt.Evidence.ControllerProofArtifact, receipt.Evidence.ControllerProofSHA256)
	if err != nil {
		return destructivePowerReceipt{}, nil, fmt.Errorf("power controller proof rebase: %w", err)
	}
	evidence.DatabaseArtifact, err = qualificationArtifactPathForReference(artifacts, receipt.Evidence.DatabaseArtifact, receipt.Trial.DatabaseSHA256)
	if err != nil {
		return destructivePowerReceipt{}, nil, fmt.Errorf("power crash image rebase: %w", err)
	}
	paths := []string{evidence.MarkerArtifact, evidence.ControllerArtifact, evidence.ControllerProofArtifact, evidence.DatabaseArtifact}
	for _, path := range paths {
		if !qualificationPathWithin(artifacts.Root, path) || path == artifacts.Root {
			return destructivePowerReceipt{}, nil, errors.New("rebased power artifact escapes the secured root")
		}
	}
	return rebased, paths, nil
}
