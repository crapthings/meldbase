package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type qualificationSessionPowerPaths struct {
	Marker          string `json:"marker"`
	ControllerProof string `json:"controllerProof"`
	ControllerEvent string `json:"controllerEvent"`
	RecoveryReceipt string `json:"recoveryReceipt"`
}

type qualificationSessionPowerStatus struct {
	SchemaVersion                  uint32                         `json:"schemaVersion"`
	SessionID                      string                         `json:"sessionId"`
	Phase                          string                         `json:"phase"`
	Step                           qualificationSessionStep       `json:"step"`
	Paths                          qualificationSessionPowerPaths `json:"paths"`
	ControllerMethod               string                         `json:"controllerMethod"`
	ControllerPublicKeySHA256      string                         `json:"controllerPublicKeySha256"`
	ControllerTargetIdentitySHA256 string                         `json:"controllerTargetIdentitySha256"`
}

var (
	qualificationSessionPowerPrepareFn = runDestructivePowerPrepare
	qualificationSessionPowerRecoverFn = runDestructivePowerRecover
	qualificationSessionPowerRecordFn  = runQualificationSessionRecord
)

func runQualificationSessionPowerStatus(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-session-power-status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "qualification session plan whose physical power phase is inspected")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *planPath == "" || flags.NArg() != 0 {
		return errors.New("qualification-session-power-status requires --plan")
	}
	plan, _, state, err := loadQualificationSession(*planPath, true)
	if err != nil {
		return err
	}
	step, paths, err := qualificationSessionPhysicalPowerStep(plan, state)
	if err != nil {
		return err
	}
	present := make([]bool, 4)
	for index, path := range []string{paths.Marker, paths.ControllerProof, paths.ControllerEvent, paths.RecoveryReceipt} {
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 {
			return fmt.Errorf("qualification power trial artifact is not a nonempty regular file: %s", path)
		}
		present[index] = true
	}
	phase := "prepare"
	if present[1] != present[2] || !present[0] && (present[1] || present[2] || present[3]) || present[3] && (!present[1] || !present[2]) {
		return errors.New("qualification physical power trial contains a partial or out-of-order publication")
	}
	if present[0] {
		var marker destructivePowerMarker
		markerRaw, err := readQualificationReceipt(paths.Marker, &marker)
		if err != nil || validateDestructivePowerMarkerForController(marker, plan.SourceRevision) != nil || marker.TrialID != step.PowerTrialID || marker.Boundary != step.PublicationBoundary {
			return errors.New("qualification physical power marker differs from the immutable next session step")
		}
		phase = "controller"
		if present[1] && present[2] {
			var event destructivePowerControllerEvent
			if _, err := readQualificationReceipt(paths.ControllerEvent, &event); err != nil {
				return err
			}
			proofRaw, err := os.ReadFile(paths.ControllerProof)
			if err != nil {
				return err
			}
			publicRaw, err := base64.StdEncoding.Strict().DecodeString(event.ControllerPublicKey)
			if err != nil || len(publicRaw) != ed25519.PublicKeySize || qualificationSHA256(publicRaw) != plan.ControllerPublicKeySHA256 || verifyDestructivePhysicalPowerEvidence(proofRaw, event, markerRaw, ed25519.PublicKey(publicRaw)) != nil {
				return errors.New("qualification physical controller evidence differs from the immutable session plan")
			}
			var proof destructivePowerControllerProof
			if err := decodeOneStrictJSON(proofRaw, &proof); err != nil || proof.Response.TargetIdentitySHA256 != plan.ControllerTargetIdentitySHA256 {
				return errors.New("qualification physical controller target differs from the immutable session plan")
			}
			phase = "recover"
		}
		if present[3] {
			stateCopy := state
			if _, _, err := validateQualificationSessionReceipt(plan, step, paths.RecoveryReceipt, &stateCopy); err != nil {
				return fmt.Errorf("qualification physical recovery receipt: %w", err)
			}
			phase = "record"
		}
	}
	result := qualificationSessionPowerStatus{
		SchemaVersion: 1, SessionID: plan.SessionID, Phase: phase, Step: step, Paths: paths,
		ControllerMethod: plan.ControllerMethod, ControllerPublicKeySHA256: plan.ControllerPublicKeySHA256,
		ControllerTargetIdentitySHA256: plan.ControllerTargetIdentitySHA256,
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func runQualificationSessionPowerPrepare(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-session-power-prepare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "qualification session plan whose exact next step is prepared")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *planPath == "" || flags.NArg() != 0 {
		return errors.New("qualification-session-power-prepare requires --plan")
	}
	plan, _, state, err := loadQualificationSession(*planPath, true)
	if err != nil {
		return err
	}
	step, paths, err := qualificationSessionPhysicalPowerStep(plan, state)
	if err != nil {
		return err
	}
	for _, path := range []string{paths.Marker, paths.ControllerProof, paths.ControllerEvent, paths.RecoveryReceipt} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("qualification power trial output already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return qualificationSessionPowerPrepareFn([]string{
		"--dir", plan.Volume.Directory,
		"--control-dir", plan.ArtifactsRoot,
		"--marker", paths.Marker,
		"--trial-id", step.PowerTrialID,
		"--boundary", step.PublicationBoundary,
		"--destructive-token", plan.Volume.DestructiveToken,
		"--source-revision", plan.SourceRevision,
		"--require-clean-source",
	}, stderr)
}

func runQualificationSessionPowerRecover(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-session-power-recover", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "qualification session plan whose exact next step is recovered and recorded")
	publicKeyPath := flags.String("controller-public-key", "", "trusted physical controller-agent public key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *planPath == "" || *publicKeyPath == "" || flags.NArg() != 0 {
		return errors.New("qualification-session-power-recover requires --plan and --controller-public-key")
	}
	plan, _, state, err := loadQualificationSession(*planPath, true)
	if err != nil {
		return err
	}
	_, paths, err := qualificationSessionPhysicalPowerStep(plan, state)
	if err != nil {
		return err
	}
	publicKey, err := loadAnchorQualificationPublicKey(*publicKeyPath)
	if err != nil || qualificationSHA256(publicKey) != plan.ControllerPublicKeySHA256 {
		return errors.New("physical power recovery public key differs from the immutable session plan")
	}
	if _, err := os.Lstat(paths.RecoveryReceipt); errors.Is(err, os.ErrNotExist) {
		var proof destructivePowerControllerProof
		if _, err := readQualificationReceipt(paths.ControllerProof, &proof); err != nil || proof.Response.TargetIdentitySHA256 != plan.ControllerTargetIdentitySHA256 {
			return errors.New("physical power controller proof target differs from the immutable session plan")
		}
		var recoveryOutput bytes.Buffer
		if err := qualificationSessionPowerRecoverFn([]string{
			"--dir", plan.Volume.Directory,
			"--control-dir", plan.ArtifactsRoot,
			"--marker", paths.Marker,
			"--controller-event", paths.ControllerEvent,
			"--controller-proof", paths.ControllerProof,
			"--controller-public-key", *publicKeyPath,
			"--out", paths.RecoveryReceipt,
			"--destructive-token", plan.Volume.DestructiveToken,
		}, &recoveryOutput, stderr); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return qualificationSessionPowerRecordFn([]string{
		"--plan", *planPath, "--kind", "power", "--receipt", paths.RecoveryReceipt,
	}, stdout, stderr)
}

func qualificationSessionPhysicalPowerStep(plan qualificationSessionPlan, state qualificationSessionState) (qualificationSessionStep, qualificationSessionPowerPaths, error) {
	if !qualificationPhysicalPowerMethod(plan.ControllerMethod) {
		return qualificationSessionStep{}, qualificationSessionPowerPaths{}, errors.New("qualification session physical power command requires a physical controller method")
	}
	if len(state.Events) >= len(plan.Steps) {
		return qualificationSessionStep{}, qualificationSessionPowerPaths{}, errors.New("qualification session has no remaining power step")
	}
	step := plan.Steps[len(state.Events)]
	if step.Kind != "power" || !qualificationSafeName(step.PowerTrialID, 64) || !qualificationBoundaryAllowed(qualificationTrialPower, step.PublicationBoundary) {
		return qualificationSessionStep{}, qualificationSessionPowerPaths{}, errors.New("qualification session next step is not a valid physical power trial")
	}
	prefix := filepath.Join(plan.ArtifactsRoot, step.PowerTrialID)
	return step, qualificationSessionPowerPaths{
		Marker: prefix + "-marker.json", ControllerProof: prefix + "-controller-proof.json",
		ControllerEvent: prefix + "-controller.json", RecoveryReceipt: prefix + "-recovery.json",
	}, nil
}
