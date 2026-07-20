package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const destructiveFlushEIORecoveryBindingSchema uint32 = 1

type destructiveFlushEIORecoveryPlan struct {
	SchemaVersion     uint32    `json:"schemaVersion"`
	TargetImage       string    `json:"targetImage"`
	TargetImageSHA256 string    `json:"targetImageSha256"`
	TargetImageSize   int64     `json:"targetImageSize"`
	FaultSHA256       string    `json:"faultSha256"`
	ProofSHA256       string    `json:"proofSha256"`
	CreatedAt         time.Time `json:"createdAt"`
}

type destructiveFlushEIORecoveryReady struct {
	SchemaVersion   uint32    `json:"schemaVersion"`
	PlanSHA256      string    `json:"planSha256"`
	RawDevice       string    `json:"rawDevice"`
	RawDeviceSHA256 string    `json:"rawDeviceSha256"`
	RawDeviceSize   int64     `json:"rawDeviceSize"`
	BootID          string    `json:"bootId"`
	ObservedAt      time.Time `json:"observedAt"`
}

func hashOpenFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

func validateDestructiveFlushEIORecoveryPlan(plan destructiveFlushEIORecoveryPlan, faultRaw, proofRaw []byte, proof destructiveQMPFlushEIOProof) error {
	if plan.SchemaVersion != destructiveFlushEIORecoveryBindingSchema || !filepath.IsAbs(plan.TargetImage) || !qualificationHexDigest(plan.TargetImageSHA256) || plan.TargetImageSize <= 0 || plan.FaultSHA256 != qualificationSHA256(faultRaw) || plan.ProofSHA256 != qualificationSHA256(proofRaw) || plan.TargetImage != proof.TargetImage || plan.FaultSHA256 != proof.FaultSHA256 || plan.CreatedAt.Before(proof.FinishedAt) {
		return errors.New("flush EIO recovery plan is incomplete or not bound to the stopped fault image")
	}
	return nil
}

func validateDestructiveFlushEIORecoveryReady(ready destructiveFlushEIORecoveryReady, planRaw []byte, plan destructiveFlushEIORecoveryPlan, fault destructiveFlushEIOFaultResult) error {
	if ready.SchemaVersion != destructiveFlushEIORecoveryBindingSchema || ready.PlanSHA256 != qualificationSHA256(planRaw) || ready.RawDevice != "/dev/vda" || ready.RawDeviceSHA256 != plan.TargetImageSHA256 || ready.RawDeviceSize != plan.TargetImageSize || !qualificationSafeName(ready.BootID, 64) || ready.BootID == fault.BootID || ready.ObservedAt.Before(plan.CreatedAt) {
		return errors.New("flush EIO recovery ready receipt does not bind a fresh boot to the exact stopped image")
	}
	return nil
}

func runDestructiveFlushEIORecoveryPlan(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-flush-eio-recovery-plan", flag.ContinueOnError)
	flags.SetOutput(stderr)
	targetPath := flags.String("target-image", "", "stopped raw target image from the fault VM")
	faultPath := flags.String("fault", "", "durable fault-stage result")
	proofPath := flags.String("proof", "", "durable QMP fault proof")
	outputPath := flags.String("out", "", "new durable recovery plan")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *targetPath == "" || *faultPath == "" || *proofPath == "" || *outputPath == "" {
		return errors.New("destructive-flush-eio-recovery-plan requires target-image, fault, proof and out")
	}
	target, err := existingRegularAbsolutePath(*targetPath)
	if err != nil {
		return err
	}
	var fault destructiveFlushEIOFaultResult
	faultRaw, err := readQualificationReceipt(*faultPath, &fault)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIOFaultResult(fault); err != nil {
		return err
	}
	var proof destructiveQMPFlushEIOProof
	proofRaw, err := readQualificationReceipt(*proofPath, &proof)
	if err != nil {
		return err
	}
	if err := validateQMPFlushEIOProofStructure(proof); err != nil {
		return err
	}
	digest, size, err := hashOpenFile(target)
	if err != nil {
		return err
	}
	plan := destructiveFlushEIORecoveryPlan{destructiveFlushEIORecoveryBindingSchema, target, digest, size, qualificationSHA256(faultRaw), qualificationSHA256(proofRaw), time.Now().UTC()}
	if err := validateDestructiveFlushEIORecoveryPlan(plan, faultRaw, proofRaw, proof); err != nil {
		return err
	}
	output, err := filepath.Abs(filepath.Clean(*outputPath))
	if err != nil {
		return err
	}
	if filepath.Dir(output) != filepath.Dir(target) || output == target {
		return errors.New("flush EIO recovery plan must be directly beside and distinct from the target image")
	}
	if err := writeJSONExclusiveDurable(output, plan); err != nil {
		return err
	}
	if err := makeQualificationArtifactReadable(output); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(plan)
}

func runDestructiveFlushEIORecoveryPreflight(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-flush-eio-recovery-preflight", flag.ContinueOnError)
	flags.SetOutput(stderr)
	rawPath := flags.String("raw-device", "", "unmounted raw recovery block device")
	planPath := flags.String("plan", "", "host recovery plan")
	faultPath := flags.String("fault", "", "durable fault-stage result")
	proofPath := flags.String("proof", "", "durable QMP fault proof")
	outputPath := flags.String("out", "", "new raw-device recovery-ready receipt")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *rawPath != "/dev/vda" || *planPath == "" || *faultPath == "" || *proofPath == "" || *outputPath == "" || runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return errors.New("destructive-flush-eio-recovery-preflight requires Linux root, raw-device /dev/vda, plan, fault, proof and out")
	}
	info, err := os.Stat(*rawPath)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeDevice == 0 {
		return errors.New("flush EIO recovery raw path is not a block device")
	}
	var fault destructiveFlushEIOFaultResult
	faultRaw, err := readQualificationReceipt(*faultPath, &fault)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIOFaultResult(fault); err != nil {
		return err
	}
	var proof destructiveQMPFlushEIOProof
	proofRaw, err := readQualificationReceipt(*proofPath, &proof)
	if err != nil {
		return err
	}
	if err := validateQMPFlushEIOProofStructure(proof); err != nil {
		return err
	}
	var plan destructiveFlushEIORecoveryPlan
	planRaw, err := readQualificationReceipt(*planPath, &plan)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIORecoveryPlan(plan, faultRaw, proofRaw, proof); err != nil {
		return err
	}
	digest, size, err := hashOpenFile(*rawPath)
	if err != nil {
		return err
	}
	bootID, err := destructiveBootIDFn()
	if err != nil {
		return err
	}
	ready := destructiveFlushEIORecoveryReady{destructiveFlushEIORecoveryBindingSchema, qualificationSHA256(planRaw), *rawPath, digest, size, bootID, time.Now().UTC()}
	if err := validateDestructiveFlushEIORecoveryReady(ready, planRaw, plan, fault); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(*outputPath, ready); err != nil {
		return err
	}
	if err := makeQualificationArtifactReadable(*outputPath); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(ready)
}

func makeQualificationArtifactReadable(path string) error {
	if err := os.Chmod(path, 0o644); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := syncProbeDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync readable qualification artifact parent: %w", err)
	}
	return nil
}
