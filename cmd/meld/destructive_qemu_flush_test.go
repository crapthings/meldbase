package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFlushEIOHandshakeRejectsMismatchedReady(t *testing.T) {
	readyAt := time.Now().UTC()
	armed := destructiveFlushEIOArmed{SchemaVersion: 1, ReadySHA256: strings.Repeat("a", 64), QMPArmSHA256: strings.Repeat("b", 64), ArmedAt: readyAt.Add(time.Millisecond)}
	if err := validateDestructiveFlushEIOArmed(armed, strings.Repeat("a", 64), readyAt); err != nil {
		t.Fatalf("valid armed receipt: %v", err)
	}
	if err := validateDestructiveFlushEIOArmed(armed, strings.Repeat("c", 64), readyAt); err == nil {
		t.Fatal("mismatched ready digest was accepted")
	}
	armed.ArmedAt = readyAt.Add(-time.Nanosecond)
	if err := validateDestructiveFlushEIOArmed(armed, strings.Repeat("a", 64), readyAt); err == nil {
		t.Fatal("armed-before-ready receipt was accepted")
	}
}

func TestFlushEIOEvidencePreservesCompoundWriteClassification(t *testing.T) {
	raw := json.RawMessage(`{"event":"BLOCK_IO_ERROR","data":{"operation":"write","action":"report","nospace":false,"reason":"Input/output error"}}`)
	exchanges := []destructiveQMPExchange{{Direction: "receive", At: time.Now().UTC(), Message: raw}}
	if count := countQMPFlushIOErrors(exchanges); count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if operation := qmpFlushEIOOperation(exchanges); operation != "write" {
		t.Fatalf("operation = %q, want write", operation)
	}
	bad := json.RawMessage(`{"event":"BLOCK_IO_ERROR","data":{"operation":"read","action":"report","nospace":false,"reason":"Input/output error"}}`)
	if count := countQMPFlushIOErrors([]destructiveQMPExchange{{Direction: "receive", Message: bad}}); count != 0 {
		t.Fatalf("read error count = %d, want 0", count)
	}
}

func TestFlushArmExchangeSHAStopsAtArmedInventory(t *testing.T) {
	exchanges := []destructiveQMPExchange{
		{Direction: "send", Message: json.RawMessage(`{"execute":"query-named-block-nodes","id":"flush-query-nodes-armed"}`)},
		{Direction: "receive", Message: json.RawMessage(`{"return":[],"id":"flush-query-nodes-armed"}`)},
		{Direction: "receive", Message: json.RawMessage(`{"event":"BLOCK_IO_ERROR"}`)},
	}
	wantRaw, err := json.Marshal(exchanges[:2])
	if err != nil {
		t.Fatal(err)
	}
	got, err := flushArmExchangeSHA(exchanges)
	if err != nil {
		t.Fatal(err)
	}
	if want := qualificationSHA256(wantRaw); got != want {
		t.Fatalf("arm SHA = %s, want %s", got, want)
	}
	exchanges[1].Message = json.RawMessage(`{"return":[],"id":"different"}`)
	if _, err := flushArmExchangeSHA(exchanges); err == nil {
		t.Fatal("missing armed inventory response was accepted")
	}
}

func TestFlushArmCommandEvidenceRejectsRuleWidening(t *testing.T) {
	valid := []destructiveQMPExchange{
		{Direction: "send", Message: json.RawMessage(`{"execute":"blockdev-add","arguments":{"driver":"blkdebug","image":"meld-file","inject-error":[{"errno":5,"event":"flush_to_disk","iotype":"flush","once":true}],"node-name":"meld-flush-debug"},"id":"flush-arm-add"}`)},
		{Direction: "send", Message: json.RawMessage(`{"execute":"blockdev-reopen","arguments":{"options":[{"driver":"raw","file":"meld-flush-debug","node-name":"meld-raw"}]},"id":"flush-arm-switch"}`)},
	}
	if err := validateFlushArmCommandEvidence(valid); err != nil {
		t.Fatalf("valid arm commands: %v", err)
	}
	widened := append([]destructiveQMPExchange(nil), valid...)
	widened[0].Message = json.RawMessage(`{"execute":"blockdev-add","arguments":{"driver":"blkdebug","image":"meld-file","inject-error":[{"errno":5,"event":"flush_to_disk","iotype":"flush","once":false}],"node-name":"meld-flush-debug"},"id":"flush-arm-add"}`)
	if err := validateFlushArmCommandEvidence(widened); err == nil {
		t.Fatal("repeat-forever flush rule was accepted")
	}
}

func TestFlushEIORecoveryResultRequiresFreshBootAndEvidenceBinding(t *testing.T) {
	now := time.Now().UTC()
	result := destructiveFlushEIOWorkerResult{
		SchemaVersion: 1, GOOS: "linux", GOARCH: "amd64", GoVersion: "go-test",
		StartedAt: now, FinishedAt: now.Add(time.Second), FaultBootID: "fault-boot", RecoveryBootID: "recovery-boot",
		FaultSHA256: strings.Repeat("a", 64), ProofSHA256: strings.Repeat("b", 64), RecoveryPlanSHA256: strings.Repeat("e", 64), RecoveryReadySHA256: strings.Repeat("f", 64), DatabaseArtifact: "/control/recovered.meld",
		BeforeSHA256: strings.Repeat("c", 64), AfterSHA256: strings.Repeat("d", 64), BeforeSequence: 16, RecoveredSequence: 16,
		OfflineVerified: true, FreeSpaceValid: true, PersistentFreeSpace: true, Passed: true,
	}
	if err := validateDestructiveFlushEIOWorkerResult(result); err != nil {
		t.Fatalf("valid recovery result: %v", err)
	}
	result.RecoveryBootID = result.FaultBootID
	if err := validateDestructiveFlushEIOWorkerResult(result); err == nil {
		t.Fatal("same-boot recovery result was accepted")
	}
	result.RecoveryBootID = "recovery-boot"
	result.ProofSHA256 = ""
	if err := validateDestructiveFlushEIOWorkerResult(result); err == nil {
		t.Fatal("recovery result without proof binding was accepted")
	}
}

func TestFlushEIORecoveryBindingRejectsImageSubstitution(t *testing.T) {
	now := time.Now().UTC()
	faultRaw := []byte(`{"fault":true}`)
	proofRaw := []byte(`{"proof":true}`)
	proof := destructiveQMPFlushEIOProof{TargetImage: "/control/target.img", FaultSHA256: qualificationSHA256(faultRaw), FinishedAt: now}
	plan := destructiveFlushEIORecoveryPlan{SchemaVersion: 1, TargetImage: proof.TargetImage, TargetImageSHA256: strings.Repeat("a", 64), TargetImageSize: 128 << 20, FaultSHA256: qualificationSHA256(faultRaw), ProofSHA256: qualificationSHA256(proofRaw), CreatedAt: now.Add(time.Second)}
	if err := validateDestructiveFlushEIORecoveryPlan(plan, faultRaw, proofRaw, proof); err != nil {
		t.Fatalf("valid recovery plan: %v", err)
	}
	planRaw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	fault := destructiveFlushEIOFaultResult{BootID: "fault-boot"}
	ready := destructiveFlushEIORecoveryReady{SchemaVersion: 1, PlanSHA256: qualificationSHA256(planRaw), RawDevice: "/dev/vda", RawDeviceSHA256: plan.TargetImageSHA256, RawDeviceSize: plan.TargetImageSize, BootID: "recovery-boot", ObservedAt: plan.CreatedAt.Add(time.Second)}
	if err := validateDestructiveFlushEIORecoveryReady(ready, planRaw, plan, fault); err != nil {
		t.Fatalf("valid recovery ready: %v", err)
	}
	ready.RawDeviceSHA256 = strings.Repeat("b", 64)
	if err := validateDestructiveFlushEIORecoveryReady(ready, planRaw, plan, fault); err == nil {
		t.Fatal("substituted recovery block image was accepted")
	}
	ready.RawDeviceSHA256 = plan.TargetImageSHA256
	ready.BootID = fault.BootID
	if err := validateDestructiveFlushEIORecoveryReady(ready, planRaw, plan, fault); err == nil {
		t.Fatal("same-boot recovery preflight was accepted")
	}
}
