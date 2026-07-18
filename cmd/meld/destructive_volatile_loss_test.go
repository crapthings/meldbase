package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestVolatileLossResultRequiresMonotonicFloorRejection(t *testing.T) {
	result := destructiveVolatileLossResult{
		SchemaVersion: 1, GOOS: "linux", GOARCH: "amd64", GoVersion: "go-test",
		BootIDBefore: "boot-before", BootIDAfter: "boot-after", RecoveredAt: time.Now().UTC(),
		MarkerSHA256: strings.Repeat("a", 64), ControllerSHA256: strings.Repeat("b", 64),
		DatabaseArtifact: "/control/recovered.meld", RecoveredSHA256: strings.Repeat("c", 64),
		AcknowledgedCommitSequence: 2, RecoveredCommitSequence: 1,
		AcknowledgedStateSHA256: strings.Repeat("d", 64), RecoveredStateSHA256: strings.Repeat("e", 64),
		AcknowledgedCommitLost: true, MonotonicFloorRejected: true, UnsafeStorageDetected: true, NegativeControlPassed: true,
	}
	if err := validateDestructiveVolatileLossResult(result); err != nil {
		t.Fatalf("valid negative control: %v", err)
	}
	result.MonotonicFloorRejected = false
	if err := validateDestructiveVolatileLossResult(result); err == nil {
		t.Fatal("result passed without monotonic-floor rejection")
	}
}

func TestQMPVolatileSnapshotInventoryBindsExactBacking(t *testing.T) {
	raw := json.RawMessage(`[{"inserted":{"ro":false,"drv":"qcow2","backing_file":"/control/base.img","image":{"backing-image":{"filename":"/control/base.img"}}}}]`)
	if err := validateQMPVolatileSnapshotInventory(raw, "/control/base.img"); err != nil {
		t.Fatalf("valid inventory: %v", err)
	}
	if err := validateQMPVolatileSnapshotInventory(raw, "/control/other.img"); err == nil {
		t.Fatal("wrong durable backing image was accepted")
	}
	var nodes []map[string]any
	if err := json.Unmarshal(raw, &nodes); err != nil {
		t.Fatal(err)
	}
	inserted := nodes[0]["inserted"].(map[string]any)
	inserted["ro"] = true
	readOnly, err := json.Marshal(nodes)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateQMPVolatileSnapshotInventory(readOnly, "/control/base.img"); err == nil {
		t.Fatal("read-only temporary overlay was accepted")
	}
}
