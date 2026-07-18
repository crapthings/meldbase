package main

import (
	"encoding/json"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestValidateQMPKillProofBindsSIGKILLPausedRestartAndDirectIO(t *testing.T) {
	now := time.Now().UTC()
	markerRaw := []byte("{\"marker\":\"kill-proof\"}\n")
	before := []destructiveQMPExchange{
		qmpTestExchange("receive", now, `{"QMP":{"version":{}}}`),
		qmpTestExchange("send", now.Add(time.Millisecond), `{"execute":"qmp_capabilities","id":"kill-capabilities"}`),
		qmpTestExchange("receive", now.Add(2*time.Millisecond), `{"return":{},"id":"kill-capabilities"}`),
		qmpTestExchange("send", now.Add(3*time.Millisecond), `{"execute":"query-block","id":"kill-query-block"}`),
		qmpTestExchange("receive", now.Add(4*time.Millisecond), `{"return":[{"inserted":{"ro":false,"file":"/target.img","cache":{"direct":true,"no-flush":false}}}],"id":"kill-query-block"}`),
	}
	restart := []destructiveQMPExchange{
		qmpTestExchange("receive", now.Add(8*time.Millisecond), `{"QMP":{"version":{}}}`),
		qmpTestExchange("send", now.Add(9*time.Millisecond), `{"execute":"qmp_capabilities","id":"restart-capabilities"}`),
		qmpTestExchange("receive", now.Add(10*time.Millisecond), `{"return":{},"id":"restart-capabilities"}`),
		qmpTestExchange("send", now.Add(11*time.Millisecond), `{"execute":"query-status","id":"restart-query-status"}`),
		qmpTestExchange("receive", now.Add(12*time.Millisecond), `{"return":{"running":false,"status":"prelaunch"},"id":"restart-query-status"}`),
		qmpTestExchange("receive", now.Add(12500*time.Microsecond), `{"event":"DEVICE_TRAY_MOVED","data":{"device":"unused"}}`),
		qmpTestExchange("send", now.Add(13*time.Millisecond), `{"execute":"cont","id":"restart-cont"}`),
		qmpTestExchange("receive", now.Add(13500*time.Microsecond), `{"event":"RESUME","timestamp":{"seconds":1,"microseconds":2}}`),
		qmpTestExchange("receive", now.Add(14*time.Millisecond), `{"return":{},"id":"restart-cont"}`),
	}
	proof := destructiveQMPKillProof{
		SchemaVersion: destructiveQMPKillProofSchema, MarkerSHA256: qualificationSHA256(markerRaw),
		QEMUExecutableSHA256: strings.Repeat("11", 32), QEMUArgumentsSHA256: strings.Repeat("22", 32),
		TargetImage: "/target.img",
		StartedAt:   now, MarkerObservedAt: now.Add(5 * time.Millisecond), KillRequestedAt: now.Add(6 * time.Millisecond),
		KilledProcessExitedAt: now.Add(7 * time.Millisecond), RestartContinuedAt: now.Add(13 * time.Millisecond),
		FinishedAt: now.Add(15 * time.Millisecond), KilledPID: 100, RestartPID: 101,
		WaitStatus: uint32(syscall.SIGKILL), Signal: "SIGKILL", BeforeKillQMPExchanges: before, RestartQMPExchanges: restart,
	}
	raw, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	event := destructivePowerControllerEvent{
		Method: "qemu-host-sigkill", MarkerSHA256: qualificationSHA256(markerRaw), MarkerObservedAt: proof.MarkerObservedAt,
		CutRequestedAt: proof.KillRequestedAt, PowerRestoredAt: proof.RestartContinuedAt, ControllerProofSHA256: qualificationSHA256(raw),
	}
	if err := validateDestructiveQMPKillProof(raw, event, markerRaw); err != nil {
		t.Fatal(err)
	}
	proof.WaitStatus = 0
	tampered, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	event.ControllerProofSHA256 = qualificationSHA256(tampered)
	if err := validateDestructiveQMPKillProof(tampered, event, markerRaw); err == nil {
		t.Fatal("non-SIGKILL QEMU termination proof was accepted")
	}
}

func TestValidateQMPBlockInventoryRequiresDirectIOAndFlushes(t *testing.T) {
	for name, raw := range map[string]json.RawMessage{
		"host cache":   json.RawMessage(`[{"inserted":{"ro":false,"file":"/target.img","cache":{"direct":false,"no-flush":false}}}]`),
		"no flush":     json.RawMessage(`[{"inserted":{"ro":false,"file":"/target.img","cache":{"direct":true,"no-flush":true}}}]`),
		"read only":    json.RawMessage(`[{"inserted":{"ro":true,"file":"/target.img","cache":{"direct":true,"no-flush":false}}}]`),
		"missing file": json.RawMessage(`[{"inserted":{"ro":false,"cache":{"direct":true,"no-flush":false}}}]`),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateQMPBlockInventory(raw); err == nil {
				t.Fatal("unsafe QMP block inventory was accepted")
			}
		})
	}
	safe := json.RawMessage(`[{"inserted":{"ro":false,"file":"/target.img","cache":{"direct":true,"no-flush":false}}}]`)
	if err := validateQMPBlockInventory(safe, "/different.img"); err == nil {
		t.Fatal("QMP inventory for a different writable image was accepted")
	}
}

func qmpTestExchange(direction string, at time.Time, message string) destructiveQMPExchange {
	return destructiveQMPExchange{Direction: direction, At: at, Message: json.RawMessage(message)}
}
