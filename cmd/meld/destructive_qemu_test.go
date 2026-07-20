package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDestructiveQEMUResetCapturesHostResetProof(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "meld-qmp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socketPath := filepath.Join(directory, "qmp.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer connection.Close()
		encoder, decoder := json.NewEncoder(connection), json.NewDecoder(connection)
		if err := encoder.Encode(map[string]any{"QMP": map[string]any{"version": map[string]any{"qemu": map[string]int{"major": 10, "minor": 0, "micro": 3}}, "capabilities": []any{}}}); err != nil {
			serverDone <- err
			return
		}
		for {
			var command struct {
				Execute string `json:"execute"`
				ID      string `json:"id"`
			}
			if err := decoder.Decode(&command); err != nil {
				serverDone <- err
				return
			}
			switch command.Execute {
			case "qmp_capabilities":
				err = encoder.Encode(map[string]any{"return": map[string]any{}, "id": command.ID})
			case "query-block":
				err = encoder.Encode(map[string]any{"return": []any{map[string]any{"device": "qualification-target", "inserted": map[string]any{"ro": false, "file": "/target.img", "cache": map[string]bool{"direct": true, "no-flush": false}}}}, "id": command.ID})
			case "system_reset":
				if err = encoder.Encode(map[string]any{"event": "RESET", "data": map[string]any{"guest": false, "reason": "host-qmp-system-reset"}, "timestamp": map[string]int64{"seconds": time.Now().Unix(), "microseconds": 0}}); err == nil {
					err = encoder.Encode(map[string]any{"return": map[string]any{}, "id": command.ID})
				}
				serverDone <- err
				return
			default:
				serverDone <- errors.New("unexpected QMP command: " + command.Execute)
				return
			}
			if err != nil {
				serverDone <- err
				return
			}
		}
	}()

	started := time.Now().UTC().Add(-time.Minute)
	marker := destructivePowerMarker{
		SchemaVersion: destructivePowerMarkerSchema, TrialID: "qemu-power-001", Boundary: "after-meta-write",
		BootIDBefore: "11111111-1111-1111-1111-111111111111", StartedAt: started, ReachedAt: started.Add(time.Second),
	}
	markerPath := filepath.Join(directory, "marker.json")
	if err := writeJSONExclusiveDurable(markerPath, marker); err != nil {
		t.Fatal(err)
	}
	proofPath, eventPath := filepath.Join(directory, "qmp-proof.json"), filepath.Join(directory, "controller-event.json")
	var stdout, stderr bytes.Buffer
	if err := runDestructiveQEMUReset([]string{
		"--marker", markerPath, "--qmp-socket", socketPath, "--proof", proofPath, "--out", eventPath, "--timeout", "5s",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("controller=%v stderr=%s", err, stderr.String())
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	var event destructivePowerControllerEvent
	eventRaw, err := readQualificationReceipt(eventPath, &event)
	if err != nil || len(eventRaw) == 0 || event.Method != "qemu-system-reset" || event.TrialID != marker.TrialID {
		t.Fatalf("event=%+v raw=%d err=%v", event, len(eventRaw), err)
	}
	proofRaw, err := os.ReadFile(proofPath)
	if err != nil {
		t.Fatal(err)
	}
	markerRaw, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateDestructiveQMPProof(proofRaw, event, markerRaw); err != nil {
		t.Fatal(err)
	}
	proofRaw[len(proofRaw)/2] ^= 1
	if err := validateDestructiveQMPProof(proofRaw, event, markerRaw); err == nil {
		t.Fatal("tampered QMP transcript was accepted")
	}
}

func TestQMPProofRejectsGuestReset(t *testing.T) {
	now := time.Now().UTC()
	exchanges := []destructiveQMPExchange{
		{Direction: "receive", At: now, Message: json.RawMessage(`{"QMP":{"version":{}}}`)},
		{Direction: "send", At: now.Add(time.Millisecond), Message: json.RawMessage(`{"execute":"qmp_capabilities","id":"meld-capabilities"}`)},
		{Direction: "receive", At: now.Add(2 * time.Millisecond), Message: json.RawMessage(`{"return":{},"id":"meld-capabilities"}`)},
		{Direction: "send", At: now.Add(3 * time.Millisecond), Message: json.RawMessage(`{"execute":"query-block","id":"meld-query-block"}`)},
		{Direction: "receive", At: now.Add(4 * time.Millisecond), Message: json.RawMessage(`{"return":[{"device":"disk","inserted":{"ro":false,"file":"/target.img","cache":{"direct":true,"no-flush":false}}}],"id":"meld-query-block"}`)},
		{Direction: "send", At: now.Add(5 * time.Millisecond), Message: json.RawMessage(`{"execute":"system_reset","id":"meld-system-reset"}`)},
		{Direction: "receive", At: now.Add(6 * time.Millisecond), Message: json.RawMessage(`{"return":{},"id":"meld-system-reset"}`)},
		{Direction: "receive", At: now.Add(7 * time.Millisecond), Message: json.RawMessage(`{"event":"RESET","data":{"guest":true,"reason":"guest-reset"}}`)},
	}
	proof := destructiveQMPProof{SchemaVersion: destructiveQMPProofSchema, MarkerSHA256: bytesSHA256([]byte("marker")), StartedAt: now, FinishedAt: now.Add(time.Second), Exchanges: exchanges}
	if err := validateQMPProofStructure(proof); err == nil {
		t.Fatal("guest-originated RESET was accepted")
	}
}

func TestWaitForPowerMarkerRetriesVisibleEmptyInode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "marker.json")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	marker := destructivePowerMarker{
		SchemaVersion: destructivePowerMarkerSchema, TrialID: "visible-empty", Boundary: "after-data-sync",
		BootIDBefore: "11111111-1111-1111-1111-111111111111", ReachedAt: time.Now().UTC(),
	}
	written := make(chan error, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		raw, err := json.Marshal(marker)
		if err == nil {
			err = os.WriteFile(path, raw, 0o600)
		}
		written <- err
	}()
	actual, raw, err := waitForPowerMarker(path, time.Now().Add(time.Second))
	if err != nil || actual.TrialID != marker.TrialID || len(raw) == 0 {
		t.Fatalf("marker=%+v raw=%d err=%v", actual, len(raw), err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}
