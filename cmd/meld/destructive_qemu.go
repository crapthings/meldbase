package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

const destructiveQMPProofSchema uint32 = 1

type destructiveQMPExchange struct {
	Direction string          `json:"direction"`
	At        time.Time       `json:"at"`
	Message   json.RawMessage `json:"message"`
}

type destructiveQMPProof struct {
	SchemaVersion uint32                   `json:"schemaVersion"`
	MarkerSHA256  string                   `json:"markerSha256"`
	StartedAt     time.Time                `json:"startedAt"`
	FinishedAt    time.Time                `json:"finishedAt"`
	Exchanges     []destructiveQMPExchange `json:"exchanges"`
}

type qmpMessageEnvelope struct {
	QMP    json.RawMessage `json:"QMP,omitempty"`
	Return json.RawMessage `json:"return,omitempty"`
	Error  json.RawMessage `json:"error,omitempty"`
	Event  string          `json:"event,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
	ID     string          `json:"id,omitempty"`
}

func runDestructiveQEMUReset(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-qemu-reset", flag.ContinueOnError)
	flags.SetOutput(stderr)
	markerPath := flags.String("marker", "", "durable power-prepare marker shared with the QEMU host")
	qmpSocket := flags.String("qmp-socket", "", "QEMU QMP Unix socket")
	proofPath := flags.String("proof", "", "new durable QMP transcript path beside the marker")
	outputPath := flags.String("out", "", "new durable controller-event path beside the marker")
	timeout := flags.Duration("timeout", 2*time.Minute, "maximum time to wait for marker and QMP reset evidence")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *markerPath == "" || *qmpSocket == "" || *proofPath == "" || *outputPath == "" || *timeout < time.Second || *timeout > 30*time.Minute {
		return errors.New("destructive-qemu-reset requires --marker, --qmp-socket, --proof, --out and a timeout from 1s to 30m")
	}
	markerClean, err := filepath.Abs(filepath.Clean(*markerPath))
	if err != nil {
		return err
	}
	controlDirectory := filepath.Dir(markerClean)
	paths := make([]string, 2)
	for index, value := range []string{*proofPath, *outputPath} {
		absolute, err := filepath.Abs(filepath.Clean(value))
		if err != nil || filepath.Dir(absolute) != controlDirectory || absolute == markerClean {
			return errors.New("QMP proof and controller event must be distinct new files directly beside the marker")
		}
		if _, err := os.Lstat(absolute); err == nil {
			return fmt.Errorf("QMP output already exists: %s", absolute)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		paths[index] = absolute
	}
	proofClean, outputClean := paths[0], paths[1]
	if proofClean == outputClean {
		return errors.New("QMP proof and controller event paths must be different")
	}
	qmpClean, err := filepath.Abs(filepath.Clean(*qmpSocket))
	if err != nil {
		return err
	}
	deadline := time.Now().Add(*timeout)
	marker, markerRaw, err := waitForPowerMarker(markerClean, deadline)
	if err != nil {
		return err
	}
	if marker.SchemaVersion != destructivePowerMarkerSchema || !qualificationSafeName(marker.TrialID, 64) ||
		!qualificationBoundaryAllowed(qualificationTrialPower, marker.Boundary) || marker.BootIDBefore == "" || marker.ReachedAt.IsZero() {
		return errors.New("QMP controller received an invalid power marker")
	}
	markerObserved := time.Now().UTC()

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return errors.New("QMP controller timed out before connecting")
	}
	connection, err := net.DialTimeout("unix", qmpClean, remaining)
	if err != nil {
		return fmt.Errorf("connect QMP: %w", err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(deadline); err != nil {
		return err
	}
	decoder := json.NewDecoder(io.LimitReader(connection, 4*qualificationReceiptMaxBytes))
	writer := bufio.NewWriter(connection)
	proof := destructiveQMPProof{
		SchemaVersion: destructiveQMPProofSchema, MarkerSHA256: qualificationSHA256(markerRaw), StartedAt: time.Now().UTC(),
	}
	receive := func() (json.RawMessage, qmpMessageEnvelope, error) {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, qmpMessageEnvelope{}, err
		}
		if len(raw) == 0 || len(raw) > qualificationReceiptMaxBytes {
			return nil, qmpMessageEnvelope{}, errors.New("QMP message is empty or oversized")
		}
		var envelope qmpMessageEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, qmpMessageEnvelope{}, err
		}
		proof.Exchanges = append(proof.Exchanges, destructiveQMPExchange{Direction: "receive", At: time.Now().UTC(), Message: raw})
		return raw, envelope, nil
	}
	send := func(command map[string]string) error {
		raw, err := json.Marshal(command)
		if err != nil {
			return err
		}
		proof.Exchanges = append(proof.Exchanges, destructiveQMPExchange{Direction: "send", At: time.Now().UTC(), Message: raw})
		if _, err := writer.Write(append(raw, '\n')); err != nil {
			return err
		}
		return writer.Flush()
	}
	_, greeting, err := receive()
	if err != nil || len(greeting.QMP) == 0 {
		return errors.Join(errors.New("QMP greeting is missing"), err)
	}
	if err := send(map[string]string{"execute": "qmp_capabilities", "id": "meld-capabilities"}); err != nil {
		return err
	}
	if err := receiveQMPResponse(receive, "meld-capabilities", nil); err != nil {
		return err
	}
	if err := send(map[string]string{"execute": "query-block", "id": "meld-query-block"}); err != nil {
		return err
	}
	if err := receiveQMPResponse(receive, "meld-query-block", func(envelope qmpMessageEnvelope) error {
		return validateQMPBlockInventory(envelope.Return)
	}); err != nil {
		return err
	}
	if err := send(map[string]string{"execute": "system_reset", "id": "meld-system-reset"}); err != nil {
		return err
	}
	cutRequested := proof.Exchanges[len(proof.Exchanges)-1].At
	resetResponse, resetEvent := false, false
	var restoredAt time.Time
	for !resetResponse || !resetEvent {
		_, envelope, err := receive()
		if err != nil {
			return fmt.Errorf("wait for QMP reset evidence: %w", err)
		}
		if envelope.ID == "meld-system-reset" {
			if len(envelope.Error) != 0 || len(envelope.Return) == 0 {
				return errors.New("QMP system_reset returned an error or empty response")
			}
			resetResponse = true
		}
		if envelope.Event == "RESET" {
			var data struct {
				Guest  bool   `json:"guest"`
				Reason string `json:"reason"`
			}
			if err := json.Unmarshal(envelope.Data, &data); err != nil || data.Guest || data.Reason == "" {
				return errors.New("QMP RESET event was not a host hard reset")
			}
			resetEvent, restoredAt = true, proof.Exchanges[len(proof.Exchanges)-1].At
		}
	}
	proof.FinishedAt = time.Now().UTC()
	if err := validateQMPProofStructure(proof); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(proofClean, proof); err != nil {
		return err
	}
	proofRaw, err := os.ReadFile(proofClean)
	if err != nil {
		return err
	}
	event := destructivePowerControllerEvent{
		SchemaVersion: destructivePowerEventSchema, TrialID: marker.TrialID, Method: "qemu-system-reset",
		MarkerSHA256: qualificationSHA256(markerRaw), BootIDBefore: marker.BootIDBefore,
		MarkerObservedAt: markerObserved, CutRequestedAt: cutRequested, PowerRestoredAt: restoredAt,
		ControllerProofSHA256: qualificationSHA256(proofRaw),
	}
	if err := validateDestructiveQMPProof(proofRaw, event, markerRaw); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(outputClean, event); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(event)
}

func receiveQMPResponse(receive func() (json.RawMessage, qmpMessageEnvelope, error), id string, validate func(qmpMessageEnvelope) error) error {
	for {
		_, envelope, err := receive()
		if err != nil {
			return err
		}
		if envelope.ID != id {
			continue
		}
		if len(envelope.Error) != 0 {
			return fmt.Errorf("QMP command %s returned an error: %s", id, envelope.Error)
		}
		if len(envelope.Return) == 0 {
			return fmt.Errorf("QMP command %s returned an empty response", id)
		}
		if validate != nil {
			return validate(envelope)
		}
		return nil
	}
}

func waitForPowerMarker(path string, deadline time.Time) (destructivePowerMarker, []byte, error) {
	for time.Now().Before(deadline) {
		var marker destructivePowerMarker
		raw, err := readQualificationReceipt(path, &marker)
		if err == nil {
			return marker, raw, nil
		}
		// The durable writer uses O_EXCL followed by write+fsync, so filesystems
		// such as 9P may briefly expose the new inode before its JSON bytes.
		// Treat every read/parse failure as unpublished until the deadline; the
		// final error remains fail-closed and no reset is sent in this state.
		time.Sleep(2 * time.Millisecond)
	}
	return destructivePowerMarker{}, nil, errors.New("timed out waiting for durable power marker")
}

func validateDestructiveQMPProof(raw []byte, event destructivePowerControllerEvent, markerRaw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var proof destructiveQMPProof
	if err := decoder.Decode(&proof); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("QMP proof contains trailing JSON")
	}
	if qualificationSHA256(raw) != event.ControllerProofSHA256 || proof.MarkerSHA256 != qualificationSHA256(markerRaw) ||
		event.Method != "qemu-system-reset" || event.MarkerObservedAt.IsZero() || event.MarkerObservedAt.After(proof.StartedAt) ||
		event.CutRequestedAt.Before(event.MarkerObservedAt) {
		return errors.New("QMP proof is not bound to the marker and controller event")
	}
	if err := validateQMPProofStructure(proof); err != nil {
		return err
	}
	var resetSentAt, resetReceivedAt time.Time
	for _, exchange := range proof.Exchanges {
		var command struct {
			Execute string `json:"execute"`
			ID      string `json:"id"`
		}
		var envelope qmpMessageEnvelope
		if exchange.Direction == "send" {
			_ = json.Unmarshal(exchange.Message, &command)
			if command.Execute == "system_reset" && command.ID == "meld-system-reset" {
				resetSentAt = exchange.At
			}
		} else {
			_ = json.Unmarshal(exchange.Message, &envelope)
			if envelope.Event == "RESET" {
				var data struct {
					Guest bool `json:"guest"`
				}
				if json.Unmarshal(envelope.Data, &data) == nil && !data.Guest {
					resetReceivedAt = exchange.At
				}
			}
		}
	}
	if !event.CutRequestedAt.Equal(resetSentAt) || !event.PowerRestoredAt.Equal(resetReceivedAt) {
		return errors.New("QMP controller timestamps do not match its transcript")
	}
	return nil
}

func validateQMPProofStructure(proof destructiveQMPProof) error {
	if proof.SchemaVersion != destructiveQMPProofSchema || !qualificationHexDigest(proof.MarkerSHA256) || proof.StartedAt.IsZero() ||
		!proof.FinishedAt.After(proof.StartedAt) || len(proof.Exchanges) < 8 || len(proof.Exchanges) > 1024 {
		return errors.New("QMP proof identity, timing or exchange count is invalid")
	}
	wantCommands := []struct{ execute, id string }{{"qmp_capabilities", "meld-capabilities"}, {"query-block", "meld-query-block"}, {"system_reset", "meld-system-reset"}}
	commandIndex := 0
	stage := 0
	greeting, capabilities, blocks, resetResponse, resetEvent := false, false, false, false, false
	for index, exchange := range proof.Exchanges {
		if exchange.At.Before(proof.StartedAt) || exchange.At.After(proof.FinishedAt) || len(exchange.Message) == 0 || len(exchange.Message) > qualificationReceiptMaxBytes {
			return fmt.Errorf("QMP exchange %d has invalid timing or size", index+1)
		}
		switch exchange.Direction {
		case "send":
			var command struct {
				Execute string `json:"execute"`
				ID      string `json:"id"`
			}
			if err := json.Unmarshal(exchange.Message, &command); err != nil || commandIndex >= len(wantCommands) ||
				command.Execute != wantCommands[commandIndex].execute || command.ID != wantCommands[commandIndex].id {
				return fmt.Errorf("QMP exchange %d contains an unexpected command", index+1)
			}
			wantStage := 1 + 2*commandIndex
			if stage != wantStage {
				return fmt.Errorf("QMP exchange %d sends a command before its prerequisite response", index+1)
			}
			commandIndex++
			stage++
		case "receive":
			var envelope qmpMessageEnvelope
			if err := json.Unmarshal(exchange.Message, &envelope); err != nil {
				return err
			}
			if index == 0 && len(envelope.QMP) != 0 {
				greeting = true
				stage = 1
			}
			if envelope.ID == "meld-capabilities" && len(envelope.Return) != 0 && len(envelope.Error) == 0 {
				if stage != 2 || capabilities {
					return errors.New("QMP capabilities response is out of order or duplicated")
				}
				capabilities = true
				stage = 3
			}
			if envelope.ID == "meld-query-block" && len(envelope.Return) != 0 && len(envelope.Error) == 0 {
				if stage != 4 || blocks {
					return errors.New("QMP block inventory response is out of order or duplicated")
				}
				blocks = validateQMPBlockInventory(envelope.Return) == nil
				if blocks {
					stage = 5
				}
			}
			if envelope.ID == "meld-system-reset" && len(envelope.Return) != 0 && len(envelope.Error) == 0 {
				if stage != 6 || resetResponse {
					return errors.New("QMP reset response is out of order or duplicated")
				}
				resetResponse = true
			}
			if envelope.Event == "RESET" {
				if stage != 6 || resetEvent {
					return errors.New("QMP RESET event is out of order or duplicated")
				}
				var data struct {
					Guest  bool   `json:"guest"`
					Reason string `json:"reason"`
				}
				resetEvent = json.Unmarshal(envelope.Data, &data) == nil && !data.Guest && data.Reason != ""
			}
		default:
			return fmt.Errorf("QMP exchange %d has invalid direction", index+1)
		}
	}
	if stage != 6 || commandIndex != len(wantCommands) || !greeting || !capabilities || !blocks || !resetResponse || !resetEvent {
		return errors.New("QMP proof lacks greeting, ordered commands, block inventory, reset response or host RESET event")
	}
	return nil
}

func validateQMPBlockInventory(raw json.RawMessage, expectedFile ...string) error {
	var devices []struct {
		Inserted *struct {
			ReadOnly bool   `json:"ro"`
			File     string `json:"file"`
			Cache    struct {
				Direct  bool `json:"direct"`
				NoFlush bool `json:"no-flush"`
			} `json:"cache"`
		} `json:"inserted"`
	}
	if err := json.Unmarshal(raw, &devices); err != nil || len(devices) == 0 {
		return errors.New("QMP query-block did not report any block devices")
	}
	for _, device := range devices {
		if device.Inserted != nil && !device.Inserted.ReadOnly && device.Inserted.File != "" &&
			device.Inserted.Cache.Direct && !device.Inserted.Cache.NoFlush &&
			(len(expectedFile) == 0 || filepath.Clean(device.Inserted.File) == filepath.Clean(expectedFile[0])) {
			return nil
		}
	}
	return errors.New("QMP query-block lacks the expected writable direct-I/O device with flushes enabled")
}
