package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

const destructiveQMPKillProofSchema uint32 = 2

type destructiveQMPKillProof struct {
	SchemaVersion          uint32                   `json:"schemaVersion"`
	MarkerSHA256           string                   `json:"markerSha256"`
	QEMUExecutableSHA256   string                   `json:"qemuExecutableSha256"`
	QEMUArgumentsSHA256    string                   `json:"qemuArgumentsSha256"`
	TargetImage            string                   `json:"targetImage"`
	StartedAt              time.Time                `json:"startedAt"`
	MarkerObservedAt       time.Time                `json:"markerObservedAt"`
	KillRequestedAt        time.Time                `json:"killRequestedAt"`
	KilledProcessExitedAt  time.Time                `json:"killedProcessExitedAt"`
	RestartContinuedAt     time.Time                `json:"restartContinuedAt"`
	FinishedAt             time.Time                `json:"finishedAt"`
	KilledPID              int                      `json:"killedPid"`
	RestartPID             int                      `json:"restartPid"`
	WaitStatus             uint32                   `json:"waitStatus"`
	Signal                 string                   `json:"signal"`
	BeforeKillQMPExchanges []destructiveQMPExchange `json:"beforeKillQmpExchanges"`
	RestartQMPExchanges    []destructiveQMPExchange `json:"restartQmpExchanges"`
}

type qmpRecordingClient struct {
	connection net.Conn
	decoder    *json.Decoder
	writer     *bufio.Writer
	exchanges  []destructiveQMPExchange
}

func runDestructiveQEMUProcessKill(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-qemu-process-kill", flag.ContinueOnError)
	flags.SetOutput(stderr)
	markerPath := flags.String("marker", "", "durable power-prepare marker shared with the QEMU host")
	qmpSocket := flags.String("qmp-socket", "", "QEMU QMP Unix socket configured in the child arguments")
	proofPath := flags.String("proof", "", "new durable QEMU process-kill proof beside the marker")
	outputPath := flags.String("out", "", "new durable controller event beside the marker")
	qemuLogPath := flags.String("qemu-log", "", "new QEMU stdout/stderr log beside the marker")
	recoveryPath := flags.String("recovery-receipt", "", "power recovery receipt produced by the restarted guest")
	targetImage := flags.String("target-image", "", "exact writable raw image that must use direct I/O with flushes enabled")
	artifactUID := flags.Int("artifact-uid", -1, "optional uid that must read the private controller artifacts")
	artifactGID := flags.Int("artifact-gid", -1, "optional gid that must read the private controller artifacts")
	timeout := flags.Duration("timeout", 5*time.Minute, "total launch, kill, restart and recovery deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	qemuCommand := flags.Args()
	if *markerPath == "" || *qmpSocket == "" || *proofPath == "" || *outputPath == "" || *qemuLogPath == "" ||
		*recoveryPath == "" || *targetImage == "" || len(qemuCommand) < 2 || *timeout < 10*time.Second || *timeout > 30*time.Minute {
		return errors.New("destructive-qemu-process-kill requires marker, QMP, proof, event, log, recovery, timeout and a QEMU command after --")
	}
	if (*artifactUID < 0) != (*artifactGID < 0) || *artifactUID > 1<<31-1 || *artifactGID > 1<<31-1 {
		return errors.New("artifact uid and gid must both be omitted or both be non-negative 32-bit values")
	}
	markerClean, err := filepath.Abs(filepath.Clean(*markerPath))
	if err != nil {
		return err
	}
	controlDirectory := filepath.Dir(markerClean)
	cleanPaths := make([]string, 5)
	for index, value := range []string{*qmpSocket, *proofPath, *outputPath, *qemuLogPath, *recoveryPath} {
		absolute, err := filepath.Abs(filepath.Clean(value))
		if err != nil || filepath.Dir(absolute) != controlDirectory || absolute == markerClean {
			return errors.New("QMP socket, proof, event, log and recovery receipt must be distinct paths directly beside the marker")
		}
		cleanPaths[index] = absolute
	}
	qmpClean, proofClean, outputClean, logClean, recoveryClean := cleanPaths[0], cleanPaths[1], cleanPaths[2], cleanPaths[3], cleanPaths[4]
	targetImageClean, err := filepath.Abs(filepath.Clean(*targetImage))
	if err != nil {
		return err
	}
	if info, err := os.Stat(targetImageClean); err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return errors.New("QEMU process-kill target image must be an existing nonempty regular file")
	}
	seenPaths := map[string]struct{}{markerClean: {}}
	for _, path := range cleanPaths {
		if _, exists := seenPaths[path]; exists {
			return errors.New("QEMU process-kill paths must be distinct")
		}
		seenPaths[path] = struct{}{}
	}
	for _, path := range []string{proofClean, outputClean, logClean, recoveryClean} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("QEMU process-kill output already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if !qemuArgumentsBindSocket(qemuCommand[1:], qmpClean) {
		return errors.New("QEMU child arguments must contain the exact --qmp-socket as -qmp unix:PATH,server=on,wait=off")
	}
	for _, argument := range qemuCommand[1:] {
		if argument == "-daemonize" || argument == "-S" || argument == "-snapshot" {
			return fmt.Errorf("QEMU process-kill rejects child argument %q", argument)
		}
	}
	qemuExecutable, err := filepath.Abs(filepath.Clean(qemuCommand[0]))
	if err != nil {
		return err
	}
	executableHash, err := hashRegularFile(qemuExecutable, 1<<30)
	if err != nil {
		return fmt.Errorf("QEMU executable: %w", err)
	}
	argumentsRaw, err := json.Marshal(qemuCommand[1:])
	if err != nil {
		return err
	}
	deadline := time.Now().Add(*timeout)
	logFile, err := os.OpenFile(logClean, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	_ = os.Remove(qmpClean)

	proof := destructiveQMPKillProof{
		SchemaVersion: destructiveQMPKillProofSchema, QEMUExecutableSHA256: executableHash,
		QEMUArgumentsSHA256: bytesSHA256(argumentsRaw), TargetImage: targetImageClean,
		StartedAt: time.Now().UTC(), Signal: "SIGKILL",
	}
	first, cancelFirst := qemuChildCommand(deadline, qemuExecutable, qemuCommand[1:], logFile)
	defer cancelFirst()
	if err := first.Start(); err != nil {
		return err
	}
	firstReaped := false
	defer func() {
		if !firstReaped {
			_ = first.Process.Kill()
			_ = first.Wait()
		}
	}()
	proof.KilledPID = first.Process.Pid
	beforeClient, err := openQMPRecordingClient(qmpClean, deadline)
	if err != nil {
		return err
	}
	if err := beforeClient.handshakeAndQueryBlock("kill", targetImageClean); err != nil {
		beforeClient.Close()
		return err
	}
	marker, markerRaw, err := waitForPowerMarker(markerClean, deadline)
	if err != nil {
		beforeClient.Close()
		return err
	}
	if marker.SchemaVersion != destructivePowerMarkerSchema || !qualificationSafeName(marker.TrialID, 64) ||
		!qualificationBoundaryAllowed(qualificationTrialPower, marker.Boundary) || marker.BootIDBefore == "" {
		beforeClient.Close()
		return errors.New("QEMU process-kill controller received an invalid marker")
	}
	proof.MarkerSHA256 = qualificationSHA256(markerRaw)
	proof.MarkerObservedAt = time.Now().UTC()
	proof.BeforeKillQMPExchanges = append([]destructiveQMPExchange(nil), beforeClient.exchanges...)
	proof.KillRequestedAt = time.Now().UTC()
	killErr := first.Process.Signal(os.Kill)
	waitErr := first.Wait()
	firstReaped = true
	beforeClient.Close()
	proof.KilledProcessExitedAt = time.Now().UTC()
	if killErr != nil || !destructiveProcessWasKilled(waitErr) {
		return errors.Join(killErr, fmt.Errorf("QEMU process did not exit from SIGKILL: %w", waitErr))
	}
	var exitError *exec.ExitError
	if !errors.As(waitErr, &exitError) {
		return errors.New("QEMU SIGKILL did not produce an exit status")
	}
	waitStatus, ok := exitError.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		return errors.New("QEMU SIGKILL wait status is unavailable")
	}
	proof.WaitStatus = uint32(waitStatus)
	if err := os.Remove(qmpClean); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	restartArgs := append(append([]string(nil), qemuCommand[1:]...), "-S")
	restart, cancelRestart := qemuChildCommand(deadline, qemuExecutable, restartArgs, logFile)
	defer cancelRestart()
	if err := restart.Start(); err != nil {
		return err
	}
	restartReaped := false
	defer func() {
		if !restartReaped {
			_ = restart.Process.Kill()
			_ = restart.Wait()
		}
	}()
	proof.RestartPID = restart.Process.Pid
	restartClient, err := openQMPRecordingClient(qmpClean, deadline)
	if err != nil {
		return err
	}
	if err := restartClient.handshake("restart"); err != nil {
		restartClient.Close()
		return err
	}
	if err := restartClient.queryStoppedStatus(); err != nil {
		restartClient.Close()
		return err
	}
	proof.RestartContinuedAt = time.Now().UTC()
	if err := restartClient.send(map[string]string{"execute": "cont", "id": "restart-cont"}); err != nil {
		restartClient.Close()
		return err
	}
	proof.RestartContinuedAt = restartClient.exchanges[len(restartClient.exchanges)-1].At
	if err := receiveQMPResponse(restartClient.receive, "restart-cont", nil); err != nil {
		restartClient.Close()
		return err
	}
	proof.RestartQMPExchanges = append([]destructiveQMPExchange(nil), restartClient.exchanges...)
	restartClient.Close()
	proof.FinishedAt = time.Now().UTC()
	if err := validateQMPKillProofStructure(proof); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(proofClean, proof); err != nil {
		return err
	}
	if err := setQualificationArtifactOwner(proofClean, *artifactUID, *artifactGID); err != nil {
		return err
	}
	proofRaw, err := os.ReadFile(proofClean)
	if err != nil {
		return err
	}
	event := destructivePowerControllerEvent{
		SchemaVersion: destructivePowerEventSchema, TrialID: marker.TrialID, Method: "qemu-host-sigkill",
		MarkerSHA256: qualificationSHA256(markerRaw), BootIDBefore: marker.BootIDBefore,
		MarkerObservedAt: proof.MarkerObservedAt, CutRequestedAt: proof.KillRequestedAt,
		PowerRestoredAt: proof.RestartContinuedAt, ControllerProofSHA256: qualificationSHA256(proofRaw),
	}
	if err := validateDestructiveQMPKillProof(proofRaw, event, markerRaw); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(outputClean, event); err != nil {
		return err
	}
	if err := setQualificationArtifactOwner(outputClean, *artifactUID, *artifactGID); err != nil {
		return err
	}
	if waitErr := restart.Wait(); waitErr != nil {
		restartReaped = true
		return fmt.Errorf("restarted QEMU did not exit cleanly after recovery: %w", waitErr)
	}
	restartReaped = true
	var receipt destructivePowerReceipt
	if _, err := readQualificationReceipt(recoveryClean, &receipt); err != nil {
		return fmt.Errorf("restarted guest recovery receipt: %w", err)
	}
	if err := validateDestructivePowerReceipt(receipt); err != nil {
		return fmt.Errorf("restarted guest recovery receipt: %w", err)
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(event)
}

func qemuChildCommand(deadline time.Time, executable string, args []string, log io.Writer) (*exec.Cmd, context.CancelFunc) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	command := exec.CommandContext(ctx, executable, args...)
	command.Stdout, command.Stderr = log, log
	return command, cancel
}

func openQMPRecordingClient(socket string, deadline time.Time) (*qmpRecordingClient, error) {
	var connection net.Conn
	var err error
	for time.Now().Before(deadline) {
		connection, err = net.DialTimeout("unix", socket, 250*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil || connection == nil {
		if err == nil {
			err = errors.New("QMP connection deadline expired")
		}
		return nil, fmt.Errorf("connect QMP: %w", err)
	}
	if err := connection.SetDeadline(deadline); err != nil {
		connection.Close()
		return nil, err
	}
	return &qmpRecordingClient{
		connection: connection, decoder: json.NewDecoder(io.LimitReader(connection, 4*qualificationReceiptMaxBytes)),
		writer: bufio.NewWriter(connection),
	}, nil
}

func (client *qmpRecordingClient) Close() error { return client.connection.Close() }

func (client *qmpRecordingClient) receive() (json.RawMessage, qmpMessageEnvelope, error) {
	var raw json.RawMessage
	if err := client.decoder.Decode(&raw); err != nil {
		return nil, qmpMessageEnvelope{}, err
	}
	if len(raw) == 0 || len(raw) > qualificationReceiptMaxBytes {
		return nil, qmpMessageEnvelope{}, errors.New("QMP message is empty or oversized")
	}
	var envelope qmpMessageEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, qmpMessageEnvelope{}, err
	}
	client.exchanges = append(client.exchanges, destructiveQMPExchange{Direction: "receive", At: time.Now().UTC(), Message: raw})
	return raw, envelope, nil
}

func (client *qmpRecordingClient) send(command map[string]string) error {
	return client.sendValue(command)
}

func (client *qmpRecordingClient) sendValue(command any) error {
	raw, err := json.Marshal(command)
	if err != nil {
		return err
	}
	client.exchanges = append(client.exchanges, destructiveQMPExchange{Direction: "send", At: time.Now().UTC(), Message: raw})
	if _, err := client.writer.Write(append(raw, '\n')); err != nil {
		return err
	}
	return client.writer.Flush()
}

func (client *qmpRecordingClient) handshake(prefix string) error {
	_, greeting, err := client.receive()
	if err != nil || len(greeting.QMP) == 0 {
		return errors.Join(errors.New("QMP greeting is missing"), err)
	}
	id := prefix + "-capabilities"
	if err := client.send(map[string]string{"execute": "qmp_capabilities", "id": id}); err != nil {
		return err
	}
	return receiveQMPResponse(client.receive, id, nil)
}

func (client *qmpRecordingClient) handshakeAndQueryBlock(prefix, targetImage string) error {
	if err := client.handshake(prefix); err != nil {
		return err
	}
	id := prefix + "-query-block"
	if err := client.send(map[string]string{"execute": "query-block", "id": id}); err != nil {
		return err
	}
	return receiveQMPResponse(client.receive, id, func(envelope qmpMessageEnvelope) error {
		return validateQMPBlockInventory(envelope.Return, targetImage)
	})
}

func (client *qmpRecordingClient) queryStoppedStatus() error {
	if err := client.send(map[string]string{"execute": "query-status", "id": "restart-query-status"}); err != nil {
		return err
	}
	return receiveQMPResponse(client.receive, "restart-query-status", func(envelope qmpMessageEnvelope) error {
		var status struct {
			Running bool   `json:"running"`
			Status  string `json:"status"`
		}
		if err := json.Unmarshal(envelope.Return, &status); err != nil || status.Running || status.Status == "" {
			return errors.New("restarted QEMU was not paused before evidence publication")
		}
		return nil
	})
}

func qemuArgumentsBindSocket(args []string, socket string) bool {
	want := "unix:" + socket + ",server=on,wait=off"
	for index, argument := range args {
		if argument == "-qmp" && index+1 < len(args) && args[index+1] == want || argument == "-qmp="+want {
			return true
		}
	}
	return false
}

func setQualificationArtifactOwner(path string, uid, gid int) error {
	if uid < 0 {
		return nil
	}
	if err := os.Chown(path, uid, gid); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	fileErr := errors.Join(file.Sync(), file.Close())
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.Join(fileErr, err)
	}
	return errors.Join(fileErr, directory.Sync(), directory.Close())
}

func validateDestructiveQMPKillProof(raw []byte, event destructivePowerControllerEvent, markerRaw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var proof destructiveQMPKillProof
	if err := decoder.Decode(&proof); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("QEMU process-kill proof contains trailing JSON")
	}
	if qualificationSHA256(raw) != event.ControllerProofSHA256 || proof.MarkerSHA256 != qualificationSHA256(markerRaw) ||
		event.Method != "qemu-host-sigkill" || !event.MarkerObservedAt.Equal(proof.MarkerObservedAt) ||
		!event.CutRequestedAt.Equal(proof.KillRequestedAt) || !event.PowerRestoredAt.Equal(proof.RestartContinuedAt) {
		return errors.New("QEMU process-kill proof is not bound to the marker and controller event")
	}
	return validateQMPKillProofStructure(proof)
}

func validateQMPKillProofStructure(proof destructiveQMPKillProof) error {
	waitStatus := syscall.WaitStatus(proof.WaitStatus)
	if proof.SchemaVersion != destructiveQMPKillProofSchema || !qualificationHexDigest(proof.MarkerSHA256) ||
		!qualificationHexDigest(proof.QEMUExecutableSHA256) || !qualificationHexDigest(proof.QEMUArgumentsSHA256) ||
		!filepath.IsAbs(proof.TargetImage) || filepath.Clean(proof.TargetImage) != proof.TargetImage ||
		proof.StartedAt.IsZero() || proof.MarkerObservedAt.Before(proof.StartedAt) || proof.KillRequestedAt.Before(proof.MarkerObservedAt) ||
		proof.KilledProcessExitedAt.Before(proof.KillRequestedAt) || proof.RestartContinuedAt.Before(proof.KilledProcessExitedAt) ||
		proof.FinishedAt.Before(proof.RestartContinuedAt) || proof.KilledPID <= 0 || proof.RestartPID <= 0 ||
		proof.Signal != "SIGKILL" || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		return errors.New("QEMU process-kill proof identity, timing or SIGKILL wait status is invalid")
	}
	if err := validateQMPExchangeTimeline(proof.BeforeKillQMPExchanges, proof.StartedAt, proof.MarkerObservedAt); err != nil {
		return err
	}
	if err := validateQMPExchangeTimeline(proof.RestartQMPExchanges, proof.KilledProcessExitedAt, proof.FinishedAt); err != nil {
		return err
	}
	if err := validateQMPKillBeforeExchanges(proof.BeforeKillQMPExchanges, proof.TargetImage); err != nil {
		return err
	}
	if err := validateQMPRestartExchanges(proof.RestartQMPExchanges); err != nil {
		return err
	}
	var continuedAt time.Time
	for _, exchange := range proof.RestartQMPExchanges {
		if exchange.Direction != "send" {
			continue
		}
		var command struct {
			Execute string `json:"execute"`
			ID      string `json:"id"`
		}
		if json.Unmarshal(exchange.Message, &command) == nil && command.Execute == "cont" && command.ID == "restart-cont" {
			continuedAt = exchange.At
		}
	}
	if !continuedAt.Equal(proof.RestartContinuedAt) {
		return errors.New("QEMU process-kill restart timestamp does not match its cont command")
	}
	return nil
}

func validateQMPKillBeforeExchanges(exchanges []destructiveQMPExchange, targetImage string) error {
	return validateQMPOrderedExchanges(exchanges, []qmpExpectedCommand{
		{execute: "qmp_capabilities", id: "kill-capabilities"},
		{execute: "query-block", id: "kill-query-block", validate: func(envelope qmpMessageEnvelope) error {
			return validateQMPBlockInventory(envelope.Return, targetImage)
		}},
	})
}

func validateQMPExchangeTimeline(exchanges []destructiveQMPExchange, earliest, latest time.Time) error {
	previous := earliest
	for index, exchange := range exchanges {
		if exchange.At.IsZero() || exchange.At.Before(previous) || exchange.At.Before(earliest) || exchange.At.After(latest) {
			return fmt.Errorf("QMP process-kill exchange %d has an invalid or non-monotonic timestamp", index+1)
		}
		previous = exchange.At
	}
	return nil
}

func validateQMPRestartExchanges(exchanges []destructiveQMPExchange) error {
	return validateQMPOrderedExchanges(exchanges, []qmpExpectedCommand{
		{execute: "qmp_capabilities", id: "restart-capabilities"},
		{execute: "query-status", id: "restart-query-status", validate: func(envelope qmpMessageEnvelope) error {
			var status struct {
				Running bool   `json:"running"`
				Status  string `json:"status"`
			}
			if err := json.Unmarshal(envelope.Return, &status); err != nil || status.Running || status.Status == "" {
				return errors.New("QEMU restart proof does not show a paused VM")
			}
			return nil
		}},
		{execute: "cont", id: "restart-cont"},
	})
}

type qmpExpectedCommand struct {
	execute  string
	id       string
	validate func(qmpMessageEnvelope) error
}

func validateQMPOrderedExchanges(exchanges []destructiveQMPExchange, commands []qmpExpectedCommand) error {
	if len(exchanges) < 1+2*len(commands) || len(exchanges) > 64 {
		return errors.New("QMP process-kill exchange count is invalid")
	}
	var greeting qmpMessageEnvelope
	if exchanges[0].Direction != "receive" || json.Unmarshal(exchanges[0].Message, &greeting) != nil || len(greeting.QMP) == 0 {
		return errors.New("QMP process-kill proof lacks its greeting")
	}
	stage := 0
	expectSend := true
	for index := 1; index < len(exchanges); index++ {
		exchange := exchanges[index]
		if stage >= len(commands) {
			if exchange.Direction == "receive" {
				var trailing qmpMessageEnvelope
				if json.Unmarshal(exchange.Message, &trailing) == nil && trailing.Event != "" && trailing.ID == "" {
					continue
				}
			}
			return errors.New("QMP process-kill proof contains data after its final response")
		}
		command := commands[stage]
		if expectSend {
			if exchange.Direction == "receive" {
				var asynchronous qmpMessageEnvelope
				if json.Unmarshal(exchange.Message, &asynchronous) == nil && asynchronous.Event != "" && asynchronous.ID == "" {
					continue
				}
			}
			var sent struct {
				Execute string `json:"execute"`
				ID      string `json:"id"`
			}
			if exchange.Direction != "send" || json.Unmarshal(exchange.Message, &sent) != nil || sent.Execute != command.execute || sent.ID != command.id {
				return fmt.Errorf("QMP process-kill command %d is missing or out of order", stage+1)
			}
			expectSend = false
			continue
		}
		var response qmpMessageEnvelope
		if exchange.Direction != "receive" || json.Unmarshal(exchange.Message, &response) != nil {
			return fmt.Errorf("QMP process-kill response %d is missing or invalid", stage+1)
		}
		if response.ID == "" && response.Event != "" {
			continue
		}
		if response.ID != command.id || len(response.Error) != 0 || len(response.Return) == 0 {
			return fmt.Errorf("QMP process-kill response %d is missing or invalid", stage+1)
		}
		if command.validate != nil {
			if err := command.validate(response); err != nil {
				return err
			}
		}
		stage++
		expectSend = true
	}
	if stage != len(commands) || !expectSend {
		return errors.New("QMP process-kill proof is incomplete")
	}
	return nil
}
