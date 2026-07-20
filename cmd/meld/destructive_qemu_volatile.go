package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

const destructiveQMPVolatileLossProofSchema uint32 = 1

type destructiveVolatileLossControllerEvent struct {
	SchemaVersion       uint32    `json:"schemaVersion"`
	Method              string    `json:"method"`
	MarkerSHA256        string    `json:"markerSha256"`
	RecoveryReadySHA256 string    `json:"recoveryReadySha256"`
	ProofSHA256         string    `json:"proofSha256"`
	BootIDBefore        string    `json:"bootIdBefore"`
	BootIDAfter         string    `json:"bootIdAfter"`
	CutAt               time.Time `json:"cutAt"`
	RestartedAt         time.Time `json:"restartedAt"`
}

type destructiveQMPVolatileLossProof struct {
	SchemaVersion         uint32                   `json:"schemaVersion"`
	QEMUExecutable        string                   `json:"qemuExecutable"`
	QEMUExecutableSHA256  string                   `json:"qemuExecutableSha256"`
	QEMUArguments         []string                 `json:"qemuArguments"`
	QEMUArgumentsSHA256   string                   `json:"qemuArgumentsSha256"`
	BaseImage             string                   `json:"baseImage"`
	BaseImageArtifact     string                   `json:"baseImageArtifact"`
	BaseBeforeSHA256      string                   `json:"baseBeforeSha256"`
	BaseAfterKillSHA256   string                   `json:"baseAfterKillSha256"`
	MarkerSHA256          string                   `json:"markerSha256"`
	RecoveryReadySHA256   string                   `json:"recoveryReadySha256"`
	StartedAt             time.Time                `json:"startedAt"`
	MarkerObservedAt      time.Time                `json:"markerObservedAt"`
	KillRequestedAt       time.Time                `json:"killRequestedAt"`
	KilledProcessExitedAt time.Time                `json:"killedProcessExitedAt"`
	RestartContinuedAt    time.Time                `json:"restartContinuedAt"`
	RecoveryReadyAt       time.Time                `json:"recoveryReadyAt"`
	FinishedAt            time.Time                `json:"finishedAt"`
	KilledPID             int                      `json:"killedPid"`
	RestartPID            int                      `json:"restartPid"`
	WaitStatus            uint32                   `json:"waitStatus"`
	Signal                string                   `json:"signal"`
	SnapshotMode          string                   `json:"snapshotMode"`
	FirstQMPExchanges     []destructiveQMPExchange `json:"firstQmpExchanges"`
	RestartQMPExchanges   []destructiveQMPExchange `json:"restartQmpExchanges"`
}

func runDestructiveQEMUVolatileLoss(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-qemu-volatile-loss", flag.ContinueOnError)
	flags.SetOutput(stderr)
	markerPath := flags.String("marker", "", "acknowledged-commit marker")
	readyPath := flags.String("recovery-ready", "", "replacement-boot ready receipt")
	resultPath := flags.String("result", "", "negative-control recovery result")
	qmpSocket := flags.String("qmp-socket", "", "QMP socket present in child arguments")
	proofPath := flags.String("proof", "", "new volatile-loss proof")
	eventPath := flags.String("event", "", "new controller event")
	logPath := flags.String("qemu-log", "", "new QEMU process log")
	targetImage := flags.String("target-image", "", "durable base image hidden below the temporary snapshot")
	baseArtifact := flags.String("base-artifact", "", "new copy of the unchanged base captured after SIGKILL")
	artifactUID := flags.Int("artifact-uid", -1, "optional uid that must read controller artifacts")
	artifactGID := flags.Int("artifact-gid", -1, "optional gid that must read controller artifacts")
	timeout := flags.Duration("timeout", 5*time.Minute, "total first boot, cut and replacement boot deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	qemuCommand := flags.Args()
	if *markerPath == "" || *readyPath == "" || *resultPath == "" || *qmpSocket == "" || *proofPath == "" || *eventPath == "" || *logPath == "" || *targetImage == "" || *baseArtifact == "" || len(qemuCommand) < 2 || *timeout < 10*time.Second || *timeout > 30*time.Minute {
		return errors.New("destructive-qemu-volatile-loss requires marker, recovery-ready, result, QMP, proof, event, log, target, base artifact, timeout and a QEMU command after --")
	}
	if (*artifactUID < 0) != (*artifactGID < 0) {
		return errors.New("volatile-loss artifact uid and gid must be provided together")
	}
	values := []string{*markerPath, *readyPath, *resultPath, *qmpSocket, *proofPath, *eventPath, *logPath, *baseArtifact}
	paths := make([]string, len(values))
	controlDirectory := ""
	seen := map[string]struct{}{}
	for index, value := range values {
		absolute, err := filepath.Abs(filepath.Clean(value))
		if err != nil {
			return err
		}
		if index == 0 {
			controlDirectory = filepath.Dir(absolute)
		} else if filepath.Dir(absolute) != controlDirectory {
			return errors.New("volatile-loss control paths must be directly inside one directory")
		}
		if _, duplicate := seen[absolute]; duplicate {
			return errors.New("volatile-loss control paths must be distinct")
		}
		seen[absolute] = struct{}{}
		paths[index] = absolute
	}
	markerClean, readyClean, resultClean, qmpClean, proofClean, eventClean, logClean, artifactClean := paths[0], paths[1], paths[2], paths[3], paths[4], paths[5], paths[6], paths[7]
	for _, path := range []string{readyClean, resultClean, proofClean, eventClean, logClean, artifactClean} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("volatile-loss output already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	targetClean, err := existingRegularAbsolutePath(*targetImage)
	if err != nil {
		return err
	}
	if _, duplicate := seen[targetClean]; duplicate {
		return errors.New("volatile-loss target image must be distinct from control outputs")
	}
	if !qemuArgumentsBindSocket(qemuCommand[1:], qmpClean) {
		return errors.New("volatile-loss QEMU arguments do not bind the exact QMP socket")
	}
	snapshotCount := 0
	for _, argument := range qemuCommand[1:] {
		if argument == "-snapshot" {
			snapshotCount++
		}
		if argument == "-daemonize" || argument == "-S" {
			return fmt.Errorf("volatile-loss rejects QEMU child argument %q", argument)
		}
	}
	if snapshotCount != 1 {
		return errors.New("volatile-loss first boot requires exactly one -snapshot argument")
	}
	qemuExecutable, err := filepath.Abs(filepath.Clean(qemuCommand[0]))
	if err != nil {
		return err
	}
	executableHash, err := hashRegularFile(qemuExecutable, 1<<30)
	if err != nil {
		return err
	}
	argumentsRaw, err := json.Marshal(qemuCommand[1:])
	if err != nil {
		return err
	}
	baseBefore, err := hashRegularFile(targetClean, 1<<30)
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
	proof := destructiveQMPVolatileLossProof{SchemaVersion: 1, QEMUExecutable: qemuExecutable, QEMUExecutableSHA256: executableHash, QEMUArguments: append([]string(nil), qemuCommand[1:]...), QEMUArgumentsSHA256: bytesSHA256(argumentsRaw), BaseImage: targetClean, BaseImageArtifact: artifactClean, BaseBeforeSHA256: baseBefore, StartedAt: time.Now().UTC(), Signal: "SIGKILL", SnapshotMode: "qemu-temporary-overlay"}
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
	firstClient, err := openQMPRecordingClient(qmpClean, deadline)
	if err != nil {
		return err
	}
	if err := firstClient.handshake("volatile-first"); err != nil {
		firstClient.Close()
		return err
	}
	if err := qmpVolatileQuerySnapshot(firstClient, "volatile-first-query-block", targetClean); err != nil {
		firstClient.Close()
		return err
	}
	var marker destructiveVolatileLossMarker
	markerRaw, err := waitForVolatileLossReceipt(markerClean, deadline, &marker, func() error { return validateDestructiveVolatileLossMarker(marker) })
	if err != nil {
		firstClient.Close()
		return err
	}
	proof.MarkerSHA256 = qualificationSHA256(markerRaw)
	proof.MarkerObservedAt = time.Now().UTC()
	proof.FirstQMPExchanges = append([]destructiveQMPExchange(nil), firstClient.exchanges...)
	proof.KillRequestedAt = time.Now().UTC()
	killErr := first.Process.Signal(os.Kill)
	waitErr := first.Wait()
	firstReaped = true
	firstClient.Close()
	proof.KilledProcessExitedAt = time.Now().UTC()
	if killErr != nil || !destructiveProcessWasKilled(waitErr) {
		return errors.Join(killErr, fmt.Errorf("volatile-loss first QEMU was not killed: %w", waitErr))
	}
	var exitError *exec.ExitError
	if !errors.As(waitErr, &exitError) {
		return errors.New("volatile-loss SIGKILL lacks exit status")
	}
	status, ok := exitError.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		return errors.New("volatile-loss SIGKILL wait status unavailable")
	}
	proof.WaitStatus = uint32(status)
	baseAfter, err := hashRegularFile(targetClean, 1<<30)
	if err != nil {
		return err
	}
	proof.BaseAfterKillSHA256 = baseAfter
	if baseAfter != baseBefore {
		return errors.New("QEMU temporary snapshot modified the durable base image")
	}
	if err := copyFileExclusiveDurable(artifactClean, targetClean); err != nil {
		return err
	}
	if err := os.Remove(qmpClean); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	restartArgs := make([]string, 0, len(qemuCommand))
	for _, argument := range qemuCommand[1:] {
		if argument != "-snapshot" {
			restartArgs = append(restartArgs, argument)
		}
	}
	restartArgs = append(restartArgs, "-S")
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
	defer restartClient.Close()
	if err := restartClient.handshake("volatile-restart"); err != nil {
		return err
	}
	if err := restartClient.queryStoppedStatus(); err != nil {
		return err
	}
	if err := qmpVolatileQueryBase(restartClient, "volatile-restart-query-block", targetClean); err != nil {
		return err
	}
	if err := restartClient.send(map[string]string{"execute": "cont", "id": "volatile-restart-cont"}); err != nil {
		return err
	}
	proof.RestartContinuedAt = restartClient.exchanges[len(restartClient.exchanges)-1].At
	if err := receiveQMPResponse(restartClient.receive, "volatile-restart-cont", nil); err != nil {
		return err
	}
	var ready destructiveVolatileLossRecoveryReady
	readyRaw, err := waitForVolatileLossReceipt(readyClean, deadline, &ready, func() error { return validateDestructiveVolatileLossRecoveryReady(ready) })
	if err != nil {
		return err
	}
	if ready.MarkerSHA256 != proof.MarkerSHA256 || ready.BootIDBefore != marker.BootID {
		return errors.New("volatile-loss replacement boot is not bound to the acknowledged marker")
	}
	proof.RecoveryReadySHA256 = qualificationSHA256(readyRaw)
	proof.RecoveryReadyAt = time.Now().UTC()
	proof.RestartQMPExchanges = append([]destructiveQMPExchange(nil), restartClient.exchanges...)
	proof.FinishedAt = time.Now().UTC()
	if err := validateQMPVolatileLossProofStructure(proof); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(proofClean, proof); err != nil {
		return err
	}
	proofRaw, err := os.ReadFile(proofClean)
	if err != nil {
		return err
	}
	event := destructiveVolatileLossControllerEvent{SchemaVersion: 1, Method: "qemu-temporary-overlay-loss", MarkerSHA256: proof.MarkerSHA256, RecoveryReadySHA256: proof.RecoveryReadySHA256, ProofSHA256: qualificationSHA256(proofRaw), BootIDBefore: marker.BootID, BootIDAfter: ready.BootIDAfter, CutAt: proof.KillRequestedAt, RestartedAt: proof.RestartContinuedAt}
	if err := validateDestructiveVolatileLossControllerEvent(event, proof.MarkerSHA256); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(eventClean, event); err != nil {
		return err
	}
	if *artifactUID >= 0 {
		for _, path := range []string{proofClean, eventClean} {
			if err := setQualificationArtifactOwner(path, *artifactUID, *artifactGID); err != nil {
				return err
			}
		}
	}
	var result destructiveVolatileLossResult
	_, err = waitForVolatileLossReceipt(resultClean, deadline, &result, func() error { return validateDestructiveVolatileLossResult(result) })
	if err != nil {
		return err
	}
	eventRaw, err := os.ReadFile(eventClean)
	if err != nil {
		return err
	}
	if result.MarkerSHA256 != proof.MarkerSHA256 || result.ControllerSHA256 != qualificationSHA256(eventRaw) {
		return errors.New("volatile-loss recovery result is not bound to controller evidence")
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- restart.Wait() }()
	select {
	case waitErr := <-waitDone:
		restartReaped = true
		if waitErr != nil {
			return fmt.Errorf("volatile-loss replacement QEMU exit: %w", waitErr)
		}
	case <-time.After(time.Until(deadline)):
		return errors.New("timed out waiting for replacement QEMU shutdown")
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(proof)
}

func qmpVolatileQuerySnapshot(client *qmpRecordingClient, id, base string) error {
	if err := client.send(map[string]string{"execute": "query-block", "id": id}); err != nil {
		return err
	}
	return receiveQMPResponse(client.receive, id, func(envelope qmpMessageEnvelope) error {
		return validateQMPVolatileSnapshotInventory(envelope.Return, base)
	})
}

func qmpVolatileQueryBase(client *qmpRecordingClient, id, base string) error {
	if err := client.send(map[string]string{"execute": "query-block", "id": id}); err != nil {
		return err
	}
	return receiveQMPResponse(client.receive, id, func(envelope qmpMessageEnvelope) error { return validateQMPBlockInventory(envelope.Return, base) })
}

func validateQMPVolatileSnapshotInventory(raw json.RawMessage, base string) error {
	var blocks []struct {
		Inserted *struct {
			ReadOnly    bool   `json:"ro"`
			Driver      string `json:"drv"`
			BackingFile string `json:"backing_file"`
			Image       struct {
				BackingImage *struct {
					Filename string `json:"filename"`
				} `json:"backing-image"`
			} `json:"image"`
		} `json:"inserted"`
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return err
	}
	for _, block := range blocks {
		if block.Inserted != nil && !block.Inserted.ReadOnly && block.Inserted.Driver == "qcow2" && block.Inserted.BackingFile == base && block.Inserted.Image.BackingImage != nil && block.Inserted.Image.BackingImage.Filename == base {
			return nil
		}
	}
	return errors.New("QMP inventory does not prove a writable temporary qcow2 overlay backed by the exact durable image")
}

func waitForVolatileLossReceipt(path string, deadline time.Time, target any, validate func() error) ([]byte, error) {
	var lastErr error
	for time.Now().Before(deadline) {
		raw, err := readQualificationReceipt(path, target)
		if err == nil {
			if err = validate(); err == nil {
				return raw, nil
			}
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	return nil, errors.Join(errors.New("timed out waiting for volatile-loss receipt"), lastErr)
}

func validateDestructiveVolatileLossControllerEvent(event destructiveVolatileLossControllerEvent, markerSHA string) error {
	if event.SchemaVersion != 1 || event.Method != "qemu-temporary-overlay-loss" || event.MarkerSHA256 != markerSHA || !qualificationHexDigest(event.RecoveryReadySHA256) || !qualificationHexDigest(event.ProofSHA256) || !qualificationSafeName(event.BootIDBefore, 64) || !qualificationSafeName(event.BootIDAfter, 64) || event.BootIDBefore == event.BootIDAfter || event.CutAt.IsZero() || !event.RestartedAt.After(event.CutAt) {
		return errors.New("volatile-loss controller event is incomplete")
	}
	return nil
}

func validateQMPVolatileLossProofStructure(proof destructiveQMPVolatileLossProof) error {
	if proof.SchemaVersion != destructiveQMPVolatileLossProofSchema || !filepath.IsAbs(proof.QEMUExecutable) || len(proof.QEMUArguments) < 2 || len(proof.QEMUArguments) > 256 || !qualificationHexDigest(proof.QEMUExecutableSHA256) || !qualificationHexDigest(proof.QEMUArgumentsSHA256) || !filepath.IsAbs(proof.BaseImage) || !filepath.IsAbs(proof.BaseImageArtifact) || proof.BaseImage == proof.BaseImageArtifact || !qualificationHexDigest(proof.BaseBeforeSHA256) || proof.BaseAfterKillSHA256 != proof.BaseBeforeSHA256 || !qualificationHexDigest(proof.MarkerSHA256) || !qualificationHexDigest(proof.RecoveryReadySHA256) || proof.StartedAt.IsZero() || proof.MarkerObservedAt.Before(proof.StartedAt) || proof.KillRequestedAt.Before(proof.MarkerObservedAt) || !proof.KilledProcessExitedAt.After(proof.KillRequestedAt) || !proof.RestartContinuedAt.After(proof.KilledProcessExitedAt) || !proof.RecoveryReadyAt.After(proof.RestartContinuedAt) || proof.FinishedAt.Before(proof.RecoveryReadyAt) || proof.KilledPID <= 0 || proof.RestartPID <= 0 || proof.KilledPID == proof.RestartPID || proof.Signal != "SIGKILL" || proof.SnapshotMode != "qemu-temporary-overlay" {
		return errors.New("QMP volatile-loss proof identity, hashes, timing or process evidence is invalid")
	}
	snapshotCount := 0
	for _, argument := range proof.QEMUArguments {
		if argument == "-snapshot" {
			snapshotCount++
		}
		if argument == "-daemonize" || argument == "-S" {
			return errors.New("QMP volatile-loss proof contains forbidden QEMU arguments")
		}
	}
	if snapshotCount != 1 {
		return errors.New("QMP volatile-loss proof does not contain exactly one temporary snapshot request")
	}
	waitStatus := syscall.WaitStatus(proof.WaitStatus)
	if !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		return errors.New("QMP volatile-loss proof wait status is not SIGKILL")
	}
	if err := validateQMPOrderedExchanges(proof.FirstQMPExchanges, []qmpExpectedCommand{{execute: "qmp_capabilities", id: "volatile-first-capabilities"}, {execute: "query-block", id: "volatile-first-query-block", validate: func(e qmpMessageEnvelope) error {
		return validateQMPVolatileSnapshotInventory(e.Return, proof.BaseImage)
	}}}); err != nil {
		return err
	}
	return validateQMPOrderedExchanges(proof.RestartQMPExchanges, []qmpExpectedCommand{{execute: "qmp_capabilities", id: "volatile-restart-capabilities"}, {execute: "query-status", id: "restart-query-status"}, {execute: "query-block", id: "volatile-restart-query-block", validate: func(e qmpMessageEnvelope) error { return validateQMPBlockInventory(e.Return, proof.BaseImage) }}, {execute: "cont", id: "volatile-restart-cont"}})
}

func validateRetainedQMPVolatileLossProof(proof destructiveQMPVolatileLossProof) error {
	if err := validateQMPVolatileLossProofStructure(proof); err != nil {
		return err
	}
	executableHash, err := hashRegularFile(proof.QEMUExecutable, 1<<30)
	if err != nil || executableHash != proof.QEMUExecutableSHA256 {
		return errors.Join(err, errors.New("volatile-loss QEMU executable is missing or mismatched"))
	}
	argumentsRaw, err := json.Marshal(proof.QEMUArguments)
	if err != nil || bytesSHA256(argumentsRaw) != proof.QEMUArgumentsSHA256 {
		return errors.Join(err, errors.New("volatile-loss QEMU arguments are mismatched"))
	}
	artifactHash, err := hashRegularFile(proof.BaseImageArtifact, 1<<30)
	if err != nil || artifactHash != proof.BaseBeforeSHA256 {
		return errors.Join(err, errors.New("volatile-loss unchanged base artifact is missing or mismatched"))
	}
	return nil
}

func runDestructiveQMPVolatileLossProofCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-qemu-volatile-loss-proof-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	proofPath := flags.String("proof", "", "QMP volatile-loss proof")
	markerPath := flags.String("marker", "", "acknowledged-commit marker")
	readyPath := flags.String("recovery-ready", "", "replacement-boot ready receipt")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *proofPath == "" || *markerPath == "" || *readyPath == "" {
		return errors.New("destructive-qemu-volatile-loss-proof-check requires proof, marker and recovery-ready")
	}
	var proof destructiveQMPVolatileLossProof
	proofRaw, err := readQualificationReceipt(*proofPath, &proof)
	if err != nil {
		return err
	}
	if err := validateRetainedQMPVolatileLossProof(proof); err != nil {
		return err
	}
	var marker destructiveVolatileLossMarker
	markerRaw, err := readQualificationReceipt(*markerPath, &marker)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolatileLossMarker(marker); err != nil {
		return err
	}
	var ready destructiveVolatileLossRecoveryReady
	readyRaw, err := readQualificationReceipt(*readyPath, &ready)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolatileLossRecoveryReady(ready); err != nil {
		return err
	}
	if proof.MarkerSHA256 != qualificationSHA256(markerRaw) || proof.RecoveryReadySHA256 != qualificationSHA256(readyRaw) || ready.MarkerSHA256 != proof.MarkerSHA256 || ready.BootIDBefore != marker.BootID {
		return errors.New("volatile-loss QMP proof is not bound to marker and replacement boot")
	}
	return json.NewEncoder(stdout).Encode(struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		ProofSHA256   string `json:"proofSha256"`
		Passed        bool   `json:"passed"`
	}{1, qualificationSHA256(proofRaw), true})
}

func runDestructiveVolatileLossBundleCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-volatile-loss-bundle-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	seedPath := flags.String("seed", "", "durable base database seed receipt")
	markerPath := flags.String("marker", "", "acknowledged-commit marker")
	readyPath := flags.String("recovery-ready", "", "replacement-boot ready receipt")
	proofPath := flags.String("proof", "", "QMP volatile-loss proof")
	eventPath := flags.String("event", "", "controller event")
	resultPath := flags.String("result", "", "negative-control result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *seedPath == "" || *markerPath == "" || *readyPath == "" || *proofPath == "" || *eventPath == "" || *resultPath == "" {
		return errors.New("destructive-volatile-loss-bundle-check requires seed, marker, recovery-ready, proof, event and result")
	}
	var seed destructiveVolatileLossSeed
	seedRaw, err := readQualificationReceipt(*seedPath, &seed)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolatileLossSeed(seed); err != nil {
		return err
	}
	var marker destructiveVolatileLossMarker
	markerRaw, err := readQualificationReceipt(*markerPath, &marker)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolatileLossMarker(marker); err != nil {
		return err
	}
	var ready destructiveVolatileLossRecoveryReady
	readyRaw, err := readQualificationReceipt(*readyPath, &ready)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolatileLossRecoveryReady(ready); err != nil {
		return err
	}
	var proof destructiveQMPVolatileLossProof
	proofRaw, err := readQualificationReceipt(*proofPath, &proof)
	if err != nil {
		return err
	}
	if err := validateRetainedQMPVolatileLossProof(proof); err != nil {
		return err
	}
	var event destructiveVolatileLossControllerEvent
	eventRaw, err := readQualificationReceipt(*eventPath, &event)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolatileLossControllerEvent(event, qualificationSHA256(markerRaw)); err != nil {
		return err
	}
	var result destructiveVolatileLossResult
	resultRaw, err := readQualificationReceipt(*resultPath, &result)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolatileLossResult(result); err != nil {
		return err
	}
	if seed.BuildRevision != marker.BuildRevision || seed.BuildRevision != ready.BuildRevision || seed.BuildRevision != result.BuildRevision || seed.BuildModified != marker.BuildModified || seed.BuildModified != ready.BuildModified || seed.BuildModified != result.BuildModified || seed.GOOS != marker.GOOS || seed.GOOS != ready.GOOS || seed.GOOS != result.GOOS || seed.GOARCH != marker.GOARCH || seed.GOARCH != ready.GOARCH || seed.GOARCH != result.GOARCH || seed.GoVersion != marker.GoVersion || seed.GoVersion != ready.GoVersion || seed.GoVersion != result.GoVersion {
		return errors.New("volatile-loss bundle does not share one build and runtime")
	}
	markerSHA, readySHA, proofSHA, eventSHA := qualificationSHA256(markerRaw), qualificationSHA256(readyRaw), qualificationSHA256(proofRaw), qualificationSHA256(eventRaw)
	if seed.DatabaseSHA256 != marker.BeforeSHA256 || seed.CommitSequence != marker.OldCommitSequence || seed.StateSHA256 != marker.OldStateSHA256 || proof.MarkerSHA256 != markerSHA || proof.RecoveryReadySHA256 != readySHA || ready.MarkerSHA256 != markerSHA || event.MarkerSHA256 != markerSHA || event.RecoveryReadySHA256 != readySHA || event.ProofSHA256 != proofSHA || event.BootIDBefore != marker.BootID || event.BootIDAfter != ready.BootIDAfter || result.MarkerSHA256 != markerSHA || result.ControllerSHA256 != eventSHA || result.BootIDBefore != marker.BootID || result.BootIDAfter != ready.BootIDAfter || result.AcknowledgedCommitSequence != marker.AcknowledgedCommitSequence || result.AcknowledgedStateSHA256 != marker.AcknowledgedStateSHA256 {
		return errors.New("volatile-loss seed, acknowledgement, cut, replacement boot and result are not one transition")
	}
	verified, err := verifyDestructiveVolatileLossDatabase(result.DatabaseArtifact)
	if err != nil || fmt.Sprintf("%x", verified.SHA256) != result.RecoveredSHA256 || verified.Meta.CommitSequence != result.RecoveredCommitSequence {
		return errors.Join(err, errors.New("volatile-loss recovered artifact is missing or mismatched"))
	}
	file, meta, err := storagev2.Open(result.DatabaseArtifact)
	if err != nil {
		return err
	}
	value, exists, readErr := file.GetDocument("items", destructiveVolatileLossDocumentID)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || !exists || meta.CommitSequence != seed.CommitSequence || bytesSHA256(value) != seed.StateSHA256 || result.RecoveredStateSHA256 != seed.StateSHA256 {
		return errors.Join(readErr, closeErr, errors.New("volatile-loss recovered logical state is not the durable seed"))
	}
	return json.NewEncoder(stdout).Encode(struct {
		SchemaVersion         uint32 `json:"schemaVersion"`
		SeedSHA256            string `json:"seedSha256"`
		MarkerSHA256          string `json:"markerSha256"`
		RecoveryReadySHA256   string `json:"recoveryReadySha256"`
		ProofSHA256           string `json:"proofSha256"`
		EventSHA256           string `json:"eventSha256"`
		ResultSHA256          string `json:"resultSha256"`
		UnsafeStorageDetected bool   `json:"unsafeStorageDetected"`
		NegativeControlPassed bool   `json:"negativeControlPassed"`
	}{1, qualificationSHA256(seedRaw), markerSHA, readySHA, proofSHA, eventSHA, qualificationSHA256(resultRaw), true, true})
}
