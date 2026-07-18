package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const destructiveQMPFlushEIOProofSchema uint32 = 1

type destructiveQMPFlushEIOProof struct {
	SchemaVersion     uint32                   `json:"schemaVersion"`
	TargetImage       string                   `json:"targetImage"`
	ReadySHA256       string                   `json:"readySha256"`
	ArmedSHA256       string                   `json:"armedSha256"`
	FaultSHA256       string                   `json:"faultSha256"`
	StartedAt         time.Time                `json:"startedAt"`
	ReadyObservedAt   time.Time                `json:"readyObservedAt"`
	ArmedAt           time.Time                `json:"armedAt"`
	FinishedAt        time.Time                `json:"finishedAt"`
	RuleEvent         string                   `json:"ruleEvent"`
	RuleIOType        string                   `json:"ruleIoType"`
	RuleErrno         int                      `json:"ruleErrno"`
	RuleOnce          bool                     `json:"ruleOnce"`
	ObservedOperation string                   `json:"observedOperation"`
	Exchanges         []destructiveQMPExchange `json:"exchanges"`
}

func runDestructiveQEMUFlushEIO(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-qemu-flush-eio", flag.ContinueOnError)
	flags.SetOutput(stderr)
	readyPath := flags.String("ready", "", "durable guest-ready receipt")
	armedPath := flags.String("armed", "", "new durable host-armed receipt")
	faultPath := flags.String("fault", "", "guest fault-stage result")
	qmpSocket := flags.String("qmp-socket", "", "QEMU QMP Unix socket")
	proofPath := flags.String("proof", "", "new durable QMP flush EIO proof")
	ackPath := flags.String("ack", "", "new durable acknowledgement that permits guest shutdown")
	targetImage := flags.String("target-image", "", "exact raw ext4 target image")
	timeout := flags.Duration("timeout", 3*time.Minute, "controller deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *readyPath == "" || *armedPath == "" || *faultPath == "" || *qmpSocket == "" || *proofPath == "" || *ackPath == "" || *targetImage == "" || *timeout < 10*time.Second || *timeout > 30*time.Minute {
		return errors.New("destructive-qemu-flush-eio requires ready, armed, fault, QMP socket, proof, ack, target image and a timeout from 10s to 30m")
	}
	paths := make([]string, 6)
	for index, value := range []string{*readyPath, *armedPath, *faultPath, *qmpSocket, *proofPath, *ackPath} {
		absolute, err := filepath.Abs(filepath.Clean(value))
		if err != nil {
			return err
		}
		paths[index] = absolute
	}
	readyClean, armedClean, faultClean, qmpClean, proofClean, ackClean := paths[0], paths[1], paths[2], paths[3], paths[4], paths[5]
	directory := filepath.Dir(readyClean)
	seen := map[string]struct{}{}
	for _, path := range paths {
		if filepath.Dir(path) != directory {
			return errors.New("flush EIO control paths must be directly inside one directory")
		}
		if _, duplicate := seen[path]; duplicate {
			return errors.New("flush EIO controller paths must be distinct")
		}
		seen[path] = struct{}{}
	}
	for _, path := range []string{armedClean, proofClean, ackClean} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("flush EIO controller output already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	targetClean, err := existingRegularAbsolutePath(*targetImage)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(*timeout)
	var ready destructiveFlushEIOReady
	var readyRaw []byte
	for time.Now().Before(deadline) {
		ready = destructiveFlushEIOReady{}
		readyRaw, err = readQualificationReceipt(readyClean, &ready)
		if err == nil && validateDestructiveFlushEIOReady(ready) == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(readyRaw) == 0 || validateDestructiveFlushEIOReady(ready) != nil {
		return errors.New("timed out waiting for a valid flush EIO ready receipt")
	}
	proof := destructiveQMPFlushEIOProof{SchemaVersion: 1, TargetImage: targetClean, ReadySHA256: qualificationSHA256(readyRaw), StartedAt: time.Now().UTC(), ReadyObservedAt: time.Now().UTC(), RuleEvent: "flush_to_disk", RuleIOType: "flush", RuleErrno: 5, RuleOnce: true}
	client, err := openQMPRecordingClient(qmpClean, deadline)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.handshake("flush"); err != nil {
		return err
	}
	if err := qmpFlushQueryNodes(client, "flush-query-nodes-before", targetClean, false); err != nil {
		return err
	}
	if err := armQEMUBlkdebugFlushEIO(client, "flush-arm"); err != nil {
		return err
	}
	if err := qmpFlushQueryNodes(client, "flush-query-nodes-armed", targetClean, true); err != nil {
		return err
	}
	armRaw, err := json.Marshal(client.exchanges)
	if err != nil {
		return err
	}
	armed := destructiveFlushEIOArmed{SchemaVersion: 1, ReadySHA256: proof.ReadySHA256, QMPArmSHA256: qualificationSHA256(armRaw), ArmedAt: time.Now().UTC()}
	if err := validateDestructiveFlushEIOArmed(armed, proof.ReadySHA256, ready.ReadyAt); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(armedClean, armed); err != nil {
		return err
	}
	if err := os.Chmod(armedClean, 0o644); err != nil {
		return err
	}
	if err := syncProbeDirectory(filepath.Dir(armedClean)); err != nil {
		return err
	}
	armedRaw, err := os.ReadFile(armedClean)
	if err != nil {
		return err
	}
	proof.ArmedSHA256, proof.ArmedAt = qualificationSHA256(armedRaw), armed.ArmedAt
	for countQMPFlushIOErrors(client.exchanges) == 0 {
		_, _, err := client.receive()
		if err != nil {
			return fmt.Errorf("wait for QMP flush BLOCK_IO_ERROR: %w", err)
		}
	}
	proof.ObservedOperation = qmpFlushEIOOperation(client.exchanges)
	var fault destructiveFlushEIOFaultResult
	var faultRaw []byte
	for time.Now().Before(deadline) {
		fault = destructiveFlushEIOFaultResult{}
		faultRaw, err = readQualificationReceipt(faultClean, &fault)
		if err == nil && validateDestructiveFlushEIOFaultResult(fault) == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(faultRaw) == 0 || validateDestructiveFlushEIOFaultResult(fault) != nil {
		return errors.New("timed out waiting for a valid flush EIO fault result")
	}
	if fault.ReadySHA256 != proof.ReadySHA256 || fault.ArmedSHA256 != proof.ArmedSHA256 {
		return errors.New("flush EIO fault result is not bound to ready and armed receipts")
	}
	if err := qmpFlushQueryNodes(client, "flush-query-nodes-after", targetClean, true); err != nil {
		return err
	}
	proof.FaultSHA256 = qualificationSHA256(faultRaw)
	proof.FinishedAt = time.Now().UTC()
	proof.Exchanges = append([]destructiveQMPExchange(nil), client.exchanges...)
	if err := validateQMPFlushEIOProofStructure(proof); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(proofClean, proof); err != nil {
		return err
	}
	if err := os.Chmod(proofClean, 0o644); err != nil {
		return err
	}
	proofFile, err := os.Open(proofClean)
	if err != nil {
		return err
	}
	if syncErr := proofFile.Sync(); syncErr != nil {
		_ = proofFile.Close()
		return syncErr
	}
	if err := proofFile.Close(); err != nil {
		return err
	}
	if err := syncProbeDirectory(filepath.Dir(proofClean)); err != nil {
		return err
	}
	proofRaw, err := os.ReadFile(proofClean)
	if err != nil {
		return err
	}
	ack := struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		ProofSHA256   string `json:"proofSha256"`
	}{1, qualificationSHA256(proofRaw)}
	if err := writeJSONExclusiveDurable(ackClean, ack); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(proof)
}

func qmpFlushQueryNodes(client *qmpRecordingClient, id, targetImage string, armed bool) error {
	if err := client.send(map[string]string{"execute": "query-named-block-nodes", "id": id}); err != nil {
		return err
	}
	return receiveQMPResponse(client.receive, id, func(envelope qmpMessageEnvelope) error {
		return validateQMPFlushNodeInventory(envelope.Return, targetImage, armed)
	})
}

func validateQMPFlushNodeInventory(raw json.RawMessage, targetImage string, armed bool) error {
	var nodes []struct {
		NodeName string `json:"node-name"`
		Driver   string `json:"drv"`
		File     string `json:"file"`
		Cache    struct {
			Direct  bool `json:"direct"`
			NoFlush bool `json:"no-flush"`
		} `json:"cache"`
	}
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return err
	}
	foundFile, foundRaw, foundSafe, foundArmed := false, false, false, false
	for _, node := range nodes {
		switch node.NodeName {
		case "meld-file":
			foundFile = node.Driver == "file" && node.File == targetImage && node.Cache.Direct && !node.Cache.NoFlush
		case "meld-raw":
			foundRaw = node.Driver == "raw"
		case "meld-debug":
			foundSafe = node.Driver == "blkdebug"
		case "meld-flush-debug":
			foundArmed = node.Driver == "blkdebug"
		}
	}
	if !foundFile || !foundRaw || !foundSafe || foundArmed != armed {
		return errors.New("QMP named block graph does not bind the expected direct-I/O flush-enabled target and arm state")
	}
	return nil
}

func countQMPFlushIOErrors(exchanges []destructiveQMPExchange) int {
	count := 0
	for _, exchange := range exchanges {
		if exchange.Direction != "receive" {
			continue
		}
		var envelope qmpMessageEnvelope
		if json.Unmarshal(exchange.Message, &envelope) != nil || envelope.Event != "BLOCK_IO_ERROR" {
			continue
		}
		var data struct {
			Operation string `json:"operation"`
			Action    string `json:"action"`
			NoSpace   bool   `json:"nospace"`
			Reason    string `json:"reason"`
		}
		if json.Unmarshal(envelope.Data, &data) == nil && (data.Operation == "flush" || data.Operation == "write") && data.Action == "report" && !data.NoSpace && data.Reason != "" {
			count++
		}
	}
	return count
}

func qmpFlushEIOOperation(exchanges []destructiveQMPExchange) string {
	for _, exchange := range exchanges {
		if exchange.Direction != "receive" {
			continue
		}
		var envelope qmpMessageEnvelope
		if json.Unmarshal(exchange.Message, &envelope) != nil || envelope.Event != "BLOCK_IO_ERROR" {
			continue
		}
		var data struct {
			Operation string `json:"operation"`
			Action    string `json:"action"`
			NoSpace   bool   `json:"nospace"`
			Reason    string `json:"reason"`
		}
		if json.Unmarshal(envelope.Data, &data) == nil && (data.Operation == "flush" || data.Operation == "write") && data.Action == "report" && !data.NoSpace && data.Reason != "" {
			return data.Operation
		}
	}
	return ""
}

func validateQMPFlushEIOProofStructure(proof destructiveQMPFlushEIOProof) error {
	if proof.SchemaVersion != destructiveQMPFlushEIOProofSchema || !filepath.IsAbs(proof.TargetImage) || !qualificationHexDigest(proof.ReadySHA256) || !qualificationHexDigest(proof.ArmedSHA256) || !qualificationHexDigest(proof.FaultSHA256) || proof.StartedAt.IsZero() || proof.ReadyObservedAt.Before(proof.StartedAt) || proof.ArmedAt.Before(proof.ReadyObservedAt) || !proof.FinishedAt.After(proof.ArmedAt) || proof.RuleEvent != "flush_to_disk" || proof.RuleIOType != "flush" || proof.RuleErrno != 5 || !proof.RuleOnce || (proof.ObservedOperation != "flush" && proof.ObservedOperation != "write") || len(proof.Exchanges) < 14 || len(proof.Exchanges) > 256 {
		return errors.New("QMP flush EIO proof identity, timing, rule or exchange count is invalid")
	}
	if count := countQMPFlushIOErrors(proof.Exchanges); count < 1 || count > 32 {
		return errors.New("QMP flush EIO proof lacks bounded flush BLOCK_IO_ERROR evidence")
	}
	if err := validateFlushArmCommandEvidence(proof.Exchanges); err != nil {
		return err
	}
	return validateQMPOrderedExchanges(proof.Exchanges, []qmpExpectedCommand{
		{execute: "qmp_capabilities", id: "flush-capabilities"},
		{execute: "query-named-block-nodes", id: "flush-query-nodes-before", validate: func(e qmpMessageEnvelope) error {
			return validateQMPFlushNodeInventory(e.Return, proof.TargetImage, false)
		}},
		{execute: "blockdev-add", id: "flush-arm-add"},
		{execute: "blockdev-reopen", id: "flush-arm-switch"},
		{execute: "query-named-block-nodes", id: "flush-query-nodes-armed", validate: func(e qmpMessageEnvelope) error {
			return validateQMPFlushNodeInventory(e.Return, proof.TargetImage, true)
		}},
		{execute: "query-named-block-nodes", id: "flush-query-nodes-after", validate: func(e qmpMessageEnvelope) error {
			return validateQMPFlushNodeInventory(e.Return, proof.TargetImage, true)
		}},
	})
}

func validateFlushArmCommandEvidence(exchanges []destructiveQMPExchange) error {
	expected := map[string]json.RawMessage{
		"flush-arm-add":    json.RawMessage(`{"execute":"blockdev-add","arguments":{"driver":"blkdebug","image":"meld-file","inject-error":[{"errno":5,"event":"flush_to_disk","iotype":"flush","once":true}],"node-name":"meld-flush-debug"},"id":"flush-arm-add"}`),
		"flush-arm-switch": json.RawMessage(`{"execute":"blockdev-reopen","arguments":{"options":[{"driver":"raw","file":"meld-flush-debug","node-name":"meld-raw"}]},"id":"flush-arm-switch"}`),
	}
	seen := make(map[string]bool)
	for _, exchange := range exchanges {
		if exchange.Direction != "send" {
			continue
		}
		var envelope struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(exchange.Message, &envelope) != nil {
			return errors.New("QMP flush EIO proof contains malformed sent JSON")
		}
		want, relevant := expected[envelope.ID]
		if !relevant {
			continue
		}
		if seen[envelope.ID] {
			return errors.New("QMP flush EIO proof duplicates an arm command")
		}
		seen[envelope.ID] = true
		var gotValue, wantValue any
		if json.Unmarshal(exchange.Message, &gotValue) != nil || json.Unmarshal(want, &wantValue) != nil {
			return errors.New("QMP flush EIO arm command cannot be decoded")
		}
		gotCanonical, gotErr := json.Marshal(gotValue)
		wantCanonical, wantErr := json.Marshal(wantValue)
		if gotErr != nil || wantErr != nil || string(gotCanonical) != string(wantCanonical) {
			return errors.New("QMP flush EIO proof arm command is not the exact flush-only one-shot rule and graph switch")
		}
	}
	if !seen["flush-arm-add"] || !seen["flush-arm-switch"] {
		return errors.New("QMP flush EIO proof lacks exact arm commands")
	}
	return nil
}

func flushArmExchangeSHA(exchanges []destructiveQMPExchange) (string, error) {
	for index, exchange := range exchanges {
		if exchange.Direction != "receive" {
			continue
		}
		var envelope qmpMessageEnvelope
		if json.Unmarshal(exchange.Message, &envelope) != nil || envelope.ID != "flush-query-nodes-armed" || len(envelope.Return) == 0 {
			continue
		}
		raw, err := json.Marshal(exchanges[:index+1])
		if err != nil {
			return "", err
		}
		return qualificationSHA256(raw), nil
	}
	return "", errors.New("flush EIO proof lacks the completed armed graph query")
}

func runDestructiveQMPFlushEIOProofCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-qemu-flush-eio-proof-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	proofPath := flags.String("proof", "", "QMP flush EIO proof")
	readyPath := flags.String("ready", "", "bound guest-ready receipt")
	armedPath := flags.String("armed", "", "bound host-armed receipt")
	faultPath := flags.String("fault", "", "bound guest fault result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *proofPath == "" || *readyPath == "" || *armedPath == "" || *faultPath == "" {
		return errors.New("destructive-qemu-flush-eio-proof-check requires proof, ready, armed and fault")
	}
	var proof destructiveQMPFlushEIOProof
	proofRaw, err := readQualificationReceipt(*proofPath, &proof)
	if err != nil {
		return err
	}
	if err := validateQMPFlushEIOProofStructure(proof); err != nil {
		return err
	}
	var ready destructiveFlushEIOReady
	readyRaw, err := readQualificationReceipt(*readyPath, &ready)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIOReady(ready); err != nil {
		return err
	}
	var armed destructiveFlushEIOArmed
	armedRaw, err := readQualificationReceipt(*armedPath, &armed)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIOArmed(armed, qualificationSHA256(readyRaw), ready.ReadyAt); err != nil {
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
	armSHA, err := flushArmExchangeSHA(proof.Exchanges)
	if err != nil {
		return err
	}
	if proof.ReadySHA256 != qualificationSHA256(readyRaw) || proof.ArmedSHA256 != qualificationSHA256(armedRaw) || proof.FaultSHA256 != qualificationSHA256(faultRaw) || armed.QMPArmSHA256 != armSHA || fault.ReadySHA256 != proof.ReadySHA256 || fault.ArmedSHA256 != proof.ArmedSHA256 {
		return errors.New("flush EIO proof receipts or QMP arm exchange are mismatched")
	}
	if _, err := existingRegularAbsolutePath(proof.TargetImage); err != nil {
		return errors.Join(err, errors.New("flush EIO target image is missing"))
	}
	return json.NewEncoder(stdout).Encode(struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		ProofSHA256   string `json:"proofSha256"`
		Passed        bool   `json:"passed"`
	}{1, qualificationSHA256(proofRaw), true})
}

func runDestructiveFlushEIOBundleCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-flush-eio-bundle-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	seedPath := flags.String("seed", "", "seed result")
	readyPath := flags.String("ready", "", "guest-ready receipt")
	armedPath := flags.String("armed", "", "host-armed receipt")
	faultPath := flags.String("fault", "", "guest fault-stage result")
	resultPath := flags.String("result", "", "guest result")
	proofPath := flags.String("proof", "", "QMP proof")
	planPath := flags.String("recovery-plan", "", "stopped-image recovery plan")
	recoveryReadyPath := flags.String("recovery-ready", "", "fresh-boot raw-device receipt")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *seedPath == "" || *readyPath == "" || *armedPath == "" || *faultPath == "" || *resultPath == "" || *proofPath == "" || *planPath == "" || *recoveryReadyPath == "" {
		return errors.New("destructive-flush-eio-bundle-check requires seed, ready, armed, fault, result, proof, recovery-plan and recovery-ready")
	}
	var seed destructiveEIOSeedResult
	seedRaw, err := readQualificationReceipt(*seedPath, &seed)
	if err != nil {
		return err
	}
	if err := validateDestructiveEIOSeedResult(seed); err != nil {
		return err
	}
	var ready destructiveFlushEIOReady
	readyRaw, err := readQualificationReceipt(*readyPath, &ready)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIOReady(ready); err != nil {
		return err
	}
	var armed destructiveFlushEIOArmed
	armedRaw, err := readQualificationReceipt(*armedPath, &armed)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIOArmed(armed, qualificationSHA256(readyRaw), ready.ReadyAt); err != nil {
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
	var result destructiveFlushEIOWorkerResult
	resultRaw, err := readQualificationReceipt(*resultPath, &result)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIOWorkerResult(result); err != nil {
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
	var recoveryReady destructiveFlushEIORecoveryReady
	recoveryReadyRaw, err := readQualificationReceipt(*recoveryReadyPath, &recoveryReady)
	if err != nil {
		return err
	}
	if err := validateDestructiveFlushEIORecoveryReady(recoveryReady, planRaw, plan, fault); err != nil {
		return err
	}
	armSHA, err := flushArmExchangeSHA(proof.Exchanges)
	if err != nil {
		return err
	}
	if seed.BuildRevision != ready.BuildRevision || seed.BuildRevision != fault.BuildRevision || seed.BuildRevision != result.BuildRevision || seed.BuildModified != ready.BuildModified || seed.BuildModified != fault.BuildModified || seed.BuildModified != result.BuildModified || seed.GOOS != ready.GOOS || seed.GOOS != fault.GOOS || seed.GOOS != result.GOOS || seed.GOARCH != ready.GOARCH || seed.GOARCH != fault.GOARCH || seed.GOARCH != result.GOARCH || seed.GoVersion != ready.GoVersion || seed.GoVersion != fault.GoVersion || seed.GoVersion != result.GoVersion || seed.DatabaseSHA256 != ready.DatabaseSHA256 || seed.DatabaseSHA256 != fault.BeforeSHA256 || seed.DatabaseSHA256 != result.BeforeSHA256 || seed.CommitSequence != ready.CommitSequence || seed.CommitSequence != fault.BeforeSequence || seed.CommitSequence != result.BeforeSequence || proof.ReadySHA256 != qualificationSHA256(readyRaw) || proof.ArmedSHA256 != qualificationSHA256(armedRaw) || proof.FaultSHA256 != qualificationSHA256(faultRaw) || result.FaultSHA256 != qualificationSHA256(faultRaw) || result.ProofSHA256 != qualificationSHA256(proofRaw) || result.RecoveryPlanSHA256 != qualificationSHA256(planRaw) || result.RecoveryReadySHA256 != qualificationSHA256(recoveryReadyRaw) || result.FaultBootID != fault.BootID || result.RecoveryBootID != recoveryReady.BootID || armed.QMPArmSHA256 != armSHA {
		return errors.New("flush EIO seed, handshake, result and proof do not form one transition")
	}
	verified, err := verifyDestructiveEIODatabase(result.DatabaseArtifact)
	if err != nil || fmt.Sprintf("%x", verified.SHA256) != result.AfterSHA256 || verified.Meta.CommitSequence != result.RecoveredSequence {
		return errors.Join(err, errors.New("flush EIO recovered database is missing or mismatched"))
	}
	return json.NewEncoder(stdout).Encode(struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		SeedSHA256    string `json:"seedSha256"`
		ReadySHA256   string `json:"readySha256"`
		ArmedSHA256   string `json:"armedSha256"`
		FaultSHA256   string `json:"faultSha256"`
		PlanSHA256    string `json:"recoveryPlanSha256"`
		Ready2SHA256  string `json:"recoveryReadySha256"`
		ResultSHA256  string `json:"resultSha256"`
		ProofSHA256   string `json:"proofSha256"`
		Passed        bool   `json:"passed"`
	}{1, qualificationSHA256(seedRaw), qualificationSHA256(readyRaw), qualificationSHA256(armedRaw), qualificationSHA256(faultRaw), qualificationSHA256(planRaw), qualificationSHA256(recoveryReadyRaw), qualificationSHA256(resultRaw), qualificationSHA256(proofRaw), true})
}

func runDestructiveQEMUFlushArmProbe(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-qemu-flush-arm-probe", flag.ContinueOnError)
	flags.SetOutput(stderr)
	qmpSocket := flags.String("qmp-socket", "", "QEMU QMP Unix socket for a named meld-debug block node")
	timeout := flags.Duration("timeout", 30*time.Second, "probe deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *qmpSocket == "" || *timeout < time.Second || *timeout > time.Minute {
		return errors.New("destructive-qemu-flush-arm-probe requires --qmp-socket and a timeout from 1s to 1m")
	}
	socket, err := filepath.Abs(filepath.Clean(*qmpSocket))
	if err != nil {
		return err
	}
	client, err := openQMPRecordingClient(socket, time.Now().Add(*timeout))
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.handshake("flush-probe"); err != nil {
		return err
	}
	if err := armQEMUBlkdebugFlushEIO(client, "flush-probe-arm"); err != nil {
		return err
	}
	packet := struct {
		SchemaVersion uint32                   `json:"schemaVersion"`
		Armed         bool                     `json:"armed"`
		Exchanges     []destructiveQMPExchange `json:"exchanges"`
	}{SchemaVersion: 1, Armed: true, Exchanges: client.exchanges}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(packet)
}

func armQEMUBlkdebugFlushEIO(client *qmpRecordingClient, id string) error {
	add := struct {
		Execute   string         `json:"execute"`
		Arguments map[string]any `json:"arguments"`
		ID        string         `json:"id"`
	}{Execute: "blockdev-add", ID: id + "-add"}
	add.Arguments = map[string]any{
		"driver": "blkdebug", "node-name": "meld-flush-debug", "image": "meld-file",
		"inject-error": []map[string]any{{
			"event": "flush_to_disk", "iotype": "flush", "errno": 5, "once": true,
		}},
	}
	if err := client.sendValue(add); err != nil {
		return err
	}
	if err := receiveQMPResponse(client.receive, add.ID, nil); err != nil {
		return err
	}
	reopen := struct {
		Execute   string `json:"execute"`
		Arguments struct {
			Options []map[string]any `json:"options"`
		} `json:"arguments"`
		ID string `json:"id"`
	}{Execute: "blockdev-reopen", ID: id + "-switch"}
	reopen.Arguments.Options = []map[string]any{{
		"driver": "raw", "node-name": "meld-raw", "file": "meld-flush-debug",
	}}
	if err := client.sendValue(reopen); err != nil {
		return err
	}
	return receiveQMPResponse(client.receive, reopen.ID, nil)
}
