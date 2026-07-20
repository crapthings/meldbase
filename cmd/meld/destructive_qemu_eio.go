package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const destructiveQMPEIOProofSchema uint32 = 1

type destructiveQMPEIOProof struct {
	SchemaVersion       uint32                   `json:"schemaVersion"`
	TargetImage         string                   `json:"targetImage"`
	BlkdebugConfig      string                   `json:"blkdebugConfig"`
	BlkdebugSHA256      string                   `json:"blkdebugSha256"`
	InjectedSectorCount int                      `json:"injectedSectorCount"`
	ResultSHA256        string                   `json:"resultSha256"`
	StartedAt           time.Time                `json:"startedAt"`
	FinishedAt          time.Time                `json:"finishedAt"`
	Exchanges           []destructiveQMPExchange `json:"exchanges"`
}

func runDestructiveQEMUEIO(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-qemu-eio", flag.ContinueOnError)
	flags.SetOutput(stderr)
	resultPath := flags.String("result", "", "guest worker result on the independent control device")
	qmpSocket := flags.String("qmp-socket", "", "QEMU QMP Unix socket")
	proofPath := flags.String("proof", "", "new durable QMP EIO proof")
	ackPath := flags.String("ack", "", "new durable acknowledgement that permits guest shutdown")
	targetImage := flags.String("target-image", "", "exact raw ext4 target image below blkdebug")
	blkdebugPath := flags.String("blkdebug-config", "", "strict write_aio EIO sector rule file")
	timeout := flags.Duration("timeout", 2*time.Minute, "maximum time to observe block EIO and guest result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *resultPath == "" || *qmpSocket == "" || *proofPath == "" || *ackPath == "" || *targetImage == "" || *blkdebugPath == "" ||
		*timeout < 10*time.Second || *timeout > 30*time.Minute {
		return errors.New("destructive-qemu-eio requires result, QMP socket, proof, ack, target image, blkdebug config and timeout")
	}
	controlPaths := make([]string, 5)
	for index, value := range []string{*resultPath, *qmpSocket, *proofPath, *ackPath, *blkdebugPath} {
		absolute, err := filepath.Abs(filepath.Clean(value))
		if err != nil {
			return err
		}
		controlPaths[index] = absolute
	}
	resultClean, qmpClean, proofClean, ackClean, configClean := controlPaths[0], controlPaths[1], controlPaths[2], controlPaths[3], controlPaths[4]
	controlDirectory := filepath.Dir(resultClean)
	for _, path := range controlPaths[1:] {
		if filepath.Dir(path) != controlDirectory {
			return errors.New("EIO result, QMP, proof, ack and config must be directly inside one control directory")
		}
	}
	if resultClean == proofClean || resultClean == ackClean || proofClean == ackClean || qmpClean == proofClean || qmpClean == ackClean {
		return errors.New("EIO controller paths must be distinct")
	}
	for _, path := range []string{proofClean, ackClean} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("EIO controller output already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	targetClean, err := existingRegularAbsolutePath(*targetImage)
	if err != nil {
		return err
	}
	configRaw, err := os.ReadFile(configClean)
	if err != nil {
		return err
	}
	sectors, err := validateBlkdebugEIOConfig(configRaw)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(*timeout)
	client, err := openQMPRecordingClient(qmpClean, deadline)
	if err != nil {
		return err
	}
	defer client.Close()
	proof := destructiveQMPEIOProof{
		SchemaVersion: destructiveQMPEIOProofSchema, TargetImage: targetClean, BlkdebugConfig: configClean,
		BlkdebugSHA256: qualificationSHA256(configRaw), InjectedSectorCount: len(sectors), StartedAt: time.Now().UTC(),
	}
	if err := client.handshake("eio"); err != nil {
		return err
	}
	if err := qmpEIOQueryBlock(client, "eio-query-block-before", targetClean, configClean); err != nil {
		return err
	}
	for countQMPBlockIOErrors(client.exchanges) == 0 {
		if _, _, err := client.receive(); err != nil {
			return fmt.Errorf("wait for QMP BLOCK_IO_ERROR: %w", err)
		}
	}
	var result destructiveEIOWorkerResult
	var resultRaw []byte
	var resultErr error
	for time.Now().Before(deadline) {
		info, statErr := os.Stat(resultClean)
		if statErr == nil && info.Size() > qualificationReceiptMaxBytes {
			return errors.New("guest EIO result exceeds the receipt size limit")
		}
		if statErr == nil && info.Size() > 0 {
			result = destructiveEIOWorkerResult{}
			resultRaw, resultErr = readQualificationReceipt(resultClean, &result)
		}
		if resultErr == nil && len(resultRaw) != 0 {
			break
		}
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(resultRaw) == 0 {
		return errors.Join(errors.New("timed out waiting for complete guest EIO result"), resultErr)
	}
	if err := validateDestructiveEIOWorkerResult(result); err != nil {
		return err
	}
	if err := qmpEIOQueryBlock(client, "eio-query-block-after", targetClean, configClean); err != nil {
		return err
	}
	proof.ResultSHA256 = qualificationSHA256(resultRaw)
	proof.FinishedAt = time.Now().UTC()
	proof.Exchanges = append([]destructiveQMPExchange(nil), client.exchanges...)
	if err := validateQMPEIOProofStructure(proof); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(proofClean, proof); err != nil {
		return err
	}
	ack := struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		ProofSHA256   string `json:"proofSha256"`
	}{SchemaVersion: 1}
	proofRaw, err := os.ReadFile(proofClean)
	if err != nil {
		return err
	}
	ack.ProofSHA256 = qualificationSHA256(proofRaw)
	if err := writeJSONExclusiveDurable(ackClean, ack); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(proof)
}

func qmpEIOQueryBlock(client *qmpRecordingClient, id, targetImage, config string) error {
	if err := client.send(map[string]string{"execute": "query-block", "id": id}); err != nil {
		return err
	}
	return receiveQMPResponse(client.receive, id, func(envelope qmpMessageEnvelope) error {
		return validateQMPBlkdebugInventory(envelope.Return, targetImage, config)
	})
}

func validateQMPBlkdebugInventory(raw json.RawMessage, targetImage, config string) error {
	var blocks []struct {
		Inserted *struct {
			ReadOnly bool   `json:"ro"`
			Driver   string `json:"drv"`
			File     string `json:"file"`
			Image    struct {
				Filename string `json:"filename"`
			} `json:"image"`
			Cache struct {
				Direct  bool `json:"direct"`
				NoFlush bool `json:"no-flush"`
			} `json:"cache"`
		} `json:"inserted"`
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return err
	}
	want := "blkdebug:" + config + ":" + targetImage
	for _, block := range blocks {
		inserted := block.Inserted
		if inserted != nil && !inserted.ReadOnly && inserted.Driver == "raw" && inserted.File == want && inserted.Image.Filename == want &&
			inserted.Cache.Direct && !inserted.Cache.NoFlush {
			return nil
		}
	}
	return errors.New("QMP block inventory does not bind writable direct-I/O blkdebug config to the exact target with flushes enabled")
}

func validateBlkdebugEIOConfig(raw []byte) (map[uint64]struct{}, error) {
	if len(raw) == 0 || len(raw) > qualificationReceiptMaxBytes {
		return nil, errors.New("blkdebug EIO config is empty or oversized")
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	sectors := make(map[uint64]struct{})
	section := []string{}
	flushSection := func() error {
		if len(section) == 0 {
			return nil
		}
		if len(section) != 6 || section[0] != "[inject-error]" || section[1] != `event = "write_aio"` ||
			section[2] != `iotype = "write"` || section[3] != `errno = "5"` ||
			!strings.HasPrefix(section[4], `sector = "`) || !strings.HasSuffix(section[4], `"`) || section[5] != `once = "on"` {
			return errors.New("blkdebug config contains a non-canonical EIO rule")
		}
		value := strings.TrimSuffix(strings.TrimPrefix(section[4], `sector = "`), `"`)
		sector, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.New("blkdebug config contains an invalid sector")
		}
		if _, duplicate := sectors[sector]; duplicate {
			return errors.New("blkdebug config duplicates a sector")
		}
		sectors[sector] = struct{}{}
		section = section[:0]
		return nil
	}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			if err := flushSection(); err != nil {
				return nil, err
			}
			continue
		}
		section = append(section, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if err := flushSection(); err != nil {
		return nil, err
	}
	if len(sectors) == 0 || len(sectors) > 100_000 {
		return nil, errors.New("blkdebug config sector coverage is invalid")
	}
	return sectors, nil
}

func countQMPBlockIOErrors(exchanges []destructiveQMPExchange) int {
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
		if json.Unmarshal(envelope.Data, &data) == nil && data.Operation == "write" && data.Action == "report" && !data.NoSpace && data.Reason != "" {
			count++
		}
	}
	return count
}

func validateQMPEIOProofStructure(proof destructiveQMPEIOProof) error {
	if proof.SchemaVersion != destructiveQMPEIOProofSchema || !filepath.IsAbs(proof.TargetImage) || !filepath.IsAbs(proof.BlkdebugConfig) ||
		!qualificationHexDigest(proof.BlkdebugSHA256) || !qualificationHexDigest(proof.ResultSHA256) || proof.InjectedSectorCount <= 0 ||
		proof.StartedAt.IsZero() || !proof.FinishedAt.After(proof.StartedAt) || len(proof.Exchanges) < 8 || len(proof.Exchanges) > 256 {
		return errors.New("QMP EIO proof identity, timing or exchange count is invalid")
	}
	if count := countQMPBlockIOErrors(proof.Exchanges); count < 1 || count > 32 {
		return errors.New("QMP EIO proof lacks bounded write BLOCK_IO_ERROR evidence")
	}
	return validateQMPOrderedExchanges(proof.Exchanges, []qmpExpectedCommand{
		{execute: "qmp_capabilities", id: "eio-capabilities"},
		{execute: "query-block", id: "eio-query-block-before", validate: func(envelope qmpMessageEnvelope) error {
			return validateQMPBlkdebugInventory(envelope.Return, proof.TargetImage, proof.BlkdebugConfig)
		}},
		{execute: "query-block", id: "eio-query-block-after", validate: func(envelope qmpMessageEnvelope) error {
			return validateQMPBlkdebugInventory(envelope.Return, proof.TargetImage, proof.BlkdebugConfig)
		}},
	})
}

func runDestructiveQMPEIOProofCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-qemu-eio-proof-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	proofPath := flags.String("proof", "", "QMP EIO proof")
	resultPath := flags.String("result", "", "bound guest EIO result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *proofPath == "" || *resultPath == "" {
		return errors.New("destructive-qemu-eio-proof-check requires --proof and --result")
	}
	var proof destructiveQMPEIOProof
	proofRaw, err := readQualificationReceipt(*proofPath, &proof)
	if err != nil {
		return err
	}
	if err := validateQMPEIOProofStructure(proof); err != nil {
		return err
	}
	configRaw, err := os.ReadFile(proof.BlkdebugConfig)
	if err != nil || qualificationSHA256(configRaw) != proof.BlkdebugSHA256 {
		return errors.New("QMP EIO proof blkdebug config is missing or mismatched")
	}
	sectors, err := validateBlkdebugEIOConfig(configRaw)
	if err != nil || len(sectors) != proof.InjectedSectorCount {
		return errors.Join(err, errors.New("QMP EIO proof sector rules do not reproduce"))
	}
	var result destructiveEIOWorkerResult
	resultRaw, err := readQualificationReceipt(*resultPath, &result)
	if err != nil || qualificationSHA256(resultRaw) != proof.ResultSHA256 {
		return errors.Join(err, errors.New("QMP EIO proof result is missing or mismatched"))
	}
	if err := validateDestructiveEIOWorkerResult(result); err != nil {
		return err
	}
	packet := struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		ProofSHA256   string `json:"proofSha256"`
		Passed        bool   `json:"passed"`
	}{SchemaVersion: 1, ProofSHA256: qualificationSHA256(proofRaw), Passed: true}
	return json.NewEncoder(stdout).Encode(packet)
}

func runDestructiveEIOBundleCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-eio-bundle-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	seedPath := flags.String("seed", "", "seed result")
	resultPath := flags.String("result", "", "guest EIO result")
	proofPath := flags.String("proof", "", "QMP EIO proof")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *seedPath == "" || *resultPath == "" || *proofPath == "" {
		return errors.New("destructive-eio-bundle-check requires --seed, --result and --proof")
	}
	var seed destructiveEIOSeedResult
	seedRaw, err := readQualificationReceipt(*seedPath, &seed)
	if err != nil {
		return err
	}
	if err := validateDestructiveEIOSeedResult(seed); err != nil {
		return err
	}
	var result destructiveEIOWorkerResult
	resultRaw, err := readQualificationReceipt(*resultPath, &result)
	if err != nil {
		return err
	}
	if err := validateDestructiveEIOWorkerResult(result); err != nil {
		return err
	}
	var proof destructiveQMPEIOProof
	proofRaw, err := readQualificationReceipt(*proofPath, &proof)
	if err != nil {
		return err
	}
	if err := validateQMPEIOProofStructure(proof); err != nil {
		return err
	}
	if seed.BuildRevision != result.BuildRevision || seed.BuildModified != result.BuildModified || seed.GOOS != result.GOOS ||
		seed.GOARCH != result.GOARCH || seed.GoVersion != result.GoVersion || seed.DatabaseSHA256 != result.BeforeSHA256 ||
		seed.CommitSequence != result.BeforeSequence || proof.ResultSHA256 != qualificationSHA256(resultRaw) {
		return errors.New("EIO seed, guest result and QMP proof do not share one build and database transition")
	}
	configRaw, err := os.ReadFile(proof.BlkdebugConfig)
	if err != nil || qualificationSHA256(configRaw) != proof.BlkdebugSHA256 {
		return errors.Join(err, errors.New("EIO bundle blkdebug config is missing or mismatched"))
	}
	sectors, err := validateBlkdebugEIOConfig(configRaw)
	if err != nil || len(sectors) != proof.InjectedSectorCount {
		return errors.Join(err, errors.New("EIO bundle sector coverage is mismatched"))
	}
	if _, err := existingRegularAbsolutePath(proof.TargetImage); err != nil {
		return errors.Join(err, errors.New("EIO bundle target image is missing"))
	}
	verified, err := verifyDestructiveEIODatabase(result.DatabaseArtifact)
	if err != nil || fmt.Sprintf("%x", verified.SHA256) != result.AfterSHA256 || verified.Meta.CommitSequence != result.RecoveredSequence {
		return errors.Join(err, errors.New("EIO bundle recovered database is missing or mismatched"))
	}
	packet := struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		SeedSHA256    string `json:"seedSha256"`
		ResultSHA256  string `json:"resultSha256"`
		ProofSHA256   string `json:"proofSha256"`
		Passed        bool   `json:"passed"`
	}{
		SchemaVersion: 1, SeedSHA256: qualificationSHA256(seedRaw), ResultSHA256: qualificationSHA256(resultRaw),
		ProofSHA256: qualificationSHA256(proofRaw), Passed: true,
	}
	return json.NewEncoder(stdout).Encode(packet)
}
