package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

const (
	destructivePhysicalPowerEventSchema   uint32 = 2
	destructivePowerControllerProofSchema uint32 = 1
	destructivePowerAdapterProtocolSchema uint32 = 1
	destructivePowerControllerDomain             = "meldbase/destructive-power-controller-event/v1\x00"
	destructivePowerAdapterMaxOutput             = 1 << 20
)

var destructivePowerControllerBuildIdentity = durabilityBuildIdentity

type destructivePowerAdapterRequest struct {
	SchemaVersion        uint32    `json:"schemaVersion"`
	ControllerRunID      string    `json:"controllerRunId"`
	TrialID              string    `json:"trialId"`
	Method               string    `json:"method"`
	MarkerSHA256         string    `json:"markerSha256"`
	BootIDBefore         string    `json:"bootIdBefore"`
	TargetIdentitySHA256 string    `json:"targetIdentitySha256"`
	RequestedAt          time.Time `json:"requestedAt"`
}

type destructivePowerAdapterResponse struct {
	SchemaVersion          uint32    `json:"schemaVersion"`
	ControllerRunID        string    `json:"controllerRunId"`
	TrialID                string    `json:"trialId"`
	Method                 string    `json:"method"`
	OperationID            string    `json:"operationId"`
	TargetIdentitySHA256   string    `json:"targetIdentitySha256"`
	AcceptedAt             time.Time `json:"acceptedAt"`
	PowerLostAt            time.Time `json:"powerLostAt"`
	PowerRestoredAt        time.Time `json:"powerRestoredAt"`
	HardwareEvidenceSHA256 string    `json:"hardwareEvidenceSha256"`
	HardwareEvidenceBase64 string    `json:"hardwareEvidenceBase64"`
	Success                bool      `json:"success"`
}

type destructivePowerControllerProof struct {
	SchemaVersion       uint32                          `json:"schemaVersion"`
	SourceRevision      string                          `json:"sourceRevision"`
	BuildRevision       string                          `json:"buildRevision"`
	BuildModified       bool                            `json:"buildModified"`
	GOOS                string                          `json:"goos"`
	GOARCH              string                          `json:"goarch"`
	GoVersion           string                          `json:"goVersion"`
	AdapterSHA256       string                          `json:"adapterSha256"`
	AdapterStderrSHA256 string                          `json:"adapterStderrSha256"`
	StartedAt           time.Time                       `json:"startedAt"`
	FinishedAt          time.Time                       `json:"finishedAt"`
	Request             destructivePowerAdapterRequest  `json:"request"`
	Response            destructivePowerAdapterResponse `json:"response"`
}

type destructiveBoundedBuffer struct {
	buffer  bytes.Buffer
	maximum int
}

func (buffer *destructiveBoundedBuffer) Write(value []byte) (int, error) {
	if len(value) > buffer.maximum-buffer.buffer.Len() {
		return 0, errors.New("power controller adapter output exceeded 1 MiB")
	}
	return buffer.buffer.Write(value)
}

func runDestructivePowerControllerKeygen(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-power-controller-keygen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	privatePath := flags.String("private", "", "new external controller-agent private Ed25519 key")
	publicPath := flags.String("public", "", "new controller-agent public verification key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *privatePath == "" || *publicPath == "" || flags.NArg() != 0 {
		return errors.New("destructive-power-controller-keygen requires private and public paths")
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	if err := writeAnchorQualificationKey(*privatePath, privateKey, 0o600); err != nil {
		return err
	}
	if err := writeAnchorQualificationKey(*publicPath, publicKey, 0o644); err != nil {
		return errors.Join(err, os.Remove(*privatePath))
	}
	_, err = fmt.Fprintln(stdout, "Created independent physical power controller-agent signing and verification keys")
	return err
}

func runDestructivePowerControllerRun(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-power-controller-run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	markerPath := flags.String("marker", "", "durable marker visible to the external controller host")
	method := flags.String("method", "", "physical hard-reset controller method")
	targetIdentity := flags.String("target-identity-sha256", "", "pre-approved physical server/chassis/outlet identity")
	adapterPath := flags.String("adapter", "", "fixed executable implementing adapter protocol v1 on stdin/stdout")
	privateKeyPath := flags.String("signing-key", "", "controller-agent private Ed25519 key")
	proofPath := flags.String("proof", "", "new durable structured controller proof")
	outputPath := flags.String("out", "", "new durable signed controller event")
	sourceRevision := flags.String("source-revision", "", "exact clean controller-agent source revision")
	timeout := flags.Duration("timeout", 5*time.Minute, "adapter execution deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *markerPath == "" || !qualificationPhysicalPowerMethod(*method) || !qualificationHexDigest(*targetIdentity) || *adapterPath == "" || *privateKeyPath == "" || *proofPath == "" || *outputPath == "" || !validDurabilitySourceRevision(*sourceRevision) || *timeout < 5*time.Second || *timeout > 30*time.Minute || flags.NArg() != 0 {
		return errors.New("destructive-power-controller-run requires marker, physical method and target identity, adapter, signing key, proof, out, clean source revision and timeout from 5s to 30m")
	}
	if runtime.GOOS != "linux" {
		return errors.New("physical power controller execution requires Linux file-descriptor binding")
	}
	buildRevision, buildModified := destructivePowerControllerBuildIdentity()
	if buildRevision != *sourceRevision || buildModified {
		return errors.New("physical power controller requires a clean binary built from the qualified revision")
	}
	markerPathValue, proofPathValue, outputPathValue, err := validateDestructivePowerControllerPaths(*markerPath, *proofPath, *outputPath)
	if err != nil {
		return err
	}
	var marker destructivePowerMarker
	markerRaw, err := readQualificationReceipt(markerPathValue, &marker)
	if err != nil {
		return err
	}
	if err := validateDestructivePowerMarkerForController(marker, *sourceRevision); err != nil {
		return err
	}
	adapterFile, adapterSHA, err := openDestructivePowerAdapter(*adapterPath)
	if err != nil {
		return err
	}
	defer adapterFile.Close()
	privateKey, err := loadAnchorQualificationPrivateKey(*privateKeyPath)
	if err != nil {
		return err
	}
	runBytes := make([]byte, 16)
	if _, err := rand.Read(runBytes); err != nil {
		return err
	}
	started := time.Now().UTC()
	request := destructivePowerAdapterRequest{
		SchemaVersion: destructivePowerAdapterProtocolSchema, ControllerRunID: hex.EncodeToString(runBytes),
		TrialID: marker.TrialID, Method: *method, MarkerSHA256: qualificationSHA256(markerRaw),
		BootIDBefore: marker.BootIDBefore, TargetIdentitySHA256: *targetIdentity, RequestedAt: started,
	}
	requestRaw, err := json.Marshal(request)
	if err != nil {
		return err
	}
	requestRaw = append(requestRaw, '\n')
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	command := exec.CommandContext(ctx, "/proc/self/fd/3")
	command.ExtraFiles = []*os.File{adapterFile}
	command.Stdin = bytes.NewReader(requestRaw)
	command.Env = os.Environ()
	output, adapterStderr := &destructiveBoundedBuffer{maximum: destructivePowerAdapterMaxOutput}, &destructiveBoundedBuffer{maximum: destructivePowerAdapterMaxOutput}
	command.Stdout, command.Stderr = output, adapterStderr
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return errors.New("physical power controller adapter timed out")
		}
		return fmt.Errorf("physical power controller adapter failed: %w", err)
	}
	var response destructivePowerAdapterResponse
	if err := decodeOneStrictJSON(output.buffer.Bytes(), &response); err != nil {
		return fmt.Errorf("physical power controller adapter response: %w", err)
	}
	if err := validateDestructivePowerAdapterExchange(request, response); err != nil {
		return err
	}
	proof := destructivePowerControllerProof{destructivePowerControllerProofSchema, *sourceRevision, buildRevision, buildModified, runtime.GOOS, runtime.GOARCH, runtime.Version(), adapterSHA, qualificationSHA256(adapterStderr.buffer.Bytes()), started, time.Now().UTC(), request, response}
	if err := validateDestructivePowerControllerProof(proof, marker, markerRaw); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(proofPathValue, proof); err != nil {
		return err
	}
	proofRaw, err := os.ReadFile(proofPathValue)
	if err != nil {
		return err
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	event := destructivePowerControllerEvent{SchemaVersion: destructivePhysicalPowerEventSchema, TrialID: marker.TrialID, Method: *method, MarkerSHA256: qualificationSHA256(markerRaw), BootIDBefore: marker.BootIDBefore, MarkerObservedAt: request.RequestedAt, CutRequestedAt: response.AcceptedAt, PowerRestoredAt: response.PowerRestoredAt, ControllerProofSHA256: qualificationSHA256(proofRaw), ControllerRunID: request.ControllerRunID, ControllerPublicKey: base64.StdEncoding.EncodeToString(publicKey)}
	if err := signDestructivePowerControllerEvent(&event, privateKey); err != nil {
		return err
	}
	if err := verifyDestructivePhysicalPowerEvidence(proofRaw, event, markerRaw, publicKey); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(outputPathValue, event); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(event)
}

func openDestructivePowerAdapter(path string) (*os.File, string, error) {
	absolute, err := existingRegularAbsolutePath(path)
	if err != nil {
		return nil, "", err
	}
	file, err := os.Open(absolute)
	if err != nil {
		return nil, "", err
	}
	fail := func(err error) (*os.File, string, error) {
		return nil, "", errors.Join(err, file.Close())
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 || info.Size() <= 0 || info.Size() > 128<<20 {
		return fail(errors.New("power controller adapter must be a nonempty executable regular file no larger than 128 MiB"))
	}
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, (128<<20)+1))
	if err != nil || written != info.Size() || written > 128<<20 {
		return fail(errors.New("power controller adapter changed or exceeded its size bound while hashing"))
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	return file, hex.EncodeToString(hash.Sum(nil)), nil
}

func validateDestructivePowerControllerPaths(markerPath, proofPath, outputPath string) (string, string, string, error) {
	marker, err := existingRegularAbsolutePath(markerPath)
	if err != nil {
		return "", "", "", fmt.Errorf("controller marker path: %w", err)
	}
	proof, err := newAbsolutePath(proofPath)
	if err != nil {
		return "", "", "", fmt.Errorf("controller proof path: %w", err)
	}
	output, err := newAbsolutePath(outputPath)
	if err != nil {
		return "", "", "", fmt.Errorf("controller event path: %w", err)
	}
	markerDirectory := filepath.Dir(marker)
	if proof == output || filepath.Dir(proof) != markerDirectory || filepath.Dir(output) != markerDirectory {
		return "", "", "", errors.New("controller marker, proof and event must be distinct direct children of one durable control directory")
	}
	return marker, proof, output, nil
}

func decodeOneStrictJSON(raw []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("expected exactly one JSON value")
	}
	return nil
}

func validateDestructivePowerMarkerForController(marker destructivePowerMarker, sourceRevision string) error {
	if marker.SchemaVersion != destructivePowerMarkerSchema || marker.SourceRevision != sourceRevision || marker.BuildRevision != sourceRevision || marker.BuildModified || marker.GOOS != "linux" || marker.GOARCH == "" || !qualificationSafeName(marker.TrialID, 64) || !qualificationBoundaryAllowed(qualificationTrialPower, marker.Boundary) || !qualificationSafeName(marker.BootIDBefore, 64) || marker.StartedAt.IsZero() || marker.ReachedAt.Before(marker.StartedAt) || marker.OldCommitSequence != 1 || marker.NewCommitSequence != 2 || !qualificationHexDigest(marker.OldStateSHA256) || !qualificationHexDigest(marker.NewStateSHA256) || marker.OldStateSHA256 == marker.NewStateSHA256 || !validPowerDatabaseRelative(marker.DatabaseRelative, marker.TrialID) {
		return errors.New("physical power controller marker is incomplete or differs from the clean qualified revision")
	}
	return nil
}

func validateDestructivePowerAdapterExchange(request destructivePowerAdapterRequest, response destructivePowerAdapterResponse) error {
	hardwareEvidence, evidenceErr := base64.StdEncoding.Strict().DecodeString(response.HardwareEvidenceBase64)
	if request.SchemaVersion != destructivePowerAdapterProtocolSchema || !anchorQualificationHex(request.ControllerRunID, 16) || !qualificationSafeName(request.TrialID, 64) || !qualificationPhysicalPowerMethod(request.Method) || !qualificationHexDigest(request.MarkerSHA256) || !qualificationSafeName(request.BootIDBefore, 64) || !qualificationHexDigest(request.TargetIdentitySHA256) || request.RequestedAt.IsZero() || response.SchemaVersion != destructivePowerAdapterProtocolSchema || response.ControllerRunID != request.ControllerRunID || response.TrialID != request.TrialID || response.Method != request.Method || !qualificationSafeName(response.OperationID, 128) || response.TargetIdentitySHA256 != request.TargetIdentitySHA256 || response.AcceptedAt.Before(request.RequestedAt) || response.PowerLostAt.Before(response.AcceptedAt) || !response.PowerRestoredAt.After(response.PowerLostAt) || evidenceErr != nil || len(hardwareEvidence) == 0 || len(hardwareEvidence) > 512<<10 || qualificationSHA256(hardwareEvidence) != response.HardwareEvidenceSHA256 || !response.Success {
		return errors.New("physical power adapter response is incomplete, unsuccessful or not bound to its request")
	}
	return nil
}

func validateDestructivePowerControllerProof(proof destructivePowerControllerProof, marker destructivePowerMarker, markerRaw []byte) error {
	if proof.SchemaVersion != destructivePowerControllerProofSchema || !validDurabilitySourceRevision(proof.SourceRevision) || proof.BuildRevision != proof.SourceRevision || proof.BuildModified || proof.GOOS == "" || proof.GOARCH == "" || proof.GoVersion == "" || !qualificationHexDigest(proof.AdapterSHA256) || !qualificationHexDigest(proof.AdapterStderrSHA256) || proof.StartedAt.IsZero() || !proof.FinishedAt.After(proof.StartedAt) || proof.Request.TrialID != marker.TrialID || proof.Request.MarkerSHA256 != qualificationSHA256(markerRaw) || proof.Request.BootIDBefore != marker.BootIDBefore || proof.StartedAt != proof.Request.RequestedAt || proof.FinishedAt.Before(proof.Response.PowerRestoredAt) {
		return errors.New("physical power controller proof identity, build, timing or marker binding is invalid")
	}
	return validateDestructivePowerAdapterExchange(proof.Request, proof.Response)
}

func signDestructivePowerControllerEvent(event *destructivePowerControllerEvent, privateKey ed25519.PrivateKey) error {
	event.Signature = ""
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	payload = append([]byte(destructivePowerControllerDomain), payload...)
	event.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return nil
}

func verifyDestructivePhysicalPowerEvidence(proofRaw []byte, event destructivePowerControllerEvent, markerRaw []byte, publicKey ed25519.PublicKey) error {
	if event.SchemaVersion != destructivePhysicalPowerEventSchema || !qualificationPhysicalPowerMethod(event.Method) || !anchorQualificationHex(event.ControllerRunID, 16) || len(publicKey) != ed25519.PublicKeySize || qualificationSHA256(proofRaw) != event.ControllerProofSHA256 || event.MarkerSHA256 != qualificationSHA256(markerRaw) {
		return errors.New("physical power controller event is incomplete or not bound to its proof and marker")
	}
	embedded, err := base64.StdEncoding.Strict().DecodeString(event.ControllerPublicKey)
	if err != nil || !ed25519.PublicKey(embedded).Equal(publicKey) {
		return errors.New("physical power controller public key differs from the trusted key")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(event.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("physical power controller signature is malformed")
	}
	unsigned := event
	unsigned.Signature = ""
	payload, err := json.Marshal(unsigned)
	if err != nil {
		return err
	}
	payload = append([]byte(destructivePowerControllerDomain), payload...)
	if !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("physical power controller signature verification failed")
	}
	var proof destructivePowerControllerProof
	if err := decodeOneStrictJSON(proofRaw, &proof); err != nil {
		return err
	}
	var marker destructivePowerMarker
	if err := decodeOneStrictJSON(markerRaw, &marker); err != nil {
		return err
	}
	if err := validateDestructivePowerControllerProof(proof, marker, markerRaw); err != nil {
		return err
	}
	if proof.Request.ControllerRunID != event.ControllerRunID || proof.Request.TrialID != event.TrialID || proof.Request.Method != event.Method || proof.Request.RequestedAt != event.MarkerObservedAt || proof.Response.AcceptedAt != event.CutRequestedAt || proof.Response.PowerRestoredAt != event.PowerRestoredAt {
		return errors.New("physical power event differs from the signed adapter exchange")
	}
	return nil
}

func qualificationPhysicalPowerMethod(value string) bool {
	switch value {
	case "hypervisor-hard-reset", "ipmi-chassis-power-cycle", "pdu-power-cycle", "redfish-computer-system-power-cycle":
		return true
	default:
		return false
	}
}
