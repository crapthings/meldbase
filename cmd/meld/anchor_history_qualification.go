package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"reflect"
	"runtime"
	"time"

	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/integrations/anchorhttp"
	"github.com/crapthings/meldbase/internal/qualification"
)

const (
	anchorHistoryControllerSchema uint32 = 3
	anchorHistoryReceiptSchema    uint32 = 1
	anchorHistoryFinalLoadID             = "qualification-final-load"
)

type anchorHistoryWireValue struct {
	Exists                bool   `json:"exists"`
	DatabaseIDHex         string `json:"databaseIdHex,omitempty"`
	MinimumCommitSequence uint64 `json:"minimumCommitSequence"`
	MinimumGeneration     uint64 `json:"minimumGeneration"`
}

type anchorHistoryWireOperation struct {
	ID      string                 `json:"id"`
	Kind    string                 `json:"kind"`
	Outcome string                 `json:"outcome"`
	Invoke  uint64                 `json:"invoke"`
	Return  uint64                 `json:"return"`
	Value   anchorHistoryWireValue `json:"value"`
}

type anchorHistoryControllerRecord struct {
	SchemaVersion   uint32                       `json:"schemaVersion"`
	RunID           string                       `json:"runId"`
	ConfigurationID string                       `json:"configurationId"`
	SourceRevision  string                       `json:"sourceRevision"`
	BuildRevision   string                       `json:"buildRevision,omitempty"`
	BuildModified   bool                         `json:"buildModified"`
	GOOS            string                       `json:"goos"`
	GOARCH          string                       `json:"goarch"`
	GoVersion       string                       `json:"goVersion"`
	Agents          []anchorHistoryAgentIdentity `json:"agents"`
	Initial         anchorHistoryWireValue       `json:"initial"`
	Operations      []anchorHistoryWireOperation `json:"operations"`
	Fragments       []anchorHistoryAgentFragment `json:"fragments"`
}

type anchorHistoryQualificationReceipt struct {
	SchemaVersion          uint32                           `json:"schemaVersion"`
	ProtocolVersion        uint32                           `json:"protocolVersion"`
	RunID                  string                           `json:"runId"`
	ControllerSHA256       string                           `json:"controllerSha256"`
	ExternalEvidenceSHA256 string                           `json:"externalEvidenceSha256"`
	SourceRevision         string                           `json:"sourceRevision,omitempty"`
	BuildRevision          string                           `json:"buildRevision,omitempty"`
	BuildModified          bool                             `json:"buildModified"`
	GOOS                   string                           `json:"goos"`
	GOARCH                 string                           `json:"goarch"`
	GoVersion              string                           `json:"goVersion"`
	StartedAt              time.Time                        `json:"startedAt"`
	FinishedAt             time.Time                        `json:"finishedAt"`
	ConfigurationID        string                           `json:"configurationId"`
	Replicas               uint64                           `json:"replicas"`
	Quorum                 uint64                           `json:"quorum"`
	Members                []anchorQualificationMember      `json:"members"`
	Initial                anchorHistoryWireValue           `json:"initial"`
	Operations             []anchorHistoryWireOperation     `json:"operations"`
	Check                  qualification.AnchorHistoryCheck `json:"check"`
	Passed                 bool                             `json:"passed"`
	SigningPublicKey       string                           `json:"signingPublicKey"`
	Signature              string                           `json:"signature"`
}

func runAnchorHistoryQualificationSign(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("anchor-qualification history-sign", flag.ContinueOnError)
	flags.SetOutput(stderr)
	historyPath := flags.String("history", "", "strict controller history JSON")
	outputPath := flags.String("out", "", "new exclusive signed history receipt")
	signingKeyPath := flags.String("signing-key", "", "private Ed25519 receipt signing key")
	externalEvidence := flags.String("external-evidence-sha256", "", "SHA-256 of secured topology and fault-controller evidence")
	sourceRevision := flags.String("source-revision", "", "exact 40- or 64-hex source revision")
	requireClean := flags.Bool("require-clean-source", false, "require clean build metadata matching source revision")
	timeout := flags.Duration("timeout", 10*time.Second, "deadline for each full-member probe or quorum load")
	cluster := flags.String("cluster", "", "static anchor cluster ID")
	anchorName := flags.String("anchor-name", "", "disposable qualification anchor resource name")
	keyID := flags.String("key-id", "", "anchor HMAC key ID")
	keyFile := flags.String("key-file", "", "private base64 anchor HMAC key file")
	serverCA := flags.String("ca", "", "anchor server CA PEM")
	clientCert := flags.String("client-cert", "", "anchor mTLS client certificate PEM")
	clientKey := flags.String("client-key", "", "private anchor mTLS client key PEM")
	var replicas anchorReplicaFlags
	flags.Var(&replicas, "replica", "repeatable member-id=https://endpoint")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *historyPath == "" || *outputPath == "" || *signingKeyPath == "" || !validDurabilitySourceRevision(*sourceRevision) || !qualificationHexDigest(*externalEvidence) || *timeout <= 0 || *timeout > time.Hour || flags.NArg() != 0 {
		return errors.New("anchor-qualification history-sign requires history, output, signing key, source revision, external evidence and a valid timeout")
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if *requireClean && (*sourceRevision == "" || buildRevision != *sourceRevision || buildModified) {
		return errors.New("anchor history qualification clean source verification failed")
	}
	var controller anchorHistoryControllerRecord
	controllerRaw, err := readQualificationReceipt(*historyPath, &controller)
	if err != nil {
		return fmt.Errorf("controller history: %w", err)
	}
	history, err := validateAnchorHistoryController(controller, qualification.MaxAnchorHistoryOperations-1)
	if err != nil {
		return err
	}
	if controller.SourceRevision != *sourceRevision {
		return errors.New("controller history source revision differs from the signer revision")
	}
	if *requireClean {
		if controller.BuildRevision != *sourceRevision || controller.BuildModified {
			return errors.New("controller history was not produced by a clean release build")
		}
		for _, fragment := range controller.Fragments {
			if fragment.SourceRevision != *sourceRevision || fragment.BuildRevision != *sourceRevision || fragment.BuildModified {
				return errors.New("controller history contains an agent that was not a clean release build")
			}
		}
	}
	precheck, err := qualification.CheckAnchorHistory(history)
	if err != nil || !precheck.Linearizable {
		return errors.Join(errors.New("controller history is not linearizable before final observation"), err)
	}
	privateKey, err := loadAnchorQualificationPrivateKey(*signingKeyPath)
	if err != nil {
		return err
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	config := remoteAnchorConfig{
		clusterID: *cluster, replicaSpecs: replicas, anchorName: *anchorName, keyID: *keyID, keyFile: *keyFile,
		serverCAFile: *serverCA, clientCertFile: *clientCert, clientKeyFile: *clientKey, operationTimeout: *timeout,
	}
	store, transport, err := newRemoteAnchorStore(config)
	if err != nil {
		return err
	}
	defer transport.CloseIdleConnections()
	if len(config.replicaSpecs) < 3 || len(config.replicaSpecs)%2 == 0 {
		return errors.New("history qualification requires an odd multi-member static quorum")
	}
	if controller.ConfigurationID != store.ConfigurationID() {
		return errors.New("controller history was executed against a different anchor configuration")
	}
	started := time.Now().UTC()
	before, err := anchorHistoryProbeMembers(store, config.replicaSpecs, *timeout)
	if err != nil {
		return err
	}
	operation, cancel := context.WithTimeout(context.Background(), *timeout)
	retained, exists, err := store.Load(operation)
	cancel()
	if err != nil {
		return fmt.Errorf("history qualification final quorum load: %w", err)
	}
	after, err := anchorHistoryProbeMembers(store, config.replicaSpecs, *timeout)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(before, after) {
		return errors.New("anchor members changed during final history observation")
	}
	maximumEvent := uint64(0)
	for _, entry := range controller.Operations {
		if entry.Invoke > maximumEvent {
			maximumEvent = entry.Invoke
		}
		if entry.Return > maximumEvent {
			maximumEvent = entry.Return
		}
	}
	if maximumEvent > math.MaxUint64-2 {
		return errors.New("controller history event ordinal is exhausted")
	}
	finalValue := anchorHistoryWireValueFromAnchor(retained, exists)
	finalOperation := anchorHistoryWireOperation{
		ID: anchorHistoryFinalLoadID, Kind: string(qualification.AnchorHistoryLoad), Outcome: string(qualification.AnchorHistorySucceeded),
		Invoke: maximumEvent + 1, Return: maximumEvent + 2, Value: finalValue,
	}
	operations := append(append([]anchorHistoryWireOperation(nil), controller.Operations...), finalOperation)
	completeHistory, err := anchorHistoryFromWire(controller.Initial, operations)
	if err != nil {
		return err
	}
	check, err := qualification.CheckAnchorHistory(completeHistory)
	if err != nil || !check.Linearizable {
		return errors.Join(errors.New("final anchor history is not linearizable"), err)
	}
	receipt := anchorHistoryQualificationReceipt{
		SchemaVersion: anchorHistoryReceiptSchema, ProtocolVersion: anchorhttp.ProtocolVersion, RunID: controller.RunID,
		ControllerSHA256: qualificationSHA256(controllerRaw), ExternalEvidenceSHA256: *externalEvidence,
		SourceRevision: *sourceRevision, BuildRevision: buildRevision, BuildModified: buildModified,
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(), StartedAt: started, FinishedAt: time.Now().UTC(),
		ConfigurationID: store.ConfigurationID(), Replicas: uint64(len(config.replicaSpecs)), Quorum: uint64(len(config.replicaSpecs)/2 + 1), Members: after,
		Initial: controller.Initial, Operations: operations, Check: check, Passed: true,
		SigningPublicKey: base64.StdEncoding.EncodeToString(publicKey),
	}
	if err := signAnchorHistoryQualificationReceipt(&receipt, privateKey); err != nil {
		return err
	}
	if err := validateAnchorHistoryQualificationReceipt(receipt); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(*outputPath, receipt); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(receipt)
}

func runAnchorHistoryQualificationVerify(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("anchor-qualification history-verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	publicKeyPath := flags.String("public-key", "", "Ed25519 verification key")
	receiptPath := flags.String("receipt", "", "signed history qualification receipt")
	historyPath := flags.String("history", "", "exact controller history JSON bound by the receipt")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *publicKeyPath == "" || *receiptPath == "" || *historyPath == "" || flags.NArg() != 0 {
		return errors.New("anchor-qualification history-verify requires public key, receipt and controller history")
	}
	publicKey, err := loadAnchorQualificationPublicKey(*publicKeyPath)
	if err != nil {
		return err
	}
	verified, err := verifyAnchorHistoryQualificationFiles(*receiptPath, *historyPath, publicKey)
	if err != nil {
		return err
	}
	result := struct {
		SchemaVersion  uint32 `json:"schemaVersion"`
		RunID          string `json:"runId"`
		Operations     int    `json:"operations"`
		ExploredStates uint64 `json:"exploredStates"`
		ReceiptSHA256  string `json:"receiptSha256"`
		Passed         bool   `json:"passed"`
	}{1, verified.Receipt.RunID, len(verified.Receipt.Operations), verified.Receipt.Check.ExploredStates, verified.ReceiptSHA256, true}
	return json.NewEncoder(stdout).Encode(result)
}

type anchorHistoryQualificationVerification struct {
	Receipt          anchorHistoryQualificationReceipt
	Controller       anchorHistoryControllerRecord
	ReceiptSHA256    string
	ControllerSHA256 string
}

func verifyAnchorHistoryQualificationFiles(receiptPath, historyPath string, publicKey ed25519.PublicKey) (anchorHistoryQualificationVerification, error) {
	if receiptPath == "" || historyPath == "" || len(publicKey) != ed25519.PublicKeySize {
		return anchorHistoryQualificationVerification{}, errors.New("anchor history qualification verification inputs are incomplete")
	}
	var receipt anchorHistoryQualificationReceipt
	receiptRaw, err := readQualificationReceipt(receiptPath, &receipt)
	if err != nil {
		return anchorHistoryQualificationVerification{}, err
	}
	if err := verifyAnchorHistoryQualificationReceipt(receipt, publicKey); err != nil {
		return anchorHistoryQualificationVerification{}, err
	}
	var controller anchorHistoryControllerRecord
	controllerRaw, err := readQualificationReceipt(historyPath, &controller)
	if err != nil {
		return anchorHistoryQualificationVerification{}, err
	}
	if _, err := validateAnchorHistoryController(controller, qualification.MaxAnchorHistoryOperations-1); err != nil {
		return anchorHistoryQualificationVerification{}, err
	}
	if receipt.ControllerSHA256 != qualificationSHA256(controllerRaw) || receipt.RunID != controller.RunID || receipt.ConfigurationID != controller.ConfigurationID ||
		receipt.SourceRevision != controller.SourceRevision || receipt.Initial != controller.Initial || len(receipt.Operations) != len(controller.Operations)+1 {
		return anchorHistoryQualificationVerification{}, errors.New("signed receipt does not bind the supplied controller history")
	}
	for index := range controller.Operations {
		if receipt.Operations[index] != controller.Operations[index] {
			return anchorHistoryQualificationVerification{}, errors.New("signed receipt changes a controller operation")
		}
	}
	return anchorHistoryQualificationVerification{
		Receipt: receipt, Controller: controller, ReceiptSHA256: qualificationSHA256(receiptRaw), ControllerSHA256: qualificationSHA256(controllerRaw),
	}, nil
}

func validateAnchorHistoryController(controller anchorHistoryControllerRecord, maximum int) (qualification.AnchorHistory, error) {
	if controller.SchemaVersion != anchorHistoryControllerSchema || !anchorQualificationHex(controller.RunID, 16) || !qualificationHexDigest(controller.ConfigurationID) ||
		!validDurabilitySourceRevision(controller.SourceRevision) || (controller.BuildRevision != "" && !validDurabilitySourceRevision(controller.BuildRevision)) ||
		controller.GOOS == "" || controller.GOARCH == "" || controller.GoVersion == "" ||
		len(controller.Operations) < 1 || len(controller.Operations) > maximum {
		return qualification.AnchorHistory{}, errors.New("controller history identity or operation count is invalid")
	}
	if err := validateAnchorHistoryAgentEvidence(controller); err != nil {
		return qualification.AnchorHistory{}, err
	}
	for _, operation := range controller.Operations {
		if operation.ID == anchorHistoryFinalLoadID {
			return qualification.AnchorHistory{}, errors.New("controller history uses the reserved final-load ID")
		}
	}
	history, err := anchorHistoryFromWire(controller.Initial, controller.Operations)
	if err != nil {
		return qualification.AnchorHistory{}, err
	}
	if _, err := qualification.CheckAnchorHistory(history); err != nil {
		return qualification.AnchorHistory{}, err
	}
	return history, nil
}

func anchorHistoryFromWire(initial anchorHistoryWireValue, operations []anchorHistoryWireOperation) (qualification.AnchorHistory, error) {
	convertedInitial, err := anchorHistoryValueFromWire(initial)
	if err != nil {
		return qualification.AnchorHistory{}, fmt.Errorf("initial anchor history value: %w", err)
	}
	converted := make([]qualification.AnchorHistoryOperation, len(operations))
	for index, operation := range operations {
		value, err := anchorHistoryValueFromWire(operation.Value)
		if err != nil {
			return qualification.AnchorHistory{}, fmt.Errorf("anchor history operation %d: %w", index+1, err)
		}
		converted[index] = qualification.AnchorHistoryOperation{
			ID: operation.ID, Kind: qualification.AnchorHistoryOperationKind(operation.Kind), Outcome: qualification.AnchorHistoryOutcome(operation.Outcome),
			Invoke: operation.Invoke, Return: operation.Return, Value: value,
		}
	}
	return qualification.AnchorHistory{Initial: convertedInitial, Operations: converted}, nil
}

func anchorHistoryValueFromWire(value anchorHistoryWireValue) (qualification.AnchorHistoryValue, error) {
	if !value.Exists {
		if value.DatabaseIDHex != "" || value.MinimumCommitSequence != 0 || value.MinimumGeneration != 0 {
			return qualification.AnchorHistoryValue{}, errors.New("missing value contains anchor fields")
		}
		return qualification.AnchorHistoryValue{}, nil
	}
	if !anchorQualificationHex(value.DatabaseIDHex, 16) || value.MinimumGeneration == 0 {
		return qualification.AnchorHistoryValue{}, errors.New("existing value is invalid")
	}
	decoded, _ := hex.DecodeString(value.DatabaseIDHex)
	converted := qualification.AnchorHistoryValue{Exists: true, CommitSequence: value.MinimumCommitSequence, Generation: value.MinimumGeneration}
	copy(converted.DatabaseID[:], decoded)
	return converted, nil
}

func anchorHistoryWireValueFromAnchor(anchor meldbase.RollbackAnchor, exists bool) anchorHistoryWireValue {
	if !exists {
		return anchorHistoryWireValue{}
	}
	return anchorHistoryWireValue{
		Exists: true, DatabaseIDHex: hex.EncodeToString(anchor.DatabaseID[:]),
		MinimumCommitSequence: anchor.MinimumCommitSequence, MinimumGeneration: anchor.MinimumGeneration,
	}
}

func anchorHistoryProbeMembers(store *anchorhttp.QuorumStore, replicas []string, timeout time.Duration) ([]anchorQualificationMember, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	checks, err := store.CheckReplicas(ctx)
	cancel()
	if err != nil {
		return nil, err
	}
	members, _, _ := anchorQualificationMembers(checks, replicas)
	for _, member := range members {
		if member.State != string(anchorhttp.ReplicaAvailable) && member.State != string(anchorhttp.ReplicaMissing) {
			return nil, fmt.Errorf("history qualification member %q is not reachable: %s", member.MemberID, member.State)
		}
	}
	return members, nil
}

func validateAnchorHistoryQualificationReceipt(receipt anchorHistoryQualificationReceipt) error {
	if receipt.SchemaVersion != anchorHistoryReceiptSchema || receipt.ProtocolVersion != anchorhttp.ProtocolVersion || !anchorQualificationHex(receipt.RunID, 16) ||
		!qualificationHexDigest(receipt.ControllerSHA256) || !qualificationHexDigest(receipt.ExternalEvidenceSHA256) || !qualificationHexDigest(receipt.ConfigurationID) ||
		receipt.Replicas < 3 || receipt.Replicas%2 == 0 || receipt.Quorum != receipt.Replicas/2+1 || uint64(len(receipt.Members)) != receipt.Replicas ||
		receipt.StartedAt.IsZero() || !receipt.FinishedAt.After(receipt.StartedAt) || receipt.GOOS == "" || receipt.GOARCH == "" || receipt.GoVersion == "" ||
		!receipt.Passed || receipt.SigningPublicKey == "" || receipt.Signature == "" || len(receipt.Operations) < 2 || len(receipt.Operations) > qualification.MaxAnchorHistoryOperations {
		return errors.New("anchor history qualification receipt is incomplete")
	}
	if !validDurabilitySourceRevision(receipt.SourceRevision) {
		return errors.New("anchor history qualification source revision is invalid")
	}
	last := receipt.Operations[len(receipt.Operations)-1]
	if last.ID != anchorHistoryFinalLoadID || last.Kind != string(qualification.AnchorHistoryLoad) || last.Outcome != string(qualification.AnchorHistorySucceeded) {
		return errors.New("anchor history qualification lacks its final load")
	}
	history, err := anchorHistoryFromWire(receipt.Initial, receipt.Operations)
	if err != nil {
		return err
	}
	recomputed, err := qualification.CheckAnchorHistory(history)
	if err != nil || !recomputed.Linearizable || !reflect.DeepEqual(recomputed, receipt.Check) || receipt.Check.Violation != "" {
		return errors.New("anchor history qualification checker result is invalid")
	}
	seen := make(map[string]struct{}, len(receipt.Members))
	for _, member := range receipt.Members {
		if member.MemberID == "" || !qualificationHexDigest(member.EndpointSHA256) || (member.State != string(anchorhttp.ReplicaAvailable) && member.State != string(anchorhttp.ReplicaMissing)) {
			return errors.New("anchor history qualification member evidence is invalid")
		}
		if _, duplicate := seen[member.MemberID]; duplicate {
			return errors.New("anchor history qualification member is duplicated")
		}
		seen[member.MemberID] = struct{}{}
		if member.State == string(anchorhttp.ReplicaAvailable) {
			if !member.Exists || !anchorQualificationHex(member.DatabaseIDHex, 16) || member.MinimumGeneration == 0 {
				return errors.New("available history qualification member has an invalid anchor")
			}
		} else if member.Exists || member.DatabaseIDHex != "" || member.MinimumCommitSequence != 0 || member.MinimumGeneration != 0 {
			return errors.New("missing history qualification member claims an anchor")
		}
	}
	if !anchorHistoryFinalValueSupported(last.Value, receipt.Members, receipt.Quorum) {
		return errors.New("final history load is not supported by the recorded member quorum")
	}
	return nil
}

func anchorHistoryFinalValueSupported(value anchorHistoryWireValue, members []anchorQualificationMember, quorum uint64) bool {
	if !value.Exists {
		var missing uint64
		for _, member := range members {
			if member.State == string(anchorhttp.ReplicaMissing) {
				missing++
			}
		}
		return missing >= quorum
	}
	var dominated uint64
	observed := false
	for _, member := range members {
		if member.State == string(anchorhttp.ReplicaMissing) {
			dominated++
			continue
		}
		if member.DatabaseIDHex == value.DatabaseIDHex && member.MinimumCommitSequence <= value.MinimumCommitSequence && member.MinimumGeneration <= value.MinimumGeneration {
			dominated++
			if member.MinimumCommitSequence == value.MinimumCommitSequence && member.MinimumGeneration == value.MinimumGeneration {
				observed = true
			}
		}
	}
	return observed && dominated >= quorum
}

func signAnchorHistoryQualificationReceipt(receipt *anchorHistoryQualificationReceipt, privateKey ed25519.PrivateKey) error {
	receipt.Signature = ""
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	receipt.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return nil
}

func verifyAnchorHistoryQualificationReceipt(receipt anchorHistoryQualificationReceipt, publicKey ed25519.PublicKey) error {
	if err := validateAnchorHistoryQualificationReceipt(receipt); err != nil {
		return err
	}
	embedded, err := base64.StdEncoding.Strict().DecodeString(receipt.SigningPublicKey)
	if err != nil || !ed25519.PublicKey(embedded).Equal(publicKey) {
		return errors.New("anchor history qualification signing public key differs")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(receipt.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("anchor history qualification signature is malformed")
	}
	receipt.Signature = ""
	payload, err := json.Marshal(receipt)
	if err != nil || !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("anchor history qualification signature verification failed")
	}
	return nil
}
