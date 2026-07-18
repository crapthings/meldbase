package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/crapthings/meldbase/integrations/anchorhttp"
	"github.com/crapthings/meldbase/internal/qualification"
)

func TestSignedAnchorHistoryReceiptRejectsTamperWrongKeyAndChangedController(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	publicPath := filepath.Join(directory, "history.pub")
	if err := writeAnchorQualificationKey(publicPath, publicKey, 0o644); err != nil {
		t.Fatal(err)
	}
	target := anchorHistoryWireValue{Exists: true, DatabaseIDHex: strings.Repeat("01", 16), MinimumCommitSequence: 1, MinimumGeneration: 2}
	controller := anchorHistoryControllerRecord{
		SchemaVersion: anchorHistoryControllerSchema, RunID: strings.Repeat("ab", 16), ConfigurationID: strings.Repeat("e", 64),
		Operations: []anchorHistoryWireOperation{
			{ID: "ambiguous-advance", Kind: "advance", Outcome: "failed", Invoke: 1, Return: 2, Value: target},
			{ID: "observed-load", Kind: "load", Outcome: "succeeded", Invoke: 3, Return: 4, Value: target},
		},
	}
	attachAnchorHistoryTestAgentEvidence(t, &controller)
	controllerPath := filepath.Join(directory, "controller.json")
	if err := writeJSONExclusiveDurable(controllerPath, controller); err != nil {
		t.Fatal(err)
	}
	controllerRaw, err := os.ReadFile(controllerPath)
	if err != nil {
		t.Fatal(err)
	}
	operations := append(append([]anchorHistoryWireOperation(nil), controller.Operations...), anchorHistoryWireOperation{
		ID: anchorHistoryFinalLoadID, Kind: "load", Outcome: "succeeded", Invoke: 5, Return: 6, Value: target,
	})
	history, err := anchorHistoryFromWire(controller.Initial, operations)
	if err != nil {
		t.Fatal(err)
	}
	check, err := qualification.CheckAnchorHistory(history)
	if err != nil || !check.Linearizable || len(check.Linearization) != 3 || !check.Linearization[0].AmbiguousApplied {
		t.Fatalf("check=%+v err=%v", check, err)
	}
	members := []anchorQualificationMember{
		{MemberID: "member-a", EndpointSHA256: strings.Repeat("a", 64), State: string(anchorhttp.ReplicaAvailable), Exists: true, DatabaseIDHex: target.DatabaseIDHex, MinimumCommitSequence: 1, MinimumGeneration: 2},
		{MemberID: "member-b", EndpointSHA256: strings.Repeat("b", 64), State: string(anchorhttp.ReplicaAvailable), Exists: true, DatabaseIDHex: target.DatabaseIDHex, MinimumCommitSequence: 1, MinimumGeneration: 2},
		{MemberID: "member-c", EndpointSHA256: strings.Repeat("c", 64), State: string(anchorhttp.ReplicaMissing)},
	}
	now := time.Now().UTC()
	receipt := anchorHistoryQualificationReceipt{
		SchemaVersion: anchorHistoryReceiptSchema, ProtocolVersion: anchorhttp.ProtocolVersion, RunID: controller.RunID,
		ControllerSHA256: qualificationSHA256(controllerRaw), ExternalEvidenceSHA256: strings.Repeat("d", 64),
		SourceRevision: qualificationTestRevision, BuildRevision: qualificationTestRevision,
		GOOS: "linux", GOARCH: "amd64", GoVersion: "go1.24", StartedAt: now, FinishedAt: now.Add(time.Second),
		ConfigurationID: strings.Repeat("e", 64), Replicas: 3, Quorum: 2, Members: members,
		Initial: controller.Initial, Operations: operations, Check: check, Passed: true,
		SigningPublicKey: base64PublicKey(publicKey),
	}
	if err := signAnchorHistoryQualificationReceipt(&receipt, privateKey); err != nil {
		t.Fatal(err)
	}
	if err := validateAnchorHistoryQualificationReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	receiptPath := filepath.Join(directory, "receipt.json")
	if err := writeJSONExclusiveDurable(receiptPath, receipt); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runAnchorQualification([]string{
		"history-verify", "--public-key", publicPath, "--receipt", receiptPath, "--history", controllerPath,
	}, &output, &output); err != nil || !strings.Contains(output.String(), `"passed":true`) {
		t.Fatalf("verify err=%v output=%s", err, output.String())
	}

	tampered := receipt
	tampered.ExternalEvidenceSHA256 = strings.Repeat("f", 64)
	tamperedPath := filepath.Join(directory, "receipt-tampered.json")
	if err := writeJSONExclusiveDurable(tamperedPath, tampered); err != nil {
		t.Fatal(err)
	}
	if err := runAnchorQualification([]string{
		"history-verify", "--public-key", publicPath, "--receipt", tamperedPath, "--history", controllerPath,
	}, &output, &output); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("tampered receipt error=%v", err)
	}

	changedController := controller
	changedController.Operations = append([]anchorHistoryWireOperation(nil), controller.Operations...)
	changedController.Operations[0].ID = "changed-operation"
	changedPath := filepath.Join(directory, "controller-changed.json")
	if err := writeJSONExclusiveDurable(changedPath, changedController); err != nil {
		t.Fatal(err)
	}
	if err := runAnchorQualification([]string{
		"history-verify", "--public-key", publicPath, "--receipt", receiptPath, "--history", changedPath,
	}, &output, &output); err == nil {
		t.Fatal("receipt accepted another controller history")
	}

	wrongPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongPath := filepath.Join(directory, "wrong.pub")
	if err := writeAnchorQualificationKey(wrongPath, wrongPublic, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runAnchorQualification([]string{
		"history-verify", "--public-key", wrongPath, "--receipt", receiptPath, "--history", controllerPath,
	}, &output, &output); err == nil {
		t.Fatal("history receipt verified with another key")
	}

	forgedCheck := receipt
	forgedCheck.Check.ExploredStates++
	if err := signAnchorHistoryQualificationReceipt(&forgedCheck, privateKey); err != nil {
		t.Fatal(err)
	}
	if err := verifyAnchorHistoryQualificationReceipt(forgedCheck, publicKey); err == nil || !strings.Contains(err.Error(), "checker") {
		t.Fatalf("re-signed forged checker result error=%v", err)
	}

	unsupported := receipt
	unsupported.Members = append([]anchorQualificationMember(nil), receipt.Members...)
	unsupported.Members[1] = anchorQualificationMember{
		MemberID: "member-b", EndpointSHA256: strings.Repeat("b", 64), State: string(anchorhttp.ReplicaAvailable), Exists: true,
		DatabaseIDHex: target.DatabaseIDHex, MinimumCommitSequence: 2, MinimumGeneration: 3,
	}
	unsupported.Members[2] = anchorQualificationMember{
		MemberID: "member-c", EndpointSHA256: strings.Repeat("c", 64), State: string(anchorhttp.ReplicaAvailable), Exists: true,
		DatabaseIDHex: target.DatabaseIDHex, MinimumCommitSequence: 2, MinimumGeneration: 3,
	}
	if err := signAnchorHistoryQualificationReceipt(&unsupported, privateKey); err != nil {
		t.Fatal(err)
	}
	if err := verifyAnchorHistoryQualificationReceipt(unsupported, publicKey); err == nil || !strings.Contains(err.Error(), "supported") {
		t.Fatalf("re-signed unsupported final quorum error=%v", err)
	}

	unknownControllerPath := filepath.Join(directory, "controller-unknown.json")
	unknownRaw := bytes.Replace(controllerRaw, []byte("\n}"), []byte(",\n  \"unknown\": true\n}"), 1)
	if bytes.Equal(unknownRaw, controllerRaw) {
		t.Fatal("unknown controller field setup did not change input")
	}
	if err := os.WriteFile(unknownControllerPath, unknownRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runAnchorQualification([]string{
		"history-verify", "--public-key", publicPath, "--receipt", receiptPath, "--history", unknownControllerPath,
	}, &output, &output); err == nil {
		t.Fatal("controller history with unknown field was accepted")
	}
}

func TestAnchorHistorySignRejectsNonLinearizableControllerBeforeRemoteAccess(t *testing.T) {
	directory := t.TempDir()
	target := anchorHistoryWireValue{Exists: true, DatabaseIDHex: strings.Repeat("01", 16), MinimumCommitSequence: 1, MinimumGeneration: 2}
	controller := anchorHistoryControllerRecord{
		SchemaVersion: anchorHistoryControllerSchema, RunID: strings.Repeat("ab", 16), ConfigurationID: strings.Repeat("e", 64),
		Operations: []anchorHistoryWireOperation{
			{ID: "advance", Kind: "advance", Outcome: "succeeded", Invoke: 1, Return: 2, Value: target},
			{ID: "stale-load", Kind: "load", Outcome: "succeeded", Invoke: 3, Return: 4, Value: anchorHistoryWireValue{}},
		},
	}
	attachAnchorHistoryTestAgentEvidence(t, &controller)
	controllerPath := filepath.Join(directory, "nonlinear.json")
	if err := writeJSONExclusiveDurable(controllerPath, controller); err != nil {
		t.Fatal(err)
	}
	err := runAnchorQualification([]string{
		"history-sign", "--history", controllerPath, "--out", filepath.Join(directory, "never.json"),
		"--signing-key", filepath.Join(directory, "missing.key"), "--source-revision", qualificationTestRevision, "--external-evidence-sha256", strings.Repeat("a", 64),
	}, ioDiscard{}, ioDiscard{})
	if err == nil || !strings.Contains(err.Error(), "not linearizable") {
		t.Fatalf("non-linear controller error=%v", err)
	}
}

func base64PublicKey(key ed25519.PublicKey) string {
	return base64.StdEncoding.EncodeToString(key)
}

func attachAnchorHistoryTestAgentEvidence(t *testing.T, controller *anchorHistoryControllerRecord) {
	t.Helper()
	_ = attachAnchorHistoryTestAgentEvidenceWithKeys(t, controller)
}

func attachAnchorHistoryTestAgentEvidenceWithKeys(t *testing.T, controller *anchorHistoryControllerRecord) []ed25519.PrivateKey {
	t.Helper()
	if controller.SourceRevision == "" {
		controller.SourceRevision = qualificationTestRevision
		controller.BuildRevision = qualificationTestRevision
		controller.GOOS, controller.GOARCH, controller.GoVersion = "linux", "amd64", "go1.25.0"
	}
	identities := make([]anchorHistoryAgentIdentity, 2)
	privateKeys := make([]ed25519.PrivateKey, 2)
	for index := range identities {
		publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		identities[index] = anchorHistoryAgentIdentity{
			AgentID: fmt.Sprintf("agent-%c", 'a'+index), EndpointSHA256: strings.Repeat(fmt.Sprintf("%x", index+7), 64), SigningPublicKey: base64PublicKey(publicKey),
		}
		privateKeys[index] = privateKey
	}
	controller.Agents = identities
	controller.Fragments = make([]anchorHistoryAgentFragment, len(controller.Operations))
	now := time.Now().UTC()
	for index, operation := range controller.Operations {
		identity := identities[index%len(identities)]
		target := anchorHistoryWireValue{}
		if operation.Kind == string(qualification.AnchorHistoryAdvance) {
			target = operation.Value
		}
		request := anchorHistoryAgentRequest{
			SchemaVersion: anchorHistoryAgentProtocolSchema, RunID: controller.RunID, ConfigurationID: controller.ConfigurationID,
			SourceRevision: controller.SourceRevision,
			AgentID:        identity.AgentID, OperationID: operation.ID, Kind: operation.Kind, Target: target,
			Nonce: strings.Repeat(fmt.Sprintf("%02x", index+1), 16), IssuedAt: now, ExpiresAt: now.Add(time.Minute),
		}
		requestRaw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		fragment := anchorHistoryAgentFragment{
			SchemaVersion: anchorHistoryAgentProtocolSchema, Request: request, RequestSHA256: qualificationSHA256(requestRaw),
			Outcome: operation.Outcome, Value: operation.Value, StartedAt: now, FinishedAt: now.Add(time.Millisecond),
			SourceRevision: controller.SourceRevision, BuildRevision: controller.BuildRevision, BuildModified: controller.BuildModified,
			GOOS: controller.GOOS, GOARCH: controller.GOARCH, GoVersion: controller.GoVersion,
			SigningPublicKey: identity.SigningPublicKey,
		}
		if err := signAnchorHistoryAgentFragment(&fragment, privateKeys[index%len(privateKeys)]); err != nil {
			t.Fatal(err)
		}
		controller.Fragments[index] = fragment
	}
	return privateKeys
}
