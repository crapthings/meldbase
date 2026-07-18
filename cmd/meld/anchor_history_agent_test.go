package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
)

type anchorHistoryStubExecutor struct {
	mu            sync.Mutex
	configuration string
	anchor        meldbase.RollbackAnchor
	exists        bool
	loads         int
	advances      int
}

type anchorHistoryDropFirstResponseTransport struct {
	base    http.RoundTripper
	mu      sync.Mutex
	dropped bool
}

func (transport *anchorHistoryDropFirstResponseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	transport.mu.Lock()
	drop := !transport.dropped && err == nil
	if drop {
		transport.dropped = true
	}
	transport.mu.Unlock()
	if drop {
		_ = response.Body.Close()
		return nil, errors.New("injected response loss after agent execution")
	}
	return response, err
}

func (stub *anchorHistoryStubExecutor) ConfigurationID() string { return stub.configuration }
func (stub *anchorHistoryStubExecutor) Load(context.Context) (meldbase.RollbackAnchor, bool, error) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.loads++
	return stub.anchor, stub.exists, nil
}
func (stub *anchorHistoryStubExecutor) Advance(_ context.Context, anchor meldbase.RollbackAnchor) error {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.advances++
	stub.anchor, stub.exists = anchor, true
	return nil
}

func TestHistoryAgentRealMTLSSignsAndDeduplicatesExecution(t *testing.T) {
	pki := newAnchorTestPKI(t)
	controllerCertificate, err := x509.ParseCertificate(pki.client.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	configuration := strings.Repeat("a", 64)
	executor := &anchorHistoryStubExecutor{configuration: configuration}
	handler := &anchorHistoryAgentHandler{
		agentID: "agent-a", controllerSPKISHA256: anchorHistoryCertificateSPKI(controllerCertificate), signingKey: privateKey,
		sourceRevision: qualificationTestRevision, buildRevision: qualificationTestRevision, goos: "linux", goarch: "amd64", goVersion: "go1.25.0",
		executor: executor, operationTimeout: time.Second, results: make(map[string]anchorHistoryAgentFragment), requestDigests: make(map[string]string),
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pki.server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.rootPool,
	}
	server.StartTLS()
	defer server.Close()
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pki.rootPool, Certificates: []tls.Certificate{pki.client}}}
	dropper := &anchorHistoryDropFirstResponseTransport{base: transport}
	client := &http.Client{Transport: dropper}
	defer transport.CloseIdleConnections()
	now := time.Now().UTC()
	request := anchorHistoryAgentRequest{
		SchemaVersion: anchorHistoryAgentProtocolSchema, RunID: strings.Repeat("b", 32), ConfigurationID: configuration,
		SourceRevision: qualificationTestRevision,
		AgentID:        "agent-a", OperationID: "load-once", Kind: "load", Nonce: strings.Repeat("c", 32), IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	encodedKey := base64.StdEncoding.EncodeToString(publicKey)
	first, err := executeAnchorHistoryAgent(client, server.URL, request, encodedKey, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err := executeAnchorHistoryAgent(client, server.URL, request, encodedKey, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !dropper.dropped || first.Signature != second.Signature || first.RequestSHA256 != second.RequestSHA256 || executor.loads != 1 {
		t.Fatalf("dedup first=%+v second=%+v loads=%d", first, second, executor.loads)
	}
	changed := request
	changed.Nonce = strings.Repeat("d", 32)
	if _, err := executeAnchorHistoryAgent(client, server.URL, changed, encodedKey, time.Second); err == nil || !strings.Contains(err.Error(), "different content") {
		t.Fatalf("changed replay error=%v", err)
	}
	wrongPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executeAnchorHistoryAgent(client, server.URL, request, base64.StdEncoding.EncodeToString(wrongPublic), time.Second); err == nil || !strings.Contains(err.Error(), "identity-mismatched") {
		t.Fatalf("wrong agent key error=%v", err)
	}
	handler.controllerSPKISHA256 = strings.Repeat("f", 64)
	pinnedOut := request
	pinnedOut.OperationID = "wrong-controller-leaf"
	pinnedOut.Nonce = strings.Repeat("e", 32)
	if _, err := executeAnchorHistoryAgent(client, server.URL, pinnedOut, encodedKey, time.Second); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("CA-valid but unpinned controller certificate error=%v", err)
	}
	if executor.loads != 1 {
		t.Fatalf("unpinned controller executed operation; loads=%d", executor.loads)
	}
}

func TestHistoryAgentAdvanceSurvivesCanceledControllerContext(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	configuration := strings.Repeat("1", 64)
	executor := &anchorHistoryStubExecutor{configuration: configuration}
	handler := &anchorHistoryAgentHandler{
		agentID: "agent-b", controllerSPKISHA256: strings.Repeat("2", 64), signingKey: privateKey,
		sourceRevision: qualificationTestRevision, buildRevision: qualificationTestRevision, goos: "linux", goarch: "amd64", goVersion: "go1.25.0",
		executor: executor, operationTimeout: time.Second, results: make(map[string]anchorHistoryAgentFragment), requestDigests: make(map[string]string),
	}
	now := time.Now().UTC()
	target := anchorHistoryWireValue{Exists: true, DatabaseIDHex: strings.Repeat("03", 16), MinimumCommitSequence: 4, MinimumGeneration: 5}
	request := anchorHistoryAgentRequest{
		SchemaVersion: anchorHistoryAgentProtocolSchema, RunID: strings.Repeat("4", 32), ConfigurationID: configuration,
		SourceRevision: qualificationTestRevision,
		AgentID:        "agent-b", OperationID: "advance-after-disconnect", Kind: "advance", Target: target,
		Nonce: strings.Repeat("5", 32), IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	fragment, err := handler.execute(canceled, request, now)
	if err != nil {
		t.Fatal(err)
	}
	identity := anchorHistoryAgentIdentity{AgentID: "agent-b", EndpointSHA256: strings.Repeat("6", 64), SigningPublicKey: base64.StdEncoding.EncodeToString(publicKey)}
	if fragment.Outcome != "succeeded" || fragment.Value != target || executor.advances != 1 {
		t.Fatalf("fragment=%+v advances=%d", fragment, executor.advances)
	}
	if err := verifyAnchorHistoryAgentFragment(fragment, identity); err != nil {
		t.Fatal(err)
	}

	expired := request
	expired.OperationID = "expired"
	expired.Nonce = strings.Repeat("7", 32)
	expired.IssuedAt = now.Add(-2 * time.Minute)
	expired.ExpiresAt = now.Add(-time.Minute)
	if _, err := handler.execute(context.Background(), expired, now); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired request error=%v", err)
	}
}

func TestHistoryAgentDurableJournalSurvivesRestartAndRejectsTamper(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	configuration := strings.Repeat("8", 64)
	executor := &anchorHistoryStubExecutor{configuration: configuration}
	newHandler := func() *anchorHistoryAgentHandler {
		return &anchorHistoryAgentHandler{
			agentID: "durable-agent", stateDirectory: directory, controllerSPKISHA256: strings.Repeat("9", 64), signingKey: privateKey,
			sourceRevision: qualificationTestRevision, buildRevision: qualificationTestRevision, goos: "linux", goarch: "amd64", goVersion: "go1.25.0",
			executor: executor, operationTimeout: time.Second, results: make(map[string]anchorHistoryAgentFragment),
			requestDigests: make(map[string]string), inFlight: make(map[string]chan struct{}),
		}
	}
	now := time.Now().UTC()
	request := anchorHistoryAgentRequest{
		SchemaVersion: anchorHistoryAgentProtocolSchema, RunID: strings.Repeat("a", 32), ConfigurationID: configuration,
		SourceRevision: qualificationTestRevision,
		AgentID:        "durable-agent", OperationID: "restart-load", Kind: "load", Nonce: strings.Repeat("b", 32),
		IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	firstHandler := newHandler()
	first, err := firstHandler.execute(context.Background(), request, now)
	if err != nil {
		t.Fatal(err)
	}
	secondHandler := newHandler()
	if err := secondHandler.loadDurableState(); err != nil {
		t.Fatal(err)
	}
	second, err := secondHandler.execute(context.Background(), request, now)
	if err != nil {
		t.Fatal(err)
	}
	if first.Signature != second.Signature || executor.loads != 1 {
		t.Fatalf("journal replay executed twice: first=%+v second=%+v loads=%d", first, second, executor.loads)
	}
	identity := anchorHistoryAgentIdentity{AgentID: "durable-agent", SigningPublicKey: base64.StdEncoding.EncodeToString(publicKey)}
	if err := verifyAnchorHistoryAgentFragment(second, identity); err != nil {
		t.Fatal(err)
	}

	resultPath := directory + "/" + anchorHistoryAgentJournalFilename("result", request.OperationID)
	raw, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(string(raw), `"outcome": "succeeded"`, `"outcome": "failed"`, 1)
	if tampered == string(raw) {
		t.Fatal("journal tamper setup did not change result")
	}
	if err := os.WriteFile(resultPath, []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := newHandler().loadDurableState(); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("tampered durable journal error=%v", err)
	}
}

func TestHistoryAgentResumesDurablyReservedOperationAfterCrash(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	configuration := strings.Repeat("c", 64)
	executor := &anchorHistoryStubExecutor{configuration: configuration}
	now := time.Now().UTC()
	request := anchorHistoryAgentRequest{
		SchemaVersion: anchorHistoryAgentProtocolSchema, RunID: strings.Repeat("d", 32), ConfigurationID: configuration,
		SourceRevision: qualificationTestRevision,
		AgentID:        "crash-agent", OperationID: "reserved-before-crash", Kind: "load", Nonce: strings.Repeat("e", 32),
		IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	beforeCrash := &anchorHistoryAgentHandler{
		agentID: "crash-agent", stateDirectory: directory, sourceRevision: qualificationTestRevision, buildRevision: qualificationTestRevision,
		goos: "linux", goarch: "amd64", goVersion: "go1.25.0", signingKey: privateKey, executor: executor,
	}
	if err := beforeCrash.persistRunIdentity(request.RunID); err != nil {
		t.Fatal(err)
	}
	if err := beforeCrash.persistRequest(request); err != nil {
		t.Fatal(err)
	}
	afterRestart := &anchorHistoryAgentHandler{
		agentID: "crash-agent", stateDirectory: directory, signingKey: privateKey, executor: executor, operationTimeout: time.Second,
		sourceRevision: qualificationTestRevision, buildRevision: qualificationTestRevision, goos: "linux", goarch: "amd64", goVersion: "go1.25.0",
		results: make(map[string]anchorHistoryAgentFragment), requestDigests: make(map[string]string), inFlight: make(map[string]chan struct{}),
	}
	if err := afterRestart.loadDurableState(); err != nil {
		t.Fatal(err)
	}
	if _, err := afterRestart.execute(context.Background(), request, now); err != nil {
		t.Fatal(err)
	}
	if executor.loads != 1 {
		t.Fatalf("reserved operation executions=%d", executor.loads)
	}
	changed := request
	changed.Nonce = strings.Repeat("f", 32)
	if _, err := afterRestart.execute(context.Background(), changed, now); err == nil || !strings.Contains(err.Error(), "different content") {
		t.Fatalf("changed reserved operation error=%v", err)
	}
}
