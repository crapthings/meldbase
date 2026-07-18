package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/internal/qualification"
)

const (
	anchorHistoryAgentProtocolSchema uint32 = 2
	anchorHistoryRunPlanSchema       uint32 = 2
	anchorHistoryAgentBodyMaximum           = 64 << 10
	anchorHistoryAgentMaximumLease          = 2 * time.Minute
)

type anchorHistoryAgentIdentity struct {
	AgentID          string `json:"agentId"`
	EndpointSHA256   string `json:"endpointSha256"`
	SigningPublicKey string `json:"signingPublicKey"`
}

type anchorHistoryAgentRequest struct {
	SchemaVersion   uint32                 `json:"schemaVersion"`
	RunID           string                 `json:"runId"`
	ConfigurationID string                 `json:"configurationId"`
	SourceRevision  string                 `json:"sourceRevision"`
	AgentID         string                 `json:"agentId"`
	OperationID     string                 `json:"operationId"`
	Kind            string                 `json:"kind"`
	Target          anchorHistoryWireValue `json:"target"`
	Nonce           string                 `json:"nonce"`
	IssuedAt        time.Time              `json:"issuedAt"`
	ExpiresAt       time.Time              `json:"expiresAt"`
}

type anchorHistoryAgentFragment struct {
	SchemaVersion    uint32                    `json:"schemaVersion"`
	Request          anchorHistoryAgentRequest `json:"request"`
	RequestSHA256    string                    `json:"requestSha256"`
	Outcome          string                    `json:"outcome"`
	Value            anchorHistoryWireValue    `json:"value"`
	StartedAt        time.Time                 `json:"startedAt"`
	FinishedAt       time.Time                 `json:"finishedAt"`
	SourceRevision   string                    `json:"sourceRevision"`
	BuildRevision    string                    `json:"buildRevision,omitempty"`
	BuildModified    bool                      `json:"buildModified"`
	GOOS             string                    `json:"goos"`
	GOARCH           string                    `json:"goarch"`
	GoVersion        string                    `json:"goVersion"`
	SigningPublicKey string                    `json:"signingPublicKey"`
	Signature        string                    `json:"signature"`
}

type anchorHistoryRunPlanOperation struct {
	ID      string                 `json:"id"`
	AgentID string                 `json:"agentId"`
	Kind    string                 `json:"kind"`
	Target  anchorHistoryWireValue `json:"target"`
}

type anchorHistoryRunPlanWave struct {
	Operations []anchorHistoryRunPlanOperation `json:"operations"`
}

type anchorHistoryRunPlan struct {
	SchemaVersion  uint32                      `json:"schemaVersion"`
	RunID          string                      `json:"runId"`
	SourceRevision string                      `json:"sourceRevision"`
	Agents         []anchorHistoryRunPlanAgent `json:"agents"`
	Waves          []anchorHistoryRunPlanWave  `json:"waves"`
}

type anchorHistoryRunPlanAgent struct {
	AgentID          string `json:"agentId"`
	Endpoint         string `json:"endpoint"`
	SigningPublicKey string `json:"signingPublicKey"`
}

type anchorHistoryAgentExecutor interface {
	ConfigurationID() string
	Load(context.Context) (meldbase.RollbackAnchor, bool, error)
	Advance(context.Context, meldbase.RollbackAnchor) error
}

type anchorHistoryAgentOptions struct {
	address              string
	agentID              string
	stateDirectory       string
	sourceRevision       string
	buildRevision        string
	buildModified        bool
	goos                 string
	goarch               string
	goVersion            string
	controllerSPKISHA256 string
	certificate          tls.Certificate
	controllerCAs        *x509.CertPool
	signingKey           ed25519.PrivateKey
	executor             anchorHistoryAgentExecutor
	operationTimeout     time.Duration
	shutdownTimeout      time.Duration
	ready                chan<- net.Addr
}

type anchorHistoryAgentHandler struct {
	agentID              string
	stateDirectory       string
	sourceRevision       string
	buildRevision        string
	buildModified        bool
	goos                 string
	goarch               string
	goVersion            string
	controllerSPKISHA256 string
	signingKey           ed25519.PrivateKey
	executor             anchorHistoryAgentExecutor
	operationTimeout     time.Duration
	mu                   sync.Mutex
	runID                string
	results              map[string]anchorHistoryAgentFragment
	requestDigests       map[string]string
	inFlight             map[string]chan struct{}
}

type anchorHistoryAgentJournalIdentity struct {
	SchemaVersion    uint32 `json:"schemaVersion"`
	RunID            string `json:"runId"`
	AgentID          string `json:"agentId"`
	ConfigurationID  string `json:"configurationId"`
	SourceRevision   string `json:"sourceRevision"`
	BuildRevision    string `json:"buildRevision,omitempty"`
	BuildModified    bool   `json:"buildModified"`
	SigningPublicKey string `json:"signingPublicKey"`
}

func runAnchorHistoryAgent(args []string, stdout, stderr io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runAnchorHistoryAgentContext(ctx, args, stdout, stderr)
}

func runAnchorHistoryAgentContext(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("anchor-qualification history-agent", flag.ContinueOnError)
	flags.SetOutput(stderr)
	address := flags.String("addr", ":9443", "TLS listen address")
	agentID := flags.String("agent-id", "", "stable qualification agent ID")
	stateDirectory := flags.String("state-dir", "", "private durable single-run agent journal directory")
	sourceRevision := flags.String("source-revision", "", "40- or 64-hex release revision executed by this agent")
	requireClean := flags.Bool("require-clean-source", false, "require this agent binary to be a clean build of source revision")
	tlsCert := flags.String("tls-cert", "", "agent TLS server certificate")
	tlsKey := flags.String("tls-key", "", "private agent TLS server key")
	controllerCA := flags.String("controller-ca", "", "CA used to authenticate the controller certificate")
	controllerSPKI := flags.String("controller-spki-sha256", "", "pinned controller certificate SPKI SHA-256")
	signingKey := flags.String("signing-key", "", "private Ed25519 fragment signing key")
	operationTimeout := flags.Duration("timeout", 10*time.Second, "maximum anchor operation duration")
	shutdownTimeout := flags.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown deadline")
	cluster := flags.String("cluster", "", "static anchor cluster ID")
	anchorName := flags.String("anchor-name", "", "disposable qualification anchor resource name")
	keyID := flags.String("key-id", "", "anchor HMAC key ID")
	keyFile := flags.String("key-file", "", "private base64 anchor HMAC key file")
	serverCA := flags.String("anchor-ca", "", "anchor server CA PEM")
	clientCert := flags.String("anchor-client-cert", "", "anchor mTLS client certificate PEM")
	clientKey := flags.String("anchor-client-key", "", "private anchor mTLS client key PEM")
	var replicas anchorReplicaFlags
	flags.Var(&replicas, "replica", "repeatable member-id=https://endpoint")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if ctx == nil || *agentID == "" || *stateDirectory == "" || !validDurabilitySourceRevision(*sourceRevision) || *tlsCert == "" || *tlsKey == "" || *controllerCA == "" || !qualificationHexDigest(*controllerSPKI) ||
		*signingKey == "" || *operationTimeout <= 0 || *operationTimeout > time.Minute || *shutdownTimeout <= 0 || *shutdownTimeout > time.Minute || flags.NArg() != 0 {
		return errors.New("history-agent requires agent identity, private state directory, TLS/mTLS files, pinned controller SPKI, signing key and valid timeouts")
	}
	if !qualificationSafeName(*agentID, 64) {
		return errors.New("history-agent has an invalid agent ID")
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if *requireClean && (buildRevision != *sourceRevision || buildModified) {
		return errors.New("history-agent clean source verification failed")
	}
	certificate, err := loadAnchorCertificate(*tlsCert, *tlsKey)
	if err != nil {
		return err
	}
	controllerCAs, err := loadAnchorClientCAs(*controllerCA)
	if err != nil {
		return err
	}
	privateKey, err := loadAnchorQualificationPrivateKey(*signingKey)
	if err != nil {
		return err
	}
	store, transport, err := newRemoteAnchorStore(remoteAnchorConfig{
		clusterID: *cluster, replicaSpecs: replicas, anchorName: *anchorName, keyID: *keyID, keyFile: *keyFile,
		serverCAFile: *serverCA, clientCertFile: *clientCert, clientKeyFile: *clientKey, operationTimeout: *operationTimeout,
	})
	if err != nil {
		return err
	}
	defer transport.CloseIdleConnections()
	return serveAnchorHistoryAgent(ctx, anchorHistoryAgentOptions{
		address: *address, agentID: *agentID, stateDirectory: *stateDirectory, sourceRevision: *sourceRevision,
		buildRevision: buildRevision, buildModified: buildModified, goos: runtime.GOOS, goarch: runtime.GOARCH, goVersion: runtime.Version(),
		controllerSPKISHA256: *controllerSPKI, certificate: certificate,
		controllerCAs: controllerCAs, signingKey: privateKey, executor: store, operationTimeout: *operationTimeout,
		shutdownTimeout: *shutdownTimeout,
	}, stdout)
}

func serveAnchorHistoryAgent(ctx context.Context, options anchorHistoryAgentOptions, stdout io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !qualificationSafeName(options.agentID, 64) || !validDurabilitySourceRevision(options.sourceRevision) || !qualificationHexDigest(options.controllerSPKISHA256) || len(options.signingKey) != ed25519.PrivateKeySize ||
		options.goos == "" || options.goarch == "" || options.goVersion == "" || options.executor == nil || options.controllerCAs == nil || len(options.certificate.Certificate) == 0 || options.operationTimeout <= 0 || options.shutdownTimeout <= 0 {
		return errors.New("invalid history-agent service configuration")
	}
	if options.stateDirectory == "" {
		return errors.New("history-agent requires an existing private durable state directory")
	}
	if err := validateAnchorDirectory(options.stateDirectory); err != nil {
		return fmt.Errorf("history-agent state directory: %w", err)
	}
	handler := &anchorHistoryAgentHandler{
		agentID: options.agentID, controllerSPKISHA256: options.controllerSPKISHA256, signingKey: options.signingKey,
		stateDirectory: options.stateDirectory, sourceRevision: options.sourceRevision, buildRevision: options.buildRevision, buildModified: options.buildModified,
		goos: options.goos, goarch: options.goarch, goVersion: options.goVersion, executor: options.executor, operationTimeout: options.operationTimeout,
		results: make(map[string]anchorHistoryAgentFragment), requestDigests: make(map[string]string),
		inFlight: make(map[string]chan struct{}),
	}
	if err := handler.loadDurableState(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", options.address)
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: options.operationTimeout + 5*time.Second,
		WriteTimeout: options.operationTimeout + 5*time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{options.certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: options.controllerCAs, NextProtos: []string{"h2", "http/1.1"}},
	}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	if options.ready != nil {
		options.ready <- listener.Addr()
	}
	fmt.Fprintf(stdout, "Meldbase history qualification agent %s listening on https://%s\n", options.agentID, listener.Addr())
	select {
	case serveErr := <-done:
		if errors.Is(serveErr, http.ErrServerClosed) {
			return nil
		}
		return serveErr
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), options.shutdownTimeout)
		shutdownErr := server.Shutdown(shutdown)
		cancel()
		serveErr := <-done
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(shutdownErr, serveErr)
	}
}

func (handler *anchorHistoryAgentHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	if request.Method != http.MethodPost || request.URL.Path != "/v1/execute" || request.URL.RawQuery != "" {
		http.Error(response, "not found", http.StatusNotFound)
		return
	}
	if request.TLS == nil || len(request.TLS.PeerCertificates) < 1 || anchorHistoryCertificateSPKI(request.TLS.PeerCertificates[0]) != handler.controllerSPKISHA256 {
		http.Error(response, "controller identity rejected", http.StatusForbidden)
		return
	}
	if request.Header.Get("Content-Type") != "application/json" || request.ContentLength <= 0 || request.ContentLength > anchorHistoryAgentBodyMaximum {
		http.Error(response, "invalid request body", http.StatusBadRequest)
		return
	}
	var command anchorHistoryAgentRequest
	decoder := json.NewDecoder(io.LimitReader(request.Body, anchorHistoryAgentBodyMaximum+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil {
		http.Error(response, "invalid request JSON", http.StatusBadRequest)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		http.Error(response, "invalid trailing JSON", http.StatusBadRequest)
		return
	}
	fragment, err := handler.execute(request.Context(), command, time.Now().UTC())
	if err != nil {
		http.Error(response, err.Error(), http.StatusConflict)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(fragment)
}

func (handler *anchorHistoryAgentHandler) execute(parent context.Context, request anchorHistoryAgentRequest, now time.Time) (anchorHistoryAgentFragment, error) {
	requestDigest, err := validateAnchorHistoryAgentRequest(request, handler.agentID, handler.executor.ConfigurationID(), handler.sourceRevision, now)
	if err != nil {
		return anchorHistoryAgentFragment{}, err
	}
	handler.mu.Lock()
	if handler.runID != "" && handler.runID != request.RunID {
		handler.mu.Unlock()
		return anchorHistoryAgentFragment{}, errors.New("agent is already bound to another run")
	}
	if previous, exists := handler.results[request.OperationID]; exists {
		if handler.requestDigests[request.OperationID] != requestDigest {
			handler.mu.Unlock()
			return anchorHistoryAgentFragment{}, errors.New("operation ID replayed with different content")
		}
		handler.mu.Unlock()
		return previous, nil
	}
	resumeReserved := false
	if previousDigest, exists := handler.requestDigests[request.OperationID]; exists {
		wait := handler.inFlight[request.OperationID]
		if previousDigest != requestDigest {
			handler.mu.Unlock()
			return anchorHistoryAgentFragment{}, errors.New("operation ID replayed with different content")
		}
		if wait == nil {
			if handler.inFlight == nil {
				handler.inFlight = make(map[string]chan struct{})
			}
			handler.inFlight[request.OperationID] = make(chan struct{})
			resumeReserved = true
		} else {
			handler.mu.Unlock()
			select {
			case <-wait:
				handler.mu.Lock()
				previous, exists := handler.results[request.OperationID]
				handler.mu.Unlock()
				if !exists {
					return anchorHistoryAgentFragment{}, errors.New("operation result could not be signed")
				}
				return previous, nil
			case <-parent.Done():
				return anchorHistoryAgentFragment{}, parent.Err()
			}
		}
	}
	if !resumeReserved && len(handler.requestDigests) >= qualification.MaxAnchorHistoryOperations-1 {
		handler.mu.Unlock()
		return anchorHistoryAgentFragment{}, errors.New("agent operation limit reached")
	}
	if !resumeReserved && handler.runID == "" {
		if err := handler.persistRunIdentity(request.RunID); err != nil {
			handler.mu.Unlock()
			return anchorHistoryAgentFragment{}, err
		}
		handler.runID = request.RunID
	}
	// Reserve the ID before executing so two concurrent duplicates cannot run twice.
	if handler.inFlight == nil {
		handler.inFlight = make(map[string]chan struct{})
	}
	if !resumeReserved {
		if err := handler.persistRequest(request); err != nil {
			handler.mu.Unlock()
			return anchorHistoryAgentFragment{}, err
		}
		handler.requestDigests[request.OperationID] = requestDigest
		handler.inFlight[request.OperationID] = make(chan struct{})
	}
	handler.mu.Unlock()

	started := time.Now().UTC()
	deadline := request.ExpiresAt
	if maximum := started.Add(handler.operationTimeout); deadline.After(maximum) {
		deadline = maximum
	}
	// The operation must finish and be cached even if the controller connection
	// disappears after dispatch; a byte-stream lifetime is not a transaction.
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	var value anchorHistoryWireValue
	var operationErr error
	switch request.Kind {
	case string(qualification.AnchorHistoryLoad):
		anchor, exists, loadErr := handler.executor.Load(ctx)
		operationErr = loadErr
		if loadErr == nil {
			value = anchorHistoryWireValueFromAnchor(anchor, exists)
		}
	case string(qualification.AnchorHistoryAdvance):
		anchor, conversionErr := anchorHistoryAnchorFromWire(request.Target)
		if conversionErr != nil {
			operationErr = conversionErr
		} else {
			operationErr = handler.executor.Advance(ctx, anchor)
		}
	}
	cancel()
	fragment := anchorHistoryAgentFragment{
		SchemaVersion: anchorHistoryAgentProtocolSchema, Request: request, RequestSHA256: requestDigest,
		Outcome: string(qualification.AnchorHistorySucceeded), Value: value, StartedAt: started, FinishedAt: time.Now().UTC(),
		SourceRevision: handler.sourceRevision, BuildRevision: handler.buildRevision, BuildModified: handler.buildModified,
		GOOS: handler.goos, GOARCH: handler.goarch, GoVersion: handler.goVersion,
		SigningPublicKey: base64.StdEncoding.EncodeToString(handler.signingKey.Public().(ed25519.PublicKey)),
	}
	if request.Kind == string(qualification.AnchorHistoryAdvance) {
		fragment.Value = request.Target
	}
	if operationErr != nil {
		fragment.Outcome = string(qualification.AnchorHistoryFailed)
	}
	if err := signAnchorHistoryAgentFragment(&fragment, handler.signingKey); err != nil {
		handler.mu.Lock()
		close(handler.inFlight[request.OperationID])
		delete(handler.inFlight, request.OperationID)
		handler.mu.Unlock()
		return anchorHistoryAgentFragment{}, err
	}
	if err := handler.persistResult(fragment); err != nil {
		handler.mu.Lock()
		close(handler.inFlight[request.OperationID])
		delete(handler.inFlight, request.OperationID)
		handler.mu.Unlock()
		return anchorHistoryAgentFragment{}, err
	}
	handler.mu.Lock()
	handler.results[request.OperationID] = fragment
	close(handler.inFlight[request.OperationID])
	delete(handler.inFlight, request.OperationID)
	handler.mu.Unlock()
	return fragment, nil
}

func (handler *anchorHistoryAgentHandler) persistRunIdentity(runID string) error {
	if handler.stateDirectory == "" {
		return nil
	}
	identity := anchorHistoryAgentJournalIdentity{
		SchemaVersion: anchorHistoryAgentProtocolSchema, RunID: runID, AgentID: handler.agentID,
		ConfigurationID: handler.executor.ConfigurationID(), SourceRevision: handler.sourceRevision, BuildRevision: handler.buildRevision, BuildModified: handler.buildModified,
		SigningPublicKey: base64.StdEncoding.EncodeToString(handler.signingKey.Public().(ed25519.PublicKey)),
	}
	if err := writeJSONExclusiveDurable(filepath.Join(handler.stateDirectory, "identity.json"), identity); err != nil {
		return fmt.Errorf("persist history-agent run identity: %w", err)
	}
	return nil
}

func (handler *anchorHistoryAgentHandler) persistRequest(request anchorHistoryAgentRequest) error {
	if handler.stateDirectory == "" {
		return nil
	}
	path := filepath.Join(handler.stateDirectory, anchorHistoryAgentJournalFilename("request", request.OperationID))
	if err := writeJSONExclusiveDurable(path, request); err != nil {
		return fmt.Errorf("persist history-agent request reservation: %w", err)
	}
	return nil
}

func (handler *anchorHistoryAgentHandler) persistResult(fragment anchorHistoryAgentFragment) error {
	if handler.stateDirectory == "" {
		return nil
	}
	path := filepath.Join(handler.stateDirectory, anchorHistoryAgentJournalFilename("result", fragment.Request.OperationID))
	if err := writeJSONExclusiveDurable(path, fragment); err != nil {
		return fmt.Errorf("persist history-agent signed result: %w", err)
	}
	return nil
}

func (handler *anchorHistoryAgentHandler) loadDurableState() error {
	entries, err := os.ReadDir(handler.stateDirectory)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	var identity anchorHistoryAgentJournalIdentity
	if _, err := readQualificationReceipt(filepath.Join(handler.stateDirectory, "identity.json"), &identity); err != nil {
		return fmt.Errorf("history-agent journal identity: %w", err)
	}
	wantPublic := base64.StdEncoding.EncodeToString(handler.signingKey.Public().(ed25519.PublicKey))
	if identity.SchemaVersion != anchorHistoryAgentProtocolSchema || !anchorQualificationHex(identity.RunID, 16) || identity.AgentID != handler.agentID ||
		identity.ConfigurationID != handler.executor.ConfigurationID() || identity.SourceRevision != handler.sourceRevision || identity.BuildRevision != handler.buildRevision ||
		identity.BuildModified != handler.buildModified || identity.SigningPublicKey != wantPublic {
		return errors.New("history-agent journal identity differs from runtime configuration")
	}
	handler.runID = identity.RunID
	requests := make(map[string]anchorHistoryAgentRequest)
	for _, entry := range entries {
		name := entry.Name()
		if name == "identity.json" || strings.HasPrefix(name, "result-") {
			continue
		}
		if !strings.HasPrefix(name, "request-") || !strings.HasSuffix(name, ".json") {
			return fmt.Errorf("history-agent journal contains unexpected entry %q", name)
		}
		info, infoErr := entry.Info()
		if infoErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("history-agent journal request is not a regular file")
		}
		var request anchorHistoryAgentRequest
		if _, err := readQualificationReceipt(filepath.Join(handler.stateDirectory, name), &request); err != nil {
			return err
		}
		digest, err := validateAnchorHistoryAgentRequestStatic(request, handler.agentID, handler.executor.ConfigurationID(), handler.sourceRevision)
		if err != nil || request.RunID != handler.runID || name != anchorHistoryAgentJournalFilename("request", request.OperationID) {
			return errors.Join(err, errors.New("history-agent journal request is invalid or misnamed"))
		}
		if _, duplicate := requests[request.OperationID]; duplicate {
			return errors.New("history-agent journal duplicates a request")
		}
		requests[request.OperationID] = request
		handler.requestDigests[request.OperationID] = digest
	}
	identityEvidence := anchorHistoryAgentIdentity{AgentID: handler.agentID, SigningPublicKey: wantPublic}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "result-") {
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			return fmt.Errorf("history-agent journal contains unexpected entry %q", name)
		}
		info, infoErr := entry.Info()
		if infoErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("history-agent journal result is not a regular file")
		}
		var fragment anchorHistoryAgentFragment
		if _, err := readQualificationReceipt(filepath.Join(handler.stateDirectory, name), &fragment); err != nil {
			return err
		}
		reserved, exists := requests[fragment.Request.OperationID]
		if !exists || fragment.Request != reserved || name != anchorHistoryAgentJournalFilename("result", fragment.Request.OperationID) {
			return errors.New("history-agent journal result lacks its exact request reservation")
		}
		if err := validateAnchorHistoryAgentFragmentEvidence(fragment, identityEvidence, handler.runID, handler.executor.ConfigurationID(), handler.sourceRevision); err != nil {
			return err
		}
		handler.results[fragment.Request.OperationID] = fragment
	}
	if len(handler.requestDigests) > qualification.MaxAnchorHistoryOperations-1 {
		return errors.New("history-agent journal exceeds the operation limit")
	}
	return nil
}

func anchorHistoryAgentJournalFilename(kind, operationID string) string {
	return kind + "-" + qualificationSHA256([]byte(operationID)) + ".json"
}

func validateAnchorHistoryAgentRequest(request anchorHistoryAgentRequest, agentID, configurationID, sourceRevision string, now time.Time) (string, error) {
	digest, err := validateAnchorHistoryAgentRequestStatic(request, agentID, configurationID, sourceRevision)
	if err != nil {
		return "", err
	}
	if now.Before(request.IssuedAt.Add(-30*time.Second)) || now.After(request.ExpiresAt) {
		return "", errors.New("expired or not-yet-valid history-agent request")
	}
	return digest, nil
}

func validateAnchorHistoryAgentRequestStatic(request anchorHistoryAgentRequest, agentID, configurationID, sourceRevision string) (string, error) {
	if request.SchemaVersion != anchorHistoryAgentProtocolSchema || request.AgentID != agentID || request.ConfigurationID != configurationID || request.SourceRevision != sourceRevision ||
		!anchorQualificationHex(request.RunID, 16) || !qualificationHexDigest(request.ConfigurationID) ||
		!validDurabilitySourceRevision(request.SourceRevision) ||
		!qualificationSafeName(request.OperationID, 128) || !anchorQualificationHex(request.Nonce, 16) || request.IssuedAt.IsZero() || request.ExpiresAt.IsZero() ||
		!request.ExpiresAt.After(request.IssuedAt) || request.ExpiresAt.Sub(request.IssuedAt) > anchorHistoryAgentMaximumLease {
		return "", errors.New("invalid or misbound history-agent request")
	}
	switch request.Kind {
	case string(qualification.AnchorHistoryLoad):
		if request.Target != (anchorHistoryWireValue{}) {
			return "", errors.New("history-agent load request contains a target")
		}
	case string(qualification.AnchorHistoryAdvance):
		if _, err := anchorHistoryAnchorFromWire(request.Target); err != nil {
			return "", err
		}
	default:
		return "", errors.New("history-agent request has an invalid operation kind")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	return qualificationSHA256(payload), nil
}

func signAnchorHistoryAgentFragment(fragment *anchorHistoryAgentFragment, privateKey ed25519.PrivateKey) error {
	fragment.Signature = ""
	payload, err := json.Marshal(fragment)
	if err != nil {
		return err
	}
	fragment.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return nil
}

func verifyAnchorHistoryAgentFragment(fragment anchorHistoryAgentFragment, identity anchorHistoryAgentIdentity) error {
	if fragment.SchemaVersion != anchorHistoryAgentProtocolSchema || fragment.Request.AgentID != identity.AgentID || fragment.SigningPublicKey != identity.SigningPublicKey ||
		fragment.Outcome != string(qualification.AnchorHistorySucceeded) && fragment.Outcome != string(qualification.AnchorHistoryFailed) ||
		fragment.StartedAt.IsZero() || fragment.FinishedAt.Before(fragment.StartedAt) || !validDurabilitySourceRevision(fragment.SourceRevision) ||
		(fragment.BuildRevision != "" && !validDurabilitySourceRevision(fragment.BuildRevision)) || fragment.GOOS == "" || fragment.GOARCH == "" || fragment.GoVersion == "" {
		return errors.New("history-agent fragment is incomplete or identity-mismatched")
	}
	requestRaw, err := json.Marshal(fragment.Request)
	if err != nil || fragment.RequestSHA256 != qualificationSHA256(requestRaw) {
		return errors.New("history-agent fragment request digest differs")
	}
	publicRaw, err := base64.StdEncoding.Strict().DecodeString(identity.SigningPublicKey)
	if err != nil || len(publicRaw) != ed25519.PublicKeySize {
		return errors.New("history-agent public key is malformed")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(fragment.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("history-agent fragment signature is malformed")
	}
	fragment.Signature = ""
	payload, err := json.Marshal(fragment)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(publicRaw), payload, signature) {
		return errors.New("history-agent fragment signature verification failed")
	}
	return nil
}

func validateAnchorHistoryAgentFragmentEvidence(fragment anchorHistoryAgentFragment, identity anchorHistoryAgentIdentity, runID, configurationID, sourceRevision string) error {
	if fragment.Request.RunID != runID || fragment.Request.ConfigurationID != configurationID || fragment.SourceRevision != sourceRevision || fragment.Request.SourceRevision != sourceRevision {
		return errors.New("history-agent fragment is not bound to its run and configuration")
	}
	if err := verifyAnchorHistoryAgentFragment(fragment, identity); err != nil {
		return err
	}
	if _, err := validateAnchorHistoryAgentRequestStatic(fragment.Request, identity.AgentID, configurationID, sourceRevision); err != nil {
		return err
	}
	if fragment.StartedAt.Before(fragment.Request.IssuedAt.Add(-30*time.Second)) || fragment.FinishedAt.After(fragment.Request.ExpiresAt.Add(time.Second)) {
		return errors.New("history-agent fragment timing is outside its signed lease")
	}
	if fragment.Request.Kind == string(qualification.AnchorHistoryAdvance) && fragment.Value != fragment.Request.Target {
		return errors.New("history-agent advance result differs from its signed target")
	}
	if fragment.Request.Kind == string(qualification.AnchorHistoryLoad) {
		if fragment.Outcome == string(qualification.AnchorHistoryFailed) && fragment.Value != (anchorHistoryWireValue{}) {
			return errors.New("failed history-agent load claims a value")
		}
		if fragment.Outcome == string(qualification.AnchorHistorySucceeded) {
			if _, err := anchorHistoryValueFromWire(fragment.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateAnchorHistoryAgentEvidence(controller anchorHistoryControllerRecord) error {
	if len(controller.Agents) < 2 || len(controller.Agents) > 16 || len(controller.Fragments) != len(controller.Operations) {
		return errors.New("controller history lacks complete signed agent evidence")
	}
	agents := make(map[string]anchorHistoryAgentIdentity, len(controller.Agents))
	for _, identity := range controller.Agents {
		if !qualificationSafeName(identity.AgentID, 64) || !qualificationHexDigest(identity.EndpointSHA256) || identity.SigningPublicKey == "" {
			return errors.New("controller history has an invalid agent identity")
		}
		if _, duplicate := agents[identity.AgentID]; duplicate {
			return errors.New("controller history duplicates an agent identity")
		}
		agents[identity.AgentID] = identity
	}
	fragments := make(map[string]anchorHistoryAgentFragment, len(controller.Fragments))
	usedAgents := make(map[string]struct{}, len(controller.Agents))
	for _, fragment := range controller.Fragments {
		identity, exists := agents[fragment.Request.AgentID]
		if !exists {
			return errors.New("controller history fragment is not bound to its run, configuration and agent")
		}
		if err := validateAnchorHistoryAgentFragmentEvidence(fragment, identity, controller.RunID, controller.ConfigurationID, controller.SourceRevision); err != nil {
			return err
		}
		if _, duplicate := fragments[fragment.Request.OperationID]; duplicate {
			return errors.New("controller history duplicates an agent fragment")
		}
		fragments[fragment.Request.OperationID] = fragment
		usedAgents[fragment.Request.AgentID] = struct{}{}
	}
	if len(usedAgents) != len(controller.Agents) {
		return errors.New("controller history contains an agent with no signed operation")
	}
	for _, operation := range controller.Operations {
		fragment, exists := fragments[operation.ID]
		if !exists || operation.Kind != fragment.Request.Kind || operation.Outcome != fragment.Outcome || operation.Value != fragment.Value {
			return errors.New("controller operation differs from its signed agent fragment")
		}
	}
	return nil
}

func runAnchorHistoryQualificationRun(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("anchor-qualification history-run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "strict multi-agent execution plan JSON")
	outputPath := flags.String("out", "", "new exclusive controller history JSON")
	sourceRevision := flags.String("source-revision", "", "40- or 64-hex release revision under qualification")
	requireClean := flags.Bool("require-clean-source", false, "require this controller binary to be a clean build of source revision")
	agentCA := flags.String("agent-ca", "", "history-agent server CA PEM")
	agentClientCert := flags.String("agent-client-cert", "", "controller mTLS client certificate PEM")
	agentClientKey := flags.String("agent-client-key", "", "private controller mTLS client key PEM")
	timeout := flags.Duration("timeout", 10*time.Second, "per-operation deadline")
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
	if *planPath == "" || *outputPath == "" || !validDurabilitySourceRevision(*sourceRevision) || *agentCA == "" || *agentClientCert == "" || *agentClientKey == "" || *timeout <= 0 || *timeout > time.Minute || flags.NArg() != 0 {
		return errors.New("history-run requires plan, output, source revision, agent mTLS files and a valid timeout")
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if *requireClean && (buildRevision != *sourceRevision || buildModified) {
		return errors.New("history-run clean source verification failed")
	}
	var plan anchorHistoryRunPlan
	if _, err := readQualificationReceipt(*planPath, &plan); err != nil {
		return fmt.Errorf("history run plan: %w", err)
	}
	if err := validateAnchorHistoryRunPlan(plan); err != nil {
		return err
	}
	if plan.SourceRevision != *sourceRevision {
		return errors.New("history-run plan source revision differs from the requested revision")
	}
	store, anchorTransport, err := newRemoteAnchorStore(remoteAnchorConfig{
		clusterID: *cluster, replicaSpecs: replicas, anchorName: *anchorName, keyID: *keyID, keyFile: *keyFile,
		serverCAFile: *serverCA, clientCertFile: *clientCert, clientKeyFile: *clientKey, operationTimeout: *timeout,
	})
	if err != nil {
		return err
	}
	defer anchorTransport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	initial, exists, err := store.Load(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("history-run initial quorum load: %w", err)
	}
	agentTransport, err := newAnchorHistoryAgentTransport(*agentCA, *agentClientCert, *agentClientKey, *timeout, len(plan.Agents))
	if err != nil {
		return err
	}
	defer agentTransport.CloseIdleConnections()
	client := &http.Client{Transport: agentTransport}
	identities := make([]anchorHistoryAgentIdentity, len(plan.Agents))
	agents := make(map[string]anchorHistoryRunPlanAgent, len(plan.Agents))
	for index, agent := range plan.Agents {
		agents[agent.AgentID] = agent
		identities[index] = anchorHistoryAgentIdentity{AgentID: agent.AgentID, EndpointSHA256: qualificationSHA256([]byte(agent.Endpoint)), SigningPublicKey: agent.SigningPublicKey}
	}
	var events atomic.Uint64
	operations := make([]anchorHistoryWireOperation, 0)
	fragments := make([]anchorHistoryAgentFragment, 0)
	for _, wave := range plan.Waves {
		type result struct {
			operation anchorHistoryWireOperation
			fragment  anchorHistoryAgentFragment
			err       error
		}
		results := make(chan result, len(wave.Operations))
		dispatch := make(chan struct{})
		var armed sync.WaitGroup
		armed.Add(len(wave.Operations))
		for _, scheduled := range wave.Operations {
			agent := agents[scheduled.AgentID]
			go func() {
				issued := time.Now().UTC()
				nonce, nonceErr := anchorHistoryNonce()
				if nonceErr != nil {
					armed.Done()
					results <- result{err: nonceErr}
					return
				}
				command := anchorHistoryAgentRequest{
					SchemaVersion: anchorHistoryAgentProtocolSchema, RunID: plan.RunID, ConfigurationID: store.ConfigurationID(), SourceRevision: plan.SourceRevision, AgentID: scheduled.AgentID,
					OperationID: scheduled.ID, Kind: scheduled.Kind, Target: scheduled.Target, Nonce: nonce, IssuedAt: issued, ExpiresAt: issued.Add(*timeout),
				}
				invoke := events.Add(1)
				armed.Done()
				<-dispatch
				fragment, executeErr := executeAnchorHistoryAgent(client, agent.Endpoint, command, agent.SigningPublicKey, *timeout)
				if executeErr == nil && *requireClean && (fragment.SourceRevision != *sourceRevision || fragment.BuildRevision != *sourceRevision || fragment.BuildModified) {
					executeErr = errors.New("history agent was not a clean build of the requested source revision")
				}
				returned := events.Add(1)
				if executeErr != nil {
					results <- result{err: fmt.Errorf("agent %s operation %s: %w", agent.AgentID, scheduled.ID, executeErr)}
					return
				}
				results <- result{operation: anchorHistoryWireOperation{ID: scheduled.ID, Kind: scheduled.Kind, Outcome: fragment.Outcome, Invoke: invoke, Return: returned, Value: fragment.Value}, fragment: fragment}
			}()
		}
		armed.Wait()
		close(dispatch)
		for range wave.Operations {
			completed := <-results
			if completed.err != nil {
				return completed.err
			}
			operations = append(operations, completed.operation)
			fragments = append(fragments, completed.fragment)
		}
	}
	sort.Slice(operations, func(i, j int) bool { return operations[i].Invoke < operations[j].Invoke })
	sort.Slice(fragments, func(i, j int) bool { return fragments[i].Request.OperationID < fragments[j].Request.OperationID })
	sort.Slice(identities, func(i, j int) bool { return identities[i].AgentID < identities[j].AgentID })
	controller := anchorHistoryControllerRecord{
		SchemaVersion: anchorHistoryControllerSchema, RunID: plan.RunID, ConfigurationID: store.ConfigurationID(),
		SourceRevision: plan.SourceRevision, BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(), Agents: identities,
		Initial: anchorHistoryWireValueFromAnchor(initial, exists), Operations: operations, Fragments: fragments,
	}
	history, err := validateAnchorHistoryController(controller, qualification.MaxAnchorHistoryOperations-1)
	if err != nil {
		return err
	}
	check, err := qualification.CheckAnchorHistory(history)
	if err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(*outputPath, controller); err != nil {
		return err
	}
	if !check.Linearizable {
		return errors.New("signed multi-agent history is not linearizable; controller artifact was preserved")
	}
	return json.NewEncoder(stdout).Encode(struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		RunID         string `json:"runId"`
		Operations    int    `json:"operations"`
		Agents        int    `json:"agents"`
		Passed        bool   `json:"passed"`
	}{1, plan.RunID, len(operations), len(identities), true})
}

func validateAnchorHistoryRunPlan(plan anchorHistoryRunPlan) error {
	if plan.SchemaVersion != anchorHistoryRunPlanSchema || !anchorQualificationHex(plan.RunID, 16) || !validDurabilitySourceRevision(plan.SourceRevision) || len(plan.Agents) < 2 || len(plan.Agents) > 16 || len(plan.Waves) < 1 {
		return errors.New("history run plan identity, agents or waves are invalid")
	}
	agents := make(map[string]struct{}, len(plan.Agents))
	for _, agent := range plan.Agents {
		parsed, err := url.Parse(agent.Endpoint)
		key, keyErr := base64.StdEncoding.Strict().DecodeString(agent.SigningPublicKey)
		if !qualificationSafeName(agent.AgentID, 64) || err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" ||
			keyErr != nil || len(key) != ed25519.PublicKeySize {
			return errors.New("history run plan has an invalid agent endpoint or key")
		}
		if _, duplicate := agents[agent.AgentID]; duplicate {
			return errors.New("history run plan duplicates an agent")
		}
		agents[agent.AgentID] = struct{}{}
	}
	operations := make(map[string]struct{})
	usedAgents := make(map[string]struct{}, len(plan.Agents))
	count := 0
	for _, wave := range plan.Waves {
		if len(wave.Operations) < 1 {
			return errors.New("history run plan contains an empty wave")
		}
		for _, operation := range wave.Operations {
			count++
			if _, exists := agents[operation.AgentID]; !exists || !qualificationSafeName(operation.ID, 128) || operation.ID == anchorHistoryFinalLoadID {
				return errors.New("history run plan operation identity is invalid")
			}
			if _, duplicate := operations[operation.ID]; duplicate {
				return errors.New("history run plan duplicates an operation ID")
			}
			operations[operation.ID] = struct{}{}
			usedAgents[operation.AgentID] = struct{}{}
			switch operation.Kind {
			case string(qualification.AnchorHistoryLoad):
				if operation.Target != (anchorHistoryWireValue{}) {
					return errors.New("history run load has a target")
				}
			case string(qualification.AnchorHistoryAdvance):
				if _, err := anchorHistoryAnchorFromWire(operation.Target); err != nil {
					return err
				}
			default:
				return errors.New("history run operation kind is invalid")
			}
		}
	}
	if count < 1 || count > qualification.MaxAnchorHistoryOperations-1 {
		return errors.New("history run plan operation count is invalid")
	}
	if len(usedAgents) != len(plan.Agents) {
		return errors.New("history run plan contains an unused agent")
	}
	return nil
}

func newAnchorHistoryAgentTransport(caPath, certificatePath, keyPath string, timeout time.Duration, agents int) (*http.Transport, error) {
	certificate, err := loadAnchorClientCertificate(certificatePath, keyPath)
	if err != nil {
		return nil, err
	}
	roots, err := loadAnchorClientCAs(caPath)
	if err != nil {
		return nil, err
	}
	return &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: min(timeout, 5*time.Second), KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, TLSHandshakeTimeout: min(timeout, 5*time.Second), ResponseHeaderTimeout: timeout,
		IdleConnTimeout: 30 * time.Second, MaxIdleConns: agents * 2, MaxIdleConnsPerHost: 2, DisableCompression: true,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{certificate}},
	}, nil
}

func executeAnchorHistoryAgent(client *http.Client, endpoint string, request anchorHistoryAgentRequest, publicKey string, timeout time.Duration) (anchorHistoryAgentFragment, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return anchorHistoryAgentFragment{}, err
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		remaining := time.Until(request.ExpiresAt)
		if remaining <= 0 {
			break
		}
		fragment, retryable, attemptErr := executeAnchorHistoryAgentOnce(client, endpoint, request, publicKey, body, min(timeout, remaining))
		if attemptErr == nil {
			return fragment, nil
		}
		lastErr = attemptErr
		if !retryable {
			break
		}
	}
	return anchorHistoryAgentFragment{}, errors.Join(lastErr, errors.New("history-agent response unavailable after idempotent retry"))
}

func executeAnchorHistoryAgentOnce(client *http.Client, endpoint string, request anchorHistoryAgentRequest, publicKey string, body []byte, timeout time.Duration) (anchorHistoryAgentFragment, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/execute", bytes.NewReader(body))
	if err != nil {
		return anchorHistoryAgentFragment{}, false, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := client.Do(httpRequest)
	if err != nil {
		return anchorHistoryAgentFragment{}, true, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/json" {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return anchorHistoryAgentFragment{}, response.StatusCode >= 500, fmt.Errorf("agent HTTP status %d: %s", response.StatusCode, strings.TrimSpace(string(message)))
	}
	var fragment anchorHistoryAgentFragment
	decoder := json.NewDecoder(io.LimitReader(response.Body, anchorHistoryAgentBodyMaximum+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fragment); err != nil {
		return anchorHistoryAgentFragment{}, true, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return anchorHistoryAgentFragment{}, true, errors.New("agent response has trailing JSON")
	}
	identity := anchorHistoryAgentIdentity{AgentID: request.AgentID, SigningPublicKey: publicKey}
	if fragment.Request != request {
		return anchorHistoryAgentFragment{}, false, errors.New("agent changed the execution request")
	}
	if err := verifyAnchorHistoryAgentFragment(fragment, identity); err != nil {
		return anchorHistoryAgentFragment{}, false, err
	}
	return fragment, false, nil
}

func anchorHistoryAnchorFromWire(value anchorHistoryWireValue) (meldbase.RollbackAnchor, error) {
	converted, err := anchorHistoryValueFromWire(value)
	if err != nil || !converted.Exists {
		return meldbase.RollbackAnchor{}, errors.Join(err, errors.New("anchor advance target is invalid"))
	}
	return meldbase.RollbackAnchor{DatabaseID: converted.DatabaseID, MinimumCommitSequence: converted.CommitSequence, MinimumGeneration: converted.Generation}, nil
}

func anchorHistoryNonce() (string, error) {
	var nonce [16]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(nonce[:]), nil
}

func anchorHistoryCertificateSPKI(certificate *x509.Certificate) string {
	digest := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(digest[:])
}
