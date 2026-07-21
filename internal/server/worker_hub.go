package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/internal/policyrecord"
)

var workerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var workerOperationIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var workerCollectionNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,127}$`)

var errRPCWorkerBusy = errors.New("RPC worker is busy")
var errPolicyInvalidationUnavailable = errors.New("durable policy invalidation is unavailable")

type workerProtocolError struct{ message string }

func (err *workerProtocolError) Error() string { return err.message }
func workerProtocol(message string) error      { return &workerProtocolError{message: message} }

// WorkerIdentity identifies a control-plane worker credential. It is separate
// from the application Actor passed to RPC and publication handlers.
type WorkerIdentity struct{ ID string }

// WorkerAuthenticator is a separate control-plane trust boundary. Client
// authenticators must never be reused implicitly for worker connections.
type WorkerAuthenticator interface {
	AuthenticateWorker(*http.Request) (WorkerIdentity, error)
}

type workerTokenAuthenticator struct{ digest [32]byte }

// NewWorkerTokenAuthenticator creates a constant-time bearer authenticator.
// The raw token is not retained after construction.
func NewWorkerTokenAuthenticator(token string) (WorkerAuthenticator, error) {
	if len(token) < 32 || len(token) > 4096 {
		return nil, errors.New("worker token must contain between 32 and 4096 bytes")
	}
	return &workerTokenAuthenticator{digest: sha256.Sum256([]byte(token))}, nil
}

func (authenticator *workerTokenAuthenticator) AuthenticateWorker(request *http.Request) (WorkerIdentity, error) {
	if authenticator == nil || request == nil || request.URL == nil || request.URL.RawQuery != "" {
		return WorkerIdentity{}, ErrUnauthenticated
	}
	header := request.Header.Get("authorization")
	if len(header) < 8 || !strings.EqualFold(header[:7], "bearer ") || strings.TrimSpace(header[7:]) != header[7:] {
		return WorkerIdentity{}, ErrUnauthenticated
	}
	digest := sha256.Sum256([]byte(header[7:]))
	if subtle.ConstantTimeCompare(digest[:], authenticator.digest[:]) != 1 {
		return WorkerIdentity{}, ErrUnauthenticated
	}
	return WorkerIdentity{ID: "static-token"}, nil
}

type WorkerHubConfig struct {
	Authenticator            WorkerAuthenticator
	PublicationCollections   []string
	RegistrationTimeout      time.Duration
	MaxFrameBytes            int
	MaxMethodsPerWorker      int
	MaxPublicationsPerWorker int
	MaxPendingCalls          int
	MaxOperationsPerCall     int
	PolicyQueryLimits        meldbase.QueryLimits
	PolicyEvaluationTimeout  time.Duration
	PolicyGenerationStore    PolicyGenerationStore
}

// WorkerHub routes dynamically registered, separately authenticated worker
// methods. Mount it on a private control listener and pass it as both resolver
// fields when transactional worker methods are desired.
type WorkerHub struct {
	config              WorkerHubConfig
	mu                  sync.RWMutex
	owners              map[string]workerMethodOwner
	publications        map[string]workerPublicationOwner
	managedPublications map[string]struct{}
	reserved            map[string]struct{}
	stats               workerHubMetrics
}

type workerMethodOwner struct {
	connection *workerConnection
	mode       string
}

type workerPublicationOwner struct {
	connection  *workerConnection
	publication workerPublication
}

type WorkerHubStats struct {
	ConnectedWorkers       uint64 `json:"connectedWorkers"`
	RegisteredMethods      uint64 `json:"registeredMethods"`
	RegisteredPublications uint64 `json:"registeredPublications"`
	CallsStarted           uint64 `json:"callsStarted"`
	CallsActive            uint64 `json:"callsActive"`
	CallsSucceeded         uint64 `json:"callsSucceeded"`
	CallsFailed            uint64 `json:"callsFailed"`
	CallsCanceled          uint64 `json:"callsCanceled"`
	CallsBusy              uint64 `json:"callsBusy"`
	ProtocolFailures       uint64 `json:"protocolFailures"`
	BytesReceived          uint64 `json:"bytesReceived"`
	BytesSent              uint64 `json:"bytesSent"`
	TransactionOps         uint64 `json:"transactionOps"`
	PolicyEvaluations      uint64 `json:"policyEvaluations"`
	PolicyActive           uint64 `json:"policyActive"`
	PolicySucceeded        uint64 `json:"policySucceeded"`
	PolicyDenied           uint64 `json:"policyDenied"`
	PolicyFailed           uint64 `json:"policyFailed"`
	PolicyCanceled         uint64 `json:"policyCanceled"`
	PolicyBusy             uint64 `json:"policyBusy"`
	PolicyInvalidations    uint64 `json:"policyInvalidations"`
}

type workerHubMetrics struct {
	connectedWorkers, registeredMethods, registeredPublications atomic.Uint64
	callsStarted, callsActive                                   atomic.Uint64
	callsSucceeded, callsFailed                                 atomic.Uint64
	callsCanceled, callsBusy                                    atomic.Uint64
	protocolFailures                                            atomic.Uint64
	bytesReceived, bytesSent                                    atomic.Uint64
	transactionOps                                              atomic.Uint64
	policyEvaluations, policyActive                             atomic.Uint64
	policySucceeded, policyDenied                               atomic.Uint64
	policyFailed, policyCanceled                                atomic.Uint64
	policyBusy                                                  atomic.Uint64
	policyInvalidations                                         atomic.Uint64
}

func NewWorkerHub(config WorkerHubConfig) (*WorkerHub, error) {
	if config.Authenticator == nil {
		return nil, errors.New("worker authenticator is required")
	}
	if config.RegistrationTimeout <= 0 {
		config.RegistrationTimeout = 5 * time.Second
	}
	if config.RegistrationTimeout > 30*time.Second {
		return nil, errors.New("worker registration timeout exceeds 30 seconds")
	}
	if config.MaxFrameBytes <= 0 {
		config.MaxFrameBytes = 1 << 20
	}
	if config.MaxFrameBytes > 16<<20 {
		return nil, errors.New("worker frame limit exceeds 16 MiB")
	}
	if config.MaxMethodsPerWorker <= 0 {
		config.MaxMethodsPerWorker = 128
	}
	if config.MaxMethodsPerWorker > 4096 {
		return nil, errors.New("worker method limit exceeds 4096")
	}
	if config.MaxPublicationsPerWorker <= 0 {
		config.MaxPublicationsPerWorker = 128
	}
	if config.MaxPublicationsPerWorker > 4096 {
		return nil, errors.New("worker publication limit exceeds 4096")
	}
	if config.MaxPendingCalls <= 0 {
		config.MaxPendingCalls = 64
	}
	if config.MaxPendingCalls > 4096 {
		return nil, errors.New("worker pending call limit exceeds 4096")
	}
	if config.MaxOperationsPerCall <= 0 {
		config.MaxOperationsPerCall = 256
	}
	if config.MaxOperationsPerCall > 4096 {
		return nil, errors.New("worker transaction operation limit exceeds 4096")
	}
	if config.PolicyEvaluationTimeout <= 0 {
		config.PolicyEvaluationTimeout = 2 * time.Second
	}
	if config.PolicyEvaluationTimeout > 30*time.Second {
		return nil, errors.New("worker policy evaluation timeout exceeds 30 seconds")
	}
	managedPublications := make(map[string]struct{}, len(config.PublicationCollections))
	for _, collection := range config.PublicationCollections {
		if !workerCollectionNamePattern.MatchString(collection) {
			return nil, errors.New("worker publication collection is invalid")
		}
		if _, duplicate := managedPublications[collection]; duplicate {
			return nil, errors.New("worker publication collection is duplicated")
		}
		managedPublications[collection] = struct{}{}
	}
	config.PublicationCollections = append([]string(nil), config.PublicationCollections...)
	return &WorkerHub{
		config: config, owners: make(map[string]workerMethodOwner),
		publications: make(map[string]workerPublicationOwner), managedPublications: managedPublications,
		reserved: make(map[string]struct{}),
	}, nil
}

func (hub *WorkerHub) reserveRPCMethods(names []string) error {
	if hub == nil {
		return errors.New("worker hub is required")
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	for _, name := range names {
		if owner, exists := hub.owners[name]; exists && owner.connection != nil {
			return errors.New("local RPC method conflicts with a connected worker registration")
		}
	}
	for _, name := range names {
		hub.reserved[name] = struct{}{}
	}
	return nil
}

func (hub *WorkerHub) Stats() WorkerHubStats {
	if hub == nil {
		return WorkerHubStats{}
	}
	return WorkerHubStats{
		ConnectedWorkers: hub.stats.connectedWorkers.Load(), RegisteredMethods: hub.stats.registeredMethods.Load(),
		RegisteredPublications: hub.stats.registeredPublications.Load(),
		CallsStarted:           hub.stats.callsStarted.Load(), CallsActive: hub.stats.callsActive.Load(),
		CallsSucceeded: hub.stats.callsSucceeded.Load(), CallsFailed: hub.stats.callsFailed.Load(),
		CallsCanceled: hub.stats.callsCanceled.Load(), CallsBusy: hub.stats.callsBusy.Load(),
		ProtocolFailures: hub.stats.protocolFailures.Load(), BytesReceived: hub.stats.bytesReceived.Load(),
		BytesSent: hub.stats.bytesSent.Load(), TransactionOps: hub.stats.transactionOps.Load(),
		PolicyEvaluations: hub.stats.policyEvaluations.Load(), PolicyActive: hub.stats.policyActive.Load(),
		PolicySucceeded: hub.stats.policySucceeded.Load(), PolicyDenied: hub.stats.policyDenied.Load(),
		PolicyFailed: hub.stats.policyFailed.Load(), PolicyCanceled: hub.stats.policyCanceled.Load(),
		PolicyBusy:          hub.stats.policyBusy.Load(),
		PolicyInvalidations: hub.stats.policyInvalidations.Load(),
	}
}

func (hub *WorkerHub) ResolveRPCMethod(name string) (RPCMethod, bool) {
	owner, ok := hub.resolve(name, "rpc")
	if !ok {
		return nil, false
	}
	return func(ctx context.Context, actor Actor, arguments []meldbase.Value) (meldbase.Value, error) {
		return owner.connection.invoke(ctx, name, "rpc", actor, arguments, nil)
	}, true
}

func (hub *WorkerHub) ResolveRPCTransactionalMethod(name string) (RPCTransactionalMethod, bool) {
	owner, ok := hub.resolve(name, "transactional")
	if !ok {
		return nil, false
	}
	return func(ctx context.Context, actor Actor, arguments []meldbase.Value, tx *meldbase.WriteTransaction) (meldbase.Value, error) {
		return owner.connection.invoke(ctx, name, "transactional", actor, arguments, tx)
	}, true
}

func (hub *WorkerHub) ResolveQueryPolicy(ctx context.Context, actor Actor, collection string, query meldbase.QuerySpec) (QueryPolicy, bool, error) {
	if hub == nil || !workerCollectionNamePattern.MatchString(collection) {
		return QueryPolicy{}, false, nil
	}
	hub.mu.RLock()
	_, managed := hub.managedPublications[collection]
	owner, ok := hub.publications[collection]
	hub.mu.RUnlock()
	if !managed {
		return QueryPolicy{}, false, nil
	}
	if !ok || owner.connection == nil {
		return QueryPolicy{}, true, ErrForbidden
	}
	policy, err := owner.connection.invokePolicy(ctx, actor, query, owner.publication)
	return policy, true, err
}

func (hub *WorkerHub) resolve(name, mode string) (workerMethodOwner, bool) {
	if hub == nil || !validRPCMethodName(name) {
		return workerMethodOwner{}, false
	}
	hub.mu.RLock()
	owner, ok := hub.owners[name]
	hub.mu.RUnlock()
	return owner, ok && owner.mode == mode
}

type workerRegistration struct {
	Version      int                             `json:"v"`
	Type         string                          `json:"type"`
	WorkerID     string                          `json:"workerId"`
	Methods      []workerRegistrationMethod      `json:"methods"`
	Publications []workerRegistrationPublication `json:"publications,omitempty"`
}

type workerRegistrationMethod struct {
	Name string `json:"name"`
	Mode string `json:"mode"`
}

type workerRegistrationPublication struct {
	Collection   string          `json:"collection"`
	Version      string          `json:"version"`
	MaxResults   int             `json:"maxResults"`
	QueryPaths   json.RawMessage `json:"queryPaths"`
	ResultFields json.RawMessage `json:"resultFields"`
}

type workerPublication struct {
	collection           string
	declarationDigest    [32]byte
	generation           [16]byte
	policyVersion        string
	maxResults           int
	allowAllQueryPaths   bool
	allowedQueryPaths    map[string]struct{}
	allowAllResultFields bool
	allowedResultFields  map[string]struct{}
	lease                *QueryPolicyLease
}

func normalizeWorkerPublication(registration workerRegistrationPublication) (workerPublication, error) {
	if !workerCollectionNamePattern.MatchString(registration.Collection) || !validPolicyVersion(registration.Version) ||
		registration.MaxResults <= 0 || registration.MaxResults > meldbase.DefaultQueryLimits.MaxLimit {
		return workerPublication{}, errors.New("invalid worker publication")
	}
	queryAll, queryPaths, queryList, err := decodeWorkerPolicyFields(registration.QueryPaths, true)
	if err != nil {
		return workerPublication{}, err
	}
	resultAll, resultFields, resultList, err := decodeWorkerPolicyFields(registration.ResultFields, false)
	if err != nil {
		return workerPublication{}, err
	}
	canonical, err := json.Marshal(map[string]any{
		"collection": registration.Collection, "version": registration.Version, "maxResults": registration.MaxResults,
		"queryPaths": policyFieldDeclaration(queryAll, queryList), "resultFields": policyFieldDeclaration(resultAll, resultList),
	})
	if err != nil {
		return workerPublication{}, err
	}
	publication := workerPublication{
		collection: registration.Collection, declarationDigest: sha256.Sum256(canonical), maxResults: registration.MaxResults,
		allowAllQueryPaths: queryAll, allowedQueryPaths: queryPaths,
		allowAllResultFields: resultAll, allowedResultFields: resultFields,
	}
	return activateWorkerPublication(publication, [16]byte{})
}

func activateWorkerPublication(publication workerPublication, generation [16]byte) (workerPublication, error) {
	versionInput := make([]byte, 0, len(publication.declarationDigest)+len(generation))
	versionInput = append(versionInput, publication.declarationDigest[:]...)
	versionInput = append(versionInput, generation[:]...)
	digest := sha256.Sum256(versionInput)
	publication.policyVersion = "publication-" + hex.EncodeToString(digest[:])
	publication.generation = generation
	lease, err := NewQueryPolicyLease(publication.policyVersion)
	if err != nil {
		return workerPublication{}, err
	}
	publication.lease = lease
	return publication, nil
}

func decodeWorkerPolicyFields(raw json.RawMessage, queryPaths bool) (bool, map[string]struct{}, []string, error) {
	var wildcard string
	if err := json.Unmarshal(raw, &wildcard); err == nil {
		if wildcard == "*" {
			return true, nil, nil, nil
		}
		return false, nil, nil, errors.New("invalid worker publication wildcard")
	}
	var values []string
	if len(raw) == 0 || decodeStrict(raw, &values) != nil || len(values) > 256 {
		return false, nil, nil, errors.New("invalid worker publication field policy")
	}
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, duplicate := result[value]; duplicate {
			return false, nil, nil, errors.New("duplicate worker publication field")
		}
		if queryPaths {
			expression, _ := json.Marshal(map[string]any{"version": 1, "where": map[string]any{"op": "exists", "path": value, "value": true}})
			if _, err := meldbase.DecodeQuerySpecJSON(expression, meldbase.QueryLimits{}); err != nil {
				return false, nil, nil, fmt.Errorf("invalid worker query path: %w", err)
			}
		} else if strings.Contains(value, ".") || (meldbase.Document{value: meldbase.Null()}).Validate() != nil {
			return false, nil, nil, errors.New("invalid worker result field")
		}
		result[value] = struct{}{}
	}
	sort.Strings(values)
	return false, result, values, nil
}

func policyFieldDeclaration(all bool, values []string) any {
	if all {
		return "*"
	}
	return values
}

func (hub *WorkerHub) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if hub == nil || request == nil || request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if request.Header.Get("origin") != "" {
		http.Error(writer, "browser origins are forbidden", http.StatusForbidden)
		return
	}
	if _, err := hub.config.Authenticator.AuthenticateWorker(request); err != nil {
		http.Error(writer, "unauthenticated", http.StatusUnauthorized)
		return
	}
	if !requestsWorkerCapabilities(request) {
		http.Error(writer, "worker protocol v1 is required", http.StatusUpgradeRequired)
		return
	}
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	connection.SetReadLimit(int64(hub.config.MaxFrameBytes))
	worker := &workerConnection{
		hub: hub, socket: connection, done: make(chan struct{}), pending: make(map[string]*workerPendingCall),
	}
	registrationContext, cancel := context.WithTimeout(request.Context(), hub.config.RegistrationTimeout)
	registration, err := worker.readRegistration(registrationContext)
	if err != nil {
		cancel()
		hub.stats.protocolFailures.Add(1)
		_ = connection.Close(websocket.StatusPolicyViolation, "invalid registration")
		return
	}
	if err := hub.register(registrationContext, worker, registration); err != nil {
		cancel()
		hub.stats.protocolFailures.Add(1)
		_ = connection.Close(websocket.StatusPolicyViolation, "registration rejected")
		return
	}
	cancel()
	defer hub.unregister(worker)
	sessionID, err := randomToken16()
	registered := map[string]any{
		"v": protocolVersion, "type": "registered", "sessionId": hex.EncodeToString(sessionID[:]),
		"limits": map[string]any{
			"maxPendingCalls": hub.config.MaxPendingCalls, "maxOperationsPerCall": hub.config.MaxOperationsPerCall,
			"maxPublicationsPerWorker":  hub.config.MaxPublicationsPerWorker,
			"policyEvaluationTimeoutMs": hub.config.PolicyEvaluationTimeout.Milliseconds(),
		},
	}
	registered["protocol"] = workerProtocolDescriptor()
	if err != nil || worker.send(request.Context(), registered) != nil {
		return
	}
	if err := worker.readLoop(request.Context()); isWorkerProtocolError(err) {
		hub.stats.protocolFailures.Add(1)
	}
}

func isWorkerProtocolError(err error) bool {
	var protocol *workerProtocolError
	return errors.As(err, &protocol)
}

func (worker *workerConnection) readRegistration(ctx context.Context) (workerRegistration, error) {
	raw, err := worker.readText(ctx)
	if err != nil {
		return workerRegistration{}, err
	}
	var registration workerRegistration
	if err := decodeStrict(raw, &registration); err != nil || registration.Version != protocolVersion || registration.Type != "register" ||
		!workerIDPattern.MatchString(registration.WorkerID) ||
		(len(registration.Methods) == 0 && len(registration.Publications) == 0) ||
		len(registration.Methods) > worker.hub.config.MaxMethodsPerWorker ||
		len(registration.Publications) > worker.hub.config.MaxPublicationsPerWorker {
		return workerRegistration{}, errors.New("invalid worker registration")
	}
	seen := make(map[string]struct{}, len(registration.Methods))
	for _, method := range registration.Methods {
		if !validRPCMethodName(method.Name) || (method.Mode != "rpc" && method.Mode != "transactional") {
			return workerRegistration{}, errors.New("invalid worker method")
		}
		if _, duplicate := seen[method.Name]; duplicate {
			return workerRegistration{}, errors.New("duplicate worker method")
		}
		seen[method.Name] = struct{}{}
	}
	seenPublications := make(map[string]struct{}, len(registration.Publications))
	for _, publication := range registration.Publications {
		if _, duplicate := seenPublications[publication.Collection]; duplicate {
			return workerRegistration{}, errors.New("duplicate worker publication")
		}
		if _, err := normalizeWorkerPublication(publication); err != nil {
			return workerRegistration{}, err
		}
		seenPublications[publication.Collection] = struct{}{}
	}
	return registration, nil
}

func (hub *WorkerHub) register(ctx context.Context, worker *workerConnection, registration workerRegistration) error {
	publications := make([]workerPublication, len(registration.Publications))
	for index, declared := range registration.Publications {
		publication, err := normalizeWorkerPublication(declared)
		if err != nil {
			return err
		}
		if hub.config.PolicyGenerationStore != nil {
			generation, _, err := hub.config.PolicyGenerationStore.LoadPolicyGeneration(ctx, publication.collection)
			if err != nil {
				return err
			}
			publication, err = activateWorkerPublication(publication, generation)
			if err != nil {
				return err
			}
		}
		publications[index] = publication
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	for _, method := range registration.Methods {
		if _, reserved := hub.reserved[method.Name]; reserved {
			return errors.New("worker method is reserved by a local registration")
		}
		if _, exists := hub.owners[method.Name]; exists {
			return errors.New("worker method already owned")
		}
	}
	for _, publication := range publications {
		if _, managed := hub.managedPublications[publication.collection]; !managed {
			return errors.New("worker publication collection is not managed by this hub")
		}
		if _, exists := hub.publications[publication.collection]; exists {
			return errors.New("worker publication already owned")
		}
	}
	worker.workerID = registration.WorkerID
	worker.methods = append([]workerRegistrationMethod(nil), registration.Methods...)
	worker.publications = publications
	for _, method := range worker.methods {
		hub.owners[method.Name] = workerMethodOwner{connection: worker, mode: method.Mode}
	}
	for _, publication := range worker.publications {
		hub.publications[publication.collection] = workerPublicationOwner{connection: worker, publication: publication}
	}
	hub.stats.connectedWorkers.Add(1)
	hub.stats.registeredMethods.Add(uint64(len(worker.methods)))
	hub.stats.registeredPublications.Add(uint64(len(worker.publications)))
	return nil
}

func (hub *WorkerHub) unregister(worker *workerConnection) {
	if hub == nil || worker == nil {
		return
	}
	hub.mu.Lock()
	for _, method := range worker.methods {
		if owner, exists := hub.owners[method.Name]; exists && owner.connection == worker {
			delete(hub.owners, method.Name)
		}
	}
	for _, publication := range worker.publications {
		if owner, exists := hub.publications[publication.collection]; exists && owner.connection == worker {
			delete(hub.publications, publication.collection)
		}
		revokePolicyLeaseNow(publication.lease)
	}
	hub.mu.Unlock()
	worker.fail(errors.New("worker disconnected"))
	hub.stats.connectedWorkers.Add(^uint64(0))
	if len(worker.methods) > 0 {
		hub.stats.registeredMethods.Add(^uint64(len(worker.methods) - 1))
	}
	if len(worker.publications) > 0 {
		hub.stats.registeredPublications.Add(^uint64(len(worker.publications) - 1))
	}
}

func (hub *WorkerHub) stagePolicyInvalidation(worker *workerConnection, tx *meldbase.WriteTransaction, collection string) error {
	if hub == nil || worker == nil || tx == nil || hub.config.PolicyGenerationStore == nil || !workerCollectionNamePattern.MatchString(collection) {
		return errPolicyInvalidationUnavailable
	}
	hub.mu.RLock()
	owner, owned := hub.publications[collection]
	hub.mu.RUnlock()
	if !owned || owner.connection != worker {
		return ErrForbidden
	}
	generation, err := randomToken16()
	if err != nil {
		return err
	}
	mutation, err := policyrecord.GenerationMutation(collection, generation)
	if err != nil {
		return err
	}
	return tx.MeldbaseStageSystemMutation(mutation, func(_ uint64) {
		hub.commitPolicyInvalidation(worker, collection, generation)
	})
}

func (hub *WorkerHub) commitPolicyInvalidation(worker *workerConnection, collection string, generation [16]byte) {
	if hub == nil || worker == nil || generation == [16]byte{} {
		return
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	owner, exists := hub.publications[collection]
	if !exists || owner.connection != worker {
		return
	}
	previous := owner.publication
	next, err := activateWorkerPublication(previous, generation)
	if err != nil {
		delete(hub.publications, collection)
		revokePolicyLeaseNow(previous.lease)
		return
	}
	hub.publications[collection] = workerPublicationOwner{connection: worker, publication: next}
	for index := range worker.publications {
		if worker.publications[index].collection == collection {
			worker.publications[index] = next
			break
		}
	}
	revokePolicyLeaseNow(previous.lease)
	hub.stats.policyInvalidations.Add(1)
}

func revokePolicyLeaseNow(lease *QueryPolicyLease) {
	if lease == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = lease.Revoke(ctx)
}

type workerConnection struct {
	hub          *WorkerHub
	socket       *websocket.Conn
	workerID     string
	methods      []workerRegistrationMethod
	publications []workerPublication
	writeMu      sync.Mutex
	mu           sync.Mutex
	pending      map[string]*workerPendingCall
	done         chan struct{}
	close        sync.Once
}

type workerPendingCall struct {
	result     chan workerCallResult
	policy     chan workerPolicyResult
	tx         *meldbase.WriteTransaction
	opMu       sync.Mutex
	operations int
	seenOps    map[string]struct{}
}

type workerCallResult struct {
	value meldbase.Value
	err   error
}

type workerPolicyResult struct {
	constraint *meldbase.QuerySpec
	err        error
}

func (worker *workerConnection) invoke(ctx context.Context, method, mode string, actor Actor, arguments []meldbase.Value, tx *meldbase.WriteTransaction) (meldbase.Value, error) {
	if worker == nil || worker.hub == nil || worker.socket == nil || ctx == nil || (mode == "transactional") != (tx != nil) {
		return meldbase.Value{}, errors.New("worker invocation unavailable")
	}
	if actor.ID == "" || len(actor.ID) > 4096 || len(actor.TenantID) > 4096 ||
		!utf8.ValidString(actor.ID) || !utf8.ValidString(actor.TenantID) {
		return meldbase.Value{}, errors.New("worker actor identity is invalid")
	}
	callToken, err := randomToken16()
	if err != nil {
		return meldbase.Value{}, err
	}
	callID := hex.EncodeToString(callToken[:])
	pending := &workerPendingCall{result: make(chan workerCallResult, 1), tx: tx, seenOps: make(map[string]struct{})}
	worker.mu.Lock()
	if len(worker.pending) >= worker.hub.config.MaxPendingCalls {
		worker.mu.Unlock()
		worker.hub.stats.callsBusy.Add(1)
		return meldbase.Value{}, errRPCWorkerBusy
	}
	select {
	case <-worker.done:
		worker.mu.Unlock()
		return meldbase.Value{}, errors.New("worker disconnected")
	default:
	}
	worker.pending[callID] = pending
	worker.mu.Unlock()
	defer func() {
		worker.mu.Lock()
		delete(worker.pending, callID)
		worker.mu.Unlock()
	}()
	wireArguments := make([]json.RawMessage, len(arguments))
	for index, argument := range arguments {
		encoded, err := meldbase.MarshalWireValue(argument)
		if err != nil {
			return meldbase.Value{}, err
		}
		wireArguments[index] = encoded
	}
	worker.hub.stats.callsStarted.Add(1)
	worker.hub.stats.callsActive.Add(1)
	defer worker.hub.stats.callsActive.Add(^uint64(0))
	if err := worker.send(ctx, map[string]any{
		"v": protocolVersion, "type": "invoke", "callId": callID, "method": method, "mode": mode,
		"actor": map[string]any{"id": actor.ID, "tenantId": actor.TenantID}, "arguments": wireArguments,
	}); err != nil {
		worker.hub.stats.callsFailed.Add(1)
		return meldbase.Value{}, err
	}
	select {
	case result := <-pending.result:
		if result.err != nil {
			worker.hub.stats.callsFailed.Add(1)
			return meldbase.Value{}, result.err
		}
		worker.hub.stats.callsSucceeded.Add(1)
		return result.value, nil
	case <-ctx.Done():
		worker.hub.stats.callsCanceled.Add(1)
		cancelContext, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_ = worker.send(cancelContext, map[string]any{"v": protocolVersion, "type": "cancel", "callId": callID})
		cancel()
		return meldbase.Value{}, ctx.Err()
	case <-worker.done:
		worker.hub.stats.callsFailed.Add(1)
		return meldbase.Value{}, errors.New("worker disconnected")
	}
}

func (worker *workerConnection) invokePolicy(ctx context.Context, actor Actor, query meldbase.QuerySpec, publication workerPublication) (QueryPolicy, error) {
	if worker == nil || worker.hub == nil || worker.socket == nil || ctx == nil || publication.lease == nil ||
		actor.ID == "" || len(actor.ID) > 4096 || len(actor.TenantID) > 4096 ||
		!utf8.ValidString(actor.ID) || !utf8.ValidString(actor.TenantID) {
		return QueryPolicy{}, errors.New("worker policy invocation unavailable")
	}
	encodedQuery, err := meldbase.MarshalQuerySpecJSON(query)
	if err != nil {
		return QueryPolicy{}, err
	}
	callToken, err := randomToken16()
	if err != nil {
		return QueryPolicy{}, err
	}
	callID := hex.EncodeToString(callToken[:])
	evaluationContext, cancelEvaluation := context.WithTimeout(ctx, worker.hub.config.PolicyEvaluationTimeout)
	defer cancelEvaluation()
	pending := &workerPendingCall{policy: make(chan workerPolicyResult, 1)}
	worker.mu.Lock()
	if len(worker.pending) >= worker.hub.config.MaxPendingCalls {
		worker.mu.Unlock()
		worker.hub.stats.policyBusy.Add(1)
		return QueryPolicy{}, errRPCWorkerBusy
	}
	select {
	case <-worker.done:
		worker.mu.Unlock()
		return QueryPolicy{}, errors.New("worker disconnected")
	default:
	}
	worker.pending[callID] = pending
	worker.mu.Unlock()
	defer func() {
		worker.mu.Lock()
		delete(worker.pending, callID)
		worker.mu.Unlock()
	}()
	worker.hub.stats.policyEvaluations.Add(1)
	worker.hub.stats.policyActive.Add(1)
	defer worker.hub.stats.policyActive.Add(^uint64(0))
	if err := worker.send(evaluationContext, map[string]any{
		"v": protocolVersion, "type": "authorize_query", "callId": callID, "collection": publication.collection,
		"actor": map[string]any{"id": actor.ID, "tenantId": actor.TenantID},
		"query": json.RawMessage(encodedQuery),
	}); err != nil {
		worker.hub.stats.policyFailed.Add(1)
		return QueryPolicy{}, err
	}
	select {
	case result := <-pending.policy:
		if result.err != nil {
			if errors.Is(result.err, ErrForbidden) {
				worker.hub.stats.policyDenied.Add(1)
			} else {
				worker.hub.stats.policyFailed.Add(1)
			}
			return QueryPolicy{}, result.err
		}
		worker.hub.stats.policySucceeded.Add(1)
		return QueryPolicy{
			PolicyVersion: publication.policyVersion, Lease: publication.lease, Constraint: result.constraint,
			MaxResults:         publication.maxResults,
			AllowAllQueryPaths: publication.allowAllQueryPaths, AllowedQueryPaths: publication.allowedQueryPaths,
			AllowAllResultFields: publication.allowAllResultFields, AllowedResultFields: publication.allowedResultFields,
		}, nil
	case <-evaluationContext.Done():
		if ctx.Err() != nil {
			worker.hub.stats.policyCanceled.Add(1)
		} else {
			worker.hub.stats.policyFailed.Add(1)
		}
		cancelContext, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_ = worker.send(cancelContext, map[string]any{"v": protocolVersion, "type": "cancel", "callId": callID})
		cancel()
		return QueryPolicy{}, evaluationContext.Err()
	case <-worker.done:
		worker.hub.stats.policyFailed.Add(1)
		return QueryPolicy{}, errors.New("worker disconnected")
	}
}

func (worker *workerConnection) send(ctx context.Context, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > worker.hub.config.MaxFrameBytes {
		return errors.New("worker outbound frame exceeds limit")
	}
	worker.writeMu.Lock()
	defer worker.writeMu.Unlock()
	if err := worker.socket.Write(ctx, websocket.MessageText, encoded); err != nil {
		return err
	}
	worker.hub.stats.bytesSent.Add(uint64(len(encoded)))
	return nil
}

func (worker *workerConnection) readText(ctx context.Context) ([]byte, error) {
	messageType, raw, err := worker.socket.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText || len(raw) == 0 || len(raw) > worker.hub.config.MaxFrameBytes {
		return nil, workerProtocol("invalid worker frame")
	}
	worker.hub.stats.bytesReceived.Add(uint64(len(raw)))
	if err := meldbase.ValidateStrictJSON(raw, worker.hub.config.MaxFrameBytes); err != nil {
		return nil, workerProtocol("invalid worker JSON")
	}
	return raw, nil
}

func (worker *workerConnection) readLoop(ctx context.Context) error {
	for {
		raw, err := worker.readText(ctx)
		if err != nil {
			return err
		}
		var header struct {
			Version int    `json:"v"`
			Type    string `json:"type"`
		}
		if err := json.Unmarshal(raw, &header); err != nil || header.Version != protocolVersion {
			return workerProtocol("invalid worker frame header")
		}
		switch header.Type {
		case "result":
			if err := worker.handleResult(raw); err != nil {
				return err
			}
		case "error":
			if err := worker.handleError(raw); err != nil {
				return err
			}
		case "tx_op":
			if err := worker.handleTransactionOperation(ctx, raw); err != nil {
				return err
			}
		case "policy":
			if err := worker.handlePolicy(raw); err != nil {
				return err
			}
		case "policy_error":
			if err := worker.handlePolicyError(raw); err != nil {
				return err
			}
		default:
			return workerProtocol("unknown worker frame type")
		}
	}
}

type workerResultFrame struct {
	Version int             `json:"v"`
	Type    string          `json:"type"`
	CallID  string          `json:"callId"`
	Result  json.RawMessage `json:"result"`
}

func (worker *workerConnection) handleResult(raw []byte) error {
	var frame workerResultFrame
	if err := decodeStrict(raw, &frame); err != nil || frame.Version != protocolVersion || frame.Type != "result" || !workerOperationIDPattern.MatchString(frame.CallID) {
		return workerProtocol("invalid worker result")
	}
	value, err := meldbase.UnmarshalWireValue(frame.Result, meldbase.QueryLimits{})
	if err != nil {
		return workerProtocol("invalid typed worker result")
	}
	canonical, err := meldbase.MarshalWireValue(value)
	if err != nil || !bytes.Equal(canonical, frame.Result) {
		return workerProtocol("non-canonical worker result")
	}
	pending := worker.takePending(frame.CallID)
	if pending == nil || pending.result == nil {
		return workerProtocol("worker result has no active call")
	}
	pending.result <- workerCallResult{value: value}
	return nil
}

type workerErrorFrame struct {
	Version int    `json:"v"`
	Type    string `json:"type"`
	CallID  string `json:"callId"`
	Error   struct {
		Code string `json:"code"`
	} `json:"error"`
}

func (worker *workerConnection) handleError(raw []byte) error {
	var frame workerErrorFrame
	if err := decodeStrict(raw, &frame); err != nil || frame.Version != protocolVersion || frame.Type != "error" ||
		!workerOperationIDPattern.MatchString(frame.CallID) || !rpcErrorCodePattern.MatchString(frame.Error.Code) {
		return workerProtocol("invalid worker error")
	}
	pending := worker.takePending(frame.CallID)
	if pending == nil || pending.result == nil {
		return workerProtocol("worker error has no active call")
	}
	pending.result <- workerCallResult{err: &RPCError{Code: frame.Error.Code}}
	return nil
}

type workerPolicyFrame struct {
	Version    int             `json:"v"`
	Type       string          `json:"type"`
	CallID     string          `json:"callId"`
	Constraint json.RawMessage `json:"constraint"`
}

func (worker *workerConnection) handlePolicy(raw []byte) error {
	var frame workerPolicyFrame
	if err := decodeStrict(raw, &frame); err != nil || frame.Version != protocolVersion || frame.Type != "policy" ||
		!workerOperationIDPattern.MatchString(frame.CallID) || len(frame.Constraint) == 0 {
		return workerProtocol("invalid worker policy")
	}
	constraint, err := meldbase.DecodeQuerySpecJSON(frame.Constraint, worker.hub.config.PolicyQueryLimits)
	if err != nil || constraint.HasModifiers() {
		return workerProtocol("invalid worker policy constraint")
	}
	pending := worker.takePending(frame.CallID)
	if pending == nil || pending.policy == nil {
		return workerProtocol("worker policy has no active evaluation")
	}
	pending.policy <- workerPolicyResult{constraint: &constraint}
	return nil
}

func (worker *workerConnection) handlePolicyError(raw []byte) error {
	var frame workerErrorFrame
	if err := decodeStrict(raw, &frame); err != nil || frame.Version != protocolVersion || frame.Type != "policy_error" ||
		!workerOperationIDPattern.MatchString(frame.CallID) || (frame.Error.Code != "forbidden" && frame.Error.Code != "internal") {
		return workerProtocol("invalid worker policy error")
	}
	pending := worker.takePending(frame.CallID)
	if pending == nil || pending.policy == nil {
		return workerProtocol("worker policy error has no active evaluation")
	}
	if frame.Error.Code == "forbidden" {
		pending.policy <- workerPolicyResult{err: ErrForbidden}
	} else {
		pending.policy <- workerPolicyResult{err: errors.New("worker policy failed")}
	}
	return nil
}

func (worker *workerConnection) takePending(callID string) *workerPendingCall {
	worker.mu.Lock()
	pending := worker.pending[callID]
	if pending != nil {
		delete(worker.pending, callID)
	}
	worker.mu.Unlock()
	return pending
}

type workerTransactionOperationFrame struct {
	Version     int             `json:"v"`
	Type        string          `json:"type"`
	CallID      string          `json:"callId"`
	OperationID string          `json:"opId"`
	Operation   string          `json:"operation"`
	Collection  string          `json:"collection"`
	ID          string          `json:"id,omitempty"`
	Document    json.RawMessage `json:"document,omitempty"`
	Mutation    json.RawMessage `json:"mutation,omitempty"`
}

func (worker *workerConnection) handleTransactionOperation(ctx context.Context, raw []byte) error {
	var frame workerTransactionOperationFrame
	if err := decodeStrict(raw, &frame); err != nil || frame.Version != protocolVersion || frame.Type != "tx_op" ||
		!workerOperationIDPattern.MatchString(frame.CallID) || !workerOperationIDPattern.MatchString(frame.OperationID) {
		return workerProtocol("invalid worker transaction operation")
	}
	worker.mu.Lock()
	pending := worker.pending[frame.CallID]
	worker.mu.Unlock()
	if pending == nil || pending.tx == nil {
		return workerProtocol("transaction operation has no active transaction")
	}
	pending.opMu.Lock()
	defer pending.opMu.Unlock()
	if pending.operations >= worker.hub.config.MaxOperationsPerCall {
		return workerProtocol("worker transaction operation limit exceeded")
	}
	if _, duplicate := pending.seenOps[frame.OperationID]; duplicate {
		return workerProtocol("duplicate worker transaction operation")
	}
	pending.seenOps[frame.OperationID] = struct{}{}
	pending.operations++
	worker.hub.stats.transactionOps.Add(1)
	var result meldbase.Value
	var err error
	if frame.Operation == "invalidate_publication" {
		if frame.ID != "" || len(frame.Document) != 0 || len(frame.Mutation) != 0 {
			err = meldbase.ErrInvalidDocument
		} else {
			err = worker.hub.stagePolicyInvalidation(worker, pending.tx, frame.Collection)
			result = meldbase.Null()
		}
	} else {
		result, err = executeWorkerTransactionOperation(pending.tx, frame)
	}
	if err != nil {
		return worker.send(ctx, map[string]any{
			"v": protocolVersion, "type": "tx_error", "callId": frame.CallID, "opId": frame.OperationID,
			"error": map[string]any{"code": classifyWorkerTransactionError(err)},
		})
	}
	encoded, err := meldbase.MarshalWireValue(result)
	if err != nil {
		return err
	}
	return worker.send(ctx, map[string]any{
		"v": protocolVersion, "type": "tx_result", "callId": frame.CallID, "opId": frame.OperationID, "result": json.RawMessage(encoded),
	})
}

func executeWorkerTransactionOperation(tx *meldbase.WriteTransaction, frame workerTransactionOperationFrame) (meldbase.Value, error) {
	if tx == nil || !workerCollectionNamePattern.MatchString(frame.Collection) {
		return meldbase.Value{}, meldbase.ErrInvalidCollection
	}
	parseID := func() (meldbase.DocumentID, error) {
		if frame.ID == "" {
			return meldbase.DocumentID{}, meldbase.ErrInvalidDocument
		}
		return meldbase.ParseDocumentID(frame.ID)
	}
	parseDocument := func() (meldbase.Document, error) {
		if len(frame.Document) == 0 {
			return nil, meldbase.ErrInvalidDocument
		}
		value, err := meldbase.UnmarshalWireValue(frame.Document, meldbase.QueryLimits{})
		if err != nil {
			return nil, err
		}
		document, ok := value.ObjectValue()
		if !ok {
			return nil, meldbase.ErrInvalidDocument
		}
		return document, nil
	}
	switch frame.Operation {
	case "get":
		if len(frame.Document) != 0 || len(frame.Mutation) != 0 {
			return meldbase.Value{}, meldbase.ErrInvalidDocument
		}
		id, err := parseID()
		if err != nil {
			return meldbase.Value{}, err
		}
		document, err := tx.GetOne(frame.Collection, id)
		return meldbase.Object(document), err
	case "insert":
		if frame.ID != "" || len(frame.Mutation) != 0 {
			return meldbase.Value{}, meldbase.ErrInvalidDocument
		}
		document, err := parseDocument()
		if err != nil {
			return meldbase.Value{}, err
		}
		id, err := tx.InsertOne(frame.Collection, document)
		return meldbase.ID(id), err
	case "replace":
		if len(frame.Mutation) != 0 {
			return meldbase.Value{}, meldbase.ErrInvalidDocument
		}
		id, err := parseID()
		if err != nil {
			return meldbase.Value{}, err
		}
		document, err := parseDocument()
		if err != nil {
			return meldbase.Value{}, err
		}
		return meldbase.Null(), tx.ReplaceOne(frame.Collection, id, document)
	case "update":
		if len(frame.Document) != 0 || len(frame.Mutation) == 0 {
			return meldbase.Value{}, meldbase.ErrInvalidDocument
		}
		id, err := parseID()
		if err != nil {
			return meldbase.Value{}, err
		}
		mutation, err := meldbase.DecodeMutationSpecJSON(frame.Mutation, meldbase.QueryLimits{})
		if err != nil {
			return meldbase.Value{}, err
		}
		return meldbase.Null(), tx.UpdateOne(frame.Collection, id, mutation)
	case "delete":
		if len(frame.Document) != 0 || len(frame.Mutation) != 0 {
			return meldbase.Value{}, meldbase.ErrInvalidDocument
		}
		id, err := parseID()
		if err != nil {
			return meldbase.Value{}, err
		}
		return meldbase.Null(), tx.DeleteOne(frame.Collection, id)
	default:
		return meldbase.Value{}, meldbase.ErrInvalidDocument
	}
}

func classifyWorkerTransactionError(err error) string {
	switch {
	case errors.Is(err, meldbase.ErrNotFound):
		return "not_found"
	case errors.Is(err, meldbase.ErrDuplicateID):
		return "duplicate_id"
	case errors.Is(err, meldbase.ErrInvalidCollection):
		return "invalid_collection"
	case errors.Is(err, meldbase.ErrImmutableID):
		return "immutable_id"
	case errors.Is(err, meldbase.ErrInvalidUpdate):
		return "invalid_update"
	case errors.Is(err, ErrForbidden):
		return "forbidden"
	case errors.Is(err, errPolicyInvalidationUnavailable):
		return "policy_invalidation_unavailable"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	default:
		return "invalid_document"
	}
}

func (worker *workerConnection) fail(err error) {
	worker.close.Do(func() {
		close(worker.done)
		worker.mu.Lock()
		pending := worker.pending
		worker.pending = make(map[string]*workerPendingCall)
		worker.mu.Unlock()
		for _, call := range pending {
			if call.result != nil {
				select {
				case call.result <- workerCallResult{err: err}:
				default:
				}
			}
			if call.policy != nil {
				select {
				case call.policy <- workerPolicyResult{err: err}:
				default:
				}
			}
		}
		_ = worker.socket.CloseNow()
	})
}
