// Package anchorhttp provides an authenticated HTTPS quorum implementation of
// meldbase.RollbackAnchorStore. It is a reference trust-plane building block,
// not a replacement for independently operated failure domains.
package anchorhttp

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/crapthings/meldbase/core"
)

const (
	// ProtocolVersion identifies the strict HTTP/JSON and signing contract.
	ProtocolVersion      uint32 = 2
	anchorPathPrefix            = "/store/anchors/"
	defaultMaxClockSkew         = 30 * time.Second
	maximumBodyBytes     int64  = 4096
	nodeManifestName            = ".meldbase-anchor-node.json"
	nodeManifestLockName        = ".meldbase-anchor-node.lock"
	nodeManifestVersion  uint32 = 1
)

var (
	ErrAuthentication    = errors.New("meldbase anchor HTTP: authentication failed")
	ErrConfiguration     = errors.New("meldbase anchor HTTP: configuration or member mismatch")
	ErrConflict          = errors.New("meldbase anchor HTTP: monotonic anchor conflict")
	ErrInsecureTransport = errors.New("meldbase anchor HTTP: HTTPS is required")
	ErrProtocol          = errors.New("meldbase anchor HTTP: invalid protocol response")
	ErrQuorum            = errors.New("meldbase anchor HTTP: quorum unavailable")
)

type wireAnchor struct {
	Version               uint32 `json:"version"`
	DatabaseID            string `json:"databaseId"`
	MinimumCommitSequence uint64 `json:"minimumCommitSequence"`
	MinimumGeneration     uint64 `json:"minimumGeneration"`
}

type HandlerOptions struct {
	Directory    string
	ClusterID    string
	Members      []string
	MemberID     string
	Keys         map[string][]byte
	MaxClockSkew time.Duration
}

type Handler struct {
	directory       string
	configurationID string
	memberID        string
	keys            map[string][]byte
	dummyKey        []byte
	clockSkew       time.Duration
	now             func() time.Time
}

// NewHandler creates a multi-anchor HTTP handler backed by an existing trusted
// directory. Deploy it behind TLS; request authentication does not encrypt
// traffic or authenticate the server to the client.
func NewHandler(options HandlerOptions) (*Handler, error) {
	configurationID, err := configurationDigest(options.ClusterID, options.Members, options.MemberID)
	if err != nil {
		return nil, err
	}
	if len(options.Keys) < 1 || len(options.Keys) > 16 {
		return nil, errors.New("meldbase anchor HTTP: Keys must contain between 1 and 16 entries")
	}
	keys := make(map[string][]byte, len(options.Keys))
	for keyID, key := range options.Keys {
		if !validKeyID(keyID) || len(key) < 32 {
			return nil, errors.New("meldbase anchor HTTP: every key needs a safe ID and at least 32 bytes")
		}
		keys[keyID] = append([]byte(nil), key...)
	}
	directory, err := filepath.Abs(filepath.Clean(options.Directory))
	if err != nil || options.Directory == "" {
		return nil, errors.Join(err, errors.New("meldbase anchor HTTP: invalid directory"))
	}
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		return nil, errors.Join(err, errors.New("meldbase anchor HTTP: directory must already exist"))
	}
	skew := options.MaxClockSkew
	if skew == 0 {
		skew = defaultMaxClockSkew
	}
	if skew < time.Second || skew > 10*time.Minute {
		return nil, errors.New("meldbase anchor HTTP: MaxClockSkew must be between 1s and 10m")
	}
	if err := bindNodeDirectory(directory, configurationID, options.MemberID); err != nil {
		return nil, err
	}
	dummyKey := sha256.Sum256([]byte("meldbase-anchor-http-unknown-key-store"))
	return &Handler{directory: directory, configurationID: configurationID, memberID: options.MemberID, keys: keys, dummyKey: dummyKey[:], clockSkew: skew, now: time.Now}, nil
}

func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	if handler == nil || request == nil || (request.Method != http.MethodGet && request.Method != http.MethodPut) {
		writeError(response, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	name, ok := anchorNameFromPath(request.URL.Path)
	if !ok || request.URL.RawQuery != "" || request.URL.EscapedPath() != request.URL.Path {
		writeError(response, http.StatusNotFound, "not_found")
		return
	}
	bodyReader := io.Reader(http.NoBody)
	if request.Body != nil {
		bodyReader = request.Body
	}
	body, err := io.ReadAll(io.LimitReader(bodyReader, maximumBodyBytes+1))
	if err != nil || int64(len(body)) > maximumBodyBytes || (request.Method == http.MethodGet && len(body) != 0) {
		writeError(response, http.StatusBadRequest, "invalid_body")
		return
	}
	if err := handler.authenticate(request, body); errors.Is(err, ErrConfiguration) {
		writeError(response, http.StatusPreconditionFailed, "configuration_mismatch")
		return
	} else if err != nil {
		writeError(response, http.StatusUnauthorized, "unauthorized")
		return
	}
	store, err := meldbase.NewFileRollbackAnchorStore(filepath.Join(handler.directory, name+".anchor"))
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, "storage_unavailable")
		return
	}
	if request.Method == http.MethodGet {
		handler.load(response, request, store)
		return
	}
	handler.advance(response, request, store, body)
}

func (handler *Handler) authenticate(request *http.Request, body []byte) error {
	if len(request.Header.Values("Meldbase-Anchor-Configuration-ID")) != 1 || len(request.Header.Values("Meldbase-Anchor-Member-ID")) != 1 || len(request.Header.Values("Meldbase-Anchor-Key-ID")) != 1 || len(request.Header.Values("Meldbase-Anchor-Timestamp")) != 1 || len(request.Header.Values("Meldbase-Anchor-Signature")) != 1 {
		return ErrAuthentication
	}
	configurationID := request.Header.Get("Meldbase-Anchor-Configuration-ID")
	memberID := request.Header.Get("Meldbase-Anchor-Member-ID")
	keyID := request.Header.Get("Meldbase-Anchor-Key-ID")
	timestampText := request.Header.Get("Meldbase-Anchor-Timestamp")
	signatureText := request.Header.Get("Meldbase-Anchor-Signature")
	if len(configurationID) != sha256.Size*2 || configurationID != strings.ToLower(configurationID) || !validIdentifier(memberID, 128) || !validKeyID(keyID) || len(timestampText) < 1 || len(timestampText) > 20 {
		return ErrAuthentication
	}
	timestamp, err := strconv.ParseInt(timestampText, 10, 64)
	if err != nil || len(signatureText) != sha256.Size*2 || signatureText != strings.ToLower(signatureText) {
		return ErrAuthentication
	}
	observed := time.UnixMilli(timestamp)
	delta := handler.now().Sub(observed)
	if delta < 0 {
		delta = -delta
	}
	if delta > handler.clockSkew {
		return ErrAuthentication
	}
	key, knownKey := handler.keys[keyID]
	if !knownKey {
		key = handler.dummyKey
	}
	want := requestSignature(key, request.Method, request.URL.EscapedPath(), configurationID, memberID, keyID, timestampText, body)
	got, err := hex.DecodeString(signatureText)
	if err != nil || !knownKey || !hmac.Equal(got, want) {
		return ErrAuthentication
	}
	if configurationID != handler.configurationID || memberID != handler.memberID {
		return ErrConfiguration
	}
	return nil
}

func (handler *Handler) load(response http.ResponseWriter, request *http.Request, store meldbase.RollbackAnchorStore) {
	anchor, exists, err := store.Load(request.Context())
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, "storage_unavailable")
		return
	}
	if !exists {
		writeError(response, http.StatusNotFound, "not_found")
		return
	}
	response.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(response).Encode(encodeAnchor(anchor))
}

func (handler *Handler) advance(response http.ResponseWriter, request *http.Request, store meldbase.RollbackAnchorStore, body []byte) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var record wireAnchor
	if err := decoder.Decode(&record); err != nil || !decoderAtEOF(decoder) {
		writeError(response, http.StatusBadRequest, "invalid_anchor")
		return
	}
	anchor, err := decodeAnchor(record)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_anchor")
		return
	}
	current, exists, err := store.Load(request.Context())
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, "storage_unavailable")
		return
	}
	if exists && (current.DatabaseID != anchor.DatabaseID || current.MinimumCommitSequence > anchor.MinimumCommitSequence || current.MinimumGeneration > anchor.MinimumGeneration) {
		writeError(response, http.StatusConflict, "anchor_conflict")
		return
	}
	if err := store.Advance(request.Context(), anchor); err != nil {
		retained, retainedExists, loadErr := store.Load(request.Context())
		if loadErr == nil && retainedExists && (retained.DatabaseID != anchor.DatabaseID || retained.MinimumCommitSequence > anchor.MinimumCommitSequence || retained.MinimumGeneration > anchor.MinimumGeneration) {
			writeError(response, http.StatusConflict, "anchor_conflict")
			return
		}
		writeError(response, http.StatusServiceUnavailable, "storage_unavailable")
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

// Replica binds one endpoint URL to the unique static member expected behind
// it. Distinct URLs with the same MemberID cannot manufacture a quorum.
type Replica struct {
	Endpoint string
	MemberID string
}

// ReplicaCheckState is a fixed qualification classification for one configured
// static member. It contains no endpoint or database identity.
type ReplicaCheckState string

const (
	ReplicaAvailable      ReplicaCheckState = "available"
	ReplicaMissing        ReplicaCheckState = "missing"
	ReplicaUnavailable    ReplicaCheckState = "unavailable"
	ReplicaAuthentication ReplicaCheckState = "authentication-failure"
	ReplicaConfiguration  ReplicaCheckState = "configuration-failure"
	ReplicaProtocol       ReplicaCheckState = "protocol-failure"
)

// ReplicaCheck reports one full-membership qualification probe. Anchor is set
// only when State is ReplicaAvailable and Exists is true.
type ReplicaCheck struct {
	MemberID string
	State    ReplicaCheckState
	Exists   bool
	Anchor   meldbase.RollbackAnchor
}

type QuorumOptions struct {
	ClusterID         string
	Replicas          []Replica
	AnchorName        string
	KeyID             string
	SharedKey         []byte
	Client            *http.Client
	AllowInsecureHTTP bool
}

type QuorumStore struct {
	replicas        []Replica
	configurationID string
	anchorName      string
	keyID           string
	key             []byte
	client          *http.Client
	now             func() time.Time
	metrics         quorumMetrics
}

type quorumMetrics struct {
	loads                  atomic.Uint64
	advances               atomic.Uint64
	endpointFailures       atomic.Uint64
	quorumFailures         atomic.Uint64
	conflicts              atomic.Uint64
	authenticationFailures atomic.Uint64
	protocolFailures       atomic.Uint64
	configurationFailures  atomic.Uint64
}

// NewQuorumStore creates a majority-read/majority-write anchor store. Endpoint
// count must be one or an odd number of at least three, and duplicate endpoints
// are rejected so they cannot manufacture a quorum.
func NewQuorumStore(options QuorumOptions) (*QuorumStore, error) {
	if !validAnchorName(options.AnchorName) {
		return nil, errors.New("meldbase anchor HTTP: invalid AnchorName")
	}
	if len(options.SharedKey) < 32 {
		return nil, errors.New("meldbase anchor HTTP: SharedKey must contain at least 32 bytes")
	}
	if !validKeyID(options.KeyID) {
		return nil, errors.New("meldbase anchor HTTP: invalid KeyID")
	}
	if len(options.Replicas) != 1 && (len(options.Replicas) < 3 || len(options.Replicas)%2 == 0) {
		return nil, errors.New("meldbase anchor HTTP: Replicas must contain one or an odd number of at least three servers")
	}
	seenEndpoints := make(map[string]struct{}, len(options.Replicas))
	seenMembers := make(map[string]struct{}, len(options.Replicas))
	replicas := make([]Replica, 0, len(options.Replicas))
	members := make([]string, 0, len(options.Replicas))
	for _, replica := range options.Replicas {
		if !validIdentifier(replica.MemberID, 128) {
			return nil, errors.New("meldbase anchor HTTP: invalid replica MemberID")
		}
		raw := replica.Endpoint
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			return nil, errors.New("meldbase anchor HTTP: invalid endpoint")
		}
		if parsed.Scheme != "https" && !(options.AllowInsecureHTTP && parsed.Scheme == "http") {
			return nil, ErrInsecureTransport
		}
		normalized := strings.TrimSuffix(parsed.String(), "/")
		if _, duplicate := seenEndpoints[normalized]; duplicate {
			return nil, errors.New("meldbase anchor HTTP: duplicate endpoint")
		}
		if _, duplicate := seenMembers[replica.MemberID]; duplicate {
			return nil, errors.New("meldbase anchor HTTP: duplicate member ID")
		}
		seenEndpoints[normalized] = struct{}{}
		seenMembers[replica.MemberID] = struct{}{}
		members = append(members, replica.MemberID)
		replicas = append(replicas, Replica{Endpoint: normalized, MemberID: replica.MemberID})
	}
	configurationID, err := configurationDigest(options.ClusterID, members, "")
	if err != nil {
		return nil, err
	}
	client := options.Client
	if client == nil {
		client = http.DefaultClient
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &QuorumStore{replicas: replicas, configurationID: configurationID, anchorName: options.AnchorName, keyID: options.KeyID, key: append([]byte(nil), options.SharedKey...), client: &clientCopy, now: time.Now}, nil
}

func (store *QuorumStore) Load(ctx context.Context) (meldbase.RollbackAnchor, bool, error) {
	if store == nil || ctx == nil {
		return meldbase.RollbackAnchor{}, false, ErrQuorum
	}
	store.metrics.loads.Add(1)
	operation, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan loadResult, len(store.replicas))
	for _, replica := range store.replicas {
		go func(replica Replica) { results <- store.loadOne(operation, replica) }(replica)
	}
	quorum := len(store.replicas)/2 + 1
	successes := make([]loadResult, 0, quorum)
	for received := 0; received < len(store.replicas); received++ {
		select {
		case <-ctx.Done():
			store.metrics.quorumFailures.Add(1)
			return meldbase.RollbackAnchor{}, false, errors.Join(ErrQuorum, ctx.Err())
		case result := <-results:
			if errors.Is(result.err, ErrAuthentication) || errors.Is(result.err, ErrConfiguration) || errors.Is(result.err, ErrProtocol) {
				cancel()
				if errors.Is(result.err, ErrConfiguration) {
					store.metrics.configurationFailures.Add(1)
				} else if errors.Is(result.err, ErrAuthentication) {
					store.metrics.authenticationFailures.Add(1)
				} else {
					store.metrics.protocolFailures.Add(1)
				}
				return meldbase.RollbackAnchor{}, false, result.err
			}
			if result.err == nil {
				successes = append(successes, result)
				if anchor, exists, ready := selectLoadQuorum(successes, quorum); ready {
					cancel()
					return anchor, exists, nil
				}
			} else {
				store.metrics.endpointFailures.Add(1)
			}
			if len(successes)+len(store.replicas)-received-1 < quorum {
				store.metrics.quorumFailures.Add(1)
				return meldbase.RollbackAnchor{}, false, ErrQuorum
			}
		}
	}
	if len(successes) >= quorum {
		store.metrics.conflicts.Add(1)
		return meldbase.RollbackAnchor{}, false, ErrConflict
	}
	store.metrics.quorumFailures.Add(1)
	return meldbase.RollbackAnchor{}, false, ErrQuorum
}

func (store *QuorumStore) Advance(ctx context.Context, anchor meldbase.RollbackAnchor) error {
	if store == nil || ctx == nil {
		return ErrQuorum
	}
	store.metrics.advances.Add(1)
	if anchor.DatabaseID == ([16]byte{}) || anchor.MinimumGeneration == 0 {
		store.metrics.protocolFailures.Add(1)
		return ErrProtocol
	}
	record, err := json.Marshal(encodeAnchor(anchor))
	if err != nil {
		return errors.Join(ErrProtocol, err)
	}
	operation, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(store.replicas))
	for _, replica := range store.replicas {
		go func(replica Replica) { results <- store.advanceOne(operation, replica, record) }(replica)
	}
	quorum := len(store.replicas)/2 + 1
	succeeded := 0
	conflicts := 0
	for received := 0; received < len(store.replicas); received++ {
		select {
		case <-ctx.Done():
			store.metrics.quorumFailures.Add(1)
			return errors.Join(ErrQuorum, ctx.Err())
		case result := <-results:
			if errors.Is(result, ErrAuthentication) || errors.Is(result, ErrConfiguration) || errors.Is(result, ErrProtocol) {
				cancel()
				switch {
				case errors.Is(result, ErrConfiguration):
					store.metrics.configurationFailures.Add(1)
				case errors.Is(result, ErrAuthentication):
					store.metrics.authenticationFailures.Add(1)
				default:
					store.metrics.protocolFailures.Add(1)
				}
				return result
			}
			if errors.Is(result, ErrConflict) {
				conflicts++
				store.metrics.conflicts.Add(1)
			} else if result == nil {
				succeeded++
				if succeeded == quorum {
					cancel()
					return nil
				}
			} else {
				store.metrics.endpointFailures.Add(1)
			}
			if succeeded+len(store.replicas)-received-1 < quorum {
				store.metrics.quorumFailures.Add(1)
				if conflicts > 0 {
					return ErrConflict
				}
				return ErrQuorum
			}
		}
	}
	store.metrics.quorumFailures.Add(1)
	return ErrQuorum
}

// RollbackAnchorStatus returns a lock-free, identity-free process-session
// snapshot suitable for Meldbase admin telemetry.
func (store *QuorumStore) RollbackAnchorStatus() meldbase.RollbackAnchorStoreStatus {
	if store == nil {
		return meldbase.RollbackAnchorStoreStatus{}
	}
	return meldbase.RollbackAnchorStoreStatus{
		Replicas: uint64(len(store.replicas)), Quorum: uint64(len(store.replicas)/2 + 1),
		Loads: store.metrics.loads.Load(), Advances: store.metrics.advances.Load(),
		EndpointFailures: store.metrics.endpointFailures.Load(), QuorumFailures: store.metrics.quorumFailures.Load(),
		Conflicts: store.metrics.conflicts.Load(), AuthenticationFailures: store.metrics.authenticationFailures.Load(),
		ProtocolFailures:      store.metrics.protocolFailures.Load(),
		ConfigurationFailures: store.metrics.configurationFailures.Load(),
	}
}

// ConfigurationID returns the non-secret digest of the cluster and complete
// sorted static member list.
func (store *QuorumStore) ConfigurationID() string {
	if store == nil {
		return ""
	}
	return store.configurationID
}

// CheckReplicas waits for a signed GET result from every configured member.
// It is intended for qualification and operator diagnostics, not commit paths.
func (store *QuorumStore) CheckReplicas(ctx context.Context) ([]ReplicaCheck, error) {
	if store == nil || ctx == nil {
		return nil, ErrQuorum
	}
	type checkedResult struct {
		index  int
		result loadResult
	}
	results := make(chan checkedResult, len(store.replicas))
	for index, replica := range store.replicas {
		go func(index int, replica Replica) {
			results <- checkedResult{index: index, result: store.loadOne(ctx, replica)}
		}(index, replica)
	}
	checks := make([]ReplicaCheck, len(store.replicas))
	for index, replica := range store.replicas {
		checks[index].MemberID = replica.MemberID
	}
	for range store.replicas {
		checked := <-results
		result := checked.result
		check := &checks[checked.index]
		switch {
		case result.err == nil && result.exists:
			check.State, check.Exists, check.Anchor = ReplicaAvailable, true, result.anchor
		case result.err == nil:
			check.State = ReplicaMissing
		case errors.Is(result.err, ErrAuthentication):
			check.State = ReplicaAuthentication
		case errors.Is(result.err, ErrConfiguration):
			check.State = ReplicaConfiguration
		case errors.Is(result.err, ErrProtocol):
			check.State = ReplicaProtocol
		default:
			check.State = ReplicaUnavailable
		}
	}
	return checks, nil
}

type loadResult struct {
	anchor meldbase.RollbackAnchor
	exists bool
	err    error
}

func (store *QuorumStore) loadOne(ctx context.Context, replica Replica) loadResult {
	request, err := store.request(ctx, http.MethodGet, replica, nil)
	if err != nil {
		return loadResult{err: err}
	}
	response, err := store.client.Do(request)
	if err != nil {
		return loadResult{err: err}
	}
	defer response.Body.Close()
	defer io.Copy(io.Discard, io.LimitReader(response.Body, maximumBodyBytes+1))
	if response.StatusCode == http.StatusNotFound {
		return loadResult{}
	}
	if response.StatusCode == http.StatusUnauthorized {
		return loadResult{err: ErrAuthentication}
	}
	if response.StatusCode == http.StatusPreconditionFailed {
		return loadResult{err: ErrConfiguration}
	}
	if response.StatusCode == http.StatusServiceUnavailable || response.StatusCode == http.StatusGatewayTimeout || response.StatusCode == http.StatusBadGateway {
		return loadResult{err: errors.New("meldbase anchor HTTP: endpoint unavailable")}
	}
	if response.StatusCode != http.StatusOK {
		return loadResult{err: ErrProtocol}
	}
	var record wireAnchor
	if err := decodeBoundedJSON(response.Body, &record); err != nil {
		return loadResult{err: errors.Join(ErrProtocol, err)}
	}
	anchor, err := decodeAnchor(record)
	return loadResult{anchor: anchor, exists: err == nil, err: err}
}

func (store *QuorumStore) advanceOne(ctx context.Context, replica Replica, body []byte) error {
	request, err := store.request(ctx, http.MethodPut, replica, body)
	if err != nil {
		return err
	}
	response, err := store.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maximumBodyBytes+1))
	switch response.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusConflict:
		return ErrConflict
	case http.StatusUnauthorized:
		return ErrAuthentication
	case http.StatusPreconditionFailed:
		return ErrConfiguration
	case http.StatusBadRequest:
		return ErrProtocol
	case http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusBadGateway:
		return errors.New("meldbase anchor HTTP: endpoint unavailable")
	default:
		return fmt.Errorf("meldbase anchor HTTP: unexpected status %d", response.StatusCode)
	}
}

func (store *QuorumStore) request(ctx context.Context, method string, replica Replica, body []byte) (*http.Request, error) {
	path := anchorPathPrefix + store.anchorName
	request, err := http.NewRequestWithContext(ctx, method, replica.Endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	timestamp := strconv.FormatInt(store.now().UnixMilli(), 10)
	request.Header.Set("Meldbase-Anchor-Configuration-ID", store.configurationID)
	request.Header.Set("Meldbase-Anchor-Member-ID", replica.MemberID)
	request.Header.Set("Meldbase-Anchor-Key-ID", store.keyID)
	request.Header.Set("Meldbase-Anchor-Timestamp", timestamp)
	request.Header.Set("Meldbase-Anchor-Signature", hex.EncodeToString(requestSignature(store.key, method, path, store.configurationID, replica.MemberID, store.keyID, timestamp, body)))
	if method == http.MethodPut {
		request.Header.Set("Content-Type", "application/json")
	}
	return request, nil
}

func mergeLoadQuorum(results []loadResult) (meldbase.RollbackAnchor, bool, error) {
	if selected, exists, ready := selectLoadQuorum(results, len(results)); ready {
		return selected, exists, nil
	}
	return meldbase.RollbackAnchor{}, false, ErrConflict
}

// selectLoadQuorum finds a successful subset with an observed maximum. Lower
// members need not be mutually comparable when the maximum dominates them all.
// This lets a clean majority make progress past a crossed minority without ever
// inventing a coordinate or accepting a sub-majority floor.
func selectLoadQuorum(results []loadResult, quorum int) (meldbase.RollbackAnchor, bool, bool) {
	if quorum < 1 || len(results) < quorum {
		return meldbase.RollbackAnchor{}, false, false
	}
	missing := 0
	for _, result := range results {
		if !result.exists {
			missing++
		}
	}
	qualified := make([]meldbase.RollbackAnchor, 0, len(results))
	for _, candidate := range results {
		if !candidate.exists {
			continue
		}
		dominated := missing
		for _, result := range results {
			if !result.exists {
				continue
			}
			if anchorBeforeOrEqual(result.anchor, candidate.anchor) {
				dominated++
			}
		}
		if dominated >= quorum {
			qualified = append(qualified, candidate.anchor)
		}
	}
	for _, candidate := range qualified {
		dominatesAll := true
		for _, other := range qualified {
			if !anchorBeforeOrEqual(other, candidate) {
				dominatesAll = false
				break
			}
		}
		if dominatesAll {
			return candidate, true, true
		}
	}
	if missing >= quorum {
		return meldbase.RollbackAnchor{}, false, true
	}
	return meldbase.RollbackAnchor{}, false, false
}

func anchorBeforeOrEqual(left, right meldbase.RollbackAnchor) bool {
	return left.DatabaseID == right.DatabaseID &&
		left.MinimumCommitSequence <= right.MinimumCommitSequence &&
		left.MinimumGeneration <= right.MinimumGeneration
}

func encodeAnchor(anchor meldbase.RollbackAnchor) wireAnchor {
	return wireAnchor{Version: ProtocolVersion, DatabaseID: hex.EncodeToString(anchor.DatabaseID[:]), MinimumCommitSequence: anchor.MinimumCommitSequence, MinimumGeneration: anchor.MinimumGeneration}
}

func decodeAnchor(record wireAnchor) (meldbase.RollbackAnchor, error) {
	if record.Version != ProtocolVersion || len(record.DatabaseID) != 32 || record.DatabaseID != strings.ToLower(record.DatabaseID) || record.MinimumGeneration == 0 {
		return meldbase.RollbackAnchor{}, ErrProtocol
	}
	identity, err := hex.DecodeString(record.DatabaseID)
	if err != nil || len(identity) != 16 {
		return meldbase.RollbackAnchor{}, ErrProtocol
	}
	var anchor meldbase.RollbackAnchor
	copy(anchor.DatabaseID[:], identity)
	if anchor.DatabaseID == ([16]byte{}) {
		return meldbase.RollbackAnchor{}, ErrProtocol
	}
	anchor.MinimumCommitSequence = record.MinimumCommitSequence
	anchor.MinimumGeneration = record.MinimumGeneration
	return anchor, nil
}

func requestSignature(key []byte, method, escapedPath, configurationID, memberID, keyID, timestamp string, body []byte) []byte {
	digest := sha256.Sum256(body)
	canonical := method + "\n" + escapedPath + "\n" + configurationID + "\n" + memberID + "\n" + keyID + "\n" + timestamp + "\n" + hex.EncodeToString(digest[:]) + "\n"
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(canonical))
	return mac.Sum(nil)
}

func anchorNameFromPath(path string) (string, bool) {
	if !strings.HasPrefix(path, anchorPathPrefix) {
		return "", false
	}
	name := strings.TrimPrefix(path, anchorPathPrefix)
	return name, validAnchorName(name)
}

func validAnchorName(name string) bool {
	if len(name) < 1 || len(name) > 128 {
		return false
	}
	for _, character := range name {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return name != "." && name != ".."
}

func validKeyID(value string) bool {
	return len(value) <= 64 && validAnchorName(value)
}

func validIdentifier(value string, maximum int) bool {
	return len(value) <= maximum && validAnchorName(value)
}

func configurationDigest(clusterID string, members []string, requiredMember string) (string, error) {
	if !validIdentifier(clusterID, 128) || (len(members) != 1 && (len(members) < 3 || len(members)%2 == 0)) {
		return "", errors.New("meldbase anchor HTTP: ClusterID and a one-or-odd static member list are required")
	}
	ordered := append([]string(nil), members...)
	sort.Strings(ordered)
	foundRequired := requiredMember == ""
	for index, member := range ordered {
		if !validIdentifier(member, 128) || (index > 0 && member == ordered[index-1]) {
			return "", errors.New("meldbase anchor HTTP: members need unique safe identifiers")
		}
		if member == requiredMember {
			foundRequired = true
		}
	}
	if !foundRequired {
		return "", errors.New("meldbase anchor HTTP: MemberID is not present in Members")
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("meldbase-anchor-http-static-configuration-store\x00"))
	_, _ = hash.Write([]byte(clusterID))
	for _, member := range ordered {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(member))
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

type nodeManifest struct {
	Version         uint32 `json:"version"`
	ConfigurationID string `json:"configurationId"`
	MemberID        string `json:"memberId"`
	Checksum        string `json:"checksum"`
}

func bindNodeDirectory(directory, configurationID, memberID string) error {
	lock, err := os.OpenFile(filepath.Join(directory, nodeManifestLockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("%w: open node manifest lock: %v", ErrConfiguration, err)
	}
	defer lock.Close()
	if info, statErr := lock.Stat(); statErr != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: invalid node manifest lock", ErrConfiguration)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("%w: lock node manifest: %v", ErrConfiguration, err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

	path := filepath.Join(directory, nodeManifestName)
	info, err := os.Lstat(path)
	if err == nil {
		if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximumBodyBytes {
			return fmt.Errorf("%w: invalid node manifest file", ErrConfiguration)
		}
		file, openErr := os.Open(path)
		if openErr != nil {
			return fmt.Errorf("%w: open node manifest: %v", ErrConfiguration, openErr)
		}
		openedInfo, statErr := file.Stat()
		if statErr != nil || !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
			_ = file.Close()
			return fmt.Errorf("%w: node manifest changed while opening", ErrConfiguration)
		}
		var manifest nodeManifest
		decodeErr := decodeBoundedJSON(file, &manifest)
		closeErr := file.Close()
		if decodeErr != nil || closeErr != nil || manifest.Version != nodeManifestVersion || manifest.Checksum != nodeManifestChecksum(manifest.ConfigurationID, manifest.MemberID) {
			return fmt.Errorf("%w: corrupt node manifest", ErrConfiguration)
		}
		if manifest.ConfigurationID != configurationID || manifest.MemberID != memberID {
			return ErrConfiguration
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: inspect node manifest: %v", ErrConfiguration, err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("%w: inspect node directory: %v", ErrConfiguration, err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".anchor") {
			return fmt.Errorf("%w: unbound directory already contains anchors", ErrConfiguration)
		}
	}
	manifest := nodeManifest{Version: nodeManifestVersion, ConfigurationID: configurationID, MemberID: memberID, Checksum: nodeManifestChecksum(configurationID, memberID)}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: encode node manifest: %v", ErrConfiguration, err)
	}
	raw = append(raw, '\n')
	temporary, err := os.CreateTemp(directory, ".meldbase-anchor-node.tmp-")
	if err != nil {
		return fmt.Errorf("%w: create node manifest: %v", ErrConfiguration, err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() { _ = temporary.Close(); _ = os.Remove(temporaryPath) }
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()
		return fmt.Errorf("%w: secure node manifest: %v", ErrConfiguration, err)
	}
	written, writeErr := temporary.Write(raw)
	if writeErr == nil && written != len(raw) {
		writeErr = io.ErrShortWrite
	}
	writeErr = errors.Join(writeErr, temporary.Sync(), temporary.Close())
	if writeErr != nil {
		cleanup()
		return fmt.Errorf("%w: persist node manifest: %v", ErrConfiguration, writeErr)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		cleanup()
		return fmt.Errorf("%w: publish node manifest: %v", ErrConfiguration, err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("%w: open node directory: %v", ErrConfiguration, err)
	}
	err = errors.Join(directoryFile.Sync(), directoryFile.Close())
	if err != nil {
		return fmt.Errorf("%w: sync node directory: %v", ErrConfiguration, err)
	}
	return nil
}

func nodeManifestChecksum(configurationID, memberID string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("meldbase-anchor-http-node-manifest-v1\x00"))
	_, _ = hash.Write([]byte(configurationID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(memberID))
	return hex.EncodeToString(hash.Sum(nil))
}

func decoderAtEOF(decoder *json.Decoder) bool {
	var trailing any
	return errors.Is(decoder.Decode(&trailing), io.EOF)
}

func decodeBoundedJSON(reader io.Reader, target any) error {
	raw, err := io.ReadAll(io.LimitReader(reader, maximumBodyBytes+1))
	if err != nil || int64(len(raw)) > maximumBodyBytes {
		return errors.Join(ErrProtocol, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if !decoderAtEOF(decoder) {
		return ErrProtocol
	}
	return nil
}

func writeError(response http.ResponseWriter, status int, code string) {
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(struct {
		Version uint32 `json:"version"`
		Code    string `json:"code"`
	}{ProtocolVersion, code})
}

var _ meldbase.RollbackAnchorStore = (*QuorumStore)(nil)
var _ meldbase.RollbackAnchorStatusProvider = (*QuorumStore)(nil)

func (store *QuorumStore) String() string {
	if store == nil {
		return "anchorhttp.QuorumStore<nil>"
	}
	return fmt.Sprintf("anchorhttp.QuorumStore{%d replicas, quorum %d}", len(store.replicas), len(store.replicas)/2+1)
}
