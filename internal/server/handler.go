package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/crapthings/meldbase/core"
)

type Config struct {
	DB                             *meldbase.DB
	Authenticator                  Authenticator
	Authorizer                     Authorizer
	QueryPolicyResolver            QueryPolicyResolver
	PublicRealtimeURL              string
	OriginPatterns                 []string
	AllowedHTTPOrigins             []string
	TicketTTL                      time.Duration
	ResumeTokenKey                 []byte
	ResumeTokenTTL                 time.Duration
	MaxBodyBytes                   int
	MaxQueryResultBytes            int
	MaxRealtimeFrameBytes          int
	MaxRealtimeOutboundBytes       int
	MaxSubscriptionsPerConnection  int
	QueryLimits                    meldbase.QueryLimits
	ReplaySource                   meldbase.QueryReplaySource
	RPCMethods                     map[string]RPCMethod
	RPCTransactionalMethods        map[string]RPCTransactionalMethod
	RPCMethodResolver              RPCMethodResolver
	RPCTransactionalMethodResolver RPCTransactionalMethodResolver
	RPCAuthorizer                  RPCAuthorizer
	MaxConcurrentRPC               int
	MaxRPCPerConnection            int
	MaxRPCArguments                int
	MaxRPCResultBytes              int
	RPCIdempotencyStore            RPCIdempotencyStore
	RPCIdempotencyRetention        time.Duration
	RPCIdempotencyCommitTimeout    time.Duration
}

type Handler struct {
	config           Config
	mux              *http.ServeMux
	tickets          *ticketStore
	resume           *resumeTokenService
	rpcSlots         chan struct{}
	startedAt        time.Time
	rpcSessionID     [16]byte
	operationalState func() meldbase.OperationalState
	metrics          serverMetrics
}

func New(config Config) (*Handler, error) {
	if config.DB == nil || config.Authenticator == nil || config.Authorizer == nil {
		return nil, errors.New("database, authenticator, and authorizer are required")
	}
	realtimeURL, err := url.Parse(config.PublicRealtimeURL)
	if err != nil || (realtimeURL.Scheme != "ws" && realtimeURL.Scheme != "wss") || realtimeURL.Host == "" {
		return nil, errors.New("valid public ws(s) realtime URL is required")
	}
	if config.TicketTTL <= 0 {
		config.TicketTTL = 30 * time.Second
	}
	if config.TicketTTL > 5*time.Minute {
		return nil, errors.New("ticket TTL exceeds five minutes")
	}
	if config.ResumeTokenTTL <= 0 {
		config.ResumeTokenTTL = 15 * time.Minute
	}
	if config.ResumeTokenTTL > 24*time.Hour {
		return nil, errors.New("resume token TTL exceeds 24 hours")
	}
	if len(config.ResumeTokenKey) == 0 {
		config.ResumeTokenKey = make([]byte, 32)
		if _, err := rand.Read(config.ResumeTokenKey); err != nil {
			return nil, errors.New("could not generate resume token key")
		}
	}
	if len(config.ResumeTokenKey) < 32 {
		return nil, errors.New("resume token key must contain at least 32 bytes")
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = 1 << 20
	}
	if config.MaxQueryResultBytes <= 0 {
		config.MaxQueryResultBytes = config.MaxBodyBytes
	}
	if config.MaxQueryResultBytes > 16<<20 {
		return nil, errors.New("max query result bytes exceeds 16 MiB")
	}
	if config.MaxRealtimeFrameBytes <= 0 {
		config.MaxRealtimeFrameBytes = config.MaxBodyBytes
	}
	if config.MaxRealtimeFrameBytes < 1024 || config.MaxRealtimeFrameBytes > 16<<20 {
		return nil, errors.New("max realtime frame bytes must be between 1 KiB and 16 MiB")
	}
	if config.MaxRealtimeOutboundBytes <= 0 {
		config.MaxRealtimeOutboundBytes = 8 * config.MaxRealtimeFrameBytes
	}
	if config.MaxRealtimeOutboundBytes < config.MaxRealtimeFrameBytes || config.MaxRealtimeOutboundBytes > 64<<20 {
		return nil, errors.New("max realtime outbound bytes must be between one frame and 64 MiB")
	}
	if config.MaxSubscriptionsPerConnection <= 0 {
		config.MaxSubscriptionsPerConnection = 32
	}
	if config.MaxConcurrentRPC <= 0 {
		config.MaxConcurrentRPC = 64
	}
	if config.MaxConcurrentRPC > 4096 {
		return nil, errors.New("max concurrent RPC exceeds 4096")
	}
	if config.MaxRPCPerConnection <= 0 {
		config.MaxRPCPerConnection = min(8, config.MaxConcurrentRPC)
	}
	if config.MaxRPCPerConnection > config.MaxConcurrentRPC {
		return nil, errors.New("max RPC per connection exceeds global RPC concurrency")
	}
	if config.MaxRPCArguments <= 0 {
		config.MaxRPCArguments = 32
	}
	if config.MaxRPCArguments > 1024 {
		return nil, errors.New("max RPC arguments exceeds 1024")
	}
	if config.MaxRPCResultBytes <= 0 {
		config.MaxRPCResultBytes = config.MaxBodyBytes
	}
	if config.MaxRPCResultBytes > 16<<20 {
		return nil, errors.New("max RPC result bytes exceeds 16 MiB")
	}
	if config.RPCIdempotencyStore != nil {
		if config.RPCIdempotencyRetention <= 0 {
			config.RPCIdempotencyRetention = 24 * time.Hour
		}
		if config.RPCIdempotencyRetention < time.Minute || config.RPCIdempotencyRetention > 30*24*time.Hour {
			return nil, errors.New("RPC idempotency retention must be between one minute and 30 days")
		}
		if config.RPCIdempotencyCommitTimeout <= 0 {
			config.RPCIdempotencyCommitTimeout = 5 * time.Second
		}
		if config.RPCIdempotencyCommitTimeout > 30*time.Second {
			return nil, errors.New("RPC idempotency commit timeout exceeds 30 seconds")
		}
	}
	methods := make(map[string]RPCMethod, len(config.RPCMethods))
	for name, method := range config.RPCMethods {
		if !validRPCMethodName(name) || method == nil {
			return nil, errors.New("RPC methods require valid names and non-nil handlers")
		}
		methods[name] = method
	}
	config.RPCMethods = methods
	transactionalMethods := make(map[string]RPCTransactionalMethod, len(config.RPCTransactionalMethods))
	for name, method := range config.RPCTransactionalMethods {
		if !validRPCMethodName(name) || method == nil {
			return nil, errors.New("transactional RPC methods require valid names and non-nil handlers")
		}
		if _, duplicate := methods[name]; duplicate {
			return nil, errors.New("RPC method name is registered in both standard and transactional registries")
		}
		transactionalMethods[name] = method
	}
	config.RPCTransactionalMethods = transactionalMethods
	reservedNames := make([]string, 0, len(methods)+len(transactionalMethods))
	for name := range methods {
		reservedNames = append(reservedNames, name)
	}
	for name := range transactionalMethods {
		reservedNames = append(reservedNames, name)
	}
	if len(transactionalMethods) > 0 {
		store, ok := config.RPCIdempotencyStore.(*durableRPCIdempotencyStore)
		if !ok || store.db != config.DB {
			return nil, errors.New("transactional RPC methods require the built-in durable idempotency store for the same database")
		}
	}
	if config.RPCTransactionalMethodResolver != nil {
		store, ok := config.RPCIdempotencyStore.(*durableRPCIdempotencyStore)
		if !ok || store.db != config.DB {
			return nil, errors.New("transactional RPC resolver requires the built-in durable idempotency store for the same database")
		}
	}
	if (len(methods) > 0 || len(transactionalMethods) > 0 || config.RPCMethodResolver != nil || config.RPCTransactionalMethodResolver != nil) && config.RPCAuthorizer == nil {
		return nil, errors.New("RPC authorizer is required when RPC methods are registered")
	}
	if config.ReplaySource == nil {
		config.ReplaySource = config.DB
	}
	for _, origin := range config.AllowedHTTPOrigins {
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, errors.New("allowed HTTP origins must be exact http(s) origins")
		}
	}
	for _, pattern := range config.OriginPatterns {
		if unrestrictedRealtimeOriginPattern(pattern) {
			return nil, errors.New("realtime origin patterns must be non-empty and cannot allow every origin")
		}
		if _, err := path.Match(pattern, ""); err != nil {
			return nil, fmt.Errorf("invalid realtime origin pattern %q: %w", pattern, err)
		}
	}
	rpcSessionID, err := randomToken16()
	if err != nil {
		return nil, errors.New("could not generate RPC session identity")
	}
	standardHub, standardWorkerResolver := config.RPCMethodResolver.(*WorkerHub)
	transactionalHub, transactionalWorkerResolver := config.RPCTransactionalMethodResolver.(*WorkerHub)
	policyHub, policyWorkerResolver := config.QueryPolicyResolver.(*WorkerHub)
	if standardWorkerResolver && transactionalWorkerResolver && standardHub != transactionalHub {
		return nil, errors.New("standard and transactional worker resolvers must use the same worker hub")
	}
	reservationHub := standardHub
	if reservationHub == nil {
		reservationHub = transactionalHub
	}
	if policyWorkerResolver {
		if store, ok := policyHub.config.PolicyGenerationStore.(*DurablePolicyGenerationStore); ok && store.db != config.DB {
			return nil, errors.New("worker policy generation store must use the server database")
		}
		if reservationHub != nil && reservationHub != policyHub {
			return nil, errors.New("method and publication worker resolvers must use the same worker hub")
		}
		reservationHub = policyHub
	}
	if reservationHub != nil {
		if err := reservationHub.reserveRPCMethods(reservedNames); err != nil {
			return nil, err
		}
	}
	h := &Handler{
		config: config, mux: http.NewServeMux(), tickets: newTicketStore(config.TicketTTL),
		resume:           newResumeTokenService(config.ResumeTokenKey, config.ResumeTokenTTL),
		rpcSlots:         make(chan struct{}, config.MaxConcurrentRPC),
		startedAt:        time.Now(),
		rpcSessionID:     rpcSessionID,
		operationalState: config.DB.OperationalState,
	}
	h.mux.HandleFunc("GET /health", h.health)
	h.mux.HandleFunc("GET /livez", h.liveness)
	h.mux.HandleFunc("GET /readyz", h.readiness)
	h.mux.HandleFunc("POST /v1/collections/{collection}/query", h.query)
	h.mux.HandleFunc("POST /v1/collections/{collection}/documents", h.insert)
	h.mux.HandleFunc("POST /v1/collections/{collection}/mutations", h.mutate)
	h.mux.HandleFunc("POST /v1/realtime/tickets", h.issueTicket)
	h.mux.HandleFunc("POST /v1/rpc", h.rpc)
	h.mux.HandleFunc("GET /v1/realtime", h.realtime)
	return h, nil
}

func unrestrictedRealtimeOriginPattern(pattern string) bool {
	if pattern == "" {
		return true
	}
	if _, hostPattern, found := strings.Cut(pattern, "://"); found {
		pattern = hostPattern
	}
	return pattern == "*" || pattern == "*:*"
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("origin")
	// A WebSocket upgrade has its own Origin enforcement in realtime via
	// websocket.Accept. Do not apply the HTTP CORS allowlist here: deployments
	// may intentionally use a narrower exact CORS policy for ticket issuance
	// and a separate host-pattern policy for the realtime connection.
	isRealtimeUpgrade := r.Method == http.MethodGet && r.URL.Path == "/v1/realtime"
	if origin != "" && !isRealtimeUpgrade {
		if !containsString(h.config.AllowedHTTPOrigins, origin) {
			writeError(w, http.StatusForbidden, "origin_forbidden")
			return
		}
		w.Header().Set("access-control-allow-origin", origin)
		w.Header().Set("vary", "Origin")
		if r.Method == http.MethodOptions {
			if !validPreflight(r) {
				writeError(w, http.StatusForbidden, "preflight_forbidden")
				return
			}
			w.Header().Set("access-control-allow-methods", "GET, POST")
			w.Header().Set("access-control-allow-headers", "Authorization, Content-Type")
			w.Header().Set("access-control-max-age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	h.mux.ServeHTTP(w, r)
}

type probeResponse struct {
	Version  int    `json:"version"`
	Status   string `json:"status"`
	Readable *bool  `json:"readable,omitempty"`
	Writable *bool  `json:"writable,omitempty"`
}

func (h *Handler) liveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("cache-control", "no-store")
	writeJSON(w, http.StatusOK, probeResponse{Version: 1, Status: "live"})
}

func (h *Handler) health(w http.ResponseWriter, request *http.Request) {
	h.readiness(w, request)
}

func (h *Handler) readiness(w http.ResponseWriter, _ *http.Request) {
	state := meldbase.OperationalState{}
	if h.operationalState != nil {
		state = h.operationalState()
	}
	status, code := "ready", http.StatusOK
	if !state.Readable || !state.Writable {
		status, code = "not_ready", http.StatusServiceUnavailable
	}
	w.Header().Set("cache-control", "no-store")
	writeJSON(w, code, probeResponse{
		Version: 1, Status: status, Readable: &state.Readable, Writable: &state.Writable,
	})
}

type socketSession struct {
	handler       *Handler
	principal     Principal
	ctx           context.Context
	cancel        context.CancelFunc
	connection    *websocket.Conn
	outgoing      chan socketOutbound
	outgoingMu    sync.Mutex
	outgoingBytes uint64
	mu            sync.Mutex
	byRequest     map[string]*socketSubscription
	byServer      map[string]*socketSubscription
	rpcCalls      map[string]context.CancelFunc
}

type socketOutbound struct{ data []byte }
type socketSubscription struct {
	requestID, serverID string
	cancel              context.CancelFunc
}

func (h *Handler) realtime(w http.ResponseWriter, r *http.Request) {
	if !configuredRealtimeOriginAllowed(r, h.config.OriginPatterns) {
		writeError(w, http.StatusForbidden, "origin_forbidden")
		return
	}
	connection, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: h.config.OriginPatterns, CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return
	}
	connection.SetReadLimit(int64(h.config.MaxBodyBytes))
	ctx, cancel := context.WithCancel(context.Background())
	session := &socketSession{
		handler: h, ctx: ctx, cancel: cancel, connection: connection, outgoing: make(chan socketOutbound, 64),
		byRequest: make(map[string]*socketSubscription), byServer: make(map[string]*socketSubscription),
		rpcCalls: make(map[string]context.CancelFunc),
	}
	defer session.shutdown()
	authCtx, authCancel := context.WithTimeout(ctx, 5*time.Second)
	raw, err := readSocketJSON(authCtx, connection, h.config.MaxBodyBytes)
	authCancel()
	if err != nil {
		connection.Close(websocket.StatusPolicyViolation, "authentication required")
		return
	}
	var auth struct {
		V      int    `json:"v"`
		Type   string `json:"type"`
		Ticket string `json:"ticket"`
	}
	if err := decodeStrict(raw, &auth); err != nil || auth.V != protocolVersion || auth.Type != "authenticate" || auth.Ticket == "" {
		connection.Close(websocket.StatusPolicyViolation, "invalid authentication")
		return
	}
	principal, ok := h.tickets.consume(auth.Ticket)
	if !ok {
		connection.Close(websocket.StatusPolicyViolation, "invalid ticket")
		return
	}
	session.principal = principal
	h.metrics.connectionsAccepted.Add(1)
	h.metrics.activeConnections.Add(1)
	defer h.metrics.activeConnections.Add(^uint64(0))
	writerDone := make(chan error, 1)
	go session.writeLoop(writerDone)
	if !session.enqueue(map[string]any{"v": protocolVersion, "type": "authenticated"}) {
		return
	}
	for {
		raw, err := readSocketJSON(ctx, connection, h.config.MaxBodyBytes)
		if err != nil {
			return
		}
		if err := session.handleMessage(raw); err != nil {
			connection.Close(websocket.StatusPolicyViolation, "invalid message")
			return
		}
		select {
		case <-writerDone:
			return
		default:
		}
	}
}

// configuredRealtimeOriginAllowed makes a configured OriginPatterns list a
// strict browser allowlist. coder/websocket deliberately accepts a request
// whose Origin host equals the request host before consulting patterns; that is
// useful as its default, but would defeat a scheme+host pattern such as
// "https://app.example". An empty list keeps the library's default same-host
// behavior for programmatic users that did not configure a browser boundary.
func configuredRealtimeOriginAllowed(request *http.Request, patterns []string) bool {
	origin := request.Header.Get("origin")
	if origin == "" || len(patterns) == 0 {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return false
	}
	for _, pattern := range patterns {
		target := parsed.Host
		if strings.Contains(pattern, "://") {
			target = parsed.Scheme + "://" + parsed.Host
		}
		matched, err := path.Match(strings.ToLower(pattern), strings.ToLower(target))
		if err == nil && matched {
			return true
		}
	}
	return false
}

func (s *socketSession) handleMessage(raw []byte) error {
	if err := meldbase.ValidateStrictJSON(raw, s.handler.config.MaxBodyBytes); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := decodeStrict(raw, &fields); err != nil {
		return errors.New("invalid envelope")
	}
	var version int
	var messageType string
	if value, ok := fields["v"]; !ok || decodeStrict(value, &version) != nil || version != protocolVersion {
		return errors.New("invalid version")
	}
	if value, ok := fields["type"]; !ok || decodeStrict(value, &messageType) != nil {
		return errors.New("invalid type")
	}
	switch messageType {
	case "ping":
		var message struct {
			V    int    `json:"v"`
			Type string `json:"type"`
		}
		if err := decodeStrict(raw, &message); err != nil {
			return err
		}
		return enqueueError(s.enqueue(map[string]any{"v": protocolVersion, "type": "pong"}))
	case "unsubscribe":
		var message struct {
			V              int    `json:"v"`
			Type           string `json:"type"`
			SubscriptionID string `json:"subscriptionId"`
		}
		if err := decodeStrict(raw, &message); err != nil {
			return err
		}
		if message.SubscriptionID == "" {
			return errors.New("missing subscription id")
		}
		s.unsubscribe(message.SubscriptionID)
		return nil
	case "subscribe":
		var message struct {
			V           int             `json:"v"`
			Type        string          `json:"type"`
			RequestID   string          `json:"requestId"`
			Collection  string          `json:"collection"`
			Query       json.RawMessage `json:"query"`
			ResumeToken string          `json:"resumeToken,omitempty"`
			Mode        string          `json:"mode,omitempty"`
		}
		if err := decodeStrict(raw, &message); err != nil {
			return err
		}
		if message.RequestID == "" || message.Collection == "" || len(message.Query) == 0 {
			return errors.New("incomplete subscription")
		}
		if message.Mode != "" && message.Mode != "delta" {
			return errors.New("invalid subscription mode")
		}
		return s.subscribe(message.RequestID, message.Collection, message.Query, message.ResumeToken, message.Mode)
	case "call":
		envelope, err := decodeRPCCallEnvelope(raw, s.handler.config.MaxRPCArguments)
		if err != nil {
			return err
		}
		s.startRPCCall(envelope, len(raw))
		return nil
	case "cancel":
		var message struct {
			V         int    `json:"v"`
			Type      string `json:"type"`
			RequestID string `json:"requestId"`
		}
		if err := decodeStrict(raw, &message); err != nil || message.V != protocolVersion || message.Type != "cancel" || !rpcRequestIDPattern.MatchString(message.RequestID) {
			return errors.New("invalid RPC cancellation")
		}
		s.cancelRPCCall(message.RequestID)
		return nil
	default:
		return errors.New("unknown message type")
	}
}

func (s *socketSession) subscribe(requestID, collection string, rawQuery []byte, resumeToken, mode string) error {
	query, err := meldbase.DecodeQuerySpecJSON(rawQuery, s.handler.config.QueryLimits)
	if err != nil {
		s.subscriptionError(requestID, "invalid_query")
		return nil
	}
	policy, err := s.handler.authorizeQuery(s.ctx, s.principal, collection, query)
	if err != nil {
		s.subscriptionError(requestID, "forbidden")
		return nil
	}
	query, err = applyPolicy(query, policy)
	if err != nil {
		s.subscriptionError(requestID, "forbidden")
		return nil
	}
	var resumePosition *uint64
	if resumeToken != "" {
		position, err := s.handler.resume.validate(resumeToken, s.handler.config.DB.DatabaseIdentity(), s.principal, collection, query, policy.PolicyVersion)
		if err != nil || mode != "delta" || s.handler.config.ReplaySource == nil {
			return enqueueError(s.enqueue(map[string]any{"v": protocolVersion, "type": "resync_required", "requestId": requestID}))
		}
		resumePosition = &position
	}
	s.mu.Lock()
	_, rpcExists := s.rpcCalls[requestID]
	if _, exists := s.byRequest[requestID]; exists || rpcExists || len(s.byRequest) >= s.handler.config.MaxSubscriptionsPerConnection {
		s.mu.Unlock()
		s.subscriptionError(requestID, "subscription_limit_or_duplicate")
		return nil
	}
	s.mu.Unlock()
	if resumePosition != nil {
		return s.startResumedDeltaSubscription(requestID, collection, query, policy, resumeToken, *resumePosition)
	}
	if mode == "delta" {
		return s.startDeltaSubscription(requestID, collection, query, policy)
	}
	return s.startSnapshotSubscription(requestID, collection, query, policy)
}

func (s *socketSession) startResumedDeltaSubscription(requestID, collection string, query meldbase.QuerySpec, policy QueryPolicy, clientToken string, position uint64) error {
	ctx, cancel := context.WithCancel(s.ctx)
	replay, err := s.handler.config.ReplaySource.OpenQueryReplay(ctx, collection, query, position, 8)
	if err != nil || replay == nil || replay.Initial.Token != position {
		cancel()
		if replay != nil {
			replay.Close()
		}
		if err != nil && engineErrorCode(err, "") == "database_unavailable" {
			s.subscriptionError(requestID, "database_unavailable")
			return nil
		}
		return enqueueError(s.enqueue(map[string]any{"v": protocolVersion, "type": "resync_required", "requestId": requestID}))
	}
	serverID, err := randomID()
	if err != nil {
		cancel()
		replay.Close()
		return err
	}
	subscription := &socketSubscription{requestID: requestID, serverID: serverID, cancel: cancel}
	s.mu.Lock()
	s.byRequest[requestID] = subscription
	s.byServer[serverID] = subscription
	s.mu.Unlock()
	var overlay *visibilityOverlay
	authorized, err := underQueryPolicy(policy, func() error {
		var err error
		overlay, _, err = newVisibilityOverlay(replay.Initial, policy)
		if err != nil {
			return err
		}
		return enqueueError(s.enqueue(map[string]any{
			"v": protocolVersion, "type": "resumed", "requestId": requestID,
			"subscriptionId": serverID, "token": clientToken,
		}))
	})
	if !authorized || err != nil {
		cancel()
		replay.Close()
		s.remove(subscription)
		if !authorized {
			s.enqueue(map[string]any{"v": protocolVersion, "type": "resync_required", "requestId": requestID})
		} else {
			s.subscriptionError(requestID, "resume_failed")
		}
		return nil
	}
	go func() {
		defer cancel()
		defer replay.Close()
		defer s.remove(subscription)
		lastClientToken := clientToken
		for {
			select {
			case <-ctx.Done():
				return
			case <-policy.Lease.Done():
				s.policyResync(subscription)
				return
			case <-policy.additionalLease.Done():
				s.policyResync(subscription)
				return
			case err := <-replay.Errors:
				if engineErrorCode(err, "") == "database_unavailable" {
					subscription.cancel()
					s.remove(subscription)
					s.subscriptionError(requestID, "database_unavailable")
				} else {
					s.policyResync(subscription)
				}
				return
			case delta, ok := <-replay.Deltas:
				if !ok {
					s.policyResync(subscription)
					return
				}
				authorized, err := underQueryPolicy(policy, func() error {
					visible, changed, err := overlay.apply(delta, policy)
					if err != nil || !changed {
						return err
					}
					nextToken, err := s.handler.resume.issue(s.handler.config.DB.DatabaseIdentity(), s.principal, collection, query, policy.PolicyVersion, delta.Token)
					if err != nil {
						return err
					}
					operations, err := encodeVisibleDelta(visible, s.handler.config.MaxRealtimeFrameBytes)
					if err != nil {
						return err
					}
					if !s.enqueue(map[string]any{
						"v": protocolVersion, "type": "delta", "requestId": requestID, "subscriptionId": serverID,
						"fromToken": lastClientToken, "token": nextToken, "operations": operations,
					}) {
						return errors.New("outbound queue full")
					}
					lastClientToken = nextToken
					return nil
				})
				if !authorized {
					s.policyResync(subscription)
					return
				}
				if err != nil {
					if engineErrorCode(err, "") == "resource_limit_exceeded" {
						s.subscriptionError(requestID, "resource_limit_exceeded")
					} else {
						s.policyResync(subscription)
					}
					return
				}
			}
		}
	}()
	return nil
}

func (s *socketSession) startSnapshotSubscription(requestID, collection string, query meldbase.QuerySpec, policy QueryPolicy) error {
	ctx, cancel := context.WithCancel(s.ctx)
	live, err := s.handler.config.DB.Collection(collection).SubscribeQuery(ctx, query, 2)
	if err != nil {
		cancel()
		s.subscriptionError(requestID, engineErrorCode(err, "subscription_failed"))
		return nil
	}
	serverID, err := randomID()
	if err != nil {
		cancel()
		live.Close()
		return err
	}
	subscription := &socketSubscription{requestID: requestID, serverID: serverID, cancel: cancel}
	s.mu.Lock()
	s.byRequest[requestID] = subscription
	s.byServer[serverID] = subscription
	s.mu.Unlock()
	go func() {
		defer cancel()
		defer live.Close()
		defer s.remove(subscription)
		for {
			select {
			case <-ctx.Done():
				return
			case <-policy.Lease.Done():
				s.policyResync(subscription)
				return
			case <-policy.additionalLease.Done():
				s.policyResync(subscription)
				return
			case err, ok := <-live.Errors:
				if ok && err != nil {
					s.subscriptionError(requestID, engineErrorCode(err, "subscription_ended"))
				}
				return
			case snapshot, ok := <-live.Snapshots:
				if !ok {
					return
				}
				authorized, err := underQueryPolicy(policy, func() error {
					documents, err := encodeProjectedDocuments(snapshot.Documents, policy, s.handler.config.MaxRealtimeFrameBytes, realtimeEnvelopeBytes)
					if err != nil {
						return err
					}
					resumeToken, err := s.handler.resume.issue(s.handler.config.DB.DatabaseIdentity(), s.principal, collection, query, policy.PolicyVersion, snapshot.Token)
					if err != nil {
						return err
					}
					return enqueueError(s.enqueue(map[string]any{"v": protocolVersion, "type": "snapshot", "requestId": requestID, "subscriptionId": serverID, "token": resumeToken, "documents": documents}))
				})
				if !authorized {
					s.policyResync(subscription)
					return
				}
				if err != nil {
					s.subscriptionError(requestID, engineErrorCode(err, "snapshot_failed"))
					return
				}
			}
		}
	}()
	return nil
}

func (s *socketSession) startDeltaSubscription(requestID, collection string, query meldbase.QuerySpec, policy QueryPolicy) error {
	ctx, cancel := context.WithCancel(s.ctx)
	live, err := s.handler.config.DB.Collection(collection).SubscribeQueryDeltas(ctx, query, 8)
	if err != nil {
		cancel()
		s.subscriptionError(requestID, engineErrorCode(err, "subscription_failed"))
		return nil
	}
	serverID, err := randomID()
	if err != nil {
		cancel()
		live.Close()
		return err
	}
	subscription := &socketSubscription{requestID: requestID, serverID: serverID, cancel: cancel}
	s.mu.Lock()
	s.byRequest[requestID] = subscription
	s.byServer[serverID] = subscription
	s.mu.Unlock()
	var initial *visibilityOverlay
	var initialVisible []meldbase.Document
	var initialToken string
	authorized, err := underQueryPolicy(policy, func() error {
		var err error
		initial, initialVisible, err = newVisibilityOverlay(live.Initial, policy)
		if err != nil {
			return err
		}
		initialToken, err = s.handler.resume.issue(s.handler.config.DB.DatabaseIdentity(), s.principal, collection, query, policy.PolicyVersion, initial.visibleToken)
		if err != nil {
			return err
		}
		initialDocuments, err := encodeVisibleDocuments(initialVisible, s.handler.config.MaxRealtimeFrameBytes)
		if err != nil {
			return err
		}
		return enqueueError(s.enqueue(map[string]any{
			"v": protocolVersion, "type": "snapshot", "requestId": requestID, "subscriptionId": serverID,
			"token": initialToken, "documents": initialDocuments,
		}))
	})
	if !authorized {
		cancel()
		live.Close()
		s.remove(subscription)
		s.enqueue(map[string]any{"v": protocolVersion, "type": "resync_required", "requestId": requestID})
		return nil
	}
	if err != nil {
		cancel()
		live.Close()
		s.remove(subscription)
		s.subscriptionError(requestID, engineErrorCode(err, "initial_snapshot_failed"))
		return nil
	}
	go func() {
		defer cancel()
		defer live.Close()
		defer s.remove(subscription)
		clientToken := initialToken
		for {
			select {
			case <-ctx.Done():
				return
			case <-policy.Lease.Done():
				s.policyResync(subscription)
				return
			case <-policy.additionalLease.Done():
				s.policyResync(subscription)
				return
			case err, ok := <-live.Errors:
				if ok && err != nil {
					s.subscriptionError(requestID, engineErrorCode(err, "subscription_ended"))
				}
				return
			case delta, ok := <-live.Deltas:
				if !ok {
					return
				}
				authorized, err := underQueryPolicy(policy, func() error {
					visible, changed, err := initial.apply(delta, policy)
					if err != nil || !changed {
						return err
					}
					nextToken, err := s.handler.resume.issue(s.handler.config.DB.DatabaseIdentity(), s.principal, collection, query, policy.PolicyVersion, delta.Token)
					if err != nil {
						return err
					}
					operations, err := encodeVisibleDelta(visible, s.handler.config.MaxRealtimeFrameBytes)
					if err != nil {
						return err
					}
					if !s.enqueue(map[string]any{
						"v": protocolVersion, "type": "delta", "requestId": requestID, "subscriptionId": serverID,
						"fromToken": clientToken, "token": nextToken, "operations": operations,
					}) {
						return errors.New("outbound queue full")
					}
					clientToken = nextToken
					return nil
				})
				if !authorized {
					s.policyResync(subscription)
					return
				}
				if err != nil {
					s.subscriptionError(requestID, engineErrorCode(err, "delta_failed"))
					return
				}
			}
		}
	}()
	return nil
}

func (s *socketSession) writeLoop(done chan<- error) {
	for {
		select {
		case <-s.ctx.Done():
			done <- s.ctx.Err()
			return
		case message := <-s.outgoing:
			ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
			err := s.connection.Write(ctx, websocket.MessageText, message.data)
			cancel()
			s.releaseOutgoing(uint64(len(message.data)))
			if err != nil {
				done <- err
				s.cancel()
				return
			}
		}
	}
}
func (s *socketSession) enqueue(message any) bool {
	if s == nil || s.handler == nil {
		return false
	}
	data, err := json.Marshal(message)
	if err != nil || len(data) == 0 || len(data) > s.handler.config.MaxRealtimeFrameBytes {
		s.handler.metrics.realtimeOutboundOverflows.Add(1)
		s.cancel()
		return false
	}
	bytes := uint64(len(data))
	s.outgoingMu.Lock()
	defer s.outgoingMu.Unlock()
	if s.ctx.Err() != nil || bytes > uint64(s.handler.config.MaxRealtimeOutboundBytes) ||
		s.outgoingBytes > uint64(s.handler.config.MaxRealtimeOutboundBytes)-bytes {
		s.handler.metrics.realtimeOutboundOverflows.Add(1)
		s.cancel()
		return false
	}
	select {
	case s.outgoing <- socketOutbound{data: data}:
		s.outgoingBytes += bytes
		return true
	default:
		s.handler.metrics.realtimeOutboundOverflows.Add(1)
		s.cancel()
		return false
	}
}

func (s *socketSession) releaseOutgoing(bytes uint64) {
	if s == nil || bytes == 0 {
		return
	}
	s.outgoingMu.Lock()
	if bytes > s.outgoingBytes {
		s.outgoingBytes = 0
	} else {
		s.outgoingBytes -= bytes
	}
	s.outgoingMu.Unlock()
}
func (s *socketSession) subscriptionError(requestID, code string) {
	s.enqueue(rpcErrorMessage(requestID, code))
}

func (s *socketSession) policyResync(subscription *socketSubscription) {
	if subscription == nil {
		return
	}
	// Remove before publication so a client may immediately reuse requestId.
	subscription.cancel()
	s.remove(subscription)
	s.enqueue(map[string]any{"v": protocolVersion, "type": "resync_required", "requestId": subscription.requestID})
}

func (s *socketSession) unsubscribe(serverID string) {
	s.mu.Lock()
	subscription := s.byServer[serverID]
	s.mu.Unlock()
	if subscription != nil {
		subscription.cancel()
	}
}
func (s *socketSession) remove(subscription *socketSubscription) {
	s.mu.Lock()
	delete(s.byRequest, subscription.requestID)
	delete(s.byServer, subscription.serverID)
	s.mu.Unlock()
}
func (s *socketSession) shutdown() {
	s.cancel()
	s.mu.Lock()
	for _, subscription := range s.byRequest {
		subscription.cancel()
	}
	for _, cancel := range s.rpcCalls {
		cancel()
	}
	s.byRequest = map[string]*socketSubscription{}
	s.byServer = map[string]*socketSubscription{}
	s.rpcCalls = map[string]context.CancelFunc{}
	s.mu.Unlock()
	s.connection.Close(websocket.StatusNormalClosure, "")
}

func readSocketJSON(ctx context.Context, connection *websocket.Conn, max int) ([]byte, error) {
	kind, data, err := connection.Read(ctx)
	if err != nil {
		return nil, err
	}
	if kind != websocket.MessageText || len(data) > max {
		return nil, errors.New("invalid websocket message")
	}
	if err := meldbase.ValidateStrictJSON(data, max); err != nil {
		return nil, err
	}
	return data, nil
}
func writeSocketJSON(ctx context.Context, connection *websocket.Conn, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return connection.Write(ctx, websocket.MessageText, data)
}
func enqueueError(ok bool) error {
	if !ok {
		return errors.New("outbound queue full")
	}
	return nil
}
func randomID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
func readBounded(w http.ResponseWriter, r *http.Request, max int) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, int64(max))
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}
func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("x-content-type-options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"code": code}})
}
func writeReadError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large")
		return
	}
	writeError(w, http.StatusBadRequest, "invalid_request")
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func validPreflight(request *http.Request) bool {
	method := request.Header.Get("access-control-request-method")
	if method != http.MethodGet && method != http.MethodPost {
		return false
	}
	for _, header := range strings.Split(request.Header.Get("access-control-request-headers"), ",") {
		header = strings.ToLower(strings.TrimSpace(header))
		if header != "" && header != "authorization" && header != "content-type" {
			return false
		}
	}
	return true
}
func writeEngineError(w http.ResponseWriter, err error) {
	status, code := engineErrorStatusCode(err)
	writeError(w, status, code)
}

func engineErrorStatusCode(err error) (int, string) {
	status, code := http.StatusInternalServerError, "internal"
	// Admission cancellation can race a durable  final-Meta acknowledgement.
	// It must win over the wrapped Context error below so a client never treats
	// an acknowledged mutation as safely canceled and retries it blindly.
	if errors.Is(err, meldbase.ErrCommitOutcomeUnknown) {
		// Protocol v1 deliberately has one fixed outcome-unknown code shared by
		// RPC and mutations. Adding a generic synonym would be a wire-contract
		// revision, not an implementation detail.
		return http.StatusConflict, "rpc_outcome_unknown"
	}
	// ErrDurability is a fail-stop state: reads can continue, but the result of
	// the write that entered the state must not be exposed as an ordinary 500.
	// ErrClosed makes both reads and writes unavailable. The public transport
	// intentionally gives both the same non-sensitive recovery signal.
	if errors.Is(err, meldbase.ErrDurability) || errors.Is(err, meldbase.ErrClosed) {
		return http.StatusServiceUnavailable, "database_unavailable"
	}
	if errors.Is(err, meldbase.ErrInvalidCollection) || errors.Is(err, meldbase.ErrInvalidFilter) {
		status, code = http.StatusBadRequest, "invalid_request"
	}
	if errors.Is(err, meldbase.ErrInvalidUpdate) || errors.Is(err, meldbase.ErrImmutableID) {
		status, code = http.StatusUnprocessableEntity, "invalid_update"
	}
	if errors.Is(err, meldbase.ErrMutationLimit) {
		status, code = http.StatusConflict, "mutation_limit_exceeded"
	}
	if errors.Is(err, meldbase.ErrResourceLimit) {
		status, code = http.StatusRequestEntityTooLarge, "resource_limit_exceeded"
	}
	if errors.Is(err, meldbase.ErrDuplicateID) || errors.Is(err, meldbase.ErrDuplicateKey) {
		status, code = http.StatusConflict, "duplicate_key"
	}
	if errors.Is(err, context.Canceled) {
		status, code = 499, "cancelled"
	}
	return status, code
}

func engineErrorCode(err error, fallback string) string {
	_, code := engineErrorStatusCode(err)
	if code == "internal" {
		return fallback
	}
	return code
}
