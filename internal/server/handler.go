package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/crapthings/meldbase"
)

type Config struct {
	DB                            *meldbase.DB
	Authenticator                 Authenticator
	Authorizer                    Authorizer
	PublicRealtimeURL             string
	OriginPatterns                []string
	AllowedHTTPOrigins            []string
	TicketTTL                     time.Duration
	ResumeTokenKey                []byte
	ResumeTokenTTL                time.Duration
	MaxBodyBytes                  int
	MaxSubscriptionsPerConnection int
	QueryLimits                   meldbase.QueryLimits
}

type Handler struct {
	config  Config
	mux     *http.ServeMux
	tickets *ticketStore
	resume  *resumeTokenService
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
	if config.MaxSubscriptionsPerConnection <= 0 {
		config.MaxSubscriptionsPerConnection = 32
	}
	for _, origin := range config.AllowedHTTPOrigins {
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, errors.New("allowed HTTP origins must be exact http(s) origins")
		}
	}
	h := &Handler{config: config, mux: http.NewServeMux(), tickets: newTicketStore(config.TicketTTL), resume: newResumeTokenService(config.ResumeTokenKey, config.ResumeTokenTTL)}
	h.mux.HandleFunc("GET /health", h.health)
	h.mux.HandleFunc("POST /v1/collections/{collection}/query", h.query)
	h.mux.HandleFunc("POST /v1/collections/{collection}/documents", h.insert)
	h.mux.HandleFunc("POST /v1/collections/{collection}/mutations", h.mutate)
	h.mux.HandleFunc("POST /v1/realtime/tickets", h.issueTicket)
	h.mux.HandleFunc("GET /v1/realtime", h.realtime)
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("origin")
	if origin != "" {
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
func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (h *Handler) insert(w http.ResponseWriter, r *http.Request) {
	principal, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	body, err := readBounded(w, r, h.config.MaxBodyBytes)
	if err != nil {
		writeReadError(w, err)
		return
	}
	if err := meldbase.ValidateStrictJSON(body, h.config.MaxBodyBytes); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	var envelope struct {
		Version  int             `json:"version"`
		Document json.RawMessage `json:"document"`
	}
	if err := decodeStrict(body, &envelope); err != nil || envelope.Version != 1 || len(envelope.Document) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_document_envelope")
		return
	}
	document, err := meldbase.UnmarshalWireInputDocument(envelope.Document, h.config.QueryLimits)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_document")
		return
	}
	policy, err := h.config.Authorizer.AuthorizeInsert(r.Context(), principal, r.PathValue("collection"), document.Clone())
	if err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	if !policy.AllowAllInputFields {
		for field := range document {
			if field == "_id" {
				continue
			}
			if _, allowed := policy.AllowedInputFields[field]; !allowed {
				writeError(w, http.StatusForbidden, "forbidden_field")
				return
			}
		}
	}
	for field, value := range policy.SetFields {
		if field == "_id" {
			writeError(w, http.StatusInternalServerError, "invalid_policy")
			return
		}
		document[field] = value.Clone()
	}
	id, err := h.config.DB.Collection(r.PathValue("collection")).InsertOne(r.Context(), document)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	inserted, err := h.config.DB.Collection(r.PathValue("collection")).FindOne(r.Context(), meldbase.Filter{"_id": id})
	if err != nil {
		writeEngineError(w, err)
		return
	}
	raw, err := meldbase.MarshalWireDocument(projectInsert(inserted, policy))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"version": 1, "document": json.RawMessage(raw)})
}

func (h *Handler) mutate(w http.ResponseWriter, r *http.Request) {
	principal, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	body, err := readBounded(w, r, h.config.MaxBodyBytes)
	if err != nil {
		writeReadError(w, err)
		return
	}
	if err := meldbase.ValidateStrictJSON(body, h.config.MaxBodyBytes); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	var envelope struct {
		Version int             `json:"version"`
		Action  string          `json:"action"`
		Query   json.RawMessage `json:"query"`
		Update  json.RawMessage `json:"update,omitempty"`
	}
	if err := decodeStrict(body, &envelope); err != nil || envelope.Version != 1 || len(envelope.Query) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_mutation_envelope")
		return
	}
	query, err := meldbase.DecodeQuerySpecJSON(envelope.Query, h.config.QueryLimits)
	if err != nil || query.HasModifiers() {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}
	collectionName := r.PathValue("collection")
	collection := h.config.DB.Collection(collectionName)
	switch envelope.Action {
	case "updateOne", "updateMany":
		if len(envelope.Update) == 0 {
			writeError(w, http.StatusBadRequest, "missing_update")
			return
		}
		mutation, err := meldbase.DecodeMutationSpecJSON(envelope.Update, h.config.QueryLimits)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_update")
			return
		}
		policy, err := h.config.Authorizer.AuthorizeUpdate(r.Context(), principal, collectionName, query, mutation)
		if err != nil {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		query, err = applyUpdatePolicy(query, mutation, policy)
		if err != nil {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		var result meldbase.UpdateResult
		if envelope.Action == "updateOne" {
			result, err = collection.UpdateOneQuery(r.Context(), query, mutation)
		} else {
			result, err = collection.UpdateManyQueryLimited(r.Context(), query, mutation, policy.MaxAffected)
		}
		if err != nil {
			writeEngineError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"version": 1, "matchedCount": result.MatchedCount, "modifiedCount": result.ModifiedCount})
	case "deleteOne", "deleteMany":
		if len(envelope.Update) != 0 {
			writeError(w, http.StatusBadRequest, "unexpected_update")
			return
		}
		policy, err := h.config.Authorizer.AuthorizeDelete(r.Context(), principal, collectionName, query)
		if err != nil {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		query, err = applyDeletePolicy(query, policy)
		if err != nil {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		var result meldbase.DeleteResult
		if envelope.Action == "deleteOne" {
			result, err = collection.DeleteOneQuery(r.Context(), query)
		} else {
			result, err = collection.DeleteManyQueryLimited(r.Context(), query, policy.MaxAffected)
		}
		if err != nil {
			writeEngineError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"version": 1, "deletedCount": result.DeletedCount})
	default:
		writeError(w, http.StatusBadRequest, "unknown_mutation_action")
	}
}

func (h *Handler) query(w http.ResponseWriter, r *http.Request) {
	principal, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	body, err := readBounded(w, r, h.config.MaxBodyBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if err := meldbase.ValidateStrictJSON(body, h.config.MaxBodyBytes); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	var envelope struct {
		Version int             `json:"version"`
		Query   json.RawMessage `json:"query"`
	}
	if err := decodeStrict(body, &envelope); err != nil || envelope.Version != 1 || len(envelope.Query) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_query_envelope")
		return
	}
	query, err := meldbase.DecodeQuerySpecJSON(envelope.Query, h.config.QueryLimits)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}
	policy, err := h.config.Authorizer.AuthorizeQuery(r.Context(), principal, r.PathValue("collection"), query)
	if err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	query, err = applyPolicy(query, policy)
	if err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	cursor, err := h.config.DB.Collection(r.PathValue("collection")).FindQuery(r.Context(), query)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	documents, err := cursor.All(r.Context())
	if err != nil {
		writeEngineError(w, err)
		return
	}
	encoded := make([]json.RawMessage, len(documents))
	for i, document := range documents {
		raw, err := meldbase.MarshalWireDocument(project(document, policy))
		if err != nil {
			writeEngineError(w, err)
			return
		}
		encoded[i] = raw
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": 1, "documents": encoded})
}

func (h *Handler) issueTicket(w http.ResponseWriter, r *http.Request) {
	principal, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	ticket, err := h.tickets.issue(principal)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	w.Header().Set("cache-control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{"url": h.config.PublicRealtimeURL, "ticket": ticket})
}

type socketSession struct {
	handler    *Handler
	principal  Principal
	ctx        context.Context
	cancel     context.CancelFunc
	connection *websocket.Conn
	outgoing   chan any
	mu         sync.Mutex
	byRequest  map[string]*socketSubscription
	byServer   map[string]*socketSubscription
}
type socketSubscription struct {
	requestID, serverID string
	cancel              context.CancelFunc
}

func (h *Handler) realtime(w http.ResponseWriter, r *http.Request) {
	connection, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: h.config.OriginPatterns, CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return
	}
	connection.SetReadLimit(int64(h.config.MaxBodyBytes))
	ctx, cancel := context.WithCancel(context.Background())
	session := &socketSession{handler: h, ctx: ctx, cancel: cancel, connection: connection, outgoing: make(chan any, 64), byRequest: make(map[string]*socketSubscription), byServer: make(map[string]*socketSubscription)}
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
	if err := decodeStrict(raw, &auth); err != nil || auth.V != 1 || auth.Type != "authenticate" || auth.Ticket == "" {
		connection.Close(websocket.StatusPolicyViolation, "invalid authentication")
		return
	}
	principal, ok := h.tickets.consume(auth.Ticket)
	if !ok {
		connection.Close(websocket.StatusPolicyViolation, "invalid ticket")
		return
	}
	session.principal = principal
	writerDone := make(chan error, 1)
	go session.writeLoop(writerDone)
	if !session.enqueue(map[string]any{"v": 1, "type": "authenticated"}) {
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
	if value, ok := fields["v"]; !ok || decodeStrict(value, &version) != nil || version != 1 {
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
		return enqueueError(s.enqueue(map[string]any{"v": 1, "type": "pong"}))
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
		}
		if err := decodeStrict(raw, &message); err != nil {
			return err
		}
		if message.RequestID == "" || message.Collection == "" || len(message.Query) == 0 {
			return errors.New("incomplete subscription")
		}
		return s.subscribe(message.RequestID, message.Collection, message.Query, message.ResumeToken)
	default:
		return errors.New("unknown message type")
	}
}

func (s *socketSession) subscribe(requestID, collection string, rawQuery []byte, resumeToken string) error {
	query, err := meldbase.DecodeQuerySpecJSON(rawQuery, s.handler.config.QueryLimits)
	if err != nil {
		s.subscriptionError(requestID, "invalid_query")
		return nil
	}
	policy, err := s.handler.config.Authorizer.AuthorizeQuery(s.ctx, s.principal, collection, query)
	if err != nil {
		s.subscriptionError(requestID, "forbidden")
		return nil
	}
	query, err = applyPolicy(query, policy)
	if err != nil {
		s.subscriptionError(requestID, "forbidden")
		return nil
	}
	if policy.PolicyVersion == "" {
		s.subscriptionError(requestID, "invalid_policy")
		return nil
	}
	if resumeToken != "" {
		position, err := s.handler.resume.validate(resumeToken, s.handler.config.DB.DatabaseIdentity(), s.principal, collection, query, policy.PolicyVersion)
		if err != nil || !s.handler.config.DB.CanResumeFrom(position) {
			return enqueueError(s.enqueue(map[string]any{"v": 1, "type": "resync_required", "requestId": requestID}))
		}
	}
	s.mu.Lock()
	if _, exists := s.byRequest[requestID]; exists || len(s.byRequest) >= s.handler.config.MaxSubscriptionsPerConnection {
		s.mu.Unlock()
		s.subscriptionError(requestID, "subscription_limit_or_duplicate")
		return nil
	}
	s.mu.Unlock()
	ctx, cancel := context.WithCancel(s.ctx)
	live, err := s.handler.config.DB.Collection(collection).SubscribeQuery(ctx, query, 2)
	if err != nil {
		cancel()
		s.subscriptionError(requestID, "subscription_failed")
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
		defer live.Close()
		defer s.remove(subscription)
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-live.Errors:
				if ok && err != nil {
					s.subscriptionError(requestID, "subscription_ended")
				}
				return
			case snapshot, ok := <-live.Snapshots:
				if !ok {
					return
				}
				documents := make([]json.RawMessage, len(snapshot.Documents))
				for i, document := range snapshot.Documents {
					raw, err := meldbase.MarshalWireDocument(project(document, policy))
					if err != nil {
						s.subscriptionError(requestID, "encode_failed")
						return
					}
					documents[i] = raw
				}
				resumeToken, err := s.handler.resume.issue(s.handler.config.DB.DatabaseIdentity(), s.principal, collection, query, policy.PolicyVersion, snapshot.Token)
				if err != nil {
					s.subscriptionError(requestID, "resume_token_failed")
					return
				}
				if !s.enqueue(map[string]any{"v": 1, "type": "snapshot", "requestId": requestID, "subscriptionId": serverID, "token": resumeToken, "documents": documents}) {
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
			err := writeSocketJSON(ctx, s.connection, message)
			cancel()
			if err != nil {
				done <- err
				s.cancel()
				return
			}
		}
	}
}
func (s *socketSession) enqueue(message any) bool {
	select {
	case s.outgoing <- message:
		return true
	default:
		s.cancel()
		return false
	}
}
func (s *socketSession) subscriptionError(requestID, code string) {
	s.enqueue(map[string]any{"v": 1, "type": "error", "requestId": requestID, "message": code})
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
	s.byRequest = map[string]*socketSubscription{}
	s.byServer = map[string]*socketSubscription{}
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
	status := http.StatusInternalServerError
	code := "internal"
	if errors.Is(err, meldbase.ErrInvalidCollection) || errors.Is(err, meldbase.ErrInvalidFilter) {
		status, code = http.StatusBadRequest, "invalid_request"
	}
	if errors.Is(err, meldbase.ErrInvalidUpdate) || errors.Is(err, meldbase.ErrImmutableID) {
		status, code = http.StatusUnprocessableEntity, "invalid_update"
	}
	if errors.Is(err, meldbase.ErrMutationLimit) {
		status, code = http.StatusConflict, "mutation_limit_exceeded"
	}
	if errors.Is(err, meldbase.ErrDuplicateID) || errors.Is(err, meldbase.ErrDuplicateKey) {
		status, code = http.StatusConflict, "duplicate_key"
	}
	if errors.Is(err, context.Canceled) {
		status, code = 499, "cancelled"
	}
	writeError(w, status, code)
}
