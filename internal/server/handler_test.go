package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/crapthings/meldbase"
)

type testAuthenticator struct{}

func (testAuthenticator) AuthenticateHTTP(request *http.Request) (Principal, error) {
	if request.Header.Get("authorization") != "Bearer valid" {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Subject: "user-1", Tenant: "mine"}, nil
}

type testAuthorizer struct{}

func (testAuthorizer) AuthorizeQuery(_ context.Context, principal Principal, collection string, _ meldbase.QuerySpec) (QueryPolicy, error) {
	if collection != "items" || principal.Subject != "user-1" {
		return QueryPolicy{}, ErrForbidden
	}
	constraint, err := meldbase.CompileQuery(meldbase.Filter{"tenant": principal.Tenant}, meldbase.QueryOptions{})
	if err != nil {
		return QueryPolicy{}, err
	}
	return QueryPolicy{
		PolicyVersion: "test-v1", Constraint: &constraint, MaxResults: 10,
		AllowedQueryPaths:   map[string]struct{}{"rank": {}, "title": {}},
		AllowedResultFields: map[string]struct{}{"rank": {}, "title": {}},
	}, nil
}

func (testAuthorizer) AuthorizeInsert(_ context.Context, principal Principal, collection string, _ meldbase.Document) (InsertPolicy, error) {
	if collection != "items" || principal.Subject != "user-1" {
		return InsertPolicy{}, ErrForbidden
	}
	return InsertPolicy{AllowedInputFields: map[string]struct{}{"rank": {}, "title": {}}, SetFields: meldbase.Document{"tenant": meldbase.String(principal.Tenant)}, AllowedResultFields: map[string]struct{}{"rank": {}, "title": {}}}, nil
}

func (testAuthorizer) AuthorizeUpdate(ctx context.Context, principal Principal, collection string, query meldbase.QuerySpec, _ meldbase.MutationSpec) (UpdatePolicy, error) {
	base, err := (testAuthorizer{}).AuthorizeQuery(ctx, principal, collection, query)
	if err != nil {
		return UpdatePolicy{}, err
	}
	return UpdatePolicy{QueryPolicy: base, AllowedUpdatePaths: map[string]struct{}{"rank": {}, "title": {}}, MaxAffected: 10}, nil
}

func (testAuthorizer) AuthorizeDelete(ctx context.Context, principal Principal, collection string, query meldbase.QuerySpec) (DeletePolicy, error) {
	base, err := (testAuthorizer{}).AuthorizeQuery(ctx, principal, collection, query)
	return DeletePolicy{QueryPolicy: base, MaxAffected: 10}, err
}

func TestHTTPQueryAppliesRowPolicyBeforeLimitAndRedactsFields(t *testing.T) {
	db, handler, server := newTestServer(t)
	collection := db.Collection("items")
	insertServerDocument(t, collection, "other", 1, "hidden first")
	mineID := insertServerDocument(t, collection, "mine", 2, "visible")
	query := `{"version":1,"query":{"version":1,"where":{"op":"true"},"sort":[{"path":"rank","direction":1}],"limit":1}}`
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/collections/items/query", strings.NewReader(query))
	request.Header.Set("authorization", "Bearer valid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var body struct {
		Documents []json.RawMessage `json:"documents"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Documents) != 1 {
		t.Fatalf("documents = %d", len(body.Documents))
	}
	document, err := meldbase.UnmarshalWireDocument(body.Documents[0], meldbase.DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := document.ID()
	if id != mineID {
		t.Fatalf("id = %s, want %s", id, mineID)
	}
	if _, leaked := document["tenant"]; leaked {
		t.Fatal("tenant field leaked through projection")
	}
	if title, _ := document["title"].StringValue(); title != "visible" {
		t.Fatalf("title = %q", title)
	}

	unauthorized, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/collections/items/query", strings.NewReader(query))
	unauthorizedResponse, err := http.DefaultClient.Do(unauthorized)
	if err != nil {
		t.Fatal(err)
	}
	unauthorizedResponse.Body.Close()
	if unauthorizedResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorizedResponse.StatusCode)
	}

	forbiddenQuery := `{"version":1,"query":{"version":1,"where":{"op":"exists","path":"tenant","value":true}}}`
	forbidden, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/collections/items/query", strings.NewReader(forbiddenQuery))
	forbidden.Header.Set("authorization", "Bearer valid")
	forbiddenResponse, err := http.DefaultClient.Do(forbidden)
	if err != nil {
		t.Fatal(err)
	}
	forbiddenResponse.Body.Close()
	if forbiddenResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("forbidden status = %d", forbiddenResponse.StatusCode)
	}
	_ = handler
}

func TestHTTPInsertAppliesServerOwnedFieldsAndRejectsForbiddenInput(t *testing.T) {
	db, _, server := newTestServer(t)
	document := `{"t":"object","v":[["rank",{"t":"int64","v":"7"}],["title",{"t":"string","v":"created"}]]}`
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/collections/items/documents", strings.NewReader(`{"version":1,"document":`+document+`}`))
	request.Header.Set("authorization", "Bearer valid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var body struct {
		Document json.RawMessage `json:"document"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	returned, err := meldbase.UnmarshalWireDocument(body.Document, meldbase.DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, leaked := returned["tenant"]; leaked {
		t.Fatal("server-owned tenant leaked in response")
	}
	stored, err := db.Collection("items").FindOne(context.Background(), meldbase.Filter{"title": "created"})
	if err != nil {
		t.Fatal(err)
	}
	if tenant, _ := stored["tenant"].StringValue(); tenant != "mine" {
		t.Fatalf("tenant = %q", tenant)
	}

	forbiddenDocument := `{"t":"object","v":[["tenant",{"t":"string","v":"other"}],["title",{"t":"string","v":"attack"}]]}`
	forbidden, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/collections/items/documents", strings.NewReader(`{"version":1,"document":`+forbiddenDocument+`}`))
	forbidden.Header.Set("authorization", "Bearer valid")
	forbiddenResponse, err := http.DefaultClient.Do(forbidden)
	if err != nil {
		t.Fatal(err)
	}
	forbiddenResponse.Body.Close()
	if forbiddenResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("forbidden status = %d", forbiddenResponse.StatusCode)
	}
}

func TestHTTPUpdateDeleteApplyRowAndFieldPolicies(t *testing.T) {
	db, _, server := newTestServer(t)
	collection := db.Collection("items")
	insertServerDocument(t, collection, "mine", 1, "mine-one")
	insertServerDocument(t, collection, "mine", 2, "mine-two")
	insertServerDocument(t, collection, "other", 3, "other")
	query := `{"version":1,"where":{"op":"true"}}`
	update := `{"version":1,"operations":[{"op":"set","path":"title","value":{"t":"string","v":"updated"}}]}`
	response := postMutation(t, server.URL, `{"version":1,"action":"updateMany","query":`+query+`,"update":`+update+`}`)
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("update status = %d body = %s", response.StatusCode, body)
	}
	var updateResult struct{ MatchedCount, ModifiedCount int64 }
	if err := json.NewDecoder(response.Body).Decode(&updateResult); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if updateResult.MatchedCount != 2 || updateResult.ModifiedCount != 2 {
		t.Fatalf("update = %+v", updateResult)
	}
	other, err := collection.FindOne(context.Background(), meldbase.Filter{"tenant": "other"})
	if err != nil {
		t.Fatal(err)
	}
	title, _ := other["title"].StringValue()
	if title != "other" {
		t.Fatalf("cross-tenant update leaked, title = %q", title)
	}

	forbidden := `{"version":1,"operations":[{"op":"set","path":"tenant","value":{"t":"string","v":"other"}}]}`
	response = postMutation(t, server.URL, `{"version":1,"action":"updateMany","query":`+query+`,"update":`+forbidden+`}`)
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("forbidden update status = %d", response.StatusCode)
	}

	response = postMutation(t, server.URL, `{"version":1,"action":"deleteMany","query":`+query+`}`)
	var deleteResult struct{ DeletedCount int64 }
	if err := json.NewDecoder(response.Body).Decode(&deleteResult); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || deleteResult.DeletedCount != 2 {
		t.Fatalf("delete status=%d result=%+v", response.StatusCode, deleteResult)
	}
	cursor, _ := collection.Find(context.Background(), meldbase.Filter{})
	remaining, _ := cursor.All(context.Background())
	if len(remaining) != 1 {
		t.Fatalf("remaining = %d", len(remaining))
	}
}

func TestHTTPMutationAffectedLimitRejectsWholeBatch(t *testing.T) {
	db, _, server := newTestServer(t)
	collection := db.Collection("items")
	for rank := 0; rank < 11; rank++ {
		insertServerDocument(t, collection, "mine", int64(rank), "original")
	}
	query := `{"version":1,"where":{"op":"true"}}`
	update := `{"version":1,"operations":[{"op":"set","path":"title","value":{"t":"string","v":"changed"}}]}`
	response := postMutation(t, server.URL, `{"version":1,"action":"updateMany","query":`+query+`,"update":`+update+`}`)
	defer response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status = %d body = %s", response.StatusCode, body)
	}
	cursor, err := collection.Find(context.Background(), meldbase.Filter{"title": "changed"})
	if err != nil {
		t.Fatal(err)
	}
	documents, err := cursor.All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(documents) != 0 {
		t.Fatalf("limited mutation partially changed %d documents", len(documents))
	}
}

func TestHTTPOriginAllowlistAndBoundedPreflight(t *testing.T) {
	_, _, server := newTestServer(t)
	preflight, _ := http.NewRequest(http.MethodOptions, server.URL+"/v1/collections/items/query", nil)
	preflight.Header.Set("origin", "http://localhost:5173")
	preflight.Header.Set("access-control-request-method", "POST")
	preflight.Header.Set("access-control-request-headers", "authorization, content-type")
	response, err := http.DefaultClient.Do(preflight)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent || response.Header.Get("access-control-allow-origin") != "http://localhost:5173" {
		t.Fatalf("preflight status=%d origin=%q", response.StatusCode, response.Header.Get("access-control-allow-origin"))
	}
	badHeader, _ := http.NewRequest(http.MethodOptions, server.URL+"/v1/collections/items/query", nil)
	badHeader.Header.Set("origin", "http://localhost:5173")
	badHeader.Header.Set("access-control-request-method", "POST")
	badHeader.Header.Set("access-control-request-headers", "x-unbounded-header")
	response, err = http.DefaultClient.Do(badHeader)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("bad preflight status = %d", response.StatusCode)
	}
	badOrigin, _ := http.NewRequest(http.MethodGet, server.URL+"/health", nil)
	badOrigin.Header.Set("origin", "https://attacker.example")
	response, err = http.DefaultClient.Do(badOrigin)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden || response.Header.Get("access-control-allow-origin") != "" {
		t.Fatalf("bad origin status=%d allow=%q", response.StatusCode, response.Header.Get("access-control-allow-origin"))
	}
}

func TestRealtimeTicketSubscriptionAndAtomicSnapshot(t *testing.T) {
	db, handler, server := newTestServer(t)
	collection := db.Collection("items")
	insertServerDocument(t, collection, "mine", 1, "one")
	ticketRequest, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/realtime/tickets", nil)
	ticketRequest.Header.Set("authorization", "Bearer valid")
	ticketResponse, err := http.DefaultClient.Do(ticketRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer ticketResponse.Body.Close()
	var ticket struct{ URL, Ticket string }
	if err := json.NewDecoder(ticketResponse.Body).Decode(&ticket); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, ticket.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "authenticate", "ticket": ticket.Ticket}); err != nil {
		t.Fatal(err)
	}
	authenticated := readMap(t, ctx, connection)
	if authenticated["type"] != "authenticated" {
		t.Fatalf("auth = %v", authenticated)
	}
	query := map[string]any{"version": 1, "where": map[string]any{"op": "true"}, "sort": []any{map[string]any{"path": "rank", "direction": 1}}}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "requestId": "request-1", "collection": "items", "query": query}); err != nil {
		t.Fatal(err)
	}
	initial := readSnapshot(t, ctx, connection)
	if len(initial.Documents) != 1 || initial.RequestID != "request-1" {
		t.Fatalf("initial = %+v", initial)
	}
	insertServerDocument(t, collection, "mine", 2, "two")
	next := readSnapshot(t, ctx, connection)
	if len(next.Documents) != 2 || next.Token == initial.Token {
		t.Fatalf("next = %+v", next)
	}
	if _, ok := handler.tickets.consume(ticket.Ticket); ok {
		t.Fatal("realtime ticket was reusable")
	}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "unsubscribe", "subscriptionId": next.SubscriptionID}); err != nil {
		t.Fatal(err)
	}
}

func TestRealtimeResumeUsesBoundSignedTokenAndRejectsTampering(t *testing.T) {
	db, _, server := newTestServer(t)
	insertServerDocument(t, db.Collection("items"), "mine", 1, "one")
	ticket := obtainTicket(t, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, ticket.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "authenticate", "ticket": ticket.Ticket}); err != nil {
		t.Fatal(err)
	}
	_ = readMap(t, ctx, connection)
	query := map[string]any{"version": 1, "where": map[string]any{"op": "true"}}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "requestId": "initial", "collection": "items", "query": query}); err != nil {
		t.Fatal(err)
	}
	initial := readSnapshot(t, ctx, connection)
	if initial.Token == "" || strings.Contains(initial.Token, "volatile:") {
		t.Fatalf("token = %q", initial.Token)
	}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "unsubscribe", "subscriptionId": initial.SubscriptionID}); err != nil {
		t.Fatal(err)
	}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "requestId": "resume", "collection": "items", "query": query, "resumeToken": initial.Token}); err != nil {
		t.Fatal(err)
	}
	resumed := readSnapshot(t, ctx, connection)
	if resumed.RequestID != "resume" || len(resumed.Documents) != 1 {
		t.Fatalf("resumed = %+v", resumed)
	}
	tampered := initial.Token[:len(initial.Token)-1] + "x"
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "requestId": "tampered", "collection": "items", "query": query, "resumeToken": tampered}); err != nil {
		t.Fatal(err)
	}
	message := readMap(t, ctx, connection)
	if message["type"] != "resync_required" {
		t.Fatalf("message = %v", message)
	}
}

func TestStrictMessagesAndTicketTTLConfiguration(t *testing.T) {
	db := meldbase.New()
	t.Cleanup(func() { _ = db.Close() })
	if _, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://example/realtime", TicketTTL: 6 * time.Minute}); err == nil {
		t.Fatal("expected excessive ticket TTL error")
	}
	if err := meldbase.ValidateStrictJSON([]byte(`{"v":1,"v":1}`), 100); err == nil {
		t.Fatal("duplicate JSON key accepted")
	}
	var target struct {
		V int `json:"v"`
	}
	if err := decodeStrict([]byte(`{"v":1,"extra":true}`), &target); err == nil {
		t.Fatal("unknown message field accepted")
	}
}

type ticketPayload struct{ URL, Ticket string }

func obtainTicket(t *testing.T, serverURL string) ticketPayload {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, serverURL+"/v1/realtime/tickets", nil)
	request.Header.Set("authorization", "Bearer valid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var ticket ticketPayload
	if err := json.NewDecoder(response.Body).Decode(&ticket); err != nil {
		t.Fatal(err)
	}
	return ticket
}

func postMutation(t *testing.T, serverURL, body string) *http.Response {
	t.Helper()
	if err := meldbase.ValidateStrictJSON([]byte(body), 1<<16); err != nil {
		t.Fatalf("invalid test mutation JSON: %v: %s", err, body)
	}
	request, _ := http.NewRequest(http.MethodPost, serverURL+"/v1/collections/items/mutations", strings.NewReader(body))
	request.Header.Set("authorization", "Bearer valid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func newTestServer(t *testing.T) (*meldbase.DB, *Handler, *httptest.Server) {
	t.Helper()
	db := meldbase.New()
	handler, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", AllowedHTTPOrigins: []string{"http://localhost:5173"}, TicketTTL: time.Minute, ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"), MaxBodyBytes: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	handler.config.PublicRealtimeURL = "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime"
	t.Cleanup(func() { server.Close(); _ = db.Close() })
	return db, handler, server
}

func insertServerDocument(t *testing.T, collection *meldbase.Collection, tenant string, rank int64, title string) meldbase.DocumentID {
	t.Helper()
	id, err := collection.InsertOne(context.Background(), meldbase.Document{"tenant": meldbase.String(tenant), "rank": meldbase.Int(rank), "title": meldbase.String(title)})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func readMap(t *testing.T, ctx context.Context, connection *websocket.Conn) map[string]any {
	t.Helper()
	raw, err := readSocketJSON(ctx, connection, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	var message map[string]any
	if err := json.Unmarshal(raw, &message); err != nil {
		t.Fatal(err)
	}
	return message
}

type snapshotMessage struct {
	RequestID      string            `json:"requestId"`
	SubscriptionID string            `json:"subscriptionId"`
	Token          string            `json:"token"`
	Documents      []json.RawMessage `json:"documents"`
}

func readSnapshot(t *testing.T, ctx context.Context, connection *websocket.Conn) snapshotMessage {
	t.Helper()
	raw, err := readSocketJSON(ctx, connection, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	var message snapshotMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		t.Fatal(err)
	}
	return message
}
