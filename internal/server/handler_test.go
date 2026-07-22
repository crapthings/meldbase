package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/crapthings/meldbase/core"
)

type testAuthenticator struct{}

func (testAuthenticator) AuthenticateHTTP(request *http.Request) (Actor, error) {
	if request.Header.Get("authorization") != "Bearer valid" {
		return Actor{}, ErrUnauthenticated
	}
	return Actor{ID: "user-1", WorkspaceID: "mine"}, nil
}

type testAuthorizer struct{}

// aggregateOnlyFieldAuthorizer models a field that may be grouped but is not
// safe to return. Group keys must not turn aggregate capability into value
// enumeration.
type aggregateOnlyFieldAuthorizer struct{ testAuthorizer }

func (aggregateOnlyFieldAuthorizer) AuthorizeQuery(ctx context.Context, actor Actor, collection string, query meldbase.QuerySpec) (QueryPolicy, error) {
	policy, err := (testAuthorizer{}).AuthorizeQuery(ctx, actor, collection, query)
	if err != nil {
		return QueryPolicy{}, err
	}
	policy.AllowedAggregateFields = map[string]struct{}{"workspace": {}}
	return policy, nil
}

type leaseAuthorizer struct {
	testAuthorizer
	mu      sync.RWMutex
	version string
	lease   *QueryPolicyLease
}

func (authorizer *leaseAuthorizer) AuthorizeQuery(ctx context.Context, actor Actor, collection string, query meldbase.QuerySpec) (QueryPolicy, error) {
	policy, err := authorizer.testAuthorizer.AuthorizeQuery(ctx, actor, collection, query)
	if err != nil {
		return QueryPolicy{}, err
	}
	authorizer.mu.RLock()
	defer authorizer.mu.RUnlock()
	policy.PolicyVersion = authorizer.version
	policy.Lease = authorizer.lease
	return policy, nil
}

func (authorizer *leaseAuthorizer) set(version string, lease *QueryPolicyLease) {
	authorizer.mu.Lock()
	authorizer.version, authorizer.lease = version, lease
	authorizer.mu.Unlock()
}

func (testAuthorizer) AuthorizeQuery(_ context.Context, actor Actor, collection string, _ meldbase.QuerySpec) (QueryPolicy, error) {
	if collection != "items" || actor.ID != "user-1" {
		return QueryPolicy{}, ErrForbidden
	}
	constraint, err := meldbase.CompileQuery(meldbase.Filter{"workspace": actor.WorkspaceID}, meldbase.QueryOptions{})
	if err != nil {
		return QueryPolicy{}, err
	}
	return QueryPolicy{
		PolicyVersion: "test-v1", Constraint: &constraint, MaxResults: 10,
		AllowedQueryPaths:      map[string]struct{}{"rank": {}, "title": {}},
		AllowedAggregateFields: map[string]struct{}{"rank": {}},
		AllowedResultFields:    map[string]struct{}{"rank": {}, "title": {}},
	}, nil
}

func (testAuthorizer) AuthorizeInsert(_ context.Context, actor Actor, collection string, _ meldbase.Document) (InsertPolicy, error) {
	if collection != "items" || actor.ID != "user-1" {
		return InsertPolicy{}, ErrForbidden
	}
	return InsertPolicy{AllowedInputFields: map[string]struct{}{"rank": {}, "title": {}}, SetFields: meldbase.Document{"workspace": meldbase.String(actor.WorkspaceID)}, AllowedResultFields: map[string]struct{}{"rank": {}, "title": {}}}, nil
}

func (testAuthorizer) AuthorizeUpdate(ctx context.Context, actor Actor, collection string, query meldbase.QuerySpec, _ meldbase.MutationSpec) (UpdatePolicy, error) {
	base, err := (testAuthorizer{}).AuthorizeQuery(ctx, actor, collection, query)
	if err != nil {
		return UpdatePolicy{}, err
	}
	return UpdatePolicy{QueryPolicy: base, AllowedUpdatePaths: map[string]struct{}{"rank": {}, "title": {}}, MaxAffected: 10}, nil
}

func (testAuthorizer) AuthorizeDelete(ctx context.Context, actor Actor, collection string, query meldbase.QuerySpec) (DeletePolicy, error) {
	base, err := (testAuthorizer{}).AuthorizeQuery(ctx, actor, collection, query)
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
	if _, leaked := document["workspace"]; leaked {
		t.Fatal("workspace field leaked through projection")
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

	forbiddenQuery := `{"version":1,"query":{"version":1,"where":{"op":"exists","path":"workspace","value":true}}}`
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

func TestHTTPCountAppliesWorkspacePolicyAndCapsResult(t *testing.T) {
	db, _, server := newTestServer(t)
	collection := db.Collection("items")
	if err := collection.CreateIndex(context.Background(), "by_workspace", []meldbase.IndexField{{Field: "workspace", Order: 1}}, meldbase.IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	insertServerDocument(t, collection, "other", 1, "hidden")
	for rank := 0; rank < 12; rank++ {
		insertServerDocument(t, collection, "mine", int64(rank), "visible")
	}
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/collections/items/count", strings.NewReader(`{"version":1,"query":{"version":1,"where":{"op":"true"}}}`))
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
		Count  int  `json:"count"`
		Capped bool `json:"capped"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Count != 10 || !body.Capped {
		t.Fatalf("count response = %+v, want capped visible lower bound", body)
	}
	if db.Stats().Queries.IndexScans == 0 {
		t.Fatal("count did not use the workspace constraint index")
	}
}

func TestHTTPGroupCountAppliesWorkspacePolicyAndCapsResult(t *testing.T) {
	db, _, server := newTestServer(t)
	collection := db.Collection("items")
	if err := collection.CreateIndex(context.Background(), "by_workspace", []meldbase.IndexField{{Field: "workspace", Order: 1}}, meldbase.IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	insertServerDocument(t, collection, "other", 0, "hidden")
	for rank := 0; rank < 12; rank++ {
		insertServerDocument(t, collection, "mine", int64(rank%2), "visible")
	}
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/collections/items/group-count", strings.NewReader(`{"version":1,"query":{"version":1,"where":{"op":"true"}},"groupBy":"rank"}`))
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
		Groups []struct {
			Key   json.RawMessage `json:"key"`
			Count int             `json:"count"`
		} `json:"groups"`
		Capped bool `json:"capped"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if !body.Capped || len(body.Groups) != 2 {
		t.Fatalf("group count response = %+v, want two capped visible groups", body)
	}
	for _, group := range body.Groups {
		key, err := meldbase.UnmarshalWireValue(group.Key, meldbase.DefaultQueryLimits)
		if err != nil {
			t.Fatal(err)
		}
		rank, ok := key.Int64()
		if !ok || (rank != 0 && rank != 1) || group.Count != 5 {
			t.Fatalf("group = key %v count %d, want rank 0 or 1 with count 5", key, group.Count)
		}
	}

	forbidden, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/collections/items/group-count", strings.NewReader(`{"version":1,"query":{"version":1,"where":{"op":"true"}},"groupBy":"workspace"}`))
	forbidden.Header.Set("authorization", "Bearer valid")
	forbiddenResponse, err := http.DefaultClient.Do(forbidden)
	if err != nil {
		t.Fatal(err)
	}
	forbiddenResponse.Body.Close()
	if forbiddenResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("forbidden group status = %d", forbiddenResponse.StatusCode)
	}

	_, _, aggregateOnlyServer := newTestServerWithAuthorizer(t, aggregateOnlyFieldAuthorizer{})
	defer aggregateOnlyServer.Close()
	aggregateOnly, _ := http.NewRequest(http.MethodPost, aggregateOnlyServer.URL+"/v1/collections/items/group-count", strings.NewReader(`{"version":1,"query":{"version":1,"where":{"op":"true"}},"groupBy":"workspace"}`))
	aggregateOnly.Header.Set("authorization", "Bearer valid")
	aggregateOnlyResponse, err := http.DefaultClient.Do(aggregateOnly)
	if err != nil {
		t.Fatal(err)
	}
	aggregateOnlyResponse.Body.Close()
	if aggregateOnlyResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("aggregate-only group status = %d", aggregateOnlyResponse.StatusCode)
	}
	if db.Stats().Queries.IndexScans == 0 {
		t.Fatal("group count did not use the workspace constraint index")
	}
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
	if _, leaked := returned["workspace"]; leaked {
		t.Fatal("server-owned workspace leaked in response")
	}
	stored, err := db.Collection("items").FindOne(context.Background(), meldbase.Filter{"title": "created"})
	if err != nil {
		t.Fatal(err)
	}
	if workspace, _ := stored["workspace"].StringValue(); workspace != "mine" {
		t.Fatalf("workspace = %q", workspace)
	}

	forbiddenDocument := `{"t":"object","v":[["workspace",{"t":"string","v":"other"}],["title",{"t":"string","v":"attack"}]]}`
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
	other, err := collection.FindOne(context.Background(), meldbase.Filter{"workspace": "other"})
	if err != nil {
		t.Fatal(err)
	}
	title, _ := other["title"].StringValue()
	if title != "other" {
		t.Fatalf("cross-workspace update leaked, title = %q", title)
	}

	forbidden := `{"version":1,"operations":[{"op":"set","path":"workspace","value":{"t":"string","v":"other"}}]}`
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

func TestRealtimeOriginPatternsRejectUntrustedBrowserAndAllowConfiguredClient(t *testing.T) {
	db := meldbase.New()
	handler, err := New(Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime",
		OriginPatterns:    []string{"https://client.example", "[[]::1]:*"},
		TicketTTL:         time.Minute,
		ResumeTokenKey:    []byte("0123456789abcdef0123456789abcdef"),
		MaxBodyBytes:      1 << 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	handler.config.PublicRealtimeURL = "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime"
	t.Cleanup(func() { server.Close(); _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blockedTicket := obtainTicket(t, server.URL)
	_, response, err := websocket.Dial(ctx, blockedTicket.URL, &websocket.DialOptions{HTTPHeader: http.Header{
		"origin": []string{"https://attacker.example"},
	}})
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("untrusted origin err=%v response=%v", err, response)
	}
	schemeTicket := obtainTicket(t, server.URL)
	_, response, err = websocket.Dial(ctx, schemeTicket.URL, &websocket.DialOptions{HTTPHeader: http.Header{
		"origin": []string{"http://client.example"},
	}})
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong origin scheme err=%v response=%v", err, response)
	}

	allowedTicket := obtainTicket(t, server.URL)
	connection, response, err := websocket.Dial(ctx, allowedTicket.URL, &websocket.DialOptions{HTTPHeader: http.Header{
		"origin": []string{"https://client.example"},
	}})
	if err != nil {
		t.Fatalf("configured origin err=%v response=%v", err, response)
	}
	defer connection.CloseNow()

	loopbackTicket := obtainTicket(t, server.URL)
	loopback, response, err := websocket.Dial(ctx, loopbackTicket.URL, &websocket.DialOptions{HTTPHeader: http.Header{
		"origin": []string{"http://[::1]:5173"},
	}})
	if err != nil {
		t.Fatalf("escaped IPv6 origin pattern err=%v response=%v", err, response)
	}
	defer loopback.CloseNow()
}

func TestRealtimeConfiguredSchemePatternRejectsSameHostWrongScheme(t *testing.T) {
	_, handler, server := newTestServer(t)
	host := strings.TrimPrefix(server.URL, "http://")
	handler.config.OriginPatterns = []string{"https://" + host}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wrongSchemeTicket := obtainTicket(t, server.URL)
	_, response, err := websocket.Dial(ctx, wrongSchemeTicket.URL, &websocket.DialOptions{HTTPHeader: http.Header{
		"origin": []string{"http://" + host},
	}})
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("same-host wrong-scheme err=%v response=%v", err, response)
	}

	allowedTicket := obtainTicket(t, server.URL)
	connection, response, err := websocket.Dial(ctx, allowedTicket.URL, &websocket.DialOptions{HTTPHeader: http.Header{
		"origin": []string{"https://" + host},
	}})
	if err != nil {
		t.Fatalf("same-host configured scheme err=%v response=%v", err, response)
	}
	defer connection.CloseNow()
}

func TestLivenessAndReadinessHaveDistinctFailStopSemantics(t *testing.T) {
	db, handler, server := newTestServer(t)
	for _, path := range []string{"/health", "/readyz"} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var body probeResponse
		decodeErr := json.NewDecoder(response.Body).Decode(&body)
		response.Body.Close()
		if decodeErr != nil || response.StatusCode != http.StatusOK || body.Version != 1 || body.Status != "ready" ||
			body.Readable == nil || !*body.Readable || body.Writable == nil || !*body.Writable ||
			response.Header.Get("cache-control") != "no-store" {
			t.Fatalf("ready %s status=%d body=%+v err=%v headers=%v", path, response.StatusCode, body, decodeErr, response.Header)
		}
	}
	handler.operationalState = func() meldbase.OperationalState {
		return meldbase.OperationalState{Readable: true, Writable: false}
	}
	response, err := http.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	var failStop probeResponse
	decodeErr := json.NewDecoder(response.Body).Decode(&failStop)
	response.Body.Close()
	if decodeErr != nil || response.StatusCode != http.StatusServiceUnavailable || failStop.Status != "not_ready" ||
		failStop.Readable == nil || !*failStop.Readable || failStop.Writable == nil || *failStop.Writable {
		t.Fatalf("fail-stop readiness status=%d body=%+v err=%v", response.StatusCode, failStop, decodeErr)
	}
	handler.operationalState = db.OperationalState
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/health", "/readyz"} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var body probeResponse
		decodeErr := json.NewDecoder(response.Body).Decode(&body)
		response.Body.Close()
		if decodeErr != nil || response.StatusCode != http.StatusServiceUnavailable || body.Status != "not_ready" ||
			body.Readable == nil || *body.Readable || body.Writable == nil || *body.Writable {
			t.Fatalf("closed %s status=%d body=%+v err=%v", path, response.StatusCode, body, decodeErr)
		}
	}
	response, err = http.Get(server.URL + "/livez")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var live probeResponse
	if err := json.NewDecoder(response.Body).Decode(&live); err != nil || response.StatusCode != http.StatusOK ||
		live.Version != 1 || live.Status != "live" || live.Readable != nil || live.Writable != nil {
		t.Fatalf("live status=%d body=%+v err=%v", response.StatusCode, live, err)
	}
}

func TestEngineAvailabilityErrorsUseStableServiceUnavailableCode(t *testing.T) {
	for _, err := range []error{meldbase.ErrClosed, meldbase.ErrDurability, errors.Join(errors.New("disk"), meldbase.ErrDurability)} {
		status, code := engineErrorStatusCode(err)
		if status != http.StatusServiceUnavailable || code != "database_unavailable" {
			t.Fatalf("classification for %v = %d %q", err, status, code)
		}
	}

	db, _, server := newTestServer(t)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	requests := []struct{ path, body string }{
		{"/v1/collections/items/query", `{"version":1,"query":{"version":1,"where":{"op":"true"}}}`},
		{"/v1/collections/items/documents", `{"version":1,"document":{"t":"object","v":[["title",{"t":"string","v":"x"}]]}}`},
		{"/v1/collections/items/mutations", `{"version":1,"action":"deleteOne","query":{"version":1,"where":{"op":"true"}}}`},
	}
	for _, item := range requests {
		request, _ := http.NewRequest(http.MethodPost, server.URL+item.path, strings.NewReader(item.body))
		request.Header.Set("authorization", "Bearer valid")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&body)
		response.Body.Close()
		if decodeErr != nil || response.StatusCode != http.StatusServiceUnavailable || body.Error.Code != "database_unavailable" {
			t.Fatalf("%s status=%d body=%+v decode=%v", item.path, response.StatusCode, body, decodeErr)
		}
	}
}

func TestEngineCommitOutcomeUnknownUsesStableReconciliationCode(t *testing.T) {
	status, code := engineErrorStatusCode(errors.Join(meldbase.ErrCommitOutcomeUnknown, context.Canceled))
	if status != http.StatusConflict || code != "rpc_outcome_unknown" {
		t.Fatalf("commit outcome classification=%d %q", status, code)
	}
}

func TestResourceLimitErrorsUseStableTerminalCode(t *testing.T) {
	status, code := engineErrorStatusCode(errors.Join(errors.New("oversized"), meldbase.ErrResourceLimit))
	if status != http.StatusRequestEntityTooLarge || code != "resource_limit_exceeded" {
		t.Fatalf("engine classification = %d %q", status, code)
	}
	status, code = classifyRPCError(meldbase.ErrResourceLimit)
	if status != http.StatusRequestEntityTooLarge || code != "resource_limit_exceeded" {
		t.Fatalf("RPC classification = %d %q", status, code)
	}
}

func TestUnsupportedTransactionalStorageIsDatabaseUnavailable(t *testing.T) {
	status, code := classifyRPCError(meldbase.ErrWriteTransactionUnsupported)
	if status != http.StatusServiceUnavailable || code != "database_unavailable" {
		t.Fatalf("transaction storage classification = %d %q", status, code)
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

func TestRealtimeOutboundFrameLimitClosesOnlySlowConnection(t *testing.T) {
	db, handler, server := newTestServer(t)
	handler.config.MaxRealtimeFrameBytes = 256
	handler.config.MaxRealtimeOutboundBytes = 512
	insertServerDocument(t, db.Collection("items"), "mine", 1, strings.Repeat("x", 1024))
	ticket := obtainTicket(t, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, ticket.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "authenticate", "ticket": ticket.Ticket}); err != nil {
		t.Fatal(err)
	}
	if message := readMap(t, ctx, connection); message["type"] != "authenticated" {
		t.Fatalf("auth=%+v", message)
	}
	query := map[string]any{"version": 1, "where": map[string]any{"op": "true"}}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "requestId": "too-large", "collection": "items", "query": query}); err != nil {
		t.Fatal(err)
	}
	message := readMap(t, ctx, connection)
	errorBody, _ := message["error"].(map[string]any)
	if message["type"] != "error" || errorBody["code"] != "resource_limit_exceeded" {
		t.Fatalf("oversized snapshot result=%+v", message)
	}
	if stats := handler.Stats(); stats.RealtimeOutboundOverflows != 0 {
		t.Fatalf("result budget should reject before outbound queue: %+v", stats)
	}
}

func TestRealtimeDeltaResultBudgetRejectsBeforeOutboundQueue(t *testing.T) {
	db, handler, server := newTestServer(t)
	handler.config.MaxRealtimeFrameBytes = 1024
	handler.config.MaxRealtimeOutboundBytes = 2048
	id := insertServerDocument(t, db.Collection("items"), "mine", 1, "small")
	ticket := obtainTicket(t, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "mode": "delta", "requestId": "delta-too-large", "collection": "items", "query": query}); err != nil {
		t.Fatal(err)
	}
	_ = readSnapshot(t, ctx, connection)
	if _, err := db.Collection("items").UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"title": strings.Repeat("x", 2048)}}); err != nil {
		t.Fatal(err)
	}
	message := readMap(t, ctx, connection)
	errorBody, _ := message["error"].(map[string]any)
	if message["type"] != "error" || errorBody["code"] != "resource_limit_exceeded" || message["requestId"] != "delta-too-large" {
		t.Fatalf("oversized delta result=%+v", message)
	}
	if stats := handler.Stats(); stats.RealtimeOutboundOverflows != 0 {
		t.Fatalf("delta budget should reject before outbound queue: %+v", stats)
	}
}

func TestHTTPQueryResultBudgetRejectsBeforeResponseMarshal(t *testing.T) {
	db, handler, server := newTestServer(t)
	handler.config.MaxQueryResultBytes = 128
	insertServerDocument(t, db.Collection("items"), "other", 0, "hidden")
	insertServerDocument(t, db.Collection("items"), "mine", 1, strings.Repeat("x", 1024))
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/collections/items/query", strings.NewReader(`{"version":1,"query":{"version":1,"where":{"op":"true"},"sort":[{"path":"rank","direction":1}],"limit":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("authorization", "Bearer valid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		responseBody, _ := io.ReadAll(response.Body)
		t.Fatalf("status=%d body=%s", response.StatusCode, responseBody)
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "resource_limit_exceeded" {
		t.Fatalf("code=%q", body.Error.Code)
	}
}

func TestRealtimeDeltaModeUsesVisibilityOverlayAndOpaqueTokenChain(t *testing.T) {
	db, _, server := newTestServer(t)
	collection := db.Collection("items")
	id := insertServerDocument(t, collection, "mine", 1, "one")
	if _, err := collection.UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"secret": "initial-secret"}}); err != nil {
		t.Fatal(err)
	}
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
	query := map[string]any{"version": 1, "where": map[string]any{"op": "true"}, "sort": []any{map[string]any{"path": "rank", "direction": 1}}}
	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": 1, "type": "subscribe", "mode": "delta", "requestId": "delta-1", "collection": "items", "query": query,
	}); err != nil {
		t.Fatal(err)
	}
	initial := readSnapshot(t, ctx, connection)
	if len(initial.Documents) != 1 || initial.Token == "" {
		t.Fatalf("initial = %+v", initial)
	}
	initialDocument, err := meldbase.UnmarshalWireDocument(initial.Documents[0], meldbase.DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, leaked := initialDocument["workspace"]; leaked {
		t.Fatal("workspace leaked in delta initial snapshot")
	}
	if _, leaked := initialDocument["secret"]; leaked {
		t.Fatal("secret leaked in delta initial snapshot")
	}

	// This first commit changes only a hidden field. The overlay must suppress
	// it, and the next visible delta must still chain from the client's initial
	// opaque token rather than the hidden internal position.
	if _, err := collection.UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"secret": "changed-secret"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"title": "visible-change"}}); err != nil {
		t.Fatal(err)
	}
	message := readMap(t, ctx, connection)
	if message["type"] != "delta" || message["fromToken"] != initial.Token || message["token"] == initial.Token {
		t.Fatalf("delta envelope = %+v", message)
	}
	operations, ok := message["operations"].([]any)
	if !ok || len(operations) != 1 {
		t.Fatalf("delta operations = %#v", message["operations"])
	}
	operation, ok := operations[0].(map[string]any)
	if !ok || operation["op"] != "change" || operation["id"] != id.String() {
		t.Fatalf("delta operation = %#v", operations[0])
	}
	rawDocument, err := json.Marshal(operation["document"])
	if err != nil {
		t.Fatal(err)
	}
	visible, err := meldbase.UnmarshalWireDocument(rawDocument, meldbase.DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	if title, _ := visible["title"].StringValue(); title != "visible-change" {
		t.Fatalf("visible title = %q", title)
	}
	if _, leaked := visible["workspace"]; leaked {
		t.Fatal("workspace leaked in delta change")
	}
	if _, leaked := visible["secret"]; leaked {
		t.Fatal("secret leaked in delta change")
	}
}

func TestRealtimePolicyLeaseRevocationForcesSafeResubscribe(t *testing.T) {
	for _, mode := range []string{"", "delta"} {
		name := "snapshot"
		if mode != "" {
			name = mode
		}
		t.Run(name, func(t *testing.T) {
			lease1, _ := NewQueryPolicyLease("policy-v1")
			authorizer := &leaseAuthorizer{version: "policy-v1", lease: lease1}
			db, _, server := newTestServerWithAuthorizer(t, authorizer)
			collection := db.Collection("items")
			id := insertServerDocument(t, collection, "mine", 1, "initial")
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
			subscribe := map[string]any{"v": 1, "type": "subscribe", "requestId": "policy", "collection": "items", "query": query}
			if mode != "" {
				subscribe["mode"] = mode
			}
			if err := writeSocketJSON(ctx, connection, subscribe); err != nil {
				t.Fatal(err)
			}
			initial := readMap(t, ctx, connection)
			if initial["type"] != "snapshot" {
				t.Fatalf("initial message = %+v", initial)
			}

			if err := lease1.Revoke(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := collection.UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"title": "after-revoke"}}); err != nil {
				t.Fatal(err)
			}
			resync := readMap(t, ctx, connection)
			if resync["type"] != "resync_required" || resync["requestId"] != "policy" {
				t.Fatalf("revocation message = %+v", resync)
			}

			lease2, _ := NewQueryPolicyLease("policy-store")
			authorizer.set("policy-store", lease2)
			if err := writeSocketJSON(ctx, connection, subscribe); err != nil {
				t.Fatal(err)
			}
			fresh := readMap(t, ctx, connection)
			if fresh["type"] != "snapshot" || fresh["requestId"] != "policy" {
				t.Fatalf("fresh subscription message = %+v", fresh)
			}
		})
	}
}

func TestHTTPQueryRejectsAlreadyRevokedPolicyLease(t *testing.T) {
	lease, _ := NewQueryPolicyLease("policy-v1")
	if err := lease.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	authorizer := &leaseAuthorizer{version: "policy-v1", lease: lease}
	_, _, server := newTestServerWithAuthorizer(t, authorizer)
	body := `{"version":1,"query":{"version":1,"where":{"op":"true"}}}`
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/collections/items/query", strings.NewReader(body))
	request.Header.Set("authorization", "Bearer valid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("status=%d body=%s", response.StatusCode, payload)
	}
}

func TestVisibilityOverlayAppliesHiddenAndStructuralDeltasIncrementally(t *testing.T) {
	id1, _ := meldbase.ParseDocumentID("00000000000000000000000000000001")
	id2, _ := meldbase.ParseDocumentID("00000000000000000000000000000002")
	id3, _ := meldbase.ParseDocumentID("00000000000000000000000000000003")
	document := func(id meldbase.DocumentID, title, secret string) meldbase.Document {
		return meldbase.Document{"_id": meldbase.ID(id), "title": meldbase.String(title), "secret": meldbase.String(secret)}
	}
	policy := QueryPolicy{AllowedResultFields: map[string]struct{}{"title": {}}}
	overlay, initial, err := newVisibilityOverlay(meldbase.QuerySnapshot{
		Token: 1, Documents: []meldbase.Document{document(id1, "one", "a"), document(id2, "two", "b")},
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(initial) != 2 {
		t.Fatalf("projected initial = %+v", initial)
	}
	if _, leaked := initial[0]["secret"]; leaked {
		t.Fatalf("projected initial = %+v", initial)
	}

	hidden, changed, err := overlay.apply(meldbase.QueryDelta{
		FromToken: 1, Token: 2,
		Operations: []meldbase.QueryDeltaOperation{{Kind: meldbase.QueryDeltaChange, DocumentID: id1, Document: document(id1, "one", "changed")}},
	}, policy)
	if err != nil || changed || len(hidden.Operations) != 0 {
		t.Fatalf("hidden delta = %+v changed=%v err=%v", hidden, changed, err)
	}

	visible, changed, err := overlay.apply(meldbase.QueryDelta{
		FromToken: 2, Token: 3,
		Operations: []meldbase.QueryDeltaOperation{
			{Kind: meldbase.QueryDeltaAdd, DocumentID: id3, BeforeID: id1, Document: document(id3, "three", "c")},
			{Kind: meldbase.QueryDeltaMove, DocumentID: id2, BeforeID: id1},
			{Kind: meldbase.QueryDeltaChange, DocumentID: id1, Document: document(id1, "changed", "changed-again")},
		},
	}, policy)
	if err != nil || !changed || visible.FromToken != 1 || visible.Token != 3 || len(visible.Operations) != 3 {
		t.Fatalf("visible delta = %+v changed=%v err=%v", visible, changed, err)
	}
	var order []meldbase.DocumentID
	for node := overlay.head; node != nil; node = node.next {
		order = append(order, node.id)
		if _, leaked := node.document["secret"]; leaked {
			t.Fatal("projected overlay retained a hidden field")
		}
	}
	if len(order) != 3 || order[0] != id3 || order[1] != id2 || order[2] != id1 {
		t.Fatalf("overlay order = %v", order)
	}
}

func BenchmarkVisibilityOverlayDeltaTenThousand(b *testing.B) {
	const count = 10_000
	documents := make([]meldbase.Document, count)
	var changedID meldbase.DocumentID
	for index := range documents {
		value := index + 1
		var id meldbase.DocumentID
		id[13], id[14], id[15] = byte(value>>16), byte(value>>8), byte(value)
		documents[index] = meldbase.Document{"_id": meldbase.ID(id), "title": meldbase.String("initial"), "secret": meldbase.String("hidden")}
		changedID = id
	}
	policy := QueryPolicy{AllowedResultFields: map[string]struct{}{"title": {}}}
	overlay, _, err := newVisibilityOverlay(meldbase.QuerySnapshot{Token: 1, Documents: documents}, policy)
	if err != nil {
		b.Fatal(err)
	}
	token := uint64(1)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		title := "a"
		if iteration%2 == 1 {
			title = "b"
		}
		next := token + 1
		_, visible, err := overlay.apply(meldbase.QueryDelta{
			FromToken: token, Token: next,
			Operations: []meldbase.QueryDeltaOperation{{
				Kind: meldbase.QueryDeltaChange, DocumentID: changedID,
				Document: meldbase.Document{"_id": meldbase.ID(changedID), "title": meldbase.String(title), "secret": meldbase.String("hidden")},
			}},
		}, policy)
		if err != nil || !visible {
			b.Fatalf("visible=%v err=%v", visible, err)
		}
		token = next
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
	resync := readMap(t, ctx, connection)
	if resync["type"] != "resync_required" || resync["requestId"] != "resume" {
		t.Fatalf("resync = %+v", resync)
	}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "requestId": "resume", "collection": "items", "query": query}); err != nil {
		t.Fatal(err)
	}
	resumed := readSnapshot(t, ctx, connection)
	if resumed.RequestID != "resume" || len(resumed.Documents) != 1 {
		t.Fatalf("fresh snapshot = %+v", resumed)
	}
	replacement := byte('A')
	if initial.Token[0] == replacement {
		replacement = 'B'
	}
	tampered := string(replacement) + initial.Token[1:]
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "requestId": "tampered", "collection": "items", "query": query, "resumeToken": tampered}); err != nil {
		t.Fatal(err)
	}
	message := readMap(t, ctx, connection)
	if message["type"] != "resync_required" {
		t.Fatalf("message = %v", message)
	}
}

type replaySourceStub struct {
	initial meldbase.QuerySnapshot
	deltas  chan meldbase.QueryDelta
	errors  chan error
	after   atomic.Uint64
}

func (source *replaySourceStub) OpenQueryReplay(_ context.Context, _ string, _ meldbase.QuerySpec, afterToken uint64, _ int) (*meldbase.QueryReplaySubscription, error) {
	source.after.Store(afterToken)
	return &meldbase.QueryReplaySubscription{Initial: source.initial, Deltas: source.deltas, Errors: source.errors}, nil
}

func TestRealtimeResumeAcknowledgesThenReplaysAndSafelyResyncs(t *testing.T) {
	db, handler, server := newTestServer(t)
	id := insertServerDocument(t, db.Collection("items"), "mine", 1, "one")
	source := &replaySourceStub{deltas: make(chan meldbase.QueryDelta, 1), errors: make(chan error, 1)}
	handler.config.ReplaySource = source
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
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "mode": "delta", "requestId": "initial", "collection": "items", "query": query}); err != nil {
		t.Fatal(err)
	}
	initial := readSnapshot(t, ctx, connection)
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "unsubscribe", "subscriptionId": initial.SubscriptionID}); err != nil {
		t.Fatal(err)
	}
	source.initial = meldbase.QuerySnapshot{Token: 1, Documents: []meldbase.Document{{
		"_id": meldbase.ID(id), "workspace": meldbase.String("mine"), "rank": meldbase.Int(1), "title": meldbase.String("one"),
	}}}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "mode": "delta", "requestId": "resume", "collection": "items", "query": query, "resumeToken": initial.Token}); err != nil {
		t.Fatal(err)
	}
	resumed := readMap(t, ctx, connection)
	if resumed["type"] != "resumed" || resumed["requestId"] != "resume" || resumed["token"] != initial.Token || resumed["subscriptionId"] == "" || source.after.Load() != 1 {
		t.Fatalf("resumed=%+v after=%d", resumed, source.after.Load())
	}
	source.deltas <- meldbase.QueryDelta{FromToken: 1, Token: 2, Operations: []meldbase.QueryDeltaOperation{{
		Kind: meldbase.QueryDeltaChange, DocumentID: id, Document: meldbase.Document{
			"_id": meldbase.ID(id), "workspace": meldbase.String("mine"), "rank": meldbase.Int(1), "title": meldbase.String("two"),
		},
	}}}
	delta := readMap(t, ctx, connection)
	if delta["type"] != "delta" || delta["requestId"] != "resume" || delta["subscriptionId"] != resumed["subscriptionId"] || delta["fromToken"] != initial.Token || delta["token"] == initial.Token {
		t.Fatalf("delta=%+v", delta)
	}
	source.errors <- meldbase.ErrHistoryLost
	resync := readMap(t, ctx, connection)
	if resync["type"] != "resync_required" || resync["requestId"] != "resume" {
		t.Fatalf("resync=%+v", resync)
	}
}

func TestRealtimeResumedSubscriptionHonorsPolicyLeaseRevocation(t *testing.T) {
	lease, err := NewQueryPolicyLease("policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	authorizer := &leaseAuthorizer{version: "policy-v1", lease: lease}
	db, handler, server := newTestServerWithAuthorizer(t, authorizer)
	id := insertServerDocument(t, db.Collection("items"), "mine", 1, "one")
	source := &replaySourceStub{deltas: make(chan meldbase.QueryDelta), errors: make(chan error)}
	handler.config.ReplaySource = source
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
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "mode": "delta", "requestId": "initial", "collection": "items", "query": query}); err != nil {
		t.Fatal(err)
	}
	initial := readSnapshot(t, ctx, connection)
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "unsubscribe", "subscriptionId": initial.SubscriptionID}); err != nil {
		t.Fatal(err)
	}
	source.initial = meldbase.QuerySnapshot{Token: 1, Documents: []meldbase.Document{{
		"_id": meldbase.ID(id), "workspace": meldbase.String("mine"), "rank": meldbase.Int(1), "title": meldbase.String("one"),
	}}}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "mode": "delta", "requestId": "resume-policy", "collection": "items", "query": query, "resumeToken": initial.Token}); err != nil {
		t.Fatal(err)
	}
	if resumed := readMap(t, ctx, connection); resumed["type"] != "resumed" {
		t.Fatalf("resumed=%+v", resumed)
	}
	if err := lease.Revoke(ctx); err != nil {
		t.Fatal(err)
	}
	resync := readMap(t, ctx, connection)
	if resync["type"] != "resync_required" || resync["requestId"] != "resume-policy" {
		t.Fatalf("resync=%+v", resync)
	}
}

func TestRealtimeResumeReplaysMissedDeltaEndToEnd(t *testing.T) {
	db, err := meldbase.Open(filepath.Join(t.TempDir(), "server-store.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", TicketTTL: time.Minute,
		ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"), MaxBodyBytes: 1 << 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	handler.config.PublicRealtimeURL = "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime"
	t.Cleanup(func() { server.Close(); _ = db.Close() })
	id := insertServerDocument(t, db.Collection("items"), "mine", 1, "one")
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
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "subscribe", "mode": "delta", "requestId": "store-initial", "collection": "items", "query": query}); err != nil {
		t.Fatal(err)
	}
	initial := readSnapshot(t, ctx, connection)
	connection.CloseNow()
	if _, err := db.Collection("items").UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"title": "missed"}}); err != nil {
		t.Fatal(err)
	}
	ticket = obtainTicket(t, server.URL)
	connection, _, err = websocket.Dial(ctx, ticket.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "authenticate", "ticket": ticket.Ticket}); err != nil {
		t.Fatal(err)
	}
	_ = readMap(t, ctx, connection)
	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": 1, "type": "subscribe", "mode": "delta", "requestId": "store-resume",
		"collection": "items", "query": query, "resumeToken": initial.Token,
	}); err != nil {
		t.Fatal(err)
	}
	resumed := readMap(t, ctx, connection)
	if resumed["type"] != "resumed" || resumed["token"] != initial.Token {
		t.Fatalf("resumed=%+v", resumed)
	}
	delta := readMap(t, ctx, connection)
	if delta["type"] != "delta" || delta["requestId"] != "store-resume" || delta["subscriptionId"] != resumed["subscriptionId"] || delta["fromToken"] != initial.Token || delta["token"] == initial.Token {
		t.Fatalf("delta=%+v", delta)
	}
	operations, ok := delta["operations"].([]any)
	if !ok || len(operations) != 1 {
		t.Fatalf("operations=%#v", delta["operations"])
	}
	operation, ok := operations[0].(map[string]any)
	if !ok || operation["op"] != "change" || operation["id"] != id.String() {
		t.Fatalf("operation=%#v", operations[0])
	}
}

func TestStrictMessagesAndTicketTTLConfiguration(t *testing.T) {
	db := meldbase.New()
	t.Cleanup(func() { _ = db.Close() })
	if _, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://example/realtime", TicketTTL: 6 * time.Minute}); err == nil {
		t.Fatal("expected excessive ticket TTL error")
	}
	if _, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://example/realtime", MaxRealtimeFrameBytes: 16<<20 + 1}); err == nil {
		t.Fatal("expected excessive realtime frame limit error")
	}
	if _, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://example/realtime", MaxRealtimeFrameBytes: 1023}); err == nil {
		t.Fatal("expected too-small realtime frame limit error")
	}
	if _, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://example/realtime", MaxRealtimeFrameBytes: 1024, MaxRealtimeOutboundBytes: 1023}); err == nil {
		t.Fatal("expected invalid realtime outbound limit error")
	}
	if _, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://example/realtime", MaxQueryResultBytes: 16<<20 + 1}); err == nil {
		t.Fatal("expected excessive query result limit error")
	}
	for _, pattern := range []string{"*", "*:*", "https://*", "wss://*:*"} {
		if _, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://example/realtime", OriginPatterns: []string{pattern}}); err == nil {
			t.Fatalf("unrestricted realtime origin pattern %q was accepted", pattern)
		}
	}
	if _, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://example/realtime", OriginPatterns: []string{"[broken"}}); err == nil {
		t.Fatal("expected invalid realtime origin pattern error")
	}
	if _, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://example/realtime", OriginPatterns: []string{"[[]::1]:*"}}); err != nil {
		t.Fatalf("escaped IPv6 origin pattern rejected: %v", err)
	}
	if _, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, PublicRealtimeURL: "ws://example/realtime", OriginPatterns: []string{"https://*.example.com"}}); err != nil {
		t.Fatalf("scoped realtime origin wildcard rejected: %v", err)
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

func TestSocketSessionOutboundByteBudgetBoundsQueuedFrames(t *testing.T) {
	message := map[string]any{"v": protocolVersion, "type": "snapshot", "payload": strings.Repeat("x", 128)}
	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := &Handler{config: Config{
		MaxRealtimeFrameBytes:    len(encoded),
		MaxRealtimeOutboundBytes: len(encoded)*2 - 1,
	}}
	session := &socketSession{
		handler: handler, ctx: ctx, cancel: cancel,
		outgoing: make(chan socketOutbound, 64),
	}
	if !session.enqueue(message) {
		t.Fatal("first frame was rejected")
	}
	if session.enqueue(message) {
		t.Fatal("second frame exceeded aggregate byte budget but was accepted")
	}
	if got := handler.metrics.realtimeOutboundOverflows.Load(); got != 1 {
		t.Fatalf("outbound overflows=%d", got)
	}
	if session.outgoingBytes != uint64(len(encoded)) {
		t.Fatalf("queued bytes=%d want=%d", session.outgoingBytes, len(encoded))
	}
	queued := <-session.outgoing
	session.releaseOutgoing(uint64(len(queued.data)))
	if session.outgoingBytes != 0 {
		t.Fatalf("written frame retained bytes=%d", session.outgoingBytes)
	}

	frameContext, frameCancel := context.WithCancel(context.Background())
	defer frameCancel()
	frameHandler := &Handler{config: Config{MaxRealtimeFrameBytes: len(encoded) - 1, MaxRealtimeOutboundBytes: len(encoded)}}
	frameSession := &socketSession{handler: frameHandler, ctx: frameContext, cancel: frameCancel, outgoing: make(chan socketOutbound, 1)}
	if frameSession.enqueue(message) {
		t.Fatal("oversized frame was accepted")
	}
	if got := frameHandler.metrics.realtimeOutboundOverflows.Load(); got != 1 {
		t.Fatalf("frame overflow=%d", got)
	}
}

func TestPublicDataAndRealtimeIngressRejectDuplicateJSONKeys(t *testing.T) {
	_, handler, server := newTestServer(t)
	for _, scenario := range []struct {
		path string
		body string
	}{
		{path: "/v1/collections/items/query", body: `{"version":1,"version":1,"query":{"version":1,"where":{"op":"true"}}}`},
		{path: "/v1/collections/items/documents", body: `{"version":1,"document":{"t":"object","v":[],"v":[]}}`},
		{path: "/v1/collections/items/mutations", body: `{"version":1,"action":"deleteOne","query":{"version":1,"where":{"op":"true","op":"true"}}}`},
	} {
		request, err := http.NewRequest(http.MethodPost, server.URL+scenario.path, strings.NewReader(scenario.body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("authorization", "Bearer valid")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), `"code":"invalid_json"`) {
			t.Fatalf("%s accepted duplicate key: status=%d body=%s", scenario.path, response.StatusCode, body)
		}
	}

	ticket := obtainTicket(t, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, ticket.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	duplicateAuthentication := []byte(`{"v":1,"v":1,"type":"authenticate","ticket":"` + ticket.Ticket + `"}`)
	if err := connection.Write(ctx, websocket.MessageText, duplicateAuthentication); err != nil {
		t.Fatal(err)
	}
	if _, _, err := connection.Read(ctx); err == nil {
		t.Fatal("public realtime connection accepted a duplicate authentication key")
	}
	if stats := handler.Stats(); stats.ConnectionsAccepted != 0 || stats.ActiveConnections != 0 {
		t.Fatalf("invalid authentication reached an accepted realtime session: %+v", stats)
	}
}

func TestRealtimeTicketCapabilityDiscoveryIsExplicitAndFixed(t *testing.T) {
	_, _, server := newTestServer(t)
	issue := func(accept string) map[string]any {
		t.Helper()
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/realtime/tickets", nil)
		request.Header.Set("authorization", "Bearer valid")
		if accept != "" {
			request.Header.Set("accept", accept)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var payload map[string]any
		if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&payload) != nil {
			t.Fatalf("ticket status=%d payload=%+v", response.StatusCode, payload)
		}
		return payload
	}
	if legacy := issue(""); legacy["protocol"] != nil {
		t.Fatalf("legacy ticket unexpectedly changed shape: %+v", legacy)
	}
	discovered := issue("application/json, application/vnd.meldbase.realtime-ticket+json; capabilities=1")
	protocol, ok := discovered["protocol"].(map[string]any)
	if !ok {
		t.Fatalf("missing protocol descriptor: %+v", discovered)
	}
	if versions, ok := protocol["versions"].([]any); !ok || len(versions) != 1 || versions[0] != float64(1) {
		t.Fatalf("protocol versions=%#v", protocol["versions"])
	}
	capabilities, ok := protocol["capabilities"].([]any)
	want := []string{"query.delta", "query.resume", "rpc", "rpc.cancel"}
	if !ok || len(capabilities) != len(want) {
		t.Fatalf("protocol capabilities=%#v", protocol["capabilities"])
	}
	for index, capability := range want {
		if capabilities[index] != capability {
			t.Fatalf("protocol capability %d=%#v want=%q", index, capabilities[index], capability)
		}
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
	return newTestServerWithAuthorizer(t, testAuthorizer{})
}

func newTestServerWithAuthorizer(t *testing.T, authorizer Authorizer) (*meldbase.DB, *Handler, *httptest.Server) {
	t.Helper()
	db := meldbase.New()
	handler, err := New(Config{DB: db, Authenticator: testAuthenticator{}, Authorizer: authorizer, PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", AllowedHTTPOrigins: []string{"http://localhost:5173"}, TicketTTL: time.Minute, ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"), MaxBodyBytes: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	handler.config.PublicRealtimeURL = "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime"
	t.Cleanup(func() { server.Close(); _ = db.Close() })
	return db, handler, server
}

func insertServerDocument(t *testing.T, collection *meldbase.Collection, workspace string, rank int64, title string) meldbase.DocumentID {
	t.Helper()
	id, err := collection.InsertOne(context.Background(), meldbase.Document{"workspace": meldbase.String(workspace), "rank": meldbase.Int(rank), "title": meldbase.String(title)})
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
