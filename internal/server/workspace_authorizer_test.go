package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/crapthings/meldbase/core"
)

func TestWorkspaceAuthorizerEnforcesIsolationAcrossHTTPReadsAndWrites(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	authenticator, err := NewHS256JWTAuthenticator(HS256JWTAuthenticatorConfig{Secret: secret, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewWorkspaceAuthorizer(WorkspaceAuthorizerConfig{Collections: []string{"tasks"}, WorkspaceField: "workspaceId"})
	if err != nil {
		t.Fatal(err)
	}
	db := meldbase.New()
	handler, err := New(Config{
		DB: db, Authenticator: authenticator, Authorizer: authorizer, PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime",
		TicketTTL: time.Minute, ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"), MaxBodyBytes: 1 << 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(handler)
	t.Cleanup(func() { api.Close(); _ = db.Close() })

	collection := db.Collection("tasks")
	insertWorkspaceDocument(t, collection, "team-a", "private-a")
	insertWorkspaceDocument(t, collection, "team-b", "private-b")
	tokenA := signedHS256JWT(t, secret, map[string]any{"sub": "user-a", "workspace_id": "team-a", "exp": now.Add(time.Minute).Unix()})

	query := postWorkspaceRequest(t, api.URL+"/v1/collections/tasks/query", tokenA, `{"version":1,"query":{"version":1,"where":{"op":"true"}}}`)
	defer query.Body.Close()
	if query.StatusCode != http.StatusOK {
		t.Fatalf("query status=%d", query.StatusCode)
	}
	var queried struct {
		Documents []json.RawMessage `json:"documents"`
	}
	if err := json.NewDecoder(query.Body).Decode(&queried); err != nil {
		t.Fatal(err)
	}
	if len(queried.Documents) != 1 {
		t.Fatalf("query returned %d documents, want 1", len(queried.Documents))
	}
	returned, err := meldbase.UnmarshalWireDocument(queried.Documents[0], meldbase.DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	if workspace, _ := returned["workspaceId"].StringValue(); workspace != "team-a" {
		t.Fatalf("read workspace=%q", workspace)
	}

	forgedInsert := postWorkspaceRequest(t, api.URL+"/v1/collections/tasks/documents", tokenA, `{"version":1,"document":{"t":"object","v":[["workspaceId",{"t":"string","v":"team-b"}],["title",{"t":"string","v":"created"}]]}}`)
	if forgedInsert.StatusCode != http.StatusCreated {
		forgedInsert.Body.Close()
		t.Fatalf("insert status=%d", forgedInsert.StatusCode)
	}
	forgedInsert.Body.Close()
	created, err := collection.FindOne(context.Background(), meldbase.Filter{"title": "created"})
	if err != nil {
		t.Fatal(err)
	}
	if workspace, _ := created["workspaceId"].StringValue(); workspace != "team-a" {
		t.Fatalf("inserted workspace=%q", workspace)
	}

	forgedUpdate := postWorkspaceRequest(t, api.URL+"/v1/collections/tasks/mutations", tokenA, `{"version":1,"action":"updateMany","query":{"version":1,"where":{"op":"true"}},"update":{"version":1,"operations":[{"op":"set","path":"workspaceId","value":{"t":"string","v":"team-b"}}]}}`)
	forgedUpdate.Body.Close()
	if forgedUpdate.StatusCode != http.StatusForbidden {
		t.Fatalf("workspace update status=%d", forgedUpdate.StatusCode)
	}

	update := postWorkspaceRequest(t, api.URL+"/v1/collections/tasks/mutations", tokenA, `{"version":1,"action":"updateMany","query":{"version":1,"where":{"op":"true"}},"update":{"version":1,"operations":[{"op":"set","path":"title","value":{"t":"string","v":"updated"}}]}}`)
	defer update.Body.Close()
	if update.StatusCode != http.StatusOK {
		t.Fatalf("update status=%d", update.StatusCode)
	}
	teamB, err := collection.FindOne(context.Background(), meldbase.Filter{"workspaceId": "team-b"})
	if err != nil {
		t.Fatal(err)
	}
	if title, _ := teamB["title"].StringValue(); title != "private-b" {
		t.Fatalf("cross-workspace update changed title to %q", title)
	}
}

func TestCollectionAccessModesEnforceOwnerAndRPCOnlyBoundaries(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	authenticator, err := NewHS256JWTAuthenticator(HS256JWTAuthenticatorConfig{Secret: secret, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewWorkspaceAuthorizer(WorkspaceAuthorizerConfig{
		WorkspaceField: "workspaceId",
		CollectionAccess: []CollectionAccess{
			{Collection: "tasks", Mode: CollectionAccessCollaborative},
			{Collection: "private_notes", Mode: CollectionAccessOwner, OwnerField: "ownerId", Fields: &CollectionAccessFields{
				QueryPaths: []string{"title"}, ResultFields: []string{"title"},
				InputFields: []string{"title"}, UpdatePaths: []string{"title"},
			}},
			{Collection: "payroll", Mode: CollectionAccessRPCOnly},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	db := meldbase.New()
	handler, err := New(Config{
		DB: db, Authenticator: authenticator, Authorizer: authorizer, PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime",
		TicketTTL: time.Minute, ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"), MaxBodyBytes: 1 << 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(handler)
	t.Cleanup(func() { api.Close(); _ = db.Close() })
	privateNotes := db.Collection("private_notes")
	insertWorkspaceOwnedDocument(t, privateNotes, "team-a", "user-a", "visible")
	insertWorkspaceOwnedDocument(t, privateNotes, "team-a", "user-b", "other-owner")
	insertWorkspaceOwnedDocument(t, privateNotes, "team-b", "user-a", "other-workspace")
	tokenA := signedHS256JWT(t, secret, map[string]any{"sub": "user-a", "workspace_id": "team-a", "exp": now.Add(time.Minute).Unix()})

	query := postWorkspaceRequest(t, api.URL+"/v1/collections/private_notes/query", tokenA, `{"version":1,"query":{"version":1,"where":{"op":"true"}}}`)
	defer query.Body.Close()
	if query.StatusCode != http.StatusOK {
		t.Fatalf("owner query status=%d", query.StatusCode)
	}
	var queried struct {
		Documents []json.RawMessage `json:"documents"`
	}
	if err := json.NewDecoder(query.Body).Decode(&queried); err != nil {
		t.Fatal(err)
	}
	if len(queried.Documents) != 1 {
		t.Fatalf("owner query returned %d documents, want 1", len(queried.Documents))
	}
	visible, err := meldbase.UnmarshalWireDocument(queried.Documents[0], meldbase.DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	if title, _ := visible["title"].StringValue(); title != "visible" {
		t.Fatalf("owner query title=%q", title)
	}
	if _, leaked := visible["ownerId"]; leaked {
		t.Fatalf("owner field leaked through result policy: %+v", visible)
	}
	forbiddenQuery := postWorkspaceRequest(t, api.URL+"/v1/collections/private_notes/query", tokenA, `{"version":1,"query":{"version":1,"where":{"op":"exists","path":"ownerId","value":true}}}`)
	forbiddenQuery.Body.Close()
	if forbiddenQuery.StatusCode != http.StatusForbidden {
		t.Fatalf("restricted query status=%d", forbiddenQuery.StatusCode)
	}
	forbiddenInput := postWorkspaceRequest(t, api.URL+"/v1/collections/private_notes/documents", tokenA, `{"version":1,"document":{"t":"object","v":[["title",{"t":"string","v":"forbidden"}],["body",{"t":"string","v":"private"}]]}}`)
	forbiddenInput.Body.Close()
	if forbiddenInput.StatusCode != http.StatusForbidden {
		t.Fatalf("restricted insert status=%d", forbiddenInput.StatusCode)
	}

	forgedInsert := postWorkspaceRequest(t, api.URL+"/v1/collections/private_notes/documents", tokenA, `{"version":1,"document":{"t":"object","v":[["workspaceId",{"t":"string","v":"team-b"}],["ownerId",{"t":"string","v":"user-b"}],["title",{"t":"string","v":"created"}]]}}`)
	if forgedInsert.StatusCode != http.StatusCreated {
		forgedInsert.Body.Close()
		t.Fatalf("owner insert status=%d", forgedInsert.StatusCode)
	}
	forgedInsert.Body.Close()
	created, err := privateNotes.FindOne(context.Background(), meldbase.Filter{"title": "created"})
	if err != nil {
		t.Fatal(err)
	}
	if workspace, _ := created["workspaceId"].StringValue(); workspace != "team-a" {
		t.Fatalf("created workspace=%q", workspace)
	}
	if owner, _ := created["ownerId"].StringValue(); owner != "user-a" {
		t.Fatalf("created owner=%q", owner)
	}

	forgedUpdate := postWorkspaceRequest(t, api.URL+"/v1/collections/private_notes/mutations", tokenA, `{"version":1,"action":"updateMany","query":{"version":1,"where":{"op":"true"}},"update":{"version":1,"operations":[{"op":"set","path":"ownerId","value":{"t":"string","v":"user-b"}}]}}`)
	forgedUpdate.Body.Close()
	if forgedUpdate.StatusCode != http.StatusForbidden {
		t.Fatalf("owner update status=%d", forgedUpdate.StatusCode)
	}
	forbiddenFieldUpdate := postWorkspaceRequest(t, api.URL+"/v1/collections/private_notes/mutations", tokenA, `{"version":1,"action":"updateMany","query":{"version":1,"where":{"op":"true"}},"update":{"version":1,"operations":[{"op":"set","path":"body","value":{"t":"string","v":"private"}}]}}`)
	forbiddenFieldUpdate.Body.Close()
	if forbiddenFieldUpdate.StatusCode != http.StatusForbidden {
		t.Fatalf("restricted update status=%d", forbiddenFieldUpdate.StatusCode)
	}

	rpcOnly := postWorkspaceRequest(t, api.URL+"/v1/collections/payroll/query", tokenA, `{"version":1,"query":{"version":1,"where":{"op":"true"}}}`)
	rpcOnly.Body.Close()
	if rpcOnly.StatusCode != http.StatusForbidden {
		t.Fatalf("rpc-only query status=%d", rpcOnly.StatusCode)
	}
}

func TestCollectionAccessManifestIsStrictAndValidatesModes(t *testing.T) {
	manifest, err := ParseCollectionAccessManifestJSON([]byte(`{
		"$schema": "https://crapthings.github.io/meldbase/schemas/collection-access-manifest-v1.schema.json",
		"version": 1,
		"workspaceField": "workspaceId",
		"collections": [
			{"collection": "tasks", "mode": "collaborative"},
			{"collection": "private_notes", "mode": "owner", "ownerField": "ownerId", "fields": {"queryPaths":["title"],"resultFields":["title"],"inputFields":["title"],"updatePaths":["title"]}},
			{"collection": "payroll", "mode": "rpc_only"}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	config, err := manifest.WorkspaceAuthorizerConfig()
	if err != nil || manifest.SchemaURL != CollectionAccessManifestSchemaURL || len(config.CollectionAccess) != 3 || config.CollectionAccess[1].Fields == nil || len(config.CollectionAccess[1].Fields.UpdatePaths) != 1 {
		t.Fatalf("manifest config=%+v err=%v", config, err)
	}
	for _, input := range []string{
		`{"version":1,"workspaceField":"workspaceId","collections":[{"collection":"private_notes","mode":"owner"}]}`,
		`{"version":1,"workspaceField":"workspaceId","collections":[{"collection":"tasks","mode":"collaborative","unexpected":true}]}`,
		`{"version":1,"workspaceField":"workspaceId","collections":[{"collection":"tasks","mode":"rpc_only","fields":{"resultFields":["title"]}}]}`,
		`{"$schema":"https://example.test/other.json","version":1,"workspaceField":"workspaceId","collections":[{"collection":"tasks","mode":"collaborative"}]}`,
		`{"version":2,"workspaceField":"workspaceId","collections":[{"collection":"tasks","mode":"collaborative"}]}`,
	} {
		if _, err := ParseCollectionAccessManifestJSON([]byte(input)); err == nil {
			t.Fatalf("manifest %s was accepted", input)
		}
	}
	emptyFields, err := NewWorkspaceAuthorizer(WorkspaceAuthorizerConfig{
		WorkspaceField: "workspaceId",
		CollectionAccess: []CollectionAccess{{
			Collection: "empty", Mode: CollectionAccessCollaborative, Fields: &CollectionAccessFields{ResultFields: []string{}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := emptyFields.AuthorizeQuery(context.Background(), Principal{Subject: "user", Tenant: "team"}, "empty", meldbase.QuerySpec{})
	if err != nil || policy.AllowAllResultFields || len(policy.AllowedResultFields) != 0 {
		t.Fatalf("explicit empty result fields policy=%+v err=%v", policy, err)
	}
}

func TestWorkspaceAuthorizerScopesRealtimeSubscriptionFromJWTTicket(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	authenticator, err := NewHS256JWTAuthenticator(HS256JWTAuthenticatorConfig{Secret: secret, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewWorkspaceAuthorizer(WorkspaceAuthorizerConfig{Collections: []string{"tasks"}, WorkspaceField: "workspaceId"})
	if err != nil {
		t.Fatal(err)
	}
	db := meldbase.New()
	handler, err := New(Config{
		DB: db, Authenticator: authenticator, Authorizer: authorizer, PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime",
		TicketTTL: time.Minute, ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"), MaxBodyBytes: 1 << 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(handler)
	handler.config.PublicRealtimeURL = "ws" + strings.TrimPrefix(api.URL, "http") + "/v1/realtime"
	t.Cleanup(func() { api.Close(); _ = db.Close() })
	insertWorkspaceDocument(t, db.Collection("tasks"), "team-a", "private-a")
	insertWorkspaceDocument(t, db.Collection("tasks"), "team-b", "private-b")
	tokenA := signedHS256JWT(t, secret, map[string]any{"sub": "user-a", "workspace_id": "team-a", "exp": now.Add(time.Minute).Unix()})

	ticketRequest, _ := http.NewRequest(http.MethodPost, api.URL+"/v1/realtime/tickets", nil)
	ticketRequest.Header.Set("Authorization", "Bearer "+tokenA)
	ticketResponse, err := http.DefaultClient.Do(ticketRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer ticketResponse.Body.Close()
	if ticketResponse.StatusCode != http.StatusOK {
		t.Fatalf("ticket status=%d", ticketResponse.StatusCode)
	}
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
	_ = readMap(t, ctx, connection)
	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": 1, "type": "subscribe", "mode": "delta", "requestId": "team-a", "collection": "tasks",
		"query": map[string]any{"version": 1, "where": map[string]any{"op": "true"}},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := readSnapshot(t, ctx, connection)
	if len(snapshot.Documents) != 1 {
		t.Fatalf("snapshot documents=%d, want 1", len(snapshot.Documents))
	}
	document, err := meldbase.UnmarshalWireDocument(snapshot.Documents[0], meldbase.DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	if workspace, _ := document["workspaceId"].StringValue(); workspace != "team-a" {
		t.Fatalf("realtime workspace=%q", workspace)
	}

	if err := connection.Close(websocket.StatusNormalClosure, "switch workspace"); err != nil {
		t.Fatal(err)
	}
	tokenB := signedHS256JWT(t, secret, map[string]any{"sub": "user-a", "workspace_id": "team-b", "exp": now.Add(time.Minute).Unix()})
	ticketRequest, _ = http.NewRequest(http.MethodPost, api.URL+"/v1/realtime/tickets", nil)
	ticketRequest.Header.Set("Authorization", "Bearer "+tokenB)
	ticketResponse, err = http.DefaultClient.Do(ticketRequest)
	if err != nil {
		t.Fatal(err)
	}
	var ticketB struct{ URL, Ticket string }
	if err := json.NewDecoder(ticketResponse.Body).Decode(&ticketB); err != nil {
		ticketResponse.Body.Close()
		t.Fatal(err)
	}
	ticketResponse.Body.Close()
	second, _, err := websocket.Dial(ctx, ticketB.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseNow()
	if err := writeSocketJSON(ctx, second, map[string]any{"v": 1, "type": "authenticate", "ticket": ticketB.Ticket}); err != nil {
		t.Fatal(err)
	}
	_ = readMap(t, ctx, second)
	if err := writeSocketJSON(ctx, second, map[string]any{
		"v": 1, "type": "subscribe", "mode": "delta", "requestId": "team-b", "collection": "tasks",
		"query": map[string]any{"version": 1, "where": map[string]any{"op": "true"}}, "resumeToken": snapshot.Token,
	}); err != nil {
		t.Fatal(err)
	}
	if resumed := readMap(t, ctx, second); resumed["type"] != "resync_required" {
		t.Fatalf("cross-workspace resume=%+v, want resync_required", resumed)
	}
}

type workspaceRPCAllowlist struct{}

func (workspaceRPCAllowlist) AuthorizeRPC(_ context.Context, principal Principal, method string) error {
	if principal.Subject != "user-a" || principal.Tenant != "team-a" || method != "workspace.echo" {
		return ErrForbidden
	}
	return nil
}

func TestJWTWorkspacePrincipalIsForwardedToTrustedWorkerRPC(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	authenticator, err := NewHS256JWTAuthenticator(HS256JWTAuthenticatorConfig{Secret: secret, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewWorkspaceAuthorizer(WorkspaceAuthorizerConfig{Collections: []string{"tasks"}, WorkspaceField: "workspaceId"})
	if err != nil {
		t.Fatal(err)
	}
	hub := newTestWorkerHub(t)
	control := httptest.NewServer(hub)
	defer control.Close()
	worker, workerContext := openTestWorker(t, control.URL, []map[string]any{{"name": "workspace.echo", "mode": "rpc"}})
	defer worker.CloseNow()
	db := meldbase.New()
	defer db.Close()
	handler, err := New(Config{
		DB: db, Authenticator: authenticator, Authorizer: authorizer, PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime",
		ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"), RPCMethodResolver: hub, RPCAuthorizer: workspaceRPCAllowlist{},
	})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(handler)
	defer api.Close()
	workerDone := make(chan error, 1)
	go func() {
		invoke := readMap(t, workerContext, worker)
		principal, _ := invoke["principal"].(map[string]any)
		if invoke["type"] != "invoke" || principal["subject"] != "user-a" || principal["tenant"] != "team-a" {
			workerDone <- context.Canceled
			return
		}
		arguments, _ := invoke["arguments"].([]any)
		workerDone <- writeSocketJSON(workerContext, worker, map[string]any{
			"v": 1, "type": "result", "callId": invoke["callId"], "result": arguments[0],
		})
	}()
	tokenA := signedHS256JWT(t, secret, map[string]any{"sub": "user-a", "workspace_id": "team-a", "exp": now.Add(time.Minute).Unix()})
	request, _ := http.NewRequest(http.MethodPost, api.URL+"/v1/rpc", strings.NewReader(`{"v":1,"type":"call","requestId":"workspace-call","method":"workspace.echo","arguments":[{"t":"string","v":"ok"}]}`))
	request.Header.Set("Authorization", "Bearer "+tokenA)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("worker RPC status=%d", response.StatusCode)
	}
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}

	tokenB := signedHS256JWT(t, secret, map[string]any{"sub": "user-a", "workspace_id": "team-b", "exp": now.Add(time.Minute).Unix()})
	request, _ = http.NewRequest(http.MethodPost, api.URL+"/v1/rpc", strings.NewReader(`{"v":1,"type":"call","requestId":"workspace-denied","method":"workspace.echo","arguments":[]}`))
	request.Header.Set("Authorization", "Bearer "+tokenB)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-workspace worker RPC status=%d", response.StatusCode)
	}
}

func insertWorkspaceDocument(t *testing.T, collection *meldbase.Collection, workspace, title string) {
	t.Helper()
	if _, err := collection.InsertOne(context.Background(), meldbase.Document{"workspaceId": meldbase.String(workspace), "title": meldbase.String(title)}); err != nil {
		t.Fatal(err)
	}
}

func insertWorkspaceOwnedDocument(t *testing.T, collection *meldbase.Collection, workspace, owner, title string) {
	t.Helper()
	if _, err := collection.InsertOne(context.Background(), meldbase.Document{
		"workspaceId": meldbase.String(workspace), "ownerId": meldbase.String(owner), "title": meldbase.String(title),
	}); err != nil {
		t.Fatal(err)
	}
}

func postWorkspaceRequest(t *testing.T, url, token, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
