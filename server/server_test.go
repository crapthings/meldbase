package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/crapthings/meldbase"
	"github.com/crapthings/meldbase/server"
)

// authenticator assigns the test user to every HTTP request.
type authenticator struct{}

// AuthenticateHTTP returns the fixed test actor.
func (authenticator) AuthenticateHTTP(*http.Request) (server.Actor, error) {
	return server.Actor{ID: "user"}, nil
}

// authorizer denies document operations and allows only the test user to call echo.
type authorizer struct{}

// AuthorizeQuery denies every document query.
func (authorizer) AuthorizeQuery(context.Context, server.Actor, string, meldbase.QuerySpec) (server.QueryPolicy, error) {
	return server.QueryPolicy{}, server.ErrForbidden
}

// AuthorizeInsert denies every document insertion.
func (authorizer) AuthorizeInsert(context.Context, server.Actor, string, meldbase.Document) (server.InsertPolicy, error) {
	return server.InsertPolicy{}, server.ErrForbidden
}

// AuthorizeUpdate denies every document update.
func (authorizer) AuthorizeUpdate(context.Context, server.Actor, string, meldbase.QuerySpec, meldbase.MutationSpec) (server.UpdatePolicy, error) {
	return server.UpdatePolicy{}, server.ErrForbidden
}

// AuthorizeDelete denies every document deletion.
func (authorizer) AuthorizeDelete(context.Context, server.Actor, string, meldbase.QuerySpec) (server.DeletePolicy, error) {
	return server.DeletePolicy{}, server.ErrForbidden
}

// AuthorizeRPC allows only the test user to call the echo method.
func (authorizer) AuthorizeRPC(_ context.Context, actor server.Actor, method string) error {
	if actor.ID != "user" || method != "echo" {
		return server.ErrForbidden
	}
	return nil
}

// TestPublicServerFacadeCanRegisterAndCallRPC checks that the public server API can serve a registered RPC method over HTTP.
func TestPublicServerFacadeCanRegisterAndCallRPC(t *testing.T) {
	if server.ProtocolVersion != 1 {
		t.Fatalf("public protocol version=%d", server.ProtocolVersion)
	}
	db := meldbase.New()
	defer db.Close()
	handler, err := server.New(server.Config{
		DB: db, Authenticator: authenticator{}, Authorizer: authorizer{}, RPCAuthorizer: authorizer{},
		PublicRealtimeURL: "ws://example.invalid/v1/realtime",
		RPCMethods: map[string]server.RPCMethod{
			"echo": func(_ context.Context, _ server.Actor, input meldbase.Value) (meldbase.Value, error) {
				return input, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	response, err := http.Post(httpServer.URL+"/v1/rpc", "application/json", strings.NewReader(`{"v":1,"type":"call","requestId":"public-1","method":"echo","input":{"t":"date","v":"2026-07-16T00:00:00.000Z"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", response.StatusCode)
	}
}
