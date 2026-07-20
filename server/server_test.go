package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/server"
)

type authenticator struct{}

func (authenticator) AuthenticateHTTP(*http.Request) (server.Principal, error) {
	return server.Principal{Subject: "user"}, nil
}

type authorizer struct{}

func (authorizer) AuthorizeQuery(context.Context, server.Principal, string, meldbase.QuerySpec) (server.QueryPolicy, error) {
	return server.QueryPolicy{}, server.ErrForbidden
}
func (authorizer) AuthorizeInsert(context.Context, server.Principal, string, meldbase.Document) (server.InsertPolicy, error) {
	return server.InsertPolicy{}, server.ErrForbidden
}
func (authorizer) AuthorizeUpdate(context.Context, server.Principal, string, meldbase.QuerySpec, meldbase.MutationSpec) (server.UpdatePolicy, error) {
	return server.UpdatePolicy{}, server.ErrForbidden
}
func (authorizer) AuthorizeDelete(context.Context, server.Principal, string, meldbase.QuerySpec) (server.DeletePolicy, error) {
	return server.DeletePolicy{}, server.ErrForbidden
}
func (authorizer) AuthorizeRPC(_ context.Context, principal server.Principal, method string) error {
	if principal.Subject != "user" || method != "echo" {
		return server.ErrForbidden
	}
	return nil
}

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
			"echo": func(_ context.Context, _ server.Principal, arguments []meldbase.Value) (meldbase.Value, error) {
				return meldbase.Array(arguments...), nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	response, err := http.Post(httpServer.URL+"/v1/rpc", "application/json", strings.NewReader(`{"v":1,"type":"call","requestId":"public-1","method":"echo","arguments":[{"t":"date","v":"2026-07-16T00:00:00.000Z"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", response.StatusCode)
	}
}
