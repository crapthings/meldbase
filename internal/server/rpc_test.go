package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/crapthings/meldbase/core"
)

type rpcTestAuthorizer struct{ allow bool }

func (authorizer rpcTestAuthorizer) AuthorizeRPC(_ context.Context, actor Actor, method string) error {
	if !authorizer.allow || actor.ID != "user-1" || method == "forbidden" {
		return ErrForbidden
	}
	return nil
}

func TestRPCUsesTypedValuesExplicitAuthorizationAndSafeErrors(t *testing.T) {
	var calls atomic.Uint64
	methods := map[string]RPCMethod{
		"math.add": func(_ context.Context, actor Actor, input meldbase.Value) (meldbase.Value, error) {
			calls.Add(1)
			values, ok := input.ArrayValue()
			if actor.WorkspaceID != "mine" || !ok || len(values) != 2 {
				return meldbase.Value{}, errors.New("bad invocation")
			}
			left, leftOK := values[0].Int64()
			right, rightOK := values[1].Int64()
			if !leftOK || !rightOK {
				return meldbase.Value{}, &MeldbaseError{Code: "math.invalid_input"}
			}
			return meldbase.Int(left + right), nil
		},
		"fails": func(context.Context, Actor, meldbase.Value) (meldbase.Value, error) {
			return meldbase.Value{}, errors.New("secret database detail")
		},
		"panics": func(context.Context, Actor, meldbase.Value) (meldbase.Value, error) {
			panic("secret panic detail")
		},
		"forbidden": func(context.Context, Actor, meldbase.Value) (meldbase.Value, error) {
			calls.Add(1)
			return meldbase.Null(), nil
		},
	}
	_, handler, server := newRPCServer(t, methods, rpcTestAuthorizer{allow: true}, Config{})

	response := postRPC(t, server.URL, "math.add", `{"version":1,"input":{"t":"array","v":[{"t":"int64","v":"9223372036854775800"},{"t":"int64","v":"7"}]}}`, true)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("success status=%d", response.StatusCode)
	}
	var success struct {
		Version   int             `json:"v"`
		Type      string          `json:"type"`
		RequestID string          `json:"requestId"`
		Result    json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&success); err != nil || success.Version != 1 || success.Type != "result" || success.RequestID != "request-1" {
		t.Fatalf("success=%+v err=%v", success, err)
	}
	value, err := meldbase.UnmarshalWireValue(success.Result, meldbase.QueryLimits{})
	if sum, ok := value.Int64(); err != nil || !ok || sum != 9223372036854775807 {
		t.Fatalf("result=%v/%t err=%v", sum, ok, err)
	}

	for _, scenario := range []struct {
		method string
		body   string
		auth   bool
		status int
		code   string
	}{
		{"math.add", `{"version":1,"input":{"t":"null"}}`, false, http.StatusUnauthorized, "unauthenticated"},
		{"missing", `{"version":1,"input":{"t":"null"}}`, true, http.StatusNotFound, "rpc_not_found"},
		{"forbidden", `{"version":1,"input":{"t":"null"}}`, true, http.StatusForbidden, "forbidden"},
		{"math.add", `{"version":1}`, true, http.StatusBadRequest, "invalid_rpc_envelope"},
		{"math.add", `{"version":1,"input":{"t":"unknown"}}`, true, http.StatusBadRequest, "invalid_rpc_argument"},
		{"math.add", `{"version":1,"input":{"t":"string","v":"x"}}`, true, http.StatusBadRequest, "math.invalid_input"},
		{"fails", `{"version":1,"input":{"t":"null"}}`, true, http.StatusInternalServerError, "internal"},
		{"panics", `{"version":1,"input":{"t":"null"}}`, true, http.StatusInternalServerError, "internal"},
	} {
		response := postRPC(t, server.URL, scenario.method, scenario.body, scenario.auth)
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		kind := "internal"
		if strings.Contains(scenario.code, ".") {
			kind = "business"
		}
		if response.StatusCode != scenario.status || !strings.Contains(string(body), `"kind":"`+kind+`"`) || !strings.Contains(string(body), `"code":"`+scenario.code+`"`) {
			t.Fatalf("%s status=%d body=%s", scenario.method, response.StatusCode, body)
		}
		if strings.Contains(string(body), "secret") {
			t.Fatalf("%s leaked internal error: %s", scenario.method, body)
		}
	}
	if calls.Load() != 2 { // successful call plus invalid-argument application call
		t.Fatalf("method calls=%d; unauthorized/forbidden calls executed", calls.Load())
	}
	stats := handler.Stats()
	if stats.RPCRequests != 7 || stats.RPCActive != 0 || stats.RPCSucceeded != 1 || stats.RPCFailed != 3 ||
		stats.RPCCanceled != 0 || stats.RPCRejected != 3 || stats.RPCBusy != 0 ||
		stats.RPCRequestBytes == 0 || stats.RPCResultBytes == 0 || stats.RPCTotalNanos == 0 || stats.RPCMaxLatency <= 0 {
		t.Fatalf("RPC stats=%+v", stats)
	}
}

func TestHTTPRPCIngressRejectsDuplicateJSONKeysBeforeMethodExecution(t *testing.T) {
	var calls atomic.Uint64
	method := func(context.Context, Actor, meldbase.Value) (meldbase.Value, error) {
		calls.Add(1)
		return meldbase.Null(), nil
	}
	_, handler, server := newRPCServer(t, map[string]RPCMethod{"safe": method}, rpcTestAuthorizer{allow: true}, Config{})
	for _, body := range []string{
		`{"v":1,"v":1,"type":"call","requestId":"duplicate-version","method":"safe","input":{"t":"null"}}`,
		`{"v":1,"type":"call","requestId":"duplicate-value","method":"safe","input":{"t":"string","v":"first","v":"second"}}`,
	} {
		request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/rpc", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("authorization", "Bearer valid")
		request.Header.Set("content-type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		encoded, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response.StatusCode != http.StatusBadRequest || !strings.Contains(string(encoded), `"code":"invalid_json"`) {
			t.Fatalf("duplicate RPC JSON status=%d body=%s", response.StatusCode, encoded)
		}
	}
	if calls.Load() != 0 || handler.Stats().RPCRequests != 0 {
		t.Fatalf("duplicate RPC input reached execution or request accounting: calls=%d stats=%+v", calls.Load(), handler.Stats())
	}
}

func TestRPCCallEnvelopeRejectsLegacyArgumentsField(t *testing.T) {
	_, err := decodeRPCCallEnvelope([]byte(`{"v":1,"type":"call","requestId":"legacy","method":"safe","input":{"t":"null"},"arguments":[]}`))
	if err == nil {
		t.Fatal("legacy arguments field was accepted")
	}
}

func TestRPCMapsDatabaseAvailabilityWithoutLeakingEngineErrors(t *testing.T) {
	method := func(context.Context, Actor, meldbase.Value) (meldbase.Value, error) {
		return meldbase.Value{}, errors.Join(errors.New("private disk detail"), meldbase.ErrDurability)
	}
	_, _, server := newRPCServer(t, map[string]RPCMethod{"failstop": method}, rpcTestAuthorizer{allow: true}, Config{})
	response := postRPC(t, server.URL, "failstop", `{"version":1,"input":{"t":"null"}}`, true)
	assertRPCError(t, response, http.StatusServiceUnavailable, "database_unavailable")
}

func TestRPCMapsCommitOutcomeUnknownWithoutInvitingRetry(t *testing.T) {
	method := func(context.Context, Actor, meldbase.Value) (meldbase.Value, error) {
		return meldbase.Value{}, errors.Join(meldbase.ErrCommitOutcomeUnknown, context.Canceled)
	}
	_, _, server := newRPCServer(t, map[string]RPCMethod{"uncertain": method}, rpcTestAuthorizer{allow: true}, Config{})
	response := postRPC(t, server.URL, "uncertain", `{"version":1,"input":{"t":"null"}}`, true)
	assertRPCError(t, response, http.StatusConflict, "rpc_outcome_unknown")
}

func TestRPCRegistryIsFrozenAndConcurrencyIsBounded(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	original := map[string]RPCMethod{
		"wait": func(ctx context.Context, _ Actor, _ meldbase.Value) (meldbase.Value, error) {
			close(started)
			select {
			case <-release:
				return meldbase.String("done"), nil
			case <-ctx.Done():
				return meldbase.Value{}, ctx.Err()
			}
		},
	}
	_, _, server := newRPCServer(t, original, rpcTestAuthorizer{allow: true}, Config{MaxConcurrentRPC: 1})
	delete(original, "wait")
	original["injected"] = func(context.Context, Actor, meldbase.Value) (meldbase.Value, error) {
		return meldbase.Null(), nil
	}

	firstDone := make(chan *http.Response, 1)
	go func() { firstDone <- postRPC(t, server.URL, "wait", `{"version":1,"input":{"t":"null"}}`, true) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first RPC did not start")
	}
	busy := postRPC(t, server.URL, "wait", `{"version":1,"input":{"t":"null"}}`, true)
	busyBody, _ := io.ReadAll(busy.Body)
	busy.Body.Close()
	if busy.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(busyBody), `"rpc_busy"`) {
		t.Fatalf("busy status=%d body=%s", busy.StatusCode, busyBody)
	}
	injected := postRPC(t, server.URL, "injected", `{"version":1,"input":{"t":"null"}}`, true)
	_ = injected.Body.Close()
	if injected.StatusCode != http.StatusNotFound {
		t.Fatalf("mutated registry method status=%d", injected.StatusCode)
	}
	close(release)
	first := <-firstDone
	defer first.Body.Close()
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first status=%d", first.StatusCode)
	}
}

func TestRPCClientCancellationPropagatesToMethodContext(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	methods := map[string]RPCMethod{
		"cancel": func(ctx context.Context, _ Actor, _ meldbase.Value) (meldbase.Value, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return meldbase.Value{}, ctx.Err()
		},
	}
	_, _, server := newRPCServer(t, methods, rpcTestAuthorizer{allow: true}, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/rpc", strings.NewReader(`{"v":1,"type":"call","requestId":"cancel-1","method":"cancel","input":{"t":"null"}}`))
	request.Header.Set("authorization", "Bearer valid")
	request.Header.Set("content-type", "application/json")
	done := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("RPC did not start")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("method context was not canceled")
	}
	if err := <-done; err == nil {
		t.Fatal("HTTP client did not observe cancellation")
	}
}

func TestRPCConfigurationRequiresExplicitSafeBounds(t *testing.T) {
	db := meldbase.New()
	defer db.Close()
	base := Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://example.invalid/v1/realtime",
		RPCMethods: map[string]RPCMethod{"ok": func(context.Context, Actor, meldbase.Value) (meldbase.Value, error) {
			return meldbase.Null(), nil
		}},
	}
	if _, err := New(base); err == nil {
		t.Fatal("registered RPC without authorizer")
	}
	base.RPCAuthorizer = rpcTestAuthorizer{allow: true}
	base.RPCMethods = map[string]RPCMethod{"bad/name": base.RPCMethods["ok"]}
	if _, err := New(base); err == nil {
		t.Fatal("invalid RPC method name accepted")
	}
	base.RPCMethods = map[string]RPCMethod{"ok": nil}
	if _, err := New(base); err == nil {
		t.Fatal("nil RPC method accepted")
	}
}

func TestWebSocketRPCUsesSameEnvelopeAndSurvivesApplicationErrors(t *testing.T) {
	methods := map[string]RPCMethod{
		"echo": func(_ context.Context, _ Actor, input meldbase.Value) (meldbase.Value, error) {
			return input, nil
		},
		"reject": func(context.Context, Actor, meldbase.Value) (meldbase.Value, error) {
			return meldbase.Value{}, &MeldbaseError{Code: "orders.not_ready"}
		},
	}
	_, _, server := newRPCServer(t, methods, rpcTestAuthorizer{allow: true}, Config{})
	connection, ctx := openAuthenticatedRPCSocket(t, server.URL)
	defer connection.Close(websocket.StatusNormalClosure, "")

	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": 1, "type": "call", "requestId": "ws-1", "method": "echo",
		"input": map[string]any{"t": "array", "v": []any{map[string]any{"t": "int64", "v": "9223372036854775807"}, map[string]any{"t": "binary", "v": "AP8="}}},
	}); err != nil {
		t.Fatal(err)
	}
	result := readMap(t, ctx, connection)
	if result["type"] != "result" || result["requestId"] != "ws-1" {
		t.Fatalf("result=%+v", result)
	}
	rawResult, err := json.Marshal(result["result"])
	if err != nil {
		t.Fatal(err)
	}
	value, err := meldbase.UnmarshalWireValue(rawResult, meldbase.QueryLimits{})
	values, ok := value.ArrayValue()
	if err != nil || !ok || len(values) != 2 {
		t.Fatalf("typed result=%+v/%t err=%v", values, ok, err)
	}
	if integer, ok := values[0].Int64(); !ok || integer != 9223372036854775807 {
		t.Fatalf("int64=%d/%t", integer, ok)
	}
	if binary, ok := values[1].BinaryValue(); !ok || len(binary) != 2 || binary[1] != 255 {
		t.Fatalf("binary=%v/%t", binary, ok)
	}

	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "call", "requestId": "ws-2", "method": "reject", "input": map[string]any{"t": "null"}}); err != nil {
		t.Fatal(err)
	}
	rejected := readMap(t, ctx, connection)
	errorBody, _ := rejected["error"].(map[string]any)
	if rejected["type"] != "error" || rejected["requestId"] != "ws-2" || errorBody["kind"] != "business" || errorBody["code"] != "orders.not_ready" {
		t.Fatalf("rejected=%+v", rejected)
	}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "ping"}); err != nil {
		t.Fatal(err)
	}
	if pong := readMap(t, ctx, connection); pong["type"] != "pong" {
		t.Fatalf("pong=%+v", pong)
	}
}

func TestWebSocketRPCCancelDuplicateLimitAndDisconnect(t *testing.T) {
	started := make(chan string, 4)
	canceled := make(chan string, 4)
	methods := map[string]RPCMethod{
		"wait": func(ctx context.Context, _ Actor, input meldbase.Value) (meldbase.Value, error) {
			name, _ := input.StringValue()
			started <- name
			<-ctx.Done()
			canceled <- name
			return meldbase.Value{}, ctx.Err()
		},
	}
	_, handler, server := newRPCServer(t, methods, rpcTestAuthorizer{allow: true}, Config{MaxConcurrentRPC: 2, MaxRPCPerConnection: 1})
	connection, ctx := openAuthenticatedRPCSocket(t, server.URL)

	call := func(requestID, name string) {
		t.Helper()
		if err := writeSocketJSON(ctx, connection, map[string]any{
			"v": 1, "type": "call", "requestId": requestID, "method": "wait",
			"input": map[string]any{"t": "string", "v": name},
		}); err != nil {
			t.Fatal(err)
		}
	}
	call("hold", "first")
	select {
	case name := <-started:
		if name != "first" {
			t.Fatalf("started=%q", name)
		}
	case <-time.After(time.Second):
		t.Fatal("first socket RPC did not start")
	}
	call("hold", "duplicate")
	duplicate := readMap(t, ctx, connection)
	if socketRPCErrorCode(duplicate) != "rpc_duplicate_request" {
		t.Fatalf("duplicate=%+v", duplicate)
	}
	call("second", "limited")
	limited := readMap(t, ctx, connection)
	if socketRPCErrorCode(limited) != "rpc_busy" {
		t.Fatalf("limited=%+v", limited)
	}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "cancel", "requestId": "hold"}); err != nil {
		t.Fatal(err)
	}
	if canceledFrame := readMap(t, ctx, connection); socketRPCErrorCode(canceledFrame) != "rpc_canceled" || canceledFrame["requestId"] != "hold" {
		t.Fatalf("canceled frame=%+v", canceledFrame)
	}
	select {
	case name := <-canceled:
		if name != "first" {
			t.Fatalf("canceled=%q", name)
		}
	case <-time.After(time.Second):
		t.Fatal("explicit cancel did not reach method")
	}

	// The completed request ID can be reused after its terminal frame.
	call("hold", "disconnect")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reused request ID did not start")
	}
	if err := connection.Close(websocket.StatusNormalClosure, "disconnect test"); err != nil {
		t.Fatal(err)
	}
	select {
	case name := <-canceled:
		if name != "disconnect" {
			t.Fatalf("disconnect canceled=%q", name)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnect did not cancel method")
	}
	deadline := time.Now().Add(time.Second)
	for handler.Stats().ActiveConnections != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stats := handler.Stats()
	if stats.ActiveConnections != 0 || stats.ConnectionsAccepted != 1 || stats.RPCRequests != 4 || stats.RPCActive != 0 ||
		stats.RPCCanceled != 2 || stats.RPCRejected != 1 || stats.RPCBusy != 1 {
		t.Fatalf("WebSocket RPC stats=%+v", stats)
	}
}

func newRPCServer(t *testing.T, methods map[string]RPCMethod, authorizer RPCAuthorizer, overrides Config) (*meldbase.DB, *Handler, *httptest.Server) {
	t.Helper()
	db := meldbase.New()
	config := Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"),
		MaxBodyBytes: 1 << 16, RPCMethods: methods, RPCAuthorizer: authorizer,
		MaxConcurrentRPC: overrides.MaxConcurrentRPC, MaxRPCPerConnection: overrides.MaxRPCPerConnection,
		MaxRPCResultBytes:           overrides.MaxRPCResultBytes,
		RPCIdempotencyStore:         overrides.RPCIdempotencyStore,
		RPCIdempotencyRetention:     overrides.RPCIdempotencyRetention,
		RPCIdempotencyCommitTimeout: overrides.RPCIdempotencyCommitTimeout,
	}
	handler, err := New(config)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	handler.config.PublicRealtimeURL = "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime"
	t.Cleanup(func() { server.Close(); _ = db.Close() })
	return db, handler, server
}

func openAuthenticatedRPCSocket(t *testing.T, serverURL string) (*websocket.Conn, context.Context) {
	t.Helper()
	ticket := obtainTicket(t, serverURL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	connection, _, err := websocket.Dial(ctx, ticket.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "authenticate", "ticket": ticket.Ticket}); err != nil {
		t.Fatal(err)
	}
	if authenticated := readMap(t, ctx, connection); authenticated["type"] != "authenticated" {
		t.Fatalf("authenticated=%+v", authenticated)
	}
	return connection, ctx
}

func socketRPCErrorCode(message map[string]any) string {
	errorBody, _ := message["error"].(map[string]any)
	code, _ := errorBody["code"].(string)
	return code
}

func postRPC(t *testing.T, serverURL, method, body string, authenticated bool) *http.Response {
	t.Helper()
	var legacy struct {
		Version int             `json:"version"`
		Input   json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal([]byte(body), &legacy); err != nil {
		t.Fatal(err)
	}
	var input any
	if len(legacy.Input) > 0 {
		input = json.RawMessage(legacy.Input)
	}
	envelope, err := json.Marshal(map[string]any{
		"v": legacy.Version, "type": "call", "requestId": "request-1", "method": method, "input": input,
	})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, serverURL+"/v1/rpc", strings.NewReader(string(envelope)))
	request.Header.Set("content-type", "application/json")
	if authenticated {
		request.Header.Set("authorization", "Bearer valid")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
