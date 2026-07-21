package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crapthings/meldbase/core"
)

type memoryIdempotencyRecord struct {
	claim    RPCIdempotencyClaim
	decision RPCIdempotencyDecision
}

type memoryIdempotencyStore struct {
	mu           sync.Mutex
	records      map[[64]byte]memoryIdempotencyRecord
	failClaim    bool
	failComplete bool
	failUnknown  bool
}

func newMemoryIdempotencyStore() *memoryIdempotencyStore {
	return &memoryIdempotencyStore{records: make(map[[64]byte]memoryIdempotencyRecord)}
}

func idempotencyRecordKey(claim RPCIdempotencyClaim) [64]byte {
	var key [64]byte
	copy(key[:32], claim.ScopeHash[:])
	copy(key[32:], claim.KeyHash[:])
	return key
}

func (store *memoryIdempotencyStore) Claim(_ context.Context, claim RPCIdempotencyClaim) (RPCIdempotencyDecision, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failClaim {
		return RPCIdempotencyDecision{}, errors.New("claim unavailable")
	}
	key := idempotencyRecordKey(claim)
	record, exists := store.records[key]
	if !exists {
		store.records[key] = memoryIdempotencyRecord{claim: claim, decision: RPCIdempotencyDecision{Kind: RPCIdempotencyExecute}}
		return RPCIdempotencyDecision{Kind: RPCIdempotencyExecute}, nil
	}
	if !sameRPCFingerprint(record.claim.Fingerprint, claim.Fingerprint) {
		return RPCIdempotencyDecision{Kind: RPCIdempotencyConflict}, nil
	}
	if record.decision.Kind == RPCIdempotencyExecute {
		if record.claim.SessionID == claim.SessionID {
			return RPCIdempotencyDecision{Kind: RPCIdempotencyInProgress}, nil
		}
		record.decision = RPCIdempotencyDecision{Kind: RPCIdempotencyOutcomeUnknown}
		store.records[key] = record
		return record.decision, nil
	}
	decision := record.decision
	decision.Result = append([]byte(nil), decision.Result...)
	return decision, nil
}

func (store *memoryIdempotencyStore) Complete(_ context.Context, completion RPCIdempotencyCompletion) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failComplete {
		return errors.New("completion unavailable")
	}
	key := idempotencyRecordKey(completion.Claim)
	record, exists := store.records[key]
	if !exists || record.decision.Kind != RPCIdempotencyExecute || record.claim.SessionID != completion.Claim.SessionID || record.claim.ClaimID != completion.Claim.ClaimID {
		return errors.New("claim ownership mismatch")
	}
	if len(completion.Result) > 0 {
		record.decision = RPCIdempotencyDecision{Kind: RPCIdempotencyReplayResult, Result: append([]byte(nil), completion.Result...)}
	} else {
		record.decision = RPCIdempotencyDecision{Kind: RPCIdempotencyReplayError, ErrorCode: completion.ErrorCode, ErrorStatus: completion.ErrorStatus}
	}
	store.records[key] = record
	return nil
}

func (store *memoryIdempotencyStore) MarkUnknown(_ context.Context, claim RPCIdempotencyClaim) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failUnknown {
		return errors.New("unknown transition unavailable")
	}
	key := idempotencyRecordKey(claim)
	record, exists := store.records[key]
	if !exists || record.decision.Kind != RPCIdempotencyExecute || record.claim.SessionID != claim.SessionID || record.claim.ClaimID != claim.ClaimID {
		return errors.New("claim ownership mismatch")
	}
	record.decision = RPCIdempotencyDecision{Kind: RPCIdempotencyOutcomeUnknown}
	store.records[key] = record
	return nil
}

func TestRPCIdempotencyReplaysCanonicalResultAndRejectsKeyReuse(t *testing.T) {
	store := newMemoryIdempotencyStore()
	var calls atomic.Uint64
	methods := map[string]RPCMethod{"echo": func(_ context.Context, _ Actor, arguments []meldbase.Value) (meldbase.Value, error) {
		calls.Add(1)
		return arguments[0], nil
	}}
	_, _, server := newRPCServer(t, methods, rpcTestAuthorizer{allow: true}, Config{RPCIdempotencyStore: store})
	key := "abcdefghijklmnopqrstuv"

	first := postIdempotentRPC(t, server.URL, "echo", key, []any{map[string]any{"t": "int64", "v": "7"}})
	assertRPCResultInt(t, first, 7)
	second := postIdempotentRPC(t, server.URL, "echo", key, []any{map[string]any{"t": "int64", "v": "7"}})
	assertRPCResultInt(t, second, 7)
	if calls.Load() != 1 {
		t.Fatalf("idempotent method calls=%d", calls.Load())
	}

	conflict := postIdempotentRPC(t, server.URL, "echo", key, []any{map[string]any{"t": "int64", "v": "8"}})
	assertRPCError(t, conflict, http.StatusConflict, "rpc_idempotency_conflict")
	if calls.Load() != 1 {
		t.Fatalf("conflicting key executed method: %d", calls.Load())
	}
}

func TestRPCIdempotencyReplaysApplicationErrorsAndFailsClosedWithoutStore(t *testing.T) {
	store := newMemoryIdempotencyStore()
	var calls atomic.Uint64
	method := func(context.Context, Actor, []meldbase.Value) (meldbase.Value, error) {
		calls.Add(1)
		return meldbase.Value{}, &RPCError{Code: "quota_exceeded"}
	}
	_, _, server := newRPCServer(t, map[string]RPCMethod{"fail": method}, rpcTestAuthorizer{allow: true}, Config{RPCIdempotencyStore: store})
	key := "abcdefghijklmnopqrstuv"
	assertRPCError(t, postIdempotentRPC(t, server.URL, "fail", key, []any{}), http.StatusBadRequest, "quota_exceeded")
	assertRPCError(t, postIdempotentRPC(t, server.URL, "fail", key, []any{}), http.StatusBadRequest, "quota_exceeded")
	if calls.Load() != 1 {
		t.Fatalf("application error calls=%d", calls.Load())
	}

	_, _, withoutStore := newRPCServer(t, map[string]RPCMethod{"fail": method}, rpcTestAuthorizer{allow: true}, Config{})
	assertRPCError(t, postIdempotentRPC(t, withoutStore.URL, "fail", key, []any{}), http.StatusServiceUnavailable, "rpc_idempotency_unavailable")
	if calls.Load() != 1 {
		t.Fatal("method ran when idempotency store was unavailable")
	}
}

func TestRPCIdempotencyConcurrentDuplicateIsInProgress(t *testing.T) {
	store := newMemoryIdempotencyStore()
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Uint64
	method := func(context.Context, Actor, []meldbase.Value) (meldbase.Value, error) {
		calls.Add(1)
		close(started)
		<-release
		return meldbase.String("done"), nil
	}
	_, _, server := newRPCServer(t, map[string]RPCMethod{"work": method}, rpcTestAuthorizer{allow: true}, Config{RPCIdempotencyStore: store})
	key := "abcdefghijklmnopqrstuv"
	firstDone := make(chan *http.Response, 1)
	go func() { firstDone <- postIdempotentRPC(t, server.URL, "work", key, []any{}) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first call did not start")
	}
	assertRPCError(t, postIdempotentRPC(t, server.URL, "work", key, []any{}), http.StatusConflict, "rpc_in_progress")
	close(release)
	response := <-firstDone
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("first status=%d calls=%d", response.StatusCode, calls.Load())
	}
}

func TestRPCIdempotencyCompletionFailureBecomesOutcomeUnknown(t *testing.T) {
	store := newMemoryIdempotencyStore()
	store.failComplete = true
	var calls atomic.Uint64
	method := func(context.Context, Actor, []meldbase.Value) (meldbase.Value, error) {
		calls.Add(1)
		return meldbase.String("charged"), nil
	}
	_, _, server := newRPCServer(t, map[string]RPCMethod{"charge": method}, rpcTestAuthorizer{allow: true}, Config{
		RPCIdempotencyStore: store, RPCIdempotencyCommitTimeout: time.Millisecond,
	})
	key := "abcdefghijklmnopqrstuv"
	assertRPCError(t, postIdempotentRPC(t, server.URL, "charge", key, []any{}), http.StatusConflict, "rpc_outcome_unknown")
	assertRPCError(t, postIdempotentRPC(t, server.URL, "charge", key, []any{}), http.StatusConflict, "rpc_outcome_unknown")
	if calls.Load() != 1 {
		t.Fatalf("unknown outcome re-executed: %d", calls.Load())
	}
}

func TestRPCIdempotencyReplaysAcrossHTTPAndWebSocketTransports(t *testing.T) {
	store := newMemoryIdempotencyStore()
	var calls atomic.Uint64
	method := func(context.Context, Actor, []meldbase.Value) (meldbase.Value, error) {
		calls.Add(1)
		return meldbase.Binary([]byte{0, 255}), nil
	}
	_, _, server := newRPCServer(t, map[string]RPCMethod{"receipt": method}, rpcTestAuthorizer{allow: true}, Config{RPCIdempotencyStore: store})
	key := "abcdefghijklmnopqrstuv"
	response := postIdempotentRPC(t, server.URL, "receipt", key, []any{})
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("HTTP status=%d body=%s", response.StatusCode, body)
	}

	connection, ctx := openAuthenticatedRPCSocket(t, server.URL)
	defer connection.CloseNow()
	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": 1, "type": "call", "requestId": "socket-attempt", "idempotencyKey": key, "method": "receipt", "arguments": []any{},
	}); err != nil {
		t.Fatal(err)
	}
	message := readMap(t, ctx, connection)
	if message["type"] != "result" || message["requestId"] != "socket-attempt" || calls.Load() != 1 {
		t.Fatalf("socket replay=%+v calls=%d", message, calls.Load())
	}
	encoded, err := json.Marshal(message["result"])
	if err != nil {
		t.Fatal(err)
	}
	value, err := meldbase.UnmarshalWireValue(encoded, meldbase.QueryLimits{})
	bytes, ok := value.BinaryValue()
	if err != nil || !ok || len(bytes) != 2 || bytes[0] != 0 || bytes[1] != 255 {
		t.Fatalf("socket typed replay=%v/%t err=%v", bytes, ok, err)
	}
}

func TestRPCIdempotencyCancellationIsPersistedAsOutcomeUnknown(t *testing.T) {
	store := newMemoryIdempotencyStore()
	started := make(chan struct{})
	var calls atomic.Uint64
	method := func(ctx context.Context, _ Actor, _ []meldbase.Value) (meldbase.Value, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return meldbase.Value{}, ctx.Err()
	}
	_, _, server := newRPCServer(t, map[string]RPCMethod{"slow": method}, rpcTestAuthorizer{allow: true}, Config{RPCIdempotencyStore: store})
	connection, ctx := openAuthenticatedRPCSocket(t, server.URL)
	defer connection.CloseNow()
	key := "abcdefghijklmnopqrstuv"
	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": 1, "type": "call", "requestId": "slow-attempt", "idempotencyKey": key, "method": "slow", "arguments": []any{},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("idempotent method did not start")
	}
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": 1, "type": "cancel", "requestId": "slow-attempt"}); err != nil {
		t.Fatal(err)
	}
	message := readMap(t, ctx, connection)
	if socketRPCErrorCode(message) != "rpc_outcome_unknown" {
		t.Fatalf("cancel result=%+v", message)
	}
	assertRPCError(t, postIdempotentRPC(t, server.URL, "slow", key, []any{}), http.StatusConflict, "rpc_outcome_unknown")
	if calls.Load() != 1 {
		t.Fatalf("canceled unknown call re-executed: %d", calls.Load())
	}
}

func TestRPCIdempotencyFingerprintIsCanonicalAndIdentityFramed(t *testing.T) {
	key := "abcdefghijklmnopqrstuv"
	session := [16]byte{1}
	envelope := rpcCallEnvelope{Method: "echo", IdempotencyKey: &key}
	left, err := newRPCIdempotencyClaim(Actor{TenantID: "ab", ID: "c"}, envelope, []meldbase.Value{
		meldbase.Object(meldbase.Document{"b": meldbase.Int(2), "a": meldbase.Int(1)}),
	}, session, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	right, err := newRPCIdempotencyClaim(Actor{TenantID: "ab", ID: "c"}, envelope, []meldbase.Value{
		meldbase.Object(meldbase.Document{"a": meldbase.Int(1), "b": meldbase.Int(2)}),
	}, session, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if left.Fingerprint != right.Fingerprint || left.ScopeHash != right.ScopeHash || left.KeyHash != right.KeyHash {
		t.Fatal("canonical equivalent claims produced different hashes")
	}
	otherScope, err := newRPCIdempotencyClaim(Actor{TenantID: "a", ID: "bc"}, envelope, []meldbase.Value{
		meldbase.Object(meldbase.Document{"a": meldbase.Int(1), "b": meldbase.Int(2)}),
	}, session, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if left.ScopeHash == otherScope.ScopeHash {
		t.Fatal("length framing allowed an identity boundary collision")
	}
}

func TestRPCIdempotencyEnvelopeRejectsWeakOrMalformedKeys(t *testing.T) {
	arguments := `"arguments":[]`
	for _, key := range []string{"", "too-short", "contains spaces and is long", strings.Repeat("a", 129)} {
		raw := []byte(`{"v":1,"type":"call","requestId":"r","method":"echo",` + arguments + `,"idempotencyKey":"` + key + `"}`)
		if _, err := decodeRPCCallEnvelope(raw, 32); err == nil {
			t.Fatalf("accepted idempotency key %q", key)
		}
	}
}

func postIdempotentRPC(t *testing.T, serverURL, method, key string, arguments []any) *http.Response {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"v": 1, "type": "call", "requestId": "attempt-1", "idempotencyKey": key, "method": method, "arguments": arguments,
	})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, serverURL+"/v1/rpc", strings.NewReader(string(body)))
	request.Header.Set("content-type", "application/json")
	request.Header.Set("authorization", "Bearer valid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func assertRPCResultInt(t *testing.T, response *http.Response, expected int64) {
	t.Helper()
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("result status=%d body=%s", response.StatusCode, body)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	value, err := meldbase.UnmarshalWireValue(envelope.Result, meldbase.QueryLimits{})
	actual, ok := value.Int64()
	if err != nil || !ok || actual != expected {
		t.Fatalf("result=%d/%t err=%v", actual, ok, err)
	}
}

func assertRPCError(t *testing.T, response *http.Response, status int, code string) {
	t.Helper()
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != status || !strings.Contains(string(body), `"code":"`+code+`"`) {
		t.Fatalf("error status=%d body=%s want=%d/%s", response.StatusCode, body, status, code)
	}
}
