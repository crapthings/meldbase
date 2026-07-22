package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/crapthings/meldbase/core"
)

const testWorkerToken = "worker-control-token-0123456789abcdef"

type synchronizedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (buffer *synchronizedBuffer) Write(value []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.b.Write(value)
}

func (buffer *synchronizedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.b.String()
}

func TestWorkerHubServerSDKEndToEnd(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is required for the server SDK end-to-end test")
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "sdk", "server", "test", "worker-hub-e2e.mjs"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(script); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(script), "..", "dist", "index.js")); err != nil {
		t.Skip("server SDK is not built; run pnpm --filter @meldbase/server build first")
	}

	db, err := meldbase.Open(filepath.Join(t.TempDir(), "worker-sdk-e2e.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	policyStore, err := NewDurablePolicyGenerationStore(db)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := NewWorkerTokenAuthenticator(testWorkerToken)
	if err != nil {
		t.Fatal(err)
	}
	hub, err := NewWorkerHub(WorkerHubConfig{
		Authenticator: authenticator, PublicationCollections: []string{"items"}, PolicyGenerationStore: policyStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(hub)
	defer control.Close()
	idempotency, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	api, err := New(Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"),
		RPCMethodResolver: hub, RPCTransactionalMethodResolver: hub, QueryPolicyResolver: hub,
		RPCAuthorizer: rpcTestAuthorizer{allow: true}, RPCIdempotencyStore: idempotency,
	})
	if err != nil {
		t.Fatal(err)
	}
	public := httptest.NewServer(api)
	defer public.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var output synchronizedBuffer
	command := exec.CommandContext(ctx, node, script)
	command.Env = append(os.Environ(),
		"MELDBASE_WORKER_URL=ws"+strings.TrimPrefix(control.URL, "http"),
		"MELDBASE_WORKER_TOKEN="+testWorkerToken,
	)
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer cancel()
	defer func() {
		if err := command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("stop server SDK worker: %v", err)
		}
		if err := command.Wait(); err != nil {
			t.Errorf("server SDK worker exited: %v\n%s", err, output.String())
		}
		if err := ctx.Err(); err != nil {
			t.Errorf("server SDK worker shutdown exceeded deadline: %v\n%s", err, output.String())
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, registered := hub.ResolveRPCMethod("sdk.echo"); registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server SDK worker did not register\n%s", output.String())
		}
		time.Sleep(time.Millisecond)
	}
	response := postRPC(t, public.URL, "sdk.echo", `{"version":1,"arguments":[{"t":"int64","v":"42"}]}`, true)
	assertRPCResultInt(t, response, 42)
	transaction := postIdempotentRPC(t, public.URL, "sdk.create", "worker_sdk_e2e_key_0001", []any{})
	if transaction.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(transaction.Body)
		transaction.Body.Close()
		t.Fatalf("transaction status=%d body=%s", transaction.StatusCode, body)
	}
	transaction.Body.Close()
	if stats := db.Stats(); stats.Documents != 1 || stats.Collections != 1 {
		t.Fatalf("transaction did not commit through Go: %+v", stats)
	}
	exercise := postIdempotentRPC(t, public.URL, "sdk.exercise", "worker_sdk_e2e_key_0002", []any{})
	if exercise.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(exercise.Body)
		exercise.Body.Close()
		t.Fatalf("exercise status=%d body=%s", exercise.StatusCode, body)
	}
	exerciseBody, err := io.ReadAll(exercise.Body)
	exercise.Body.Close()
	if err != nil || !strings.Contains(string(exerciseBody), `"updated"`) {
		t.Fatalf("exercise response=%s error=%v", exerciseBody, err)
	}
	if stats := db.Stats(); stats.Documents != 2 || stats.Collections != 1 {
		t.Fatalf("transactional point-operation exercise did not delete its temporary document and commit its final write: %+v", stats)
	}
	request, err := http.NewRequest(http.MethodPost, public.URL+"/v1/collections/items/query", strings.NewReader(`{"version":1,"query":{"version":1,"where":{"op":"true"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("authorization", "Bearer valid")
	query, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer query.Body.Close()
	if query.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(query.Body)
		t.Fatalf("publication query status=%d body=%s", query.StatusCode, body)
	}
	var result struct {
		Documents []json.RawMessage `json:"documents"`
	}
	if err := json.NewDecoder(query.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if len(result.Documents) != 2 {
		t.Fatalf("publication response=%s", result.Documents)
	}
	documents := string(result.Documents[0]) + string(result.Documents[1])
	if !strings.Contains(documents, `"created"`) || !strings.Contains(documents, `"committed"`) || strings.Contains(documents, `"workspace"`) {
		t.Fatalf("publication response=%s", result.Documents)
	}
	if !strings.Contains(output.String(), "ready") {
		t.Fatalf("server SDK worker did not complete startup\n%s", output.String())
	}
}

func TestWorkerHubRoutesTypedRPCAndUnregistersOnDisconnect(t *testing.T) {
	hub := newTestWorkerHub(t)
	control := httptest.NewServer(hub)
	defer control.Close()
	worker, workerContext := openTestWorker(t, control.URL, []map[string]any{{"name": "math.echo", "mode": "rpc"}})

	db := meldbase.New()
	defer db.Close()
	handler, err := New(Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"),
		RPCMethodResolver: hub, RPCAuthorizer: rpcTestAuthorizer{allow: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(handler)
	defer api.Close()
	workerDone := make(chan error, 1)
	go func() {
		message := readMap(t, workerContext, worker)
		if message["type"] != "invoke" || message["method"] != "math.echo" || message["mode"] != "rpc" {
			workerDone <- io.ErrUnexpectedEOF
			return
		}
		actor, _ := message["actor"].(map[string]any)
		if actor["id"] != "user-1" {
			workerDone <- io.ErrUnexpectedEOF
			return
		}
		arguments, _ := message["arguments"].([]any)
		workerDone <- writeSocketJSON(workerContext, worker, map[string]any{
			"v": protocolVersion, "type": "result", "callId": message["callId"], "result": arguments[0],
		})
	}()
	response := postRPC(t, api.URL, "math.echo", `{"version":1,"arguments":[{"t":"int64","v":"42"}]}`, true)
	assertRPCResultInt(t, response, 42)
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	stats := hub.Stats()
	if stats.ConnectedWorkers != 1 || stats.RegisteredMethods != 1 || stats.CallsStarted != 1 || stats.CallsSucceeded != 1 || stats.CallsActive != 0 {
		t.Fatalf("worker stats=%+v", stats)
	}
	if err := worker.Close(websocket.StatusNormalClosure, "test complete"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for hub.Stats().ConnectedWorkers != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, ok := hub.ResolveRPCMethod("math.echo"); ok || hub.Stats().RegisteredMethods != 0 {
		t.Fatalf("disconnected worker remained registered: %+v", hub.Stats())
	}
	if failures := hub.Stats().ProtocolFailures; failures != 0 {
		t.Fatalf("normal worker disconnect counted as protocol failure: %d", failures)
	}
}

func TestWorkerHubCountsMalformedFrameAndUnregistersWorker(t *testing.T) {
	hub := newTestWorkerHub(t)
	control := httptest.NewServer(hub)
	defer control.Close()
	worker, workerContext := openTestWorker(t, control.URL, []map[string]any{{"name": "bad.frames", "mode": "rpc"}})
	if err := writeSocketJSON(workerContext, worker, map[string]any{"v": protocolVersion, "type": "unknown"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := worker.Read(workerContext); err == nil {
		t.Fatal("worker remained connected after malformed frame")
	}
	deadline := time.Now().Add(time.Second)
	for hub.Stats().ConnectedWorkers != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stats := hub.Stats()
	if stats.ConnectedWorkers != 0 || stats.RegisteredMethods != 0 || stats.ProtocolFailures != 1 {
		t.Fatalf("malformed frame stats=%+v", stats)
	}
}

func TestWorkerHubRejectsDuplicateJSONKeysBeforeRegistration(t *testing.T) {
	hub := newTestWorkerHub(t)
	control := httptest.NewServer(hub)
	defer control.Close()
	worker, ctx := dialTestWorker(t, control.URL)
	defer worker.CloseNow()
	frame := []byte(`{"v":2,"type":"register","workerId":"first","workerId":"second","methods":[]}`)
	if err := worker.Write(ctx, websocket.MessageText, frame); err != nil {
		t.Fatal(err)
	}
	if _, _, err := worker.Read(ctx); err == nil {
		t.Fatal("worker hub accepted a duplicate registration key")
	}
	deadline := time.Now().Add(time.Second)
	for hub.Stats().ProtocolFailures == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := hub.Stats(); stats.ConnectedWorkers != 0 || stats.RegisteredMethods != 0 || stats.ProtocolFailures != 1 {
		t.Fatalf("duplicate registration reached worker state: %+v", stats)
	}
}

func TestWorkerHubRequiresCapabilityDiscoveryAndUsesFixedV1Descriptor(t *testing.T) {
	hub := newTestWorkerHub(t)
	control := httptest.NewServer(hub)
	defer control.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, offered := range []string{"", "capabilities-v0"} {
		header := http.Header{"authorization": []string{"Bearer " + testWorkerToken}}
		if offered != "" {
			header.Set("Meldbase-Protocol", offered)
		}
		connection, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(control.URL, "http"), &websocket.DialOptions{HTTPHeader: header})
		if err == nil {
			_ = connection.CloseNow()
			t.Fatalf("worker protocol %q unexpectedly connected", offered)
		}
		if response == nil || response.StatusCode != http.StatusUpgradeRequired {
			t.Fatalf("worker protocol %q status=%v error=%v", offered, response, err)
		}
	}
	header := http.Header{"authorization": []string{"Bearer " + testWorkerToken}, "Meldbase-Protocol": []string{"capabilities-v1"}}
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(control.URL, "http"), &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": protocolVersion, "type": "register", "workerId": "capability-worker",
		"methods": []map[string]any{{"name": "echo", "mode": "rpc"}},
	}); err != nil {
		t.Fatal(err)
	}
	registered := readMap(t, ctx, connection)
	protocol, ok := registered["protocol"].(map[string]any)
	if !ok {
		t.Fatalf("registered protocol=%#v", registered)
	}
	versions, _ := protocol["versions"].([]any)
	capabilities, _ := protocol["capabilities"].([]any)
	if len(versions) != 1 || versions[0] != float64(protocolVersion) || len(capabilities) != 7 || capabilities[0] != "cancel" || capabilities[4] != "transaction.compiled_update" || capabilities[6] != "transaction.point_operations" {
		t.Fatalf("worker protocol=%+v", protocol)
	}
}

func TestWorkerHubTransactionalOpsCommitThroughGoAtomicPath(t *testing.T) {
	db, err := meldbase.Open(filepath.Join(t.TempDir(), "worker-transaction.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	policyStore, err := NewDurablePolicyGenerationStore(db)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, _ := NewWorkerTokenAuthenticator(testWorkerToken)
	hub, err := NewWorkerHub(WorkerHubConfig{
		Authenticator: authenticator, PublicationCollections: []string{"orders"}, PolicyGenerationStore: policyStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(hub)
	defer control.Close()
	worker, workerContext := dialTestWorker(t, control.URL)
	if err := writeSocketJSON(workerContext, worker, map[string]any{
		"v": protocolVersion, "type": "register", "workerId": "transaction-policy-worker",
		"methods": []map[string]any{
			{"name": "orders.create", "mode": "transactional"},
			{"name": "orders.invalidate", "mode": "transactional"},
		},
		"publications": []map[string]any{{
			"collection": "orders", "version": "orders-v1", "maxResults": 10,
			"queryPaths": "*", "resultFields": "*",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	readMap(t, workerContext, worker)
	hub.mu.RLock()
	initialPublication := hub.publications["orders"].publication
	hub.mu.RUnlock()
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"),
		RPCMethodResolver: hub, RPCTransactionalMethodResolver: hub, QueryPolicyResolver: hub,
		RPCAuthorizer: rpcTestAuthorizer{allow: true}, RPCIdempotencyStore: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(handler)
	defer api.Close()
	workerDone := make(chan error, 1)
	go func() {
		invoke := readMap(t, workerContext, worker)
		if invoke["type"] != "invoke" || invoke["mode"] != "transactional" {
			workerDone <- io.ErrUnexpectedEOF
			return
		}
		callID := invoke["callId"]
		document := map[string]any{"t": "object", "v": []any{
			[]any{"status", map[string]any{"t": "string", "v": "created"}},
		}}
		if err := writeSocketJSON(workerContext, worker, map[string]any{
			"v": protocolVersion, "type": "tx_op", "callId": callID, "opId": "insert-1", "operation": "insert", "collection": "orders", "document": document,
		}); err != nil {
			workerDone <- err
			return
		}
		inserted := readMap(t, workerContext, worker)
		result, _ := inserted["result"].(map[string]any)
		id, _ := result["v"].(string)
		if inserted["type"] != "tx_result" || len(id) != 32 {
			workerDone <- io.ErrUnexpectedEOF
			return
		}
		if err := writeSocketJSON(workerContext, worker, map[string]any{
			"v": protocolVersion, "type": "tx_op", "callId": callID, "opId": "get-1", "operation": "get", "collection": "orders", "id": id,
		}); err != nil {
			workerDone <- err
			return
		}
		readBack := readMap(t, workerContext, worker)
		if readBack["type"] != "tx_result" {
			workerDone <- io.ErrUnexpectedEOF
			return
		}
		if err := writeSocketJSON(workerContext, worker, map[string]any{
			"v": protocolVersion, "type": "tx_op", "callId": callID, "opId": "update-1", "operation": "update", "collection": "orders", "id": id,
			"mutation": map[string]any{"version": 1, "operations": []map[string]any{
				{"op": "set", "path": "status", "value": map[string]any{"t": "string", "v": "confirmed"}},
			}},
		}); err != nil {
			workerDone <- err
			return
		}
		updated := readMap(t, workerContext, worker)
		if updated["type"] != "tx_result" {
			workerDone <- io.ErrUnexpectedEOF
			return
		}
		if err := writeSocketJSON(workerContext, worker, map[string]any{
			"v": protocolVersion, "type": "tx_op", "callId": callID, "opId": "invalidate-1",
			"operation": "invalidate_publication", "collection": "orders",
		}); err != nil {
			workerDone <- err
			return
		}
		invalidated := readMap(t, workerContext, worker)
		if invalidated["type"] != "tx_result" {
			workerDone <- io.ErrUnexpectedEOF
			return
		}
		workerDone <- writeSocketJSON(workerContext, worker, map[string]any{
			"v": protocolVersion, "type": "result", "callId": callID, "result": map[string]any{"t": "string", "v": "worker-created"},
		})
	}()
	key := "workertransaction00001"
	if got := readRPCStringResult(t, postIdempotentRPC(t, api.URL, "orders.create", key, []any{})); got != "worker-created" {
		t.Fatalf("worker result=%q", got)
	}
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Documents != 1 || stats.Collections != 1 {
		t.Fatalf("atomic worker state=%+v", stats)
	}
	select {
	case <-initialPublication.lease.Done():
	case <-time.After(time.Second):
		t.Fatal("committed policy invalidation did not revoke the old lease")
	}
	hub.mu.RLock()
	committedPublication := hub.publications["orders"].publication
	hub.mu.RUnlock()
	if committedPublication.policyVersion == initialPublication.policyVersion || committedPublication.generation == [16]byte{} || !committedPublication.lease.Valid() {
		t.Fatalf("committed publication=%+v initial=%+v", committedPublication, initialPublication)
	}
	if durableGeneration, exists, err := policyStore.LoadPolicyGeneration(context.Background(), "orders"); err != nil || !exists || durableGeneration != committedPublication.generation {
		t.Fatalf("durable generation=%x exists=%v err=%v", durableGeneration, exists, err)
	}
	if document, err := db.Collection("orders").FindOne(context.Background(), meldbase.Filter{}); err != nil || !document["status"].Equal(meldbase.String("confirmed")) {
		t.Fatalf("updated worker document=%v err=%v", document, err)
	}
	if stats := hub.Stats(); stats.CallsSucceeded != 1 || stats.TransactionOps != 4 || stats.PolicyInvalidations != 1 {
		t.Fatalf("transaction worker stats=%+v", stats)
	}
	if got := readRPCStringResult(t, postIdempotentRPC(t, api.URL, "orders.create", key, []any{})); got != "worker-created" {
		t.Fatalf("worker replay=%q", got)
	}
	if stats := hub.Stats(); stats.CallsStarted != 1 || db.Stats().CommitSequence != 2 {
		t.Fatalf("replay invoked worker: hub=%+v db=%+v", stats, db.Stats())
	}

	invalidOnlyDone := make(chan error, 1)
	go func() {
		invoke := readMap(t, workerContext, worker)
		callID := invoke["callId"]
		if invoke["method"] != "orders.invalidate" {
			invalidOnlyDone <- io.ErrUnexpectedEOF
			return
		}
		if err := writeSocketJSON(workerContext, worker, map[string]any{
			"v": protocolVersion, "type": "tx_op", "callId": callID, "opId": "invalidate-only-1",
			"operation": "invalidate_publication", "collection": "orders",
		}); err != nil {
			invalidOnlyDone <- err
			return
		}
		if response := readMap(t, workerContext, worker); response["type"] != "tx_result" {
			invalidOnlyDone <- io.ErrUnexpectedEOF
			return
		}
		invalidOnlyDone <- writeSocketJSON(workerContext, worker, map[string]any{
			"v": protocolVersion, "type": "result", "callId": callID, "result": map[string]any{"t": "null"},
		})
	}()
	invalidOnlyKey := "workertransaction00002"
	assertRPCError(t, postIdempotentRPC(t, api.URL, "orders.invalidate", invalidOnlyKey, []any{}), http.StatusBadRequest, "rpc_transaction_requires_write")
	if err := <-invalidOnlyDone; err != nil {
		t.Fatal(err)
	}
	assertRPCError(t, postIdempotentRPC(t, api.URL, "orders.invalidate", invalidOnlyKey, []any{}), http.StatusBadRequest, "rpc_transaction_requires_write")
	if generation, exists, err := policyStore.LoadPolicyGeneration(context.Background(), "orders"); err != nil || !exists || generation != committedPublication.generation {
		t.Fatalf("invalid-only generation=%x exists=%v err=%v", generation, exists, err)
	}
	if stats := hub.Stats(); stats.CallsStarted != 2 || stats.PolicyInvalidations != 1 || stats.TransactionOps != 5 {
		t.Fatalf("invalid-only worker stats=%+v", stats)
	}
}

func TestWorkerTransactionUpdateRejectsAmbiguousAndMalformedFrames(t *testing.T) {
	db, err := meldbase.Open(filepath.Join(t.TempDir(), "worker-update-validation.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := meldbase.DocumentID{15: 1}
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"_id": meldbase.ID(id), "value": meldbase.Int(1)}); err != nil {
		t.Fatal(err)
	}
	err = db.RunWriteTransaction(context.Background(), func(tx *meldbase.WriteTransaction) error {
		validID := id.String()
		for _, test := range []struct {
			name  string
			frame workerTransactionOperationFrame
			want  error
		}{
			{
				name: "document-and-mutation", want: meldbase.ErrInvalidDocument,
				frame: workerTransactionOperationFrame{Operation: "update", Collection: "items", ID: validID,
					Document: json.RawMessage(`{"t":"object","v":[]}`), Mutation: json.RawMessage(`{"version":1,"operations":[{"op":"unset","path":"value"}]}`)},
			},
			{
				name: "unknown-mutation-field", want: meldbase.ErrInvalidUpdate,
				frame: workerTransactionOperationFrame{Operation: "update", Collection: "items", ID: validID,
					Mutation: json.RawMessage(`{"version":1,"operations":[{"op":"unset","path":"value","executable":"no"}]}`)},
			},
			{
				name: "mutation-on-get", want: meldbase.ErrInvalidDocument,
				frame: workerTransactionOperationFrame{Operation: "get", Collection: "items", ID: validID,
					Mutation: json.RawMessage(`{"version":1,"operations":[{"op":"unset","path":"value"}]}`)},
			},
		} {
			if _, operationErr := executeWorkerTransactionOperation(tx, test.frame); !errors.Is(operationErr, test.want) {
				return fmt.Errorf("%s err=%v want=%v", test.name, operationErr, test.want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats.CommitSequence != 1 || stats.Transactions.Noops != 1 {
		t.Fatalf("validation transaction stats=%+v", stats)
	}
}

func TestWorkerPublicationNarrowsBasePolicyAndRevokesOnDisconnect(t *testing.T) {
	authenticator, err := NewWorkerTokenAuthenticator(testWorkerToken)
	if err != nil {
		t.Fatal(err)
	}
	hub, err := NewWorkerHub(WorkerHubConfig{Authenticator: authenticator, PublicationCollections: []string{"items"}})
	if err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(hub)
	defer control.Close()
	worker, workerContext := dialTestWorker(t, control.URL)
	if err := writeSocketJSON(workerContext, worker, map[string]any{
		"v": protocolVersion, "type": "register", "workerId": "publication-worker", "methods": []any{},
		"publications": []map[string]any{{
			"collection": "items", "version": "items-v1", "maxResults": 1,
			"queryPaths": []string{"title"}, "resultFields": []string{"title"},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if registered := readMap(t, workerContext, worker); registered["type"] != "registered" {
		t.Fatalf("registered=%+v", registered)
	}

	db := meldbase.New()
	defer db.Close()
	insertServerDocument(t, db.Collection("items"), "mine", 1, "visible")
	insertServerDocument(t, db.Collection("items"), "mine", 2, "hidden")
	insertServerDocument(t, db.Collection("items"), "other", 3, "visible")
	handler, err := New(Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{}, QueryPolicyResolver: hub,
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(handler)
	defer api.Close()
	workerDone := make(chan error, 1)
	go func() {
		message := readMap(t, workerContext, worker)
		actor, _ := message["actor"].(map[string]any)
		if message["type"] != "authorize_query" || message["collection"] != "items" || actor["workspaceId"] != "mine" {
			workerDone <- io.ErrUnexpectedEOF
			return
		}
		workerDone <- writeSocketJSON(workerContext, worker, map[string]any{
			"v": protocolVersion, "type": "policy", "callId": message["callId"],
			"constraint": map[string]any{
				"version": 1,
				"where":   map[string]any{"op": "compare", "cmp": "eq", "path": "title", "value": map[string]any{"t": "string", "v": "visible"}},
			},
		})
	}()
	request, _ := http.NewRequest(http.MethodPost, api.URL+"/v1/collections/items/query", strings.NewReader(`{"version":1,"query":{"version":1,"where":{"op":"true"}}}`))
	request.Header.Set("authorization", "Bearer valid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("query status=%d body=%s", response.StatusCode, payload)
	}
	var body struct {
		Documents []json.RawMessage `json:"documents"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Documents) != 1 || strings.Contains(string(body.Documents[0]), `"rank"`) || !strings.Contains(string(body.Documents[0]), `"visible"`) {
		t.Fatalf("publication documents=%s", body.Documents)
	}
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	hub.mu.RLock()
	lease := hub.publications["items"].publication.lease
	hub.mu.RUnlock()
	if err := worker.Close(websocket.StatusNormalClosure, "disconnect publication"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lease.Done():
	case <-time.After(time.Second):
		t.Fatal("publication lease was not revoked on worker disconnect")
	}
	denied, _ := http.NewRequest(http.MethodPost, api.URL+"/v1/collections/items/query", strings.NewReader(`{"version":1,"query":{"version":1,"where":{"op":"true"}}}`))
	denied.Header.Set("authorization", "Bearer valid")
	deniedResponse, err := http.DefaultClient.Do(denied)
	if err != nil {
		t.Fatal(err)
	}
	deniedResponse.Body.Close()
	if deniedResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("managed query after worker disconnect status=%d", deniedResponse.StatusCode)
	}
	if _, found, err := hub.ResolveQueryPolicy(context.Background(), Actor{ID: "user-1", WorkspaceID: "mine"}, "items", meldbase.QuerySpec{}); !found || !errors.Is(err, ErrForbidden) {
		t.Fatalf("disconnected managed publication found=%v err=%v", found, err)
	}
	if _, found, err := hub.ResolveQueryPolicy(context.Background(), Actor{ID: "user-1", WorkspaceID: "mine"}, "other", meldbase.QuerySpec{}); found || err != nil {
		t.Fatalf("unmanaged publication found=%v err=%v", found, err)
	}
	stats := hub.Stats()
	if stats.PolicyEvaluations != 1 || stats.PolicySucceeded != 1 || stats.RegisteredPublications != 0 {
		t.Fatalf("publication stats=%+v", stats)
	}
}

func TestWorkerPublicationEvaluationHasIndependentDeadline(t *testing.T) {
	authenticator, _ := NewWorkerTokenAuthenticator(testWorkerToken)
	hub, err := NewWorkerHub(WorkerHubConfig{
		Authenticator: authenticator, PublicationCollections: []string{"items"}, PolicyEvaluationTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(hub)
	defer control.Close()
	worker, workerContext := dialTestWorker(t, control.URL)
	defer worker.CloseNow()
	if err := writeSocketJSON(workerContext, worker, map[string]any{
		"v": protocolVersion, "type": "register", "workerId": "slow-policy-worker", "methods": []any{},
		"publications": []map[string]any{{
			"collection": "items", "version": "items-v1", "maxResults": 10,
			"queryPaths": "*", "resultFields": "*",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	readMap(t, workerContext, worker)
	query, _ := meldbase.CompileQuery(meldbase.Filter{}, meldbase.QueryOptions{})
	result := make(chan error, 1)
	go func() {
		_, _, err := hub.ResolveQueryPolicy(context.Background(), Actor{ID: "user-1"}, "items", query)
		result <- err
	}()
	if frame := readMap(t, workerContext, worker); frame["type"] != "authorize_query" {
		t.Fatalf("policy invocation=%+v", frame)
	}
	if frame := readMap(t, workerContext, worker); frame["type"] != "cancel" {
		t.Fatalf("policy timeout cancellation=%+v", frame)
	}
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("policy deadline error=%v", err)
	}
	if stats := hub.Stats(); stats.PolicyEvaluations != 1 || stats.PolicyFailed != 1 || stats.PolicyActive != 0 {
		t.Fatalf("policy deadline stats=%+v", stats)
	}
}

func TestWorkerPublicationDisconnectRevokesCompositeRealtimePolicy(t *testing.T) {
	authenticator, _ := NewWorkerTokenAuthenticator(testWorkerToken)
	hub, err := NewWorkerHub(WorkerHubConfig{Authenticator: authenticator, PublicationCollections: []string{"items"}})
	if err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(hub)
	defer control.Close()
	worker, workerContext := dialTestWorker(t, control.URL)
	if err := writeSocketJSON(workerContext, worker, map[string]any{
		"v": protocolVersion, "type": "register", "workerId": "realtime-policy-worker", "methods": []any{},
		"publications": []map[string]any{{
			"collection": "items", "version": "items-v1", "maxResults": 10,
			"queryPaths": "*", "resultFields": "*",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	readMap(t, workerContext, worker)
	baseLease, _ := NewQueryPolicyLease("base-policy-v1")
	authorizer := &leaseAuthorizer{version: "base-policy-v1", lease: baseLease}
	db := meldbase.New()
	defer db.Close()
	insertServerDocument(t, db.Collection("items"), "mine", 1, "visible")
	handler, err := New(Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: authorizer, QueryPolicyResolver: hub,
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(handler)
	defer api.Close()
	handler.config.PublicRealtimeURL = "ws" + strings.TrimPrefix(api.URL, "http") + "/v1/realtime"
	workerDone := make(chan error, 1)
	go func() {
		message := readMap(t, workerContext, worker)
		workerDone <- writeSocketJSON(workerContext, worker, map[string]any{
			"v": protocolVersion, "type": "policy", "callId": message["callId"],
			"constraint": map[string]any{"version": 1, "where": map[string]any{"op": "true"}},
		})
	}()
	ticket := obtainTicket(t, api.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, ticket.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if err := writeSocketJSON(ctx, connection, map[string]any{"v": protocolVersion, "type": "authenticate", "ticket": ticket.Ticket}); err != nil {
		t.Fatal(err)
	}
	readMap(t, ctx, connection)
	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": protocolVersion, "type": "subscribe", "mode": "delta", "requestId": "worker-policy", "collection": "items",
		"query": map[string]any{"version": 1, "where": map[string]any{"op": "true"}},
	}); err != nil {
		t.Fatal(err)
	}
	if initial := readMap(t, ctx, connection); initial["type"] != "snapshot" {
		t.Fatalf("initial publication snapshot=%+v", initial)
	}
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	if err := worker.Close(websocket.StatusNormalClosure, "revoke publication"); err != nil {
		t.Fatal(err)
	}
	if message := readMap(t, ctx, connection); message["type"] != "resync_required" || message["requestId"] != "worker-policy" {
		t.Fatalf("worker policy revocation=%+v", message)
	}
	if !baseLease.Valid() {
		t.Fatal("worker disconnect revoked the independent base authorizer lease")
	}
}

func TestWorkerHubRejectsUnauthenticatedBrowserAndConflictingRegistration(t *testing.T) {
	hub := newTestWorkerHub(t)
	control := httptest.NewServer(hub)
	defer control.Close()
	request, _ := http.NewRequest(http.MethodGet, control.URL, nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", response.StatusCode)
	}
	worker, _ := openTestWorker(t, control.URL, []map[string]any{{"name": "owned.method", "mode": "rpc"}})
	defer worker.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	header := http.Header{"authorization": []string{"Bearer " + testWorkerToken}, "origin": []string{"https://evil.example"}}
	_, browserResponse, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(control.URL, "http"), &websocket.DialOptions{HTTPHeader: header})
	if err == nil || browserResponse == nil || browserResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("browser control connection err=%v response=%v", err, browserResponse)
	}
	conflict, conflictContext := dialTestWorker(t, control.URL)
	defer conflict.CloseNow()
	if err := writeSocketJSON(conflictContext, conflict, map[string]any{
		"v": protocolVersion, "type": "register", "workerId": "conflict-worker", "methods": []map[string]any{{"name": "owned.method", "mode": "rpc"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conflict.Read(conflictContext); err == nil {
		t.Fatal("conflicting registration remained connected")
	}
}

func TestWorkerHubRejectsUndeclaredPublicationAuthority(t *testing.T) {
	authenticator, _ := NewWorkerTokenAuthenticator(testWorkerToken)
	if _, err := NewWorkerHub(WorkerHubConfig{Authenticator: authenticator, PublicationCollections: []string{"bad/name"}}); err == nil {
		t.Fatal("invalid managed publication collection was accepted")
	}
	if _, err := NewWorkerHub(WorkerHubConfig{Authenticator: authenticator, PublicationCollections: []string{"items", "items"}}); err == nil {
		t.Fatal("duplicate managed publication collection was accepted")
	}
	hub, err := NewWorkerHub(WorkerHubConfig{Authenticator: authenticator, PublicationCollections: []string{"items"}})
	if err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(hub)
	defer control.Close()
	connection, ctx := dialTestWorker(t, control.URL)
	defer connection.CloseNow()
	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": protocolVersion, "type": "register", "workerId": "authority-escalation", "methods": []any{},
		"publications": []map[string]any{{
			"collection": "secrets", "version": "secrets-v1", "maxResults": 10,
			"queryPaths": "*", "resultFields": "*",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := connection.Read(ctx); err == nil {
		t.Fatal("worker registered an undeclared publication authority")
	}
}

func TestWorkerHubCannotShadowReservedLocalMethod(t *testing.T) {
	hub := newTestWorkerHub(t)
	db := meldbase.New()
	defer db.Close()
	_, err := New(Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"),
		RPCMethods: map[string]RPCMethod{"local.method": func(context.Context, Actor, []meldbase.Value) (meldbase.Value, error) {
			return meldbase.Null(), nil
		}},
		RPCMethodResolver: hub, RPCAuthorizer: rpcTestAuthorizer{allow: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(hub)
	defer control.Close()
	connection, ctx := dialTestWorker(t, control.URL)
	defer connection.CloseNow()
	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": protocolVersion, "type": "register", "workerId": "shadow-worker", "methods": []map[string]any{{"name": "local.method", "mode": "rpc"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := connection.Read(ctx); err == nil {
		t.Fatal("worker shadowed a reserved local method")
	}
	if _, ok := hub.ResolveRPCMethod("local.method"); ok {
		t.Fatal("reserved method appeared in dynamic resolver")
	}
}

func newTestWorkerHub(t *testing.T) *WorkerHub {
	t.Helper()
	authenticator, err := NewWorkerTokenAuthenticator(testWorkerToken)
	if err != nil {
		t.Fatal(err)
	}
	hub, err := NewWorkerHub(WorkerHubConfig{Authenticator: authenticator})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func dialTestWorker(t *testing.T, controlURL string) (*websocket.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	header := http.Header{"authorization": []string{"Bearer " + testWorkerToken}, "Meldbase-Protocol": []string{"capabilities-v1"}}
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(controlURL, "http"), &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatal(err)
	}
	return connection, ctx
}

func openTestWorker(t *testing.T, controlURL string, methods []map[string]any) (*websocket.Conn, context.Context) {
	t.Helper()
	connection, ctx := dialTestWorker(t, controlURL)
	if err := writeSocketJSON(ctx, connection, map[string]any{
		"v": protocolVersion, "type": "register", "workerId": "test-worker", "methods": methods,
	}); err != nil {
		t.Fatal(err)
	}
	if registered := readMap(t, ctx, connection); registered["type"] != "registered" {
		t.Fatalf("registered=%+v", registered)
	}
	return connection, ctx
}
