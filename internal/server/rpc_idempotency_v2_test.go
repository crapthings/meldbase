package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
)

func TestDurableRPCIdempotencyReplaysAfterDatabaseReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rpc-idempotency.meld2")
	db, err := meldbase.OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	var firstCalls atomic.Uint64
	firstHandler := newDurableRPCHandler(t, db, store, map[string]RPCMethod{"receipt": func(context.Context, Principal, []meldbase.Value) (meldbase.Value, error) {
		firstCalls.Add(1)
		return meldbase.String("first-process"), nil
	}})
	firstServer := httptest.NewServer(firstHandler)
	key := "abcdefghijklmnopqrstuv"
	response := postIdempotentRPC(t, firstServer.URL, "receipt", key, []any{})
	if got := readRPCStringResult(t, response); got != "first-process" {
		t.Fatalf("first result=%q", got)
	}
	response = postIdempotentRPC(t, firstServer.URL, "receipt", key, []any{})
	if got := readRPCStringResult(t, response); got != "first-process" || firstCalls.Load() != 1 {
		t.Fatalf("same-process replay=%q calls=%d", got, firstCalls.Load())
	}
	firstServer.Close()
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Documents != 0 || stats.Collections != 0 {
		t.Fatalf("system commits leaked into public catalog: %+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := meldbase.OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedStore, err := NewDurableRPCIdempotencyStore(reopened)
	if err != nil {
		t.Fatal(err)
	}
	var reopenedCalls atomic.Uint64
	reopenedHandler := newDurableRPCHandler(t, reopened, reopenedStore, map[string]RPCMethod{"receipt": func(context.Context, Principal, []meldbase.Value) (meldbase.Value, error) {
		reopenedCalls.Add(1)
		return meldbase.String("must-not-run"), nil
	}})
	reopenedServer := httptest.NewServer(reopenedHandler)
	defer reopenedServer.Close()
	response = postIdempotentRPC(t, reopenedServer.URL, "receipt", key, []any{})
	if got := readRPCStringResult(t, response); got != "first-process" || reopenedCalls.Load() != 0 {
		t.Fatalf("reopen replay=%q calls=%d", got, reopenedCalls.Load())
	}
}

func TestDurableRPCIdempotencySequencesBusinessWriteAndTerminalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rpc-business-write.meld2")
	db, err := meldbase.OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Uint64
	handler := newDurableRPCHandler(t, db, store, map[string]RPCMethod{"createOrder": func(ctx context.Context, _ Principal, _ []meldbase.Value) (meldbase.Value, error) {
		calls.Add(1)
		if _, err := db.Collection("orders").InsertOne(ctx, meldbase.Document{"status": meldbase.String("created")}); err != nil {
			return meldbase.Value{}, err
		}
		return meldbase.String("created"), nil
	}})
	httpServer := httptest.NewServer(handler)
	key := "businesswritekey000001"
	if got := readRPCStringResult(t, postIdempotentRPC(t, httpServer.URL, "createOrder", key, []any{})); got != "created" {
		t.Fatalf("first result=%q", got)
	}
	if stats := db.Stats(); stats.CommitSequence != 3 || stats.Documents != 1 || stats.Collections != 1 {
		t.Fatalf("claim, business, terminal sequence: %+v", stats)
	}
	if got := readRPCStringResult(t, postIdempotentRPC(t, httpServer.URL, "createOrder", key, []any{})); got != "created" || calls.Load() != 1 {
		t.Fatalf("replay=%q calls=%d", got, calls.Load())
	}
	if stats := db.Stats(); stats.CommitSequence != 3 || stats.Documents != 1 {
		t.Fatalf("replay performed a write: %+v", stats)
	}
	httpServer.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := meldbase.OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedStore, err := NewDurableRPCIdempotencyStore(reopened)
	if err != nil {
		t.Fatal(err)
	}
	var reopenedCalls atomic.Uint64
	reopenedHandler := newDurableRPCHandler(t, reopened, reopenedStore, map[string]RPCMethod{"createOrder": func(context.Context, Principal, []meldbase.Value) (meldbase.Value, error) {
		reopenedCalls.Add(1)
		return meldbase.String("must-not-run"), nil
	}})
	reopenedServer := httptest.NewServer(reopenedHandler)
	defer reopenedServer.Close()
	if got := readRPCStringResult(t, postIdempotentRPC(t, reopenedServer.URL, "createOrder", key, []any{})); got != "created" || reopenedCalls.Load() != 0 {
		t.Fatalf("reopen replay=%q calls=%d", got, reopenedCalls.Load())
	}
	if stats := reopened.Stats(); stats.CommitSequence != 3 || stats.Documents != 1 {
		t.Fatalf("reopen state: %+v", stats)
	}
}

func TestTransactionalRPCPublishesBusinessWriteAndResultInOneCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactional-rpc.meld2")
	db, err := meldbase.OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Uint64
	methods := map[string]RPCTransactionalMethod{"orders.create": func(_ context.Context, _ Principal, _ []meldbase.Value, tx *meldbase.WriteTransaction) (meldbase.Value, error) {
		calls.Add(1)
		if _, err := tx.InsertOne("orders", meldbase.Document{"status": meldbase.String("created")}); err != nil {
			return meldbase.Value{}, err
		}
		return meldbase.String("created"), nil
	}}
	handler := newDurableTransactionalRPCHandler(t, db, store, methods)
	httpServer := httptest.NewServer(handler)
	assertRPCError(t, postRPC(t, httpServer.URL, "orders.create", `{"version":1,"arguments":[]}`, true), http.StatusBadRequest, "rpc_idempotency_required")
	if calls.Load() != 0 || db.Stats().CommitSequence != 0 {
		t.Fatalf("keyless transactional call ran: calls=%d stats=%+v", calls.Load(), db.Stats())
	}
	key := "transactionalrpc000001"
	if got := readRPCStringResult(t, postIdempotentRPC(t, httpServer.URL, "orders.create", key, []any{})); got != "created" {
		t.Fatalf("first result=%q", got)
	}
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Documents != 1 || stats.Collections != 1 {
		t.Fatalf("claim plus atomic business/terminal commits: %+v", stats)
	}
	if got := readRPCStringResult(t, postIdempotentRPC(t, httpServer.URL, "orders.create", key, []any{})); got != "created" || calls.Load() != 1 {
		t.Fatalf("replay=%q calls=%d", got, calls.Load())
	}
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Documents != 1 {
		t.Fatalf("replay changed database: %+v", stats)
	}
	httpServer.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := meldbase.OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedStore, err := NewDurableRPCIdempotencyStore(reopened)
	if err != nil {
		t.Fatal(err)
	}
	var reopenedCalls atomic.Uint64
	reopenedHandler := newDurableTransactionalRPCHandler(t, reopened, reopenedStore, map[string]RPCTransactionalMethod{
		"orders.create": func(context.Context, Principal, []meldbase.Value, *meldbase.WriteTransaction) (meldbase.Value, error) {
			reopenedCalls.Add(1)
			return meldbase.String("must-not-run"), nil
		},
	})
	reopenedServer := httptest.NewServer(reopenedHandler)
	defer reopenedServer.Close()
	if got := readRPCStringResult(t, postIdempotentRPC(t, reopenedServer.URL, "orders.create", key, []any{})); got != "created" || reopenedCalls.Load() != 0 {
		t.Fatalf("reopen replay=%q calls=%d", got, reopenedCalls.Load())
	}
	if stats := reopened.Stats(); stats.CommitSequence != 2 || stats.Documents != 1 {
		t.Fatalf("reopen state=%+v", stats)
	}
}

func TestTransactionalRPCRollsBackWritesBeforePersistingApplicationError(t *testing.T) {
	db, err := meldbase.OpenV2(filepath.Join(t.TempDir(), "transactional-error.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Uint64
	handler := newDurableTransactionalRPCHandler(t, db, store, map[string]RPCTransactionalMethod{
		"orders.reject": func(_ context.Context, _ Principal, _ []meldbase.Value, tx *meldbase.WriteTransaction) (meldbase.Value, error) {
			calls.Add(1)
			if _, err := tx.InsertOne("orders", meldbase.Document{"status": meldbase.String("must-rollback")}); err != nil {
				return meldbase.Value{}, err
			}
			return meldbase.Value{}, &RPCError{Code: "order_rejected"}
		},
	})
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	key := "transactionalerror0001"
	assertRPCError(t, postIdempotentRPC(t, httpServer.URL, "orders.reject", key, []any{}), http.StatusBadRequest, "order_rejected")
	assertRPCError(t, postIdempotentRPC(t, httpServer.URL, "orders.reject", key, []any{}), http.StatusBadRequest, "order_rejected")
	if calls.Load() != 1 {
		t.Fatalf("application error calls=%d", calls.Load())
	}
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Documents != 0 || stats.Collections != 0 {
		t.Fatalf("rolled-back error state=%+v", stats)
	}
}

func TestTransactionalRPCNoopUsesTerminalOnlyCompletion(t *testing.T) {
	db, err := meldbase.OpenV2(filepath.Join(t.TempDir(), "transactional-noop.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	handler := newDurableTransactionalRPCHandler(t, db, store, map[string]RPCTransactionalMethod{
		"status.read": func(context.Context, Principal, []meldbase.Value, *meldbase.WriteTransaction) (meldbase.Value, error) {
			return meldbase.String("ok"), nil
		},
	})
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	if got := readRPCStringResult(t, postIdempotentRPC(t, httpServer.URL, "status.read", "transactionalnoop00001", []any{})); got != "ok" {
		t.Fatalf("noop result=%q", got)
	}
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Documents != 0 {
		t.Fatalf("noop state=%+v", stats)
	}
	stats := handler.Stats()
	if stats.RPCAtomicNoopCompletions != 1 || stats.RPCAtomicCommits != 0 || stats.RPCAtomicRollbacks != 0 {
		t.Fatalf("noop metrics=%+v", stats)
	}
}

func TestTransactionalRPCUsesSameAtomicPathOverWebSocket(t *testing.T) {
	db, err := meldbase.OpenV2(filepath.Join(t.TempDir(), "transactional-websocket.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	handler := newDurableTransactionalRPCHandler(t, db, store, map[string]RPCTransactionalMethod{
		"orders.socket": func(_ context.Context, _ Principal, _ []meldbase.Value, tx *meldbase.WriteTransaction) (meldbase.Value, error) {
			if _, err := tx.InsertOne("orders", meldbase.Document{"transport": meldbase.String("websocket")}); err != nil {
				return meldbase.Value{}, err
			}
			return meldbase.String("socket-created"), nil
		},
	})
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	handler.config.PublicRealtimeURL = "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/realtime"
	connection, socketContext := openAuthenticatedRPCSocket(t, httpServer.URL)
	defer connection.Close(1000, "")
	if err := writeSocketJSON(socketContext, connection, map[string]any{
		"v": 1, "type": "call", "requestId": "atomic-socket-1", "idempotencyKey": "transactionalsocket001", "method": "orders.socket", "arguments": []any{},
	}); err != nil {
		t.Fatal(err)
	}
	message := readMap(t, socketContext, connection)
	if message["type"] != "result" || message["requestId"] != "atomic-socket-1" {
		t.Fatalf("socket terminal=%+v", message)
	}
	if stats := db.Stats(); stats.CommitSequence != 2 || stats.Documents != 1 {
		t.Fatalf("socket atomic state=%+v", stats)
	}
}

func TestTransactionalRPCPersistsOptimisticConflictWithoutPartialWrites(t *testing.T) {
	db, err := meldbase.OpenV2(filepath.Join(t.TempDir(), "transactional-conflict.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	ready, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Uint64
	conflictedID := meldbase.DocumentID{15: 1}
	handler := newDurableTransactionalRPCHandler(t, db, store, map[string]RPCTransactionalMethod{
		"orders.conflict": func(_ context.Context, _ Principal, _ []meldbase.Value, tx *meldbase.WriteTransaction) (meldbase.Value, error) {
			calls.Add(1)
			if _, err := tx.InsertOne("orders", meldbase.Document{"_id": meldbase.ID(conflictedID), "kind": meldbase.String("stale")}); err != nil {
				return meldbase.Value{}, err
			}
			close(ready)
			<-release
			return meldbase.String("must-not-commit"), nil
		},
	})
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	key := "transactionconflict001"
	responseDone := make(chan *http.Response, 1)
	go func() { responseDone <- postIdempotentRPC(t, httpServer.URL, "orders.conflict", key, []any{}) }()
	<-ready
	if _, err := db.Collection("orders").InsertOne(context.Background(), meldbase.Document{
		"_id": meldbase.ID(conflictedID), "kind": meldbase.String("winner"),
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	assertRPCError(t, <-responseDone, http.StatusConflict, "rpc_transaction_conflict")
	assertRPCError(t, postIdempotentRPC(t, httpServer.URL, "orders.conflict", key, []any{}), http.StatusConflict, "rpc_transaction_conflict")
	if calls.Load() != 1 {
		t.Fatalf("conflicted method calls=%d", calls.Load())
	}
	if stats := db.Stats(); stats.CommitSequence != 3 || stats.Documents != 1 || stats.Collections != 1 {
		t.Fatalf("conflict state=%+v", stats)
	}
	stats := handler.Stats()
	if stats.RPCAtomicCommits != 0 || stats.RPCAtomicRollbacks != 1 {
		t.Fatalf("conflict metrics=%+v", stats)
	}
}

func TestDurableRPCIdempotencyPendingFromOldSessionBecomesUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old-session.meld2")
	database, err := meldbase.OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewDurableRPCIdempotencyStore(database)
	if err != nil {
		t.Fatal(err)
	}
	claim := durableTestClaim(2, 2, time.Now().Add(time.Hour))
	decision, err := first.Claim(context.Background(), claim)
	if err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("old session claim=%+v err=%v", decision, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = meldbase.OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	second, _ := NewDurableRPCIdempotencyStore(database)
	retry := claim
	retry.SessionID, retry.ClaimID = [16]byte{3}, [16]byte{3}
	decision, err = second.Claim(context.Background(), retry)
	if err != nil || decision.Kind != RPCIdempotencyOutcomeUnknown {
		t.Fatalf("old-session decision=%+v err=%v", decision, err)
	}
	decision, err = second.Claim(context.Background(), retry)
	if err != nil || decision.Kind != RPCIdempotencyOutcomeUnknown {
		t.Fatalf("unknown replay=%+v err=%v", decision, err)
	}
}

func TestDurableRPCIdempotencyClaimIsLinearizableUnderConcurrency(t *testing.T) {
	db, err := meldbase.OpenV2(filepath.Join(t.TempDir(), "concurrent.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 24
	var execute, inProgress, failed atomic.Uint64
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := range workers {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			claim := durableTestClaim(4, byte(index+1), time.Now().Add(time.Hour))
			decision, err := store.Claim(context.Background(), claim)
			if err != nil {
				failed.Add(1)
				return
			}
			switch decision.Kind {
			case RPCIdempotencyExecute:
				execute.Add(1)
			case RPCIdempotencyInProgress:
				inProgress.Add(1)
			default:
				failed.Add(1)
			}
		}(index)
	}
	close(start)
	wait.Wait()
	if execute.Load() != 1 || inProgress.Load() != workers-1 || failed.Load() != 0 || db.Stats().CommitSequence != 1 {
		t.Fatalf("execute=%d inProgress=%d failed=%d sequence=%d", execute.Load(), inProgress.Load(), failed.Load(), db.Stats().CommitSequence)
	}
}

func TestDurableRPCIdempotencyRetentionNeverStealsPendingClaims(t *testing.T) {
	db, err := meldbase.OpenV2(filepath.Join(t.TempDir(), "retention.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, _ := NewDurableRPCIdempotencyStore(db)

	completed := durableTestClaim(7, 7, time.Now().Add(-time.Second))
	if decision, err := store.Claim(context.Background(), completed); err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("expired initial claim=%+v err=%v", decision, err)
	}
	result, _ := meldbase.MarshalWireValue(meldbase.String("old"))
	if err := store.Complete(context.Background(), RPCIdempotencyCompletion{Claim: completed, Result: result}); err != nil {
		t.Fatal(err)
	}
	replacement := completed
	replacement.SessionID, replacement.ClaimID, replacement.Fingerprint = [16]byte{8}, [16]byte{8}, [32]byte{8}
	replacement.ExpiresAt = time.Now().Add(time.Hour)
	if decision, err := store.Claim(context.Background(), replacement); err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("expired terminal replacement=%+v err=%v", decision, err)
	}

	pending := durableTestClaim(9, 9, time.Now().Add(-time.Second))
	pending.KeyHash = [32]byte{9}
	if decision, err := store.Claim(context.Background(), pending); err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("pending claim=%+v err=%v", decision, err)
	}
	oldSessionRetry := pending
	oldSessionRetry.SessionID, oldSessionRetry.ClaimID = [16]byte{10}, [16]byte{10}
	oldSessionRetry.ExpiresAt = time.Now().Add(time.Hour)
	if decision, err := store.Claim(context.Background(), oldSessionRetry); err != nil || decision.Kind != RPCIdempotencyOutcomeUnknown {
		t.Fatalf("expired pending was stolen: decision=%+v err=%v", decision, err)
	}
}

func TestDurableRPCIdempotencyPrunesOnlyExpiredTerminalRecords(t *testing.T) {
	db, err := meldbase.OpenV2(filepath.Join(t.TempDir(), "prune.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	result, _ := meldbase.MarshalWireValue(meldbase.String("done"))

	expiredResult := durableTestClaim(20, 20, time.Now().Add(-time.Second))
	expiredResult.KeyHash = [32]byte{20}
	if decision, err := store.Claim(ctx, expiredResult); err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("expired result claim=%+v err=%v", decision, err)
	}
	if err := store.Complete(ctx, RPCIdempotencyCompletion{Claim: expiredResult, Result: result}); err != nil {
		t.Fatal(err)
	}

	expiredUnknown := durableTestClaim(21, 21, time.Now().Add(-time.Second))
	expiredUnknown.KeyHash = [32]byte{21}
	if decision, err := store.Claim(ctx, expiredUnknown); err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("expired unknown claim=%+v err=%v", decision, err)
	}
	if err := store.MarkUnknown(ctx, expiredUnknown); err != nil {
		t.Fatal(err)
	}

	expiredPending := durableTestClaim(22, 22, time.Now().Add(-time.Second))
	expiredPending.KeyHash = [32]byte{22}
	if decision, err := store.Claim(ctx, expiredPending); err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("expired pending claim=%+v err=%v", decision, err)
	}

	activeResult := durableTestClaim(23, 23, time.Now().Add(time.Hour))
	activeResult.KeyHash = [32]byte{23}
	if decision, err := store.Claim(ctx, activeResult); err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("active result claim=%+v err=%v", decision, err)
	}
	if err := store.Complete(ctx, RPCIdempotencyCompletion{Claim: activeResult, Result: result}); err != nil {
		t.Fatal(err)
	}

	if _, err := store.PruneExpired(ctx, 0); err == nil {
		t.Fatal("zero prune limit accepted")
	}
	if _, err := store.PruneExpired(ctx, 257); err == nil {
		t.Fatal("oversized prune limit accepted")
	}
	pruned, err := store.PruneExpired(ctx, 10)
	if err != nil || pruned != 2 {
		t.Fatalf("pruned=%d err=%v", pruned, err)
	}

	replacement := expiredResult
	replacement.SessionID, replacement.ClaimID = [16]byte{30}, [16]byte{30}
	replacement.ExpiresAt = time.Now().Add(time.Hour)
	if decision, err := store.Claim(ctx, replacement); err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("pruned key was not reusable: decision=%+v err=%v", decision, err)
	}
	pendingRetry := expiredPending
	pendingRetry.SessionID, pendingRetry.ClaimID = [16]byte{31}, [16]byte{31}
	pendingRetry.ExpiresAt = time.Now().Add(time.Hour)
	if decision, err := store.Claim(ctx, pendingRetry); err != nil || decision.Kind != RPCIdempotencyOutcomeUnknown {
		t.Fatalf("expired pending was pruned or stolen: decision=%+v err=%v", decision, err)
	}
	activeRetry := activeResult
	activeRetry.SessionID, activeRetry.ClaimID = [16]byte{32}, [16]byte{32}
	if decision, err := store.Claim(ctx, activeRetry); err != nil || decision.Kind != RPCIdempotencyReplayResult {
		t.Fatalf("active terminal was pruned: decision=%+v err=%v", decision, err)
	}
}

func TestDurableRPCIdempotencyPruneCursorPreventsStarvation(t *testing.T) {
	db, err := meldbase.OpenV2(filepath.Join(t.TempDir(), "prune-cursor.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for index := 1; index <= 20; index++ {
		claim := durableTestClaim(byte(40+index), byte(40+index), time.Now().Add(time.Hour))
		claim.KeyHash = [32]byte{byte(index)}
		if decision, err := store.Claim(ctx, claim); err != nil || decision.Kind != RPCIdempotencyExecute {
			t.Fatalf("active claim %d=%+v err=%v", index, decision, err)
		}
	}
	expired := durableTestClaim(80, 80, time.Now().Add(-time.Second))
	expired.KeyHash = [32]byte{250}
	if decision, err := store.Claim(ctx, expired); err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("expired claim=%+v err=%v", decision, err)
	}
	result, _ := meldbase.MarshalWireValue(meldbase.String("expired"))
	if err := store.Complete(ctx, RPCIdempotencyCompletion{Claim: expired, Result: result}); err != nil {
		t.Fatal(err)
	}

	if pruned, err := store.PruneExpired(ctx, 1); err != nil || pruned != 0 {
		t.Fatalf("first bounded pass pruned=%d err=%v", pruned, err)
	}
	if pruned, err := store.PruneExpired(ctx, 1); err != nil || pruned != 1 {
		t.Fatalf("rotated pass pruned=%d err=%v", pruned, err)
	}
}

func TestDurableRPCIdempotencySurvivesCompaction(t *testing.T) {
	directory := t.TempDir()
	db, err := meldbase.OpenV2(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	store, _ := NewDurableRPCIdempotencyStore(db)
	claim := durableTestClaim(5, 5, time.Now().Add(time.Hour))
	if decision, err := store.Claim(context.Background(), claim); err != nil || decision.Kind != RPCIdempotencyExecute {
		t.Fatalf("claim=%+v err=%v", decision, err)
	}
	result, _ := meldbase.MarshalWireValue(meldbase.String("preserved"))
	if err := store.Complete(context.Background(), RPCIdempotencyCompletion{Claim: claim, Result: result}); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "compacted.meld2")
	if err := db.CompactToV2(context.Background(), destination); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	compacted, err := meldbase.OpenV2(destination)
	if err != nil {
		t.Fatal(err)
	}
	defer compacted.Close()
	compactedStore, _ := NewDurableRPCIdempotencyStore(compacted)
	retry := claim
	retry.SessionID, retry.ClaimID = [16]byte{6}, [16]byte{6}
	decision, err := compactedStore.Claim(context.Background(), retry)
	if err != nil || decision.Kind != RPCIdempotencyReplayResult || string(decision.Result) != string(result) {
		t.Fatalf("compacted replay=%+v err=%v", decision, err)
	}
}

func TestDurableRPCIdempotencyRejectsMemoryAndV1Databases(t *testing.T) {
	memory := meldbase.New()
	defer memory.Close()
	if _, err := NewDurableRPCIdempotencyStore(memory); err == nil {
		t.Fatal("memory database accepted durable idempotency")
	}
	v1, err := meldbase.OpenV1(filepath.Join(t.TempDir(), "legacy.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer v1.Close()
	if _, err := NewDurableRPCIdempotencyStore(v1); err == nil {
		t.Fatal("V1 database accepted durable idempotency")
	}
}

func TestTransactionalRPCRegistrationRequiresMatchingBuiltInV2Store(t *testing.T) {
	db, err := meldbase.OpenV2(filepath.Join(t.TempDir(), "transactional-config.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	other, err := meldbase.OpenV2(filepath.Join(t.TempDir(), "other.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	store, err := NewDurableRPCIdempotencyStore(db)
	if err != nil {
		t.Fatal(err)
	}
	method := RPCTransactionalMethod(func(context.Context, Principal, []meldbase.Value, *meldbase.WriteTransaction) (meldbase.Value, error) {
		return meldbase.Null(), nil
	})
	base := Config{
		DB: other, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"),
		RPCTransactionalMethods: map[string]RPCTransactionalMethod{"atomic": method},
		RPCAuthorizer:           rpcTestAuthorizer{allow: true}, RPCIdempotencyStore: store,
	}
	if _, err := New(base); err == nil {
		t.Fatal("transactional RPC accepted a store from another database")
	}
	base.DB = db
	base.RPCIdempotencyStore = newMemoryIdempotencyStore()
	if _, err := New(base); err == nil {
		t.Fatal("transactional RPC accepted a custom non-atomic store")
	}
	base.RPCIdempotencyStore = store
	base.RPCMethods = map[string]RPCMethod{"atomic": func(context.Context, Principal, []meldbase.Value) (meldbase.Value, error) {
		return meldbase.Null(), nil
	}}
	if _, err := New(base); err == nil {
		t.Fatal("duplicate standard/transactional method name accepted")
	}
}

func newDurableRPCHandler(t *testing.T, db *meldbase.DB, store RPCIdempotencyStore, methods map[string]RPCMethod) *Handler {
	t.Helper()
	handler, err := New(Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"),
		RPCMethods: methods, RPCAuthorizer: rpcTestAuthorizer{allow: true}, RPCIdempotencyStore: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func newDurableTransactionalRPCHandler(t *testing.T, db *meldbase.DB, store RPCIdempotencyStore, methods map[string]RPCTransactionalMethod) *Handler {
	t.Helper()
	handler, err := New(Config{
		DB: db, Authenticator: testAuthenticator{}, Authorizer: testAuthorizer{},
		PublicRealtimeURL: "ws://placeholder.invalid/v1/realtime", ResumeTokenKey: []byte("0123456789abcdef0123456789abcdef"),
		RPCTransactionalMethods: methods, RPCAuthorizer: rpcTestAuthorizer{allow: true}, RPCIdempotencyStore: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func durableTestClaim(session, claim byte, expires time.Time) RPCIdempotencyClaim {
	return RPCIdempotencyClaim{
		ScopeHash: [32]byte{1}, KeyHash: [32]byte{2}, Fingerprint: [32]byte{3},
		SessionID: [16]byte{session}, ClaimID: [16]byte{claim}, ExpiresAt: expires,
	}
}

func readRPCStringResult(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("RPC status=%d body=%s", response.StatusCode, body)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	value, err := meldbase.UnmarshalWireValue(envelope.Result, meldbase.QueryLimits{})
	result, ok := value.StringValue()
	if err != nil || !ok {
		t.Fatalf("RPC string result=%q/%t err=%v", result, ok, err)
	}
	return result
}
