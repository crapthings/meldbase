package server

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/internal/policyrecord"
)

func TestDurablePolicyGenerationStoreSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy-generation.meld2")
	db, err := meldbase.OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	generation := [16]byte{15: 9}
	mutation, err := policyrecord.GenerationMutation("orders", generation)
	if err != nil {
		t.Fatal(err)
	}
	mutation.TransactionID = [16]byte{1}
	if result, err := db.MeldbaseSystemRecordBackend().CompareAndSwap(context.Background(), mutation); err != nil || !result.Applied {
		t.Fatalf("generation write=%+v err=%v", result, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := meldbase.OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	store, err := NewDurablePolicyGenerationStore(reopened)
	if err != nil {
		t.Fatal(err)
	}
	got, exists, err := store.LoadPolicyGeneration(context.Background(), "orders")
	if err != nil || !exists || got != generation {
		t.Fatalf("reopened generation=%x exists=%v err=%v", got, exists, err)
	}
	if _, exists, err := store.LoadPolicyGeneration(context.Background(), "unmanaged"); err != nil || exists {
		t.Fatalf("missing generation exists=%v err=%v", exists, err)
	}
	authenticator, _ := NewWorkerTokenAuthenticator(testWorkerToken)
	hub, err := NewWorkerHub(WorkerHubConfig{
		Authenticator: authenticator, PublicationCollections: []string{"orders"}, PolicyGenerationStore: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(hub)
	defer control.Close()
	worker, ctx := dialTestWorker(t, control.URL)
	defer worker.CloseNow()
	if err := writeSocketJSON(ctx, worker, map[string]any{
		"v": 1, "type": "register", "workerId": "reopened-policy-worker", "methods": []any{},
		"publications": []map[string]any{{
			"collection": "orders", "version": "orders-v1", "maxResults": 10,
			"queryPaths": "*", "resultFields": "*",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	readMap(t, ctx, worker)
	hub.mu.RLock()
	loaded := hub.publications["orders"].publication
	hub.mu.RUnlock()
	if loaded.generation != generation || loaded.lease == nil || !loaded.lease.Valid() {
		t.Fatalf("registered persisted publication=%+v", loaded)
	}
}
