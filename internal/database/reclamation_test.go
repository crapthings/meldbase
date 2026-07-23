package database

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	storage "github.com/crapthings/meldbase/internal/storage"
)

func TestReclamationConflictMapsToPublicSentinel(t *testing.T) {
	if err := mapStorageError(storage.ErrReclamationConflict); !errors.Is(err, ErrReclamationConflict) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("mapped conflict=%v", err)
	}
}

func TestReclaimPagesProtectsLazyCursorAndExportsBoundedStats(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "public-reclaim.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"value": Int(0)})
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := collection.Find(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	for revision := 1; revision <= 8; revision++ {
		if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": int64(revision)}}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := db.ReclaimPages(context.Background())
	if err != nil || result.PinnedSnapshots != 1 || result.ReusablePages == 0 || !result.Persisted {
		t.Fatalf("public reclaim=%+v err=%v", result, err)
	}
	stats := db.Stats()
	if stats.Storage.ReusablePages != result.ReusablePages || stats.Reclamation.Active != 0 ||
		!stats.Storage.PersistentFreeSpace || stats.Storage.FreeSpacePublishes != 1 ||
		stats.Reclamation.Attempts != 1 || stats.Reclamation.Completed != 1 || stats.Reclamation.Failed != 0 ||
		stats.Reclamation.Scans != 1 || stats.Reclamation.Conflicts != 0 || stats.Reclamation.LastAttempts != 1 || stats.Reclamation.LastOnline ||
		stats.Reclamation.LastReachable != result.ReachablePages || stats.Reclamation.LastReclaimable != result.ReusablePages ||
		stats.Reclamation.LastDuration <= 0 {
		t.Fatalf("reclamation stats=%+v storage=%+v", stats.Reclamation, stats.Storage)
	}
	physical := stats.Storage.PhysicalPages
	if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": int64(9)}}); err != nil {
		t.Fatal(err)
	}
	if after := db.Stats().Storage.PhysicalPages; after > physical {
		t.Fatalf("public update did not reuse pages: before=%d after=%d", physical, after)
	}
	document, exists, err := cursor.Next(context.Background())
	if err != nil || !exists {
		t.Fatalf("pinned cursor exists=%t err=%v", exists, err)
	}
	value, _ := document["value"].Int64()
	if value != 0 {
		t.Fatalf("pinned cursor observed future value=%d", value)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReclaimPages(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats().Reclamation; stats.Attempts != 2 || stats.Completed != 2 || stats.Failed != 0 {
		t.Fatalf("second reclaim stats=%+v", stats)
	}
}

func TestReclaimPagesRejectsUnsupportedAndCancelled(t *testing.T) {
	memory := New()
	defer memory.Close()
	if _, err := memory.ReclaimPages(context.Background()); !errors.Is(err, ErrReclamationUnsupported) {
		t.Fatalf("memory reclaim error=%v", err)
	}
	db, err := Open(filepath.Join(t.TempDir(), "cancel-reclaim.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.ReclaimPages(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled reclaim error=%v", err)
	}
	if stats := db.Stats().Reclamation; stats.Attempts != 0 || stats.Failed != 0 {
		t.Fatalf("pre-cancel reclaim stats=%+v", stats)
	}
	for _, attempts := range []int{-1, 33} {
		if _, err := db.ReclaimPagesWithOptions(context.Background(), ReclaimOptions{Online: true, MaxAttempts: attempts}); !errors.Is(err, ErrInvalidReclamationOptions) {
			t.Fatalf("attempts=%d error=%v", attempts, err)
		}
	}
}

func TestReclaimPagesOnlinePublishesReusablePool(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "online-public-reclaim.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"value": Int(0)})
	if err != nil {
		t.Fatal(err)
	}
	for revision := 1; revision <= 8; revision++ {
		if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": int64(revision)}}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := db.ReclaimPagesWithOptions(context.Background(), ReclaimOptions{Online: true, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Online || result.Attempts != 1 || result.ReusablePages == 0 || !result.Persisted {
		t.Fatalf("online reclaim=%+v", result)
	}
	if stats := db.Stats(); stats.Storage.ReusablePages != result.ReusablePages || !stats.Storage.PersistentFreeSpace {
		t.Fatalf("online storage stats=%+v", stats.Storage)
	} else if stats.Reclamation.Scans != 1 || stats.Reclamation.Conflicts != 0 || stats.Reclamation.LastAttempts != 1 || !stats.Reclamation.LastOnline {
		t.Fatalf("online reclamation stats=%+v", stats.Reclamation)
	}
}

func TestReclaimPagesOnlineMemoryOnlyAvoidsPhysicalMaintenanceGeneration(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "memory-only-reclaim.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"value": Int(0)})
	if err != nil {
		t.Fatal(err)
	}
	for revision := 1; revision <= 4; revision++ {
		if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": int64(revision)}}); err != nil {
			t.Fatal(err)
		}
	}
	sequenceBefore := db.Stats().Storage.CommitSequence
	result, err := db.ReclaimPagesWithOptions(context.Background(), ReclaimOptions{Online: true, MemoryOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	storage := db.Stats().Storage
	if result.Persisted || result.ReusablePages == 0 || storage.PersistentFreeSpace || storage.FreeSpacePublishes != 0 || storage.CommitSequence != sequenceBefore {
		t.Fatalf("memory-only result=%+v storage=%+v", result, storage)
	}
	if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": int64(99)}}); err != nil {
		t.Fatal(err)
	}
	if storage := db.Stats().Storage; storage.PersistentFreeSpace || storage.FreeSpacePublishes != 0 {
		t.Fatalf("business commit inherited physical free-space rebuild: %+v", storage)
	}
}
