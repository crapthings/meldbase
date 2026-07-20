package meldbase

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

func TestPublicV2StorageQuotaIsSafeAdmissionRejection(t *testing.T) {
	if DefaultV2MaxFileBytes != storagev2.DefaultMaxFileBytes {
		t.Fatalf("public/internal quota defaults drifted: %d/%d", DefaultV2MaxFileBytes, storagev2.DefaultMaxFileBytes)
	}
	if V2PageSize != storagev2.PageSize {
		t.Fatalf("public/internal V2 page size drifted: %d/%d", V2PageSize, storagev2.PageSize)
	}
	path := filepath.Join(t.TempDir(), "quota.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id, err := db.Collection("items").InsertOne(context.Background(), Document{"revision": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	used := db.Stats().Storage.StorageUsedBytes
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenWithOptions(path, OpenOptions{StorageLimits: V2StorageLimits{MaxFileBytes: used}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"revision": 2}}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("quota update error=%v", err)
	}
	stats := db.Stats()
	if stats.WritesDisabled || stats.CommitSequence != 1 || stats.Storage.StorageLimitRejections != 1 || !stats.Storage.StorageQuotaExhausted {
		t.Fatalf("quota stats=%+v", stats)
	}
	document, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": id})
	if err != nil {
		t.Fatal(err)
	}
	if revision, ok := lookup(document, "revision"); !ok || revision.i != 1 {
		t.Fatalf("rejected update became visible: %+v", document)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("quota rejection poisoned close: %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"revision": 2}}); err != nil {
		t.Fatalf("larger quota could not continue: %v", err)
	}
}

func TestPublicInvalidV2StorageQuotaDoesNotCreateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-quota.meld2")
	_, err := OpenWithOptions(path, OpenOptions{StorageLimits: V2StorageLimits{MaxFileBytes: 2*V2PageSize + 1}})
	if !errors.Is(err, ErrInvalidResourceLimits) {
		t.Fatalf("invalid quota error=%v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid quota mutated path: %v", err)
	}
}
