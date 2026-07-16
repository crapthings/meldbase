package meldbase

import (
	"context"
	"path/filepath"
	"testing"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

func TestPublicV2CommitRetentionPolicyBoundsNormalHistory(t *testing.T) {
	if DefaultV2CommitRetentionMaxCommits != storagev2.DefaultCommitRetentionMaxCommits {
		t.Fatalf("public/internal retention defaults drifted: %d/%d", DefaultV2CommitRetentionMaxCommits, storagev2.DefaultCommitRetentionMaxCommits)
	}
	if DefaultV2CommitRetentionMaxBytes != storagev2.DefaultCommitRetentionMaxBytes {
		t.Fatalf("public/internal retention byte defaults drifted: %d/%d", DefaultV2CommitRetentionMaxBytes, storagev2.DefaultCommitRetentionMaxBytes)
	}
	path := filepath.Join(t.TempDir(), "public-retention.meld2")
	db, err := OpenV2WithOptions(path, V2Options{CommitRetention: V2CommitRetentionPolicy{MaxCommits: 2}})
	if err != nil {
		t.Fatal(err)
	}
	collection := db.Collection("items")
	id, err := collection.InsertOne(context.Background(), Document{"revision": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	for revision := int64(2); revision <= 4; revision++ {
		if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"revision": revision}}); err != nil {
			t.Fatal(err)
		}
	}
	stats := db.Stats().Storage
	if stats.CommitSequence != 4 || stats.OldestRetainedSequence != 3 || stats.RetainedCommits != 2 ||
		stats.CommitRetentionMax != 2 || stats.CommitRetentionOverage != 0 || stats.RetentionPrunedCommits != 2 || stats.RetentionPressure {
		t.Fatalf("storage stats=%+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWithOptions(path, OpenOptions{V2CommitRetention: V2CommitRetentionPolicy{MaxCommits: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if stats := reopened.Stats().Storage; stats.RetainedCommits != 2 || stats.CommitRetentionMax != 2 {
		t.Fatalf("reopened stats=%+v", stats)
	}
}

func TestPublicV2CommitRetentionByteBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "public-retention-bytes.meld2")
	policy := V2CommitRetentionPolicy{MaxCommits: 100, MaxBytes: 500}
	db, err := OpenV2WithOptions(path, V2Options{CommitRetention: policy})
	if err != nil {
		t.Fatal(err)
	}
	collection := db.Collection("items")
	for revision := int64(1); revision <= 6; revision++ {
		if _, err := collection.InsertOne(context.Background(), Document{"revision": Int(revision)}); err != nil {
			t.Fatal(err)
		}
	}
	stats := db.Stats().Storage
	if stats.CommitRetentionMaxBytes != 500 || stats.RetainedCommitBytes > 500 || stats.OldestRetainedSequence <= 1 ||
		stats.CommitRetentionByteOverage != 0 || stats.RetentionPressure {
		t.Fatalf("byte storage stats=%+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWithOptions(path, OpenOptions{V2CommitRetention: policy})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if stats := reopened.Stats().Storage; stats.RetainedCommitBytes == 0 || stats.RetainedCommitBytes > 500 || stats.CommitRetentionMaxBytes != 500 {
		t.Fatalf("reopened byte stats=%+v", stats)
	}
}
