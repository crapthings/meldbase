package meldbase

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

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
	db, err := OpenWithOptions(path, OpenOptions{CommitRetention: V2CommitRetentionPolicy{MaxCommits: 2}})
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
	reopened, err := OpenWithOptions(path, OpenOptions{CommitRetention: V2CommitRetentionPolicy{MaxCommits: 2}})
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
	db, err := OpenWithOptions(path, OpenOptions{CommitRetention: policy})
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
	reopened, err := OpenWithOptions(path, OpenOptions{CommitRetention: policy})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if stats := reopened.Stats().Storage; stats.RetainedCommitBytes == 0 || stats.RetainedCommitBytes > 500 || stats.CommitRetentionMaxBytes != 500 {
		t.Fatalf("reopened byte stats=%+v", stats)
	}
}

func TestV2ReplayDeliveryTimeoutReleasesRetentionLease(t *testing.T) {
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "replay-timeout.meld2"), OpenOptions{
		CommitRetention:       V2CommitRetentionPolicy{MaxCommits: 2},
		ReplayDeliveryTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	id, err := items.InsertOne(context.Background(), Document{"value": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := db.OpenQueryReplay(context.Background(), "items", query, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	for value := int64(2); value <= 3; value++ {
		if _, err := items.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": value}}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-replay.Errors:
		if !errors.Is(err, ErrSlowConsumer) {
			t.Fatalf("replay error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stalled replay was not terminated")
	}
	deadline := time.Now().Add(time.Second)
	for {
		stats := db.Stats()
		if stats.Storage.ActiveReplayLeases == 0 && stats.Realtime.SlowConsumers == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replay lease or metric not released: %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": int64(4)}}); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats().Storage; stats.RetentionPressure || stats.RetainedCommits > 2 {
		t.Fatalf("retention did not recover after stalled replay ended: %+v", stats)
	}
}

func TestV2ReplayDeliveryTimeoutValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-replay-timeout.meld2")
	if db, err := OpenWithOptions(path, OpenOptions{ReplayDeliveryTimeout: time.Nanosecond}); db != nil || !errors.Is(err, ErrInvalidReplayDeliveryTimeout) {
		t.Fatalf("too-short timeout db=%v err=%v", db, err)
	}
	if db, err := OpenWithOptions(path, OpenOptions{ReplayDeliveryTimeout: time.Minute + time.Nanosecond}); db != nil || !errors.Is(err, ErrInvalidReplayDeliveryTimeout) {
		t.Fatalf("too-long timeout db=%v err=%v", db, err)
	}
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "generic-replay-timeout.meld2"), OpenOptions{ReplayDeliveryTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	source, ok := db.replaySource.(*v2QueryReplaySource)
	if !ok || source.deliveryTimeout != 20*time.Millisecond {
		t.Fatalf("generic replay source=%T timeout=%s", db.replaySource, source.deliveryTimeout)
	}
}
