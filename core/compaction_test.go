package meldbase

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

func TestCompactToV2PublishesVerifiedLogicalSnapshotAndReclaimsHistory(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.meld2")
	destinationPath := filepath.Join(directory, "compact.meld2")
	db, err := Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	empty := db.Collection("empty")
	temporaryID, err := empty.InsertOne(context.Background(), Document{"temporary": Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := empty.DeleteOne(context.Background(), Filter{"_id": temporaryID}); err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	first, second, third := DocumentID{15: 9}, DocumentID{15: 1}, DocumentID{15: 7}
	if _, err := items.InsertMany(context.Background(), []Document{
		{"_id": ID(first), "value": Int(10), "group": String("same")},
		{"_id": ID(second), "value": Int(20), "group": String("same")},
		{"_id": ID(third), "value": Int(30), "group": String("other")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_group", []IndexField{{Field: "group", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "group_value", []IndexField{{Field: "group", Order: 1}, {Field: "value", Order: -1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	// Generate unreachable COW generations so the compacted file has physical
	// work to reclaim while preserving only the final logical value.
	for revision := 0; revision < 80; revision++ {
		if _, err := items.UpdateOne(context.Background(), Filter{"_id": second}, Update{"$set": map[string]any{"revision": int64(revision)}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := items.DeleteOne(context.Background(), Filter{"_id": first}); err != nil {
		t.Fatal(err)
	}
	if _, err := items.InsertOne(context.Background(), Document{"_id": ID(first), "value": Int(40), "group": String("same")}); err != nil {
		t.Fatal(err)
	}
	sourceIdentity := db.DatabaseIdentity()
	expected := queryIDs(t, items, Filter{}, QueryOptions{})
	if !reflect.DeepEqual(expected, []DocumentID{second, third, first}) {
		t.Fatalf("source order=%v", expected)
	}
	if err := db.CompactToV2(context.Background(), destinationPath); err != nil {
		t.Fatal(err)
	}
	if db.DatabaseIdentity() != sourceIdentity {
		t.Fatal("compaction mutated source identity")
	}
	stats := db.Stats().Compaction
	if stats.Active != 0 || stats.Attempts != 1 || stats.Completed != 1 || stats.Failed != 0 ||
		stats.InputBytes == 0 || stats.OutputBytes == 0 || stats.OutputBytes >= stats.InputBytes || stats.LastDuration <= 0 {
		t.Fatalf("compaction stats=%+v", stats)
	}
	if matches, err := filepath.Glob(filepath.Join(directory, ".compact.meld2.compact-*")); err != nil || len(matches) != 0 {
		t.Fatalf("temporary compaction files=%v err=%v", matches, err)
	}

	compacted, err := Open(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer compacted.Close()
	if compacted.DatabaseIdentity() == sourceIdentity {
		t.Fatal("compacted database reused source identity")
	}
	if actual := queryIDs(t, compacted.Collection("items"), Filter{}, QueryOptions{}); !reflect.DeepEqual(actual, expected) {
		t.Fatalf("compacted order=%v want=%v", actual, expected)
	}
	if actual := queryIDs(t, compacted.Collection("items"), Filter{"group": "same"}, QueryOptions{}); !reflect.DeepEqual(actual, []DocumentID{second, first}) {
		t.Fatalf("compacted index result=%v", actual)
	}
	if explain, err := compacted.Collection("items").Explain(context.Background(), Filter{"group": "same", "value": map[string]any{"$gte": int64(20)}}); err != nil || explain.IndexName != "group_value" || explain.Stage != "IXSCAN" {
		t.Fatalf("compacted compound explain=%+v err=%v", explain, err)
	}
	if compacted.durability.(*v2DurableStore).file.Meta().RequiredFeatures&storagev2.RequiredFeatureCompoundIndexes == 0 {
		t.Fatal("compaction lost compound-index required feature")
	}
	if _, err := compacted.Collection("items").InsertOne(context.Background(), Document{"value": Int(40)}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("compacted unique index error=%v", err)
	}
	emptyQuery, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot, err := compacted.Collection("empty").SnapshotQuery(context.Background(), emptyQuery); err != nil || len(snapshot.Documents) != 0 {
		t.Fatalf("compacted empty collection=%+v err=%v", snapshot, err)
	}
}

func TestCompactToV2DestinationQuotaFailsWithoutPublication(t *testing.T) {
	directory := t.TempDir()
	db, err := Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "too-small.meld2")
	err = db.CompactToV2WithOptions(context.Background(), destination, V2DestinationOptions{
		StorageLimits: V2StorageLimits{MaxFileBytes: 2 * storagev2.PageSize},
	})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("compaction quota error=%v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed compaction published destination: %v", err)
	}
	if db.Stats().WritesDisabled {
		t.Fatal("destination quota poisoned source")
	}
}

func TestCompactToV2IndexBuildLimitFailsWithoutPublication(t *testing.T) {
	directory := t.TempDir()
	db, err := Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertMany(context.Background(), []Document{{"value": Int(1)}, {"value": Int(2)}, {"value": Int(3)}}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "limited.meld2")
	err = db.CompactToV2WithOptions(context.Background(), destination, V2DestinationOptions{ResourceLimits: ResourceLimits{MaxIndexBuildEntries: 2}})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("compaction index limit error=%v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed compaction published destination: %v", err)
	}
	if db.Stats().WritesDisabled {
		t.Fatal("destination index limit poisoned source")
	}
}

func TestCompactToV2FailsClosedWithoutOverwriting(t *testing.T) {
	directory := t.TempDir()
	db, err := Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "exists.meld2")
	if err := os.WriteFile(destination, []byte("owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := db.CompactToV2(context.Background(), destination); !errors.Is(err, ErrCompactionDestinationExists) {
		t.Fatalf("existing destination error=%v", err)
	}
	if content, err := os.ReadFile(destination); err != nil || string(content) != "owner" {
		t.Fatalf("destination content=%q err=%v", content, err)
	}
	if err := db.CompactToV2(context.Background(), filepath.Join(directory, "source.meld2")); !errors.Is(err, ErrCompactionDestinationExists) {
		t.Fatalf("source destination error=%v", err)
	}
	stats := db.Stats().Compaction
	if stats.Attempts != 2 || stats.Completed != 0 || stats.Failed != 2 || stats.Active != 0 {
		t.Fatalf("failed compaction stats=%+v", stats)
	}

	v1 := New()
	defer v1.Close()
	if err := v1.CompactToV2(context.Background(), filepath.Join(directory, "unsupported.meld2")); !errors.Is(err, ErrCompactionUnsupported) {
		t.Fatalf("unsupported compaction error=%v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.CompactToV2(cancelled, filepath.Join(directory, "cancelled.meld2")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled compaction error=%v", err)
	}
}

func TestCompactToV2PinsSnapshotWithoutBlockingConcurrentWrites(t *testing.T) {
	directory := t.TempDir()
	db, err := Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	id, err := items.InsertOne(context.Background(), Document{"value": String("before")})
	if err != nil {
		t.Fatal(err)
	}
	store := db.durability.(*v2DurableStore)
	snapshotPinned, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	store.testCompactionSnapshotHook = func() {
		once.Do(func() { close(snapshotPinned) })
		<-release
	}
	defer func() { store.testCompactionSnapshotHook = nil }()
	destination := filepath.Join(directory, "compact.meld2")
	compacted := make(chan error, 1)
	go func() { compacted <- db.CompactToV2(context.Background(), destination) }()
	select {
	case <-snapshotPinned:
	case <-time.After(3 * time.Second):
		t.Fatal("compaction did not pin a source snapshot")
	}

	written := make(chan error, 1)
	go func() {
		_, err := items.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": "after"}})
		written <- err
	}()
	select {
	case err := <-written:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("concurrent write waited for compacted snapshot copy")
	}
	close(release)
	select {
	case err := <-compacted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not finish")
	}

	current, err := items.FindOne(context.Background(), Filter{"_id": id})
	if err != nil {
		t.Fatal(err)
	}
	if value, _ := current["value"].StringValue(); value != "after" {
		t.Fatalf("source value=%q", value)
	}
	compactedDB, err := Open(destination)
	if err != nil {
		t.Fatal(err)
	}
	defer compactedDB.Close()
	compactedDocument, err := compactedDB.Collection("items").FindOne(context.Background(), Filter{"_id": id})
	if err != nil {
		t.Fatal(err)
	}
	if value, _ := compactedDocument["value"].StringValue(); value != "before" {
		t.Fatalf("compacted snapshot included post-snapshot write=%q", value)
	}
}

func TestV2CloseWaitsForPinnedCompactionSnapshot(t *testing.T) {
	directory := t.TempDir()
	db, err := Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	store := db.durability.(*v2DurableStore)
	pinned, release := make(chan struct{}), make(chan struct{})
	store.testCompactionSnapshotHook = func() {
		close(pinned)
		<-release
	}
	destination := filepath.Join(directory, "compact.meld2")
	compacted := make(chan error, 1)
	go func() { compacted <- db.CompactToV2(context.Background(), destination) }()
	select {
	case <-pinned:
	case <-time.After(3 * time.Second):
		_ = db.Close()
		t.Fatal("compaction did not pin its snapshot")
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		close(release)
		t.Fatalf("Close completed while compaction still held source snapshot: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-compacted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not finish")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not resume after compaction")
	}
	store.testCompactionSnapshotHook = nil
	compactedDB, err := Open(destination)
	if err != nil {
		t.Fatalf("compacted destination was not durable before Close: %v", err)
	}
	if err := compactedDB.Close(); err != nil {
		t.Fatal(err)
	}
}
