package meldbase

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	storage "github.com/crapthings/meldbase/internal/storage"
)

func TestOnlineIndexBuildPersistsResumesAndPublishes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "online.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	for index := int64(0); index < 20; index++ {
		if _, err := items.InsertOne(context.Background(), Document{"value": Int(index)}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true})
	if err != nil {
		t.Fatal(err)
	}
	if id.IsZero() {
		t.Fatal("zero build id")
	}
	if stats := db.Stats().IndexBuilds; stats.Persistent != 1 || stats.Scanning != 1 || stats.PersistentEntries != 0 {
		t.Fatalf("durable build stats=%+v", stats)
	}
	status, err := db.IndexBuild(id)
	if err != nil || status.Phase != IndexBuildPhaseScan || status.SourceSequence != 20 {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	if err := items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("reserved CreateIndex=%v", err)
	}
	// This commit is newer than the protected source snapshot and must be
	// incorporated by the durable catch-up phase after reopen.
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(100)}); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats().IndexBuilds; !stats.RetentionLeaseActive {
		t.Fatalf("lagging durable build did not expose its retention lease: %+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	builds, err := db.IndexBuilds()
	if err != nil || len(builds) != 1 || builds[0].ID != id {
		t.Fatalf("builds=%+v err=%v", builds, err)
	}
	if err := db.ResumeIndexBuild(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.IndexBuild(id); !errors.Is(err, ErrIndexBuildNotFound) {
		t.Fatalf("published build lookup=%v", err)
	}
	if builds, err := db.IndexBuilds(); err != nil || len(builds) != 0 {
		t.Fatalf("remaining builds=%+v err=%v", builds, err)
	}
	if stats := db.Stats().IndexBuilds; stats.RetentionLeaseActive {
		t.Fatalf("published build retained its replay lease: %+v", stats)
	}
	if err := db.Collection("items").CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("published index duplicate=%v", err)
	}
	explain, err := db.Collection("items").Explain(context.Background(), Filter{"value": int64(100)})
	if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "by_value" {
		t.Fatalf("catch-up index explain=%+v err=%v", explain, err)
	}
	documents := queryIDs(t, db.Collection("items"), Filter{"value": int64(100)}, QueryOptions{})
	if len(documents) != 1 {
		t.Fatalf("catch-up query ids=%v", documents)
	}
}

func TestOnlineCompoundIndexBuildResumesCatchUpAndPublishes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "online-compound.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	first, second, missing := DocumentID{1}, DocumentID{2}, DocumentID{4}
	if _, err := items.InsertMany(context.Background(), []Document{
		{"_id": ID(first), "tenant": String("a"), "score": Int(8)},
		{"_id": ID(second), "tenant": String("a"), "score": Int(9)},
		{"_id": ID(missing), "tenant": String("a")},
	}); err != nil {
		t.Fatal(err)
	}
	fields := []IndexField{{Field: "tenant", Order: 1}, {Field: "score", Order: -1}}
	id, err := items.StartIndexBuild(context.Background(), "tenant_score", fields, IndexOptions{Unique: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err := db.IndexBuild(id)
	if err != nil || !reflect.DeepEqual(status.Fields, fields) || status.Field != "tenant" {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	if required := db.durability.(*durableStore).file.Meta().RequiredFeatures; required&storage.RequiredFeatureShadowIndexBuilds == 0 || required&storage.RequiredFeatureCompoundIndexes == 0 {
		t.Fatalf("required features = %#x", required)
	}
	third, err := items.InsertOne(context.Background(), Document{"tenant": String("a"), "score": Int(7)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": second}, Update{"$set": map[string]any{"score": int64(6)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := items.DeleteOne(context.Background(), Filter{"_id": first}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.ResumeIndexBuild(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	items = db.Collection("items")
	if got := queryIDs(t, items, Filter{"tenant": "a"}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{second, missing, third}) {
		t.Fatalf("published prefix IDs = %v", got)
	}
	if got := queryIDs(t, items, Filter{"tenant": "a", "score": map[string]any{"$gte": int64(6), "$lt": int64(8)}}, QueryOptions{}); !reflect.DeepEqual(got, []DocumentID{second, third}) {
		t.Fatalf("published range IDs = %v", got)
	}
	if _, err := items.InsertOne(context.Background(), Document{"tenant": String("a"), "score": Int(7)}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("published unique tuple error = %v", err)
	}
}

func TestIndexBuildStatsAttributeBindingRetentionPressure(t *testing.T) {
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "build-retention.meld"), OpenOptions{
		CommitRetention: CommitRetentionPolicy{MaxCommits: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	id, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for value := int64(2); value <= 5; value++ {
		if _, err := items.InsertOne(context.Background(), Document{"value": Int(value)}); err != nil {
			t.Fatal(err)
		}
	}
	stats := db.Stats()
	if !stats.Storage.RetentionPressure || !stats.IndexBuilds.RetentionLeaseActive ||
		!stats.IndexBuilds.RetentionPressure || stats.Storage.OldestRetainedSequence != 2 {
		t.Fatalf("binding build retention stats=%+v", stats)
	}
	if err := db.AbortIndexBuild(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(6)}); err != nil {
		t.Fatal(err)
	}
	stats = db.Stats()
	if stats.Storage.RetentionPressure || stats.IndexBuilds.RetentionLeaseActive || stats.IndexBuilds.RetentionPressure {
		t.Fatalf("released build retention stats=%+v", stats)
	}
}

func TestOnlineIndexBuildCatchesWriteRacingFinalization(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "finalize-race.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	id, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := db.durability.(*durableStore)
	ready, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	store.testPersistentIndexBuildReadyHook = func() {
		once.Do(func() {
			close(ready)
			<-release
		})
	}
	done := make(chan error, 1)
	go func() { done <- db.ResumeIndexBuild(context.Background(), id) }()
	<-ready
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(2)}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	store.testPersistentIndexBuildReadyHook = nil
	ids := queryIDs(t, items, Filter{"value": int64(2)}, QueryOptions{})
	if len(ids) != 1 {
		t.Fatalf("racing write absent from index: %v", ids)
	}
}

func TestOnlineIndexBuildCancellationAndAbort(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "abort.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	id, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.ResumeIndexBuild(canceled, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("resume cancellation=%v", err)
	}
	if _, err := db.IndexBuild(id); err != nil {
		t.Fatalf("cancellation lost build: %v", err)
	}
	if err := db.Compact(context.Background(), filepath.Join(t.TempDir(), "compacted.meld")); !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("compaction with private build=%v", err)
	}
	if err := db.AbortIndexBuild(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.IndexBuild(id); !errors.Is(err, ErrIndexBuildNotFound) {
		t.Fatalf("aborted build lookup=%v", err)
	}
}

func TestOnlineIndexBuildRequiresAndIDRoundTrips(t *testing.T) {
	db := New()
	if _, err := db.Collection("items").StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); !errors.Is(err, ErrIndexBuildUnsupported) {
		t.Fatalf("memory start=%v", err)
	}
	id := IndexBuildID{1, 2, 3}
	parsed, err := ParseIndexBuildID(id.String())
	if err != nil || parsed != id {
		t.Fatalf("parsed=%v err=%v", parsed, err)
	}
	if _, err := ParseIndexBuildID("bad"); !errors.Is(err, ErrIndexBuildNotFound) {
		t.Fatalf("malformed=%v", err)
	}
}

func TestOnlineUniqueBuildRejectsOnlyAtomicPublication(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "unique.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	for range 2 {
		if _, err := items.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ResumeIndexBuild(context.Background(), id); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("unique resume=%v", err)
	}
	status, err := db.IndexBuild(id)
	if err != nil || status.Phase != IndexBuildPhaseReady {
		t.Fatalf("private build status=%+v err=%v", status, err)
	}
	explain, err := items.Explain(context.Background(), Filter{"value": int64(1)})
	if err != nil || explain.Stage == "IXSCAN" {
		t.Fatalf("failed build became visible: explain=%+v err=%v", explain, err)
	}
	if err := db.AbortIndexBuild(context.Background(), id); err != nil {
		t.Fatal(err)
	}
}

func TestOnlineIndexBuildResourceRejectionPreservesDurableCursor(t *testing.T) {
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "bounded.meld"), OpenOptions{ResourceLimits: ResourceLimits{
		MaxIndexBuildEntries: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	for value := int64(1); value <= 2; value++ {
		if _, err := items.InsertOne(context.Background(), Document{"value": Int(value)}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ResumeIndexBuild(context.Background(), id); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("bounded resume=%v", err)
	}
	status, err := db.IndexBuild(id)
	if err != nil || status.Phase != IndexBuildPhaseScan || status.EntryCount != 0 || status.CanonicalBytes != 0 {
		t.Fatalf("resource rejection advanced durable state: status=%+v err=%v", status, err)
	}
	if stats := db.Stats(); stats.Resources.Rejections != 1 || stats.IndexBuilds.Persistent != 1 || stats.WritesDisabled {
		t.Fatalf("resource rejection stats=%+v", stats)
	}
}
