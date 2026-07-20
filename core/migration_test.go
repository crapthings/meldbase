package meldbase

import (
	"context"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

func TestMigrateToV2PreservesCollectionsOrderDocumentsIndexesAndReplay(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.meld")
	destinationPath := filepath.Join(directory, "destination.meld2")
	source, err := OpenV1(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()

	empty := source.Collection("empty")
	emptyID, err := empty.InsertOne(context.Background(), Document{"temporary": Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := empty.DeleteOne(context.Background(), Filter{"_id": emptyID}); err != nil || result.DeletedCount != 1 {
		t.Fatalf("empty delete=%+v err=%v", result, err)
	}

	items := source.Collection("items")
	first, second, third := DocumentID{2}, DocumentID{1}, DocumentID{3}
	if _, err := items.InsertMany(context.Background(), []Document{
		{"_id": ID(first), "value": Int(10), "group": String("same"), "when": Time(time.UnixMilli(1234))},
		{"_id": ID(second), "value": Int(20), "group": String("same"), "payload": Binary([]byte{0, 1, 2})},
		{"_id": ID(third), "value": Int(30), "group": String("other"), "nested": Object(map[string]Value{"ok": Bool(true)})},
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := items.DeleteOne(context.Background(), Filter{"_id": first}); err != nil || result.DeletedCount != 1 {
		t.Fatalf("delete=%+v err=%v", result, err)
	}
	if _, err := items.InsertOne(context.Background(), Document{
		"_id": ID(first), "value": Int(40), "group": String("same"), "when": Time(time.UnixMilli(5678)),
	}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_group", []IndexField{{Field: "group", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	sourceIdentity, sourceToken := source.DatabaseIdentity(), source.Stats().CommitSequence

	if err := source.MigrateToV2(context.Background(), destinationPath); err != nil {
		t.Fatal(err)
	}
	if source.DatabaseIdentity() != sourceIdentity || source.Stats().CommitSequence != sourceToken {
		t.Fatalf("migration mutated source identity=%x token=%d", source.DatabaseIdentity(), source.Stats().CommitSequence)
	}
	if matches, err := filepath.Glob(filepath.Join(directory, ".destination.meld2.migrate-*")); err != nil || len(matches) != 0 {
		t.Fatalf("temporary files=%v err=%v", matches, err)
	}

	destination, err := OpenV2(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if destination.DatabaseIdentity() == sourceIdentity {
		t.Fatal("migration preserved source database identity")
	}
	if destination.Stats().CommitSequence != 5 {
		t.Fatalf("migration sequence=%d", destination.Stats().CommitSequence)
	}
	if data := destination.collections["empty"]; data == nil || len(data.documents) != 0 || len(data.indexes) != 0 {
		t.Fatalf("empty collection=%+v", data)
	}
	cursor, err := destination.Collection("items").Find(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	documents, err := cursor.All(context.Background())
	if err != nil || len(documents) != 3 {
		t.Fatalf("documents=%+v err=%v", documents, err)
	}
	for index, expected := range []DocumentID{second, third, first} {
		actual, _ := documents[index].ID()
		if actual != expected {
			t.Fatalf("order[%d]=%v want=%v", index, actual, expected)
		}
	}
	explain, err := destination.Collection("items").Explain(context.Background(), Filter{"value": int64(40)})
	if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "by_value" {
		t.Fatalf("explain=%+v err=%v", explain, err)
	}
	if _, err := destination.Collection("items").InsertOne(context.Background(), Document{"value": Int(40)}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("unique index error=%v", err)
	}

	query, err := CompileQuery(Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := destination.OpenQueryReplay(context.Background(), "items", query, 2, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if replay.Initial.Token != 2 || len(replay.Initial.Documents) != 0 {
		t.Fatalf("initial replay=%+v", replay.Initial)
	}
	select {
	case delta := <-replay.Deltas:
		if delta.FromToken != 2 || delta.Token != 3 || len(delta.Operations) != 3 {
			t.Fatalf("migration replay delta=%+v", delta)
		}
	case err := <-replay.Errors:
		t.Fatalf("migration replay error=%v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("migration replay timeout")
	}
}

func TestMigrateToV2DestinationQuotaFailsWithoutPublication(t *testing.T) {
	directory := t.TempDir()
	source, err := OpenV1(filepath.Join(directory, "source.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "too-small.meld2")
	err = source.MigrateToV2WithOptions(context.Background(), destination, V2DestinationOptions{
		StorageLimits: V2StorageLimits{MaxFileBytes: 2 * storagev2.PageSize},
	})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("migration quota error=%v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed migration published destination: %v", err)
	}
}

func TestMigrateToV2IndexBuildLimitFailsWithoutPublication(t *testing.T) {
	directory := t.TempDir()
	source, err := OpenV1(filepath.Join(directory, "source.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	items := source.Collection("items")
	if _, err := items.InsertMany(context.Background(), []Document{{"value": Int(1)}, {"value": Int(2)}, {"value": Int(3)}}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "limited.meld2")
	err = source.MigrateToV2WithOptions(context.Background(), destination, V2DestinationOptions{ResourceLimits: ResourceLimits{MaxIndexBuildEntries: 2}})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("migration index limit error=%v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed migration published destination: %v", err)
	}
	if source.Stats().WritesDisabled {
		t.Fatal("destination index limit poisoned source")
	}
}

func TestMigrateToV2NeverOverwritesDestination(t *testing.T) {
	directory := t.TempDir()
	source, err := OpenV1(filepath.Join(directory, "source.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "existing.meld2")
	sentinel := []byte("do-not-overwrite")
	if err := os.WriteFile(destination, sentinel, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := source.MigrateToV2(context.Background(), destination); !errors.Is(err, ErrMigrationDestinationExists) {
		t.Fatalf("destination error=%v", err)
	}
	after, err := os.ReadFile(destination)
	if err != nil || string(after) != string(sentinel) {
		t.Fatalf("destination=%q err=%v", after, err)
	}
	if err := source.MigrateToV2(context.Background(), filepath.Join(directory, "source.meld")); !errors.Is(err, ErrMigrationDestinationExists) {
		t.Fatalf("same path error=%v", err)
	}
}

func TestMigrateToV2BatchesDocumentsWithoutChangingOrder(t *testing.T) {
	directory := t.TempDir()
	source, err := OpenV1(filepath.Join(directory, "batched-source.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	documents := make([]Document, migrationDocumentBatchCount+1)
	ids := make([]DocumentID, len(documents))
	for index := range documents {
		binary.BigEndian.PutUint64(ids[index][8:], uint64(index+1))
		documents[index] = Document{"_id": ID(ids[index]), "position": Int(int64(index))}
	}
	if _, err := source.Collection("items").InsertMany(context.Background(), documents); err != nil {
		t.Fatal(err)
	}
	destinationPath := filepath.Join(directory, "batched-destination.meld2")
	if err := source.MigrateToV2(context.Background(), destinationPath); err != nil {
		t.Fatal(err)
	}
	destination, err := OpenV2(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if destination.Stats().CommitSequence != 3 {
		t.Fatalf("sequence=%d, want create + two document batches", destination.Stats().CommitSequence)
	}
	data := destination.collections["items"]
	if data == nil || len(data.order) != 0 || len(data.documents) != 0 {
		t.Fatalf("V2 retained document mirror: %+v", data)
	}
	cursor, err := destination.Collection("items").Find(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	migrated, err := cursor.All(context.Background())
	if err != nil || len(migrated) != len(ids) {
		t.Fatalf("migrated count=%d err=%v", len(migrated), err)
	}
	for index, id := range ids {
		actual, ok := migrated[index].ID()
		if !ok || actual != id {
			t.Fatalf("order[%d]=%v/%t want=%v", index, actual, ok, id)
		}
	}
}

func TestMigrateToV2RejectsUnsupportedCancelledAndCorruptSourceWithoutPublishing(t *testing.T) {
	directory := t.TempDir()
	memory := New()
	defer memory.Close()
	if err := memory.MigrateToV2(context.Background(), filepath.Join(directory, "memory.meld2")); !errors.Is(err, ErrMigrationUnsupported) {
		t.Fatalf("memory migration error=%v", err)
	}
	v2, err := OpenV2(filepath.Join(directory, "already-v2.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	if err := v2.MigrateToV2(context.Background(), filepath.Join(directory, "v2-copy.meld2")); !errors.Is(err, ErrMigrationUnsupported) {
		t.Fatalf("V2 migration error=%v", err)
	}
	_ = v2.Close()

	source, err := OpenV1(filepath.Join(directory, "source.meld"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	cancelledDestination := filepath.Join(directory, "cancelled.meld2")
	if err := source.MigrateToV2(cancelled, cancelledDestination); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	if _, err := os.Stat(cancelledDestination); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("cancelled destination stat=%v", err)
	}

	source.collections["broken"] = &collectionData{
		documents: map[DocumentID]Document{{1}: {"_id": ID(DocumentID{1})}},
		indexes:   make(map[string]*indexState),
	}
	corruptDestination := filepath.Join(directory, "corrupt.meld2")
	if err := source.MigrateToV2(context.Background(), corruptDestination); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt migration error=%v", err)
	}
	delete(source.collections, "broken")
	if _, err := os.Stat(corruptDestination); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("corrupt destination stat=%v", err)
	}
	if matches, err := filepath.Glob(filepath.Join(directory, ".corrupt.meld2.migrate-*")); err != nil || len(matches) != 0 {
		t.Fatalf("corrupt temporary files=%v err=%v", matches, err)
	}
}

func TestMigrationPublicationCommitPointIsNoOverwriteAndFailureAtomic(t *testing.T) {
	directory := t.TempDir()
	temporary := filepath.Join(directory, ".verified-migration")
	if err := os.WriteFile(temporary, []byte("complete-verified-v2"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("link failure", func(t *testing.T) {
		destination := filepath.Join(directory, "link-failure.meld2")
		injected := errors.New("injected link failure")
		err := publishMigrationFile(temporary, destination, migrationPublishOps{
			link: func(string, string) error { return injected }, remove: os.Remove, syncDirectory: syncDirectory,
		})
		if !errors.Is(err, injected) {
			t.Fatalf("error=%v", err)
		}
		if _, err := os.Stat(destination); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("destination stat=%v", err)
		}
	})

	t.Run("directory sync failure rolls back link", func(t *testing.T) {
		destination := filepath.Join(directory, "sync-failure.meld2")
		injected := errors.New("injected directory sync failure")
		calls := 0
		err := publishMigrationFile(temporary, destination, migrationPublishOps{
			link: os.Link, remove: os.Remove,
			syncDirectory: func(string) error {
				calls++
				if calls == 1 {
					return injected
				}
				return nil
			},
		})
		if !errors.Is(err, injected) || calls != 2 {
			t.Fatalf("error=%v sync calls=%d", err, calls)
		}
		if _, err := os.Stat(destination); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("destination stat=%v", err)
		}
	})

	t.Run("existing destination is unchanged", func(t *testing.T) {
		destination := filepath.Join(directory, "existing-publication.meld2")
		if err := os.WriteFile(destination, []byte("owner-data"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := publishMigrationFile(temporary, destination, migrationPublishOps{
			link: os.Link, remove: os.Remove, syncDirectory: syncDirectory,
		})
		if !errors.Is(err, ErrMigrationDestinationExists) {
			t.Fatalf("error=%v", err)
		}
		contents, err := os.ReadFile(destination)
		if err != nil || string(contents) != "owner-data" {
			t.Fatalf("contents=%q err=%v", contents, err)
		}
	})

	t.Run("successful sync commits complete hard link", func(t *testing.T) {
		destination := filepath.Join(directory, "committed.meld2")
		if err := publishMigrationFile(temporary, destination, migrationPublishOps{
			link: os.Link, remove: os.Remove, syncDirectory: syncDirectory,
		}); err != nil {
			t.Fatal(err)
		}
		if !sameFile(temporary, destination) {
			t.Fatal("destination is not the verified migration inode")
		}
		contents, err := os.ReadFile(destination)
		if err != nil || string(contents) != "complete-verified-v2" {
			t.Fatalf("contents=%q err=%v", contents, err)
		}
	})
}
