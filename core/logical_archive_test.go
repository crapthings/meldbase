package meldbase

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLogicalArchiveRoundTripPreservesSchemaAndTypedDocuments(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.meld")
	archivePath := filepath.Join(directory, "snapshot.meld.jsonl")
	destinationPath := filepath.Join(directory, "destination.meld")
	source, err := Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := source.CreateCollection(ctx, "empty"); err != nil {
		t.Fatal(err)
	}
	if err := source.CreateCollection(ctx, "indexed_empty"); err != nil {
		t.Fatal(err)
	}
	if err := source.Collection("indexed_empty").CreateIndex(ctx, "by_label", []IndexField{{Field: "label", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	items := source.Collection("items")
	array := make([]Value, 300)
	for index := range array {
		array[index] = Int(int64(index))
	}
	first, err := items.InsertOne(ctx, Document{
		"tenant": String("a"), "email": String("ada@example.com"), "when": Time(time.UnixMilli(1_719_000_000_123)),
		"blob": Binary([]byte{0, 1, 2, 255}), "nested": Object(Document{"active": Bool(true), "array": Array(array...)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := items.InsertOne(ctx, Document{"tenant": String("b"), "email": String("bea@example.com"), "score": Float(1.25)})
	if err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(ctx, "tenant_email", []IndexField{{Field: "tenant", Order: 1}, {Field: "email", Order: -1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	sourceIdentity := source.DatabaseIdentity()
	exported, err := source.ExportLogicalArchive(ctx, archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if exported.Format != logicalArchiveFormat || exported.Version != logicalArchiveVersion || exported.Collections != 3 || exported.Documents != 2 || exported.Indexes != 2 || exported.Bytes == 0 || len(exported.SHA256) != 64 {
		t.Fatalf("export result=%+v", exported)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	imported, err := ImportLogicalArchive(ctx, bytes.NewReader(archive), destinationPath, LogicalArchiveImportOptions{MaxBytes: uint64(len(archive))})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(imported, exported) {
		t.Fatalf("import=%+v export=%+v", imported, exported)
	}
	destination, err := Open(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if destination.DatabaseIdentity() == sourceIdentity {
		t.Fatal("logical import unexpectedly preserved database identity")
	}
	if _, exists := destination.collections["empty"]; !exists {
		t.Fatal("empty collection was not restored")
	}
	if _, exists := destination.collections["indexed_empty"]; !exists {
		t.Fatal("indexed empty collection was not restored")
	}
	for _, id := range []DocumentID{first, second} {
		got, err := destination.Collection("items").FindOne(ctx, Filter{"_id": id})
		if err != nil {
			t.Fatal(err)
		}
		wantSource, err := Open(sourcePath)
		if err != nil {
			t.Fatal(err)
		}
		want, findErr := wantSource.Collection("items").FindOne(ctx, Filter{"_id": id})
		closeErr := wantSource.Close()
		if findErr != nil || closeErr != nil {
			t.Fatalf("source find=%v close=%v", findErr, closeErr)
		}
		if !got.Equal(want) {
			t.Fatalf("id=%s got=%v want=%v", id, got, want)
		}
	}
	explain, err := destination.Collection("items").Explain(ctx, Filter{"tenant": "a", "email": "ada@example.com"})
	if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "tenant_email" {
		t.Fatalf("explain=%+v err=%v", explain, err)
	}
}

func TestLogicalArchiveRejectsAlteredInputWithoutPublishingDestination(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.meld")
	archivePath := filepath.Join(directory, "snapshot.jsonl")
	destinationPath := filepath.Join(directory, "destination.meld")
	db, err := Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExportLogicalArchive(context.Background(), archivePath); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	archive[len(archive)-2] = '0' // corrupt the final digest without invalidating JSON.
	if _, err := ImportLogicalArchive(context.Background(), bytes.NewReader(archive), destinationPath, LogicalArchiveImportOptions{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt archive error=%v", err)
	}
	if _, err := os.Stat(destinationPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed import published destination: %v", err)
	}
	if err := os.WriteFile(destinationPath, []byte("owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ImportLogicalArchive(context.Background(), bytes.NewReader([]byte("x")), destinationPath, LogicalArchiveImportOptions{}); !errors.Is(err, ErrLogicalArchiveDestinationExists) {
		t.Fatalf("existing destination error=%v", err)
	}
	content, err := os.ReadFile(destinationPath)
	if err != nil || string(content) != "owner" {
		t.Fatalf("destination changed=%q err=%v", content, err)
	}
}
