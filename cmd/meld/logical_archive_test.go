package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/crapthings/meldbase/core"
)

func TestLogicalArchiveCommandsExportAndImport(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.meld")
	archive := filepath.Join(directory, "archive.jsonl")
	destination := filepath.Join(directory, "destination.meld")
	db, err := meldbase.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateCollection(context.Background(), "empty"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(7)}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var exported bytes.Buffer
	if err := run([]string{"export", "--db", source, "--out", archive}, &exported, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var exportReceipt logicalArchiveCommandResult
	if err := json.Unmarshal(exported.Bytes(), &exportReceipt); err != nil {
		t.Fatal(err)
	}
	if exportReceipt.SchemaVersion != 1 || exportReceipt.ArtifactKind != logicalArchiveArtifact || exportReceipt.Collections != 2 || exportReceipt.Documents != 1 {
		t.Fatalf("export receipt=%+v", exportReceipt)
	}
	var imported bytes.Buffer
	if err := run([]string{"import", "--in", archive, "--out", destination}, &imported, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var importReceipt logicalArchiveCommandResult
	if err := json.Unmarshal(imported.Bytes(), &importReceipt); err != nil {
		t.Fatal(err)
	}
	if importReceipt.LogicalArchiveResult != exportReceipt.LogicalArchiveResult {
		t.Fatalf("import receipt=%+v export receipt=%+v", importReceipt, exportReceipt)
	}
	result, err := meldbase.Open(destination)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if _, err := result.Collection("items").FindOne(context.Background(), meldbase.Filter{"value": 7}); err != nil {
		t.Fatal(err)
	}
}
