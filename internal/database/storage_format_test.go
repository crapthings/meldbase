package database

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenCreatesAndReopensCurrentFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if format, err := DetectStorageFormat(path); err != nil || format != StorageFormatCurrent {
		t.Fatalf("format=%q err=%v", format, err)
	}
	info, err := InspectStorageFormat(path)
	if err != nil || info.Format != StorageFormatCurrent || !info.ReaderCompatible || info.DatabaseIDHex == "" {
		t.Fatalf("info=%+v err=%v", info, err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err != nil {
		t.Fatal(err)
	}
}

func TestFormatInspectionIsReadOnlyAndFailsClosed(t *testing.T) {
	directory := t.TempDir()
	missing := filepath.Join(directory, "missing.meld")
	if format, err := DetectStorageFormat(missing); err != nil || format != StorageFormatUnknown {
		t.Fatalf("missing format=%q err=%v", format, err)
	}
	path := filepath.Join(directory, "database.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InspectStorageFormat(path); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("inspection mutated path: %v", err)
	}
	corrupt := filepath.Join(directory, "corrupt.meld")
	if err := os.WriteFile(corrupt, []byte("not a database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(corrupt); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open corrupt err=%v", err)
	}
}

func TestOpenPreservesCorruptCurrentFormatBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	bytesBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	bytesBefore[8]++
	if err := os.WriteFile(path, bytesBefore, 0o600); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(path); opened != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open=%v err=%v", opened, err)
	}
	bytesAfter, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(bytesBefore, bytesAfter) {
		t.Fatalf("open mutated unsupported file: %v", err)
	}
}
