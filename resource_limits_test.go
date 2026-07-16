package meldbase

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIndexBuildLimitsRejectAtomicallyAcrossEngines(t *testing.T) {
	constructors := map[string]func(*testing.T) (*DB, string){
		"memory": func(t *testing.T) (*DB, string) {
			db, err := NewWithOptions(DatabaseOptions{ResourceLimits: ResourceLimits{MaxIndexBuildEntries: 2}})
			if err != nil {
				t.Fatal(err)
			}
			return db, ""
		},
		"v1": func(t *testing.T) (*DB, string) {
			path := filepath.Join(t.TempDir(), "database.meld")
			db, err := OpenV1WithOptions(path, V1Options{ResourceLimits: ResourceLimits{MaxIndexBuildEntries: 2}})
			if err != nil {
				t.Fatal(err)
			}
			return db, path
		},
		"v2": func(t *testing.T) (*DB, string) {
			path := filepath.Join(t.TempDir(), "database.meld2")
			db, err := OpenV2WithOptions(path, V2Options{ResourceLimits: ResourceLimits{MaxIndexBuildEntries: 2}})
			if err != nil {
				t.Fatal(err)
			}
			return db, path
		},
	}
	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			db, path := construct(t)
			defer db.Close()
			items := db.Collection("items")
			if _, err := items.InsertMany(context.Background(), []Document{{"value": Int(1)}, {"value": Int(2)}, {"value": Int(3)}}); err != nil {
				t.Fatal(err)
			}
			sequence := db.Stats().CommitSequence
			var before []byte
			var beforeWAL []byte
			if path != "" {
				before, _ = os.ReadFile(path)
				beforeWAL, _ = os.ReadFile(path + ".wal")
			}
			if err := items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("CreateIndex error=%v", err)
			}
			stats := db.Stats()
			if stats.CommitSequence != sequence || stats.Resources.Rejections != 1 || stats.WritesDisabled || stats.Indexes != 0 ||
				stats.IndexBuilds.Active != 0 || stats.IndexBuilds.Attempts != 1 || stats.IndexBuilds.Completed != 0 ||
				stats.IndexBuilds.Failed != 1 || stats.IndexBuilds.LastEntries != 2 || stats.IndexBuilds.LastBytes == 0 {
				t.Fatalf("stats=%+v", stats)
			}
			if path != "" {
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("rejected index changed durable file: equal=%v err=%v", bytes.Equal(before, after), err)
				}
				if name == "v1" {
					afterWAL, err := os.ReadFile(path + ".wal")
					if err != nil || !bytes.Equal(beforeWAL, afterWAL) {
						t.Fatalf("rejected index changed WAL: equal=%v err=%v", bytes.Equal(beforeWAL, afterWAL), err)
					}
				}
			}
			if _, err := items.InsertOne(context.Background(), Document{"value": Int(4)}); err != nil {
				t.Fatalf("write after rejection=%v", err)
			}
		})
	}
}

func TestIndexBuildByteLimitUsesCanonicalSecondaryKeyBytes(t *testing.T) {
	db, err := NewWithOptions(DatabaseOptions{ResourceLimits: ResourceLimits{MaxIndexBuildBytes: 24}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), Document{"value": String("x")}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("CreateIndex error=%v", err)
	}
}

func TestCanonicalDocumentSizeMatchesEncoder(t *testing.T) {
	document := Document{
		"array":  Array(Int(1), Object(Document{"nested": String("value")})),
		"binary": Binary([]byte{1, 2, 3}),
		"flag":   Bool(true),
	}
	size, err := canonicalDocumentSize(document)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeDocumentBinary(document)
	if err != nil {
		t.Fatal(err)
	}
	if size != uint64(len(encoded)) {
		t.Fatalf("canonical size=%d encoded=%d", size, len(encoded))
	}
	if allocations := testing.AllocsPerRun(100, func() {
		_, _ = canonicalDocumentSize(document)
	}); allocations != 0 {
		t.Fatalf("canonical measurement allocations = %g", allocations)
	}
}

func TestResourceLimitsRejectAtomicallyAndAreObservable(t *testing.T) {
	db, err := NewWithOptions(DatabaseOptions{ResourceLimits: ResourceLimits{
		MaxDocumentBytes: 64, MaxTransactionBytes: 128, MaxTransactionChanges: 2,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("items")

	if _, err := collection.InsertOne(context.Background(), Document{"value": String(strings.Repeat("x", 35))}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("large document error = %v", err)
	}
	if _, err := collection.InsertMany(context.Background(), []Document{{"value": Int(1)}, {"value": Int(2)}, {"value": Int(3)}}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("large transaction error = %v", err)
	}
	cursor, err := collection.Find(context.Background(), Filter{}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := cursor.All(context.Background())
	if err != nil || len(got) != 0 {
		t.Fatalf("rejected writes became visible: len=%d err=%v", len(got), err)
	}
	stats := db.Stats()
	if stats.Resources.Limits.MaxDocumentBytes != 64 || stats.Resources.Rejections != 2 || stats.CommitSequence != 0 {
		t.Fatalf("resource stats = %+v sequence=%d", stats.Resources, stats.CommitSequence)
	}

	for index := int64(0); index < 3; index++ {
		if _, err := collection.InsertOne(context.Background(), Document{"value": Int(index)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := collection.UpdateMany(context.Background(), Filter{}, Update{"$set": map[string]any{"changed": true}}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unbounded update error = %v", err)
	}
	if stats := db.Stats(); stats.Resources.Rejections != 3 || stats.CommitSequence != 3 {
		t.Fatalf("post-update stats = %+v sequence=%d", stats.Resources, stats.CommitSequence)
	}
}

func TestInvalidResourceLimitsFailBeforeOpen(t *testing.T) {
	invalid := ResourceLimits{MaxDocumentBytes: 128, MaxTransactionBytes: 64}
	if _, err := NewWithOptions(DatabaseOptions{ResourceLimits: invalid}); !errors.Is(err, ErrInvalidResourceLimits) {
		t.Fatalf("memory error = %v", err)
	}
	path := t.TempDir() + "/database.meld"
	if _, err := OpenWithOptions(path, OpenOptions{ResourceLimits: invalid}); !errors.Is(err, ErrInvalidResourceLimits) {
		t.Fatalf("open error = %v", err)
	}
}

func BenchmarkCanonicalDocumentSize(b *testing.B) {
	document := Document{
		"array":  Array(Int(1), Object(Document{"nested": String("value")})),
		"binary": Binary([]byte{1, 2, 3}),
		"flag":   Bool(true),
	}
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		_, _ = canonicalDocumentSize(document)
	}
}
