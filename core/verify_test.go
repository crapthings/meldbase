package meldbase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	storage "github.com/crapthings/meldbase/internal/storage"
)

func TestVerifyFileAuditsBusinessGraphAndDoesNotMutateBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	id, err := items.InsertOne(context.Background(), Document{"value": Int(1), "group": String("a")})
	if err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "group_value", []IndexField{{Field: "group", Order: 1}, {Field: "value", Order: -1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	for value := int64(2); value <= 8; value++ {
		if _, err := items.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": value}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ReclaimPages(context.Background()); err != nil {
		t.Fatal(err)
	}
	identity := db.DatabaseIdentity()
	sequence := db.Stats().CommitSequence
	if _, err := VerifyFile(context.Background(), path); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("active writer verification error=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(before)
	report, err := VerifyFile(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if report.SchemaVersion != 3 || !report.Verified || !report.IndexContentsVerified || !report.IndexBuildContentsVerified || report.Format != StorageFormatCurrent || report.Revision != 3 ||
		report.DatabaseIDHex != hex.EncodeToString(identity[:]) || report.CommitSequence != sequence ||
		report.RequiredFeatures&storage.RequiredFeatureCompoundIndexes == 0 ||
		report.FileBytes != uint64(len(before)) || report.PhysicalPages < report.CommittedPhysicalPages ||
		report.ReachablePages == 0 || report.ValidMetaSlots == 0 || report.SHA256 != hex.EncodeToString(digest[:]) ||
		!report.PersistentFreeSpace || !report.FreeSpaceValid {
		t.Fatalf("verification report=%+v", report)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("verification mutated bytes: err=%v", err)
	}
}

func TestVerifyFileAuditsCompoundAndPartialIndexContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify-compound.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	if _, err := items.InsertMany(context.Background(), []Document{
		{"workspace": String("a"), "score": Int(1)},
		{"workspace": String("a")},
		{"workspace": String("a")},
		{"score": Int(2)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "workspace_score", []IndexField{
		{Field: "workspace", Order: 1}, {Field: "score", Order: -1},
	}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyFile(context.Background(), path)
	if err != nil || !report.Verified || !report.IndexContentsVerified || !report.IndexBuildContentsVerified || report.SchemaVersion != 3 {
		t.Fatalf("compound verification=%+v err=%v", report, err)
	}
}

func TestVerifyFileAuditsCaughtUpCompoundIndexBuild(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify-index-build.meld2")
	file, _, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ids := [][16]byte{{1}, {2}, {3}}
	documents := []Document{
		{"_id": ID(DocumentID(ids[0])), "workspace": String("a"), "score": Int(3)},
		{"_id": ID(DocumentID(ids[1])), "workspace": String("a")},
		{"_id": ID(DocumentID(ids[2])), "workspace": String("b"), "score": Int(1)},
	}
	encoded := make([][]byte, len(documents))
	for index := range documents {
		encoded[index], err = encodeStoredDocument(documents[index])
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := file.ApplyDocumentTransaction(storage.DocumentTransaction{
		TransactionID: [16]byte{1}, Mutations: []storage.DocumentMutation{
			{Collection: "items", DocumentID: ids[0], Operation: storage.DocumentInsert, Document: encoded[0]},
			{Collection: "items", DocumentID: ids[1], Operation: storage.DocumentInsert, Document: encoded[1]},
		},
	}); err != nil {
		t.Fatal(err)
	}
	fields := []IndexField{{Field: "workspace", Order: 1}, {Field: "score", Order: -1}}
	definition := newIndexDefinition("workspace_score", fields, false)
	keys := make([][]byte, len(documents))
	for index := range documents {
		keys[index], _, err = projectedIndexBuildKey(encoded[index], definition, DocumentID(ids[index]))
		if err != nil {
			t.Fatal(err)
		}
	}
	buildID := [16]byte{9}
	if _, err := file.BeginIndexBuild(storage.BeginIndexBuildTransaction{
		BuildID: buildID, Collection: "items", Name: definition.Name,
		Fields: []storage.IndexField{{Path: "workspace", Direction: 1}, {Path: "score", Direction: -1}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyIndexBuildScanBatch(storage.IndexBuildScanBatch{
		BuildID: buildID, ScanAfter: ids[0], Entries: []storage.IndexEntry{{Key: keys[0], DocumentID: ids[0]}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(storage.DocumentTransaction{
		TransactionID: [16]byte{2}, Mutations: []storage.DocumentMutation{{
			Collection: "items", DocumentID: ids[2], Operation: storage.DocumentInsert, Document: encoded[2],
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyIndexBuildScanBatch(storage.IndexBuildScanBatch{
		BuildID: buildID, ExpectedScanAfter: ids[0], ScanAfter: ids[1], Complete: true,
		Entries: []storage.IndexEntry{{Key: keys[1], DocumentID: ids[1]}},
	}); err != nil {
		t.Fatal(err)
	}
	caughtUp, err := file.ApplyIndexBuildCatchUpBatch(storage.IndexBuildCatchUpBatch{
		BuildID: buildID, ExpectedAppliedSequence: 1, ThroughSequence: 2,
		Mutations: []storage.IndexBuildCatchUpMutation{{
			Sequence: 2, DocumentID: ids[2], Operation: storage.CommitInsert, AfterKey: keys[2],
		}},
	})
	if err != nil || caughtUp.AppliedCatalogRoot < 2 || file.Meta().RequiredFeatures&storage.RequiredFeatureIndexBuildAppliedRoot == 0 {
		t.Fatalf("caught-up build=%+v meta=%+v err=%v", caughtUp, file.Meta(), err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyFile(context.Background(), path)
	if err != nil || report.SchemaVersion != 3 || !report.IndexContentsVerified || !report.IndexBuildContentsVerified {
		t.Fatalf("index-build verification=%+v err=%v", report, err)
	}
}

func TestVerifyFileTreatsReadyUniqueConflictAsValidPrivateState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify-private-unique-conflict.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
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
		t.Fatalf("unique build error=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyFile(context.Background(), path)
	if err != nil || !report.IndexBuildContentsVerified {
		t.Fatalf("private unique-conflict verification=%+v err=%v", report, err)
	}
}

func TestVerifyFileRejectsUnsupportedAndCanceledInputs(t *testing.T) {
	directory := t.TempDir()
	if _, err := VerifyFile(context.Background(), filepath.Join(directory, "missing")); !errors.Is(err, ErrVerificationUnsupported) {
		t.Fatalf("missing verification error=%v", err)
	}

	databasePath := filepath.Join(directory, "cancel.meld2")
	store, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := VerifyFile(ctx, databasePath); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled verification error=%v", err)
	}
}
