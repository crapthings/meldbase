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

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

func TestVerifyV2FileAuditsBusinessGraphAndDoesNotMutateBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify.meld2")
	db, err := OpenV2(path)
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
	if _, err := db.ReclaimV2Pages(context.Background()); err != nil {
		t.Fatal(err)
	}
	identity := db.DatabaseIdentity()
	sequence := db.Stats().CommitSequence
	if _, err := VerifyV2File(context.Background(), path); !errors.Is(err, ErrDatabaseLocked) {
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
	report, err := VerifyV2File(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if report.SchemaVersion != 3 || !report.Verified || !report.IndexContentsVerified || !report.IndexBuildContentsVerified || report.Format != StorageFormatV2 || report.Revision != 3 ||
		report.DatabaseIDHex != hex.EncodeToString(identity[:]) || report.CommitSequence != sequence ||
		report.RequiredFeatures&storagev2.RequiredFeatureCompoundIndexes == 0 ||
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

func TestVerifyV2FileAuditsCompoundAndPartialIndexContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify-compound.meld2")
	db, err := OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	if _, err := items.InsertMany(context.Background(), []Document{
		{"tenant": String("a"), "score": Int(1)},
		{"tenant": String("a")},
		{"tenant": String("a")},
		{"score": Int(2)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "tenant_score", []IndexField{
		{Field: "tenant", Order: 1}, {Field: "score", Order: -1},
	}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyV2File(context.Background(), path)
	if err != nil || !report.Verified || !report.IndexContentsVerified || !report.IndexBuildContentsVerified || report.SchemaVersion != 3 {
		t.Fatalf("compound verification=%+v err=%v", report, err)
	}
}

func TestVerifyV2FileAuditsCaughtUpCompoundIndexBuild(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify-index-build.meld2")
	file, _, err := storagev2.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ids := [][16]byte{{1}, {2}, {3}}
	documents := []Document{
		{"_id": ID(DocumentID(ids[0])), "tenant": String("a"), "score": Int(3)},
		{"_id": ID(DocumentID(ids[1])), "tenant": String("a")},
		{"_id": ID(DocumentID(ids[2])), "tenant": String("b"), "score": Int(1)},
	}
	encoded := make([][]byte, len(documents))
	for index := range documents {
		encoded[index], err = encodeStoredDocument(documents[index])
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := file.ApplyDocumentTransaction(storagev2.DocumentTransaction{
		TransactionID: [16]byte{1}, Mutations: []storagev2.DocumentMutation{
			{Collection: "items", DocumentID: ids[0], Operation: storagev2.DocumentInsert, Document: encoded[0]},
			{Collection: "items", DocumentID: ids[1], Operation: storagev2.DocumentInsert, Document: encoded[1]},
		},
	}); err != nil {
		t.Fatal(err)
	}
	fields := []IndexField{{Field: "tenant", Order: 1}, {Field: "score", Order: -1}}
	definition := newIndexDefinition("tenant_score", fields, false)
	keys := make([][]byte, len(documents))
	for index := range documents {
		keys[index], _, err = projectedIndexBuildKey(encoded[index], definition, DocumentID(ids[index]))
		if err != nil {
			t.Fatal(err)
		}
	}
	buildID := [16]byte{9}
	if _, err := file.BeginIndexBuild(storagev2.BeginIndexBuildTransaction{
		BuildID: buildID, Collection: "items", Name: definition.Name,
		Fields: []storagev2.IndexField{{Path: "tenant", Direction: 1}, {Path: "score", Direction: -1}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyIndexBuildScanBatch(storagev2.IndexBuildScanBatch{
		BuildID: buildID, ScanAfter: ids[0], Entries: []storagev2.IndexEntry{{Key: keys[0], DocumentID: ids[0]}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(storagev2.DocumentTransaction{
		TransactionID: [16]byte{2}, Mutations: []storagev2.DocumentMutation{{
			Collection: "items", DocumentID: ids[2], Operation: storagev2.DocumentInsert, Document: encoded[2],
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyIndexBuildScanBatch(storagev2.IndexBuildScanBatch{
		BuildID: buildID, ExpectedScanAfter: ids[0], ScanAfter: ids[1], Complete: true,
		Entries: []storagev2.IndexEntry{{Key: keys[1], DocumentID: ids[1]}},
	}); err != nil {
		t.Fatal(err)
	}
	caughtUp, err := file.ApplyIndexBuildCatchUpBatch(storagev2.IndexBuildCatchUpBatch{
		BuildID: buildID, ExpectedAppliedSequence: 1, ThroughSequence: 2,
		Mutations: []storagev2.IndexBuildCatchUpMutation{{
			Sequence: 2, DocumentID: ids[2], Operation: storagev2.CommitInsert, AfterKey: keys[2],
		}},
	})
	if err != nil || caughtUp.AppliedCatalogRoot < 2 || file.Meta().RequiredFeatures&storagev2.RequiredFeatureIndexBuildAppliedRoot == 0 {
		t.Fatalf("caught-up build=%+v meta=%+v err=%v", caughtUp, file.Meta(), err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyV2File(context.Background(), path)
	if err != nil || report.SchemaVersion != 3 || !report.IndexContentsVerified || !report.IndexBuildContentsVerified {
		t.Fatalf("index-build verification=%+v err=%v", report, err)
	}
}

func TestVerifyV2FileTreatsReadyUniqueConflictAsValidPrivateState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify-private-unique-conflict.meld2")
	db, err := OpenV2(path)
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
	report, err := VerifyV2File(context.Background(), path)
	if err != nil || !report.IndexBuildContentsVerified {
		t.Fatalf("private unique-conflict verification=%+v err=%v", report, err)
	}
}

func TestVerifyV2FileRejectsUnsupportedAndCanceledInputs(t *testing.T) {
	directory := t.TempDir()
	v1Path := filepath.Join(directory, "legacy.meld")
	v1, err := OpenV1(v1Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyV2File(context.Background(), v1Path); !errors.Is(err, ErrVerificationUnsupported) {
		t.Fatalf("V1 verification error=%v", err)
	}
	if _, err := VerifyV2File(context.Background(), filepath.Join(directory, "missing")); !errors.Is(err, ErrVerificationUnsupported) {
		t.Fatalf("missing verification error=%v", err)
	}

	v2Path := filepath.Join(directory, "cancel.meld2")
	v2, err := OpenV2(v2Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := v2.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := VerifyV2File(ctx, v2Path); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled verification error=%v", err)
	}
}
