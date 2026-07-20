package meldbase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestBackupPublishesExactVerifiedIdentityAndHistoryPreservingCopy(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.meld2")
	destinationPath := filepath.Join(directory, "backup.meld2")
	db, err := Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
	for revision := int64(2); revision <= 12; revision++ {
		if _, err := items.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": revision}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ReclaimPages(context.Background()); err != nil {
		t.Fatal(err)
	}
	sourceBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	sourceDigest := sha256.Sum256(sourceBytes)
	sourceIdentity := db.DatabaseIdentity()
	sourceSequence := db.Stats().CommitSequence
	result, err := db.Backup(context.Background(), destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	if result.Bytes != uint64(len(sourceBytes)) || result.Pages*PageSize != uint64(len(sourceBytes)) ||
		result.CommitSequence != sourceSequence || result.DatabaseIDHex != hex.EncodeToString(sourceIdentity[:]) ||
		result.SHA256 != hex.EncodeToString(sourceDigest[:]) {
		t.Fatalf("backup result=%+v", result)
	}
	destinationBytes, err := os.ReadFile(destinationPath)
	if err != nil || !bytes.Equal(destinationBytes, sourceBytes) {
		t.Fatalf("physical backup differs: bytes=%d err=%v", len(destinationBytes), err)
	}
	if matches, err := filepath.Glob(filepath.Join(directory, ".backup.meld2.backup-*")); err != nil || len(matches) != 0 {
		t.Fatalf("temporary backups=%v err=%v", matches, err)
	}
	backup, err := Open(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	if backup.DatabaseIdentity() != sourceIdentity || backup.Stats().CommitSequence != sourceSequence {
		_ = backup.Close()
		t.Fatalf("backup identity/sequence=%x/%d", backup.DatabaseIdentity(), backup.Stats().CommitSequence)
	}
	document, err := backup.Collection("items").FindOne(context.Background(), Filter{"_id": id})
	if err != nil {
		_ = backup.Close()
		t.Fatal(err)
	}
	value, _ := document["value"].Int64()
	if value != 12 {
		_ = backup.Close()
		t.Fatalf("backup value=%d", value)
	}
	if explain, explainErr := backup.Collection("items").Explain(context.Background(), Filter{"group": "a", "value": int64(12)}); explainErr != nil || explain.Stage != "IXSCAN" || explain.IndexName != "group_value" {
		_ = backup.Close()
		t.Fatalf("backup compound explain=%+v err=%v", explain, explainErr)
	}
	if err := backup.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats().Backup; stats.Active != 0 || stats.Attempts != 1 || stats.Completed != 1 || stats.Failed != 0 || stats.LastBytes != result.Bytes || stats.LastDuration <= 0 {
		t.Fatalf("backup stats=%+v", stats)
	}

	// A backup is a restore artifact, not an independent branch. Advancing the
	// source after publication leaves the byte-identical backup at its captured
	// sequence.
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$set": map[string]any{"value": int64(13)}}); err != nil {
		t.Fatal(err)
	}
	unchanged, err := Open(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Stats().CommitSequence != sourceSequence {
		_ = unchanged.Close()
		t.Fatalf("backup sequence advanced to %d", unchanged.Stats().CommitSequence)
	}
	if err := unchanged.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBackupFailsClosedWithoutOverwrite(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.meld2")
	db, err := Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "owner")
	if err := os.WriteFile(destination, []byte("owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Backup(context.Background(), destination); !errors.Is(err, ErrBackupDestinationExists) {
		t.Fatalf("existing destination error=%v", err)
	}
	if content, err := os.ReadFile(destination); err != nil || string(content) != "owner" {
		t.Fatalf("destination=%q err=%v", content, err)
	}
	if _, err := db.Backup(context.Background(), sourcePath); !errors.Is(err, ErrBackupDestinationExists) {
		t.Fatalf("source destination error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.Backup(ctx, filepath.Join(directory, "cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled error=%v", err)
	}
	if stats := db.Stats().Backup; stats.Attempts != 2 || stats.Completed != 0 || stats.Failed != 2 || stats.Active != 0 {
		t.Fatalf("failed backup stats=%+v", stats)
	}
	memory := New()
	defer memory.Close()
	if _, err := memory.Backup(context.Background(), filepath.Join(directory, "memory")); !errors.Is(err, ErrBackupUnsupported) {
		t.Fatalf("memory backup error=%v", err)
	}
}

func TestBackupPreservesResumableShadowIndexBuild(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source-build.meld2")
	backupPath := filepath.Join(directory, "backup-build.meld2")
	db, err := Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	for value := int64(1); value <= 3; value++ {
		if _, err := items.InsertOne(context.Background(), Document{"value": Int(value)}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := items.StartIndexBuild(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true})
	if err != nil {
		t.Fatal(err)
	}
	before, err := db.IndexBuild(id)
	if err != nil || before.Phase != IndexBuildPhaseScan {
		t.Fatalf("source build=%+v err=%v", before, err)
	}
	if _, err := db.Backup(context.Background(), backupPath); err != nil {
		t.Fatal(err)
	}
	backup, err := Open(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	restored, err := backup.IndexBuild(id)
	if err != nil || !reflect.DeepEqual(restored, before) {
		t.Fatalf("restored build=%+v want=%+v err=%v", restored, before, err)
	}
	if stats := backup.Stats().IndexBuilds; stats.Persistent != 1 || stats.Scanning != 1 {
		t.Fatalf("restored build stats=%+v", stats)
	}
	if err := backup.ResumeIndexBuild(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	explain, err := backup.Collection("items").Explain(context.Background(), Filter{"value": int64(3)})
	if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "by_value" {
		t.Fatalf("restored explain=%+v err=%v", explain, err)
	}
	if source, err := db.IndexBuild(id); err != nil || source.Phase != IndexBuildPhaseScan {
		t.Fatalf("source build changed=%+v err=%v", source, err)
	}
}
