package meldbase

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestBeginV2ArchivePinsTailBeforeVerifiedPhysicalSnapshot(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.meld2")
	archivePath := filepath.Join(directory, "archive.meld2")
	db, err := OpenV2(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bootstrap, subscription, err := db.BeginV2Archive(context.Background(), "nightly", archivePath, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if bootstrap.CheckpointToken != 1 || bootstrap.SnapshotToken != 1 || bootstrap.Backup.CommitSequence != 1 || bootstrap.Backup.DatabaseIDHex == "" {
		t.Fatalf("bootstrap=%+v", bootstrap)
	}
	backup, err := OpenV2(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if backup.Stats().CommitSequence != bootstrap.SnapshotToken || backup.databaseID != db.databaseID {
		_ = backup.Close()
		t.Fatalf("backup sequence=%d identity=%x source=%x", backup.Stats().CommitSequence, backup.databaseID, db.databaseID)
	}
	snapshot, err := backup.Collection("items").SnapshotQuery(context.Background(), QuerySpec{})
	if err != nil || snapshot.Token != bootstrap.SnapshotToken || len(snapshot.Documents) != 0 {
		_ = backup.Close()
		t.Fatalf("backup snapshot=%+v err=%v", snapshot, err)
	}
	if err := backup.Close(); err != nil {
		t.Fatal(err)
	}

	id, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	tail := receiveDurableDatabaseBatch(t, subscription)
	if tail.Token != bootstrap.SnapshotToken+1 || len(tail.Changes) != 2 || tail.Changes[0].Operation != CreateCollectionOperation ||
		tail.Changes[1].Operation != InsertOperation || tail.Changes[1].DocumentID != id {
		t.Fatalf("archive tail=%+v", tail)
	}
	if err := subscription.Ack(tail.Token); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteDurableDatabaseChanges(context.Background(), "nightly"); err != nil {
		t.Fatal(err)
	}
}

func TestBeginV2ArchiveRejectsExistingDestinationBeforeCreatingCheckpoint(t *testing.T) {
	directory := t.TempDir()
	db, err := OpenV2(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := filepath.Join(directory, "existing.meld2")
	if err := os.WriteFile(destination, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	if bootstrap, subscription, err := db.BeginV2Archive(context.Background(), "nightly", destination, 1); subscription != nil || !errors.Is(err, ErrBackupDestinationExists) || bootstrap != (ArchiveV2Bootstrap{}) {
		t.Fatalf("existing destination bootstrap=%+v subscription=%v err=%v", bootstrap, subscription, err)
	}
	if db.Stats().CommitSequence != 0 {
		t.Fatalf("failed archive advanced source sequence=%d", db.Stats().CommitSequence)
	}
	if subscription, err := db.OpenDurableDatabaseChanges(context.Background(), "nightly", 1); subscription != nil || !errors.Is(err, ErrDurableConsumerNotFound) {
		t.Fatalf("failed archive left checkpoint subscription=%v err=%v", subscription, err)
	}
}
