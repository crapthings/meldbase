package database

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestImportPhysicalBackupVerifiesBeforeNoOverwritePublication(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.meld2")
	artifactPath := filepath.Join(directory, "artifact.meld2")
	destinationPath := filepath.Join(directory, "follower-bootstrap.meld2")
	source, err := Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	assertOwnerPrivateStorageArtifact(t, sourcePath)
	id, err := source.Collection("items").InsertOne(context.Background(), Document{"value": Int(7)})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Collection("items").CreateIndex(context.Background(), "by_value", []IndexField{{Field: "value", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	expected, err := source.Backup(context.Background(), artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	assertOwnerPrivateStorageArtifact(t, artifactPath)
	artifact, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := ImportPhysicalBackup(context.Background(), bytes.NewReader(artifact), destinationPath, expected, PhysicalBackupImportOptions{MaxBytes: expected.Bytes})
	if err != nil || imported != expected {
		t.Fatalf("imported=%+v expected=%+v err=%v", imported, expected, err)
	}
	assertOwnerPrivateStorageArtifact(t, destinationPath)
	destination, err := os.ReadFile(destinationPath)
	if err != nil || !bytes.Equal(destination, artifact) {
		t.Fatalf("imported bytes=%d err=%v", len(destination), err)
	}
	follower, err := OpenFollower(destinationPath, OpenOptions{RequireGraphAudit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	document, err := follower.DB().Collection("items").FindOne(context.Background(), Filter{"_id": id})
	if err != nil || !document["value"].Equal(Int(7)) {
		t.Fatalf("imported follower document=%v err=%v", document, err)
	}
}

func TestPhysicalBackupRestorePreservesIndexedQueryReplayAndRealtime(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.meld2")
	artifactPath := filepath.Join(directory, "artifact.meld2")
	restoredPath := filepath.Join(directory, "restored.meld2")
	source, err := Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	items := source.Collection("items")
	ids, err := items.InsertMany(context.Background(), []Document{
		{"state": String("open"), "rank": Int(3), "title": String("first")},
		{"state": String("held"), "rank": Int(4), "title": String("second")},
		{"state": String("done"), "rank": Int(5), "title": String("third")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "state_rank", []IndexField{{Field: "state", Order: 1}, {Field: "rank", Order: 1}}, IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	query, err := CompileQuery(Filter{"state": "open"}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	historyInitial, err := items.SnapshotQuery(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": ids[0]}, Update{"$set": map[string]any{"rank": int64(6)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := items.UpdateOne(context.Background(), Filter{"_id": ids[1]}, Update{"$set": map[string]any{"state": "open", "rank": int64(2)}}); err != nil {
		t.Fatal(err)
	}
	sourceSnapshot, err := items.SnapshotQuery(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := source.Backup(context.Background(), artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := ImportPhysicalBackup(context.Background(), bytes.NewReader(artifact), restoredPath, receipt, PhysicalBackupImportOptions{MaxBytes: receipt.Bytes})
	if err != nil || imported != receipt {
		t.Fatalf("imported=%+v receipt=%+v err=%v", imported, receipt, err)
	}
	restored, err := Open(restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	restoredItems := restored.Collection("items")
	restoredSnapshot, err := restoredItems.SnapshotQuery(context.Background(), query)
	if err != nil || restoredSnapshot.Token != sourceSnapshot.Token || !documentSlicesEqual(restoredSnapshot.Documents, sourceSnapshot.Documents) {
		t.Fatalf("restored snapshot=%+v source=%+v err=%v", restoredSnapshot, sourceSnapshot, err)
	}
	explain, err := restoredItems.ExplainWithOptions(context.Background(), Filter{"state": "open"}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: 1}}})
	if err != nil || explain.Stage != "IXSCAN" || explain.IndexName != "state_rank" {
		t.Fatalf("restored explain=%+v err=%v", explain, err)
	}

	replay, err := restored.OpenQueryReplay(context.Background(), "items", query, historyInitial.Token, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if replay.Initial.Token != historyInitial.Token || !documentSlicesEqual(replay.Initial.Documents, historyInitial.Documents) {
		t.Fatalf("replay initial=%+v history initial=%+v", replay.Initial, historyInitial)
	}
	replayed := replay.Initial
	for range 2 {
		delta := receiveQueryDelta(t, replay.Deltas)
		if delta.FromToken != replayed.Token || delta.Token <= replayed.Token {
			t.Fatalf("replay delta token chain=%d->%d previous=%d", delta.FromToken, delta.Token, replayed.Token)
		}
		replayed, err = ApplyQueryDelta(replayed, delta)
		if err != nil {
			t.Fatalf("apply replay delta=%v", err)
		}
	}
	if replayed.Token != sourceSnapshot.Token || !documentSlicesEqual(replayed.Documents, sourceSnapshot.Documents) {
		t.Fatalf("replayed=%+v source=%+v", replayed, sourceSnapshot)
	}

	live, err := restoredItems.SubscribeQueryDeltas(context.Background(), query, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if live.Initial.Token != sourceSnapshot.Token || !documentSlicesEqual(live.Initial.Documents, sourceSnapshot.Documents) {
		t.Fatalf("live initial=%+v source=%+v", live.Initial, sourceSnapshot)
	}
	if _, err := restoredItems.UpdateOne(context.Background(), Filter{"_id": ids[2]}, Update{"$set": map[string]any{"state": "open", "rank": int64(1)}}); err != nil {
		t.Fatal(err)
	}
	delta := receiveQueryDelta(t, live.Deltas)
	liveState, err := ApplyQueryDelta(live.Initial, delta)
	if err != nil {
		t.Fatal(err)
	}
	full, err := restoredItems.SnapshotQuery(context.Background(), query)
	if err != nil || liveState.Token != full.Token || !documentSlicesEqual(liveState.Documents, full.Documents) {
		t.Fatalf("live state=%+v full=%+v err=%v", liveState, full, err)
	}
	if gotIDs := queryIDs(t, restoredItems, Filter{"state": "open"}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: 1}}}); !reflect.DeepEqual(gotIDs, []DocumentID{ids[2], ids[1], ids[0]}) {
		t.Fatalf("restored indexed IDs=%v", gotIDs)
	}
}

func assertOwnerPrivateStorageArtifact(t *testing.T, path string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		return
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if permissions := info.Mode().Perm(); permissions&0o077 != 0 {
		t.Fatalf("storage artifact %s permissions=%#o, want owner-private", path, permissions)
	}
}

func TestImportPhysicalBackupRejectsBadStreamAndPreservesDestination(t *testing.T) {
	directory := t.TempDir()
	source, err := Open(filepath.Join(directory, "source.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	artifactPath := filepath.Join(directory, "artifact.meld2")
	expected, err := source.Backup(context.Background(), artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []struct {
		name     string
		reader   []byte
		receipt  BackupResult
		maxBytes uint64
	}{
		{name: "truncated", reader: artifact[:len(artifact)-1], receipt: expected, maxBytes: expected.Bytes},
		{name: "extra", reader: append(append([]byte(nil), artifact...), 1), receipt: expected, maxBytes: expected.Bytes},
		{name: "wrong-digest", reader: artifact, receipt: BackupResult{Bytes: expected.Bytes, Pages: expected.Pages, CommitSequence: expected.CommitSequence, MetaGeneration: expected.MetaGeneration, DatabaseIDHex: expected.DatabaseIDHex, SHA256: "0000000000000000000000000000000000000000000000000000000000000000"}, maxBytes: expected.Bytes},
		{name: "receiver-cap", reader: artifact, receipt: expected, maxBytes: expected.Bytes - PageSize},
	} {
		t.Run(candidate.name, func(t *testing.T) {
			destination := filepath.Join(directory, candidate.name+".meld2")
			if result, err := ImportPhysicalBackup(context.Background(), bytes.NewReader(candidate.reader), destination, candidate.receipt, PhysicalBackupImportOptions{MaxBytes: candidate.maxBytes}); result != (BackupResult{}) || err == nil {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed import published destination: %v", err)
			}
		})
	}
	destination := filepath.Join(directory, "existing.meld2")
	if err := os.WriteFile(destination, []byte("owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ImportPhysicalBackup(context.Background(), bytes.NewReader(artifact), destination, expected, PhysicalBackupImportOptions{MaxBytes: expected.Bytes}); !errors.Is(err, ErrBackupDestinationExists) {
		t.Fatalf("existing destination err=%v", err)
	}
	if contents, err := os.ReadFile(destination); err != nil || string(contents) != "owner" {
		t.Fatalf("existing destination=%q err=%v", contents, err)
	}
}
