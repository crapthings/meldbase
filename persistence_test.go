package meldbase

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	storagefile "github.com/crapthings/meldbase/internal/storage"
)

func TestWALCrashBoundaryMatrixRecoversOnlyCompleteCommitPrefixes(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "source.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	pageBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for rank := int64(1); rank <= 3; rank++ {
		if _, err := db.Collection("items").InsertOne(context.Background(), Document{"rank": Int(rank)}); err != nil {
			t.Fatal(err)
		}
	}
	walBytes, err := os.ReadFile(path + ".wal")
	if err != nil {
		t.Fatal(err)
	}
	crashClose(t, db)

	type crashPoint struct{ cut, commits int }
	points := map[int]int{0: 0}
	for offset, commits := 0, 0; offset < len(walBytes); commits++ {
		if len(walBytes)-offset < 64 {
			t.Fatal("test WAL contains partial source record")
		}
		length := int(binary.LittleEndian.Uint32(walBytes[offset+12 : offset+16]))
		end := offset + 64 + length
		for _, cut := range []int{offset, offset + 1, offset + 8, offset + 24, offset + 56, offset + 63, offset + 64, end - 1} {
			if cut >= offset && cut < end {
				points[cut] = commits
			}
		}
		points[end] = commits + 1
		offset = end
	}
	ordered := make([]crashPoint, 0, len(points))
	for cut, commits := range points {
		ordered = append(ordered, crashPoint{cut: cut, commits: commits})
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].cut < ordered[j].cut })
	for _, point := range ordered {
		t.Run(fmt.Sprintf("cut-%d", point.cut), func(t *testing.T) {
			candidate := filepath.Join(directory, fmt.Sprintf("candidate-%d.meld", point.cut))
			if err := os.WriteFile(candidate, pageBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(candidate+".wal", walBytes[:point.cut], 0o600); err != nil {
				t.Fatal(err)
			}
			recovered, err := Open(candidate)
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			cursor, err := recovered.Collection("items").Find(context.Background(), Filter{}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: 1}}})
			if err != nil {
				t.Fatal(err)
			}
			documents, err := cursor.All(context.Background())
			if err != nil || len(documents) != point.commits {
				t.Fatalf("documents=%d commits=%d err=%v", len(documents), point.commits, err)
			}
			for index, document := range documents {
				rank, _ := document["rank"].Int64()
				if rank != int64(index+1) {
					t.Fatalf("rank[%d]=%d", index, rank)
				}
			}
		})
	}
}

func TestOpenCloseCheckpointAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	users := db.Collection("users")
	id, err := users.InsertOne(context.Background(), Document{"name": String("Ada"), "count": Int(1), "when": Time(time.Date(2026, 7, 15, 1, 2, 3, 456_789_000, time.UTC)), "bytes": Binary([]byte{0, 255})})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := users.UpdateOne(context.Background(), Filter{"_id": id}, Update{"$inc": map[string]any{"count": int64(2)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("other").InsertOne(context.Background(), Document{"value": Bool(true)}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path + ".wal"); err != nil || info.Size() != 0 {
		t.Fatalf("WAL was not checkpointed: info=%v err=%v", info, err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	found, err := reopened.Collection("users").FindOne(context.Background(), Filter{"_id": id.String()})
	if err != nil {
		t.Fatal(err)
	}
	if count, _ := found["count"].Int64(); count != 3 {
		t.Fatalf("count = %d", count)
	}
	when, _ := found["when"].TimeValue()
	if when.Nanosecond() != 456_000_000 {
		t.Fatalf("time precision = %s", when)
	}
	bytes, _ := found["bytes"].BinaryValue()
	if len(bytes) != 2 || bytes[1] != 255 {
		t.Fatalf("bytes = %v", bytes)
	}
}

func TestCheckpointRoundTripsEmptyAndMultiPageDocuments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "document-sizes.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	emptyID, err := db.Collection("items").InsertOne(context.Background(), Document{})
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x5a}, 1<<20)
	largeID, err := db.Collection("items").InsertOne(context.Background(), Document{"payload": Binary(payload)})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	empty, err := reopened.Collection("items").FindOne(context.Background(), Filter{"_id": emptyID})
	if err != nil || len(empty) != 1 {
		t.Fatalf("empty document fields=%d err=%v", len(empty), err)
	}
	large, err := reopened.Collection("items").FindOne(context.Background(), Filter{"_id": largeID})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := large["payload"].BinaryValue()
	if !ok || !bytes.Equal(got, payload) {
		t.Fatalf("large payload bytes=%d ok=%v", len(got), ok)
	}
}

func TestCheckpointSeparatesCatalogDocumentsAndIndexBlobs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "structured.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	items := db.Collection("items")
	documents := make([]Document, 200)
	for number := range documents {
		documents[number] = Document{"n": Int(int64(number))}
	}
	if _, err := items.InsertMany(context.Background(), documents); err != nil {
		t.Fatal(err)
	}
	if err := items.CreateIndex(context.Background(), "items_n", []IndexField{{Field: "n", Order: 1}}, IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	pages, blobs, _, err := storagefile.OpenBlobs(path)
	if err != nil {
		t.Fatal(err)
	}
	documentBlobs, indexNodeBlobs := 0, 0
	for index, blob := range blobs {
		switch blob.Kind {
		case checkpointCatalogBlob:
			if index != 0 {
				t.Fatalf("catalog blob index = %d", index)
			}
		case checkpointDocumentBlob:
			documentBlobs++
		case checkpointIndexNodeBlob:
			if blob.Class != storagefile.BlobClassIndex {
				t.Fatalf("index node blob class = %d", blob.Class)
			}
			indexNodeBlobs++
		default:
			t.Fatalf("unexpected checkpoint blob kind %d", blob.Kind)
		}
	}
	if documentBlobs != 200 || indexNodeBlobs < 2 {
		t.Fatalf("document blobs=%d index node blobs=%d", documentBlobs, indexNodeBlobs)
	}
	if err := pages.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	explain, err := reopened.Collection("items").Explain(context.Background(), Filter{"n": int64(129)})
	if err != nil || explain.Stage != "IXSCAN" {
		t.Fatalf("reopened explain=%+v err=%v", explain, err)
	}
	if !reopened.CanResumeFrom(0) || !reopened.CanResumeFrom(1) || !reopened.CanResumeFrom(2) {
		t.Fatal("checkpoint did not restore the retained commit window")
	}
}

func TestOpenMigratesLegacyMonolithicSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-snapshot.meld")
	id := mustID(t)
	collections := map[string]*collectionData{
		"items": {documents: map[DocumentID]Document{id: {"_id": ID(id), "name": String("legacy")}}, order: []DocumentID{id}, indexes: map[string]*indexState{}},
	}
	legacy, err := encodeSnapshot(collections)
	if err != nil {
		t.Fatal(err)
	}
	pages, _, _, err := storagefile.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pages.Checkpoint(3, legacy); err != nil {
		t.Fatal(err)
	}
	if err := pages.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	found, err := db.Collection("items").FindOne(context.Background(), Filter{"_id": id})
	if err != nil {
		t.Fatal(err)
	}
	if name, _ := found["name"].StringValue(); name != "legacy" {
		t.Fatalf("name = %q", name)
	}
}

func TestWALRecoversCommittedInsertUpdateDeleteWithoutCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	collection := db.Collection("items")
	keep, err := collection.InsertOne(context.Background(), Document{"name": String("keep"), "n": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	remove, err := collection.InsertOne(context.Background(), Document{"name": String("remove")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.UpdateOne(context.Background(), Filter{"_id": keep}, Update{"$set": map[string]any{"n": int64(2)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.DeleteOne(context.Background(), Filter{"_id": remove}); err != nil {
		t.Fatal(err)
	}
	crashClose(t, db)
	recovered, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	found, err := recovered.Collection("items").FindOne(context.Background(), Filter{"_id": keep})
	if err != nil {
		t.Fatal(err)
	}
	n, _ := found["n"].Int64()
	if n != 2 {
		t.Fatalf("n = %d", n)
	}
	if _, err := recovered.Collection("items").FindOne(context.Background(), Filter{"_id": remove}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted document error = %v", err)
	}
	if recovered.token != 4 {
		t.Fatalf("recovered token = %d", recovered.token)
	}
}

func TestRecoveryDiscardsPartialWALTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id, err := db.Collection("items").InsertOne(context.Background(), Document{"safe": Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	crashClose(t, db)
	file, err := os.OpenFile(path+".wal", os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("MELD")); err != nil {
		t.Fatal(err)
	}
	file.Close()
	recovered, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if _, err := recovered.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRejectsConcurrentWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.meld")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := Open(path); err == nil {
		second.Close()
		t.Fatal("second writer opened locked database")
	}
}

func TestBinaryCodecRejectsTrailingAndUnsafeData(t *testing.T) {
	document := Document{"_id": ID(mustID(t)), "nested": Object(Document{"ok": Bool(true)}), "array": Array(Int(1), String("x"))}
	encoded, err := encodeDocumentBinary(document)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeDocumentBinary(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !document.Equal(decoded) {
		t.Fatal("binary round trip differs")
	}
	if _, err := decodeDocumentBinary(append(encoded, 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing error = %v", err)
	}
}

func TestDurabilityFailurePoisonsFurtherWritesButPreservesReadsAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failure.meld")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	collection := db.Collection("items")
	committed, err := collection.InsertOne(context.Background(), Document{"value": String("safe")})
	if err != nil {
		t.Fatal(err)
	}
	// Closing the WAL underneath the engine deterministically simulates a
	// durability-layer failure without relying on filesystem fault injection.
	if err := db.store.log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.InsertOne(context.Background(), Document{"value": String("unsafe")}); !errors.Is(err, ErrDurability) {
		t.Fatalf("first durability error = %v", err)
	}
	if _, err := collection.InsertOne(context.Background(), Document{"value": String("later")}); !errors.Is(err, ErrDurability) {
		t.Fatalf("poisoned write error = %v", err)
	}
	if _, err := collection.FindOne(context.Background(), Filter{"_id": committed}); err != nil {
		t.Fatalf("read after failure = %v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrDurability) {
		t.Fatalf("close error = %v", err)
	}
	recovered, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	cursor, _ := recovered.Collection("items").Find(context.Background(), Filter{})
	documents, _ := cursor.All(context.Background())
	if len(documents) != 1 {
		t.Fatalf("recovered documents = %d", len(documents))
	}
}

func crashClose(t *testing.T, db *DB) {
	t.Helper()
	db.mu.Lock()
	db.closed = true
	err := db.store.close()
	db.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func mustID(t *testing.T) DocumentID {
	t.Helper()
	id, err := NewDocumentID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
