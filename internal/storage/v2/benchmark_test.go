package v2

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

var benchmarkScanBytes int

func BenchmarkTreeBuildOneThousand(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		tx := &WriteTxn{file: &File{nextPage: 2}, generation: 2, sequence: 1, nextPage: 2, byID: make(map[uint64][]byte)}
		tree, err := tx.OpenTree(0, TreePrimary)
		if err != nil {
			b.Fatal(err)
		}
		for index := 0; index < 1000; index++ {
			key := [16]byte{byte(index >> 8), byte(index)}
			if err := tree.Put(key[:], []byte("small-document-value")); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := tree.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDocumentPointReadTenThousand(b *testing.B) {
	path := filepath.Join(b.TempDir(), "point-read.meld2")
	file, _, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	mutations := make([]DocumentMutation, 10_000)
	for index := range mutations {
		mutations[index] = DocumentMutation{
			Collection: "items", DocumentID: benchmarkDocumentID(index), Operation: DocumentInsert,
			Document: []byte(fmt.Sprintf("document-%d", index)),
		}
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: mutations}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		id := benchmarkDocumentID(index % len(mutations))
		value, ok, err := file.GetDocument("items", id)
		if err != nil || !ok || len(value) == 0 {
			b.Fatalf("ok=%t len=%d err=%v", ok, len(value), err)
		}
	}
}

func BenchmarkCommitReplayOneChange(b *testing.B) {
	path := filepath.Join(b.TempDir(), "replay.meld2")
	file, _, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	id := [16]byte{1}
	for sequence := 1; sequence <= 100; sequence++ {
		operation := DocumentUpdate
		if sequence == 1 {
			operation = DocumentInsert
		}
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
			TransactionID: benchmarkDocumentID(sequence),
			Mutations:     []DocumentMutation{{Collection: "items", DocumentID: id, Operation: operation, Document: []byte{byte(sequence)}}},
		}); err != nil {
			b.Fatal(err)
		}
	}
	root, err := file.DatabaseRoot()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		batch, err := file.ReadCommit(root.CommitLogRoot, uint64(index%100+1))
		if err != nil || len(batch.Changes) == 0 {
			b.Fatalf("changes=%d err=%v", len(batch.Changes), err)
		}
	}
}

func BenchmarkHistoricalSnapshotOpen(b *testing.B) {
	file := benchmarkCommitHistory(b, 100)
	defer file.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		snapshot, stream, err := file.OpenSnapshotAndStreamAt(50)
		if err != nil {
			b.Fatal(err)
		}
		meta, exists, err := snapshot.CollectionMeta("items")
		if err != nil || !exists || meta.ID == 0 {
			b.Fatalf("meta=%+v exists=%t err=%v", meta, exists, err)
		}
		_ = snapshot.Close()
		_ = stream.Close()
	}
}

func BenchmarkLiveResolveOneChange(b *testing.B) {
	file := benchmarkCommitHistory(b, 100)
	defer file.Close()
	stream, err := file.OpenLiveCommitStream(99)
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()
	batch, err := stream.Next(context.Background())
	if err != nil || len(batch.Changes) != 1 {
		b.Fatalf("batch=%+v err=%v", batch, err)
	}
	change := batch.Changes[0]
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		resolved, err := stream.ResolveChange(change)
		if err != nil || len(resolved.Before) == 0 || len(resolved.After) == 0 {
			b.Fatalf("resolved=%+v err=%v", resolved, err)
		}
	}
}

func benchmarkCommitHistory(b *testing.B, count int) *File {
	b.Helper()
	path := filepath.Join(b.TempDir(), "history.meld2")
	file, _, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	id := [16]byte{1}
	for sequence := 1; sequence <= count; sequence++ {
		operation := DocumentUpdate
		if sequence == 1 {
			operation = DocumentInsert
		}
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
			TransactionID: benchmarkDocumentID(sequence),
			Mutations:     []DocumentMutation{{Collection: "items", DocumentID: id, Operation: operation, Document: []byte{byte(sequence)}}},
		}); err != nil {
			_ = file.Close()
			b.Fatal(err)
		}
	}
	return file
}

func BenchmarkTreeScanTenThousand(b *testing.B) {
	path := filepath.Join(b.TempDir(), "scan.meld2")
	file, _, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	var rootPage uint64
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		tree, err := tx.OpenTree(0, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		for index := 0; index < 10_000; index++ {
			id := benchmarkDocumentID(index)
			if err := tree.Put(id[:], []byte("small-document-value")); err != nil {
				return DatabaseRoot{}, err
			}
		}
		rootPage, err = tree.Flush()
		return DatabaseRoot{CommitSequence: tx.Sequence(), CatalogRoot: rootPage, DocumentCount: 10_000}, err
	}); err != nil {
		b.Fatal(err)
	}
	// Warm validated immutable pages before comparing result construction.
	if _, err := file.TreeScan(rootPage, TreePrimary, nil, nil, 0); err != nil {
		b.Fatal(err)
	}

	b.Run("Materialized", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			values, err := file.TreeScan(rootPage, TreePrimary, nil, nil, 0)
			if err != nil || len(values) != 10_000 {
				b.Fatalf("values=%d err=%v", len(values), err)
			}
			benchmarkScanBytes = len(values[len(values)-1].Value)
		}
	})
	b.Run("Streaming", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			iterator, err := newTreeIterator(file, rootPage, TreePrimary, nil, nil, 0)
			if err != nil {
				b.Fatal(err)
			}
			count, bytesRead := 0, 0
			for iterator.Next() {
				count++
				bytesRead += len(iterator.Key()) + len(iterator.Value())
			}
			if err := iterator.Err(); err != nil || count != 10_000 {
				b.Fatalf("count=%d err=%v", count, err)
			}
			benchmarkScanBytes = bytesRead
			_ = iterator.Close()
		}
	})
}

func benchmarkDocumentID(index int) [16]byte {
	index++
	return [16]byte{byte(index >> 24), byte(index >> 16), byte(index >> 8), byte(index)}
}
