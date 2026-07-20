package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateCollectionPublishesEmptyTreesCatalogAndCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collection.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := file.ApplyCreateCollection(CreateCollectionTransaction{
		TransactionID: randomTransactionID(t), CommittedAt: time.Unix(123, 45), Collection: "empty_items",
	})
	if err != nil || sequence != 1 {
		t.Fatalf("sequence=%d err=%v", sequence, err)
	}
	root, err := file.DatabaseRoot()
	if err != nil || root.CommitSequence != 1 || root.CollectionCount != 1 || root.DocumentCount != 0 || root.CatalogGeneration != 1 {
		t.Fatalf("root=%+v err=%v", root, err)
	}
	snapshot, stream, err := file.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	meta, exists, err := snapshot.CollectionMeta("empty_items")
	if err != nil || !exists || meta.ID != 1 || meta.PrimaryRoot < 2 || meta.IndexCatalogRoot < 2 ||
		meta.DocumentCount != 0 || meta.NextDocumentPosition != 0 || meta.CreatedSequence != 1 || meta.UpdatedSequence != 1 {
		t.Fatalf("meta=%+v exists=%t err=%v", meta, exists, err)
	}
	indexes, err := snapshot.Indexes("empty_items")
	if err != nil || len(indexes) != 0 {
		t.Fatalf("indexes=%+v err=%v", indexes, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	batch, err := file.ReadCommit(root.CommitLogRoot, 1)
	if err != nil || !batch.CommittedAt.Equal(time.Unix(123, 45)) || len(batch.Changes) != 1 ||
		batch.Changes[0].CollectionName != "empty_items" || batch.Changes[0].Operation != CommitCatalog ||
		len(batch.Changes[0].ChangedPaths) != 1 || batch.Changes[0].ChangedPaths[0] != "_catalog" {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	if _, err := file.ApplyCreateCollection(CreateCollectionTransaction{
		TransactionID: randomTransactionID(t), Collection: "empty_items",
	}); !errors.Is(err, ErrCollectionExists) || file.Meta().CommitSequence != 1 {
		t.Fatalf("duplicate err=%v sequence=%d", err, file.Meta().CommitSequence)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, recoveredStream, err := reopened.OpenSnapshotAndStream()
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	defer recoveredStream.Close()
	meta, exists, err = recovered.CollectionMeta("empty_items")
	if err != nil || !exists || meta.PrimaryRoot < 2 || meta.IndexCatalogRoot < 2 {
		t.Fatalf("recovered meta=%+v exists=%t err=%v", meta, exists, err)
	}
}

func TestCreateCollectionCrashPublishesOldOrCompleteNewGeneration(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "base.meld2")
	base, _, err := Open(basePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	points := []faultPoint{faultAfterPageWrite, faultBeforeDataSync, faultAfterDataSync, faultAfterMetaWrite, faultAfterMetaSync}
	for _, point := range points {
		t.Run(fmt.Sprint(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.meld2")
			if err := os.WriteFile(path, baseBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			candidate, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected create collection crash")
			candidate.fault = func(current faultPoint) error {
				if current == point {
					return injected
				}
				return nil
			}
			if _, err := candidate.ApplyCreateCollection(CreateCollectionTransaction{
				TransactionID: randomTransactionID(t), Collection: "items",
			}); !errors.Is(err, injected) {
				t.Fatalf("commit error=%v", err)
			}
			if err := candidate.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			root, err := reopened.DatabaseRoot()
			if err != nil || (root.CommitSequence != 0 && root.CommitSequence != 1) {
				t.Fatalf("root=%+v err=%v", root, err)
			}
			snapshot, stream, err := reopened.OpenSnapshotAndStream()
			if err != nil {
				t.Fatal(err)
			}
			meta, exists, err := snapshot.CollectionMeta("items")
			_ = snapshot.Close()
			_ = stream.Close()
			if err != nil || exists != (root.CommitSequence == 1) {
				t.Fatalf("meta=%+v exists=%t root=%+v err=%v", meta, exists, root, err)
			}
			if exists && (root.CollectionCount != 1 || root.CatalogGeneration != 1 || meta.PrimaryRoot < 2 || meta.IndexCatalogRoot < 2) {
				t.Fatalf("partial collection root=%+v meta=%+v", root, meta)
			}
		})
	}
}
