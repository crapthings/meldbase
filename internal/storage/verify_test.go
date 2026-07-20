package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyPathContextAuditsAndHashesWithoutTruncatingTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.CommitRoot(DatabaseRoot{CommitSequence: 1}); err != nil {
		t.Fatal(err)
	}
	meta := file.Meta()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	tail := []byte("unaligned-crash-tail")
	appendFile, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appendFile.Write(tail); err != nil {
		_ = appendFile.Close()
		t.Fatal(err)
	}
	if err := appendFile.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(before)
	result, err := VerifyPathContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if result.Meta != meta || result.FileBytes != uint64(len(before)) || result.TrailingBytes != uint64(len(tail)) ||
		result.PhysicalPages != uint64(len(before)/PageSize) || result.ReachablePages == 0 ||
		result.ValidMetaSlots != 2 || result.SHA256 != digest || !result.FreeSpaceValid {
		t.Fatalf("verification=%+v", result)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("verification mutated file: err=%v before=%d after=%d", err, len(before), len(after))
	}
}

func TestVerifyPathContextFailsClosedWithoutRepairingCorruptRoots(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.CommitRoot(DatabaseRoot{CommitSequence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := file.CommitRoot(DatabaseRoot{CommitSequence: 2}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for slot := range 2 {
		meta, err := DecodeMeta(raw[slot*PageSize : (slot+1)*PageSize])
		if err != nil || meta.RootPage < 2 {
			t.Fatalf("meta[%d]=%+v err=%v", slot, meta, err)
		}
		offset := int(meta.RootPage*PageSize) + PageHeaderSize
		raw[offset] ^= 0xff
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	before := append([]byte(nil), raw...)
	if _, err := VerifyPathContext(context.Background(), path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verification error=%v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("failed verification repaired bytes: err=%v", err)
	}
}

func TestVerifyPathContextHonorsCancellationAndWriterLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyPathContext(context.Background(), path); !errors.Is(err, ErrLocked) {
		t.Fatalf("writer lock verification error=%v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := VerifyPathContext(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled verification error=%v", err)
	}
}

func TestVerifyPathContextWithIndexAuditDetectsMissingSecondaryWithConsistentCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-secondary.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{1}
	document := []byte("value")
	indexKey := []byte("key")
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: [16]byte{1},
		Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: document,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: [16]byte{2}, Collection: "items", Name: "by_value", FieldPath: "value",
		Entries: []IndexEntry{{Key: indexKey, DocumentID: id}},
	}); err != nil {
		t.Fatal(err)
	}

	// Publish an internally self-consistent tree and EntryCount that silently
	// omit the sole Secondary entry. Structural verification cannot infer that
	// the canonical Primary document should still be indexed.
	file.mu.Lock()
	err = file.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		catalog, err := tx.OpenTree(base.CatalogRoot, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encodedCollection, exists, err := catalog.Get([]byte("items"))
		if err != nil || !exists {
			return DatabaseRoot{}, errors.Join(err, ErrCorrupt)
		}
		collection, err := decodeCollectionMeta(encodedCollection)
		if err != nil {
			return DatabaseRoot{}, err
		}
		indexCatalog, err := tx.OpenTree(collection.IndexCatalogRoot, TreeIndexCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encodedIndex, exists, err := indexCatalog.Get([]byte("by_value"))
		if err != nil || !exists {
			return DatabaseRoot{}, errors.Join(err, ErrCorrupt)
		}
		index, err := decodeIndexMeta("by_value", encodedIndex)
		if err != nil {
			return DatabaseRoot{}, err
		}
		secondary, err := tx.OpenTree(index.Root, TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		completeKey, err := secondaryKey(indexKey, 1, id)
		if err != nil {
			return DatabaseRoot{}, err
		}
		removed, err := secondary.Delete(completeKey)
		if err != nil || !removed || index.EntryCount != 1 {
			return DatabaseRoot{}, errors.Join(err, ErrCorrupt)
		}
		index.Root, err = secondary.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		index.EntryCount--
		encodedIndex, err = encodeIndexMeta(index)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := indexCatalog.Put([]byte(index.Name), encodedIndex); err != nil {
			return DatabaseRoot{}, err
		}
		collection.IndexCatalogRoot, err = indexCatalog.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		encodedCollection, err = encodeCollectionMeta(collection)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := catalog.Put([]byte("items"), encodedCollection); err != nil {
			return DatabaseRoot{}, err
		}
		base.CatalogRoot, err = catalog.Flush()
		return base, err
	})
	file.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := VerifyPathContext(context.Background(), path); err != nil {
		t.Fatalf("structural verification should accept consistent counts: %v", err)
	}
	audit := func(meta IndexMeta, gotID [16]byte, gotDocument []byte) ([]byte, bool, error) {
		if meta.Name != "by_value" || gotID != id || !bytes.Equal(gotDocument, document) {
			return nil, false, ErrCorrupt
		}
		return indexKey, true, nil
	}
	if _, err := VerifyPathContextWithIndexAudit(context.Background(), path, audit); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("semantic verification error=%v", err)
	}
}

func TestVerifyPathContextRejectsDuplicateKeysInUniqueIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "duplicate-unique.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ids := [][16]byte{{1}, {2}}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: [16]byte{1},
		Mutations: []DocumentMutation{
			{Collection: "items", DocumentID: ids[0], Operation: DocumentInsert, Document: []byte("one")},
			{Collection: "items", DocumentID: ids[1], Operation: DocumentInsert, Document: []byte("two")},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: [16]byte{2}, Collection: "items", Name: "by_value", FieldPath: "value",
		Entries: []IndexEntry{{Key: []byte("same"), DocumentID: ids[0]}, {Key: []byte("same"), DocumentID: ids[1]}},
	}); err != nil {
		t.Fatal(err)
	}
	file.mu.Lock()
	err = file.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		return rewriteVerificationIndexMeta(tx, tx.BaseRoot(), "items", "by_value", func(index *IndexMeta) error {
			index.Unique = true
			return nil
		})
	})
	file.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyPathContext(context.Background(), path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unique verification error=%v", err)
	}
}

func TestVerifyPathContextWithIndexAuditDetectsCountConsistentMissingShadowEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-shadow.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ids := [][16]byte{{1}, {2}}
	documents := [][]byte{[]byte("one"), []byte("two")}
	keys := [][]byte{[]byte("a"), []byte("b")}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: [16]byte{1}, Mutations: []DocumentMutation{
			{Collection: "items", DocumentID: ids[0], Operation: DocumentInsert, Document: documents[0]},
			{Collection: "items", DocumentID: ids[1], Operation: DocumentInsert, Document: documents[1]},
		},
	}); err != nil {
		t.Fatal(err)
	}
	buildID := [16]byte{9}
	if _, err := file.BeginIndexBuild(BeginIndexBuildTransaction{
		BuildID: buildID, Collection: "items", Name: "by_value", FieldPath: "value",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: buildID, ScanAfter: ids[1], Complete: true,
		Entries: []IndexEntry{{Key: keys[0], DocumentID: ids[0]}, {Key: keys[1], DocumentID: ids[1]}},
	}); err != nil {
		t.Fatal(err)
	}
	file.mu.Lock()
	err = file.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		return rewriteVerificationIndexBuild(tx, tx.BaseRoot(), buildID, func(index *IndexBuildMeta) error {
			shadow, err := tx.OpenTree(index.ShadowRoot, TreeSecondary)
			if err != nil {
				return err
			}
			complete, err := secondaryKey(keys[1], 2, ids[1])
			if err != nil {
				return err
			}
			removed, err := shadow.Delete(complete)
			if err != nil || !removed || index.EntryCount != 2 || index.CanonicalBytes < uint64(len(complete)) {
				return errors.Join(err, ErrCorrupt)
			}
			index.ShadowRoot, err = shadow.Flush()
			index.EntryCount--
			index.CanonicalBytes -= uint64(len(complete))
			return err
		})
	})
	file.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyPathContext(context.Background(), path); err != nil {
		t.Fatalf("structural verification should accept consistent shadow accounting: %v", err)
	}
	audit := func(_ IndexMeta, id [16]byte, document []byte) ([]byte, bool, error) {
		for index := range ids {
			if id == ids[index] && bytes.Equal(document, documents[index]) {
				return keys[index], true, nil
			}
		}
		return nil, false, ErrCorrupt
	}
	if _, err := VerifyPathContextWithIndexAudit(context.Background(), path, audit); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("semantic shadow verification error=%v", err)
	}
}

func TestVerifyPathContextAuditsEachPublishedAndShadowDocumentOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "single-pass-semantics.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ids := [][16]byte{{1}, {2}, {3}}
	documents := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	mutations := make([]DocumentMutation, len(ids))
	for index := range ids {
		mutations[index] = DocumentMutation{Collection: "items", DocumentID: ids[index], Operation: DocumentInsert, Document: documents[index]}
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: mutations}); err != nil {
		t.Fatal(err)
	}
	keysFor := func(prefix byte) []IndexEntry {
		entries := make([]IndexEntry, len(ids))
		for index := range ids {
			entries[index] = IndexEntry{Key: []byte{prefix, byte(index + 1)}, DocumentID: ids[index]}
		}
		return entries
	}
	for ordinal, definition := range []struct {
		name   string
		prefix byte
	}{{"by_a", 'a'}, {"by_b", 'b'}} {
		if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
			TransactionID: [16]byte{byte(ordinal + 2)}, Collection: "items", Name: definition.name,
			FieldPath: "value", Entries: keysFor(definition.prefix),
		}); err != nil {
			t.Fatal(err)
		}
	}
	buildID := [16]byte{9}
	if _, err := file.BeginIndexBuild(BeginIndexBuildTransaction{
		BuildID: buildID, Collection: "items", Name: "by_shadow", FieldPath: "value",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: buildID, ScanAfter: ids[len(ids)-1], Complete: true, Entries: keysFor('s'),
	}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	calls := make(map[string]map[[16]byte]int)
	audit := func(meta IndexMeta, id [16]byte, document []byte) ([]byte, bool, error) {
		prefixes := map[string]byte{"by_a": 'a', "by_b": 'b', "by_shadow": 's'}
		prefix, exists := prefixes[meta.Name]
		if !exists {
			return nil, false, ErrCorrupt
		}
		index := int(id[0]) - 1
		if index < 0 || index >= len(ids) || id != ids[index] || !bytes.Equal(document, documents[index]) {
			return nil, false, ErrCorrupt
		}
		if calls[meta.Name] == nil {
			calls[meta.Name] = make(map[[16]byte]int)
		}
		calls[meta.Name][id]++
		return []byte{prefix, byte(index + 1)}, true, nil
	}
	verified, err := VerifyPathContextWithIndexAudit(context.Background(), path, audit)
	if err != nil || !verified.SemanticIndexesVerified || !verified.SemanticIndexBuildsVerified {
		t.Fatalf("verification=%+v err=%v", verified, err)
	}
	// Both valid Meta roots are protected. The older fallback generation already
	// contains by_a, so it is independently audited once there and once in the
	// selected generation. Later by_b and by_shadow exist only in the latter.
	wantCalls := map[string]int{"by_a": 2, "by_b": 1, "by_shadow": 1}
	for name, wanted := range wantCalls {
		for _, id := range ids {
			if calls[name][id] != wanted {
				t.Fatalf("audit calls[%s][%x]=%d all=%v", name, id, calls[name][id], calls)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelCalls := 0
	if _, err := VerifyPathContextWithIndexAudit(ctx, path, func(meta IndexMeta, id [16]byte, document []byte) ([]byte, bool, error) {
		cancelCalls++
		cancel()
		return audit(meta, id, document)
	}); !errors.Is(err, context.Canceled) || cancelCalls != 1 {
		t.Fatalf("mid-semantic cancellation calls=%d err=%v", cancelCalls, err)
	}
}

func TestVerifyPathContextReportsLegacyCaughtUpShadowAsUnproven(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-caught-up-shadow.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, second := [16]byte{1}, [16]byte{2}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("one"),
	}}}); err != nil {
		t.Fatal(err)
	}
	buildID := [16]byte{9}
	if _, err := file.BeginIndexBuild(BeginIndexBuildTransaction{
		BuildID: buildID, Collection: "items", Name: "by_value", FieldPath: "value",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: buildID, ScanAfter: first, Complete: true, Entries: []IndexEntry{{Key: []byte("a"), DocumentID: first}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{2}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: []byte("two"),
	}}}); err != nil {
		t.Fatal(err)
	}
	file.mu.Lock()
	err = file.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		return rewriteVerificationIndexBuild(tx, tx.BaseRoot(), buildID, func(build *IndexBuildMeta) error {
			shadow, err := tx.OpenTree(build.ShadowRoot, TreeSecondary)
			if err != nil {
				return err
			}
			complete, err := secondaryKey([]byte("b"), 2, second)
			if err != nil {
				return err
			}
			if err := shadow.Put(complete, []byte{0}); err != nil {
				return err
			}
			build.ShadowRoot, err = shadow.Flush()
			build.EntryCount++
			build.CanonicalBytes += uint64(len(complete))
			build.AppliedSequence = 2
			build.Phase = IndexBuildReady
			return err
		})
	})
	file.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if file.Meta().RequiredFeatures&RequiredFeatureIndexBuildAppliedRoot != 0 {
		t.Fatalf("legacy emulation unexpectedly negotiated applied-root feature: %+v", file.Meta())
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	audit := func(_ IndexMeta, id [16]byte, document []byte) ([]byte, bool, error) {
		switch {
		case id == first && bytes.Equal(document, []byte("one")):
			return []byte("a"), true, nil
		case id == second && bytes.Equal(document, []byte("two")):
			return []byte("b"), true, nil
		default:
			return nil, false, ErrCorrupt
		}
	}
	verified, err := VerifyPathContextWithIndexAudit(context.Background(), path, audit)
	if err != nil || verified.SemanticIndexBuildsVerified {
		t.Fatalf("legacy shadow verification=%+v err=%v", verified, err)
	}
}

func rewriteVerificationIndexMeta(tx *WriteTxn, base DatabaseRoot, collectionName, indexName string, mutate func(*IndexMeta) error) (DatabaseRoot, error) {
	catalog, err := tx.OpenTree(base.CatalogRoot, TreeCatalog)
	if err != nil {
		return DatabaseRoot{}, err
	}
	encodedCollection, exists, err := catalog.Get([]byte(collectionName))
	if err != nil || !exists {
		return DatabaseRoot{}, errors.Join(err, ErrCorrupt)
	}
	collection, err := decodeCollectionMeta(encodedCollection)
	if err != nil {
		return DatabaseRoot{}, err
	}
	indexCatalog, err := tx.OpenTree(collection.IndexCatalogRoot, TreeIndexCatalog)
	if err != nil {
		return DatabaseRoot{}, err
	}
	encodedIndex, exists, err := indexCatalog.Get([]byte(indexName))
	if err != nil || !exists {
		return DatabaseRoot{}, errors.Join(err, ErrCorrupt)
	}
	index, err := decodeIndexMeta(indexName, encodedIndex)
	if err != nil {
		return DatabaseRoot{}, err
	}
	if err := mutate(&index); err != nil {
		return DatabaseRoot{}, err
	}
	encodedIndex, err = encodeIndexMeta(index)
	if err != nil {
		return DatabaseRoot{}, err
	}
	if err := indexCatalog.Put([]byte(indexName), encodedIndex); err != nil {
		return DatabaseRoot{}, err
	}
	collection.IndexCatalogRoot, err = indexCatalog.Flush()
	if err != nil {
		return DatabaseRoot{}, err
	}
	encodedCollection, err = encodeCollectionMeta(collection)
	if err != nil {
		return DatabaseRoot{}, err
	}
	if err := catalog.Put([]byte(collectionName), encodedCollection); err != nil {
		return DatabaseRoot{}, err
	}
	base.CatalogRoot, err = catalog.Flush()
	return base, err
}

func rewriteVerificationIndexBuild(tx *WriteTxn, base DatabaseRoot, buildID [16]byte, mutate func(*IndexBuildMeta) error) (DatabaseRoot, error) {
	builds, err := tx.OpenTree(base.IndexBuildCatalogRoot, TreeIndexBuildCatalog)
	if err != nil {
		return DatabaseRoot{}, err
	}
	encoded, exists, err := builds.Get(buildID[:])
	if err != nil || !exists {
		return DatabaseRoot{}, errors.Join(err, ErrCorrupt)
	}
	build, err := decodeIndexBuildMeta(buildID[:], encoded)
	if err != nil {
		return DatabaseRoot{}, err
	}
	if err := mutate(&build); err != nil {
		return DatabaseRoot{}, err
	}
	encoded, err = encodeIndexBuildMeta(build)
	if err != nil {
		return DatabaseRoot{}, err
	}
	if err := builds.Put(buildID[:], encoded); err != nil {
		return DatabaseRoot{}, err
	}
	base.IndexBuildCatalogRoot, err = builds.Flush()
	tx.indexBuildCatalogChanged = true
	return base, err
}
