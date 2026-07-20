package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var multilevelFixtureDatabaseID = [16]byte{0x6d, 0x65, 0x6c, 0x64, 0x62, 0x61, 0x73, 0x65, 0x2d, 0x6d, 0x75, 0x6c, 0x74, 0x69, 0x2d, 0x31}

type multilevelGoldenFixture struct {
	FormatRevision     uint16   `json:"formatRevision"`
	DatabaseIDHex      string   `json:"databaseIdHex"`
	CommitSequence     uint64   `json:"commitSequence"`
	MetaGeneration     uint64   `json:"metaGeneration"`
	UncompressedBytes  int      `json:"uncompressedBytes"`
	UncompressedSHA256 string   `json:"uncompressedSha256"`
	GzipSHA256         string   `json:"gzipSha256"`
	GzipBase64Chunks   []string `json:"gzipBase64Chunks"`
}

func TestBuildDeterministicMultilevelRevision3Graph(t *testing.T) {
	raw, meta := buildDeterministicMultilevelFixture(t)
	if meta.DatabaseID != multilevelFixtureDatabaseID || meta.CommitSequence != 1 || len(raw)%PageSize != 0 {
		t.Fatalf("meta=%+v bytes=%d", meta, len(raw))
	}
	found := make(map[PageType]bool)
	for pageID := uint64(2); pageID < uint64(len(raw)/PageSize); pageID++ {
		page, err := DecodePage(raw[pageID*PageSize:(pageID+1)*PageSize], pageID)
		if err != nil {
			t.Fatalf("page %d: %v", pageID, err)
		}
		found[page.Type] = true
	}
	for _, pageType := range []PageType{
		PageCatalogBranch, PagePrimaryBranch, PageSecondaryBranch, PageCommitLogBranch,
		PageFreeSpaceBranch, PageIndexCatalogBranch, PageOrderBranch, PageSystemBranch,
		PageIndexBuildCatalogBranch,
	} {
		if !found[pageType] {
			t.Fatalf("missing reachable branch page type %d", pageType)
		}
	}
	path := filepath.Join(t.TempDir(), "multilevel-revision-3.meld2")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyPathContextWithIndexAudit(context.Background(), path, multilevelFixtureIndexAudit)
	if err != nil || !verified.SemanticIndexesVerified || !verified.SemanticIndexBuildsVerified || !verified.FreeSpaceValid {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	file, opened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if opened != meta {
		t.Fatalf("opened=%+v want=%+v", opened, meta)
	}
	stats, err := file.Reachability()
	if err != nil || stats.ReachablePages == 0 || file.StorageStats().ReusablePages < 300 {
		t.Fatalf("reachability=%+v storage=%+v err=%v", stats, file.StorageStats(), err)
	}
}

func TestMultilevelRevision3GoldenFixtureOpensAuditsAndAdvances(t *testing.T) {
	fixture := loadMultilevelGoldenFixture(t)
	raw := decodeMultilevelGoldenFixture(t, fixture)
	path := filepath.Join(t.TempDir(), "multilevel-revision-3.meld2")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyPathContextWithIndexAudit(context.Background(), path, multilevelFixtureIndexAudit)
	if err != nil || !verified.SemanticIndexesVerified || !verified.SemanticIndexBuildsVerified || !verified.FreeSpaceValid {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	file, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CommitSequence != fixture.CommitSequence || meta.Generation != fixture.MetaGeneration ||
		hex.EncodeToString(meta.DatabaseID[:]) != fixture.DatabaseIDHex {
		t.Fatalf("meta=%+v", meta)
	}
	assertMultilevelFixture(t, file)
	first := multilevelFixtureDocumentID(1)
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: [16]byte{2}, CommittedAt: time.Unix(1_700_001_100, 0).UTC(),
		Mutations: []DocumentMutation{{
			Collection: "main", DocumentID: first, Operation: DocumentUpdate, Document: []byte("document-updated"),
			ChangedPaths: []string{"value"}, Indexes: []IndexMutation{{Name: "by_value", BeforeKey: []byte("value-0000"), AfterKey: []byte("value-updated")}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, advanced, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, exists, err := reopened.GetDocument("main", first)
	if err != nil || !exists || string(value) != "document-updated" || advanced.CommitSequence != fixture.CommitSequence+1 {
		t.Fatalf("updated=%q exists=%t meta=%+v err=%v", value, exists, advanced, err)
	}
	if _, err := reopened.Reachability(); err != nil {
		t.Fatal(err)
	}
	historical, stream, err := reopened.OpenSnapshotAndStreamAt(fixture.CommitSequence)
	if err != nil {
		t.Fatal(err)
	}
	original, exists, err := historical.GetDocument("main", first)
	if closeErr := historical.Close(); err == nil {
		err = closeErr
	}
	if closeErr := stream.Close(); err == nil {
		err = closeErr
	}
	if err != nil || !exists || string(original) != "document-0000" {
		t.Fatalf("historical=%q exists=%t err=%v", original, exists, err)
	}
}

func multilevelFixtureIndexAudit(meta IndexMeta, _ [16]byte, document []byte) ([]byte, bool, error) {
	if meta.Name != "by_value" {
		return nil, false, ErrCorrupt
	}
	var ordinal int
	if _, err := fmt.Sscanf(string(document), "document-%04d", &ordinal); err != nil || ordinal < 0 || ordinal >= 600 {
		return nil, false, ErrCorrupt
	}
	return []byte(fmt.Sprintf("value-%04d", ordinal)), true, nil
}

func TestMultilevelRevision3WriterMatchesGoldenFixture(t *testing.T) {
	want := loadMultilevelGoldenFixture(t)
	raw, meta := buildDeterministicMultilevelFixture(t)
	got := makeMultilevelGoldenFixture(t, raw, meta)
	if !reflect.DeepEqual(got, want) {
		gotChunks, wantChunks := got.GzipBase64Chunks, want.GzipBase64Chunks
		got.GzipBase64Chunks, want.GzipBase64Chunks = nil, nil
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fixture metadata differs: got=%+v want=%+v", got, want)
		}
		t.Fatalf("fixture compressed chunks differ: got=%d want=%d", len(gotChunks), len(wantChunks))
	}
}

func assertMultilevelFixture(t *testing.T, file *File) {
	t.Helper()
	root, err := file.DatabaseRoot()
	if err != nil {
		t.Fatal(err)
	}
	for name, reference := range map[string]struct {
		root uint64
		kind TreeKind
	}{
		"catalog": {root.CatalogRoot, TreeCatalog}, "commit": {root.CommitLogRoot, TreeCommitLog},
		"free": {root.FreeSpaceRoot, TreeFreeSpace}, "build": {root.IndexBuildCatalogRoot, TreeIndexBuildCatalog},
	} {
		assertFixtureBranchRoot(t, file, name, reference.root, reference.kind)
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	main, exists, err := snapshot.CollectionMeta("main")
	if err != nil || !exists {
		_ = snapshot.Close()
		t.Fatalf("main collection=%+v exists=%t err=%v", main, exists, err)
	}
	assertFixtureBranchRoot(t, file, "primary", main.PrimaryRoot, TreePrimary)
	assertFixtureBranchRoot(t, file, "order", main.OrderRoot, TreeOrder)
	index, exists, err := snapshot.IndexMeta("main", "by_value")
	if err != nil || !exists {
		_ = snapshot.Close()
		t.Fatalf("main index=%+v exists=%t err=%v", index, exists, err)
	}
	assertFixtureBranchRoot(t, file, "secondary", index.Root, TreeSecondary)
	wide, exists, err := snapshot.CollectionMeta("wide-indexes")
	closeErr := snapshot.Close()
	if err != nil || !exists {
		t.Fatalf("wide collection=%+v exists=%t err=%v", wide, exists, err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	assertFixtureBranchRoot(t, file, "index catalog", wide.IndexCatalogRoot, TreeIndexCatalog)
	encodedSystem, exists, err := file.TreeGet(root.CatalogRoot, TreeCatalog, systemCatalogKey)
	if err != nil || !exists {
		t.Fatalf("system directory exists=%t err=%v", exists, err)
	}
	system, err := decodeSystemDirectory(encodedSystem)
	if err != nil {
		t.Fatal(err)
	}
	assertFixtureBranchRoot(t, file, "system", system.Root, TreeSystem)
	first, last := multilevelFixtureDocumentID(1), multilevelFixtureDocumentID(600)
	for id, want := range map[[16]byte]string{first: "document-0000", last: "document-0599"} {
		value, exists, err := file.GetDocument("main", id)
		if err != nil || !exists || string(value) != want {
			t.Fatalf("document %x=%q exists=%t err=%v", id, value, exists, err)
		}
	}
	commit, err := file.ReadCommit(root.CommitLogRoot, 1)
	if err != nil || len(commit.Changes) != 400 || commit.CatalogRoot != root.CatalogRoot {
		t.Fatalf("commit changes=%d err=%v", len(commit.Changes), err)
	}
	systemValue, exists, err := file.GetSystemRecord([]byte("system/0399"))
	if err != nil || !exists || !bytes.HasPrefix(systemValue, []byte("system-value-0399-")) {
		t.Fatalf("system value=%q exists=%t err=%v", systemValue, exists, err)
	}
	builds, err := file.IndexBuilds()
	if err != nil || len(builds) != MaxConcurrentIndexBuilds {
		t.Fatalf("builds=%d err=%v", len(builds), err)
	}
	if stats, err := file.Reachability(); err != nil || stats.ReachablePages == 0 || file.StorageStats().ReusablePages < 300 {
		t.Fatalf("reachability=%+v storage=%+v err=%v", stats, file.StorageStats(), err)
	}
}

func assertFixtureBranchRoot(t *testing.T, file *File, name string, root uint64, kind TreeKind) {
	t.Helper()
	raw := make([]byte, PageSize)
	if root < 2 {
		t.Fatalf("%s root=%d", name, root)
	}
	if _, err := file.file.ReadAt(raw, int64(root)*PageSize); err != nil {
		t.Fatal(err)
	}
	page, err := DecodePage(raw, root)
	_, branchType := treePageTypes(kind)
	if err != nil || page.Type != branchType {
		t.Fatalf("%s page=%+v want=%d err=%v", name, page, branchType, err)
	}
}

func buildDeterministicMultilevelFixture(t *testing.T) ([]byte, Meta) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "build-multilevel-revision-3.meld2")
	file, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	meta.DatabaseID = multilevelFixtureDatabaseID
	encodedMeta, err := EncodeMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	rawFile, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rawFile.WriteAt(encodedMeta, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawFile.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := rawFile.Close(); err != nil {
		t.Fatal(err)
	}
	file, _, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	err = file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		emptyPrimary, err := buildFixtureTree(tx, TreePrimary, nil)
		if err != nil {
			return DatabaseRoot{}, err
		}
		emptyOrder, err := buildFixtureTree(tx, TreeOrder, nil)
		if err != nil {
			return DatabaseRoot{}, err
		}
		emptySecondary, err := buildFixtureTree(tx, TreeSecondary, nil)
		if err != nil {
			return DatabaseRoot{}, err
		}
		emptyIndexes, err := buildFixtureTree(tx, TreeIndexCatalog, nil)
		if err != nil {
			return DatabaseRoot{}, err
		}

		primaryEntries := make([]KeyValue, 0, 600)
		orderEntries := make([]KeyValue, 0, 600)
		secondaryEntries := make([]KeyValue, 0, 600)
		for ordinal := 0; ordinal < 600; ordinal++ {
			id := multilevelFixtureDocumentID(ordinal + 1)
			position := uint64(ordinal + 1)
			stored, err := tx.storeDocumentRecord(position, []byte(fmt.Sprintf("document-%04d", ordinal)))
			if err != nil {
				return DatabaseRoot{}, err
			}
			primaryEntries = append(primaryEntries, KeyValue{Key: id[:], Value: stored})
			orderEntries = append(orderEntries, KeyValue{Key: insertionPositionKey(position), Value: id[:]})
			key, err := secondaryKey([]byte(fmt.Sprintf("value-%04d", ordinal)), position, id)
			if err != nil {
				return DatabaseRoot{}, err
			}
			secondaryEntries = append(secondaryEntries, KeyValue{Key: key, Value: []byte{0}})
		}
		primaryRoot, err := buildFixtureTree(tx, TreePrimary, primaryEntries)
		if err != nil {
			return DatabaseRoot{}, err
		}
		orderRoot, err := buildFixtureTree(tx, TreeOrder, orderEntries)
		if err != nil {
			return DatabaseRoot{}, err
		}
		secondaryRoot, err := buildFixtureTree(tx, TreeSecondary, secondaryEntries)
		if err != nil {
			return DatabaseRoot{}, err
		}
		mainIndex, err := encodeIndexMeta(IndexMeta{
			Name: "by_value", FieldPath: "value", Root: secondaryRoot, EntryCount: 600,
			CreatedSequence: 1, UpdatedSequence: 1,
		})
		if err != nil {
			return DatabaseRoot{}, err
		}
		mainIndexes, err := buildFixtureTree(tx, TreeIndexCatalog, []KeyValue{{Key: []byte("by_value"), Value: mainIndex}})
		if err != nil {
			return DatabaseRoot{}, err
		}

		wideIndexes := make([]KeyValue, 0, 120)
		for ordinal := 0; ordinal < 120; ordinal++ {
			name := fmt.Sprintf("index-%03d", ordinal)
			field := fmt.Sprintf("field.%03d.%s", ordinal, bytes.Repeat([]byte{'x'}, 180))
			encoded, err := encodeIndexMeta(IndexMeta{
				Name: name, FieldPath: field, Root: emptySecondary, CreatedSequence: 1, UpdatedSequence: 1,
			})
			if err != nil {
				return DatabaseRoot{}, err
			}
			wideIndexes = append(wideIndexes, KeyValue{Key: []byte(name), Value: encoded})
		}
		wideIndexRoot, err := buildFixtureTree(tx, TreeIndexCatalog, wideIndexes)
		if err != nil {
			return DatabaseRoot{}, err
		}

		systemEntries := make([]KeyValue, 0, 400)
		for ordinal := 0; ordinal < 400; ordinal++ {
			stored, err := tx.storeSystemValue([]byte(fmt.Sprintf("system-value-%04d-%s", ordinal, bytes.Repeat([]byte{'s'}, 80))))
			if err != nil {
				return DatabaseRoot{}, err
			}
			systemEntries = append(systemEntries, KeyValue{Key: []byte(fmt.Sprintf("system/%04d", ordinal)), Value: stored})
		}
		systemRoot, err := buildFixtureTree(tx, TreeSystem, systemEntries)
		if err != nil {
			return DatabaseRoot{}, err
		}
		systemDirectory, err := encodeSystemDirectory(systemDirectory{Root: systemRoot, Count: uint64(len(systemEntries))})
		if err != nil {
			return DatabaseRoot{}, err
		}

		catalog, err := tx.OpenTree(0, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := catalog.Put(systemCatalogKey, systemDirectory); err != nil {
			return DatabaseRoot{}, err
		}
		mainCollection := CollectionMeta{
			ID: 1, PrimaryRoot: primaryRoot, OrderRoot: orderRoot, IndexCatalogRoot: mainIndexes,
			DocumentCount: 600, CreatedSequence: 1, UpdatedSequence: 1, NextDocumentPosition: 600,
		}
		wideCollection := CollectionMeta{
			ID: 2, PrimaryRoot: emptyPrimary, OrderRoot: emptyOrder, IndexCatalogRoot: wideIndexRoot,
			CreatedSequence: 1, UpdatedSequence: 1,
		}
		buildCollection := CollectionMeta{
			ID: 3, PrimaryRoot: emptyPrimary, OrderRoot: emptyOrder, IndexCatalogRoot: emptyIndexes,
			CreatedSequence: 1, UpdatedSequence: 1,
		}
		for _, entry := range []struct {
			name       string
			collection CollectionMeta
		}{
			{name: "main", collection: mainCollection},
			{name: "wide-indexes", collection: wideCollection},
			{name: "build-source", collection: buildCollection},
		} {
			encoded, err := encodeCollectionMeta(entry.collection)
			if err != nil {
				return DatabaseRoot{}, err
			}
			if err := catalog.Put([]byte(entry.name), encoded); err != nil {
				return DatabaseRoot{}, err
			}
		}
		const emptyCollections = 200
		for ordinal := 0; ordinal < emptyCollections; ordinal++ {
			meta := CollectionMeta{
				ID: uint32(4 + ordinal), PrimaryRoot: emptyPrimary, OrderRoot: emptyOrder, IndexCatalogRoot: emptyIndexes,
				CreatedSequence: 1, UpdatedSequence: 1,
			}
			encoded, err := encodeCollectionMeta(meta)
			if err != nil {
				return DatabaseRoot{}, err
			}
			if err := catalog.Put([]byte(fmt.Sprintf("collection-%03d", ordinal)), encoded); err != nil {
				return DatabaseRoot{}, err
			}
		}
		catalogRoot, err := catalog.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}

		changes := make([]CommitChange, 400)
		for ordinal := range changes {
			changes[ordinal] = CommitChange{
				CollectionID: uint32(1 + ordinal%203), CollectionName: fmt.Sprintf("change-%03d", ordinal),
				Operation: CommitCatalog, ChangedPaths: []string{"_catalog"}, After: []byte(fmt.Sprintf("catalog-%03d", ordinal)),
			}
		}
		commitLogRoot, err := tx.AppendCommit(0, CommitBatch{
			Sequence: 1, TransactionID: [16]byte{1}, CommittedAt: time.Unix(1_700_001_000, 0).UTC(),
			CatalogRoot: catalogRoot, Changes: changes,
		})
		if err != nil {
			return DatabaseRoot{}, err
		}

		buildEntries := make([]KeyValue, 0, MaxConcurrentIndexBuilds)
		for ordinal := 0; ordinal < MaxConcurrentIndexBuilds; ordinal++ {
			var buildID [16]byte
			binary.BigEndian.PutUint64(buildID[8:], uint64(ordinal+1))
			name := fmt.Sprintf("build-%02d", ordinal)
			field := fmt.Sprintf("shadow.%02d.%s", ordinal, bytes.Repeat([]byte{'p'}, 900))
			encoded, err := encodeIndexBuildMeta(IndexBuildMeta{
				BuildID: buildID, CollectionID: 3, Collection: "build-source", Name: name, FieldPath: field,
				Phase: IndexBuildScan, SourceSequence: 1, SourceCatalogRoot: catalogRoot, ShadowRoot: emptySecondary,
				AppliedSequence: 1, CreatedAt: time.Unix(1_700_001_001, 0).UTC(), UpdatedAt: time.Unix(1_700_001_001, 0).UTC(),
			})
			if err != nil {
				return DatabaseRoot{}, err
			}
			buildEntries = append(buildEntries, KeyValue{Key: buildID[:], Value: encoded})
		}
		buildRoot, err := buildFixtureTree(tx, TreeIndexBuildCatalog, buildEntries)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := tx.requireFeature(RequiredFeatureShadowIndexBuilds); err != nil {
			return DatabaseRoot{}, err
		}
		tx.indexBuildCatalogChanged = true

		emptyPayload, err := encodeTreeNode(&treeNode{leaf: true})
		if err != nil {
			return DatabaseRoot{}, err
		}
		free := make([]uint64, 0, 310)
		for ordinal := 0; ordinal < 620; ordinal++ {
			pageID, err := tx.appendPage(PageSecondaryLeaf, 0, 0, 0, emptyPayload)
			if err != nil {
				return DatabaseRoot{}, err
			}
			if ordinal%2 == 0 {
				free = append(free, pageID)
			}
		}
		tx.freePages = free
		tx.freeSpaceTracked = true
		tx.freeSpaceRebuild = true
		tx.metadataAllocation = true
		return DatabaseRoot{
			CommitSequence: 1, CatalogRoot: catalogRoot, CommitLogRoot: commitLogRoot,
			IndexBuildCatalogRoot: buildRoot, OldestRetainedSequence: 1,
			CatalogGeneration: 1, DocumentCount: 600, CollectionCount: 203,
		}, nil
	})
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	meta = file.Meta()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw, meta
}

func buildFixtureTree(tx *WriteTxn, kind TreeKind, entries []KeyValue) (uint64, error) {
	builder, err := tx.NewSortedTreeBuilder(kind)
	if err != nil {
		return 0, err
	}
	for _, entry := range entries {
		if err := builder.Add(entry.Key, entry.Value); err != nil {
			return 0, err
		}
	}
	return builder.Finish()
}

func multilevelFixtureDocumentID(ordinal int) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], uint64(ordinal))
	return id
}

func TestGenerateMultilevelRevision3Fixture(t *testing.T) {
	if os.Getenv("MELDBASE_GENERATE_MULTILEVEL_FIXTURE") != "1" {
		t.Skip("fixture generation is an explicit maintainer action")
	}
	raw, meta := buildDeterministicMultilevelFixture(t)
	fixture := makeMultilevelGoldenFixture(t, raw, meta)
	encoded, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(encoded))
}

func makeMultilevelGoldenFixture(t *testing.T, raw []byte, meta Meta) multilevelGoldenFixture {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(compressed.Bytes())
	chunks := make([]string, 0, (len(encoded)+4095)/4096)
	for len(encoded) > 0 {
		length := min(4096, len(encoded))
		chunks = append(chunks, encoded[:length])
		encoded = encoded[length:]
	}
	return multilevelGoldenFixture{
		FormatRevision: FormatVersion, DatabaseIDHex: hex.EncodeToString(meta.DatabaseID[:]),
		CommitSequence: meta.CommitSequence, MetaGeneration: meta.Generation,
		UncompressedBytes: len(raw), UncompressedSHA256: digestHex(raw), GzipSHA256: digestHex(compressed.Bytes()),
		GzipBase64Chunks: chunks,
	}
}

func decodeMultilevelGoldenFixture(t *testing.T, fixture multilevelGoldenFixture) []byte {
	t.Helper()
	if fixture.FormatRevision != FormatVersion || len(fixture.GzipBase64Chunks) == 0 {
		t.Fatalf("fixture metadata=%+v", fixture)
	}
	compressed, err := base64.StdEncoding.DecodeString(strings.Join(fixture.GzipBase64Chunks, ""))
	if err != nil || digestHex(compressed) != fixture.GzipSHA256 {
		t.Fatalf("compressed fixture err=%v", err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil || len(raw) != fixture.UncompressedBytes || digestHex(raw) != fixture.UncompressedSHA256 {
		t.Fatalf("fixture bytes=%d err=%v", len(raw), err)
	}
	return raw
}

func loadMultilevelGoldenFixture(t *testing.T) multilevelGoldenFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "multilevel-business-revision-3.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture multilevelGoldenFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}
