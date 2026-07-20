package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var indexBuildFixtureID = [16]byte{0xfb, 15: 1}

// This fixture extends, rather than rewrites, the original revision-3 business
// fixture. It pins the separately negotiated shadow-build graph introduced
// after the base alpha fixture was checked in.
func TestIndexBuildRevision3GoldenFixtureOpensAuditsAbortsAndAdvances(t *testing.T) {
	fixture := loadIndexBuildGoldenFixture(t)
	compressed, err := base64.StdEncoding.DecodeString(fixture.GzipBase64)
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
		t.Fatalf("raw fixture bytes=%d err=%v", len(raw), err)
	}
	assertIndexBuildFixturePageFamilies(t, raw)
	path := filepath.Join(t.TempDir(), "index-build-revision-3.meld2")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyPathContext(context.Background(), path)
	if err != nil || verified.Meta.RequiredFeatures&RequiredFeatureShadowIndexBuilds == 0 || verified.ReachablePages == 0 ||
		verified.SHA256 != sha256.Sum256(raw) {
		t.Fatalf("offline verification=%+v err=%v", verified, err)
	}
	file, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CommitSequence != fixture.CommitSequence || meta.Generation != fixture.MetaGeneration ||
		hex.EncodeToString(meta.DatabaseID[:]) != fixture.DatabaseIDHex ||
		meta.RequiredFeatures&RequiredFeatureShadowIndexBuilds == 0 {
		t.Fatalf("fixture meta=%+v", meta)
	}
	assertIndexBuildFixture(t, file)
	if _, err := file.Reachability(); err != nil {
		t.Fatal(err)
	}
	if err := file.AbortIndexBuild(indexBuildFixtureID); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := file.IndexBuild(indexBuildFixtureID); err != nil || exists {
		t.Fatalf("aborted build exists=%t err=%v", exists, err)
	}
	first := fixtureDocumentID(1)
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: fixtureTransactionID(200), CommittedAt: time.Unix(1_700_000_200, 0).UTC(),
		Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: first, Operation: DocumentUpdate, Document: []byte("after-index-build-fixture"),
			ChangedPaths: []string{"value"}, Indexes: []IndexMutation{{Name: "by_value", BeforeKey: []byte("f"), AfterKey: []byte("zz")}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, advancedMeta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if advancedMeta.CommitSequence != fixture.CommitSequence+1 || advancedMeta.Generation != fixture.MetaGeneration+2 ||
		advancedMeta.RequiredFeatures&RequiredFeatureShadowIndexBuilds == 0 {
		t.Fatalf("advanced meta=%+v", advancedMeta)
	}
	value, exists, err := reopened.GetDocument("items", first)
	if err != nil || !exists || string(value) != "after-index-build-fixture" {
		t.Fatalf("advanced value=%q exists=%t err=%v", value, exists, err)
	}
	if _, err := reopened.Reachability(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexBuildRevision3WriterMatchesGoldenFixture(t *testing.T) {
	fixture := loadIndexBuildGoldenFixture(t)
	raw, meta := buildDeterministicIndexBuildFixture(t)
	if fixture.FormatRevision != FormatVersion || len(raw) != fixture.UncompressedBytes ||
		digestHex(raw) != fixture.UncompressedSHA256 || meta.CommitSequence != fixture.CommitSequence ||
		meta.Generation != fixture.MetaGeneration || hex.EncodeToString(meta.DatabaseID[:]) != fixture.DatabaseIDHex {
		t.Fatalf("writer bytes=%d digest=%s meta=%+v fixture=%+v", len(raw), digestHex(raw), meta, fixture)
	}
}

func TestGenerateIndexBuildRevision3GoldenFixture(t *testing.T) {
	if os.Getenv("MELDBASE_GENERATE_INDEX_BUILD_FIXTURE") != "1" {
		t.Skip("fixture generation is an explicit maintainer action")
	}
	raw, meta := buildDeterministicIndexBuildFixture(t)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	fixture := businessGoldenFixture{
		FormatRevision: FormatVersion, DatabaseIDHex: hex.EncodeToString(meta.DatabaseID[:]),
		CommitSequence: meta.CommitSequence, MetaGeneration: meta.Generation,
		UncompressedBytes: len(raw), UncompressedSHA256: digestHex(raw), GzipSHA256: digestHex(compressed.Bytes()),
		GzipBase64: base64.StdEncoding.EncodeToString(compressed.Bytes()),
	}
	encoded, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(encoded))
}

func buildDeterministicIndexBuildFixture(t *testing.T) ([]byte, Meta) {
	t.Helper()
	base, _ := buildDeterministicBusinessFixture(t)
	path := filepath.Join(t.TempDir(), "build-index-build-revision-3.meld2")
	if err := os.WriteFile(path, base, 0o600); err != nil {
		t.Fatal(err)
	}
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Unix(1_700_000_150, 0).UTC()
	if _, err := file.BeginIndexBuild(BeginIndexBuildTransaction{
		BuildID: indexBuildFixtureID, Collection: "items", Name: "by_shadow", FieldPath: "shadow.value",
		Unique: true, CreatedAt: created,
	}); err != nil {
		t.Fatal(err)
	}
	first, second := fixtureDocumentID(1), fixtureDocumentID(2)
	ready, err := file.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: indexBuildFixtureID, ScanAfter: second, Complete: true, UpdatedAt: created.Add(time.Second),
		Entries: []IndexEntry{{Key: []byte("shadow-a"), DocumentID: first}, {Key: []byte("shadow-b"), DocumentID: second}},
	})
	if err != nil || ready.Phase != IndexBuildReady {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	failed, err := file.FailIndexBuild(FailIndexBuildTransaction{
		BuildID: indexBuildFixtureID, Failure: IndexBuildFailureInvalidIndex, UpdatedAt: created.Add(2 * time.Second),
	})
	if err != nil || failed.Phase != IndexBuildFailed {
		t.Fatalf("failed=%+v err=%v", failed, err)
	}
	meta := file.Meta()
	assertIndexBuildFixture(t, file)
	if _, err := file.Reachability(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw, meta
}

func assertIndexBuildFixture(t *testing.T, file *File) {
	t.Helper()
	build, exists, err := file.IndexBuild(indexBuildFixtureID)
	if err != nil || !exists || build.Collection != "items" || build.Name != "by_shadow" ||
		build.FieldPath != "shadow.value" || !build.Unique || build.Phase != IndexBuildFailed ||
		build.Failure != IndexBuildFailureInvalidIndex || build.EntryCount != 2 || build.CanonicalBytes == 0 {
		t.Fatalf("build=%+v exists=%t err=%v", build, exists, err)
	}
	entries, err := file.TreeScan(build.ShadowRoot, TreeSecondary, nil, nil, 0)
	if err != nil || len(entries) != 2 {
		t.Fatalf("shadow entries=%+v err=%v", entries, err)
	}
}

func assertIndexBuildFixturePageFamilies(t *testing.T, raw []byte) {
	t.Helper()
	assertBusinessFixturePageFamilies(t, raw)
	foundBuildLeaf := false
	for pageID := uint64(2); pageID < uint64(len(raw)/PageSize); pageID++ {
		page, err := DecodePage(raw[pageID*PageSize:(pageID+1)*PageSize], pageID)
		if err != nil {
			t.Fatalf("fixture page %d: %v", pageID, err)
		}
		foundBuildLeaf = foundBuildLeaf || page.Type == PageIndexBuildCatalogLeaf
	}
	if !foundBuildLeaf {
		t.Fatal("fixture does not cover IndexBuildCatalog leaf pages")
	}
}

func loadIndexBuildGoldenFixture(t *testing.T) businessGoldenFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "index-build-revision-3.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture businessGoldenFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}
