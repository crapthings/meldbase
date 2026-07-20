package storage

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type businessGoldenFixture struct {
	FormatRevision     uint16 `json:"formatRevision"`
	DatabaseIDHex      string `json:"databaseIdHex"`
	CommitSequence     uint64 `json:"commitSequence"`
	MetaGeneration     uint64 `json:"metaGeneration"`
	UncompressedBytes  int    `json:"uncompressedBytes"`
	UncompressedSHA256 string `json:"uncompressedSha256"`
	GzipSHA256         string `json:"gzipSha256"`
	GzipBase64         string `json:"gzipBase64"`
}

var businessFixtureDatabaseID = [16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}

func TestBusinessRevision3GoldenFixtureOpensAuditsAndAdvances(t *testing.T) {
	fixture := loadBusinessGoldenFixture(t)
	if fixture.FormatRevision != FormatVersion {
		t.Fatalf("fixture revision=%d implementation=%d; add a new fixture instead of rewriting release history", fixture.FormatRevision, FormatVersion)
	}
	compressed, err := base64.StdEncoding.DecodeString(fixture.GzipBase64)
	if err != nil {
		t.Fatal(err)
	}
	if digestHex(compressed) != fixture.GzipSHA256 {
		t.Fatal("compressed business fixture digest mismatch")
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
	assertBusinessFixturePageFamilies(t, raw)
	path := filepath.Join(t.TempDir(), "business-revision-3.meld2")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	file, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CommitSequence != fixture.CommitSequence || meta.Generation != fixture.MetaGeneration ||
		hex.EncodeToString(meta.DatabaseID[:]) != fixture.DatabaseIDHex {
		t.Fatalf("fixture meta=%+v", meta)
	}
	assertBusinessFixture(t, file)
	if _, err := file.Reachability(); err != nil {
		t.Fatal(err)
	}
	first := fixtureDocumentID(1)
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: fixtureTransactionID(100), CommittedAt: time.Unix(1_700_000_100, 0).UTC(),
		Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: first, Operation: DocumentUpdate, Document: []byte("alpha-after-fixture"),
			ChangedPaths: []string{"value"}, Indexes: []IndexMutation{{Name: "by_value", BeforeKey: []byte("f"), AfterKey: []byte("z")}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if meta.CommitSequence != fixture.CommitSequence+1 || meta.Generation != fixture.MetaGeneration+1 {
		t.Fatalf("advanced meta=%+v", meta)
	}
	value, exists, err := reopened.GetDocument("items", first)
	if err != nil || !exists || string(value) != "alpha-after-fixture" {
		t.Fatalf("advanced value=%q exists=%t err=%v", value, exists, err)
	}
	if _, err := reopened.Reachability(); err != nil {
		t.Fatal(err)
	}
}

func TestBusinessRevision3WriterMatchesGoldenFixture(t *testing.T) {
	fixture := loadBusinessGoldenFixture(t)
	raw, meta := buildDeterministicBusinessFixture(t)
	if len(raw) != fixture.UncompressedBytes || digestHex(raw) != fixture.UncompressedSHA256 ||
		meta.CommitSequence != fixture.CommitSequence || meta.Generation != fixture.MetaGeneration ||
		hex.EncodeToString(meta.DatabaseID[:]) != fixture.DatabaseIDHex {
		t.Fatalf("writer output bytes=%d digest=%s meta=%+v", len(raw), digestHex(raw), meta)
	}
}

func TestGenerateBusinessRevision3GoldenFixture(t *testing.T) {
	if os.Getenv("MELDBASE_GENERATE_BUSINESS_FIXTURE") != "1" {
		t.Skip("fixture generation is an explicit maintainer action")
	}
	raw, meta := buildDeterministicBusinessFixture(t)
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

func buildDeterministicBusinessFixture(t *testing.T) ([]byte, Meta) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "build-business-revision-3.meld2")
	file, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	meta.DatabaseID = businessFixtureDatabaseID
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
	first, second := fixtureDocumentID(1), fixtureDocumentID(2)
	largeDocument := bytes.Repeat([]byte{'L'}, inlineDocumentLimit+257)
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: fixtureTransactionID(1), CommittedAt: time.Unix(1_700_000_001, 0).UTC(),
		Mutations: []DocumentMutation{
			{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("alpha-v1")},
			{Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: largeDocument},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: fixtureTransactionID(2), CommittedAt: time.Unix(1_700_000_002, 0).UTC(),
		Collection: "items", Name: "by_value", FieldPath: "value", Unique: true,
		Entries: []IndexEntry{{Key: []byte("a"), DocumentID: first}, {Key: []byte("b"), DocumentID: second}},
	}); err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 10)
	for index := range paths {
		paths[index] = fmt.Sprintf("field-%02d-%s", index, strings.Repeat(string(rune('a'+index)), 990))
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: fixtureTransactionID(3), CommittedAt: time.Unix(1_700_000_003, 0).UTC(),
		Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: first, Operation: DocumentUpdate, Document: []byte("alpha-v2"),
			ChangedPaths: paths, Indexes: []IndexMutation{{Name: "by_value", BeforeKey: []byte("a"), AfterKey: []byte("c")}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	largeSystemValue := bytes.Repeat([]byte{'S'}, inlineSystemValueLimit+333)
	if _, err := file.ApplyDocumentSystemTransaction(DocumentSystemTransaction{
		DocumentTransaction: DocumentTransaction{
			TransactionID: fixtureTransactionID(4), CommittedAt: time.Unix(1_700_000_004, 0).UTC(),
			Mutations: []DocumentMutation{{
				Collection: "items", DocumentID: second, Operation: DocumentUpdate,
				Document: bytes.Repeat([]byte{'M'}, inlineDocumentLimit+513), ChangedPaths: []string{"payload"},
				Indexes: []IndexMutation{{Name: "by_value", BeforeKey: []byte("b"), AfterKey: []byte("d")}},
			}},
		},
		SystemRecords: []SystemRecordMutation{{Key: []byte("rpc/result/fixture"), NewValue: largeSystemValue, Unconditional: true}},
	}); err != nil {
		t.Fatal(err)
	}
	for ordinal, key := range []string{"e", "g", "f"} {
		before := []string{"c", "e", "g"}[ordinal]
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
			TransactionID: fixtureTransactionID(uint64(5 + ordinal)), CommittedAt: time.Unix(1_700_000_005+int64(ordinal), 0).UTC(),
			Mutations: []DocumentMutation{{
				Collection: "items", DocumentID: first, Operation: DocumentUpdate, Document: []byte("alpha-" + key),
				ChangedPaths: []string{"value"}, Indexes: []IndexMutation{{Name: "by_value", BeforeKey: []byte(before), AfterKey: []byte(key)}},
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if stats, err := file.ReclaimPages(); err != nil || stats.ReclaimablePages == 0 {
		t.Fatalf("fixture reclaim=%+v err=%v", stats, err)
	}
	if err := file.PersistFreeSpace(); err != nil {
		t.Fatal(err)
	}
	meta = file.Meta()
	assertBusinessFixture(t, file)
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

func assertBusinessFixture(t *testing.T, file *File) {
	t.Helper()
	first, second := fixtureDocumentID(1), fixtureDocumentID(2)
	value, exists, err := file.GetDocument("items", first)
	if err != nil || !exists || string(value) != "alpha-f" {
		t.Fatalf("first value=%q exists=%t err=%v", value, exists, err)
	}
	value, exists, err = file.GetDocument("items", second)
	if err != nil || !exists || len(value) != inlineDocumentLimit+513 || value[0] != 'M' {
		t.Fatalf("second bytes=%d exists=%t err=%v", len(value), exists, err)
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	entries, scanErr := snapshot.ScanIndex("items", "by_value", nil, nil, 0)
	closeErr := snapshot.Close()
	if scanErr != nil || closeErr != nil || len(entries) != 2 || string(entries[0].Key) != "d" || entries[0].DocumentID != second || string(entries[1].Key) != "f" || entries[1].DocumentID != first {
		t.Fatalf("index=%+v scanErr=%v closeErr=%v", entries, scanErr, closeErr)
	}
	system, exists, err := file.GetSystemRecord([]byte("rpc/result/fixture"))
	if err != nil || !exists || len(system) != inlineSystemValueLimit+333 || system[0] != 'S' {
		t.Fatalf("system bytes=%d exists=%t err=%v", len(system), exists, err)
	}
	if stats := file.StorageStats(); !stats.PersistentFreeSpace || stats.ReusablePages == 0 {
		t.Fatalf("fixture storage=%+v", stats)
	}
}

func assertBusinessFixturePageFamilies(t *testing.T, raw []byte) {
	t.Helper()
	if len(raw)%PageSize != 0 || len(raw) < 2*PageSize {
		t.Fatalf("fixture file bytes=%d", len(raw))
	}
	found := make(map[PageType]bool)
	for pageID := uint64(2); pageID < uint64(len(raw)/PageSize); pageID++ {
		page, err := DecodePage(raw[pageID*PageSize:(pageID+1)*PageSize], pageID)
		if err != nil {
			t.Fatalf("fixture page %d: %v", pageID, err)
		}
		found[page.Type] = true
	}
	for _, pageType := range []PageType{
		PageDatabaseRoot, PageCatalogLeaf, PagePrimaryLeaf, PageOrderLeaf,
		PageIndexCatalogLeaf, PageSecondaryLeaf, PageCommitLogLeaf,
		PageDocumentOverflow, PageCommitOverflow, PageSystemLeaf,
		PageSystemOverflow, PageFreeSpaceLeaf,
	} {
		if !found[pageType] {
			t.Fatalf("fixture does not cover page type %d", pageType)
		}
	}
}

func loadBusinessGoldenFixture(t *testing.T) businessGoldenFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "business-revision-3.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture businessGoldenFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func digestHex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func fixtureDocumentID(ordinal byte) [16]byte {
	return [16]byte{0xf1, 15: ordinal}
}

func fixtureTransactionID(ordinal uint64) [16]byte {
	var id [16]byte
	id[0] = 0xf2
	for index := 0; index < 8; index++ {
		id[15-index] = byte(ordinal >> (index * 8))
	}
	return id
}
