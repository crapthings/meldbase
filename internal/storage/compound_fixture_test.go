package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var compoundFixtureDatabaseID = [16]byte{0xc3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

// This additive revision-3 fixture pins the independently negotiated V3 tuple
// key metadata without rewriting the original  and shadow-build fixtures.
func TestCompoundIndexRevision3GoldenFixtureOpensAuditsAndAdvances(t *testing.T) {
	fixture := loadCompoundGoldenFixture(t)
	raw := decodeGoldenFixture(t, fixture)
	path := filepath.Join(t.TempDir(), "compound-index-revision-3.meld2")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyPathContext(context.Background(), path)
	if err != nil || verified.Meta.RequiredFeatures&RequiredFeatureCompoundIndexes == 0 {
		t.Fatalf("verification=%+v err=%v", verified, err)
	}
	file, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CommitSequence != fixture.CommitSequence || meta.Generation != fixture.MetaGeneration ||
		hex.EncodeToString(meta.DatabaseID[:]) != fixture.DatabaseIDHex {
		t.Fatalf("meta=%+v", meta)
	}
	assertCompoundFixture(t, file)
	first := fixtureDocumentID(1)
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: fixtureTransactionID(302), CommittedAt: time.Unix(1_700_000_302, 0).UTC(),
		Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: first, Operation: DocumentUpdate, Document: []byte("one-advanced"),
			Indexes: []IndexMutation{{Name: "workspace_score", BeforeKey: []byte("tuple-a"), AfterKey: []byte("tuple-c")}},
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
	if advanced.CommitSequence != fixture.CommitSequence+1 || advanced.RequiredFeatures&RequiredFeatureCompoundIndexes == 0 {
		t.Fatalf("advanced meta=%+v", advanced)
	}
	if _, err := reopened.Reachability(); err != nil {
		t.Fatal(err)
	}
}

func TestCompoundIndexRevision3WriterMatchesGoldenFixture(t *testing.T) {
	fixture := loadCompoundGoldenFixture(t)
	raw, meta := buildDeterministicCompoundFixture(t)
	if fixture.FormatRevision != FormatVersion || len(raw) != fixture.UncompressedBytes ||
		digestHex(raw) != fixture.UncompressedSHA256 || meta.CommitSequence != fixture.CommitSequence ||
		meta.Generation != fixture.MetaGeneration || hex.EncodeToString(meta.DatabaseID[:]) != fixture.DatabaseIDHex {
		t.Fatalf("writer bytes=%d digest=%s meta=%+v fixture=%+v", len(raw), digestHex(raw), meta, fixture)
	}
}

func TestGenerateCompoundIndexRevision3GoldenFixture(t *testing.T) {
	if os.Getenv("MELDBASE_GENERATE_COMPOUND_FIXTURE") != "1" {
		t.Skip("fixture generation is an explicit maintainer action")
	}
	raw, meta := buildDeterministicCompoundFixture(t)
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

func buildDeterministicCompoundFixture(t *testing.T) ([]byte, Meta) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "build-compound-index-revision-3.meld2")
	file, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	meta.DatabaseID = compoundFixtureDatabaseID
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
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: fixtureTransactionID(300), CommittedAt: time.Unix(1_700_000_300, 0).UTC(),
		Mutations: []DocumentMutation{
			{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("one")},
			{Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: []byte("two")},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyCreateIndex(CreateIndexTransaction{
		TransactionID: fixtureTransactionID(301), CommittedAt: time.Unix(1_700_000_301, 0).UTC(),
		Collection: "items", Name: "workspace_score", FieldPath: "workspace",
		Fields: []IndexField{{Path: "workspace", Direction: 1}, {Path: "score", Direction: -1}}, Unique: true,
		Entries: []IndexEntry{{Key: []byte("tuple-a"), DocumentID: first}, {Key: []byte("tuple-b"), DocumentID: second}},
	}); err != nil {
		t.Fatal(err)
	}
	meta = file.Meta()
	assertCompoundFixture(t, file)
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

func assertCompoundFixture(t *testing.T, file *File) {
	t.Helper()
	if file.Meta().RequiredFeatures&RequiredFeatureCompoundIndexes == 0 {
		t.Fatal("fixture lacks compound-index required feature")
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	meta, exists, metaErr := snapshot.IndexMeta("items", "workspace_score")
	entries, scanErr := snapshot.ScanIndex("items", "workspace_score", nil, nil, 0)
	closeErr := snapshot.Close()
	wantFields := []IndexField{{Path: "workspace", Direction: 1}, {Path: "score", Direction: -1}}
	if metaErr != nil || !exists || !reflect.DeepEqual(meta.Fields, wantFields) || meta.KeyCodecVersion != indexKeyCodecV3 ||
		scanErr != nil || closeErr != nil || len(entries) != 2 || string(entries[0].Key) != "tuple-a" || string(entries[1].Key) != "tuple-b" {
		t.Fatalf("meta=%+v exists=%t entries=%+v errors=%v/%v/%v", meta, exists, entries, metaErr, scanErr, closeErr)
	}
}

func loadCompoundGoldenFixture(t *testing.T) businessGoldenFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "compound-index-revision-3.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture businessGoldenFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func decodeGoldenFixture(t *testing.T, fixture businessGoldenFixture) []byte {
	t.Helper()
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
	return raw
}
