package v2

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type metaGoldenFixture struct {
	FormatRevision         uint16 `json:"formatRevision"`
	DatabaseIDHex          string `json:"databaseIdHex"`
	Generation             uint64 `json:"generation"`
	CommitSequence         uint64 `json:"commitSequence"`
	RootPage               uint64 `json:"rootPage"`
	PhysicalPageCount      uint64 `json:"physicalPageCount"`
	OldestRetainedSequence uint64 `json:"oldestRetainedSequence"`
	RequiredFeatures       uint64 `json:"requiredFeatures"`
	OptionalFeatures       uint64 `json:"optionalFeatures"`
	EncodedPrefixBase64    string `json:"encodedPrefixBase64"`
	MetaChecksumHex        string `json:"metaChecksumHex"`
	EncodedPageSHA256      string `json:"encodedPageSha256"`
}

func TestMetaRevision3GoldenFixture(t *testing.T) {
	fixture := loadMetaGoldenFixture(t)
	if fixture.FormatRevision != FormatVersion {
		t.Fatalf("fixture revision=%d implementation=%d; add a new fixture instead of rewriting release history", fixture.FormatRevision, FormatVersion)
	}
	databaseID, err := hex.DecodeString(fixture.DatabaseIDHex)
	if err != nil || len(databaseID) != 16 {
		t.Fatalf("database id fixture=%q err=%v", fixture.DatabaseIDHex, err)
	}
	meta := Meta{
		Generation: fixture.Generation, CommitSequence: fixture.CommitSequence,
		RootPage: fixture.RootPage, PhysicalPageCount: fixture.PhysicalPageCount,
		OldestRetainedSequence: fixture.OldestRetainedSequence,
		RequiredFeatures:       fixture.RequiredFeatures, OptionalFeatures: fixture.OptionalFeatures,
	}
	copy(meta.DatabaseID[:], databaseID)
	encoded, err := EncodeMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix, err := base64.StdEncoding.DecodeString(fixture.EncodedPrefixBase64)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded[:len(wantPrefix)], wantPrefix) {
		t.Fatal("revision-3 Meta field prefix differs from the checked-in release fixture")
	}
	if hex.EncodeToString(encoded[224:256]) != fixture.MetaChecksumHex {
		t.Fatalf("revision-3 Meta checksum=%x want=%s", encoded[224:256], fixture.MetaChecksumHex)
	}
	pageDigest := sha256.Sum256(encoded)
	if hex.EncodeToString(pageDigest[:]) != fixture.EncodedPageSHA256 {
		t.Fatalf("revision-3 Meta page digest=%x want=%s", pageDigest, fixture.EncodedPageSHA256)
	}
	decoded, err := DecodeMeta(encoded)
	if err != nil || decoded != meta {
		t.Fatalf("decoded=%+v want=%+v err=%v", decoded, meta, err)
	}
}

func TestOpenAndAdvanceRevision3GoldenFixture(t *testing.T) {
	fixture := loadMetaGoldenFixture(t)
	prefix, err := base64.StdEncoding.DecodeString(fixture.EncodedPrefixBase64)
	if err != nil {
		t.Fatal(err)
	}
	checksum, err := hex.DecodeString(fixture.MetaChecksumHex)
	if err != nil || len(checksum) != 32 {
		t.Fatalf("checksum fixture err=%v length=%d", err, len(checksum))
	}
	rawFile := make([]byte, 2*PageSize)
	copy(rawFile, prefix)
	copy(rawFile[224:256], checksum)
	path := filepath.Join(t.TempDir(), "revision-3-golden.meld2")
	if err := os.WriteFile(path, rawFile, 0o600); err != nil {
		t.Fatal(err)
	}

	file, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Generation != fixture.Generation || meta.CommitSequence != 0 || meta.PhysicalPageCount != 2 {
		t.Fatalf("golden open meta=%+v", meta)
	}
	id := [16]byte{15: 1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("advanced"),
	}}}); err != nil {
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
	if meta.Generation != fixture.Generation+1 || meta.CommitSequence != 1 {
		t.Fatalf("advanced meta=%+v", meta)
	}
	value, exists, err := reopened.GetDocument("items", id)
	if err != nil || !exists || string(value) != "advanced" {
		t.Fatalf("advanced value=%q exists=%t err=%v", value, exists, err)
	}
}

func loadMetaGoldenFixture(t *testing.T) metaGoldenFixture {
	t.Helper()
	var fixture metaGoldenFixture
	raw, err := os.ReadFile(filepath.Join("testdata", "meta-revision-3.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestMetaCodecRejectsCorruptionAndReservedBytes(t *testing.T) {
	meta := Meta{
		DatabaseID: [16]byte{1}, Generation: 4, CommitSequence: 3, RootPage: 9,
		PhysicalPageCount: 10, OldestRetainedSequence: 1, OptionalFeatures: OptionalFeaturePersistentFreeSpace,
	}
	encoded, err := EncodeMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeMeta(encoded)
	if err != nil || decoded != meta {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}

	for _, offset := range []int{0, 40, 100, 224, PageSize - 1} {
		corrupt := append([]byte(nil), encoded...)
		corrupt[offset] ^= 0xff
		if _, err := DecodeMeta(corrupt); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("offset %d corruption error = %v", offset, err)
		}
	}
	previousRevision := append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint16(previousRevision[8:10], 2)
	clear(previousRevision[224:256])
	checksum := metaChecksum(previousRevision)
	copy(previousRevision[224:256], checksum[:])
	if _, err := DecodeMeta(previousRevision); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("previous encoding revision error=%v", err)
	}
	envelope, err := InspectMetaEnvelope(previousRevision)
	if err != nil || envelope.Revision != 2 || envelope.Generation != meta.Generation || envelope.DatabaseID != meta.DatabaseID {
		t.Fatalf("previous envelope=%+v err=%v", envelope, err)
	}
	tornEnvelope := append([]byte(nil), previousRevision...)
	tornEnvelope[32] ^= 0x80
	if _, err := InspectMetaEnvelope(tornEnvelope); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("torn envelope error=%v", err)
	}

	unknownRequired := append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint64(unknownRequired[72:80], 1<<63)
	rewriteMetaChecksum(unknownRequired)
	if _, err := DecodeMeta(unknownRequired); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("unknown required feature error=%v", err)
	}
	unknownOptional := append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint64(unknownOptional[80:88], meta.OptionalFeatures|1<<63)
	rewriteMetaChecksum(unknownOptional)
	decoded, err = DecodeMeta(unknownOptional)
	if err != nil || decoded.OptionalFeatures != meta.OptionalFeatures|1<<63 {
		t.Fatalf("unknown optional feature decoded=%+v err=%v", decoded, err)
	}
	if _, err := EncodeMeta(Meta{DatabaseID: [16]byte{1}, Generation: 1, PhysicalPageCount: 2, RequiredFeatures: 1 << 63}); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("encoded unknown required feature error=%v", err)
	}
	supported := meta
	supported.RequiredFeatures = RequiredFeatureShadowIndexBuilds
	supportedEncoded, err := EncodeMeta(supported)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := DecodeMeta(supportedEncoded); err != nil || decoded.RequiredFeatures != RequiredFeatureShadowIndexBuilds {
		t.Fatalf("supported required feature decoded=%+v err=%v", decoded, err)
	}
}

func TestOpenNeverDowngradesAcrossValidUnsupportedMeta(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{name: "future-version", want: ErrUnsupportedFormat, mutate: func(page []byte) {
			binary.LittleEndian.PutUint16(page[8:10], FormatVersion+1)
			rewriteMetaChecksum(page)
		}},
		{name: "required-feature", want: ErrUnsupportedFeature, mutate: func(page []byte) {
			binary.LittleEndian.PutUint64(page[72:80], 1<<63)
			rewriteMetaChecksum(page)
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path, newestSlot := createTwoGenerationFormatFile(t)
			raw, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			page := make([]byte, PageSize)
			if _, err := raw.ReadAt(page, int64(newestSlot)*PageSize); err != nil {
				t.Fatal(err)
			}
			scenario.mutate(page)
			if _, err := raw.WriteAt(page, int64(newestSlot)*PageSize); err != nil {
				t.Fatal(err)
			}
			if err := raw.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := raw.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if opened, _, err := Open(path); !errors.Is(err, scenario.want) {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatalf("open error=%v want=%v", err, scenario.want)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, before) {
				t.Fatalf("unsupported open mutated file err=%v", err)
			}
		})
	}
}

func TestTornUnsupportedVersionFieldStillFallsBack(t *testing.T) {
	path, newestSlot := createTwoGenerationFormatFile(t)
	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var version [2]byte
	binary.LittleEndian.PutUint16(version[:], FormatVersion+1)
	if _, err := raw.WriteAt(version[:], int64(newestSlot)*PageSize+8); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if meta.CommitSequence != 1 {
		t.Fatalf("fallback sequence=%d want=1", meta.CommitSequence)
	}
	id := [16]byte{15: 1}
	value, exists, err := reopened.GetDocument("items", id)
	if err != nil || !exists || string(value) != "v1" {
		t.Fatalf("fallback value=%q exists=%t err=%v", value, exists, err)
	}
}

func TestUnknownOptionalMetaFeatureSurvivesCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optional-feature.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, PageSize)
	if _, err := raw.ReadAt(page, 0); err != nil {
		t.Fatal(err)
	}
	const futureOptional = uint64(1 << 63)
	binary.LittleEndian.PutUint64(page[80:88], futureOptional)
	rewriteMetaChecksum(page)
	if _, err := raw.WriteAt(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{15: 1}
	if _, err := reopened.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("value"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if got := reopened.Meta().OptionalFeatures; got&futureOptional == 0 {
		t.Fatalf("optional features=0x%x lost future bit", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func createTwoGenerationFormatFile(t *testing.T) (string, int) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "feature-negotiation.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{15: 1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("v1"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{2}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("v2"),
	}}}); err != nil {
		t.Fatal(err)
	}
	newestSlot := file.metaSlot
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path, newestSlot
}

func rewriteMetaChecksum(page []byte) {
	clear(page[224:256])
	checksum := metaChecksum(page)
	copy(page[224:256], checksum[:])
}

func TestPageAndDatabaseRootCodec(t *testing.T) {
	root := DatabaseRoot{CommitSequence: 7, OldestRetainedSequence: 3, CatalogGeneration: 2, DocumentCount: 11, CollectionCount: 2, IndexBuildCatalogRoot: 99}
	encoded, err := EncodeDatabaseRoot(42, 8, root)
	if err != nil {
		t.Fatal(err)
	}
	decoded, page, err := DecodeDatabaseRoot(encoded, 42)
	if err != nil || decoded != root || page.Generation != 8 || page.BornSequence != 7 {
		t.Fatalf("root=%+v page=%+v err=%v", decoded, page, err)
	}
	if binary.LittleEndian.Uint32(encoded[56:60]) == 0 {
		t.Fatal("page checksum was not encoded")
	}

	for _, offset := range []int{0, 32, 64, PageSize - 1} {
		corrupt := append([]byte(nil), encoded...)
		corrupt[offset] ^= 0x40
		if _, _, err := DecodeDatabaseRoot(corrupt, 42); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("offset %d corruption error = %v", offset, err)
		}
	}
	if _, err := DecodePage(encoded, 43); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("wrong page ID error = %v", err)
	}

	pageValue := Page{Type: PagePrimaryLeaf, ID: 3, Generation: 1, BornSequence: 1, ItemCount: 2, Payload: []byte("payload")}
	raw, err := EncodePage(pageValue)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePage(raw, 3)
	if err != nil || got.Type != pageValue.Type || got.ItemCount != 2 || !bytes.Equal(got.Payload, pageValue.Payload) {
		t.Fatalf("page=%+v err=%v", got, err)
	}
}
