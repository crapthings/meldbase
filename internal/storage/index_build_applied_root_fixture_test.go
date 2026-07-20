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
	"reflect"
	"strings"
	"testing"
	"time"
)

type pageDeltaGoldenFixture struct {
	FormatRevision         uint16   `json:"formatRevision"`
	BaseFixtureSHA256      string   `json:"baseFixtureSha256"`
	DatabaseIDHex          string   `json:"databaseIdHex"`
	CommitSequence         uint64   `json:"commitSequence"`
	MetaGeneration         uint64   `json:"metaGeneration"`
	FileBytes              int      `json:"fileBytes"`
	FileSHA256             string   `json:"fileSha256"`
	ChangedPageIDs         []uint64 `json:"changedPageIds"`
	PatchUncompressedBytes int      `json:"patchUncompressedBytes"`
	PatchSHA256            string   `json:"patchSha256"`
	PatchGzipSHA256        string   `json:"patchGzipSha256"`
	PatchGzipBase64        string   `json:"patchGzipBase64"`
}

var appliedRootFixtureBuildID = [16]byte{0xac, 15: 1}

func TestIndexBuildAppliedRootRevision3DeltaFixtureOpensAuditsPublishesAndAdvances(t *testing.T) {
	fixture := loadAppliedRootGoldenFixture(t)
	raw := reconstructAppliedRootGoldenFixture(t, fixture)
	path := filepath.Join(t.TempDir(), "index-build-applied-root-revision-3.meld2")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	audit := func(meta IndexMeta, id [16]byte, document []byte) ([]byte, bool, error) {
		first, second, third := fixtureDocumentID(1), fixtureDocumentID(2), fixtureDocumentID(3)
		switch meta.Name {
		case "by_value":
			switch {
			case id == first && bytes.Equal(document, []byte("alpha-v1")):
				return []byte("a"), true, nil
			case id == first && bytes.Equal(document, []byte("alpha-v2")):
				return []byte("c"), true, nil
			case id == first && bytes.Equal(document, []byte("alpha-e")):
				return []byte("e"), true, nil
			case id == first && bytes.Equal(document, []byte("alpha-g")):
				return []byte("g"), true, nil
			case id == first && bytes.Equal(document, []byte("alpha-f")):
				return []byte("f"), true, nil
			case id == second && len(document) == inlineDocumentLimit+257 && document[0] == 'L':
				return []byte("b"), true, nil
			case id == second && len(document) == inlineDocumentLimit+513 && document[0] == 'M':
				return []byte("d"), true, nil
			case id == third && bytes.Equal(document, []byte("gamma")):
				return []byte("h"), true, nil
			}
		case "by_shadow":
			switch id {
			case first:
				return []byte("sa"), true, nil
			case second:
				return []byte("sb"), true, nil
			case third:
				return []byte("sc"), true, nil
			}
		}
		return nil, false, ErrCorrupt
	}
	verified, err := VerifyPathContextWithIndexAudit(context.Background(), path, audit)
	if err != nil || !verified.SemanticIndexesVerified || !verified.SemanticIndexBuildsVerified ||
		verified.Meta.RequiredFeatures&RequiredFeatureIndexBuildAppliedRoot == 0 ||
		verified.SHA256 != sha256.Sum256(raw) {
		t.Fatalf("verification=%+v err=%v", verified, err)
	}
	file, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CommitSequence != fixture.CommitSequence || meta.Generation != fixture.MetaGeneration ||
		hex.EncodeToString(meta.DatabaseID[:]) != fixture.DatabaseIDHex {
		t.Fatalf("fixture meta=%+v", meta)
	}
	build, exists, err := file.IndexBuild(appliedRootFixtureBuildID)
	if err != nil || !exists || build.Phase != IndexBuildReady || build.SourceSequence+1 != build.AppliedSequence ||
		build.AppliedSequence != fixture.CommitSequence || build.AppliedCatalogRoot < 2 {
		t.Fatalf("build=%+v exists=%t err=%v", build, exists, err)
	}
	if _, err := file.Reachability(); err != nil {
		t.Fatal(err)
	}
	sequence, err := file.FinalizeIndexBuild(FinalizeIndexBuildTransaction{
		BuildID: appliedRootFixtureBuildID, TransactionID: fixtureTransactionID(250),
		ExpectedAppliedSequence: build.AppliedSequence, CommittedAt: time.Unix(1_700_000_250, 0).UTC(),
	})
	if err != nil || sequence != fixture.CommitSequence+1 {
		t.Fatalf("finalize sequence=%d err=%v", sequence, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, advanced, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if advanced.CommitSequence != fixture.CommitSequence+1 || advanced.RequiredFeatures&RequiredFeatureIndexBuildAppliedRoot == 0 {
		t.Fatalf("advanced meta=%+v", advanced)
	}
	snapshot, err := reopened.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	index, exists, indexErr := snapshot.IndexMeta("items", "by_shadow")
	closeErr := snapshot.Close()
	if indexErr != nil || closeErr != nil || !exists || index.Root != build.ShadowRoot || index.EntryCount != 3 {
		t.Fatalf("published index=%+v exists=%t indexErr=%v closeErr=%v", index, exists, indexErr, closeErr)
	}
}

func TestIndexBuildAppliedRootRevision3WriterMatchesDeltaFixture(t *testing.T) {
	fixture := loadAppliedRootGoldenFixture(t)
	base, target, meta := buildDeterministicAppliedRootFixture(t)
	generated := makePageDeltaGoldenFixture(t, base, target, meta)
	if generated.PatchGzipBase64 != fixture.PatchGzipBase64 {
		limit := min(len(generated.PatchGzipBase64), len(fixture.PatchGzipBase64))
		mismatch := limit
		for index := 0; index < limit; index++ {
			if generated.PatchGzipBase64[index] != fixture.PatchGzipBase64[index] {
				mismatch = index
				break
			}
		}
		resume := strings.Index(generated.PatchGzipBase64[mismatch:], fixture.PatchGzipBase64[mismatch:min(len(fixture.PatchGzipBase64), mismatch+48)])
		missing := ""
		if resume > 0 {
			missing = generated.PatchGzipBase64[mismatch : mismatch+resume]
		}
		t.Fatalf("patch base64 mismatch at %d resume=%d gotLen=%d wantLen=%d missing=%q got=%q want=%q", mismatch, resume,
			len(generated.PatchGzipBase64), len(fixture.PatchGzipBase64),
			missing,
			generated.PatchGzipBase64[max(0, mismatch-16):min(len(generated.PatchGzipBase64), mismatch+16)],
			fixture.PatchGzipBase64[max(0, mismatch-16):min(len(fixture.PatchGzipBase64), mismatch+16)])
	}
	generatedMetadata, fixtureMetadata := generated, fixture
	generatedMetadata.PatchGzipBase64, fixtureMetadata.PatchGzipBase64 = "", ""
	if !reflect.DeepEqual(generatedMetadata, fixtureMetadata) {
		t.Fatalf("generated fixture metadata differs:\n got=%+v\nwant=%+v", generatedMetadata, fixtureMetadata)
	}
}

func TestGenerateIndexBuildAppliedRootRevision3DeltaFixture(t *testing.T) {
	if os.Getenv("MELDBASE_GENERATE_APPLIED_ROOT_FIXTURE") != "1" {
		t.Skip("fixture generation is an explicit maintainer action")
	}
	base, target, meta := buildDeterministicAppliedRootFixture(t)
	encoded, err := json.MarshalIndent(makePageDeltaGoldenFixture(t, base, target, meta), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(encoded))
}

func buildDeterministicAppliedRootFixture(t *testing.T) ([]byte, []byte, Meta) {
	t.Helper()
	base, _ := buildDeterministicBusinessFixture(t)
	path := filepath.Join(t.TempDir(), "build-index-build-applied-root-revision-3.meld2")
	if err := os.WriteFile(path, base, 0o600); err != nil {
		t.Fatal(err)
	}
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Unix(1_700_000_220, 0).UTC()
	if _, err := file.BeginIndexBuild(BeginIndexBuildTransaction{
		BuildID: appliedRootFixtureBuildID, Collection: "items", Name: "by_shadow", FieldPath: "shadow", CreatedAt: created,
	}); err != nil {
		t.Fatal(err)
	}
	first, second, third := fixtureDocumentID(1), fixtureDocumentID(2), fixtureDocumentID(3)
	ready, err := file.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: appliedRootFixtureBuildID, ScanAfter: second, Complete: true, UpdatedAt: created.Add(time.Second),
		Entries: []IndexEntry{{Key: []byte("sa"), DocumentID: first}, {Key: []byte("sb"), DocumentID: second}},
	})
	if err != nil || ready.Phase != IndexBuildReady {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{
		TransactionID: fixtureTransactionID(221), CommittedAt: created.Add(2 * time.Second),
		Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: third, Operation: DocumentInsert, Document: []byte("gamma"),
			ChangedPaths: []string{"value", "shadow"}, Indexes: []IndexMutation{{Name: "by_value", AfterKey: []byte("h")}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	caughtUp, err := file.ApplyIndexBuildCatchUpBatch(IndexBuildCatchUpBatch{
		BuildID: appliedRootFixtureBuildID, ExpectedAppliedSequence: ready.AppliedSequence,
		ThroughSequence: ready.AppliedSequence + 1, UpdatedAt: created.Add(3 * time.Second),
		Mutations: []IndexBuildCatchUpMutation{{
			Sequence: ready.AppliedSequence + 1, DocumentID: third, Operation: CommitInsert, AfterKey: []byte("sc"),
		}},
	})
	if err != nil || caughtUp.Phase != IndexBuildReady || caughtUp.AppliedCatalogRoot < 2 ||
		file.Meta().RequiredFeatures&RequiredFeatureIndexBuildAppliedRoot == 0 {
		t.Fatalf("caughtUp=%+v meta=%+v err=%v", caughtUp, file.Meta(), err)
	}
	meta := file.Meta()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	target, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return base, target, meta
}

func makePageDeltaGoldenFixture(t *testing.T, base, target []byte, meta Meta) pageDeltaGoldenFixture {
	t.Helper()
	if len(base)%PageSize != 0 || len(target)%PageSize != 0 || len(target) < len(base) {
		t.Fatalf("invalid delta lengths base=%d target=%d", len(base), len(target))
	}
	changed := make([]uint64, 0)
	patch := make([]byte, 0)
	for pageID := 0; pageID < len(target)/PageSize; pageID++ {
		start := pageID * PageSize
		different := start >= len(base) || !bytes.Equal(base[start:start+PageSize], target[start:start+PageSize])
		if !different {
			continue
		}
		changed = append(changed, uint64(pageID))
		patch = append(patch, target[start:start+PageSize]...)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(patch); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return pageDeltaGoldenFixture{
		FormatRevision: FormatVersion, BaseFixtureSHA256: digestHex(base), DatabaseIDHex: hex.EncodeToString(meta.DatabaseID[:]),
		CommitSequence: meta.CommitSequence, MetaGeneration: meta.Generation, FileBytes: len(target), FileSHA256: digestHex(target),
		ChangedPageIDs: changed, PatchUncompressedBytes: len(patch), PatchSHA256: digestHex(patch),
		PatchGzipSHA256: digestHex(compressed.Bytes()), PatchGzipBase64: base64.StdEncoding.EncodeToString(compressed.Bytes()),
	}
}

func reconstructAppliedRootGoldenFixture(t *testing.T, fixture pageDeltaGoldenFixture) []byte {
	t.Helper()
	base := decodeBusinessGoldenFixtureBytes(t)
	if fixture.FormatRevision != FormatVersion || digestHex(base) != fixture.BaseFixtureSHA256 || fixture.FileBytes < len(base) ||
		fixture.FileBytes%PageSize != 0 || fixture.PatchUncompressedBytes != len(fixture.ChangedPageIDs)*PageSize {
		t.Fatalf("invalid applied-root fixture metadata: %+v", fixture)
	}
	compressed, err := base64.StdEncoding.DecodeString(fixture.PatchGzipBase64)
	if err != nil || digestHex(compressed) != fixture.PatchGzipSHA256 {
		t.Fatalf("compressed patch err=%v", err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	patch, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil || len(patch) != fixture.PatchUncompressedBytes || digestHex(patch) != fixture.PatchSHA256 {
		t.Fatalf("patch bytes=%d err=%v", len(patch), err)
	}
	result := make([]byte, fixture.FileBytes)
	copy(result, base)
	previous := uint64(0)
	for index, pageID := range fixture.ChangedPageIDs {
		if (index > 0 && pageID <= previous) || pageID >= uint64(len(result)/PageSize) {
			t.Fatalf("invalid changed page IDs: %v", fixture.ChangedPageIDs)
		}
		copy(result[pageID*PageSize:(pageID+1)*PageSize], patch[index*PageSize:(index+1)*PageSize])
		previous = pageID
	}
	if digestHex(result) != fixture.FileSHA256 {
		t.Fatal("reconstructed applied-root fixture digest mismatch")
	}
	return result
}

func decodeBusinessGoldenFixtureBytes(t *testing.T) []byte {
	t.Helper()
	fixture := loadBusinessGoldenFixture(t)
	compressed, err := base64.StdEncoding.DecodeString(fixture.GzipBase64)
	if err != nil || digestHex(compressed) != fixture.GzipSHA256 {
		t.Fatalf("business fixture compression err=%v", err)
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
		t.Fatalf("business fixture bytes=%d err=%v", len(raw), err)
	}
	return raw
}

func loadAppliedRootGoldenFixture(t *testing.T) pageDeltaGoldenFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "index-build-applied-root-revision-3.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture pageDeltaGoldenFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}
