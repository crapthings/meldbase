package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type branchPageGolden struct {
	TreeKind TreeKind `json:"treeKind"`
	PageType PageType `json:"pageType"`
	PageID   uint64   `json:"pageId"`
	SHA256   string   `json:"sha256"`
}

type branchCorpusGoldenFixture struct {
	FormatRevision     uint16             `json:"formatRevision"`
	Generation         uint64             `json:"generation"`
	CommitSequence     uint64             `json:"commitSequence"`
	Pages              []branchPageGolden `json:"pages"`
	UncompressedBytes  int                `json:"uncompressedBytes"`
	UncompressedSHA256 string             `json:"uncompressedSha256"`
	GzipSHA256         string             `json:"gzipSha256"`
	GzipBase64         string             `json:"gzipBase64"`
}

func TestRevision3BranchCorpusDecodesEveryTreeKind(t *testing.T) {
	fixture := loadBranchCorpusGoldenFixture(t)
	pages := decodeBranchCorpus(t, fixture)
	if fixture.FormatRevision != FormatVersion || fixture.Generation == 0 || fixture.CommitSequence == 0 ||
		len(fixture.Pages) != int(TreeIndexBuildCatalog-TreeCatalog+1) {
		t.Fatalf("fixture metadata=%+v", fixture)
	}
	seenKinds, seenTypes := make(map[TreeKind]struct{}), make(map[PageType]struct{})
	for index, descriptor := range fixture.Pages {
		if descriptor.TreeKind < TreeCatalog || descriptor.TreeKind > TreeIndexBuildCatalog {
			t.Fatalf("page[%d] tree kind=%d", index, descriptor.TreeKind)
		}
		_, expectedType := treePageTypes(descriptor.TreeKind)
		if descriptor.PageType != expectedType {
			t.Fatalf("page[%d] type=%d want=%d", index, descriptor.PageType, expectedType)
		}
		if _, duplicate := seenKinds[descriptor.TreeKind]; duplicate {
			t.Fatalf("duplicate tree kind=%d", descriptor.TreeKind)
		}
		if _, duplicate := seenTypes[descriptor.PageType]; duplicate {
			t.Fatalf("duplicate page type=%d", descriptor.PageType)
		}
		seenKinds[descriptor.TreeKind], seenTypes[descriptor.PageType] = struct{}{}, struct{}{}
		raw := pages[index*PageSize : (index+1)*PageSize]
		if digestHex(raw) != descriptor.SHA256 {
			t.Fatalf("page[%d] digest mismatch", index)
		}
		page, err := DecodePage(raw, descriptor.PageID)
		if err != nil || page.Type != descriptor.PageType || page.Generation != fixture.Generation ||
			page.BornSequence != fixture.CommitSequence {
			t.Fatalf("page[%d]=%+v err=%v", index, page, err)
		}
		node, err := decodeTreeNode(page)
		if err != nil || node.leaf || len(node.children) < 2 || len(node.keys)+1 != len(node.children) || node.count == 0 {
			t.Fatalf("page[%d] node=%+v err=%v", index, node, err)
		}
	}
}

func TestRevision3BranchCorpusWriterMatchesGoldenFixture(t *testing.T) {
	want := loadBranchCorpusGoldenFixture(t)
	got := buildBranchCorpusGoldenFixture(t)
	if got.GzipBase64 != want.GzipBase64 {
		limit := min(len(got.GzipBase64), len(want.GzipBase64))
		mismatch := limit
		for index := 0; index < limit; index++ {
			if got.GzipBase64[index] != want.GzipBase64[index] {
				mismatch = index
				break
			}
		}
		resume := strings.Index(got.GzipBase64[mismatch:], want.GzipBase64[mismatch:min(len(want.GzipBase64), mismatch+48)])
		t.Fatalf("branch corpus mismatch at %d resume=%d gotLen=%d wantLen=%d", mismatch, resume, len(got.GzipBase64), len(want.GzipBase64))
	}
	got.GzipBase64, want.GzipBase64 = "", ""
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("branch corpus metadata differs: got=%+v want=%+v", got, want)
	}
}

func TestGenerateRevision3BranchCorpus(t *testing.T) {
	if os.Getenv("MELDBASE_GENERATE_BRANCH_FIXTURE") != "1" {
		t.Skip("fixture generation is an explicit maintainer action")
	}
	encoded, err := json.MarshalIndent(buildBranchCorpusGoldenFixture(t), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(encoded))
}

func buildBranchCorpusGoldenFixture(t *testing.T) branchCorpusGoldenFixture {
	t.Helper()
	file, _, err := Open(filepath.Join(t.TempDir(), "branch-corpus.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	var descriptors []branchPageGolden
	var corpus []byte
	var generation, sequence uint64
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		generation, sequence = tx.generation, tx.sequence
		for kind := TreeCatalog; kind <= TreeIndexBuildCatalog; kind++ {
			before := len(tx.pages)
			builder, err := tx.NewSortedTreeBuilder(kind)
			if err != nil {
				return DatabaseRoot{}, err
			}
			for index := 0; index < 400; index++ {
				key := []byte(fmt.Sprintf("kind-%02d-key-%04d", kind, index))
				value := bytes.Repeat([]byte{byte(kind)}, 96+(index%7))
				if err := builder.Add(key, value); err != nil {
					return DatabaseRoot{}, err
				}
			}
			root, err := builder.Finish()
			if err != nil {
				return DatabaseRoot{}, err
			}
			_, branchType := treePageTypes(kind)
			found := 0
			for _, staged := range tx.pages[before:] {
				page, err := DecodePage(staged.data, staged.id)
				if err != nil {
					return DatabaseRoot{}, err
				}
				if page.Type != branchType {
					continue
				}
				if staged.id != root {
					return DatabaseRoot{}, ErrCorrupt
				}
				found++
				descriptors = append(descriptors, branchPageGolden{
					TreeKind: kind, PageType: page.Type, PageID: staged.id, SHA256: digestHex(staged.data),
				})
				corpus = append(corpus, staged.data...)
			}
			if found != 1 {
				return DatabaseRoot{}, fmt.Errorf("tree kind %d produced %d branch roots", kind, found)
			}
		}
		return DatabaseRoot{CommitSequence: tx.Sequence()}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(corpus); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return branchCorpusGoldenFixture{
		FormatRevision: FormatVersion, Generation: generation, CommitSequence: sequence, Pages: descriptors,
		UncompressedBytes: len(corpus), UncompressedSHA256: digestHex(corpus),
		GzipSHA256: digestHex(compressed.Bytes()), GzipBase64: base64.StdEncoding.EncodeToString(compressed.Bytes()),
	}
}

func decodeBranchCorpus(t *testing.T, fixture branchCorpusGoldenFixture) []byte {
	t.Helper()
	compressed, err := base64.StdEncoding.DecodeString(fixture.GzipBase64)
	if err != nil || digestHex(compressed) != fixture.GzipSHA256 {
		t.Fatalf("compressed branch corpus err=%v", err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil || len(raw) != fixture.UncompressedBytes || len(raw) != len(fixture.Pages)*PageSize || digestHex(raw) != fixture.UncompressedSHA256 {
		t.Fatalf("branch corpus bytes=%d err=%v", len(raw), err)
	}
	return raw
}

func loadBranchCorpusGoldenFixture(t *testing.T) branchCorpusGoldenFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "branch-pages-revision-3.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture branchCorpusGoldenFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}
