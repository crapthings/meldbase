package v2

import (
	"sort"
	"testing"
)

func TestRevision3FixturePageCoverage(t *testing.T) {
	artifacts := map[string][]byte{
		"business":            decodeBusinessGoldenFixtureBytes(t),
		"multilevel-business": decodeMultilevelGoldenFixture(t, loadMultilevelGoldenFixture(t)),
		"shadow-build":        decodeGoldenFixture(t, loadIndexBuildGoldenFixture(t)),
		"compound-index":      decodeGoldenFixture(t, loadCompoundGoldenFixture(t)),
		"applied-root-delta":  reconstructAppliedRootGoldenFixture(t, loadAppliedRootGoldenFixture(t)),
	}
	coverage := make(map[PageType][]string)
	for name, raw := range artifacts {
		seen := make(map[PageType]struct{})
		for pageID := uint64(2); pageID < uint64(len(raw)/PageSize); pageID++ {
			page, err := DecodePage(raw[pageID*PageSize:(pageID+1)*PageSize], pageID)
			if err != nil {
				t.Fatalf("%s page %d: %v", name, pageID, err)
			}
			seen[page.Type] = struct{}{}
		}
		for pageType := range seen {
			coverage[pageType] = append(coverage[pageType], name)
		}
	}
	branchFixture := loadBranchCorpusGoldenFixture(t)
	_ = decodeBranchCorpus(t, branchFixture)
	for _, page := range branchFixture.Pages {
		coverage[page.PageType] = append(coverage[page.PageType], "branch-corpus")
	}
	for pageType := PageDatabaseRoot; pageType <= PageIndexBuildCatalogLeaf; pageType++ {
		sort.Strings(coverage[pageType])
		if len(coverage[pageType]) == 0 {
			t.Fatalf("revision-3 fixtures do not cover page type %d", pageType)
		}
		t.Logf("pageType=%d artifacts=%v", pageType, coverage[pageType])
	}
}
