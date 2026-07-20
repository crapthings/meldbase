package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestIndexBuildMetaCodecRoundTripsAndRejectsCorruption(t *testing.T) {
	created := time.UnixMilli(1_700_000_000_000).UTC()
	meta := IndexBuildMeta{
		BuildID: [16]byte{1}, CollectionID: 7, Collection: "items", Name: "by_value", FieldPath: "nested.value",
		Unique: true, Phase: IndexBuildCatchUp, SourceSequence: 9, SourceCatalogRoot: 20,
		ShadowRoot: 21, ScanAfter: [16]byte{8}, AppliedSequence: 11, EntryCount: 12,
		CanonicalBytes: 345, CreatedAt: created, UpdatedAt: created.Add(time.Second),
	}
	encoded, err := encodeIndexBuildMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	if digest := fmt.Sprintf("%x", sha256.Sum256(encoded)); digest != "4274729cd420acd8a292fa29e14080f422acf00785d2746f2f0852444c6e7601" {
		t.Fatalf("index build codec golden digest=%s", digest)
	}
	decoded, err := decodeIndexBuildMeta(meta.BuildID[:], encoded)
	if err != nil || !reflect.DeepEqual(decoded, meta) {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	rooted := meta
	rooted.AppliedCatalogRoot = 22
	rootedEncoded, err := encodeIndexBuildMeta(rooted)
	if err != nil || rootedEncoded[15] != 1 {
		t.Fatalf("rooted encoding flag=%d err=%v", rootedEncoded[15], err)
	}
	if decoded, err := decodeIndexBuildMeta(rooted.BuildID[:], rootedEncoded); err != nil || !reflect.DeepEqual(decoded, rooted) {
		t.Fatalf("rooted decoded=%+v err=%v", decoded, err)
	}
	for _, offset := range []int{15, 112} {
		corrupt := append([]byte(nil), rootedEncoded...)
		corrupt[offset] = 0
		if _, err := decodeIndexBuildMeta(rooted.BuildID[:], corrupt); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("rooted offset=%d err=%v", offset, err)
		}
	}
	for _, offset := range []int{0, 8, 10, 12, 13, 90, 120} {
		corrupt := append([]byte(nil), encoded...)
		corrupt[offset] ^= 0xff
		if _, err := decodeIndexBuildMeta(meta.BuildID[:], corrupt); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("offset=%d err=%v", offset, err)
		}
	}
}

func TestCompoundIndexBuildMetaCodecRoundTripsAndRejectsCorruption(t *testing.T) {
	created := time.UnixMilli(1_700_000_000_000).UTC()
	meta := IndexBuildMeta{
		BuildID: [16]byte{1}, CollectionID: 7, Collection: "items", Name: "tenant_score", FieldPath: "tenant",
		Fields: []IndexField{{Path: "tenant", Direction: 1}, {Path: "score", Direction: -1}},
		Phase:  IndexBuildScan, SourceSequence: 9, SourceCatalogRoot: 20, ShadowRoot: 21,
		AppliedSequence: 9, CreatedAt: created, UpdatedAt: created,
	}
	encoded, err := encodeIndexBuildMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeIndexBuildMeta(meta.BuildID[:], encoded)
	if err != nil || !reflect.DeepEqual(decoded, meta) {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	for name, mutate := range map[string]func([]byte){
		"count":     func(value []byte) { value[92] = 3 },
		"direction": func(value []byte) { value[indexBuildMetaHeaderBytes+len(meta.Collection)+len(meta.Name)] = 0 },
		"reserved":  func(value []byte) { value[94] = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			corrupt := append([]byte(nil), encoded...)
			mutate(corrupt)
			if _, err := decodeIndexBuildMeta(meta.BuildID[:], corrupt); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestIndexBuildAppliedRootFeatureCannotBeDetached(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index-build-applied-root-feature.meld2")
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
	build, err := file.ApplyIndexBuildCatchUpBatch(IndexBuildCatchUpBatch{
		BuildID: buildID, ExpectedAppliedSequence: 1, ThroughSequence: 2,
		Mutations: []IndexBuildCatchUpMutation{{Sequence: 2, DocumentID: second, Operation: CommitInsert, AfterKey: []byte("b")}},
	})
	if err != nil || build.AppliedCatalogRoot < 2 || file.Meta().RequiredFeatures&RequiredFeatureIndexBuildAppliedRoot == 0 {
		t.Fatalf("build=%+v meta=%+v err=%v", build, file.Meta(), err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	selectedSlot, selected := -1, Meta{}
	for slot := range 2 {
		page := make([]byte, PageSize)
		if _, err := raw.ReadAt(page, int64(slot*PageSize)); err != nil {
			t.Fatal(err)
		}
		meta, err := DecodeMeta(page)
		if err == nil && (selectedSlot < 0 || meta.Generation > selected.Generation) {
			selectedSlot, selected = slot, meta
		}
	}
	if selectedSlot < 0 || selected.RequiredFeatures&RequiredFeatureIndexBuildAppliedRoot == 0 {
		t.Fatalf("selected meta=%+v slot=%d", selected, selectedSlot)
	}
	selected.RequiredFeatures &^= RequiredFeatureIndexBuildAppliedRoot
	encoded, err := EncodeMeta(selected)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.WriteAt(encoded, int64(selectedSlot*PageSize)); err != nil {
		t.Fatal(err)
	}
	if err := raw.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	opened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.Reachability(); !errors.Is(err, ErrCorrupt) {
		_ = opened.Close()
		t.Fatalf("detached applied-root feature error=%v", err)
	}
	_ = opened.Close()
}

func TestIndexBuildBeginFaultsNeverSplitFeatureAndCatalogRoot(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "begin-base.meld2")
	base, _, err := Open(basePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: [16]byte{1}, Operation: DocumentInsert, Document: []byte("one"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range commitFaultPoints() {
		t.Run(faultPointName(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.meld2")
			if err := os.WriteFile(path, baseBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			candidate, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			injected := fmt.Errorf("injected build begin fault at %s", faultPointName(point))
			candidate.fault = func(current faultPoint) error {
				if current == point {
					return injected
				}
				return nil
			}
			if _, err := candidate.BeginIndexBuild(BeginIndexBuildTransaction{
				BuildID: [16]byte{9}, Collection: "items", Name: "by_value", FieldPath: "value",
			}); !errors.Is(err, injected) {
				t.Fatalf("begin error=%v", err)
			}
			if err := candidate.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, meta, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			root, err := reopened.DatabaseRoot()
			if err != nil {
				t.Fatal(err)
			}
			builds, err := reopened.IndexBuilds()
			if err != nil || len(builds) > 1 {
				t.Fatalf("builds=%+v err=%v", builds, err)
			}
			if len(builds) == 0 {
				if root.IndexBuildCatalogRoot != 0 {
					t.Fatalf("catalog root without build: root=%+v", root)
				}
			} else if root.IndexBuildCatalogRoot < 2 || meta.RequiredFeatures&RequiredFeatureShadowIndexBuilds == 0 {
				t.Fatalf("published build without feature/root: meta=%+v root=%+v", meta, root)
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatalf("reachability=%v", err)
			}
		})
	}
}

func TestIndexBuildCatchUpFaultsNeverSplitAppliedRootAndFeature(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "catch-up-base.meld2")
	base, _, err := Open(basePath)
	if err != nil {
		t.Fatal(err)
	}
	first, second, buildID := [16]byte{1}, [16]byte{2}, [16]byte{9}
	if _, err := base.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("one"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := base.BeginIndexBuild(BeginIndexBuildTransaction{
		BuildID: buildID, Collection: "items", Name: "by_value", FieldPath: "value",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := base.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: buildID, ScanAfter: first, Complete: true, Entries: []IndexEntry{{Key: []byte("a"), DocumentID: first}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := base.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: []byte("two"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range commitFaultPoints() {
		t.Run(faultPointName(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.meld2")
			if err := os.WriteFile(path, baseBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			candidate, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			injected := fmt.Errorf("injected build catch-up fault at %s", faultPointName(point))
			candidate.fault = func(current faultPoint) error {
				if current == point {
					return injected
				}
				return nil
			}
			if _, err := candidate.ApplyIndexBuildCatchUpBatch(IndexBuildCatchUpBatch{
				BuildID: buildID, ExpectedAppliedSequence: 1, ThroughSequence: 2,
				Mutations: []IndexBuildCatchUpMutation{{Sequence: 2, DocumentID: second, Operation: CommitInsert, AfterKey: []byte("b")}},
			}); !errors.Is(err, injected) {
				t.Fatalf("catch-up error=%v", err)
			}
			if err := candidate.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, meta, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			build, exists, err := reopened.IndexBuild(buildID)
			if err != nil || !exists {
				t.Fatalf("build=%+v exists=%t err=%v", build, exists, err)
			}
			switch build.AppliedSequence {
			case 1:
				if build.AppliedCatalogRoot != 0 || meta.RequiredFeatures&RequiredFeatureIndexBuildAppliedRoot != 0 {
					t.Fatalf("mixed old catch-up generation: build=%+v meta=%+v", build, meta)
				}
			case 2:
				if build.AppliedCatalogRoot < 2 || meta.RequiredFeatures&RequiredFeatureIndexBuildAppliedRoot == 0 {
					t.Fatalf("mixed new catch-up generation: build=%+v meta=%+v", build, meta)
				}
			default:
				t.Fatalf("unexpected applied sequence: build=%+v", build)
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatalf("reachability=%v", err)
			}
		})
	}
}

func TestIndexBuildFinalizeFaultsNeverExposeMixedPublication(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "finalize-base.meld2")
	base, _, err := Open(basePath)
	if err != nil {
		t.Fatal(err)
	}
	id, buildID := [16]byte{1}, [16]byte{9}
	if _, err := base.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("one"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := base.BeginIndexBuild(BeginIndexBuildTransaction{BuildID: buildID, Collection: "items", Name: "by_value", FieldPath: "value"}); err != nil {
		t.Fatal(err)
	}
	ready, err := base.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: buildID, ScanAfter: id, Complete: true, Entries: []IndexEntry{{Key: []byte("a"), DocumentID: id}},
	})
	if err != nil || ready.Phase != IndexBuildReady {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range commitFaultPoints() {
		t.Run(faultPointName(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.meld2")
			if err := os.WriteFile(path, baseBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			candidate, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			injected := fmt.Errorf("injected build finalize fault at %s", faultPointName(point))
			candidate.fault = func(current faultPoint) error {
				if current == point {
					return injected
				}
				return nil
			}
			if _, err := candidate.FinalizeIndexBuild(FinalizeIndexBuildTransaction{
				BuildID: buildID, TransactionID: randomTransactionID(t), ExpectedAppliedSequence: 1,
			}); !errors.Is(err, injected) {
				t.Fatalf("finalize error=%v", err)
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
			if err != nil || (root.CommitSequence != 1 && root.CommitSequence != 2) {
				t.Fatalf("root=%+v err=%v", root, err)
			}
			_, buildExists, buildErr := reopened.IndexBuild(buildID)
			snapshot, err := reopened.OpenSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			index, indexExists, indexErr := snapshot.IndexMeta("items", "by_value")
			_ = snapshot.Close()
			if buildErr != nil || indexErr != nil {
				t.Fatalf("buildErr=%v indexErr=%v", buildErr, indexErr)
			}
			if root.CommitSequence == 1 && (!buildExists || indexExists || root.IndexBuildCatalogRoot == 0) {
				t.Fatalf("mixed old generation: build=%t index=%t root=%+v", buildExists, indexExists, root)
			}
			if root.CommitSequence == 2 && (buildExists || !indexExists || root.IndexBuildCatalogRoot != 0 || index.Root != ready.ShadowRoot) {
				t.Fatalf("mixed new generation: build=%t index=%+v exists=%t root=%+v", buildExists, index, indexExists, root)
			}
			if _, err := reopened.Reachability(); err != nil {
				t.Fatalf("reachability=%v", err)
			}
		})
	}
}

func TestBeginIndexBuildProtectsRootsAcrossCommitsReopenAndAbort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shadow-build.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, second, third, fourth := [16]byte{1}, [16]byte{2}, [16]byte{3}, [16]byte{4}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("one")},
		{Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: []byte("two")},
		{Collection: "items", DocumentID: third, Operation: DocumentInsert, Document: []byte("three")},
	}}); err != nil {
		t.Fatal(err)
	}
	before := file.Meta()
	buildID := [16]byte{9}
	created := time.UnixMilli(1_700_000_000_000).UTC()
	build, err := file.BeginIndexBuild(BeginIndexBuildTransaction{
		BuildID: buildID, Collection: "items", Name: "by_value", FieldPath: "value", Unique: true, CreatedAt: created,
	})
	if err != nil {
		t.Fatal(err)
	}
	afterBegin := file.Meta()
	root, err := file.DatabaseRoot()
	if err != nil || afterBegin.CommitSequence != before.CommitSequence || afterBegin.Generation != before.Generation+1 ||
		afterBegin.RequiredFeatures&RequiredFeatureShadowIndexBuilds == 0 || root.IndexBuildCatalogRoot < 2 ||
		build.SourceSequence != 1 || build.SourceCatalogRoot < 2 || build.ShadowRoot < 2 || build.Phase != IndexBuildScan {
		t.Fatalf("meta=%+v root=%+v build=%+v err=%v", afterBegin, root, build, err)
	}
	if _, err := file.BeginIndexBuild(BeginIndexBuildTransaction{
		BuildID: [16]byte{10}, Collection: "items", Name: "by_value", FieldPath: "value",
	}); !errors.Is(err, ErrIndexBuildExists) {
		t.Fatalf("duplicate build err=%v", err)
	}
	batch, err := file.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: buildID, ScanAfter: second, Entries: []IndexEntry{{Key: []byte("a"), DocumentID: first}}, UpdatedAt: created.Add(time.Second),
	})
	if err != nil || batch.Phase != IndexBuildScan || batch.EntryCount != 1 || batch.ScanAfter != second || file.Meta().CommitSequence != 1 {
		t.Fatalf("scan batch=%+v meta=%+v err=%v", batch, file.Meta(), err)
	}
	if _, err := file.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: buildID, ExpectedScanAfter: [16]byte{99}, ScanAfter: third, Complete: true, UpdatedAt: created.Add(2 * time.Second),
	}); !errors.Is(err, ErrIndexBuildState) {
		t.Fatalf("stale scan cursor err=%v", err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: fourth, Operation: DocumentInsert, Document: []byte("four")},
	}}); err != nil {
		t.Fatal(err)
	}
	root, err = file.DatabaseRoot()
	if err != nil || file.Meta().CommitSequence != 2 || root.IndexBuildCatalogRoot < 2 {
		t.Fatalf("post-write root=%+v meta=%+v err=%v", root, file.Meta(), err)
	}
	if stats, err := file.Reachability(); err != nil || stats.ReachablePages == 0 {
		t.Fatalf("reachability=%+v err=%v", stats, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	completedScan, err := reopened.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: buildID, ExpectedScanAfter: second, ScanAfter: third,
		Entries: []IndexEntry{{Key: []byte("c"), DocumentID: third}}, Complete: true, UpdatedAt: created.Add(2 * time.Second),
	})
	if err != nil || completedScan.Phase != IndexBuildCatchUp || completedScan.EntryCount != 2 || completedScan.CanonicalBytes == 0 || reopened.Meta().CommitSequence != 2 {
		t.Fatalf("completed scan=%+v meta=%+v err=%v", completedScan, reopened.Meta(), err)
	}
	shadowEntries, err := reopened.TreeScan(completedScan.ShadowRoot, TreeSecondary, nil, nil, 0)
	if err != nil || len(shadowEntries) != 2 {
		t.Fatalf("shadow entries=%d err=%v", len(shadowEntries), err)
	}
	caughtUp, err := reopened.ApplyIndexBuildCatchUpBatch(IndexBuildCatchUpBatch{
		BuildID: buildID, ExpectedAppliedSequence: 1, ThroughSequence: 2,
		Mutations: []IndexBuildCatchUpMutation{{Sequence: 2, DocumentID: fourth, Operation: CommitInsert, AfterKey: []byte("d")}},
		UpdatedAt: created.Add(3 * time.Second),
	})
	if err != nil || caughtUp.Phase != IndexBuildReady || caughtUp.AppliedSequence != 2 || caughtUp.AppliedCatalogRoot < 2 || caughtUp.EntryCount != 3 ||
		reopened.Meta().RequiredFeatures&RequiredFeatureIndexBuildAppliedRoot == 0 {
		t.Fatalf("caught up=%+v err=%v", caughtUp, err)
	}
	shadowEntries, err = reopened.TreeScan(caughtUp.ShadowRoot, TreeSecondary, nil, nil, 0)
	if err != nil || len(shadowEntries) != 3 {
		t.Fatalf("caught-up shadow entries=%d err=%v", len(shadowEntries), err)
	}
	builds, err := reopened.IndexBuilds()
	if err != nil || len(builds) != 1 || builds[0].BuildID != buildID || builds[0].SourceSequence != 1 || builds[0].Phase != IndexBuildReady {
		currentRoot, _ := reopened.DatabaseRoot()
		raw, scanErr := reopened.TreeScan(currentRoot.IndexBuildCatalogRoot, TreeIndexBuildCatalog, nil, nil, 0)
		var decodeErr error
		if len(raw) > 0 {
			_, decodeErr = decodeIndexBuildMeta(raw[0].Key, raw[0].Value)
		}
		t.Fatalf("builds=%+v err=%v raw=%d scanErr=%v decodeErr=%v", builds, err, len(raw), scanErr, decodeErr)
	}
	finalSequence, err := reopened.FinalizeIndexBuild(FinalizeIndexBuildTransaction{
		BuildID: buildID, TransactionID: randomTransactionID(t), ExpectedAppliedSequence: 2,
	})
	if err != nil || finalSequence != 3 {
		t.Fatalf("finalize sequence=%d err=%v", finalSequence, err)
	}
	root, err = reopened.DatabaseRoot()
	if err != nil || root.CommitSequence != 3 || root.IndexBuildCatalogRoot != 0 ||
		reopened.Meta().RequiredFeatures&RequiredFeatureShadowIndexBuilds == 0 {
		t.Fatalf("finalized root=%+v meta=%+v err=%v", root, reopened.Meta(), err)
	}
	if _, exists, err := reopened.IndexBuild(buildID); err != nil || exists {
		t.Fatalf("finalized build exists=%t err=%v", exists, err)
	}
	snapshot, err := reopened.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	index, exists, err := snapshot.IndexMeta("items", "by_value")
	if closeErr := snapshot.Close(); err == nil {
		err = closeErr
	}
	if err != nil || !exists || index.Root != caughtUp.ShadowRoot || index.EntryCount != 3 || !index.Unique {
		t.Fatalf("published index=%+v exists=%t err=%v", index, exists, err)
	}
	commit, err := reopened.ReadCommit(root.CommitLogRoot, 3)
	if err != nil || len(commit.Changes) != 1 || commit.Changes[0].ChangedPaths[0] != "_indexes.by_value" {
		t.Fatalf("final commit=%+v err=%v", commit, err)
	}
	abortID := [16]byte{11}
	if _, err := reopened.BeginIndexBuild(BeginIndexBuildTransaction{BuildID: abortID, Collection: "items", Name: "by_other", FieldPath: "other"}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.AbortIndexBuild(abortID); err != nil {
		t.Fatal(err)
	}
	if err := reopened.AbortIndexBuild(abortID); !errors.Is(err, ErrIndexBuildNotFound) {
		t.Fatalf("second abort err=%v", err)
	}
	if _, err := reopened.Reachability(); err != nil {
		t.Fatalf("post-abort reachability=%v", err)
	}
}

func TestIndexBuildPersistentReplayLeaseBoundsCommitPruning(t *testing.T) {
	file, _, _, err := OpenWithOptions(filepath.Join(t.TempDir(), "build-retention.meld2"), OpenOptions{CommitRetentionMaxCommits: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: [16]byte{1}, Operation: DocumentInsert, Document: []byte("one")},
	}}); err != nil {
		t.Fatal(err)
	}
	buildID := [16]byte{7}
	if _, err := file.BeginIndexBuild(BeginIndexBuildTransaction{BuildID: buildID, Collection: "items", Name: "by_value", FieldPath: "value"}); err != nil {
		t.Fatal(err)
	}
	for sequence := byte(2); sequence <= 6; sequence++ {
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{
			{Collection: "items", DocumentID: [16]byte{sequence}, Operation: DocumentInsert, Document: []byte{sequence}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	stats := file.StorageStats()
	if stats.CommitSequence != 6 || stats.OldestRetainedSequence != 2 || !stats.RetentionPressure || stats.RetainedCommits != 5 {
		t.Fatalf("pinned retention=%+v", stats)
	}
	if err := file.AbortIndexBuild(buildID); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: [16]byte{7}, Operation: DocumentInsert, Document: []byte("seven")},
	}}); err != nil {
		t.Fatal(err)
	}
	stats = file.StorageStats()
	if stats.CommitSequence != 7 || stats.OldestRetainedSequence != 6 || stats.RetentionPressure || stats.RetainedCommits != 2 {
		t.Fatalf("released retention=%+v", stats)
	}
}

func TestFinalizeUniqueIndexBuildRejectsConflictWithoutPublication(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "unique-shadow.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	first, second := [16]byte{1}, [16]byte{2}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{
		{Collection: "items", DocumentID: first, Operation: DocumentInsert, Document: []byte("one")},
		{Collection: "items", DocumentID: second, Operation: DocumentInsert, Document: []byte("two")},
	}}); err != nil {
		t.Fatal(err)
	}
	buildID := [16]byte{12}
	if _, err := file.BeginIndexBuild(BeginIndexBuildTransaction{BuildID: buildID, Collection: "items", Name: "unique_value", FieldPath: "value", Unique: true}); err != nil {
		t.Fatal(err)
	}
	build, err := file.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: buildID, ScanAfter: second, Complete: true,
		Entries: []IndexEntry{{Key: []byte("same"), DocumentID: first}, {Key: []byte("same"), DocumentID: second}},
	})
	if err != nil || build.Phase != IndexBuildReady {
		t.Fatalf("ready build=%+v err=%v", build, err)
	}
	if _, err := file.FinalizeIndexBuild(FinalizeIndexBuildTransaction{
		BuildID: buildID, TransactionID: randomTransactionID(t), ExpectedAppliedSequence: 1,
	}); !errors.Is(err, ErrUniqueConflict) {
		t.Fatalf("finalize unique conflict=%v", err)
	}
	root, err := file.DatabaseRoot()
	if err != nil || root.CommitSequence != 1 || root.IndexBuildCatalogRoot == 0 {
		t.Fatalf("root=%+v err=%v", root, err)
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	_, exists, indexErr := snapshot.IndexMeta("items", "unique_value")
	_ = snapshot.Close()
	if indexErr != nil || exists {
		t.Fatalf("conflicted index exists=%t err=%v", exists, indexErr)
	}
	if err := file.AbortIndexBuild(buildID); err != nil {
		t.Fatal(err)
	}
}

func TestFailedIndexBuildPersistsAndReleasesReplayLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed-build.meld2")
	file, _, _, err := OpenWithOptions(path, OpenOptions{CommitRetentionMaxCommits: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: [16]byte{1}, Operation: DocumentInsert, Document: []byte("one"),
	}}}); err != nil {
		t.Fatal(err)
	}
	buildID := [16]byte{9}
	if _, err := file.BeginIndexBuild(BeginIndexBuildTransaction{BuildID: buildID, Collection: "items", Name: "by_value", FieldPath: "value"}); err != nil {
		t.Fatal(err)
	}
	for sequence := byte(2); sequence <= 4; sequence++ {
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: [16]byte{sequence}, Operation: DocumentInsert, Document: []byte{sequence},
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	if stats := file.StorageStats(); !stats.RetentionPressure || stats.OldestRetainedSequence != 2 {
		t.Fatalf("active lease stats=%+v", stats)
	}
	failed, err := file.FailIndexBuild(FailIndexBuildTransaction{BuildID: buildID, Failure: IndexBuildFailureHistoryLost})
	if err != nil || failed.Phase != IndexBuildFailed || failed.Failure != IndexBuildFailureHistoryLost || file.Meta().CommitSequence != 4 {
		t.Fatalf("failed=%+v meta=%+v err=%v", failed, file.Meta(), err)
	}
	if _, err := file.FailIndexBuild(FailIndexBuildTransaction{BuildID: buildID, Failure: IndexBuildFailureHistoryLost}); !errors.Is(err, ErrIndexBuildState) {
		t.Fatalf("second failure transition=%v", err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: [16]byte{5}, Operation: DocumentInsert, Document: []byte("five"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if stats := file.StorageStats(); stats.RetentionPressure || stats.OldestRetainedSequence != 4 || stats.RetainedCommits != 2 {
		t.Fatalf("released failed lease stats=%+v", stats)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	persisted, exists, err := reopened.IndexBuild(buildID)
	if err != nil || !exists || persisted.Phase != IndexBuildFailed || persisted.Failure != IndexBuildFailureHistoryLost {
		t.Fatalf("persisted=%+v exists=%t err=%v", persisted, exists, err)
	}
}

func TestIndexBuildCatchUpSnapshotPinsCommitAndVersionRootsAcrossReuse(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "catch-up-pin.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	id, buildID := [16]byte{1}, [16]byte{9}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("v1"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.BeginIndexBuild(BeginIndexBuildTransaction{BuildID: buildID, Collection: "items", Name: "by_value", FieldPath: "value"}); err != nil {
		t.Fatal(err)
	}
	ready, err := file.ApplyIndexBuildScanBatch(IndexBuildScanBatch{
		BuildID: buildID, ScanAfter: id, Complete: true, Entries: []IndexEntry{{Key: []byte("v1"), DocumentID: id}},
	})
	if err != nil || ready.Phase != IndexBuildReady {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("current"),
	}}}); err != nil {
		t.Fatal(err)
	}
	opened, snapshot, err := file.OpenIndexBuildCatchUpSnapshot(buildID)
	if err != nil || opened.AppliedSequence != 1 || snapshot.Sequence() != 2 {
		t.Fatalf("opened=%+v sequence=%d err=%v", opened, snapshot.Sequence(), err)
	}
	for sequence := 3; sequence <= 12; sequence++ {
		if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: randomTransactionID(t), Mutations: []DocumentMutation{{
			Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte(fmt.Sprintf("v%d", sequence)),
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := file.ReclaimPages(); err != nil {
		t.Fatal(err)
	}
	commit, err := snapshot.ReadCommit(2)
	if err != nil || len(commit.Changes) != 1 || commit.Changes[0].BeforeRef == nil || commit.Changes[0].AfterRef == nil {
		t.Fatalf("commit=%+v err=%v", commit, err)
	}
	before, err := snapshot.ReadDocumentVersion(*commit.Changes[0].BeforeRef)
	if err != nil || string(before) != "v1" {
		t.Fatalf("before=%q err=%v", before, err)
	}
	after, err := snapshot.ReadDocumentVersion(*commit.Changes[0].AfterRef)
	if err != nil || string(after) != "current" {
		t.Fatalf("after=%q err=%v", after, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyIndexBuildCatchUpBatch(IndexBuildCatchUpBatch{
		BuildID: buildID, ExpectedAppliedSequence: 1, ThroughSequence: 2,
		Mutations: []IndexBuildCatchUpMutation{{Sequence: 2, DocumentID: id, Operation: CommitUpdate, BeforeKey: []byte("v1"), AfterKey: []byte("v2")}},
	}); err != nil {
		t.Fatal(err)
	}
}
