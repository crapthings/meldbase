package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFilePublishesRootAndFallsBackFromMetaCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.meld2")
	file, initial, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Generation != 1 || initial.CommitSequence != 0 || initial.RootPage != 0 || initial.PhysicalPageCount != 2 {
		t.Fatalf("initial meta = %+v", initial)
	}
	if _, _, err := Open(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("lock error = %v", err)
	}
	if err := file.CommitRoot(DatabaseRoot{CommitSequence: 1}); err != nil {
		t.Fatal(err)
	}
	first := file.Meta()
	if err := file.CommitRoot(DatabaseRoot{CommitSequence: 2, OldestRetainedSequence: 1}); err != nil {
		t.Fatal(err)
	}
	second := file.Meta()
	if first.RootPage != 2 || second.RootPage != 3 || second.Generation != 3 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Generation 3 is in meta slot 0. A torn/corrupt newest meta must reveal
	// the independently durable generation 2 root in slot 1.
	if err := flipByte(raw, 224); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	fallback, meta, report, err := OpenWithReport(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fallback.Close()
	if meta.Generation != first.Generation || meta.CommitSequence != first.CommitSequence || meta.RootPage != first.RootPage {
		t.Fatalf("fallback meta = %+v, want %+v", meta, first)
	}
	if !report.MetaRedundancyDegraded || report.ChecksumValidMetaSlots != 1 || report.RootValidMetaSlots != 1 {
		t.Fatalf("checksum fallback report=%+v", report)
	}
}

func flipByte(file *os.File, offset int64) error {
	value := []byte{0}
	if _, err := file.ReadAt(value, offset); err != nil {
		return err
	}
	value[0] ^= 0xff
	_, err := file.WriteAt(value, offset)
	return err
}

func TestFileFallsBackFromCorruptNewestRootAndTruncatesPartialTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "root-fallback.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.CommitRoot(DatabaseRoot{CommitSequence: 1}); err != nil {
		t.Fatal(err)
	}
	first := file.Meta()
	if err := file.CommitRoot(DatabaseRoot{CommitSequence: 2}); err != nil {
		t.Fatal(err)
	}
	newest := file.Meta()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Seek(0, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Write([]byte("partial orphan page")); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.WriteAt([]byte{0xff}, int64(newest.RootPage)*PageSize+64); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	fallback, meta, report, err := OpenWithReport(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CommitSequence != first.CommitSequence || meta.RootPage != first.RootPage {
		t.Fatalf("fallback meta = %+v, want %+v", meta, first)
	}
	if !report.FallbackToOlderRoot || !report.MetaRedundancyDegraded || report.ChecksumValidMetaSlots != 2 || report.RootValidMetaSlots != 1 || report.TrailingBytesRemoved != uint64(len("partial orphan page")) {
		t.Fatalf("open report=%+v", report)
	}
	if err := fallback.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size()%PageSize != 0 {
		t.Fatalf("partial tail remains: %d", info.Size())
	}
}

func TestOpenRequireGraphAuditRejectsDeepCorruptionWithoutMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph-audit.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{0xa7, 1}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("value"),
	}}}); err != nil {
		t.Fatal(err)
	}
	root, err := file.DatabaseRoot()
	if err != nil || root.CatalogRoot == 0 {
		t.Fatalf("root=%+v err=%v", root, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := flipByte(raw, int64(root.CatalogRoot)*PageSize+PageHeaderSize); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.Seek(0, 2); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.Write([]byte("crash-tail")); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if audited, _, _, err := OpenWithOptions(path, OpenOptions{RequireGraphAudit: true}); audited != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("audited open file=%v err=%v", audited, err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatal("failed graph audit modified the database")
	}

	// The fast metadata-only open remains available for normal large-database
	// startup. It must not be mistaken for a structural integrity audit.
	fast, _, _, err := OpenWithOptions(path, OpenOptions{})
	if err != nil {
		t.Fatalf("fast open unexpectedly rejected deep corruption: %v", err)
	}
	if err := fast.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitFaultsReopenAtExactlyOldOrNewGeneration(t *testing.T) {
	base := filepath.Join(t.TempDir(), "base.meld2")
	file, _, err := Open(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.CommitRoot(DatabaseRoot{CommitSequence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	baseBytes, err := os.ReadFile(base)
	if err != nil {
		t.Fatal(err)
	}

	points := []faultPoint{faultAfterPageWrite, faultBeforeDataSync, faultAfterDataSync, faultAfterMetaWrite, faultAfterMetaSync}
	for _, point := range points {
		t.Run(fmt.Sprint(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.meld2")
			if err := os.WriteFile(path, baseBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			candidate, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected crash")
			candidate.fault = func(current faultPoint) error {
				if current == point {
					return injected
				}
				return nil
			}
			if err := candidate.CommitRoot(DatabaseRoot{CommitSequence: 2}); !errors.Is(err, injected) {
				t.Fatalf("commit error = %v", err)
			}
			if err := candidate.CommitRoot(DatabaseRoot{CommitSequence: 2}); !errors.Is(err, injected) {
				t.Fatalf("fatal commit state was not retained: %v", err)
			}
			if err := candidate.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, meta, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if meta.CommitSequence != 1 && meta.CommitSequence != 2 {
				t.Fatalf("partial state sequence = %d", meta.CommitSequence)
			}
			root, err := reopened.DatabaseRoot()
			if err != nil || root.CommitSequence != meta.CommitSequence {
				t.Fatalf("root=%+v meta=%+v err=%v", root, meta, err)
			}
		})
	}
}

func TestOpenOptionsRejectStaleSnapshotBeforeMutation(t *testing.T) {
	directory := t.TempDir()
	currentPath := filepath.Join(directory, "current.meld2")
	stalePath := filepath.Join(directory, "stale.meld2")
	current, _, err := Open(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.CommitRoot(DatabaseRoot{CommitSequence: 1}); err != nil {
		t.Fatal(err)
	}
	identity := current.Meta().DatabaseID
	staleBytes, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stalePath, staleBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := current.CommitRoot(DatabaseRoot{CommitSequence: 2, OldestRetainedSequence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}

	accepted, meta, _, err := OpenWithOptions(currentPath, OpenOptions{ExpectedDatabaseID: identity, MinimumCommitSequence: 2})
	if err != nil || meta.CommitSequence != 2 {
		t.Fatalf("current sequence=%d err=%v", meta.CommitSequence, err)
	}
	if err := accepted.Close(); err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadFile(stalePath)
	if err != nil {
		t.Fatal(err)
	}
	if file, _, _, err := OpenWithOptions(stalePath, OpenOptions{ExpectedDatabaseID: identity, MinimumCommitSequence: 2}); !errors.Is(err, ErrStaleSnapshot) || file != nil {
		t.Fatalf("stale open file=%v err=%v", file, err)
	}
	after, err := os.ReadFile(stalePath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatal("stale rejection mutated the candidate database")
	}
	if file, _, _, err := OpenWithOptions(stalePath, OpenOptions{ExpectedDatabaseID: identity, MinimumGeneration: 3}); !errors.Is(err, ErrStaleSnapshot) || file != nil {
		t.Fatalf("stale generation open file=%v err=%v", file, err)
	}
	afterGenerationGuard, err := os.ReadFile(stalePath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterGenerationGuard, before) {
		t.Fatal("generation rejection mutated the candidate database")
	}

	wrongIdentity := identity
	wrongIdentity[0] ^= 0xff
	if file, _, _, err := OpenWithOptions(stalePath, OpenOptions{ExpectedDatabaseID: wrongIdentity}); !errors.Is(err, ErrDatabaseIdentity) || file != nil {
		t.Fatalf("identity open file=%v err=%v", file, err)
	}
	missing := filepath.Join(directory, "missing.meld2")
	if file, _, _, err := OpenWithOptions(missing, OpenOptions{MinimumCommitSequence: 1}); !errors.Is(err, ErrStaleSnapshot) || file != nil {
		t.Fatalf("missing open file=%v err=%v", file, err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("minimum sequence created missing database: %v", err)
	}
	missingGeneration := filepath.Join(directory, "missing-generation.meld2")
	if file, _, _, err := OpenWithOptions(missingGeneration, OpenOptions{MinimumGeneration: 1}); !errors.Is(err, ErrStaleSnapshot) || file != nil {
		t.Fatalf("missing generation open file=%v err=%v", file, err)
	}
	if _, err := os.Stat(missingGeneration); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("minimum generation created missing database: %v", err)
	}
}

func TestOpenForQualificationReportsPublicationBoundariesInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "qualification.meld2")
	seed, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{15: 1}
	if _, err := seed.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{1}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentInsert, Document: []byte("old"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	var boundaries []QualificationBoundary
	file, _, _, err := OpenForQualification(path, OpenOptions{}, func(boundary QualificationBoundary) error {
		boundaries = append(boundaries, boundary)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ApplyDocumentTransaction(DocumentTransaction{TransactionID: [16]byte{2}, Mutations: []DocumentMutation{{
		Collection: "items", DocumentID: id, Operation: DocumentUpdate, Document: []byte("new"),
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	wantSuffix := []QualificationBoundary{
		QualificationBeforeDataSync, QualificationAfterDataSync, QualificationAfterMetaWrite, QualificationAfterMetaSync,
	}
	if len(boundaries) < len(wantSuffix)+1 || boundaries[0] != QualificationAfterPageWrite ||
		!reflect.DeepEqual(boundaries[len(boundaries)-len(wantSuffix):], wantSuffix) {
		t.Fatalf("boundaries=%v", boundaries)
	}
}
