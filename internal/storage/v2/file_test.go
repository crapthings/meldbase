package v2

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
