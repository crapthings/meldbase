package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointReopenAndMetaFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.meld")
	file, initial, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(initial) != 0 || meta.Generation != 1 {
		t.Fatalf("initial meta = %+v", meta)
	}
	one := bytes.Repeat([]byte("one"), PageSize)
	if err := file.Checkpoint(1, one); err != nil {
		t.Fatal(err)
	}
	two := bytes.Repeat([]byte("two"), PageSize+100)
	if err := file.Checkpoint(2, two); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, got, current, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, two) || current.CheckpointToken != 2 {
		t.Fatalf("current checkpoint mismatch")
	}
	reopened.Close()
	// Generation 3 is in slot 0. Corrupting only that meta page must reveal the
	// previous fully synced generation in slot 1.
	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.WriteAt([]byte{0xff}, 48); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	fallback, got, previous, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fallback.Close()
	if !bytes.Equal(got, one) || previous.CheckpointToken != 1 {
		t.Fatalf("fallback = token %d len %d", previous.CheckpointToken, len(got))
	}
}

func TestOpenRejectsLockAndRecoversTrailingPartialPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.meld")
	file, _, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := Open(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("lock error = %v", err)
	}
	if err := file.Checkpoint(1, []byte("safe")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	raw.Write([]byte("partial orphan"))
	raw.Close()
	reopened, got, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if string(got) != "safe" {
		t.Fatalf("got %q", got)
	}
}

func TestSnapshotCorruptionIsDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.meld")
	file, _, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Checkpoint(1, []byte("important")); err != nil {
		t.Fatal(err)
	}
	file.Close()
	raw, _ := os.OpenFile(path, os.O_RDWR, 0)
	raw.WriteAt([]byte{0xff}, int64(2*PageSize+pageHeaderSize))
	raw.Close()
	if _, _, _, err := Open(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corruption error = %v", err)
	}
}

func TestCheckpointUsesCatalogRootAndSlottedRecordPages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.meld")
	file, _, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("record"), PageSize)
	if err := file.Checkpoint(7, payload); err != nil {
		t.Fatal(err)
	}
	meta := file.meta
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	root := make([]byte, PageSize)
	if _, err := raw.ReadAt(root, int64(meta.RootPage)*PageSize); err != nil {
		t.Fatal(err)
	}
	if root[10] != catalogPageType {
		t.Fatalf("root page type = %d", root[10])
	}
	record := make([]byte, PageSize)
	if _, err := raw.ReadAt(record, 2*PageSize); err != nil {
		t.Fatal(err)
	}
	if record[10] != recordPageType {
		t.Fatalf("record page type = %d", record[10])
	}
}

func TestOpenReadsLegacyContiguousSnapshotPages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.meld")
	file, _, initial, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("legacy snapshot")
	page := encodePage(2, snapshotPageType, 2, 9, 0, 1, payload)
	if _, err := file.file.WriteAt(page, 2*PageSize); err != nil {
		t.Fatal(err)
	}
	meta := Meta{DatabaseID: initial.DatabaseID, Generation: 2, RootPage: 2, PageCount: 1, CheckpointToken: 9}
	metaPage, err := encodeMetaPage(1, meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.file.WriteAt(metaPage, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := file.file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, got, reopenedMeta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !bytes.Equal(got, payload) || reopenedMeta.CheckpointToken != 9 {
		t.Fatalf("legacy snapshot = %q meta=%+v", got, reopenedMeta)
	}
}

func TestTypedCheckpointBlobsRemainIndependentlyAddressed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blobs.meld")
	file, _, _, err := OpenBlobs(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []Blob{
		{Kind: 1, Data: []byte("catalog")},
		{Kind: 2, Data: bytes.Repeat([]byte("large-document"), PageSize)},
		{Kind: 3, Class: BlobClassIndex, Data: nil},
	}
	if err := file.CheckpointBlobs(11, want); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, got, meta, err := OpenBlobs(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if meta.CheckpointToken != 11 || len(got) != len(want) {
		t.Fatalf("meta=%+v blobs=%d", meta, len(got))
	}
	for index := range want {
		if got[index].Kind != want[index].Kind || got[index].Class != want[index].Class || !bytes.Equal(got[index].Data, want[index].Data) {
			t.Fatalf("blob %d mismatch", index)
		}
	}
	if _, _, _, err := Open(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("legacy API typed-blob error = %v", err)
	}
}

func TestCheckpointReusesOnlyPagesOlderThanBothMetaGenerations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reuse.meld")
	file, _, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var firstGeneration, reusedGeneration RecordID
	for token := 1; token <= 10; token++ {
		payload := bytes.Repeat([]byte{byte(token)}, PageSize+100)
		if err := file.Checkpoint(uint64(token), payload); err != nil {
			t.Fatal(err)
		}
		if token == 1 {
			firstGeneration = firstCatalogRecordID(t, file)
		}
		if token == 4 {
			reusedGeneration = firstCatalogRecordID(t, file)
		}
		if token == 5 {
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			file, _, _, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if firstGeneration.Page != reusedGeneration.Page || firstGeneration.Generation == reusedGeneration.Generation {
		t.Fatalf("reused page aliased stale RecordID: old=%+v new=%+v", firstGeneration, reusedGeneration)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Each checkpoint needs two record pages and one catalog page. Three
	// generations are enough for copy-on-write plus two-meta fallback.
	if pages := info.Size() / PageSize; pages > 11 {
		t.Fatalf("checkpoint file grew to %d pages", pages)
	}
	latest, got, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CheckpointToken != 10 || len(got) == 0 || got[0] != 10 {
		t.Fatalf("latest token=%d data length=%d", meta.CheckpointToken, len(got))
	}
	if err := latest.Close(); err != nil {
		t.Fatal(err)
	}
	// Token 10 occupies meta slot 0. Damaging it must still reveal token 9;
	// its pages were protected from the reuse allocator.
	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.WriteAt([]byte{0xff}, 48); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	fallback, got, meta, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fallback.Close()
	if meta.CheckpointToken != 9 || len(got) == 0 || got[0] != 9 {
		t.Fatalf("fallback token=%d data length=%d", meta.CheckpointToken, len(got))
	}
}

func TestCheckpointPhysicalCrashPointMatrixPublishesOnlyWholeSnapshots(t *testing.T) {
	points := []checkpointFaultPoint{
		faultAfterCheckpointPageWrite,
		faultBeforeCheckpointDataSync,
		faultAfterCheckpointDataSync,
		faultAfterCheckpointMetaWrite,
		faultAfterCheckpointMetaSync,
	}
	for _, point := range points {
		point := point
		t.Run(string(rune('0'+point)), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "checkpoint-crash.meld")
			file, _, _, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			stable := bytes.Repeat([]byte{2}, PageSize*2+127)
			if err := file.Checkpoint(1, bytes.Repeat([]byte{1}, PageSize+31)); err != nil {
				t.Fatal(err)
			}
			if err := file.Checkpoint(2, stable); err != nil {
				t.Fatal(err)
			}
			candidate := bytes.Repeat([]byte{3}, PageSize*3+257)
			injected := errors.New("injected physical checkpoint boundary")
			file.fault = func(actual checkpointFaultPoint) error {
				if actual == point {
					return injected
				}
				return nil
			}
			if err := file.Checkpoint(3, candidate); !errors.Is(err, injected) {
				t.Fatalf("checkpoint error=%v", err)
			}
			file.fault = nil
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, got, meta, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			wantToken, want := uint64(2), stable
			if point >= faultAfterCheckpointMetaWrite {
				wantToken, want = 3, candidate
			}
			if meta.CheckpointToken != wantToken || !bytes.Equal(got, want) {
				t.Fatalf("point=%d token=%d want=%d bytes=%d", point, meta.CheckpointToken, wantToken, len(got))
			}
		})
	}
}

func firstCatalogRecordID(t *testing.T, file *File) RecordID {
	t.Helper()
	page := make([]byte, PageSize)
	if _, err := file.file.ReadAt(page, int64(file.meta.RootPage)*PageSize); err != nil {
		t.Fatal(err)
	}
	_, payload, err := decodePage(page, file.meta.RootPage, catalogPageType)
	if err != nil || len(payload) < catalogV2HeaderSize+catalogV2EntrySize || binary.LittleEndian.Uint16(payload[8:10]) != 2 {
		t.Fatalf("catalog payload err=%v len=%d", err, len(payload))
	}
	offset := catalogV2HeaderSize + 13
	return RecordID{
		Page:       binary.LittleEndian.Uint64(payload[offset : offset+8]),
		Slot:       binary.LittleEndian.Uint16(payload[offset+8 : offset+10]),
		Generation: binary.LittleEndian.Uint32(payload[offset+10 : offset+14]),
	}
}
