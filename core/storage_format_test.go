package meldbase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPublicOpenRejectsChecksumValidUnsupportedV2WithoutMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future-v2.meld")
	db, err := OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	page := make([]byte, storageFormatPageSize)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(page, 0); err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(page[8:10], binary.LittleEndian.Uint16(page[8:10])+1)
	clear(page[224:256])
	checksum := sha256.Sum256(page)
	copy(page[224:256], checksum[:])
	if _, err := file.WriteAt(page, 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if format, err := DetectStorageFormat(path); err != nil || format != StorageFormatV2 {
		t.Fatalf("format=%q err=%v", format, err)
	}
	for name, open := range map[string]func(string) (*DB, error){"Open": Open, "OpenV2": OpenV2} {
		t.Run(name, func(t *testing.T) {
			opened, err := open(path)
			if opened != nil {
				_ = opened.Close()
			}
			if !errors.Is(err, ErrUnsupportedFormat) || errors.Is(err, ErrCorrupt) {
				t.Fatalf("error=%v want ErrUnsupportedFormat only", err)
			}
		})
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("unsupported open mutated file: %v", err)
	}
}

func TestOpenDefaultsToV2AndDispatchesExistingFormatsWithoutMigration(t *testing.T) {
	directory := t.TempDir()
	newPath := filepath.Join(directory, "new-default.meld")
	db, err := Open(newPath)
	if err != nil {
		t.Fatal(err)
	}
	if db.Stats().Storage.Engine != "v2" {
		t.Fatalf("new database engine=%q want=v2", db.Stats().Storage.Engine)
	}
	id, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if format, err := DetectStorageFormat(newPath); err != nil || format != StorageFormatV2 {
		t.Fatalf("new database format=%q err=%v", format, err)
	}
	if _, err := os.Stat(newPath + ".wal"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("default V2 created a legacy WAL: %v", err)
	}
	reopened, err := Open(newPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Collection("items").FindOne(context.Background(), Filter{"_id": id}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	legacyPath := filepath.Join(directory, "legacy.meld")
	legacy, err := OpenV1(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	legacyID, err := legacy.Collection("items").InsertOne(context.Background(), Document{"value": String("legacy")})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	dispatched, err := Open(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if dispatched.Stats().Storage.Engine != "v1" {
		t.Fatalf("legacy database engine=%q want=v1", dispatched.Stats().Storage.Engine)
	}
	if _, err := dispatched.Collection("items").FindOne(context.Background(), Filter{"_id": legacyID}); err != nil {
		t.Fatal(err)
	}
	if err := dispatched.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if format, err := DetectStorageFormat(legacyPath); err != nil || format != StorageFormatV1 {
		t.Fatalf("legacy database format=%q err=%v", format, err)
	}
	if string(before[:8]) != string(after[:8]) {
		t.Fatal("default open changed the legacy format family")
	}
}

func TestOpenFailsClosedForOrphanWALCorruptionAndCrossFormatUse(t *testing.T) {
	directory := t.TempDir()
	orphanPath := filepath.Join(directory, "orphan.meld")
	if err := os.WriteFile(orphanPath+".wal", []byte("possible legacy data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(orphanPath); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("orphan WAL error=%v", err)
	}
	if _, err := os.Stat(orphanPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan WAL open created main file: %v", err)
	}

	corruptPath := filepath.Join(directory, "corrupt.meld")
	if err := os.WriteFile(corruptPath, []byte("not a database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(corruptPath); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt database error=%v", err)
	}

	v1Path := filepath.Join(directory, "explicit-v1.meld")
	v1, err := OpenV1(v1Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.Close(); err != nil {
		t.Fatal(err)
	}
	v1Before, _ := os.ReadFile(v1Path)
	if _, err := OpenV2(v1Path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("OpenV2(V1) error=%v", err)
	}
	v1After, _ := os.ReadFile(v1Path)
	if string(v1Before) != string(v1After) {
		t.Fatal("OpenV2 mutated V1 after rejecting it")
	}

	v2Path := filepath.Join(directory, "explicit-v2.meld")
	v2, err := OpenV2(v2Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := v2.Close(); err != nil {
		t.Fatal(err)
	}
	v2Before, _ := os.ReadFile(v2Path)
	if _, err := OpenV1(v2Path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("OpenV1(V2) error=%v", err)
	}
	v2After, _ := os.ReadFile(v2Path)
	if string(v2Before) != string(v2After) {
		t.Fatal("OpenV1 mutated V2 after rejecting it")
	}
}

func TestOpenDispatcherPreservesExclusiveWriterLocks(t *testing.T) {
	for _, format := range []StorageFormat{StorageFormatV1, StorageFormatV2} {
		t.Run(string(format), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), string(format)+".meld")
			var first *DB
			var err error
			if format == StorageFormatV1 {
				first, err = OpenV1(path)
			} else {
				first, err = OpenV2(path)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			if second, err := Open(path); err == nil {
				_ = second.Close()
				t.Fatal("default Open bypassed the exclusive writer lock")
			}
		})
	}
}

func TestDetectStorageFormatIsReadOnlyAndFailClosed(t *testing.T) {
	directory := t.TempDir()
	missing := filepath.Join(directory, "missing.meld")
	if format, err := DetectStorageFormat(missing); err != nil || format != StorageFormatUnknown {
		t.Fatalf("missing format=%q err=%v", format, err)
	}
	empty := filepath.Join(directory, "empty.meld")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if format, err := DetectStorageFormat(empty); err != nil || format != StorageFormatUnknown {
		t.Fatalf("empty format=%q err=%v", format, err)
	}

	v1Path := filepath.Join(directory, "v1.meld")
	v1, err := OpenV1(v1Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.Close(); err != nil {
		t.Fatal(err)
	}
	v1Before, err := os.ReadFile(v1Path)
	if err != nil {
		t.Fatal(err)
	}
	if format, err := DetectStorageFormat(v1Path); err != nil || format != StorageFormatV1 {
		t.Fatalf("V1 format=%q err=%v", format, err)
	}
	v1After, _ := os.ReadFile(v1Path)
	if string(v1After) != string(v1Before) {
		t.Fatal("format detection mutated V1 file")
	}

	v2Path := filepath.Join(directory, "v2.meld2")
	v2, err := OpenV2(v2Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := v2.Close(); err != nil {
		t.Fatal(err)
	}
	v2Before, err := os.ReadFile(v2Path)
	if err != nil {
		t.Fatal(err)
	}
	if format, err := DetectStorageFormat(v2Path); err != nil || format != StorageFormatV2 {
		t.Fatalf("V2 format=%q err=%v", format, err)
	}
	v2After, _ := os.ReadFile(v2Path)
	if string(v2After) != string(v2Before) {
		t.Fatal("format detection mutated V2 file")
	}

	short := filepath.Join(directory, "short.meld")
	if err := os.WriteFile(short, []byte("MELDPAGE"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := DetectStorageFormat(short); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("short error=%v", err)
	}
	unknown := filepath.Join(directory, "unknown.meld")
	if err := os.WriteFile(unknown, make([]byte, 2*storageFormatPageSize), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := DetectStorageFormat(unknown); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unknown error=%v", err)
	}
	mixed := append([]byte(nil), v1Before...)
	copy(mixed[storageFormatPageSize:storageFormatPageSize+8], storageV2MetaMagic[:])
	mixedPath := filepath.Join(directory, "mixed.meld")
	if err := os.WriteFile(mixedPath, mixed, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := DetectStorageFormat(mixedPath); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mixed error=%v", err)
	}
}

func TestInspectStorageFormatReportsV1V2AndFutureNegotiationReadOnly(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.meld")
	if info, err := InspectStorageFormat(missing); err != nil || info.Format != StorageFormatUnknown || info.ReaderCompatible {
		t.Fatalf("missing info=%+v err=%v", info, err)
	}

	directory := t.TempDir()
	v2Path := filepath.Join(directory, "inspect-v2.meld")
	v2, err := OpenV2(v2Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v2.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	if err := v2.Close(); err != nil {
		t.Fatal(err)
	}
	v2Before, err := os.ReadFile(v2Path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := InspectStorageFormat(v2Path)
	if err != nil || info.Format != StorageFormatV2 || info.Revision != 3 || !info.ReaderCompatible ||
		info.CommitSequence != 1 || info.Generation != 2 || info.PhysicalPageCount < 2 ||
		info.DatabaseIDHex == "" || info.ValidMetaSlots != 2 {
		t.Fatalf("V2 info=%+v err=%v", info, err)
	}
	v2After, _ := os.ReadFile(v2Path)
	if !bytes.Equal(v2After, v2Before) {
		t.Fatal("V2 inspection mutated the file")
	}

	for _, scenario := range []struct {
		name   string
		mutate func([]byte)
		check  func(StorageFormatInfo) bool
	}{
		{name: "future-revision", mutate: func(page []byte) {
			binary.LittleEndian.PutUint16(page[8:10], 4)
		}, check: func(info StorageFormatInfo) bool { return info.Revision == 4 }},
		{name: "unknown-required-feature", mutate: func(page []byte) {
			binary.LittleEndian.PutUint64(page[72:80], 1<<63)
		}, check: func(info StorageFormatInfo) bool { return info.RequiredFeatures == 1<<63 }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), scenario.name+".meld")
			candidate := append([]byte(nil), v2Before...)
			newestSlot := 0
			if binary.LittleEndian.Uint64(candidate[storageFormatPageSize+32:storageFormatPageSize+40]) > binary.LittleEndian.Uint64(candidate[32:40]) {
				newestSlot = 1
			}
			page := candidate[newestSlot*storageFormatPageSize : (newestSlot+1)*storageFormatPageSize]
			scenario.mutate(page)
			clear(page[224:256])
			checksum := sha256.Sum256(page)
			copy(page[224:256], checksum[:])
			if err := os.WriteFile(path, candidate, 0o600); err != nil {
				t.Fatal(err)
			}
			info, err := InspectStorageFormat(path)
			if err != nil || info.Format != StorageFormatV2 || info.ReaderCompatible || !scenario.check(info) {
				t.Fatalf("future info=%+v err=%v", info, err)
			}
			if _, err := Open(path); !errors.Is(err, ErrUnsupportedFormat) {
				t.Fatalf("future open error=%v", err)
			}
		})
	}

	v1Path := filepath.Join(directory, "inspect-v1.meld")
	v1, err := OpenV1(v1Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v1.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	if err := v1.Close(); err != nil {
		t.Fatal(err)
	}
	v1Before, _ := os.ReadFile(v1Path)
	info, err = InspectStorageFormat(v1Path)
	if err != nil || info.Format != StorageFormatV1 || info.Revision != 1 || !info.ReaderCompatible ||
		info.CommitSequence != 1 || info.Generation < 2 || info.DatabaseIDHex == "" || info.ValidMetaSlots != 2 {
		t.Fatalf("V1 info=%+v err=%v", info, err)
	}
	v1After, _ := os.ReadFile(v1Path)
	if !bytes.Equal(v1After, v1Before) {
		t.Fatal("V1 inspection mutated the file")
	}
}
