package database

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	storage "github.com/crapthings/meldbase/internal/storage"
)

var recoveryBenchmarkSink RecoveryReport

func TestRequirePrivateFileModeRejectsExistingBroadPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix mode bits are not a Windows security boundary")
	}
	path := filepath.Join(t.TempDir(), "private-mode.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if strict, err := OpenWithOptions(path, OpenOptions{RequirePrivateFileMode: true}); strict != nil || !errors.Is(err, ErrInsecureFileMode) {
		t.Fatalf("strict open db=%v err=%v", strict, err)
	}
	if strict, err := OpenWithOptions(path, OpenOptions{RequirePrivateFileMode: true}); strict != nil || !errors.Is(err, ErrInsecureFileMode) {
		t.Fatalf("strict format-neutral open db=%v err=%v", strict, err)
	}
	// Default opening remains compatible with intentionally group-managed
	// deployment files; strict mode never chmods an operator-owned artifact.
	compatible, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := compatible.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("default open changed permissions info=%v err=%v", info, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	strict, err := OpenWithOptions(path, OpenOptions{RequirePrivateFileMode: true})
	if err != nil {
		t.Fatal(err)
	}
	defer strict.Close()
}

func TestRecoveryReportIsImmutableAndAllocationFree(t *testing.T) {
	db := New()
	defer db.Close()
	want := db.RecoveryReport()
	mutated := want
	mutated.Engine = "changed"
	if got := db.RecoveryReport(); got != want || got.Engine != "memory" {
		t.Fatalf("report mutated: got=%+v want=%+v", got, want)
	}
	if allocations := testing.AllocsPerRun(1_000, func() { _ = db.RecoveryReport() }); allocations != 0 {
		t.Fatalf("RecoveryReport allocations=%f", allocations)
	}
}

func BenchmarkRecoveryReport(b *testing.B) {
	db := New()
	defer db.Close()
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		recoveryBenchmarkSink = db.RecoveryReport()
	}
}

func TestRecoveryReportAccountsForRootFallbackAndTailRemoval(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery-store.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	collection := db.Collection("items")
	if _, err := collection.InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.InsertOne(context.Background(), Document{"value": Int(2)}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var newest storage.Meta
	for slot := range 2 {
		page := make([]byte, storage.PageSize)
		if _, err := file.ReadAt(page, int64(slot*storage.PageSize)); err != nil {
			file.Close()
			t.Fatal(err)
		}
		meta, err := storage.DecodeMeta(page)
		if err == nil && meta.Generation > newest.Generation {
			newest = meta
		}
	}
	if newest.RootPage == 0 {
		file.Close()
		t.Fatal("newest root is empty")
	}
	byteAtRoot := []byte{0}
	offset := int64(newest.RootPage*storage.PageSize + 64)
	if _, err := file.ReadAt(byteAtRoot, offset); err != nil {
		file.Close()
		t.Fatal(err)
	}
	byteAtRoot[0] ^= 0xff
	if _, err := file.WriteAt(byteAtRoot, offset); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if _, err := file.Seek(0, 2); err != nil {
		file.Close()
		t.Fatal(err)
	}
	tail := []byte("partial-store-page")
	if _, err := file.Write(tail); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before := mustReadRecoveryFile(t, path)
	if strict, err := OpenWithOptions(path, OpenOptions{Recovery: RecoveryRequireClean}); !errors.Is(err, ErrRecoveryRequired) || strict != nil {
		t.Fatalf("strict open db=%v err=%v", strict, err)
	}
	if got := mustReadRecoveryFile(t, path); !bytes.Equal(got, before) {
		t.Fatal("strict open modified the file")
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report := reopened.RecoveryReport()
	if report.Engine != "current" || report.Created || !report.Recovered || !report.FallbackToOlderRoot || !report.MetaRedundancyDegraded ||
		report.ChecksumValidMetaSlots != 2 || report.RootValidMetaSlots != 1 ||
		report.MainTailBytesRemoved != uint64(len(tail)) || report.CommitSequenceAfter != 1 {
		t.Fatalf(" recovery report=%+v newest=%+v", report, newest)
	}
}

func TestPublicGraphAuditRejectsDeepCorruptionBeforeTailRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "public-graph-audit.meld2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, _, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	root, err := file.DatabaseRoot()
	if closeErr := file.Close(); err != nil {
		t.Fatal(err)
	} else if closeErr != nil {
		t.Fatal(closeErr)
	}
	if root.CatalogRoot == 0 {
		t.Fatal("missing catalog root")
	}
	raw, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	byteAtPayload := []byte{0}
	if _, err := raw.ReadAt(byteAtPayload, int64(root.CatalogRoot)*storage.PageSize+storage.PageHeaderSize); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	byteAtPayload[0] ^= 0xff
	if _, err := raw.WriteAt(byteAtPayload, int64(root.CatalogRoot)*storage.PageSize+storage.PageHeaderSize); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.Seek(0, 2); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.Write([]byte("public-crash-tail")); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	before := mustReadRecoveryFile(t, path)

	openers := []struct {
		name string
		open func() (*DB, error)
	}{
		{name: "explicit-store", open: func() (*DB, error) {
			return OpenWithOptions(path, OpenOptions{RequireGraphAudit: true})
		}},
		{name: "format-neutral", open: func() (*DB, error) {
			return OpenWithOptions(path, OpenOptions{RequireGraphAudit: true})
		}},
	}
	for _, opener := range openers {
		t.Run(opener.name, func(t *testing.T) {
			opened, err := opener.open()
			if opened != nil || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("open db=%v err=%v", opened, err)
			}
			if after := mustReadRecoveryFile(t, path); !bytes.Equal(after, before) {
				t.Fatal("failed public graph audit modified the database")
			}
		})
	}
}

func appendRecoveryTail(t *testing.T, path string, tail []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(tail); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func mustReadRecoveryFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestRecoveryModeRejectsUnknownValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-mode.meld2")
	if db, err := OpenWithOptions(path, OpenOptions{Recovery: RecoveryMode(255)}); err == nil || db != nil {
		t.Fatalf("invalid format-neutral mode db=%v err=%v", db, err)
	}
}

func TestRequireCleanAllowsCreationAndCleanReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strict-clean.meld2")
	db, err := OpenWithOptions(path, OpenOptions{Recovery: RecoveryRequireClean})
	if err != nil {
		t.Fatal(err)
	}
	if report := db.RecoveryReport(); !report.Created || report.Recovered || report.Engine != "current" {
		t.Fatalf("created strict report=%+v", report)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWithOptions(path, OpenOptions{Recovery: RecoveryRequireClean})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if report := reopened.RecoveryReport(); report.Created || report.Recovered || report.Engine != "current" {
		t.Fatalf("clean strict reopen report=%+v", report)
	}
}
