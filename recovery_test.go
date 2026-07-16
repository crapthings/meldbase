package meldbase

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

var recoveryBenchmarkSink RecoveryReport

func TestRecoveryReportV1AccountsForCheckpointReplayAndProvableTails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery-v1.meld")
	db, err := OpenV1(path)
	if err != nil {
		t.Fatal(err)
	}
	if report := db.RecoveryReport(); report.SchemaVersion != 1 || report.Engine != "v1" || !report.Created || report.Recovered {
		t.Fatalf("created report=%+v", report)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	// Simulate process loss: release test file descriptors without the graceful
	// Close checkpoint/reset path.
	if err := db.store.close(); err != nil {
		t.Fatal(err)
	}
	mainTail := []byte("partial-main-page")
	walTail := []byte("partial-wal-frame")
	appendRecoveryTail(t, path, mainTail)
	appendRecoveryTail(t, path+".wal", walTail)
	mainBefore := mustReadRecoveryFile(t, path)
	walBefore := mustReadRecoveryFile(t, path+".wal")
	if strict, err := OpenV1WithOptions(path, V1Options{Recovery: RecoveryRequireClean}); !errors.Is(err, ErrRecoveryRequired) || strict != nil {
		t.Fatalf("strict V1 open db=%v err=%v", strict, err)
	}
	if got := mustReadRecoveryFile(t, path); !bytes.Equal(got, mainBefore) {
		t.Fatal("strict V1 open modified the main file")
	}
	if got := mustReadRecoveryFile(t, path+".wal"); !bytes.Equal(got, walBefore) {
		t.Fatal("strict V1 open modified the WAL")
	}

	reopened, err := OpenV1(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report := reopened.RecoveryReport()
	if report.SchemaVersion != 1 || report.Engine != "v1" || report.Created || !report.Recovered ||
		report.CommitSequenceBefore != 0 || report.CommitSequenceAfter != 1 || report.WALRecordsReplayed != 1 ||
		report.MainTailBytesRemoved != uint64(len(mainTail)) || report.WALTailBytesRemoved != uint64(len(walTail)) {
		t.Fatalf("recovery report=%+v", report)
	}
	if reopened.Stats().Recovery != report {
		t.Fatalf("stats recovery=%+v want=%+v", reopened.Stats().Recovery, report)
	}
}

func TestRequireCleanRejectsCompleteV1WALReplayWithoutModifyingFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strict-wal-v1.meld")
	db, err := OpenV1(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("items").InsertOne(context.Background(), Document{"value": Int(1)}); err != nil {
		t.Fatal(err)
	}
	if err := db.store.close(); err != nil {
		t.Fatal(err)
	}
	mainBefore, walBefore := mustReadRecoveryFile(t, path), mustReadRecoveryFile(t, path+".wal")
	if strict, err := OpenWithOptions(path, OpenOptions{Recovery: RecoveryRequireClean}); !errors.Is(err, ErrRecoveryRequired) || strict != nil {
		t.Fatalf("strict format-neutral open db=%v err=%v", strict, err)
	}
	if !bytes.Equal(mustReadRecoveryFile(t, path), mainBefore) || !bytes.Equal(mustReadRecoveryFile(t, path+".wal"), walBefore) {
		t.Fatal("strict WAL replay check modified storage")
	}
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

func TestRecoveryReportV2AccountsForRootFallbackAndTailRemoval(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery-v2.meld2")
	db, err := OpenV2(path)
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
	var newest storagev2.Meta
	for slot := range 2 {
		page := make([]byte, storagev2.PageSize)
		if _, err := file.ReadAt(page, int64(slot*storagev2.PageSize)); err != nil {
			file.Close()
			t.Fatal(err)
		}
		meta, err := storagev2.DecodeMeta(page)
		if err == nil && meta.Generation > newest.Generation {
			newest = meta
		}
	}
	if newest.RootPage == 0 {
		file.Close()
		t.Fatal("newest V2 root is empty")
	}
	byteAtRoot := []byte{0}
	offset := int64(newest.RootPage*storagev2.PageSize + 64)
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
	tail := []byte("partial-v2-page")
	if _, err := file.Write(tail); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before := mustReadRecoveryFile(t, path)
	if strict, err := OpenV2WithOptions(path, V2Options{Recovery: RecoveryRequireClean}); !errors.Is(err, ErrRecoveryRequired) || strict != nil {
		t.Fatalf("strict V2 open db=%v err=%v", strict, err)
	}
	if got := mustReadRecoveryFile(t, path); !bytes.Equal(got, before) {
		t.Fatal("strict V2 open modified the file")
	}

	reopened, err := OpenV2(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report := reopened.RecoveryReport()
	if report.Engine != "v2" || report.Created || !report.Recovered || !report.FallbackToOlderRoot || !report.MetaRedundancyDegraded ||
		report.ChecksumValidMetaSlots != 2 || report.RootValidMetaSlots != 1 ||
		report.MainTailBytesRemoved != uint64(len(tail)) || report.CommitSequenceAfter != 1 {
		t.Fatalf("V2 recovery report=%+v newest=%+v", report, newest)
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
	if db, err := OpenV1WithOptions(path, V1Options{Recovery: RecoveryMode(255)}); err == nil || db != nil {
		t.Fatalf("invalid V1 mode db=%v err=%v", db, err)
	}
	if db, err := OpenV2WithOptions(path, V2Options{Recovery: RecoveryMode(255)}); err == nil || db != nil {
		t.Fatalf("invalid V2 mode db=%v err=%v", db, err)
	}
}

func TestRequireCleanAllowsCreationAndCleanReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strict-clean.meld2")
	db, err := OpenWithOptions(path, OpenOptions{Recovery: RecoveryRequireClean})
	if err != nil {
		t.Fatal(err)
	}
	if report := db.RecoveryReport(); !report.Created || report.Recovered || report.Engine != "v2" {
		t.Fatalf("created strict report=%+v", report)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenV2WithOptions(path, V2Options{Recovery: RecoveryRequireClean})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if report := reopened.RecoveryReport(); report.Created || report.Recovered || report.Engine != "v2" {
		t.Fatalf("clean strict reopen report=%+v", report)
	}
}
