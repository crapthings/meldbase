package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
)

func TestServeBackupSchedulerPublishesPairedArtifactsAndPrunesOnlyVerifiedPairs(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.meld")
	backups := filepath.Join(directory, "backups")
	drills := filepath.Join(directory, "drills")
	db, err := meldbase.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items := db.Collection("items")
	if _, err := items.InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(1)}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	logger := newBufferServiceLogger(t, &output, "info")
	scheduler, err := newServeBackupScheduler(db, serveBackupSchedule{
		Directory: backups, Interval: time.Hour, Keep: 1, Timeout: time.Minute, DrillDir: drills,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 24, 15, 0, 0, 1, time.UTC)
	scheduler.now = func() time.Time { return now }
	first, err := scheduler.runOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.CommitSequence == 0 {
		t.Fatalf("first result=%+v", first)
	}
	if err := os.WriteFile(filepath.Join(backups, "operator-owned.meld"), []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := items.InsertOne(context.Background(), meldbase.Document{"value": meldbase.Int(2)}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	second, err := scheduler.runOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.CommitSequence <= first.CommitSequence {
		t.Fatalf("second=%+v first=%+v", second, first)
	}
	untrustedArtifact := filepath.Join(backups, scheduledBackupPrefix+"operator-link.meld")
	if err := os.Symlink("operator-owned.meld", untrustedArtifact); err != nil {
		t.Fatal(err)
	}
	if err := writeScheduledBackupReceipt(untrustedArtifact+".json", second); err != nil {
		t.Fatal(err)
	}
	pairs, err := scheduledBackupPairs(backups)
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 1 {
		t.Fatalf("pairs=%+v", pairs)
	}
	receipt, err := readPhysicalBackupReceipt(pairs[0].receipt)
	if err != nil || receipt.CommitSequence != second.CommitSequence {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	reopened, err := meldbase.Open(pairs[0].artifact)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Stats().CommitSequence != second.CommitSequence {
		_ = reopened.Close()
		t.Fatalf("restored sequence=%d want=%d", reopened.Stats().CommitSequence, second.CommitSequence)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(backups, "operator-owned.meld")); err != nil || string(raw) != "preserve" {
		t.Fatalf("operator file=%q err=%v", raw, err)
	}
	if _, err := os.Lstat(untrustedArtifact); err != nil {
		t.Fatalf("untrusted artifact was touched: %v", err)
	}
	if !strings.Contains(output.String(), `"event":"scheduled_backup_completed"`) {
		t.Fatalf("output=%q", output.String())
	}
	receipts, err := filepath.Glob(filepath.Join(drills, "meldbase-restore-drill-*.json"))
	if err != nil || len(receipts) != 2 {
		t.Fatalf("drill receipts=%v err=%v", receipts, err)
	}
	leftovers, err := filepath.Glob(filepath.Join(drills, ".meldbase-restore-drill-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("drill leftovers=%v err=%v", leftovers, err)
	}
	if !strings.Contains(output.String(), `"event":"scheduled_backup_restore_drill_completed"`) {
		t.Fatalf("output=%q", output.String())
	}
}

func TestServeBackupSchedulerRequiresCompleteSafeConfiguration(t *testing.T) {
	db := meldbase.New()
	defer db.Close()
	for _, schedule := range []serveBackupSchedule{
		{Directory: t.TempDir()},
		{Directory: t.TempDir(), Interval: time.Minute, Keep: 1},
		{Directory: t.TempDir(), Interval: time.Minute, Timeout: time.Minute},
		{Directory: t.TempDir(), Interval: time.Minute, Keep: 0, Timeout: time.Minute},
		{DrillDir: t.TempDir()},
	} {
		if _, err := newServeBackupScheduler(db, schedule, newBufferServiceLogger(t, &bytes.Buffer{}, "info")); err == nil {
			t.Fatalf("schedule=%+v accepted", schedule)
		}
	}
	if _, err := newServeBackupScheduler(db, serveBackupSchedule{}, newBufferServiceLogger(t, &bytes.Buffer{}, "info")); err != nil {
		t.Fatalf("disabled scheduler error=%v", err)
	}
	scheduler, err := newServeBackupScheduler(db, serveBackupSchedule{Directory: t.TempDir(), Interval: time.Minute, Keep: 2, Timeout: time.Minute}, newBufferServiceLogger(t, &bytes.Buffer{}, "info"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.runOnce(context.Background()); !errors.Is(err, meldbase.ErrBackupUnsupported) {
		t.Fatalf("memory scheduled backup error=%v", err)
	}
}
