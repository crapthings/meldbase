package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/crapthings/meldbase"
)

// scheduledBackupPrefix identifies files created by the scheduled backup service.
const scheduledBackupPrefix = "meldbase-backup-"

// serveBackupSchedule configures backup timing, retention, and optional restore drills.
type serveBackupSchedule struct {
	Directory string
	Interval  time.Duration
	Keep      int
	Timeout   time.Duration
	DrillDir  string
}

// serveBackupScheduler runs periodic backups and tracks the worker until shutdown.
type serveBackupScheduler struct {
	db       *meldbase.DB
	schedule serveBackupSchedule
	logger   *serviceLogger
	now      func() time.Time
	group    sync.WaitGroup
}

// newServeBackupScheduler validates the schedule and prepares private backup and drill directories.
func newServeBackupScheduler(db *meldbase.DB, schedule serveBackupSchedule, logger *serviceLogger) (*serveBackupScheduler, error) {
	if schedule.Directory == "" && schedule.Interval == 0 && schedule.Keep == 0 && schedule.Timeout == 0 && schedule.DrillDir == "" {
		return nil, nil
	}
	if db == nil {
		return nil, errors.New("scheduled backup requires an open database")
	}
	if schedule.Directory == "" || schedule.Interval <= 0 || schedule.Keep < 1 || schedule.Timeout <= 0 {
		return nil, errors.New("scheduled backup requires --backup-dir, positive --backup-interval, positive --backup-keep, and positive --backup-timeout")
	}
	directory, err := filepath.Abs(filepath.Clean(schedule.Directory))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create backup directory: %w", err)
	}
	info, err := os.Stat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("scheduled backup directory is not a directory")
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, fmt.Errorf("protect backup directory: %w", err)
	}
	drillDirectory := ""
	if schedule.DrillDir != "" {
		drillDirectory, err = filepath.Abs(filepath.Clean(schedule.DrillDir))
		if err != nil {
			return nil, err
		}
		if drillDirectory == directory {
			return nil, errors.New("scheduled backup drill directory must differ from --backup-dir")
		}
		if err := os.MkdirAll(drillDirectory, 0o700); err != nil {
			return nil, fmt.Errorf("create backup drill directory: %w", err)
		}
		if info, err := os.Stat(drillDirectory); err != nil || !info.IsDir() {
			return nil, errors.New("scheduled backup drill directory is not a directory")
		}
		if err := os.Chmod(drillDirectory, 0o700); err != nil {
			return nil, fmt.Errorf("protect backup drill directory: %w", err)
		}
	}
	return &serveBackupScheduler{db: db, schedule: serveBackupSchedule{
		Directory: directory, Interval: schedule.Interval, Keep: schedule.Keep, Timeout: schedule.Timeout, DrillDir: drillDirectory,
	}, logger: logger, now: time.Now}, nil
}

// start launches periodic backup attempts until the context ends.
func (scheduler *serveBackupScheduler) start(ctx context.Context) {
	if scheduler == nil {
		return
	}
	scheduler.group.Add(1)
	go func() {
		defer scheduler.group.Done()
		ticker := time.NewTicker(scheduler.schedule.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := scheduler.runOnce(ctx); err != nil {
					scheduler.logger.error("scheduled_backup_failed", map[string]any{"error": err.Error()})
				}
			}
		}
	}()
}

// wait waits for the scheduled backup worker to finish.
func (scheduler *serveBackupScheduler) wait() {
	if scheduler != nil {
		scheduler.group.Wait()
	}
}

// runOnce creates a backup and receipt, applies retention, and performs any configured restore drill.
func (scheduler *serveBackupScheduler) runOnce(parent context.Context) (meldbase.BackupResult, error) {
	if scheduler == nil || scheduler.db == nil {
		return meldbase.BackupResult{}, errors.New("scheduled backup is not configured")
	}
	createdAt := scheduler.now().UTC()
	base := scheduledBackupPrefix + createdAt.Format("20060102T150405.000000000Z")
	artifact := filepath.Join(scheduler.schedule.Directory, base+".meld")
	ctx, cancel := context.WithTimeout(parent, scheduler.schedule.Timeout)
	defer cancel()
	result, err := scheduler.db.Backup(ctx, artifact)
	if err != nil {
		return meldbase.BackupResult{}, err
	}
	if err := writeScheduledBackupReceipt(artifact+".json", result); err != nil {
		return meldbase.BackupResult{}, errors.Join(err, os.Remove(artifact))
	}
	if err := scheduler.prune(); err != nil {
		return meldbase.BackupResult{}, err
	}
	if scheduler.schedule.DrillDir != "" {
		if err := scheduler.drill(ctx, artifact, result); err != nil {
			return meldbase.BackupResult{}, err
		}
	}
	scheduler.logger.info("scheduled_backup_completed", map[string]any{
		"commitSequence": result.CommitSequence, "bytes": result.Bytes, "artifact": filepath.Base(artifact),
	})
	return result, nil
}

// drill restores and verifies a backup in a temporary directory and records the result.
func (scheduler *serveBackupScheduler) drill(ctx context.Context, artifact string, expected meldbase.BackupResult) error {
	createdAt := scheduler.now().UTC()
	directory, err := os.MkdirTemp(scheduler.schedule.DrillDir, ".meldbase-restore-drill-")
	if err != nil {
		return fmt.Errorf("create scheduled backup drill: %w", err)
	}
	defer os.RemoveAll(directory)
	input, err := os.Open(artifact)
	if err != nil {
		return err
	}
	defer input.Close()
	restored := filepath.Join(directory, "restored.meld")
	if _, err := meldbase.ImportPhysicalBackup(ctx, input, restored, expected, meldbase.PhysicalBackupImportOptions{MaxBytes: expected.Bytes}); err != nil {
		return fmt.Errorf("restore scheduled backup drill: %w", err)
	}
	verified, err := meldbase.VerifyFile(ctx, restored)
	if err != nil {
		return fmt.Errorf("verify scheduled backup drill: %w", err)
	}
	receiptPath := filepath.Join(scheduler.schedule.DrillDir, "meldbase-restore-drill-"+createdAt.Format("20060102T150405.000000000Z")+".json")
	receipt := struct {
		SchemaVersion int                         `json:"schemaVersion"`
		CompletedAt   time.Time                   `json:"completedAt"`
		Artifact      string                      `json:"artifact"`
		Backup        meldbase.BackupResult       `json:"backup"`
		Verification  meldbase.VerificationReport `json:"verification"`
	}{SchemaVersion: 1, CompletedAt: createdAt, Artifact: filepath.Base(artifact), Backup: expected, Verification: verified}
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(receiptPath, append(encoded, '\n'), 0o600); err != nil {
		return err
	}
	scheduler.logger.info("scheduled_backup_restore_drill_completed", map[string]any{
		"commitSequence": expected.CommitSequence, "artifact": filepath.Base(artifact),
	})
	return nil
}

// writeScheduledBackupReceipt creates a private receipt and syncs both the file and its directory.
func writeScheduledBackupReceipt(path string, result meldbase.BackupResult) error {
	encoded, err := json.MarshalIndent(backupCommandResult{SchemaVersion: 1, ArtifactKind: physicalRestoreArtifact, BackupResult: result}, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(encoded)
	if writeErr == nil && written != len(encoded) {
		writeErr = io.ErrShortWrite
	}
	writeErr = errors.Join(writeErr, file.Sync(), file.Close())
	if writeErr != nil {
		return writeErr
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

// prune removes backup pairs beyond the configured retention count and syncs the directory.
func (scheduler *serveBackupScheduler) prune() error {
	pairs, err := scheduledBackupPairs(scheduler.schedule.Directory)
	if err != nil {
		return err
	}
	for _, pair := range pairs[scheduler.schedule.Keep:] {
		if err := os.Remove(pair.artifact); err != nil {
			return fmt.Errorf("remove expired backup artifact: %w", err)
		}
		if err := os.Remove(pair.receipt); err != nil {
			return fmt.Errorf("remove expired backup receipt: %w", err)
		}
	}
	if len(pairs) > scheduler.schedule.Keep {
		directory, err := os.Open(scheduler.schedule.Directory)
		if err != nil {
			return err
		}
		return errors.Join(directory.Sync(), directory.Close())
	}
	return nil
}

// scheduledBackupPair links a backup artifact to its receipt file.
type scheduledBackupPair struct {
	artifact string
	receipt  string
}

// scheduledBackupPairs lists regular backup files with readable receipts in newest-name-first order.
func scheduledBackupPairs(directory string) ([]scheduledBackupPair, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	pairs := make([]scheduledBackupPair, 0)
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasPrefix(entry.Name(), scheduledBackupPrefix) || !strings.HasSuffix(entry.Name(), ".meld") {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		artifact := filepath.Join(directory, entry.Name())
		receipt := artifact + ".json"
		if info, err := os.Lstat(receipt); err != nil || !info.Mode().IsRegular() {
			continue
		}
		if _, err := readPhysicalBackupReceipt(receipt); err != nil {
			continue
		}
		pairs = append(pairs, scheduledBackupPair{artifact: artifact, receipt: receipt})
	}
	sort.Slice(pairs, func(left, right int) bool { return pairs[left].artifact > pairs[right].artifact })
	return pairs, nil
}
