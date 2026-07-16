package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/crapthings/meldbase/internal/qualification"
)

func runStorageSoak(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("storage-soak", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", ".", "existing directory on the exact target database volume")
	output := flags.String("out", "", "new no-overwrite schema-4 receipt path")
	profile := flags.String("profile", "custom", "custom, sentinel, or release")
	seconds := flags.Int("seconds", 1, "concurrent workload duration in seconds")
	documents := flags.Int("documents", 1_000, "documents retained and verified after every reopen")
	reopens := flags.Int("reopens", 3, "complete close/reopen verification phases")
	sourceRevision := flags.String("source-revision", "", "optional 40- or 64-hex claimed source revision; required for release")
	requireCleanSource := flags.Bool("require-clean-source", false, "require claimed revision to match clean Go VCS build metadata")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *output == "" {
		return errors.New("storage-soak requires --out")
	}
	cleanOutput := filepath.Clean(*output)
	if info, err := os.Lstat(cleanOutput); err == nil {
		return fmt.Errorf("storage-soak receipt already exists: %s (%s)", *output, info.Mode())
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	outputParent, err := filepath.Abs(filepath.Dir(cleanOutput))
	if err != nil {
		return err
	}
	if info, err := os.Stat(outputParent); err != nil {
		return fmt.Errorf("storage-soak receipt directory: %w", err)
	} else if !info.IsDir() {
		return errors.New("storage-soak receipt parent must be a directory")
	}
	absolute, err := filepath.Abs(filepath.Clean(*directory))
	if err != nil {
		return err
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("storage-soak --dir must be a directory")
	}
	volume, err := storageSoakVolume(absolute, info)
	if err != nil {
		return err
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if *requireCleanSource && (*sourceRevision == "" || buildRevision == "" || buildRevision != *sourceRevision || buildModified) {
		return errors.New("storage-soak clean source verification failed")
	}
	options := qualification.SoakOptions{
		TargetDirectory: absolute, Profile: *profile, Seconds: *seconds, Documents: *documents, Reopens: *reopens,
		SourceRevision: *sourceRevision, BuildRevision: buildRevision, BuildModified: buildModified, Volume: volume,
		Progress: func(progress qualification.SoakProgress) {
			fmt.Fprintf(stderr,
				"meld storage-soak: stage=%s phase=%d/%d elapsed=%s concurrent=%s writes=%d reads=%d index_batches=%d reclaim_attempts=%d reclaim_conflicts=%d\n",
				progress.Stage, progress.Phase, progress.TotalPhases, progress.Elapsed.Round(time.Millisecond),
				progress.ConcurrentDuration.Round(time.Millisecond), progress.Writes, progress.SnapshotReads,
				progress.IndexBuildBatches, progress.ReclamationAttempts, progress.ReclamationConflicts,
			)
		},
	}
	if err := qualification.ValidateSoakOptions(options); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	receipt, err := qualification.RunStorageSoak(ctx, options)
	if err != nil {
		return err
	}
	if err := writeStorageSoakReceipt(cleanOutput, receipt); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(receipt)
}

func storageSoakVolume(path string, info os.FileInfo) (qualification.VolumeIdentity, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return qualification.VolumeIdentity{}, errors.New("storage-soak cannot identify the target device")
	}
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(path, &filesystem); err != nil {
		return qualification.VolumeIdentity{}, err
	}
	if filesystem.Bsize <= 0 {
		return qualification.VolumeIdentity{}, errors.New("storage-soak target filesystem has an invalid block size")
	}
	return qualification.VolumeIdentity{
		Device: uint64(stat.Dev), FilesystemType: fmt.Sprintf("0x%x", filesystem.Type),
		FilesystemName: durabilityFilesystemName(filesystem), BlockSize: uint64(filesystem.Bsize),
	}, nil
}

func writeStorageSoakReceipt(path string, receipt qualification.SoakReceipt) error {
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	clean := filepath.Clean(path)
	file, err := os.OpenFile(clean, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create storage soak receipt: %w", err)
	}
	var writeErr error
	if written, err := file.Write(encoded); err != nil {
		writeErr = err
	} else if written != len(encoded) {
		writeErr = io.ErrShortWrite
	}
	writeErr = errors.Join(writeErr, file.Sync(), file.Close())
	if writeErr == nil {
		parent, err := filepath.Abs(filepath.Dir(clean))
		if err == nil {
			directory, openErr := os.Open(parent)
			if openErr != nil {
				err = openErr
			} else {
				err = errors.Join(directory.Sync(), directory.Close())
			}
		}
		writeErr = err
	}
	if writeErr != nil {
		removeErr := os.Remove(clean)
		var cleanupSyncErr error
		if directory, err := os.Open(filepath.Dir(clean)); err != nil {
			cleanupSyncErr = err
		} else {
			cleanupSyncErr = errors.Join(directory.Sync(), directory.Close())
		}
		return errors.Join(fmt.Errorf("publish storage soak receipt: %w", writeErr), removeErr, cleanupSyncErr)
	}
	return nil
}
