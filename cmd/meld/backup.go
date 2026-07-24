package main

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/crapthings/meldbase"
)

const physicalRestoreArtifact = "physical-restore"
const gzipPhysicalRestoreArtifact = "physical-restore-gzip"

type backupCommandResult struct {
	SchemaVersion  int    `json:"schemaVersion"`
	ArtifactKind   string `json:"artifactKind"`
	Compression    string `json:"compression,omitempty"`
	ArtifactBytes  uint64 `json:"artifactBytes,omitempty"`
	ArtifactSHA256 string `json:"artifactSha256,omitempty"`
	meldbase.BackupResult
}

func runBackup(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("backup", flag.ContinueOnError)
	flags.SetOutput(stderr)
	source := flags.String("db", "", "existing current-format database path")
	destination := flags.String("out", "", "new physical restore-artifact path (must not exist)")
	compression := flags.String("compression", "none", "artifact compression: none or gzip")
	timeout := flags.Duration("timeout", 0, "optional backup deadline, for example 10m")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("backup does not accept positional arguments")
	}
	if *source == "" || *destination == "" {
		return errors.New("backup requires --db and --out")
	}
	if *timeout < 0 {
		return errors.New("backup --timeout must not be negative")
	}
	if *compression != "none" && *compression != "gzip" {
		return errors.New("backup --compression must be none or gzip")
	}

	info, err := meldbase.InspectStorageFormat(*source)
	if err != nil {
		return err
	}
	if info.Format != meldbase.StorageFormatCurrent {
		return fmt.Errorf("backup source must be an existing current-format database: %w", meldbase.ErrBackupUnsupported)
	}
	if !info.ReaderCompatible {
		return meldbase.ErrUnsupportedFormat
	}

	db, err := meldbase.Open(*source)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	var result meldbase.BackupResult
	var artifactBytes uint64
	var artifactSHA256 string
	var backupErr error
	artifactKind := physicalRestoreArtifact
	if *compression == "gzip" {
		result, artifactBytes, artifactSHA256, backupErr = backupGzip(ctx, db, *destination)
		artifactKind = gzipPhysicalRestoreArtifact
	} else {
		result, backupErr = db.Backup(ctx, *destination)
	}
	if err := errors.Join(backupErr, db.Close()); err != nil {
		return err
	}
	output := backupCommandResult{SchemaVersion: 1, ArtifactKind: artifactKind, BackupResult: result}
	if *compression == "gzip" {
		output.Compression, output.ArtifactBytes, output.ArtifactSHA256 = "gzip", artifactBytes, artifactSHA256
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}

// backupGzip keeps the durable backup contract intact: the database first
// creates and verifies an ordinary physical artifact, then this command writes
// one compressed, no-overwrite representation of that exact file. Compression
// reduces backup storage/transfer cost but does not weaken the restore-side
// physical digest and graph verification.
func backupGzip(ctx context.Context, db *meldbase.DB, destination string) (meldbase.BackupResult, uint64, string, error) {
	if destination == "" {
		return meldbase.BackupResult{}, 0, "", errors.New("meldbase: empty backup destination")
	}
	absoluteDestination, err := filepath.Abs(filepath.Clean(destination))
	if err != nil {
		return meldbase.BackupResult{}, 0, "", err
	}
	if _, err := os.Lstat(absoluteDestination); err == nil {
		return meldbase.BackupResult{}, 0, "", meldbase.ErrBackupDestinationExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return meldbase.BackupResult{}, 0, "", err
	}
	directory := filepath.Dir(absoluteDestination)
	intermediate, err := os.CreateTemp(directory, "."+filepath.Base(absoluteDestination)+".physical-*")
	if err != nil {
		return meldbase.BackupResult{}, 0, "", err
	}
	intermediatePath := intermediate.Name()
	if err := intermediate.Close(); err != nil {
		_ = os.Remove(intermediatePath)
		return meldbase.BackupResult{}, 0, "", err
	}
	if err := os.Remove(intermediatePath); err != nil {
		return meldbase.BackupResult{}, 0, "", err
	}
	defer os.Remove(intermediatePath)

	result, err := db.Backup(ctx, intermediatePath)
	if err != nil {
		return meldbase.BackupResult{}, 0, "", err
	}
	input, err := os.Open(intermediatePath)
	if err != nil {
		return meldbase.BackupResult{}, 0, "", err
	}
	defer input.Close()
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(absoluteDestination)+".gzip-*")
	if err != nil {
		return meldbase.BackupResult{}, 0, "", err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	compressor := gzip.NewWriter(temporary)
	_, copyErr := io.Copy(compressor, input)
	closeCompressorErr := compressor.Close()
	syncErr := temporary.Sync()
	closeErr := temporary.Close()
	if err := errors.Join(copyErr, closeCompressorErr, syncErr, closeErr); err != nil {
		return meldbase.BackupResult{}, 0, "", err
	}
	artifactBytes, artifactDigest, err := backupArtifactDigest(temporaryPath)
	if err != nil {
		return meldbase.BackupResult{}, 0, "", err
	}
	if err := publishBackupArtifact(temporaryPath, absoluteDestination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return meldbase.BackupResult{}, 0, "", meldbase.ErrBackupDestinationExists
		}
		return meldbase.BackupResult{}, 0, "", err
	}
	return result, artifactBytes, artifactDigest, nil
}

func backupArtifactDigest(path string) (uint64, string, error) {
	input, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer input.Close()
	hash := sha256.New()
	bytes, err := io.Copy(hash, input)
	if err != nil {
		return 0, "", err
	}
	return uint64(bytes), fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// publishBackupArtifact gives compressed artifacts the same no-overwrite
// publication point as the physical backup implementation. os.Link is used
// instead of rename so a raced destination cannot be replaced.
func publishBackupArtifact(temporary, destination string) error {
	if err := os.Link(temporary, destination); err != nil {
		return err
	}
	if directory, err := os.Open(filepath.Dir(destination)); err == nil {
		defer directory.Close()
		if err := directory.Sync(); err != nil {
			_ = os.Remove(destination)
			return err
		}
	}
	return nil
}
