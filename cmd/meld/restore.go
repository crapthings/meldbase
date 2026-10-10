package main

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// runRestore restores a physical backup to a new database path using its required receipt.
//
// The receipt supplies the expected database identity, file structure, and
// checksum, which are checked before publishing the restored file.
func runRestore(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("restore", flag.ContinueOnError)
	flags.SetOutput(stderr)
	source := flags.String("in", "", "existing physical restore-artifact path")
	receiptPath := flags.String("receipt", "", "JSON receipt emitted by meld backup")
	destination := flags.String("out", "", "new restored database path (must not exist)")
	timeout := flags.Duration("timeout", 0, "optional restore deadline, for example 10m")
	maxBytes := flags.Uint64("max-bytes", 0, "maximum accepted artifact bytes; 0 uses the normal storage limit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("restore does not accept positional arguments")
	}
	if *source == "" || *receiptPath == "" || *destination == "" {
		return errors.New("restore requires --in, --receipt and --out")
	}
	if *timeout < 0 {
		return errors.New("restore --timeout must not be negative")
	}

	receipt, err := readBackupReceipt(*receiptPath)
	if err != nil {
		return err
	}
	absoluteSource, err := filepath.Abs(filepath.Clean(*source))
	if err != nil {
		return err
	}
	absoluteDestination, err := filepath.Abs(filepath.Clean(*destination))
	if err != nil {
		return err
	}
	if absoluteSource == absoluteDestination {
		return errors.New("restore source and destination must be different paths")
	}
	input, err := os.Open(absoluteSource)
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
	var archive io.Reader = input
	var compressed *gzip.Reader
	if receipt.ArtifactKind == gzipPhysicalRestoreArtifact {
		if err := verifyCompressedBackupArtifact(absoluteSource, receipt); err != nil {
			_ = input.Close()
			return err
		}
		compressed, err = gzip.NewReader(input)
		if err != nil {
			_ = input.Close()
			return fmt.Errorf("invalid gzip backup artifact: %w", err)
		}
		archive = compressed
	}
	result, restoreErr := meldbase.ImportPhysicalBackup(ctx, archive, absoluteDestination, receipt.BackupResult, meldbase.PhysicalBackupImportOptions{MaxBytes: *maxBytes})
	if compressed != nil {
		restoreErr = errors.Join(restoreErr, compressed.Close())
	}
	if err := errors.Join(restoreErr, input.Close()); err != nil {
		return err
	}
	output := backupCommandResult{SchemaVersion: 1, ArtifactKind: physicalRestoreArtifact, BackupResult: result}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}

// readPhysicalBackupReceipt reads a supported receipt and requires an uncompressed physical artifact.
func readPhysicalBackupReceipt(path string) (meldbase.BackupResult, error) {
	receipt, err := readBackupReceipt(path)
	if err != nil {
		return meldbase.BackupResult{}, err
	}
	if receipt.ArtifactKind != physicalRestoreArtifact {
		return meldbase.BackupResult{}, errors.New("backup receipt is not an uncompressed physical restore artifact")
	}
	return receipt.BackupResult, nil
}

// readBackupReceipt validates the receipt format and its compression metadata.
func readBackupReceipt(path string) (backupCommandResult, error) {
	input, err := os.Open(path)
	if err != nil {
		return backupCommandResult{}, err
	}
	defer input.Close()
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	var receipt backupCommandResult
	if err := decoder.Decode(&receipt); err != nil {
		return backupCommandResult{}, fmt.Errorf("invalid backup receipt: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return backupCommandResult{}, errors.New("invalid backup receipt: multiple JSON values")
		}
		return backupCommandResult{}, fmt.Errorf("invalid backup receipt: %w", err)
	}
	if receipt.SchemaVersion != 1 {
		return backupCommandResult{}, errors.New("backup receipt is not a supported physical restore artifact")
	}
	switch receipt.ArtifactKind {
	case physicalRestoreArtifact:
		if receipt.Compression != "" || receipt.ArtifactBytes != 0 || receipt.ArtifactSHA256 != "" {
			return backupCommandResult{}, errors.New("invalid uncompressed backup receipt")
		}
	case gzipPhysicalRestoreArtifact:
		if receipt.Compression != "gzip" || receipt.ArtifactBytes == 0 || len(receipt.ArtifactSHA256) != sha256.Size*2 {
			return backupCommandResult{}, errors.New("invalid gzip backup receipt")
		}
		if _, err := hex.DecodeString(receipt.ArtifactSHA256); err != nil {
			return backupCommandResult{}, errors.New("invalid gzip backup receipt")
		}
	default:
		return backupCommandResult{}, errors.New("backup receipt is not a supported physical restore artifact")
	}
	return receipt, nil
}

// verifyCompressedBackupArtifact checks the artifact byte count and digest against its receipt.
func verifyCompressedBackupArtifact(path string, receipt backupCommandResult) error {
	bytes, digest, err := backupArtifactDigest(path)
	if err != nil {
		return err
	}
	if bytes != receipt.ArtifactBytes || digest != receipt.ArtifactSHA256 {
		return errors.New("compressed backup artifact does not match its receipt")
	}
	return nil
}
