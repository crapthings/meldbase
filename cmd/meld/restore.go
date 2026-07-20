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

	"github.com/crapthings/meldbase/core"
)

// runRestore imports exactly one physical backup. The receipt is required:
// it binds the input's expected identity, shape and digest before the artifact
// is published at its new path.
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

	expected, err := readPhysicalBackupReceipt(*receiptPath)
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
	result, restoreErr := meldbase.ImportPhysicalBackup(ctx, input, absoluteDestination, expected, meldbase.PhysicalBackupImportOptions{MaxBytes: *maxBytes})
	if err := errors.Join(restoreErr, input.Close()); err != nil {
		return err
	}
	output := backupCommandResult{SchemaVersion: 1, ArtifactKind: physicalRestoreArtifact, BackupResult: result}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}

func readPhysicalBackupReceipt(path string) (meldbase.BackupResult, error) {
	input, err := os.Open(path)
	if err != nil {
		return meldbase.BackupResult{}, err
	}
	defer input.Close()
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	var receipt backupCommandResult
	if err := decoder.Decode(&receipt); err != nil {
		return meldbase.BackupResult{}, fmt.Errorf("invalid backup receipt: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return meldbase.BackupResult{}, errors.New("invalid backup receipt: multiple JSON values")
		}
		return meldbase.BackupResult{}, fmt.Errorf("invalid backup receipt: %w", err)
	}
	if receipt.SchemaVersion != 1 || receipt.ArtifactKind != physicalRestoreArtifact {
		return meldbase.BackupResult{}, errors.New("backup receipt is not a supported physical restore artifact")
	}
	return receipt.BackupResult, nil
}
