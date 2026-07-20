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
	"syscall"

	"github.com/crapthings/meldbase/core"
)

const physicalV2RestoreArtifact = "physical-v2-restore"

type backupCommandResult struct {
	SchemaVersion int    `json:"schemaVersion"`
	ArtifactKind  string `json:"artifactKind"`
	meldbase.BackupV2Result
}

func runBackup(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("backup", flag.ContinueOnError)
	flags.SetOutput(stderr)
	source := flags.String("db", "", "existing compatible V2 database path")
	destination := flags.String("out", "", "new physical restore-artifact path (must not exist)")
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

	info, err := meldbase.InspectStorageFormat(*source)
	if err != nil {
		return err
	}
	if info.Format != meldbase.StorageFormatCurrent {
		return fmt.Errorf("backup source must be an existing V2 database: %w", meldbase.ErrBackupUnsupported)
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
	result, backupErr := db.BackupV2(ctx, *destination)
	if err := errors.Join(backupErr, db.Close()); err != nil {
		return err
	}
	output := backupCommandResult{SchemaVersion: 1, ArtifactKind: physicalV2RestoreArtifact, BackupV2Result: result}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}
