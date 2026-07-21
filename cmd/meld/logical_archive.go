package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/crapthings/meldbase/core"
)

const logicalArchiveArtifact = "logical-archive"

type logicalArchiveCommandResult struct {
	SchemaVersion int    `json:"schemaVersion"`
	ArtifactKind  string `json:"artifactKind"`
	meldbase.LogicalArchiveResult
}

func runLogicalExport(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("export", flag.ContinueOnError)
	flags.SetOutput(stderr)
	source := flags.String("db", "", "existing current-format database path")
	destination := flags.String("out", "", "new logical archive path (must not exist)")
	timeout := flags.Duration("timeout", 0, "optional export deadline, for example 10m")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("export does not accept positional arguments")
	}
	if *source == "" || *destination == "" {
		return errors.New("export requires --db and --out")
	}
	if *timeout < 0 {
		return errors.New("export --timeout must not be negative")
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
	result, exportErr := db.ExportLogicalArchive(ctx, *destination)
	if err := errors.Join(exportErr, db.Close()); err != nil {
		return err
	}
	return writeLogicalArchiveResult(stdout, result)
}

func runLogicalImport(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("import", flag.ContinueOnError)
	flags.SetOutput(stderr)
	source := flags.String("in", "", "existing logical archive path")
	destination := flags.String("out", "", "new database path (must not exist)")
	timeout := flags.Duration("timeout", 0, "optional import deadline, for example 10m")
	maxBytes := flags.Uint64("max-bytes", 0, "maximum accepted archive bytes; 0 uses the normal storage limit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("import does not accept positional arguments")
	}
	if *source == "" || *destination == "" {
		return errors.New("import requires --in and --out")
	}
	if *timeout < 0 {
		return errors.New("import --timeout must not be negative")
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
		return errors.New("import source and destination must be different paths")
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
	result, importErr := meldbase.ImportLogicalArchive(ctx, input, absoluteDestination, meldbase.LogicalArchiveImportOptions{MaxBytes: *maxBytes})
	if err := errors.Join(importErr, input.Close()); err != nil {
		return err
	}
	return writeLogicalArchiveResult(stdout, result)
}

func writeLogicalArchiveResult(output io.Writer, result meldbase.LogicalArchiveResult) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(logicalArchiveCommandResult{SchemaVersion: 1, ArtifactKind: logicalArchiveArtifact, LogicalArchiveResult: result})
}
