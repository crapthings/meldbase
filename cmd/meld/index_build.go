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
	"strings"
	"syscall"
	"time"

	"github.com/crapthings/meldbase"
)

// indexBuildCommandSchemaVersion identifies the index build command output format.
const indexBuildCommandSchemaVersion = 1

// indexFieldFlags collects ordered index fields from repeated command-line flags.
type indexFieldFlags []meldbase.IndexField

// String formats the collected index paths and sort orders as a comma-separated list.
func (fields *indexFieldFlags) String() string {
	if fields == nil {
		return ""
	}
	parts := make([]string, len(*fields))
	for index, field := range *fields {
		parts[index] = fmt.Sprintf("%s:%d", field.Field, field.Order)
	}
	return strings.Join(parts, ",")
}

// Set appends an index path with its optional ascending or descending order suffix.
func (fields *indexFieldFlags) Set(value string) error {
	path, order := value, 1
	switch {
	case strings.HasSuffix(value, ":-1"):
		path, order = strings.TrimSuffix(value, ":-1"), -1
	case strings.HasSuffix(value, ":1"):
		path = strings.TrimSuffix(value, ":1")
	}
	if path == "" {
		return errors.New("index-build --field requires a non-empty path")
	}
	*fields = append(*fields, meldbase.IndexField{Field: path, Order: order})
	return nil
}

// indexBuildCommandResult reports the action and its index build status or outcome.
type indexBuildCommandResult struct {
	SchemaVersion int                          `json:"schemaVersion"`
	Action        string                       `json:"action"`
	BuildID       *meldbase.IndexBuildID       `json:"buildId,omitempty"`
	Build         *meldbase.IndexBuildStatus   `json:"build,omitempty"`
	Builds        *[]meldbase.IndexBuildStatus `json:"builds,omitempty"`
	Published     bool                         `json:"published,omitempty"`
	Aborted       bool                         `json:"aborted,omitempty"`
}

// runIndexBuild dispatches index build lifecycle commands.
func runIndexBuild(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: meld index-build <start|rebuild|list|resume|abort>")
	}
	switch args[0] {
	case "start":
		return runIndexBuildStart(args[1:], stdout, stderr, false)
	case "rebuild":
		return runIndexBuildStart(args[1:], stdout, stderr, true)
	case "list":
		return runIndexBuildList(args[1:], stdout, stderr)
	case "resume":
		return runIndexBuildResume(args[1:], stdout, stderr)
	case "abort":
		return runIndexBuildAbort(args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown index-build command %q", args[0])
	}
}

// runIndexBuildStart starts a new index build or rebuild and prints its initial status.
func runIndexBuildStart(args []string, stdout, stderr io.Writer, replaceExisting bool) error {
	action := "start"
	if replaceExisting {
		action = "rebuild"
	}
	flags := flag.NewFlagSet("index-build "+action, flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("db", "", "existing compatible database path")
	collection := flags.String("collection", "", "existing collection name")
	name := flags.String("name", "", "new index name")
	var fields indexFieldFlags
	flags.Var(&fields, "field", "ordered field path[:1|-1], repeat up to four times")
	unique := flags.Bool("unique", false, "enforce uniqueness at atomic publication")
	timeout := flags.Duration("timeout", 0, "optional operation deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *path == "" || *collection == "" || *name == "" || len(fields) == 0 {
		return errors.New("index-build " + action + " requires --db, --collection, --name and --field, with no positional arguments")
	}
	ctx, stop, err := indexBuildCommandContext(*timeout)
	if err != nil {
		return err
	}
	defer stop()
	db, err := openIndexBuildDatabase(*path)
	if err != nil {
		return err
	}
	var id meldbase.IndexBuildID
	var operationErr error
	if replaceExisting {
		id, operationErr = db.Collection(*collection).StartIndexRebuild(ctx, *name, []meldbase.IndexField(fields), meldbase.IndexOptions{Unique: *unique})
	} else {
		id, operationErr = db.Collection(*collection).StartIndexBuild(ctx, *name, []meldbase.IndexField(fields), meldbase.IndexOptions{Unique: *unique})
	}
	if operationErr != nil {
		return errors.Join(operationErr, db.Close())
	}
	status, operationErr := db.IndexBuild(id)
	if err := errors.Join(operationErr, db.Close()); err != nil {
		return err
	}
	return writeIndexBuildResult(stdout, indexBuildCommandResult{
		SchemaVersion: indexBuildCommandSchemaVersion, Action: action, BuildID: &id, Build: &status,
	})
}

// runIndexBuildList prints the stored index build statuses.
func runIndexBuildList(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("index-build list", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("db", "", "existing compatible database path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *path == "" {
		return errors.New("index-build list requires --db and no positional arguments")
	}
	db, err := openIndexBuildDatabase(*path)
	if err != nil {
		return err
	}
	builds, operationErr := db.IndexBuilds()
	if err := errors.Join(operationErr, db.Close()); err != nil {
		return err
	}
	if builds == nil {
		builds = []meldbase.IndexBuildStatus{}
	}
	return writeIndexBuildResult(stdout, indexBuildCommandResult{
		SchemaVersion: indexBuildCommandSchemaVersion, Action: "list", Builds: &builds,
	})
}

// runIndexBuildResume resumes a stored build through publication and prints its outcome.
func runIndexBuildResume(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("index-build resume", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("db", "", "existing compatible database path")
	idText := flags.String("id", "", "hex build ID from start/list")
	timeout := flags.Duration("timeout", 0, "optional resume deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *path == "" || *idText == "" {
		return errors.New("index-build resume requires --db and --id, with no positional arguments")
	}
	id, err := meldbase.ParseIndexBuildID(*idText)
	if err != nil {
		return err
	}
	ctx, stop, err := indexBuildCommandContext(*timeout)
	if err != nil {
		return err
	}
	defer stop()
	db, err := openIndexBuildDatabase(*path)
	if err != nil {
		return err
	}
	operationErr := db.ResumeIndexBuild(ctx, id)
	if err := errors.Join(operationErr, db.Close()); err != nil {
		return err
	}
	return writeIndexBuildResult(stdout, indexBuildCommandResult{
		SchemaVersion: indexBuildCommandSchemaVersion, Action: "resume", BuildID: &id, Published: true,
	})
}

// runIndexBuildAbort removes a stored build and prints its outcome.
func runIndexBuildAbort(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("index-build abort", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("db", "", "existing compatible database path")
	idText := flags.String("id", "", "hex build ID from start/list")
	timeout := flags.Duration("timeout", 0, "optional abort deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *path == "" || *idText == "" {
		return errors.New("index-build abort requires --db and --id, with no positional arguments")
	}
	id, err := meldbase.ParseIndexBuildID(*idText)
	if err != nil {
		return err
	}
	ctx, stop, err := indexBuildCommandContext(*timeout)
	if err != nil {
		return err
	}
	defer stop()
	db, err := openIndexBuildDatabase(*path)
	if err != nil {
		return err
	}
	operationErr := db.AbortIndexBuild(ctx, id)
	if err := errors.Join(operationErr, db.Close()); err != nil {
		return err
	}
	return writeIndexBuildResult(stdout, indexBuildCommandResult{
		SchemaVersion: indexBuildCommandSchemaVersion, Action: "abort", BuildID: &id, Aborted: true,
	})
}

// openIndexBuildDatabase opens an existing database after checking its format compatibility.
func openIndexBuildDatabase(path string) (*meldbase.DB, error) {
	info, err := meldbase.InspectStorageFormat(path)
	if err != nil {
		return nil, err
	}
	if info.Format != meldbase.StorageFormatCurrent {
		return nil, fmt.Errorf("index-build requires an existing database: %w", meldbase.ErrIndexBuildUnsupported)
	}
	if !info.ReaderCompatible {
		return nil, meldbase.ErrUnsupportedFormat
	}
	return meldbase.Open(path)
}

// indexBuildCommandContext creates a signal-aware context with an optional deadline.
func indexBuildCommandContext(timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if timeout < 0 {
		return nil, nil, errors.New("index-build --timeout must not be negative")
	}
	ctx, signalStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if timeout == 0 {
		return ctx, signalStop, nil
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	return deadlineCtx, func() {
		cancel()
		signalStop()
	}, nil
}

// writeIndexBuildResult writes an indented JSON command result.
func writeIndexBuildResult(output io.Writer, result indexBuildCommandResult) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
