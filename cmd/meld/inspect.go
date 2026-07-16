package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"

	"github.com/crapthings/meldbase"
)

func runInspect(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("inspect", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("db", "", "database path to inspect without opening")
	requireCompatible := flags.Bool("require-compatible", false, "fail after output unless the current reader supports the newest Meta envelope")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return errors.New("inspect requires --db")
	}
	info, err := meldbase.InspectStorageFormat(*path)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(info); err != nil {
		return err
	}
	if *requireCompatible && !info.ReaderCompatible {
		return meldbase.ErrUnsupportedFormat
	}
	return nil
}
