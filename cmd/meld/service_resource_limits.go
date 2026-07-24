package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/crapthings/meldbase"
)

const serviceResourceLimitsMaxBytes = 64 << 10

// readServiceResourceLimits deliberately accepts only the public
// ResourceLimits JSON shape. This lets an operator tune one immutable set of
// admission budgets at process start without turning individual CLI flags into
// a partially applied, hard-to-audit configuration surface. Zero fields retain
// Meldbase's production defaults and the database validates cross-field
// constraints before it opens the file.
func readServiceResourceLimits(path string) (meldbase.ResourceLimits, error) {
	info, err := os.Stat(path)
	if err != nil {
		return meldbase.ResourceLimits{}, fmt.Errorf("read resource limits: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > serviceResourceLimitsMaxBytes {
		return meldbase.ResourceLimits{}, errors.New("read resource limits: file must be regular and at most 65536 bytes")
	}
	input, err := os.Open(path)
	if err != nil {
		return meldbase.ResourceLimits{}, fmt.Errorf("read resource limits: %w", err)
	}
	defer input.Close()
	decoder := json.NewDecoder(io.LimitReader(input, serviceResourceLimitsMaxBytes+1))
	decoder.DisallowUnknownFields()
	var limits meldbase.ResourceLimits
	if err := decoder.Decode(&limits); err != nil {
		return meldbase.ResourceLimits{}, fmt.Errorf("parse resource limits: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return meldbase.ResourceLimits{}, errors.New("parse resource limits: multiple JSON values")
		}
		return meldbase.ResourceLimits{}, fmt.Errorf("parse resource limits: %w", err)
	}
	return limits, nil
}
