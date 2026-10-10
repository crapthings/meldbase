package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/crapthings/meldbase"
)

// serviceResourceLimitsMaxBytes limits the size of a resource limits configuration file.
const serviceResourceLimitsMaxBytes = 64 << 10

// readServiceResourceLimits reads one size-bounded resource limits JSON value and rejects unknown fields.
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
