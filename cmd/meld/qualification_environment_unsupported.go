//go:build !linux

package main

import "errors"

func captureQualificationEnvironment(destructiveVolumeFacts, string, string, string, uint64, string) (qualificationEnvironmentEvidence, error) {
	return qualificationEnvironmentEvidence{}, errors.New("qualification environment capture is supported only on Linux")
}
