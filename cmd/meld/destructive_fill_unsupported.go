//go:build !linux

package main

import "errors"

// destructiveFillResult records allocated space and the observed disk-full boundary.
type destructiveFillResult struct {
	Path                   string `json:"-"`
	AllocatedBytes         uint64 `json:"allocatedBytes"`
	AvailableBytesBefore   uint64 `json:"availableBytesBefore"`
	AvailableBytesAtENOSPC uint64 `json:"availableBytesAtEnospc"`
	ENOSPCOperation        string `json:"enospcOperation"`
}

// fillDestructiveVolume reports that real disk exhaustion qualification requires Linux.
func fillDestructiveVolume(string, uint64) (destructiveFillResult, error) {
	return destructiveFillResult{}, errors.New("real capacity exhaustion is supported only on Linux")
}

// destructiveAvailableBytes reports that real disk exhaustion qualification requires Linux.
func destructiveAvailableBytes(string) (uint64, error) {
	return 0, errors.New("real capacity exhaustion is supported only on Linux")
}
