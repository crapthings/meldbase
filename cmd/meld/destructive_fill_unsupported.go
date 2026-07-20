//go:build !linux

package main

import "errors"

type destructiveFillResult struct {
	Path                   string `json:"-"`
	AllocatedBytes         uint64 `json:"allocatedBytes"`
	AvailableBytesBefore   uint64 `json:"availableBytesBefore"`
	AvailableBytesAtENOSPC uint64 `json:"availableBytesAtEnospc"`
	ENOSPCOperation        string `json:"enospcOperation"`
}

func fillDestructiveVolume(string, uint64) (destructiveFillResult, error) {
	return destructiveFillResult{}, errors.New("real capacity exhaustion is supported only on Linux")
}

func destructiveAvailableBytes(string) (uint64, error) {
	return 0, errors.New("real capacity exhaustion is supported only on Linux")
}
