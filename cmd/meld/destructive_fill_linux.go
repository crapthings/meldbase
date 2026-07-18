//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type destructiveFillResult struct {
	Path                   string `json:"-"`
	AllocatedBytes         uint64 `json:"allocatedBytes"`
	AvailableBytesBefore   uint64 `json:"availableBytesBefore"`
	AvailableBytesAtENOSPC uint64 `json:"availableBytesAtEnospc"`
	ENOSPCOperation        string `json:"enospcOperation"`
}

func fillDestructiveVolume(directory string, blockSize uint64) (result destructiveFillResult, resultErr error) {
	if blockSize == 0 || blockSize > 1<<20 {
		return result, errors.New("invalid destructive fill block size")
	}
	before, err := destructiveAvailableBytes(directory)
	if err != nil {
		return result, err
	}
	result.AvailableBytesBefore = before
	file, err := os.OpenFile(filepath.Join(directory, ".meldbase-real-enospc-fill"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return result, err
	}
	result.Path = file.Name()
	defer func() {
		closeErr := file.Close()
		if resultErr != nil {
			removeErr := os.Remove(result.Path)
			syncErr := syncProbeDirectory(directory)
			resultErr = errors.Join(resultErr, closeErr, removeErr, syncErr)
			result.Path = ""
			return
		}
		resultErr = errors.Join(resultErr, closeErr)
	}()

	chunk := uint64(256 << 20)
	if chunk > before {
		chunk = before - before%blockSize
	}
	if chunk < blockSize {
		chunk = blockSize
	}
	for attempts := 0; attempts < 1_000_000; attempts++ {
		if result.AllocatedBytes > uint64(^uint64(0)>>1) || chunk > uint64(^uint64(0)>>1)-result.AllocatedBytes {
			return result, errors.New("destructive fill offset overflow")
		}
		err := syscall.Fallocate(int(file.Fd()), 0, int64(result.AllocatedBytes), int64(chunk))
		if err == nil {
			result.AllocatedBytes += chunk
			continue
		}
		if !errors.Is(err, syscall.ENOSPC) {
			return result, fmt.Errorf("real capacity allocation: %w", err)
		}
		if chunk > blockSize {
			chunk /= 2
			chunk -= chunk % blockSize
			if chunk < blockSize {
				chunk = blockSize
			}
			continue
		}
		result.ENOSPCOperation = "fallocate"
		if syncErr := file.Sync(); syncErr != nil && !errors.Is(syncErr, syscall.ENOSPC) {
			return result, syncErr
		}
		result.AvailableBytesAtENOSPC, err = destructiveAvailableBytes(directory)
		if err != nil {
			return result, err
		}
		return result, nil
	}
	return result, errors.New("destructive fill exceeded allocation attempt limit")
}

func destructiveAvailableBytes(directory string) (uint64, error) {
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(directory, &filesystem); err != nil {
		return 0, err
	}
	return checkedFilesystemBytes(filesystem.Bavail, uint64(filesystem.Bsize))
}
