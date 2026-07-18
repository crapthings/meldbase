package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

const (
	destructiveVolumeSchema    uint32 = 1
	destructiveVolumeMinBytes         = uint64(64 << 20)
	destructiveVolumeMaxBytes         = uint64(16 << 30)
	destructiveControlMinBytes        = uint64(64 << 20)
)

type destructiveVolumeReceipt struct {
	SchemaVersion         uint32 `json:"schemaVersion"`
	Eligible              bool   `json:"eligible"`
	Directory             string `json:"directory"`
	GOOS                  string `json:"goos"`
	Device                uint64 `json:"device"`
	ControlDevice         uint64 `json:"controlDevice"`
	FilesystemType        string `json:"filesystemType"`
	FilesystemName        string `json:"filesystemName"`
	BlockSize             uint64 `json:"blockSize"`
	TotalBytes            uint64 `json:"totalBytes"`
	AvailableBytes        uint64 `json:"availableBytes"`
	ControlAvailableBytes uint64 `json:"controlAvailableBytes"`
	DestructiveToken      string `json:"destructiveToken"`
}

type destructiveVolumeFacts struct {
	directory, controlDirectory      string
	filesystemType, filesystemName   string
	device, parentDevice, workDevice uint64
	controlDevice, blockSize         uint64
	totalBytes, availableBytes       uint64
	controlAvailableBytes            uint64
	effectiveUID                     int
	mountRoot, empty                 bool
}

func runDestructiveVolumeCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-volume-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", "", "root of the disposable mounted target volume")
	controlDirectory := flags.String("control-dir", "", "existing oracle/receipt directory on a different device")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *directory == "" || *controlDirectory == "" {
		return errors.New("destructive-volume-check requires --dir and --control-dir")
	}
	facts, err := inspectDestructiveVolume(*directory, *controlDirectory)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolumeFacts(facts); err != nil {
		return err
	}
	receipt := destructiveVolumeReceipt{
		SchemaVersion: destructiveVolumeSchema, Eligible: true, Directory: facts.directory, GOOS: runtime.GOOS,
		Device: facts.device, ControlDevice: facts.controlDevice, FilesystemType: facts.filesystemType,
		FilesystemName: facts.filesystemName, BlockSize: facts.blockSize,
		TotalBytes: facts.totalBytes, AvailableBytes: facts.availableBytes, ControlAvailableBytes: facts.controlAvailableBytes,
		DestructiveToken: destructiveVolumeToken(facts),
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(receipt)
}

func inspectDestructiveVolume(directory, controlDirectory string) (destructiveVolumeFacts, error) {
	if runtime.GOOS != "linux" {
		return destructiveVolumeFacts{}, errors.New("destructive volume qualification is supported only on Linux")
	}
	target, targetInfo, err := resolvedDirectory(directory)
	if err != nil {
		return destructiveVolumeFacts{}, fmt.Errorf("target directory: %w", err)
	}
	control, controlInfo, err := resolvedDirectory(controlDirectory)
	if err != nil {
		return destructiveVolumeFacts{}, fmt.Errorf("control directory: %w", err)
	}
	work, workInfo, err := resolvedDirectory(".")
	if err != nil {
		return destructiveVolumeFacts{}, fmt.Errorf("working directory: %w", err)
	}
	_ = work
	targetStat, ok := targetInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return destructiveVolumeFacts{}, errors.New("target device identity unavailable")
	}
	controlStat, ok := controlInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return destructiveVolumeFacts{}, errors.New("control device identity unavailable")
	}
	workStat, ok := workInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return destructiveVolumeFacts{}, errors.New("working device identity unavailable")
	}
	parentInfo, err := os.Stat(filepath.Dir(target))
	if err != nil {
		return destructiveVolumeFacts{}, err
	}
	parentStat, ok := parentInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return destructiveVolumeFacts{}, errors.New("target parent device identity unavailable")
	}
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(target, &filesystem); err != nil {
		return destructiveVolumeFacts{}, err
	}
	totalBytes, err := checkedFilesystemBytes(filesystem.Blocks, uint64(filesystem.Bsize))
	if err != nil {
		return destructiveVolumeFacts{}, err
	}
	availableBytes, err := checkedFilesystemBytes(filesystem.Bavail, uint64(filesystem.Bsize))
	if err != nil {
		return destructiveVolumeFacts{}, err
	}
	var controlFilesystem syscall.Statfs_t
	if err := syscall.Statfs(control, &controlFilesystem); err != nil {
		return destructiveVolumeFacts{}, err
	}
	controlAvailableBytes, err := checkedFilesystemBytes(controlFilesystem.Bavail, uint64(controlFilesystem.Bsize))
	if err != nil {
		return destructiveVolumeFacts{}, err
	}
	mountRoot, err := destructivePathIsMountRoot(target)
	if err != nil {
		return destructiveVolumeFacts{}, err
	}
	empty, err := destructiveTargetIsEmpty(target)
	if err != nil {
		return destructiveVolumeFacts{}, err
	}
	return destructiveVolumeFacts{
		directory: target, controlDirectory: control, device: uint64(targetStat.Dev), parentDevice: uint64(parentStat.Dev),
		workDevice: uint64(workStat.Dev), controlDevice: uint64(controlStat.Dev), effectiveUID: os.Geteuid(), mountRoot: mountRoot, empty: empty,
		filesystemType: fmt.Sprintf("0x%x", filesystem.Type), filesystemName: durabilityFilesystemName(filesystem),
		blockSize: uint64(filesystem.Bsize), totalBytes: totalBytes, availableBytes: availableBytes, controlAvailableBytes: controlAvailableBytes,
	}, nil
}

func resolvedDirectory(path string) (string, os.FileInfo, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", nil, err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", nil, err
	}
	if !info.IsDir() {
		return "", nil, errors.New("path must be a directory")
	}
	return filepath.Clean(resolved), info, nil
}

func destructiveTargetIsEmpty(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.Name() == "lost+found" && entry.IsDir() {
			continue
		}
		return false, nil
	}
	return true, nil
}

func validateDestructiveVolumeFacts(facts destructiveVolumeFacts) error {
	if facts.directory == "" || facts.controlDirectory == "" || !filepath.IsAbs(facts.directory) || !filepath.IsAbs(facts.controlDirectory) || facts.device == 0 || facts.controlDevice == 0 ||
		facts.blockSize == 0 || facts.totalBytes == 0 || facts.availableBytes > facts.totalBytes {
		return errors.New("destructive target identity or geometry is invalid")
	}
	if !facts.mountRoot || facts.device == facts.parentDevice {
		return errors.New("destructive target must be the root of an independently mounted device")
	}
	if facts.device == facts.workDevice {
		return errors.New("destructive target is on the current workspace device")
	}
	if facts.device == facts.controlDevice {
		return errors.New("destructive target and control directory must be on different devices")
	}
	if facts.controlAvailableBytes < destructiveControlMinBytes {
		return fmt.Errorf("destructive control directory requires at least %d available bytes", destructiveControlMinBytes)
	}
	if facts.effectiveUID == 0 {
		return errors.New("destructive capacity runner must execute as a non-root user so reserved blocks cannot mask ENOSPC")
	}
	if !facts.empty {
		return errors.New("destructive target must be empty except for lost+found")
	}
	if facts.totalBytes < destructiveVolumeMinBytes || facts.totalBytes > destructiveVolumeMaxBytes {
		return fmt.Errorf("destructive target capacity must be between %d and %d bytes", destructiveVolumeMinBytes, destructiveVolumeMaxBytes)
	}
	if !qualificationProductionFilesystem(facts.filesystemName) {
		return errors.New("destructive target filesystem is not in the production qualification matrix")
	}
	return nil
}

func destructiveVolumeToken(facts destructiveVolumeFacts) string {
	payload := fmt.Sprintf("meldbase-real-enospc-v1\n%s\n%d\n%s\n%s\n%d\n%d\n",
		facts.directory, facts.device, facts.filesystemType, facts.filesystemName, facts.blockSize, facts.totalBytes)
	digest := sha256.Sum256([]byte(payload))
	return "meldbase-enospc-" + hex.EncodeToString(digest[:12])
}
