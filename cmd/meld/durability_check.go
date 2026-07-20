package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/crapthings/meldbase/core"
)

type durabilityCheckResult struct {
	SchemaVersion  int                      `json:"schemaVersion"`
	SourceRevision string                   `json:"sourceRevision,omitempty"`
	Directory      string                   `json:"directory"`
	GOOS           string                   `json:"goos"`
	GOARCH         string                   `json:"goarch"`
	GoVersion      string                   `json:"goVersion"`
	BuildRevision  string                   `json:"buildRevision,omitempty"`
	BuildModified  bool                     `json:"buildModified"`
	Device         uint64                   `json:"device"`
	FilesystemType string                   `json:"filesystemType"`
	FilesystemName string                   `json:"filesystemName"`
	BlockSize      uint64                   `json:"blockSize"`
	TotalBytes     uint64                   `json:"totalBytes"`
	AvailableBytes uint64                   `json:"availableBytes"`
	StartedAt      time.Time                `json:"startedAt"`
	FinishedAt     time.Time                `json:"finishedAt"`
	Duration       time.Duration            `json:"durationNanos"`
	Passed         bool                     `json:"passed"`
	Checks         []durabilityCheckRecord  `json:"checks"`
	Database       *durabilityDatabaseProof `json:"database,omitempty"`
}

type durabilityDatabaseProof struct {
	VerificationSchema int    `json:"verificationSchemaVersion"`
	FormatRevision     uint16 `json:"formatRevision"`
	CommitSequence     uint64 `json:"commitSequence"`
	FileBytes          uint64 `json:"fileBytes"`
	PhysicalPages      uint64 `json:"physicalPages"`
	ReachablePages     uint64 `json:"reachablePages"`
	IndexVerified      bool   `json:"indexContentsVerified"`
	FreeSpaceValid     bool   `json:"freeSpaceValid"`
	SHA256             string `json:"sha256"`
}

type durabilityCheckRecord struct {
	Name     string        `json:"name"`
	Passed   bool          `json:"passed"`
	Duration time.Duration `json:"durationNanos"`
	Error    string        `json:"error,omitempty"`
}

func runDurabilityCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("durability-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", ".", "existing directory on the target database volume")
	output := flags.String("out", "", "optional new no-overwrite durable schema-2 receipt path")
	sourceRevision := flags.String("source-revision", "", "optional 40- or 64-hex source revision recorded in the receipt")
	requireCleanSource := flags.Bool("require-clean-source", false, "require the claimed revision to match clean Go VCS build metadata")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *sourceRevision != "" && !validDurabilitySourceRevision(*sourceRevision) {
		return errors.New("durability-check --source-revision must be 40 or 64 hexadecimal characters")
	}
	cleanOutput := ""
	if *output != "" {
		var err error
		cleanOutput, err = filepath.Abs(filepath.Clean(*output))
		if err != nil {
			return err
		}
		if _, err := os.Lstat(cleanOutput); err == nil {
			return fmt.Errorf("durability-check receipt already exists: %s", cleanOutput)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		parent, err := os.Stat(filepath.Dir(cleanOutput))
		if err != nil || !parent.IsDir() {
			return errors.New("durability-check receipt parent must be an existing directory")
		}
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if *requireCleanSource && (*sourceRevision == "" || buildRevision == "" || buildRevision != *sourceRevision || buildModified) {
		return errors.New("durability-check clean source verification failed")
	}
	absolute, err := filepath.Abs(filepath.Clean(*directory))
	if err != nil {
		return err
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("durability-check --dir must be a directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("durability-check cannot identify the target device")
	}
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(absolute, &filesystem); err != nil {
		return err
	}

	totalBytes, err := checkedFilesystemBytes(filesystem.Blocks, uint64(filesystem.Bsize))
	if err != nil {
		return err
	}
	availableBytes, err := checkedFilesystemBytes(filesystem.Bavail, uint64(filesystem.Bsize))
	if err != nil {
		return err
	}
	result := durabilityCheckResult{
		SchemaVersion: 2, SourceRevision: *sourceRevision, Directory: absolute, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GoVersion: runtime.Version(), BuildRevision: buildRevision, BuildModified: buildModified,
		StartedAt: time.Now(), Device: uint64(stat.Dev),
		FilesystemType: fmt.Sprintf("0x%x", filesystem.Type), FilesystemName: durabilityFilesystemName(filesystem),
		BlockSize: uint64(filesystem.Bsize), TotalBytes: totalBytes, AvailableBytes: availableBytes,
	}
	probeDirectory := ""
	runCheck := func(name string, check func() error) bool {
		started := time.Now()
		err := check()
		record := durabilityCheckRecord{Name: name, Passed: err == nil, Duration: time.Since(started)}
		if err != nil {
			record.Error = err.Error()
		}
		result.Checks = append(result.Checks, record)
		return err == nil
	}

	passed := runCheck("create-private-probe-directory", func() error {
		var err error
		probeDirectory, err = os.MkdirTemp(absolute, ".meldbase-durability-probe-")
		return err
	})
	if passed {
		passed = runCheck("file-write-and-fsync", func() error {
			path := filepath.Join(probeDirectory, "sync.bin")
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err != nil {
				return err
			}
			payload := make([]byte, 16*1024)
			for index := range payload {
				payload[index] = byte(index*31 + 7)
			}
			_, writeErr := file.Write(payload)
			syncErr := file.Sync()
			closeErr := file.Close()
			return errors.Join(writeErr, syncErr, closeErr)
		})
	}
	if passed {
		passed = runCheck("parent-directory-fsync-after-create", func() error { return syncProbeDirectory(absolute) })
	}
	if passed {
		passed = runCheck("probe-directory-fsync", func() error { return syncProbeDirectory(probeDirectory) })
	}
	if passed {
		passed = runCheck("exclusive-advisory-lock-and-close-release", func() error { return checkProbeLock(probeDirectory) })
	}
	if passed {
		passed = runCheck("atomic-no-overwrite-link", func() error { return checkProbeNoOverwriteLink(probeDirectory) })
	}
	if passed {
		passed = runCheck("same-directory-rename-and-fsync", func() error { return checkProbeRename(probeDirectory) })
	}
	if passed {
		passed = runCheck("meldbase-create-indexed-commit-reopen", func() error { return checkProbeDatabase(probeDirectory) })
	}
	if passed {
		passed = runCheck("meldbase-offline-full-verification", func() error {
			report, err := meldbase.VerifyV2File(context.Background(), filepath.Join(probeDirectory, "probe.meld"))
			if err != nil {
				return err
			}
			if !report.Verified || report.Revision != 3 || report.CommitSequence != 3 || !report.IndexContentsVerified || !report.FreeSpaceValid || len(report.SHA256) != 64 {
				return fmt.Errorf("unexpected verification report: %+v", report)
			}
			result.Database = &durabilityDatabaseProof{
				VerificationSchema: report.SchemaVersion, FormatRevision: report.Revision,
				CommitSequence: report.CommitSequence, FileBytes: report.FileBytes,
				PhysicalPages: report.PhysicalPages, ReachablePages: report.ReachablePages,
				IndexVerified: report.IndexContentsVerified, FreeSpaceValid: report.FreeSpaceValid, SHA256: report.SHA256,
			}
			return nil
		})
	}
	if probeDirectory != "" {
		cleanupPassed := runCheck("cleanup-and-parent-fsync", func() error {
			removeErr := os.RemoveAll(probeDirectory)
			syncErr := syncProbeDirectory(absolute)
			return errors.Join(removeErr, syncErr)
		})
		passed = passed && cleanupPassed
	}
	result.Passed = passed
	result.FinishedAt = time.Now()
	result.Duration = result.FinishedAt.Sub(result.StartedAt)
	if cleanOutput != "" {
		if err := writeJSONExclusiveDurable(cleanOutput, result); err != nil {
			return err
		}
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if !passed {
		return errors.New("target directory failed the durability capability check")
	}
	return nil
}

func validDurabilitySourceRevision(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func durabilityBuildIdentity() (string, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	var revision string
	modified := false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	if !validDurabilitySourceRevision(revision) {
		revision = ""
	}
	return revision, modified
}

func checkedFilesystemBytes(blocks, blockSize uint64) (uint64, error) {
	if blockSize == 0 || (blocks != 0 && blockSize > ^uint64(0)/blocks) {
		return 0, errors.New("durability-check filesystem capacity overflow")
	}
	return blocks * blockSize, nil
}

func syncProbeDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func checkProbeLock(directory string) error {
	path := filepath.Join(directory, "lock")
	first, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	firstOpen := true
	defer func() {
		if firstOpen {
			_ = first.Close()
		}
	}()
	second, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer second.Close()
	if err := syscall.Flock(int(first.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	if err := syscall.Flock(int(second.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		_ = syscall.Flock(int(second.Fd()), syscall.LOCK_UN)
		return errors.New("second independent handle acquired an exclusive lock")
	}
	// Process death closes descriptors. Verify the same kernel release path by
	// closing without an explicit LOCK_UN before the independent retry.
	if err := first.Close(); err != nil {
		return err
	}
	firstOpen = false
	if err := syscall.Flock(int(second.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("lock was not acquirable after release: %w", err)
	}
	return syscall.Flock(int(second.Fd()), syscall.LOCK_UN)
}

func checkProbeNoOverwriteLink(directory string) error {
	source := filepath.Join(directory, "link-source")
	destination := filepath.Join(directory, "link-destination")
	if err := os.WriteFile(source, []byte("meldbase-link-probe"), 0o600); err != nil {
		return err
	}
	file, err := os.Open(source)
	if err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := os.Link(source, destination); err != nil {
		return err
	}
	if err := syncProbeDirectory(directory); err != nil {
		return err
	}
	if err := os.Link(source, destination); !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("second no-overwrite link error=%v", err)
	}
	content, err := os.ReadFile(destination)
	if err != nil || string(content) != "meldbase-link-probe" {
		return fmt.Errorf("linked content=%q error=%v", content, err)
	}
	return nil
}

func checkProbeRename(directory string) error {
	source := filepath.Join(directory, "rename-source")
	destination := filepath.Join(directory, "rename-destination")
	if err := os.WriteFile(source, []byte("meldbase-rename-probe"), 0o600); err != nil {
		return err
	}
	file, err := os.Open(source)
	if err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := os.Rename(source, destination); err != nil {
		return err
	}
	if err := syncProbeDirectory(directory); err != nil {
		return err
	}
	content, err := os.ReadFile(destination)
	if err != nil || string(content) != "meldbase-rename-probe" {
		return fmt.Errorf("renamed content=%q error=%v", content, err)
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("rename source still exists: %v", err)
	}
	return nil
}

func checkProbeDatabase(directory string) error {
	path := filepath.Join(directory, "probe.meld")
	db, err := meldbase.OpenV2(path)
	if err != nil {
		return err
	}
	collection := db.Collection("probe")
	id, err := collection.InsertOne(context.Background(), meldbase.Document{"value": meldbase.String("initial")})
	if err == nil {
		err = collection.CreateIndex(context.Background(), "by_value", []meldbase.IndexField{{Field: "value", Order: 1}}, meldbase.IndexOptions{Unique: true})
	}
	if err == nil {
		_, err = collection.UpdateOne(context.Background(), meldbase.Filter{"_id": id}, meldbase.Update{"$set": map[string]any{"value": "durable"}})
	}
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	reopened, err := meldbase.OpenV2(path)
	if err != nil {
		return err
	}
	reopenedCollection := reopened.Collection("probe")
	explain, explainErr := reopenedCollection.Explain(context.Background(), meldbase.Filter{"value": "durable"})
	document, findErr := reopenedCollection.FindOne(context.Background(), meldbase.Filter{"value": "durable"})
	closeErr = reopened.Close()
	if explainErr != nil || findErr != nil || closeErr != nil {
		return errors.Join(explainErr, findErr, closeErr)
	}
	if explain.Stage != "IXSCAN" || explain.IndexName != "by_value" {
		return fmt.Errorf("reopened query plan=%+v", explain)
	}
	if actualID, ok := document.ID(); !ok || actualID != id {
		return fmt.Errorf("reopened document id=%s valid=%t", actualID, ok)
	}
	value, ok := document["value"].StringValue()
	if !ok || value != "durable" {
		return fmt.Errorf("reopened value=%q valid=%t", value, ok)
	}
	return nil
}
