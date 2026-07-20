// Package qualification contains operational release-evidence runners. It is
// separate from storage tests so receipts are produced by the same VCS-stamped
// executable that performs the work.
package qualification

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	storage "github.com/crapthings/meldbase/internal/storage"
)

const SoakReceiptSchemaVersion uint32 = 4

// The operational soak is a duration and recovery qualification, not a storage
// throughput benchmark. A fixed write cadence keeps its physical churn
// portable across machines and below the engine's normal 8 GiB safety quota.
// Release runs begin unthrottled only until the optimistic auditor observes one
// real concurrent-publication conflict; the remaining work uses this cadence.
const soakWriteInterval = 10 * time.Second
const soakIndexCatchUpInterval = 30 * time.Second

type SoakProgressStage string

const (
	SoakProgressStarted         SoakProgressStage = "started"
	SoakProgressPhaseRunning    SoakProgressStage = "phase_running"
	SoakProgressPhaseVerifying  SoakProgressStage = "phase_verifying"
	SoakProgressPhaseVerified   SoakProgressStage = "phase_verified"
	SoakProgressShadowVerifying SoakProgressStage = "shadow_verifying"
	SoakProgressShadowVerified  SoakProgressStage = "shadow_verified"
	SoakProgressFinalVerifying  SoakProgressStage = "final_verifying"
	SoakProgressComplete        SoakProgressStage = "complete"
)

type SoakProgress struct {
	Stage                SoakProgressStage
	Phase                int
	TotalPhases          int
	Elapsed              time.Duration
	ConcurrentDuration   time.Duration
	Writes               uint64
	SnapshotReads        uint64
	IndexBuildBatches    uint64
	ReclamationAttempts  uint64
	ReclamationConflicts uint64
}

type VolumeIdentity struct {
	Device         uint64
	FilesystemType string
	FilesystemName string
	BlockSize      uint64
}

type SoakOptions struct {
	TargetDirectory  string
	Profile          string
	Seconds          int
	Documents        int
	Reopens          int
	SourceRevision   string
	BuildRevision    string
	BuildModified    bool
	Volume           VolumeIdentity
	ProgressInterval time.Duration
	Progress         func(SoakProgress)
}

type SoakPhaseReceipt struct {
	Ordinal              int           `json:"ordinal"`
	Duration             time.Duration `json:"durationNanos"`
	ConcurrentDuration   time.Duration `json:"concurrentDurationNanos"`
	Writes               uint64        `json:"writes"`
	SnapshotReads        uint64        `json:"snapshotReads"`
	IndexBuildBatches    uint64        `json:"indexBuildBatches"`
	ReclamationAttempts  uint64        `json:"reclamationAttempts"`
	ReclamationConflicts uint64        `json:"reclamationConflicts"`
	CommitSequence       uint64        `json:"commitSequence"`
	PhysicalPages        uint64        `json:"physicalPages"`
	ReusablePages        uint64        `json:"reusablePages"`
	IndexBuildPhase      uint8         `json:"indexBuildPhase"`
}

type SoakReceipt struct {
	SchemaVersion         uint32             `json:"schemaVersion"`
	FormatRevision        uint16             `json:"formatRevision"`
	Engine                string             `json:"engine"`
	Profile               string             `json:"profile"`
	RaceEnabled           bool               `json:"raceEnabled"`
	GOOS                  string             `json:"goos"`
	GOARCH                string             `json:"goarch"`
	GoVersion             string             `json:"goVersion"`
	SourceRevision        string             `json:"sourceRevision,omitempty"`
	BuildRevision         string             `json:"buildRevision,omitempty"`
	BuildModified         bool               `json:"buildModified"`
	Device                uint64             `json:"device"`
	FilesystemType        string             `json:"filesystemType"`
	FilesystemName        string             `json:"filesystemName"`
	BlockSize             uint64             `json:"blockSize"`
	StartedAt             time.Time          `json:"startedAt"`
	FinishedAt            time.Time          `json:"finishedAt"`
	RequestedSeconds      int                `json:"requestedSeconds"`
	ConcurrentDuration    time.Duration      `json:"concurrentDurationNanos"`
	ActualDuration        time.Duration      `json:"actualDurationNanos"`
	Documents             int                `json:"documents"`
	RequestedReopens      int                `json:"requestedReopens"`
	CompletedReopens      int                `json:"completedReopens"`
	Writes                uint64             `json:"writes"`
	SnapshotReads         uint64             `json:"snapshotReads"`
	IndexBuildBatches     uint64             `json:"indexBuildBatches"`
	ReclamationAttempts   uint64             `json:"reclamationAttempts"`
	ReclamationConflicts  uint64             `json:"reclamationConflicts"`
	FinalCommitSequence   uint64             `json:"finalCommitSequence"`
	FinalFileBytes        uint64             `json:"finalFileBytes"`
	FinalPhysicalPages    uint64             `json:"finalPhysicalPages"`
	FinalReachablePages   uint64             `json:"finalReachablePages"`
	FinalReclaimablePages uint64             `json:"finalReclaimablePages"`
	FinalFileSHA256       string             `json:"finalFileSha256"`
	PersistentFreeSpace   bool               `json:"persistentFreeSpace"`
	FreeSpaceValid        bool               `json:"freeSpaceValid"`
	SemanticIndexes       bool               `json:"semanticIndexesVerified"`
	SemanticIndexBuilds   bool               `json:"semanticIndexBuildsVerified"`
	FinalIndexBuildAbsent bool               `json:"finalIndexBuildAbsent"`
	Phases                []SoakPhaseReceipt `json:"phases"`
}

type soakActivity struct {
	Writes               uint64
	SnapshotReads        uint64
	IndexBuildBatches    uint64
	ReclamationAttempts  uint64
	ReclamationConflicts uint64
}

func ValidateSoakOptions(options SoakOptions) error {
	if options.Profile != "custom" && options.Profile != "sentinel" && options.Profile != "release" {
		return fmt.Errorf("unknown storage soak profile %q", options.Profile)
	}
	if options.Seconds < 1 || options.Seconds > 6*60*60 || options.Documents < 100 || options.Documents > 1_000_000 ||
		options.Reopens < 1 || options.Reopens > 1_000 {
		return errors.New("storage soak requires 1 second..6 hours, 100..1,000,000 documents and 1..1,000 reopens")
	}
	if options.Profile == "release" && (options.Seconds < 4*60*60 || options.Documents < 10_000 || options.Reopens < 12) {
		return errors.New("release storage soak requires at least 4 hours, 10,000 documents and 12 reopens")
	}
	if options.SourceRevision != "" && !validRevision(options.SourceRevision) {
		return errors.New("storage soak source revision must be 40 or 64 hexadecimal characters")
	}
	if options.BuildRevision != "" && !validRevision(options.BuildRevision) {
		return errors.New("storage soak build revision must be 40 or 64 hexadecimal characters")
	}
	if options.Profile == "release" && (!RaceEnabled || options.SourceRevision == "" || options.BuildRevision != options.SourceRevision || options.BuildModified) {
		return errors.New("release storage soak requires a race-enabled clean binary built from the claimed source revision")
	}
	if options.TargetDirectory == "" || options.Volume.Device == 0 || options.Volume.FilesystemType == "" ||
		options.Volume.FilesystemName == "" || options.Volume.BlockSize == 0 {
		return errors.New("storage soak requires a complete target-volume identity")
	}
	if options.ProgressInterval != 0 && (options.ProgressInterval < 100*time.Millisecond || options.ProgressInterval > 5*time.Minute) {
		return errors.New("storage soak progress interval must be between 100ms and 5m")
	}
	return nil
}

func RunStorageSoak(ctx context.Context, options SoakOptions) (_ SoakReceipt, resultErr error) {
	if ctx == nil {
		return SoakReceipt{}, errors.New("storage soak context is required")
	}
	if err := ValidateSoakOptions(options); err != nil {
		return SoakReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return SoakReceipt{}, err
	}
	probeDirectory, err := os.MkdirTemp(options.TargetDirectory, ".meldbase-storage-soak-")
	if err != nil {
		return SoakReceipt{}, err
	}
	cleanupRequired := true
	defer func() {
		if cleanupRequired {
			resultErr = errors.Join(resultErr, os.RemoveAll(probeDirectory), syncDirectory(options.TargetDirectory))
		}
	}()
	if err := syncDirectory(options.TargetDirectory); err != nil {
		return SoakReceipt{}, fmt.Errorf("sync target directory after soak create: %w", err)
	}

	path := filepath.Join(probeDirectory, "online-maintenance-soak.meld2")
	file, keys, transactionOrdinal, err := createSoakDatabase(path, options.Documents)
	if err != nil {
		return SoakReceipt{}, err
	}
	defer func() {
		if file != nil {
			resultErr = errors.Join(resultErr, file.Close())
		}
	}()
	buildID := [16]byte{0xfa, 15: 1}
	if _, err := file.BeginIndexBuild(storage.BeginIndexBuildTransaction{
		BuildID: buildID, Collection: "items", Name: "by_shadow", FieldPath: "key",
	}); err != nil {
		return SoakReceipt{}, err
	}

	started := time.Now()
	startedAt := started.UTC()
	requestedConcurrent := time.Duration(options.Seconds) * time.Second
	receipt := SoakReceipt{
		SchemaVersion: SoakReceiptSchemaVersion, FormatRevision: storage.FormatVersion, Engine: "current", Profile: options.Profile,
		RaceEnabled: RaceEnabled, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		SourceRevision: options.SourceRevision, BuildRevision: options.BuildRevision, BuildModified: options.BuildModified,
		Device: options.Volume.Device, FilesystemType: options.Volume.FilesystemType,
		FilesystemName: options.Volume.FilesystemName, BlockSize: options.Volume.BlockSize,
		StartedAt: startedAt, RequestedSeconds: options.Seconds, Documents: options.Documents,
		RequestedReopens: options.Reopens, Phases: make([]SoakPhaseReceipt, 0, options.Reopens),
	}
	progressInterval := options.ProgressInterval
	if progressInterval == 0 {
		progressInterval = 30 * time.Second
	}
	emitProgress := func(stage SoakProgressStage, phase int, concurrentExtra time.Duration, activity soakActivity) {
		if options.Progress == nil {
			return
		}
		options.Progress(SoakProgress{
			Stage: stage, Phase: phase, TotalPhases: options.Reopens, Elapsed: time.Since(started),
			ConcurrentDuration: receipt.ConcurrentDuration + concurrentExtra,
			Writes:             receipt.Writes + activity.Writes, SnapshotReads: receipt.SnapshotReads + activity.SnapshotReads,
			IndexBuildBatches:    receipt.IndexBuildBatches + activity.IndexBuildBatches,
			ReclamationAttempts:  receipt.ReclamationAttempts + activity.ReclamationAttempts,
			ReclamationConflicts: receipt.ReclamationConflicts + activity.ReclamationConflicts,
		})
	}
	emitProgress(SoakProgressStarted, 0, 0, soakActivity{})
	for phase := 0; phase < options.Reopens; phase++ {
		if err := ctx.Err(); err != nil {
			return SoakReceipt{}, err
		}
		phaseStarted := time.Now()
		remaining := requestedConcurrent - receipt.ConcurrentDuration
		phaseDuration := time.Millisecond
		if remaining > 0 {
			phaseDuration = remaining / time.Duration(options.Reopens-phase)
		}
		if phaseDuration < time.Millisecond {
			phaseDuration = time.Millisecond
		}
		var activity soakActivity
		emitProgress(SoakProgressPhaseRunning, phase+1, 0, activity)
		concurrentStarted := time.Now()
		requireReclamationConflict := options.Profile == "release" && receipt.ReclamationConflicts == 0
		transactionOrdinal, activity, err = runSoakPhase(ctx, file, keys, transactionOrdinal, buildID, phaseDuration, progressInterval, requireReclamationConflict, func(current soakActivity, elapsed time.Duration) {
			emitProgress(SoakProgressPhaseRunning, phase+1, elapsed, current)
		})
		concurrentDuration := time.Since(concurrentStarted)
		if err != nil {
			return SoakReceipt{}, fmt.Errorf("phase %d concurrent work: %w", phase+1, err)
		}
		receipt.ConcurrentDuration += concurrentDuration
		emitProgress(SoakProgressPhaseVerifying, phase+1, 0, activity)
		var attempts int
		var build storage.IndexBuildMeta
		var exists bool
		err = runWithProgressHeartbeat(progressInterval, func() {
			emitProgress(SoakProgressPhaseVerifying, phase+1, 0, activity)
		}, func() error {
			if _, attempts, err = file.ReclaimPagesOptimisticContext(ctx, 3, true); err != nil {
				return fmt.Errorf("phase %d final audit: %w", phase+1, err)
			}
			if err := file.PersistFreeSpace(); err != nil {
				return fmt.Errorf("phase %d persist free space: %w", phase+1, err)
			}
			if err := file.Close(); err != nil {
				return err
			}
			file = nil
			verified, err := storage.VerifyPathContextWithIndexAudit(ctx, path, soakIndexAudit)
			if err != nil {
				return fmt.Errorf("phase %d offline verification: %w", phase+1, err)
			}
			if !verified.SemanticIndexesVerified || !verified.SemanticIndexBuildsVerified || !verified.FreeSpaceValid {
				return fmt.Errorf("phase %d offline verification is semantically incomplete", phase+1)
			}
			file, _, err = storage.Open(path)
			if err != nil {
				return fmt.Errorf("phase %d reopen: %w", phase+1, err)
			}
			if err := verifySoakContents(file, keys); err != nil {
				return fmt.Errorf("phase %d contents: %w", phase+1, err)
			}
			build, exists, err = file.IndexBuild(buildID)
			if err != nil {
				return fmt.Errorf("phase %d read index build: %w", phase+1, err)
			}
			if !exists || build.Phase == storage.IndexBuildFailed {
				return fmt.Errorf("phase %d invalid index build: exists=%t phase=%d", phase+1, exists, build.Phase)
			}
			return nil
		})
		if err != nil {
			return SoakReceipt{}, err
		}
		activity.ReclamationAttempts += uint64(attempts)
		storage := file.StorageStats()
		receipt.Phases = append(receipt.Phases, SoakPhaseReceipt{
			Ordinal: phase + 1, Duration: time.Since(phaseStarted), ConcurrentDuration: concurrentDuration, Writes: activity.Writes,
			SnapshotReads: activity.SnapshotReads, IndexBuildBatches: activity.IndexBuildBatches,
			ReclamationAttempts: activity.ReclamationAttempts, ReclamationConflicts: activity.ReclamationConflicts,
			CommitSequence: file.Meta().CommitSequence, PhysicalPages: storage.PhysicalPages,
			ReusablePages: storage.ReusablePages, IndexBuildPhase: uint8(build.Phase),
		})
		receipt.CompletedReopens++
		receipt.Writes += activity.Writes
		receipt.SnapshotReads += activity.SnapshotReads
		receipt.IndexBuildBatches += activity.IndexBuildBatches
		receipt.ReclamationAttempts += activity.ReclamationAttempts
		receipt.ReclamationConflicts += activity.ReclamationConflicts
		if options.Profile == "release" && (activity.Writes == 0 || activity.SnapshotReads == 0 || activity.IndexBuildBatches == 0 || activity.ReclamationAttempts == 0) {
			return SoakReceipt{}, fmt.Errorf("release phase %d did not exercise every worker", phase+1)
		}
		emitProgress(SoakProgressPhaseVerified, phase+1, 0, soakActivity{})
	}
	if receipt.CompletedReopens != options.Reopens || receipt.ConcurrentDuration < requestedConcurrent {
		return SoakReceipt{}, errors.New("storage soak ended before completing its reopen and concurrent-duration contract")
	}
	if options.Profile == "release" && receipt.ReclamationConflicts == 0 {
		return SoakReceipt{}, errors.New("release storage soak did not observe a real optimistic reclamation conflict")
	}
	if err := file.Close(); err != nil {
		return SoakReceipt{}, err
	}
	file = nil
	emitProgress(SoakProgressShadowVerifying, options.Reopens, 0, soakActivity{})
	var shadowVerified storage.VerificationResult
	err = runWithProgressHeartbeat(progressInterval, func() {
		emitProgress(SoakProgressShadowVerifying, options.Reopens, 0, soakActivity{})
	}, func() error {
		var verifyErr error
		shadowVerified, verifyErr = storage.VerifyPathContextWithIndexAudit(ctx, path, soakIndexAudit)
		return verifyErr
	})
	if err != nil {
		return SoakReceipt{}, fmt.Errorf("shadow verification: %w", err)
	}
	if !shadowVerified.SemanticIndexesVerified || !shadowVerified.SemanticIndexBuildsVerified || !shadowVerified.FreeSpaceValid {
		return SoakReceipt{}, errors.New("shadow verification is semantically incomplete")
	}
	receipt.SemanticIndexBuilds = true
	emitProgress(SoakProgressShadowVerified, options.Reopens, 0, soakActivity{})
	emitProgress(SoakProgressFinalVerifying, options.Reopens, 0, soakActivity{})
	var verified storage.VerificationResult
	err = runWithProgressHeartbeat(progressInterval, func() {
		emitProgress(SoakProgressFinalVerifying, options.Reopens, 0, soakActivity{})
	}, func() error {
		file, _, err = storage.Open(path)
		if err != nil {
			return err
		}
		if build, exists, err := file.IndexBuild(buildID); err != nil {
			return fmt.Errorf("read verified shadow build before abort: %w", err)
		} else if !exists || build.Phase == storage.IndexBuildFailed {
			return fmt.Errorf("verified shadow build unavailable before abort: exists=%t phase=%d", exists, build.Phase)
		}
		if err := file.AbortIndexBuild(buildID); err != nil {
			return err
		}
		if _, exists, err := file.IndexBuild(buildID); err != nil {
			return fmt.Errorf("read aborted shadow build: %w", err)
		} else if exists {
			return errors.New("aborted shadow build remains visible")
		}
		receipt.FinalIndexBuildAbsent = true
		if err := file.Close(); err != nil {
			return err
		}
		file = nil
		var verifyErr error
		verified, verifyErr = storage.VerifyPathContextWithIndexAudit(ctx, path, soakIndexAudit)
		return verifyErr
	})
	if err != nil {
		return SoakReceipt{}, fmt.Errorf("final verification: %w", err)
	}
	if !verified.SemanticIndexesVerified || !verified.SemanticIndexBuildsVerified || !verified.FreeSpaceValid {
		return SoakReceipt{}, errors.New("final verification is semantically incomplete")
	}
	receipt.FinalCommitSequence = verified.Meta.CommitSequence
	receipt.FinalFileBytes = verified.FileBytes
	receipt.FinalPhysicalPages = verified.PhysicalPages
	receipt.FinalReachablePages = verified.ReachablePages
	receipt.FinalReclaimablePages = verified.ReclaimablePages
	receipt.FinalFileSHA256 = hex.EncodeToString(verified.SHA256[:])
	receipt.PersistentFreeSpace = verified.PersistentFreeSpace
	receipt.FreeSpaceValid = verified.FreeSpaceValid
	receipt.SemanticIndexes = verified.SemanticIndexesVerified
	if receipt.Writes == 0 || receipt.SnapshotReads == 0 || receipt.IndexBuildBatches == 0 || receipt.ReclamationAttempts == 0 {
		return SoakReceipt{}, errors.New("storage soak did not exercise every worker")
	}
	if err := errors.Join(os.RemoveAll(probeDirectory), syncDirectory(options.TargetDirectory)); err != nil {
		return SoakReceipt{}, fmt.Errorf("clean storage soak target: %w", err)
	}
	cleanupRequired = false
	finished := time.Now()
	receipt.FinishedAt = finished.UTC()
	receipt.ActualDuration = finished.Sub(started)
	emitProgress(SoakProgressComplete, options.Reopens, 0, soakActivity{})
	return receipt, nil
}

func runWithProgressHeartbeat(interval time.Duration, heartbeat func(), operation func() error) error {
	result := make(chan error, 1)
	go func() {
		result <- operation()
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			return err
		case <-ticker.C:
			heartbeat()
		}
	}
}

func createSoakDatabase(path string, documentCount int) (*storage.File, []string, uint64, error) {
	file, _, err := storage.Open(path)
	if err != nil {
		return nil, nil, 0, err
	}
	keys := make([]string, documentCount)
	transactionOrdinal := uint64(1)
	for start := 0; start < documentCount; start += 256 {
		end := min(start+256, documentCount)
		mutations := make([]storage.DocumentMutation, 0, end-start)
		for index := start; index < end; index++ {
			keys[index] = soakKey(index, 0)
			mutations = append(mutations, storage.DocumentMutation{
				Collection: "items", DocumentID: soakDocumentID(index), Operation: storage.DocumentInsert, Document: []byte(keys[index]),
			})
		}
		if _, err := file.ApplyDocumentTransaction(storage.DocumentTransaction{
			TransactionID: soakTransactionID(transactionOrdinal), Mutations: mutations,
		}); err != nil {
			_ = file.Close()
			return nil, nil, 0, fmt.Errorf("initial document batch %d: %w", start/256+1, err)
		}
		transactionOrdinal++
	}
	entries := make([]storage.IndexEntry, documentCount)
	for index := range documentCount {
		entries[index] = storage.IndexEntry{Key: []byte(keys[index]), DocumentID: soakDocumentID(index)}
	}
	if _, err := file.ApplyCreateIndex(storage.CreateIndexTransaction{
		TransactionID: soakTransactionID(transactionOrdinal), Collection: "items", Name: "by_key",
		FieldPath: "key", Unique: true, Entries: entries,
	}); err != nil {
		_ = file.Close()
		return nil, nil, 0, err
	}
	return file, keys, transactionOrdinal + 1, nil
}

func runSoakPhase(parent context.Context, file *storage.File, keys []string, firstOrdinal uint64, buildID [16]byte, duration, progressInterval time.Duration, requireReclamationConflict bool, progress func(soakActivity, time.Duration)) (uint64, soakActivity, error) {
	ctx, cancel := context.WithTimeout(parent, duration)
	defer cancel()
	errorsSeen := make(chan error, 1)
	recordError := func(err error) {
		if err == nil {
			return
		}
		select {
		case errorsSeen <- err:
		default:
		}
		cancel()
	}
	var ordinal atomic.Uint64
	ordinal.Store(firstOrdinal)
	var writes, snapshotReads, indexBuildBatches atomic.Uint64
	var reclamationAttempts, reclamationConflicts atomic.Uint64
	var workers sync.WaitGroup
	workers.Add(4)
	go func() {
		defer workers.Done()
		var writeTicker *time.Ticker
		defer func() {
			if writeTicker != nil {
				writeTicker.Stop()
			}
		}()
		for ctx.Err() == nil {
			if !requireReclamationConflict || reclamationConflicts.Load() > 0 {
				if writeTicker == nil {
					writeTicker = time.NewTicker(soakWriteInterval)
				} else {
					select {
					case <-ctx.Done():
						return
					case <-writeTicker.C:
					}
				}
			}
			current := ordinal.Load()
			index := int(current % uint64(len(keys)))
			before, after := keys[index], soakKey(index, int(current))
			if _, err := file.ApplyDocumentTransaction(storage.DocumentTransaction{
				TransactionID: soakTransactionID(current), Mutations: []storage.DocumentMutation{{
					Collection: "items", DocumentID: soakDocumentID(index), Operation: storage.DocumentUpdate,
					Document: []byte(after), Indexes: []storage.IndexMutation{{Name: "by_key", BeforeKey: []byte(before), AfterKey: []byte(after)}},
				}},
			}); err != nil {
				recordError(err)
				return
			}
			keys[index] = after
			ordinal.Add(1)
			writes.Add(1)
		}
	}()
	go func() {
		defer workers.Done()
		for ctx.Err() == nil {
			applied, err := advanceSoakIndexBuild(ctx, file, buildID)
			if err != nil {
				recordError(err)
				return
			}
			if applied {
				indexBuildBatches.Add(1)
			}
		}
	}()
	go func() {
		defer workers.Done()
		var read uint64
		for ctx.Err() == nil {
			snapshot, err := file.OpenSnapshot()
			if err != nil {
				recordError(err)
				return
			}
			_, exists, readErr := snapshot.GetDocument("items", soakDocumentID(int(read%uint64(len(keys)))))
			closeErr := snapshot.Close()
			if readErr != nil || !exists || closeErr != nil {
				if !exists && readErr == nil {
					readErr = storage.ErrCorrupt
				}
				recordError(errors.Join(readErr, closeErr))
				return
			}
			snapshotReads.Add(1)
			read++
		}
	}()
	go func() {
		defer workers.Done()
		for ctx.Err() == nil {
			reclamationAttempts.Add(1)
			_, _, err := file.ReclaimPagesOptimisticContext(ctx, 1, false)
			if errors.Is(err, storage.ErrReclamationConflict) {
				reclamationConflicts.Add(1)
			}
			if err != nil && !errors.Is(err, storage.ErrReclamationConflict) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				recordError(err)
				return
			}
		}
	}()
	workersDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(workersDone)
	}()
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	started := time.Now()
	wake := true
	for wake {
		select {
		case <-workersDone:
			wake = false
		case <-ticker.C:
			if progress != nil {
				progress(loadSoakActivity(&writes, &snapshotReads, &indexBuildBatches, &reclamationAttempts, &reclamationConflicts), time.Since(started))
			}
		}
	}
	select {
	case err := <-errorsSeen:
		return 0, soakActivity{}, err
	default:
	}
	if err := parent.Err(); err != nil {
		return 0, soakActivity{}, err
	}
	return ordinal.Load(), loadSoakActivity(&writes, &snapshotReads, &indexBuildBatches, &reclamationAttempts, &reclamationConflicts), nil
}

func loadSoakActivity(writes, snapshotReads, indexBuildBatches, reclamationAttempts, reclamationConflicts *atomic.Uint64) soakActivity {
	return soakActivity{
		Writes: writes.Load(), SnapshotReads: snapshotReads.Load(), IndexBuildBatches: indexBuildBatches.Load(),
		ReclamationAttempts: reclamationAttempts.Load(), ReclamationConflicts: reclamationConflicts.Load(),
	}
}

func advanceSoakIndexBuild(ctx context.Context, file *storage.File, buildID [16]byte) (bool, error) {
	build, exists, err := file.IndexBuild(buildID)
	if err != nil || !exists {
		if err != nil {
			return false, err
		}
		return false, storage.ErrIndexBuildNotFound
	}
	switch build.Phase {
	case storage.IndexBuildScan:
		opened, iterator, err := file.OpenIndexBuildScanIterator(buildID, 256)
		if err != nil {
			return false, err
		}
		entries := make([]storage.IndexEntry, 0, 256)
		last, count := opened.ScanAfter, 0
		for iterator.Next() {
			record := iterator.Record()
			last = record.DocumentID
			count++
			entries = append(entries, storage.IndexEntry{Key: append([]byte(nil), record.Document...), DocumentID: record.DocumentID})
		}
		if err := errors.Join(iterator.Err(), iterator.Close()); err != nil {
			return false, err
		}
		_, err = file.ApplyIndexBuildScanBatch(storage.IndexBuildScanBatch{
			BuildID: buildID, ExpectedScanAfter: opened.ScanAfter, ScanAfter: last, Entries: entries, Complete: count < 256,
		})
		if errors.Is(err, storage.ErrIndexBuildState) {
			return false, nil
		}
		return err == nil, err
	case storage.IndexBuildCatchUp, storage.IndexBuildReady:
		return advanceSoakIndexBuildCatchUp(ctx, file, buildID)
	case storage.IndexBuildFailed:
		return false, storage.ErrIndexBuildState
	default:
		return false, storage.ErrCorrupt
	}
}

func advanceSoakIndexBuildCatchUp(ctx context.Context, file *storage.File, buildID [16]byte) (_ bool, resultErr error) {
	opened, snapshot, err := file.OpenIndexBuildCatchUpSnapshot(buildID)
	if err != nil {
		return false, err
	}
	defer func() { resultErr = errors.Join(resultErr, snapshot.Close()) }()
	if snapshot.Sequence() <= opened.AppliedSequence {
		timer := time.NewTimer(soakIndexCatchUpInterval)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false, nil
		case <-timer.C:
		}
		return false, nil
	}
	through := min(snapshot.Sequence(), opened.AppliedSequence+64)
	mutations := make([]storage.IndexBuildCatchUpMutation, 0)
	for sequence := opened.AppliedSequence + 1; sequence <= through; sequence++ {
		commit, err := snapshot.ReadCommit(sequence)
		if err != nil {
			return false, err
		}
		for _, change := range commit.Changes {
			if change.CollectionID != opened.CollectionID || change.Operation == storage.CommitCatalog {
				continue
			}
			mutation := storage.IndexBuildCatchUpMutation{Sequence: sequence, DocumentID: change.DocumentID, Operation: change.Operation}
			if change.BeforeRef != nil {
				mutation.BeforeKey, err = snapshot.ReadDocumentVersion(*change.BeforeRef)
				if err != nil {
					return false, err
				}
			}
			if change.AfterRef != nil {
				mutation.AfterKey, err = snapshot.ReadDocumentVersion(*change.AfterRef)
				if err != nil {
					return false, err
				}
			}
			mutations = append(mutations, mutation)
		}
	}
	_, err = file.ApplyIndexBuildCatchUpBatch(storage.IndexBuildCatchUpBatch{
		BuildID: buildID, ExpectedAppliedSequence: opened.AppliedSequence, ThroughSequence: through, Mutations: mutations,
	})
	if errors.Is(err, storage.ErrIndexBuildState) {
		return false, nil
	}
	return err == nil, err
}

func verifySoakContents(file *storage.File, keys []string) (resultErr error) {
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, snapshot.Close()) }()
	documents, err := snapshot.ScanCollection("items", nil, nil, 0)
	if err != nil {
		return err
	}
	if len(documents) != len(keys) {
		return fmt.Errorf("documents=%d want=%d", len(documents), len(keys))
	}
	positions := make([]uint64, len(keys))
	seenDocuments := make([]bool, len(keys))
	for _, record := range documents {
		index := int(binary.BigEndian.Uint64(record.DocumentID[8:]))
		if index < 0 || index >= len(keys) || record.DocumentID != soakDocumentID(index) ||
			record.InsertionPosition == 0 || string(record.Document) != keys[index] || seenDocuments[index] {
			return fmt.Errorf("invalid document id=%x position=%d", record.DocumentID, record.InsertionPosition)
		}
		seenDocuments[index] = true
		positions[index] = record.InsertionPosition
	}
	entries, err := snapshot.ScanIndex("items", "by_key", nil, nil, 0)
	if err != nil {
		return err
	}
	if len(entries) != len(keys) {
		return fmt.Errorf("index entries=%d want=%d", len(entries), len(keys))
	}
	seenEntries := make([]bool, len(keys))
	for _, entry := range entries {
		index := int(binary.BigEndian.Uint64(entry.DocumentID[8:]))
		if index < 0 || index >= len(keys) || entry.DocumentID != soakDocumentID(index) || string(entry.Key) != keys[index] ||
			entry.InsertionPosition != positions[index] || seenEntries[index] {
			return fmt.Errorf("invalid index entry id=%x key=%q", entry.DocumentID, entry.Key)
		}
		seenEntries[index] = true
	}
	return nil
}

func soakIndexAudit(meta storage.IndexMeta, _ [16]byte, document []byte) ([]byte, bool, error) {
	if meta.Name != "by_key" && meta.Name != "by_shadow" {
		return nil, false, storage.ErrCorrupt
	}
	return append([]byte(nil), document...), true, nil
}

func soakDocumentID(index int) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], uint64(index))
	id[0] = 1
	return id
}

func soakTransactionID(ordinal uint64) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], ordinal)
	id[0] = 2
	return id
}

func soakKey(index, revision int) string {
	return fmt.Sprintf("key-%012d-r%06d", index, revision)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func validRevision(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
