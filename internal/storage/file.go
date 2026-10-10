package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
)

var (
	// ErrLocked reports that another writer owns the database file.
	ErrLocked = errors.New("meldbase storage: database is locked")
	// ErrRecoveryRequired reports that opening requires recovery disallowed by the chosen options.
	ErrRecoveryRequired = errors.New("meldbase storage: recovery required")
	// ErrInvalidStorageLimit reports an invalid physical file size limit.
	ErrInvalidStorageLimit = errors.New("meldbase storage: invalid storage limit")
	// ErrStorageLimit reports that allocating pages would exceed the physical storage limit.
	ErrStorageLimit = errors.New("meldbase storage: storage limit exceeded")
	// ErrStaleSnapshot reports that the database is older than the required minimum version.
	ErrStaleSnapshot = errors.New("meldbase storage: commit sequence is below the required minimum")
	// ErrDatabaseIdentity reports a database identity different from the expected identity.
	ErrDatabaseIdentity = errors.New("meldbase storage: unexpected database identity")
	// ErrInsecureFileMode reports file permissions that allow access by other users.
	ErrInsecureFileMode = errors.New("meldbase storage: database file permissions are not owner-private")
)

// OpenOptions sets storage safety checks, retention limits, and file size bounds.
type OpenOptions struct {
	RequireClean bool
	// RequireGraphAudit verifies every page protected by either valid Meta root
	// before Open succeeds. It is deliberately opt-in: ordinary  startup is
	// metadata-only so large databases do not pay an unbounded open pause.
	// Unlike VerifyPathContext it is a structural audit only; callers needing
	// Secondary-to-document semantic proof must run the offline verifier.
	RequireGraphAudit bool
	// RequirePrivateFileMode rejects a database whose existing Unix permission
	// bits grant group or other access. It never chmods an operator-owned file:
	// callers choose this fail-closed boundary explicitly for deployments where
	// the database can contain secrets.
	RequirePrivateFileMode    bool
	ExpectedDatabaseID        [16]byte
	MinimumCommitSequence     uint64
	MinimumGeneration         uint64
	CommitRetentionMaxCommits uint64
	CommitRetentionMaxBytes   uint64
	MaxFileBytes              uint64
}

const (
	// DefaultCommitRetentionMaxCommits sets the default retained commit count.
	DefaultCommitRetentionMaxCommits uint64 = 10_000
	// DefaultCommitRetentionMaxBytes sets the default retained commit byte limit.
	DefaultCommitRetentionMaxBytes uint64 = 256 << 20
	// DefaultMaxFileBytes sets the default physical database file size limit.
	DefaultMaxFileBytes uint64 = 8 << 30
)

// faultPoint identifies a commit stage where tests can inject a failure.
type faultPoint uint8

const (
	// faultAfterPageWrite injects a failure after writing staged pages.
	faultAfterPageWrite faultPoint = 1 + iota
	// faultBeforeDataSync injects a failure before syncing staged page data.
	faultBeforeDataSync
	// faultAfterDataSync injects a failure after syncing staged page data.
	faultAfterDataSync
	// faultAfterMetaWrite injects a failure after writing publication metadata.
	faultAfterMetaWrite
	// faultAfterMetaSync injects a failure after syncing publication metadata.
	faultAfterMetaSync
)

// QualificationBoundary names a commit stage used by internal destructive tests.
//
// This type is not a storage or application API contract.
type QualificationBoundary string

const (
	// QualificationAfterPageWrite identifies the stage after staged pages are written.
	QualificationAfterPageWrite QualificationBoundary = "after-page-write"
	// QualificationBeforeDataSync identifies the stage before staged page data is synced.
	QualificationBeforeDataSync QualificationBoundary = "before-data-sync"
	// QualificationAfterDataSync identifies the stage after staged page data is synced.
	QualificationAfterDataSync QualificationBoundary = "after-data-sync"
	// QualificationAfterMetaWrite identifies the stage after publication metadata is written.
	QualificationAfterMetaWrite QualificationBoundary = "after-meta-write"
	// QualificationAfterMetaSync identifies the stage after publication metadata is synced.
	QualificationAfterMetaSync QualificationBoundary = "after-meta-sync"
)

// File owns the database file, committed roots, reader pins, and page allocation state.
type File struct {
	file                     *os.File
	mu                       sync.RWMutex
	meta                     Meta
	root                     DatabaseRoot
	metaSlot                 int
	metas                    [2]Meta
	metaOK                   [2]bool
	nextPage                 uint64
	fatalErr                 error
	fault                    func(faultPoint) error
	nextPin                  uint64
	readers                  map[uint64]readerPin
	changed                  chan struct{}
	cache                    *pageCache
	freePages                []uint64
	freePoolVersion          uint64
	treeSplits               atomic.Uint64
	treeMerges               atomic.Uint64
	freeSpaceTracked         bool
	freeSpaceNeedsRebuild    bool
	freeSpaceLoads           atomic.Uint64
	freeSpaceLoadFailures    atomic.Uint64
	freeSpacePublishes       atomic.Uint64
	freeSpaceCandidateChecks atomic.Uint64
	reclamationScanStep      func()
	commitRetentionMax       uint64
	commitRetentionMaxBytes  uint64
	retainedCommitBytes      uint64
	retentionPruned          atomic.Uint64
	retentionPressureEvents  atomic.Uint64
	retentionPressure        atomic.Bool
	maxPhysicalPages         uint64
	storageLimitRejections   atomic.Uint64
}

// readerPin records a database version that an active reader or replay stream needs retained.
type readerPin struct {
	generation, sequence, rootPage uint64
	replay                         bool
}

// stagedPage holds encoded page bytes waiting for transaction publication.
type stagedPage struct {
	id   uint64
	data []byte
}

// existingFileState holds the validated metadata and root selected while opening a file.
type existingFileState struct {
	meta     Meta
	root     DatabaseRoot
	metaSlot int
	metas    [2]Meta
	metaOK   [2]bool
	nextPage uint64
}

// OpenReport describes only recovery decisions made while opening a file.
//
// It
// contains no path or user data and is immutable after OpenWithReport returns.
type OpenReport struct {
	Created                bool
	SelectedMetaSlot       uint8
	ChecksumValidMetaSlots uint8
	RootValidMetaSlots     uint8
	MetaRedundancyDegraded bool
	FallbackToOlderRoot    bool
	TrailingBytesRemoved   uint64
	FreeSpaceLoadDegraded  bool
}

// WriteTxn stages immutable pages in memory.
//
// Nothing becomes visible until the
// DatabaseRoot and inactive MetaPage are durably published by File.Update.
type WriteTxn struct {
	file                     *File
	baseRoot                 DatabaseRoot
	generation               uint64
	sequence                 uint64
	nextPage                 uint64
	freePages                []uint64
	reusedPages              []uint64
	pages                    []stagedPage
	byID                     map[uint64][]byte
	treeSplits               uint64
	treeMerges               uint64
	freeSpaceTracked         bool
	freeSpaceRebuild         bool
	metadataAllocation       bool
	retentionEvaluated       bool
	retentionPruned          uint64
	retentionBlocked         bool
	retainedCommitBytes      uint64
	requiredFeatures         uint64
	indexBuildCatalogChanged bool
}

// Open creates or opens a database file with default storage options.
func Open(path string) (*File, Meta, error) {
	file, meta, _, err := OpenWithReport(path)
	return file, meta, err
}

// OpenWithReport opens a database file and reports the recovery decisions made during startup.
func OpenWithReport(path string) (*File, Meta, OpenReport, error) {
	return OpenWithOptions(path, OpenOptions{})
}

// OpenWithOptions opens and validates a database file using the requested safety and resource limits.
func OpenWithOptions(path string, options OpenOptions) (*File, Meta, OpenReport, error) {
	if path == "" {
		return nil, Meta{}, OpenReport{}, errors.New("meldbase storage: empty path")
	}
	maxPhysicalPages, err := normalizeMaxFileBytes(options.MaxFileBytes)
	if err != nil {
		return nil, Meta{}, OpenReport{}, err
	}
	requireExisting := options.MinimumCommitSequence > 0 || options.MinimumGeneration > 0 || !allZero(options.ExpectedDatabaseID[:])
	openFlags := os.O_RDWR | os.O_CREATE
	if requireExisting {
		openFlags = os.O_RDWR
	}
	file, err := os.OpenFile(path, openFlags, 0o600)
	if err != nil {
		if requireExisting && errors.Is(err, os.ErrNotExist) {
			if !allZero(options.ExpectedDatabaseID[:]) {
				return nil, Meta{}, OpenReport{}, fmt.Errorf("%w: database does not exist", ErrDatabaseIdentity)
			}
			return nil, Meta{}, OpenReport{}, fmt.Errorf("%w: database does not exist", ErrStaleSnapshot)
		}
		return nil, Meta{}, OpenReport{}, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, Meta{}, OpenReport{}, fmt.Errorf("%w: %v", ErrLocked, err)
	}
	cleanup := func(err error) (*File, Meta, OpenReport, error) {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, Meta{}, OpenReport{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return cleanup(err)
	}
	if options.RequirePrivateFileMode && (!info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0) {
		return cleanup(ErrInsecureFileMode)
	}
	if info.Size() == 0 {
		if requireExisting {
			if !allZero(options.ExpectedDatabaseID[:]) {
				return cleanup(fmt.Errorf("%w: database is empty", ErrDatabaseIdentity))
			}
			return cleanup(fmt.Errorf("%w: database is empty", ErrStaleSnapshot))
		}
		meta := Meta{Generation: 1, PhysicalPageCount: 2}
		if _, err := rand.Read(meta.DatabaseID[:]); err != nil {
			return cleanup(err)
		}
		encoded, err := EncodeMeta(meta)
		if err != nil {
			return cleanup(err)
		}
		if err := file.Truncate(2 * PageSize); err != nil {
			return cleanup(err)
		}
		if _, err := file.WriteAt(encoded, 0); err != nil {
			return cleanup(err)
		}
		if err := file.Sync(); err != nil {
			return cleanup(err)
		}
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return cleanup(err)
		}
		return &File{
			file: file, meta: meta, root: DatabaseRoot{}, metaSlot: 0, metas: [2]Meta{0: meta}, metaOK: [2]bool{true, false},
			nextPage: 2, readers: make(map[uint64]readerPin), changed: make(chan struct{}), cache: newPageCache(defaultCachedPages),
			commitRetentionMax:      normalizedCommitRetention(options.CommitRetentionMaxCommits),
			commitRetentionMaxBytes: normalizedCommitRetentionBytes(options.CommitRetentionMaxBytes),
			maxPhysicalPages:        maxPhysicalPages,
		}, meta, OpenReport{Created: true, SelectedMetaSlot: 0, ChecksumValidMetaSlots: 1, RootValidMetaSlots: 1}, nil
	}
	if info.Size() < 2*PageSize {
		return cleanup(ErrCorrupt)
	}
	effectiveSize := info.Size() - info.Size()%PageSize
	physicalPages := uint64(effectiveSize / PageSize)
	state, err := inspectExistingFile(file, physicalPages)
	if err != nil {
		return cleanup(err)
	}
	report := state.openReport(uint64(info.Size() - effectiveSize))
	if !allZero(options.ExpectedDatabaseID[:]) && state.meta.DatabaseID != options.ExpectedDatabaseID {
		return cleanup(ErrDatabaseIdentity)
	}
	if state.meta.CommitSequence < options.MinimumCommitSequence {
		return cleanup(fmt.Errorf("%w: found %d, require at least %d", ErrStaleSnapshot, state.meta.CommitSequence, options.MinimumCommitSequence))
	}
	if state.meta.Generation < options.MinimumGeneration {
		return cleanup(fmt.Errorf("%w: found generation %d, require at least %d", ErrStaleSnapshot, state.meta.Generation, options.MinimumGeneration))
	}
	if options.RequireClean && (report.FallbackToOlderRoot || report.MetaRedundancyDegraded || report.TrailingBytesRemoved != 0) {
		return cleanup(ErrRecoveryRequired)
	}
	opened := state.open(file)
	opened.commitRetentionMax = normalizedCommitRetention(options.CommitRetentionMaxCommits)
	opened.commitRetentionMaxBytes = normalizedCommitRetentionBytes(options.CommitRetentionMaxBytes)
	opened.maxPhysicalPages = maxPhysicalPages
	retainedBytes, err := opened.calculateRetainedCommitBytes(state.root.CommitLogRoot)
	if err != nil {
		_ = opened.Close()
		return nil, Meta{}, report, err
	}
	opened.retainedCommitBytes = retainedBytes
	retainedCommits := uint64(0)
	if state.meta.OldestRetainedSequence > 0 && state.meta.CommitSequence >= state.meta.OldestRetainedSequence {
		retainedCommits = state.meta.CommitSequence - state.meta.OldestRetainedSequence + 1
	}
	opened.retentionPressure.Store(retainedCommits > opened.commitRetentionMax || retainedBytes > opened.commitRetentionMaxBytes)
	if state.meta.OptionalFeatures&OptionalFeaturePersistentFreeSpace != 0 {
		opened.freeSpaceLoads.Add(1)
		if err := opened.restoreFreeSpace(state.root, state.meta); err != nil {
			opened.freePages = nil
			opened.freeSpaceTracked = false
			opened.freeSpaceLoadFailures.Add(1)
			report.FreeSpaceLoadDegraded = true
			if options.RequireClean {
				_ = opened.Close()
				return nil, Meta{}, report, ErrRecoveryRequired
			}
		}
	}
	if options.RequireGraphAudit {
		// Run before removing a provable unaligned crash tail. A failed explicit
		// audit therefore remains read-only even when automatic recovery is
		// otherwise enabled.
		if _, _, err := opened.reachabilityUnlocked(context.Background()); err != nil {
			_ = opened.Close()
			return nil, Meta{}, report, err
		}
	}
	if effectiveSize != info.Size() {
		if err := file.Truncate(effectiveSize); err != nil {
			_ = opened.Close()
			return nil, Meta{}, report, err
		}
		if err := file.Sync(); err != nil {
			_ = opened.Close()
			return nil, Meta{}, report, err
		}
	}
	return opened, state.meta, report, nil
}

// OpenForQualification opens a database file with a hook for destructive commit tests.
//
// The hook runs synchronously and may block while an external controller
// cuts power, exhausts a disposable volume or terminates the process. This
// entry point lives in internal/storage so external applications cannot opt
// into qualification behavior accidentally.
func OpenForQualification(path string, options OpenOptions, hook func(QualificationBoundary) error) (*File, Meta, OpenReport, error) {
	if hook == nil {
		return nil, Meta{}, OpenReport{}, errors.New("meldbase storage: nil qualification hook")
	}
	file, meta, report, err := OpenWithOptions(path, options)
	if err != nil {
		return nil, Meta{}, report, err
	}
	file.fault = func(point faultPoint) error {
		boundary, ok := qualificationBoundary(point)
		if !ok {
			return ErrCorrupt
		}
		return hook(boundary)
	}
	return file, meta, report, nil
}

// qualificationBoundary maps a test failure point to its named publication stage.
func qualificationBoundary(point faultPoint) (QualificationBoundary, bool) {
	switch point {
	case faultAfterPageWrite:
		return QualificationAfterPageWrite, true
	case faultBeforeDataSync:
		return QualificationBeforeDataSync, true
	case faultAfterDataSync:
		return QualificationAfterDataSync, true
	case faultAfterMetaWrite:
		return QualificationAfterMetaWrite, true
	case faultAfterMetaSync:
		return QualificationAfterMetaSync, true
	default:
		return "", false
	}
}

// normalizedCommitRetention supplies the default retained commit count for a zero limit.
func normalizedCommitRetention(value uint64) uint64 {
	if value == 0 {
		return DefaultCommitRetentionMaxCommits
	}
	return value
}

// normalizedCommitRetentionBytes supplies the default retained byte count for a zero limit.
func normalizedCommitRetentionBytes(value uint64) uint64 {
	if value == 0 {
		return DefaultCommitRetentionMaxBytes
	}
	return value
}

// normalizeMaxFileBytes validates an aligned file byte limit and converts it to pages.
func normalizeMaxFileBytes(value uint64) (uint64, error) {
	if value == 0 {
		value = DefaultMaxFileBytes
	}
	maxAligned := uint64(math.MaxInt64/PageSize) * PageSize
	if value < 2*PageSize || value > maxAligned || value%PageSize != 0 {
		return 0, ErrInvalidStorageLimit
	}
	return value / PageSize, nil
}

// openReport summarizes metadata validity, root fallback, and removed trailing bytes.
func (state existingFileState) openReport(trailing uint64) OpenReport {
	report := OpenReport{SelectedMetaSlot: uint8(state.metaSlot), TrailingBytesRemoved: trailing}
	newestGeneration := uint64(0)
	for slot := range 2 {
		if state.metas[slot] != (Meta{}) {
			report.ChecksumValidMetaSlots++
			if state.metas[slot].Generation > newestGeneration {
				newestGeneration = state.metas[slot].Generation
			}
		}
		if state.metaOK[slot] {
			report.RootValidMetaSlots++
		}
	}
	report.FallbackToOlderRoot = state.meta.Generation < newestGeneration
	report.MetaRedundancyDegraded = report.RootValidMetaSlots < report.ChecksumValidMetaSlots ||
		(report.ChecksumValidMetaSlots < 2 && state.nextPage > 2)
	return report
}

// inspectExistingFile selects the newest valid supported metadata and database root.
func inspectExistingFile(file *os.File, physicalPages uint64) (existingFileState, error) {
	if file == nil || physicalPages < 2 {
		return existingFileState{}, ErrCorrupt
	}
	metas := [2]Meta{}
	metaValid := [2]bool{}
	metaErrors := [2]error{}
	for slot := range 2 {
		page := make([]byte, PageSize)
		if _, err := file.ReadAt(page, int64(slot*PageSize)); err != nil {
			return existingFileState{}, err
		}
		meta, err := DecodeMeta(page)
		if err == nil && meta.PhysicalPageCount <= physicalPages {
			metas[slot], metaValid[slot] = meta, true
		} else if err != nil {
			metaErrors[slot] = err
		}
	}
	for _, metaErr := range metaErrors {
		if errors.Is(metaErr, ErrUnsupportedFormat) || errors.Is(metaErr, ErrUnsupportedFeature) {
			return existingFileState{}, metaErr
		}
	}
	if !metaValid[0] && !metaValid[1] {
		return existingFileState{}, ErrCorrupt
	}
	if metaValid[0] && metaValid[1] && metas[0].DatabaseID != metas[1].DatabaseID {
		return existingFileState{}, ErrCorrupt
	}
	if metaValid[0] && metaValid[1] && metas[0].Generation == metas[1].Generation && metas[0] != metas[1] {
		return existingFileState{}, ErrCorrupt
	}
	order := [2]int{0, 1}
	if !metaValid[0] || (metaValid[1] && metas[1].Generation > metas[0].Generation) {
		order = [2]int{1, 0}
	}
	selected := -1
	rootValid := [2]bool{}
	for _, slot := range order {
		if !metaValid[slot] {
			continue
		}
		if validateRoot(file, metas[slot]) == nil {
			rootValid[slot] = true
			if selected < 0 {
				selected = slot
			}
		}
	}
	if selected < 0 {
		return existingFileState{}, ErrCorrupt
	}
	meta := metas[selected]
	root, err := readDatabaseRoot(file, meta)
	if err != nil {
		return existingFileState{}, err
	}
	return existingFileState{
		meta: meta, root: root, metaSlot: selected, metas: metas, metaOK: rootValid, nextPage: physicalPages,
	}, nil
}

// open initializes a storage handle from validated existing file state.
func (state existingFileState) open(file *os.File) *File {
	return &File{
		file: file, meta: state.meta, root: state.root, metaSlot: state.metaSlot, metas: state.metas, metaOK: state.metaOK,
		nextPage: state.nextPage, readers: make(map[uint64]readerPin), changed: make(chan struct{}), cache: newPageCache(defaultCachedPages),
	}
}

// CommitRoot publishes the supplied database root as the next committed generation.
//
// Referenced tree roots must already be durable, and the supplied sequence must
// match the next transaction sequence.
func (f *File) CommitRoot(root DatabaseRoot) error {
	return f.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		if root.CommitSequence != tx.Sequence() {
			return DatabaseRoot{}, ErrCorrupt
		}
		return root, nil
	})
}

// Update builds and publishes a new database version atomically.
//
// The callback must not retain tx and must not perform network or user callbacks.
func (f *File) Update(build func(*WriteTxn) (DatabaseRoot, error)) error {
	if f == nil || build == nil {
		return errors.New("meldbase storage: invalid update")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updateUnlocked(true, build)
}

// updateUnlocked publishes a generation with an optional commit sequence advance while the caller holds the file lock.
func (f *File) updateUnlocked(advanceSequence bool, build func(*WriteTxn) (DatabaseRoot, error)) error {
	advance := uint64(0)
	if advanceSequence {
		advance = 1
	}
	return f.updateUnlockedAdvance(advance, build)
}

// updateUnlockedAdvance publishes a physical generation with the requested logical sequence advance.
//
// The caller holds the file lock; zero advances are for maintenance, and
// grouped document commits advance by the number of logical batches built.
func (f *File) updateUnlockedAdvance(advance uint64, build func(*WriteTxn) (DatabaseRoot, error)) error {
	if f.file == nil {
		return errors.New("meldbase storage: file is closed")
	}
	if f.fatalErr != nil {
		return f.fatalErr
	}
	baseRoot, err := f.databaseRootUnlocked()
	if err != nil {
		return err
	}
	generation := f.meta.Generation + 1
	if advance > math.MaxUint64-f.meta.CommitSequence {
		return ErrCorrupt
	}
	sequence := f.meta.CommitSequence + advance
	if generation == 0 || (advance > 0 && sequence == 0) {
		return ErrCorrupt
	}
	tx := &WriteTxn{
		file: f, baseRoot: baseRoot, generation: generation, sequence: sequence, nextPage: f.nextPage,
		freePages: append([]uint64(nil), f.freePages...), byID: make(map[uint64][]byte),
		freeSpaceTracked: f.freeSpaceTracked, freeSpaceRebuild: f.freeSpaceNeedsRebuild,
		retainedCommitBytes: f.retainedCommitBytes,
		requiredFeatures:    f.meta.RequiredFeatures,
	}
	root, err := build(tx)
	if err != nil {
		return err
	}
	if !tx.indexBuildCatalogChanged {
		root.IndexBuildCatalogRoot = baseRoot.IndexBuildCatalogRoot
	}
	if root.CommitSequence != sequence || root.OldestRetainedSequence > sequence ||
		(root.IndexBuildCatalogRoot != 0 && tx.requiredFeatures&RequiredFeatureShadowIndexBuilds == 0) {
		return ErrCorrupt
	}
	rootPageID, err := tx.allocatePageID()
	if err != nil {
		return err
	}
	if err := tx.finalizeFreeSpace(&root); err != nil {
		return err
	}
	rootPage, err := EncodeDatabaseRoot(rootPageID, generation, root)
	if err != nil {
		return err
	}
	tx.pages = append(tx.pages, stagedPage{id: rootPageID, data: rootPage})
	for _, page := range tx.pages {
		if _, err := f.file.WriteAt(page.data, int64(page.id)*PageSize); err != nil {
			return f.failCommit(err)
		}
		if err := f.inject(faultAfterPageWrite); err != nil {
			return f.failCommit(err)
		}
	}
	if err := f.inject(faultBeforeDataSync); err != nil {
		return f.failCommit(err)
	}
	if err := f.file.Sync(); err != nil {
		return f.failCommit(err)
	}
	if err := f.inject(faultAfterDataSync); err != nil {
		return f.failCommit(err)
	}
	meta := Meta{
		DatabaseID:             f.meta.DatabaseID,
		Generation:             generation,
		CommitSequence:         sequence,
		RootPage:               rootPageID,
		PhysicalPageCount:      tx.nextPage,
		OldestRetainedSequence: root.OldestRetainedSequence,
		RequiredFeatures:       tx.requiredFeatures,
		OptionalFeatures:       f.meta.OptionalFeatures,
	}
	if tx.freeSpaceTracked {
		meta.OptionalFeatures |= OptionalFeaturePersistentFreeSpace
	} else {
		meta.OptionalFeatures &^= OptionalFeaturePersistentFreeSpace
	}
	encoded, err := EncodeMeta(meta)
	if err != nil {
		return err
	}
	slot := 1 - f.metaSlot
	if _, err := f.file.WriteAt(encoded, int64(slot*PageSize)); err != nil {
		return f.failCommit(err)
	}
	if err := f.inject(faultAfterMetaWrite); err != nil {
		return f.failCommit(err)
	}
	if err := f.file.Sync(); err != nil {
		return f.failCommit(err)
	}
	if err := f.inject(faultAfterMetaSync); err != nil {
		return f.failCommit(err)
	}
	f.cache.invalidate(tx.reusedPages)
	f.meta, f.root, f.metaSlot, f.nextPage, f.freePages = meta, root, slot, tx.nextPage, tx.freePages
	f.freePoolVersion++
	f.freeSpaceTracked, f.freeSpaceNeedsRebuild = tx.freeSpaceTracked, false
	f.metas[slot], f.metaOK[slot] = meta, true
	f.treeSplits.Add(tx.treeSplits)
	f.treeMerges.Add(tx.treeMerges)
	if tx.freeSpaceRebuild {
		f.freeSpacePublishes.Add(1)
	}
	if tx.retentionEvaluated {
		f.retainedCommitBytes = tx.retainedCommitBytes
		f.retentionPruned.Add(tx.retentionPruned)
		f.retentionPressure.Store(tx.retentionBlocked)
		if tx.retentionBlocked {
			f.retentionPressureEvents.Add(1)
		}
	}
	if advance > 0 {
		close(f.changed)
		f.changed = make(chan struct{})
	}
	return nil
}

// inject runs the configured test failure hook at a commit stage.
func (f *File) inject(point faultPoint) error {
	if f.fault == nil {
		return nil
	}
	return f.fault(point)
}

// failCommit records a fatal commit error that prevents further writes.
func (f *File) failCommit(err error) error {
	if err == nil {
		err = ErrCorrupt
	}
	f.fatalErr = err
	return err
}

// Sequence returns the logical commit number being built by the transaction.
func (tx *WriteTxn) Sequence() uint64 { return tx.sequence }

// requireFeature adds a supported required feature to the transaction's published metadata.
func (tx *WriteTxn) requireFeature(feature uint64) error {
	if tx == nil || feature == 0 || feature&^SupportedRequiredFeatures != 0 {
		return ErrUnsupportedFeature
	}
	tx.requiredFeatures |= feature
	return nil
}

// BaseRoot returns the committed database root from which the transaction started.
func (tx *WriteTxn) BaseRoot() DatabaseRoot {
	if tx == nil {
		return DatabaseRoot{}
	}
	return tx.baseRoot
}

// appendPage allocates an ID and stages an encoded page for publication.
func (tx *WriteTxn) appendPage(pageType PageType, flags uint8, itemCount uint32, link uint64, payload []byte) (uint64, error) {
	if tx == nil || tx.file == nil {
		return 0, ErrCorrupt
	}
	id, err := tx.allocatePageID()
	if err != nil {
		return 0, err
	}
	if err := tx.appendPageAt(id, pageType, flags, itemCount, link, payload); err != nil {
		return 0, err
	}
	return id, nil
}

// appendPageAt stages an encoded page at an already allocated page ID.
func (tx *WriteTxn) appendPageAt(id uint64, pageType PageType, flags uint8, itemCount uint32, link uint64, payload []byte) error {
	if tx == nil || tx.file == nil || id < 2 {
		return ErrCorrupt
	}
	encoded, err := EncodePage(Page{
		Type: pageType, Flags: flags, ID: id, Generation: tx.generation,
		BornSequence: tx.sequence, ItemCount: itemCount, Link: link, Payload: payload,
	})
	if err != nil {
		return err
	}
	tx.pages = append(tx.pages, stagedPage{id: id, data: encoded})
	tx.byID[id] = encoded
	return nil
}

// allocatePageID reuses an available page or allocates one within the file size limit.
func (tx *WriteTxn) allocatePageID() (uint64, error) {
	if tx == nil || tx.nextPage < 2 {
		return 0, ErrCorrupt
	}
	if count := len(tx.freePages); count > 0 && !tx.metadataAllocation {
		id := tx.freePages[count-1]
		tx.freePages = tx.freePages[:count-1]
		if id < 2 || id >= tx.nextPage {
			return 0, ErrCorrupt
		}
		tx.reusedPages = append(tx.reusedPages, id)
		return id, nil
	}
	if tx.file.maxPhysicalPages == 0 || tx.nextPage >= tx.file.maxPhysicalPages {
		tx.file.storageLimitRejections.Add(1)
		return 0, ErrStorageLimit
	}
	id := tx.nextPage
	tx.nextPage++
	if tx.nextPage == 0 {
		return 0, ErrCorrupt
	}
	return id, nil
}

// allocatePageIDs allocates the requested number of page IDs.
func (tx *WriteTxn) allocatePageIDs(count int) ([]uint64, error) {
	if count < 0 {
		return nil, ErrCorrupt
	}
	result := make([]uint64, count)
	for index := range result {
		id, err := tx.allocatePageID()
		if err != nil {
			return nil, err
		}
		result[index] = id
	}
	return result, nil
}

// readPage reads encoded page bytes from staged changes or stored pages.
func (tx *WriteTxn) readPage(pageID uint64) ([]byte, error) {
	if page := tx.byID[pageID]; page != nil {
		return append([]byte(nil), page...), nil
	}
	if pageID < 2 || pageID >= tx.file.nextPage {
		return nil, ErrCorrupt
	}
	page, err := tx.file.readDataPageUnlocked(pageID)
	if err != nil {
		return nil, err
	}
	return page.raw, nil
}

// readDataPageUnlocked reads and validates a cached or stored page while the caller holds the file lock.
func (f *File) readDataPageUnlocked(pageID uint64) (*cachedPage, error) {
	if pageID < 2 || pageID >= f.nextPage {
		return nil, ErrCorrupt
	}
	if page, ok := f.cache.get(pageID); ok {
		return page, nil
	}
	raw := make([]byte, PageSize)
	if _, err := f.file.ReadAt(raw, int64(pageID)*PageSize); err != nil {
		return nil, err
	}
	decoded, err := decodePageView(raw, pageID)
	if err != nil {
		return nil, err
	}
	return f.cache.put(pageID, raw, decoded), nil
}

// PageCacheStats samples page cache occupancy and access counters.
func (f *File) PageCacheStats() PageCacheStats {
	if f == nil {
		return PageCacheStats{}
	}
	return f.cache.stats()
}

// StorageStats returns a bounded snapshot intended for low-frequency admin
// sampling.
//
// It does not traverse database trees or perform file IO.
func (f *File) StorageStats() StorageStats {
	if f == nil {
		return StorageStats{}
	}
	f.mu.RLock()
	stats := StorageStats{
		PageSize:                PageSize,
		Generation:              f.meta.Generation,
		PhysicalPages:           f.nextPage,
		CommitSequence:          f.meta.CommitSequence,
		OldestRetainedSequence:  f.meta.OldestRetainedSequence,
		ActiveReaders:           uint64(len(f.readers)),
		DocumentCount:           f.root.DocumentCount,
		CollectionCount:         f.root.CollectionCount,
		ReusablePages:           uint64(len(f.freePages)),
		PersistentFreeSpace:     f.freeSpaceTracked && f.meta.OptionalFeatures&OptionalFeaturePersistentFreeSpace != 0,
		CommitRetentionMax:      f.commitRetentionMax,
		RetainedCommitBytes:     f.retainedCommitBytes,
		CommitRetentionMaxBytes: f.commitRetentionMaxBytes,
		RetentionPressure:       f.retentionPressure.Load(),
		StorageUsedBytes:        f.nextPage * PageSize,
		StorageMaxBytes:         f.maxPhysicalPages * PageSize,
		StorageQuotaExhausted:   f.nextPage >= f.maxPhysicalPages && len(f.freePages) == 0,
	}
	if f.meta.OldestRetainedSequence > 0 && f.meta.CommitSequence >= f.meta.OldestRetainedSequence {
		stats.RetainedCommits = f.meta.CommitSequence - f.meta.OldestRetainedSequence + 1
	}
	if stats.RetainedCommits > stats.CommitRetentionMax {
		stats.CommitRetentionOverage = stats.RetainedCommits - stats.CommitRetentionMax
	}
	if stats.RetainedCommitBytes > stats.CommitRetentionMaxBytes {
		stats.CommitRetentionByteOverage = stats.RetainedCommitBytes - stats.CommitRetentionMaxBytes
	}
	if stats.StorageUsedBytes > stats.StorageMaxBytes {
		stats.StorageByteOverage = stats.StorageUsedBytes - stats.StorageMaxBytes
	}
	for _, reader := range f.readers {
		if reader.replay {
			stats.ActiveReplayLeases++
		}
	}
	f.mu.RUnlock()
	stats.PageCache = f.cache.stats()
	stats.TreeSplits = f.treeSplits.Load()
	stats.TreeMerges = f.treeMerges.Load()
	stats.FreeSpaceLoads = f.freeSpaceLoads.Load()
	stats.FreeSpaceLoadFailures = f.freeSpaceLoadFailures.Load()
	stats.FreeSpacePublishes = f.freeSpacePublishes.Load()
	stats.FreeSpaceCandidateChecks = f.freeSpaceCandidateChecks.Load()
	stats.RetentionPrunedCommits = f.retentionPruned.Load()
	stats.RetentionPressureEvents = f.retentionPressureEvents.Load()
	stats.StorageLimitRejections = f.storageLimitRejections.Load()
	return stats
}

// Meta returns a copy of the current committed metadata.
func (f *File) Meta() Meta {
	if f == nil {
		return Meta{}
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.meta
}

// DatabaseRoot returns the current committed root while the file is open.
func (f *File) DatabaseRoot() (DatabaseRoot, error) {
	if f == nil {
		return DatabaseRoot{}, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return DatabaseRoot{}, errors.New("meldbase storage: file is closed")
	}
	return f.databaseRootUnlocked()
}

// databaseRootUnlocked returns the cached committed root while the caller holds the file lock.
func (f *File) databaseRootUnlocked() (DatabaseRoot, error) {
	return f.root, nil
}

// readDatabaseRoot reads the root page named by metadata and checks its commit sequence.
func readDatabaseRoot(file *os.File, meta Meta) (DatabaseRoot, error) {
	if meta.RootPage == 0 {
		return DatabaseRoot{CommitSequence: meta.CommitSequence}, nil
	}
	page := make([]byte, PageSize)
	if _, err := file.ReadAt(page, int64(meta.RootPage)*PageSize); err != nil {
		return DatabaseRoot{}, err
	}
	root, _, err := DecodeDatabaseRoot(page, meta.RootPage)
	if err == nil && root.CommitSequence != meta.CommitSequence {
		return DatabaseRoot{}, ErrCorrupt
	}
	return root, err
}

// TreeGet looks up a stored tree key and returns a copied value.
func (f *File) TreeGet(rootPage uint64, kind TreeKind, key []byte) ([]byte, bool, error) {
	if f == nil {
		return nil, false, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return nil, false, errors.New("meldbase storage: file is closed")
	}
	return f.treeGetUnlocked(rootPage, kind, key)
}

// TreeScan collects copied entries from a stored tree within the requested range and limit.
func (f *File) TreeScan(rootPage uint64, kind TreeKind, start, end []byte, limit int) ([]KeyValue, error) {
	if f == nil {
		return nil, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return nil, errors.New("meldbase storage: file is closed")
	}
	iterator, err := newTreeIterator(f, rootPage, kind, start, end, limit)
	if err != nil {
		return nil, err
	}
	// The iterator normally owns the file lock per step. TreeScan already holds
	// it, so drive the unlocked implementation directly to preserve this legacy
	// materializing API without recursively acquiring the RWMutex.
	result := make([]KeyValue, 0)
	for iterator.nextUnlocked() {
		result = append(result, KeyValue{
			Key: append([]byte(nil), iterator.Key()...), Value: append([]byte(nil), iterator.Value()...),
		})
	}
	return result, iterator.err
}

// Close releases the writer lock, closes the file, and wakes commit stream waiters.
func (f *File) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil
	}
	err := syscall.Flock(int(f.file.Fd()), syscall.LOCK_UN)
	err = errors.Join(err, f.file.Close())
	f.file = nil
	close(f.changed)
	f.changed = nil
	return err
}

// validateRoot checks that a stored root matches metadata and references pages within the file bounds.
func validateRoot(file *os.File, meta Meta) error {
	if meta.RootPage == 0 {
		if meta.CommitSequence != 0 {
			return ErrCorrupt
		}
		return nil
	}
	if meta.RootPage >= meta.PhysicalPageCount {
		return ErrCorrupt
	}
	page := make([]byte, PageSize)
	if _, err := file.ReadAt(page, int64(meta.RootPage)*PageSize); err != nil {
		if errors.Is(err, io.EOF) {
			return ErrCorrupt
		}
		return err
	}
	root, decoded, err := DecodeDatabaseRoot(page, meta.RootPage)
	if err != nil || root.CommitSequence != meta.CommitSequence || decoded.Generation != meta.Generation ||
		root.OldestRetainedSequence != meta.OldestRetainedSequence {
		return ErrCorrupt
	}
	if root.IndexBuildCatalogRoot != 0 && meta.RequiredFeatures&RequiredFeatureShadowIndexBuilds == 0 {
		return ErrCorrupt
	}
	for _, referenced := range []uint64{root.CatalogRoot, root.CommitLogRoot, root.FreeSpaceRoot, root.IndexBuildCatalogRoot} {
		if referenced != 0 && (referenced < 2 || referenced >= meta.PhysicalPageCount) {
			return ErrCorrupt
		}
	}
	return nil
}

// syncDirectory flushes directory metadata to durable storage.
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
