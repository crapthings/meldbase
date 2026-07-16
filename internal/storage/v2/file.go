package v2

import (
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
	ErrLocked              = errors.New("meldbase storage v2: database is locked")
	ErrRecoveryRequired    = errors.New("meldbase storage v2: recovery required")
	ErrInvalidStorageLimit = errors.New("meldbase storage v2: invalid storage limit")
	ErrStorageLimit        = errors.New("meldbase storage v2: storage limit exceeded")
)

type OpenOptions struct {
	RequireClean              bool
	CommitRetentionMaxCommits uint64
	CommitRetentionMaxBytes   uint64
	MaxFileBytes              uint64
}

const (
	DefaultCommitRetentionMaxCommits uint64 = 10_000
	DefaultCommitRetentionMaxBytes   uint64 = 256 << 20
	DefaultMaxFileBytes              uint64 = 8 << 30
)

type faultPoint uint8

const (
	faultAfterPageWrite faultPoint = 1 + iota
	faultBeforeDataSync
	faultAfterDataSync
	faultAfterMetaWrite
	faultAfterMetaSync
)

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

type readerPin struct {
	generation, sequence, rootPage uint64
	replay                         bool
}

type stagedPage struct {
	id   uint64
	data []byte
}

type existingFileState struct {
	meta     Meta
	root     DatabaseRoot
	metaSlot int
	metas    [2]Meta
	metaOK   [2]bool
	nextPage uint64
}

// OpenReport describes only recovery decisions made while opening a file. It
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

// WriteTxn stages immutable pages in memory. Nothing becomes visible until the
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

func Open(path string) (*File, Meta, error) {
	file, meta, _, err := OpenWithReport(path)
	return file, meta, err
}

func OpenWithReport(path string) (*File, Meta, OpenReport, error) {
	return OpenWithOptions(path, OpenOptions{})
}

func OpenWithOptions(path string, options OpenOptions) (*File, Meta, OpenReport, error) {
	if path == "" {
		return nil, Meta{}, OpenReport{}, errors.New("meldbase storage v2: empty path")
	}
	maxPhysicalPages, err := normalizeMaxFileBytes(options.MaxFileBytes)
	if err != nil {
		return nil, Meta{}, OpenReport{}, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
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
	if info.Size() == 0 {
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
	if options.RequireClean && (report.FallbackToOlderRoot || report.MetaRedundancyDegraded || report.TrailingBytesRemoved != 0) {
		return cleanup(ErrRecoveryRequired)
	}
	if effectiveSize != info.Size() {
		if err := file.Truncate(effectiveSize); err != nil {
			return cleanup(err)
		}
		if err := file.Sync(); err != nil {
			return cleanup(err)
		}
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
	return opened, state.meta, report, nil
}

func normalizedCommitRetention(value uint64) uint64 {
	if value == 0 {
		return DefaultCommitRetentionMaxCommits
	}
	return value
}

func normalizedCommitRetentionBytes(value uint64) uint64 {
	if value == 0 {
		return DefaultCommitRetentionMaxBytes
	}
	return value
}

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

func (state existingFileState) open(file *os.File) *File {
	return &File{
		file: file, meta: state.meta, root: state.root, metaSlot: state.metaSlot, metas: state.metas, metaOK: state.metaOK,
		nextPage: state.nextPage, readers: make(map[uint64]readerPin), changed: make(chan struct{}), cache: newPageCache(defaultCachedPages),
	}
}

// CommitRoot appends one experimental DatabaseRoot generation and publishes it
// through the inactive meta slot. Tree roots referenced by root must already be
// durable; the first vertical slice uses zero roots to exercise atomicity.
func (f *File) CommitRoot(root DatabaseRoot) error {
	return f.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		if root.CommitSequence != tx.Sequence() {
			return DatabaseRoot{}, ErrCorrupt
		}
		return root, nil
	})
}

// Update builds and atomically publishes one copy-on-write generation. The
// callback must not retain tx and must not perform network or user callbacks.
func (f *File) Update(build func(*WriteTxn) (DatabaseRoot, error)) error {
	if f == nil || build == nil {
		return errors.New("meldbase storage v2: invalid update")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updateUnlocked(true, build)
}

func (f *File) updateUnlocked(advanceSequence bool, build func(*WriteTxn) (DatabaseRoot, error)) error {
	if f.file == nil {
		return errors.New("meldbase storage v2: file is closed")
	}
	if f.fatalErr != nil {
		return f.fatalErr
	}
	baseRoot, err := f.databaseRootUnlocked()
	if err != nil {
		return err
	}
	generation := f.meta.Generation + 1
	sequence := f.meta.CommitSequence
	if advanceSequence {
		sequence++
	}
	if generation == 0 || (advanceSequence && sequence == 0) {
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
	if advanceSequence {
		close(f.changed)
		f.changed = make(chan struct{})
	}
	return nil
}

func (f *File) inject(point faultPoint) error {
	if f.fault == nil {
		return nil
	}
	return f.fault(point)
}

func (f *File) failCommit(err error) error {
	if err == nil {
		err = ErrCorrupt
	}
	f.fatalErr = err
	return err
}

func (tx *WriteTxn) Sequence() uint64 { return tx.sequence }

func (tx *WriteTxn) requireFeature(feature uint64) error {
	if tx == nil || feature == 0 || feature&^SupportedRequiredFeatures != 0 {
		return ErrUnsupportedFeature
	}
	tx.requiredFeatures |= feature
	return nil
}
func (tx *WriteTxn) BaseRoot() DatabaseRoot {
	if tx == nil {
		return DatabaseRoot{}
	}
	return tx.baseRoot
}

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

func (f *File) PageCacheStats() PageCacheStats {
	if f == nil {
		return PageCacheStats{}
	}
	return f.cache.stats()
}

// StorageStats returns a bounded snapshot intended for low-frequency admin
// sampling. It does not traverse database trees or perform file IO.
func (f *File) StorageStats() StorageStats {
	if f == nil {
		return StorageStats{}
	}
	f.mu.RLock()
	stats := StorageStats{
		PageSize:                PageSize,
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

func (f *File) Meta() Meta {
	if f == nil {
		return Meta{}
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.meta
}

func (f *File) DatabaseRoot() (DatabaseRoot, error) {
	if f == nil {
		return DatabaseRoot{}, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return DatabaseRoot{}, errors.New("meldbase storage v2: file is closed")
	}
	return f.databaseRootUnlocked()
}

func (f *File) databaseRootUnlocked() (DatabaseRoot, error) {
	return f.root, nil
}

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

func (f *File) TreeGet(rootPage uint64, kind TreeKind, key []byte) ([]byte, bool, error) {
	if f == nil {
		return nil, false, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return nil, false, errors.New("meldbase storage v2: file is closed")
	}
	return f.treeGetUnlocked(rootPage, kind, key)
}

func (f *File) TreeScan(rootPage uint64, kind TreeKind, start, end []byte, limit int) ([]KeyValue, error) {
	if f == nil {
		return nil, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
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

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
