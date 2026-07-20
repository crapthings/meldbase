package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"syscall"
)

var ErrReclamationConflict = errors.New("meldbase storage v2: online reclamation conflicted with a commit")

const maxSemanticAuditTreeViews = 1024

type ReachabilityStats struct {
	PhysicalPages    uint64
	ReachablePages   uint64
	ReclaimablePages uint64
	PinnedSnapshots  uint64
}

// IndexAuditFunc recomputes one logical Secondary key from the canonical stored
// document. It is used only by the offline verifier; normal open, reachability,
// reclamation and commit paths never invoke it.
type IndexAuditFunc func(IndexMeta, [16]byte, []byte) (key []byte, indexed bool, err error)

type reclamationAuditToken struct {
	generation      uint64
	nextPage        uint64
	metaSlot        int
	metas           [2]Meta
	metaOK          [2]bool
	freePoolVersion uint64
}

type reachabilityWalker struct {
	file                        *File
	ctx                         context.Context
	pages                       map[uint64]PageType
	visiting                    map[uint64]struct{}
	step                        func()
	indexAudit                  IndexAuditFunc
	semanticIndexBuildsVerified bool
	treeViews                   map[uint64]semanticAuditTreeView
	treeViewHits                uint64
}

type semanticAuditTreeView struct {
	kind TreeKind
	view treeNodeView
}

// Reachability audits all pages protected by both fallback metas and active
// snapshot pins. It is intentionally explicit and potentially expensive; the
// commit hot path never runs it.
func (f *File) Reachability() (ReachabilityStats, error) {
	return f.ReachabilityContext(context.Background())
}

func (f *File) ReachabilityContext(ctx context.Context) (ReachabilityStats, error) {
	if f == nil {
		return ReachabilityStats{}, ErrCorrupt
	}
	if err := contextErr(ctx); err != nil {
		return ReachabilityStats{}, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	stats, _, err := f.reachabilityUnlocked(ctx)
	return stats, err
}

func (f *File) reachabilityUnlocked(ctx context.Context) (ReachabilityStats, *reachabilityWalker, error) {
	return f.reachabilityUnlockedWithIndexAudit(ctx, nil)
}

func (f *File) reachabilityUnlockedWithIndexAudit(ctx context.Context, indexAudit IndexAuditFunc) (ReachabilityStats, *reachabilityWalker, error) {
	if err := contextErr(ctx); err != nil {
		return ReachabilityStats{}, nil, err
	}
	if f.file == nil {
		return ReachabilityStats{}, nil, errors.New("meldbase storage v2: file is closed")
	}
	walker := &reachabilityWalker{
		file: f, ctx: ctx, pages: make(map[uint64]PageType), visiting: make(map[uint64]struct{}),
		step: f.reclamationScanStep, indexAudit: indexAudit, semanticIndexBuildsVerified: indexAudit != nil,
	}
	if indexAudit != nil {
		walker.treeViews = make(map[uint64]semanticAuditTreeView, maxSemanticAuditTreeViews)
	}
	for slot := range 2 {
		if f.metaOK[slot] && f.metas[slot].RootPage != 0 {
			if err := walker.databaseRoot(f.metas[slot].RootPage); err != nil {
				return ReachabilityStats{}, nil, err
			}
		}
	}
	for _, pin := range f.readers {
		if pin.rootPage != 0 {
			if err := walker.databaseRoot(pin.rootPage); err != nil {
				return ReachabilityStats{}, nil, err
			}
		}
	}
	for _, pageID := range f.freePages {
		if pageID < 2 || pageID >= f.nextPage {
			return ReachabilityStats{}, nil, ErrCorrupt
		}
		if _, protected := walker.pages[pageID]; protected {
			return ReachabilityStats{}, nil, fmt.Errorf("%w: reusable page %d is reachable", ErrCorrupt, pageID)
		}
	}
	reachable := uint64(len(walker.pages))
	physicalDataPages := uint64(0)
	if f.nextPage > 2 {
		physicalDataPages = f.nextPage - 2
	}
	if reachable > physicalDataPages {
		return ReachabilityStats{}, nil, ErrCorrupt
	}
	return ReachabilityStats{
		PhysicalPages: f.nextPage, ReachablePages: reachable,
		ReclaimablePages: physicalDataPages - reachable, PinnedSnapshots: uint64(len(f.readers)),
	}, walker, nil
}

// ReclaimPages performs an explicit protected-root audit and makes every page
// unreachable from both valid Meta roots and all active readers available to
// subsequent copy-on-write transactions. It does not mutate disk by itself.
func (f *File) ReclaimPages() (ReachabilityStats, error) {
	return f.ReclaimPagesContext(context.Background())
}

func (f *File) ReclaimPagesContext(ctx context.Context) (ReachabilityStats, error) {
	if f == nil {
		return ReachabilityStats{}, ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fatalErr != nil {
		return ReachabilityStats{}, f.fatalErr
	}
	stats, walker, err := f.reachabilityUnlocked(ctx)
	if err != nil {
		return ReachabilityStats{}, err
	}
	free := make([]uint64, 0, stats.ReclaimablePages)
	for pageID := uint64(2); pageID < f.nextPage; pageID++ {
		if err := contextErr(ctx); err != nil {
			return ReachabilityStats{}, err
		}
		if _, protected := walker.pages[pageID]; !protected {
			free = append(free, pageID)
		}
	}
	if uint64(len(free)) != stats.ReclaimablePages {
		return ReachabilityStats{}, ErrCorrupt
	}
	f.freePages = free
	f.freePoolVersion++
	// Online audits compare the complete pool captured at their start. A direct
	// audit can change it without publishing a new Meta generation.
	if f.meta.CommitSequence != 0 || len(free) > 0 {
		f.freeSpaceTracked = true
		f.freeSpaceNeedsRebuild = true
	}
	return stats, nil
}

// ReclaimPagesOptimisticContext scans a duplicate read handle without holding
// the writer mutex. Installation takes the mutex only long enough to prove that
// Meta generation, physical high-water mark and the previous free pool are
// unchanged. A concurrent commit causes a bounded retry rather than extending
// the commit pause across the full graph walk.
func (f *File) ReclaimPagesOptimisticContext(ctx context.Context, maxAttempts int, persistFreeSpace bool) (ReachabilityStats, int, error) {
	if f == nil || maxAttempts <= 0 {
		return ReachabilityStats{}, 0, ErrCorrupt
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := contextErr(ctx); err != nil {
			return ReachabilityStats{}, attempt - 1, err
		}
		audit, token, err := f.captureReclamationAudit()
		if err != nil {
			return ReachabilityStats{}, attempt - 1, err
		}
		stats, walker, scanErr := audit.reachabilityUnlocked(ctx)
		closeErr := audit.file.Close()
		audit.file = nil
		if scanErr != nil {
			return ReachabilityStats{}, attempt, scanErr
		}
		if closeErr != nil {
			return ReachabilityStats{}, attempt, closeErr
		}
		free := make([]uint64, 0, stats.ReclaimablePages)
		for pageID := uint64(2); pageID < token.nextPage; pageID++ {
			if err := contextErr(ctx); err != nil {
				return ReachabilityStats{}, attempt, err
			}
			if _, protected := walker.pages[pageID]; !protected {
				free = append(free, pageID)
			}
		}
		if uint64(len(free)) != stats.ReclaimablePages {
			return ReachabilityStats{}, attempt, ErrCorrupt
		}
		if f.installReclamationAudit(token, free, persistFreeSpace) {
			return stats, attempt, nil
		}
	}
	return ReachabilityStats{}, maxAttempts, ErrReclamationConflict
}

func (f *File) captureReclamationAudit() (*File, reclamationAuditToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil, reclamationAuditToken{}, errors.New("meldbase storage v2: file is closed")
	}
	if f.fatalErr != nil {
		return nil, reclamationAuditToken{}, f.fatalErr
	}
	fd, err := syscall.Dup(int(f.file.Fd()))
	if err != nil {
		return nil, reclamationAuditToken{}, err
	}
	syscall.CloseOnExec(fd)
	duplicate := os.NewFile(uintptr(fd), f.file.Name()+" (reclamation audit)")
	if duplicate == nil {
		_ = syscall.Close(fd)
		return nil, reclamationAuditToken{}, ErrCorrupt
	}
	readers := make(map[uint64]readerPin, len(f.readers))
	for id, pin := range f.readers {
		readers[id] = pin
	}
	// Committed pools are immutable slices: transactions copy before consuming
	// and every publication replaces the slice. Capturing the header therefore
	// avoids an O(free pages) writer pause while the version token detects any
	// replacement before installation.
	freePages := f.freePages
	token := reclamationAuditToken{
		generation: f.meta.Generation, nextPage: f.nextPage, metaSlot: f.metaSlot,
		metas: f.metas, metaOK: f.metaOK, freePoolVersion: f.freePoolVersion,
	}
	audit := &File{
		file: duplicate, meta: f.meta, metaSlot: f.metaSlot, metas: f.metas, metaOK: f.metaOK,
		nextPage: f.nextPage, readers: readers, freePages: freePages, cache: newPageCache(defaultCachedPages),
		reclamationScanStep: f.reclamationScanStep,
	}
	return audit, token, nil
}

func (f *File) installReclamationAudit(token reclamationAuditToken, free []uint64, persistFreeSpace bool) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil || f.fatalErr != nil || f.meta.Generation != token.generation ||
		f.nextPage != token.nextPage || f.metaSlot != token.metaSlot || f.metas != token.metas ||
		f.metaOK != token.metaOK || f.freePoolVersion != token.freePoolVersion {
		return false
	}
	f.freePages = free
	f.freePoolVersion++
	if persistFreeSpace && (f.meta.CommitSequence != 0 || len(free) > 0) {
		f.freeSpaceTracked = true
		f.freeSpaceNeedsRebuild = true
	} else if !persistFreeSpace {
		// A memory-only audit must not move extent construction into the next
		// business commit. That would turn a low-pause background scan into an
		// unbounded writer-lock step on the hot path.
		f.freeSpaceTracked = false
		f.freeSpaceNeedsRebuild = false
	}
	return true
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return ErrCorrupt
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (walker *reachabilityWalker) checkContext() error {
	if walker == nil {
		return ErrCorrupt
	}
	if walker.step != nil {
		walker.step()
	}
	return contextErr(walker.ctx)
}

func (walker *reachabilityWalker) databaseRoot(pageID uint64) error {
	if err := walker.checkContext(); err != nil {
		return err
	}
	if _, active := walker.visiting[pageID]; active {
		return ErrCorrupt
	}
	if existing, found := walker.pages[pageID]; found {
		if existing != PageDatabaseRoot {
			return ErrCorrupt
		}
		return nil
	}
	walker.visiting[pageID] = struct{}{}
	defer delete(walker.visiting, pageID)
	raw, page, fresh, err := walker.page(pageID, PageDatabaseRoot)
	if err != nil || !fresh {
		return err
	}
	root, _, err := DecodeDatabaseRoot(raw, pageID)
	if err != nil || root.CommitSequence != page.BornSequence {
		return ErrCorrupt
	}
	if err := walker.tree(root.CatalogRoot, TreeCatalog); err != nil {
		return err
	}
	if err := walker.tree(root.CommitLogRoot, TreeCommitLog); err != nil {
		return err
	}
	if err := walker.tree(root.IndexBuildCatalogRoot, TreeIndexBuildCatalog); err != nil {
		return err
	}
	// Free-space pages are optional acceleration metadata, not protected data.
	// A fallback generation may safely lose this map and reopen with an empty
	// reuse pool; business reachability remains authoritative.
	return nil
}

func (walker *reachabilityWalker) tree(pageID uint64, kind TreeKind) error {
	if err := walker.checkContext(); err != nil {
		return err
	}
	if pageID == 0 {
		return nil
	}
	expectedLeaf, expectedBranch := treePageTypes(kind)
	if _, active := walker.visiting[pageID]; active {
		return ErrCorrupt
	}
	if existing, found := walker.pages[pageID]; found {
		if existing != expectedLeaf && existing != expectedBranch {
			return ErrCorrupt
		}
		return nil
	}
	walker.visiting[pageID] = struct{}{}
	defer delete(walker.visiting, pageID)
	_, page, fresh, err := walker.pageEither(pageID, expectedLeaf, expectedBranch)
	if err != nil || !fresh {
		return err
	}
	node, err := decodeTreeNode(page)
	if err != nil {
		return err
	}
	if !node.leaf {
		for _, child := range node.children {
			if err := walker.tree(child.pageID, kind); err != nil {
				return err
			}
		}
		return nil
	}
	for index, value := range node.values {
		if err := walker.checkContext(); err != nil {
			return err
		}
		switch kind {
		case TreeCatalog:
			if bytes.Equal(node.keys[index], systemCatalogKey) {
				directory, err := decodeSystemDirectory(value)
				if err != nil {
					return ErrCorrupt
				}
				if directory.Root >= walker.file.nextPage {
					return ErrCorrupt
				}
				rootPage := make([]byte, PageSize)
				if _, err := walker.file.file.ReadAt(rootPage, int64(directory.Root)*PageSize); err != nil {
					return err
				}
				page, err := DecodePage(rootPage, directory.Root)
				if err != nil || (page.Type != PageSystemLeaf && page.Type != PageSystemBranch) {
					return ErrCorrupt
				}
				rootNode, err := decodeTreeNode(page)
				if err != nil || rootNode.count != directory.Count {
					return ErrCorrupt
				}
				if err := walker.tree(directory.Root, TreeSystem); err != nil {
					return err
				}
				continue
			}
			meta, err := decodeCollectionMeta(value)
			if err != nil {
				return ErrCorrupt
			}
			if err := walker.tree(meta.PrimaryRoot, TreePrimary); err != nil {
				return err
			}
			if err := walker.tree(meta.OrderRoot, TreeOrder); err != nil {
				return err
			}
			if err := walker.tree(meta.IndexCatalogRoot, TreeIndexCatalog); err != nil {
				return err
			}
			if err := walker.auditCollection(meta); err != nil {
				return err
			}
		case TreeIndexCatalog:
			meta, err := decodeIndexMeta(string(node.keys[index]), value)
			if err != nil {
				return err
			}
			if err := validateIndexMetaFeatures(walker.file.meta.RequiredFeatures, meta); err != nil {
				return err
			}
			if err := walker.tree(meta.Root, TreeSecondary); err != nil {
				return err
			}
		case TreePrimary:
			_, descriptor, err := decodeDocumentRecordDescriptor(value)
			if err != nil {
				return err
			}
			if _, err := walker.overflowValue(descriptor, PageDocumentOverflow, inlineDocumentLimit, maxDocumentBytes); err != nil {
				return err
			}
		case TreeOrder:
			if len(node.keys[index]) != 8 || binary.BigEndian.Uint64(node.keys[index]) == 0 || len(value) != 16 || allZero(value) {
				return ErrCorrupt
			}
		case TreeCommitLog:
			if len(node.keys[index]) != 12 {
				return ErrCorrupt
			}
			logical, err := walker.overflowValue(value, PageCommitOverflow, inlineCommitValueLimit, maxCommitValueBytes)
			if err != nil {
				return err
			}
			if binary.BigEndian.Uint32(node.keys[index][8:]) == 0 {
				if len(logical) != commitHeaderBytes || string(logical[:8]) != string(commitMagic[:]) || binary.LittleEndian.Uint16(logical[8:10]) != FormatVersion ||
					!allZero(logical[10:16]) || !allZero(logical[52:56]) {
					return ErrCorrupt
				}
				catalogRoot := binary.LittleEndian.Uint64(logical[56:64])
				if catalogRoot != 0 {
					if catalogRoot < 2 {
						return ErrCorrupt
					}
					if err := walker.tree(catalogRoot, TreeCatalog); err != nil {
						return err
					}
				}
				continue
			}
			change, err := decodeCommitChange(logical)
			if err != nil {
				return err
			}
			for _, reference := range []*DocumentVersionRef{change.BeforeRef, change.AfterRef} {
				if reference != nil {
					if err := walker.tree(reference.PrimaryRoot, TreePrimary); err != nil {
						return err
					}
				}
			}
		case TreeSecondary:
			if _, _, _, err := secondaryKeyParts(node.keys[index]); err != nil || len(value) != 1 || value[0] != 0 {
				return ErrCorrupt
			}
		case TreeSystem:
			if len(node.keys[index]) == 0 || len(node.keys[index]) > maxSystemKeyBytes {
				return ErrCorrupt
			}
			logical, err := walker.overflowValue(value, PageSystemOverflow, inlineSystemValueLimit, maxSystemValueBytes)
			if err != nil {
				return err
			}
			if len(logical) == 0 {
				return ErrCorrupt
			}
		case TreeIndexBuildCatalog:
			meta, err := decodeIndexBuildMeta(node.keys[index], value)
			if err != nil {
				return err
			}
			if err := validateIndexBuildMetaFeatures(walker.file.meta.RequiredFeatures, meta); err != nil {
				return err
			}
			if len(meta.Fields) > 0 && compoundIndexFields(meta.Fields) &&
				walker.file.meta.RequiredFeatures&RequiredFeatureCompoundIndexes == 0 {
				return ErrCorrupt
			}
			if err := walker.tree(meta.SourceCatalogRoot, TreeCatalog); err != nil {
				return err
			}
			if meta.AppliedCatalogRoot != 0 {
				if err := walker.tree(meta.AppliedCatalogRoot, TreeCatalog); err != nil {
					return err
				}
			}
			if err := walker.tree(meta.ShadowRoot, TreeSecondary); err != nil {
				return err
			}
			if err := walker.auditIndexBuild(meta); err != nil {
				return err
			}
		}
	}
	return nil
}

// auditCollection proves the bidirectional Primary↔Order mapping and validates
// every Secondary suffix against the same durable document position. It runs
// only as part of explicit Reachability, never on open or a commit hot path.
func (walker *reachabilityWalker) auditCollection(meta CollectionMeta) error {
	if walker == nil || walker.file == nil {
		return ErrCorrupt
	}
	type primaryAuditRecord struct {
		position uint64
		ordered  bool
	}
	primaryRecords := make(map[[16]byte]primaryAuditRecord, meta.DocumentCount)
	primary, err := newTreeIterator(walker.file, meta.PrimaryRoot, TreePrimary, nil, nil, 0)
	if err != nil {
		return fmt.Errorf("Primary iterator: %w", err)
	}
	var primaryCount uint64
	for primary.nextUnlocked() {
		if err := walker.checkContext(); err != nil {
			return err
		}
		key, stored := primary.Key(), primary.Value()
		if len(key) != 16 || allZero(key) {
			return fmt.Errorf("%w: Primary ID", ErrCorrupt)
		}
		var id [16]byte
		copy(id[:], key)
		if _, duplicate := primaryRecords[id]; duplicate {
			return fmt.Errorf("%w: duplicate Primary ID", ErrCorrupt)
		}
		position, _, err := decodeDocumentRecordDescriptor(stored)
		if err != nil {
			return fmt.Errorf("Primary descriptor: %w", err)
		}
		primaryRecords[id] = primaryAuditRecord{position: position}
		primaryCount++
	}
	if primary.Err() != nil || primaryCount != meta.DocumentCount {
		return fmt.Errorf("%w: Primary count %d/%d", ErrCorrupt, primaryCount, meta.DocumentCount)
	}
	order, err := newTreeIterator(walker.file, meta.OrderRoot, TreeOrder, nil, nil, 0)
	if err != nil {
		return fmt.Errorf("Order iterator: %w", err)
	}
	var orderCount uint64
	for order.nextUnlocked() {
		if err := walker.checkContext(); err != nil {
			return err
		}
		key, value := order.Key(), order.Value()
		if len(key) != 8 || binary.BigEndian.Uint64(key) == 0 || len(value) != 16 || allZero(value) {
			return fmt.Errorf("%w: Order entry", ErrCorrupt)
		}
		var id [16]byte
		copy(id[:], value)
		record, exists := primaryRecords[id]
		if !exists {
			return fmt.Errorf("%w: Order to Primary", ErrCorrupt)
		}
		if record.ordered {
			return fmt.Errorf("%w: duplicate Order ID", ErrCorrupt)
		}
		if record.position != binary.BigEndian.Uint64(key) {
			return fmt.Errorf("%w: Order position", ErrCorrupt)
		}
		record.ordered = true
		primaryRecords[id] = record
		orderCount++
	}
	if order.Err() != nil || orderCount != meta.DocumentCount {
		return fmt.Errorf("%w: Order count %d/%d", ErrCorrupt, orderCount, meta.DocumentCount)
	}
	indexCatalog, err := newTreeIterator(walker.file, meta.IndexCatalogRoot, TreeIndexCatalog, nil, nil, 0)
	if err != nil {
		return fmt.Errorf("IndexCatalog iterator: %w", err)
	}
	semanticIndexes := make([]IndexMeta, 0)
	for !indexCatalog.done && indexCatalog.nextUnlocked() {
		if err := walker.checkContext(); err != nil {
			return err
		}
		indexMeta, err := decodeIndexMeta(string(indexCatalog.Key()), indexCatalog.Value())
		if err != nil {
			return err
		}
		if err := validateIndexMetaFeatures(walker.file.meta.RequiredFeatures, indexMeta); err != nil {
			return err
		}
		secondary, err := newTreeIterator(walker.file, indexMeta.Root, TreeSecondary, nil, nil, 0)
		if err != nil {
			return err
		}
		var entryCount uint64
		seenDocuments := make(map[[16]byte]struct{}, indexMeta.EntryCount)
		var previousKey []byte
		for secondary.nextUnlocked() {
			if err := walker.checkContext(); err != nil {
				return err
			}
			storedKey, position, id, err := secondaryKeyParts(secondary.Key())
			if err != nil || len(secondary.Value()) != 1 || secondary.Value()[0] != 0 {
				return fmt.Errorf("%w: Secondary entry", ErrCorrupt)
			}
			if _, duplicate := seenDocuments[id]; duplicate {
				return fmt.Errorf("%w: duplicate Secondary ID", ErrCorrupt)
			}
			if indexMeta.Unique {
				if bytes.Equal(previousKey, storedKey) {
					return fmt.Errorf("%w: duplicate unique Secondary key", ErrCorrupt)
				}
				previousKey = append(previousKey[:0], storedKey...)
			}
			seenDocuments[id] = struct{}{}
			primary, exists := primaryRecords[id]
			if !exists {
				return fmt.Errorf("%w: Secondary to Primary", ErrCorrupt)
			}
			if primary.position != position {
				return fmt.Errorf("%w: Secondary position", ErrCorrupt)
			}
			entryCount++
		}
		if secondary.Err() != nil || entryCount != indexMeta.EntryCount {
			return fmt.Errorf("%w: Secondary count %d/%d", ErrCorrupt, entryCount, indexMeta.EntryCount)
		}
		if walker.indexAudit != nil {
			semanticIndexes = append(semanticIndexes, indexMeta)
		}
	}
	if err := indexCatalog.Err(); err != nil {
		return fmt.Errorf("IndexCatalog scan: %w", err)
	}
	if len(semanticIndexes) > 0 {
		if err := walker.auditPublishedIndexSemantics(meta, semanticIndexes); err != nil {
			return err
		}
	}
	return nil
}

// auditPublishedIndexSemantics decodes each Primary document once and proves
// all published Secondary indexes against it. Structural scans have already
// established each Secondary's count, uniqueness and one-entry-per-document
// invariants. Therefore every expected complete key existing plus an equal
// expected/stored count also proves that no extra or semantically wrong entry
// remains.
func (walker *reachabilityWalker) auditPublishedIndexSemantics(collection CollectionMeta, indexes []IndexMeta) error {
	primary, err := newTreeIterator(walker.file, collection.PrimaryRoot, TreePrimary, nil, nil, 0)
	if err != nil {
		return err
	}
	expectedCounts := make([]uint64, len(indexes))
	for primary.nextUnlocked() {
		if err := walker.checkContext(); err != nil {
			return err
		}
		var id [16]byte
		if len(primary.Key()) != len(id) {
			return fmt.Errorf("%w: Primary ID", ErrCorrupt)
		}
		copy(id[:], primary.Key())
		position, descriptor, err := decodeDocumentRecordDescriptor(primary.Value())
		if err != nil {
			return err
		}
		document, err := walker.overflowValue(descriptor, PageDocumentOverflow, inlineDocumentLimit, maxDocumentBytes)
		if err != nil {
			return err
		}
		for index := range indexes {
			if err := walker.checkContext(); err != nil {
				return err
			}
			expected, indexed, err := walker.indexAudit(indexes[index], id, document)
			if err != nil {
				return fmt.Errorf("%w: Primary semantic key", ErrCorrupt)
			}
			if !indexed {
				continue
			}
			complete, err := secondaryKey(expected, position, id)
			if err != nil {
				return fmt.Errorf("%w: Primary semantic key", ErrCorrupt)
			}
			value, exists, err := walker.treeGetUnlocked(indexes[index].Root, TreeSecondary, complete)
			if err != nil || !exists || len(value) != 1 || value[0] != 0 {
				return fmt.Errorf("%w: Primary missing Secondary", ErrCorrupt)
			}
			expectedCounts[index]++
		}
	}
	if err := primary.Err(); err != nil {
		return err
	}
	for index := range indexes {
		if expectedCounts[index] != indexes[index].EntryCount {
			return fmt.Errorf("%w: Secondary semantic count %d/%d", ErrCorrupt, expectedCounts[index], indexes[index].EntryCount)
		}
	}
	return nil
}

// treeGetUnlocked keeps a small verifier-lifetime cache of immutable decoded
// tree views. Offline semantic verification performs many exact lookups after
// the structural walk; reusing those views avoids repeatedly taking the shared
// page-cache lock and validating the same node envelope. The fixed cap bounds
// retained page bytes to at most maxSemanticAuditTreeViews*PageSize, and normal
// reachability/reclamation use the existing File lookup unchanged.
func (walker *reachabilityWalker) treeGetUnlocked(rootPage uint64, kind TreeKind, key []byte) ([]byte, bool, error) {
	if walker == nil || walker.file == nil {
		return nil, false, ErrCorrupt
	}
	if walker.treeViews == nil {
		return walker.file.treeGetUnlocked(rootPage, kind, key)
	}
	if rootPage == 0 {
		return nil, false, nil
	}
	if !validTreeKind(kind) || len(key) == 0 {
		return nil, false, ErrCorrupt
	}
	pageID := rootPage
	leafType, branchType := treePageTypes(kind)
	for {
		cached, exists := walker.treeViews[pageID]
		var view treeNodeView
		if exists {
			if cached.kind != kind {
				return nil, false, ErrCorrupt
			}
			walker.treeViewHits++
			view = cached.view
		} else {
			page, err := walker.file.readDataPageUnlocked(pageID)
			if err != nil {
				return nil, false, err
			}
			if page.page.Type != leafType && page.page.Type != branchType {
				return nil, false, ErrCorrupt
			}
			view, err = decodeTreeNodeView(page.page)
			if err != nil {
				return nil, false, err
			}
			if len(walker.treeViews) < maxSemanticAuditTreeViews {
				walker.treeViews[pageID] = semanticAuditTreeView{kind: kind, view: view}
			}
		}
		position := sort.Search(view.count, func(index int) bool {
			candidate, _, _, _, _ := view.entry(index)
			return bytes.Compare(candidate, key) >= 0
		})
		if view.leaf {
			if position >= view.count {
				return nil, false, nil
			}
			candidate, value, _, _, err := view.entry(position)
			if err != nil {
				return nil, false, err
			}
			if !bytes.Equal(candidate, key) {
				return nil, false, nil
			}
			return append([]byte(nil), value...), true, nil
		}
		if position < view.count {
			candidate, _, _, _, err := view.entry(position)
			if err != nil {
				return nil, false, err
			}
			if bytes.Equal(candidate, key) {
				position++
			}
		}
		var err error
		pageID, _, err = view.child(position)
		if err != nil {
			return nil, false, err
		}
	}
}

// auditIndexBuild proves the portion of a private shadow tree that its durable
// state machine claims. Scan/scan-failed records are complete through ScanAfter
// in SourceCatalogRoot. Catch-up and ready records are complete at the exact
// AppliedCatalogRoot watermark. Legacy catch-up records without that root remain
// structurally readable but are explicitly reported as not semantically proven.
func (walker *reachabilityWalker) auditIndexBuild(build IndexBuildMeta) error {
	if walker == nil || walker.file == nil {
		return ErrCorrupt
	}
	if build.AppliedSequence > walker.file.meta.CommitSequence {
		return fmt.Errorf("%w: shadow watermark", ErrCorrupt)
	}
	semanticAudit := walker.indexAudit != nil
	var collection CollectionMeta
	var indexMeta IndexMeta
	var exact bool
	if semanticAudit {
		catalogRoot, available := build.effectiveAppliedCatalogRoot()
		if !available {
			walker.semanticIndexBuildsVerified = false
			semanticAudit = false
		} else {
			encodedCollection, exists, err := walker.treeGetUnlocked(catalogRoot, TreeCatalog, []byte(build.Collection))
			if err != nil || !exists {
				return fmt.Errorf("%w: shadow collection", ErrCorrupt)
			}
			collection, err = decodeCollectionMeta(encodedCollection)
			if err != nil || collection.ID != build.CollectionID {
				return fmt.Errorf("%w: shadow collection identity", ErrCorrupt)
			}
			indexMeta = IndexMeta{
				Name: build.Name, FieldPath: build.FieldPath, Fields: build.Fields, Unique: build.Unique,
				Root: build.ShadowRoot, EntryCount: build.EntryCount,
			}
			exact = build.AppliedSequence > build.SourceSequence || build.Phase == IndexBuildCatchUp || build.Phase == IndexBuildReady
		}
	}
	var entryCount, canonicalBytes uint64
	var seenDocuments map[[16]byte]struct{}
	if semanticAudit {
		seenDocuments = make(map[[16]byte]struct{}, build.EntryCount)
	}
	beyondScanCursor := false
	shadow, err := newTreeIterator(walker.file, build.ShadowRoot, TreeSecondary, nil, nil, 0)
	if err != nil {
		return err
	}
	for shadow.nextUnlocked() {
		if err := walker.checkContext(); err != nil {
			return err
		}
		_, _, id, err := secondaryKeyParts(shadow.Key())
		if err != nil || len(shadow.Value()) != 1 || shadow.Value()[0] != 0 {
			return fmt.Errorf("%w: shadow Secondary entry", ErrCorrupt)
		}
		if semanticAudit {
			if _, duplicate := seenDocuments[id]; duplicate {
				return fmt.Errorf("%w: duplicate shadow document", ErrCorrupt)
			}
			seenDocuments[id] = struct{}{}
			if bytes.Compare(id[:], build.ScanAfter[:]) > 0 {
				beyondScanCursor = true
			}
		}
		if canonicalBytes > ^uint64(0)-uint64(len(shadow.Key())) {
			return ErrCorrupt
		}
		entryCount++
		canonicalBytes += uint64(len(shadow.Key()))
	}
	if shadow.Err() != nil || entryCount != build.EntryCount || canonicalBytes != build.CanonicalBytes {
		return fmt.Errorf("%w: shadow Secondary accounting", ErrCorrupt)
	}
	if !semanticAudit {
		return nil
	}
	if !exact && beyondScanCursor {
		return fmt.Errorf("%w: shadow beyond scan cursor", ErrCorrupt)
	}

	primary, err := newTreeIterator(walker.file, collection.PrimaryRoot, TreePrimary, nil, nil, 0)
	if err != nil {
		return err
	}
	var expectedCount uint64
	for primary.nextUnlocked() {
		if err := walker.checkContext(); err != nil {
			return err
		}
		var id [16]byte
		if len(primary.Key()) != len(id) {
			return fmt.Errorf("%w: shadow Primary ID", ErrCorrupt)
		}
		copy(id[:], primary.Key())
		if !exact && bytes.Compare(id[:], build.ScanAfter[:]) > 0 {
			continue
		}
		position, descriptor, err := decodeDocumentRecordDescriptor(primary.Value())
		if err != nil {
			return err
		}
		document, err := walker.overflowValue(descriptor, PageDocumentOverflow, inlineDocumentLimit, maxDocumentBytes)
		if err != nil {
			return err
		}
		expected, indexed, err := walker.indexAudit(indexMeta, id, document)
		if err != nil {
			return fmt.Errorf("%w: shadow Primary semantic key", ErrCorrupt)
		}
		if !indexed {
			continue
		}
		complete, err := secondaryKey(expected, position, id)
		if err != nil {
			return err
		}
		value, exists, err := walker.treeGetUnlocked(build.ShadowRoot, TreeSecondary, complete)
		if err != nil || !exists || len(value) != 1 || value[0] != 0 {
			return fmt.Errorf("%w: shadow missing Secondary", ErrCorrupt)
		}
		expectedCount++
	}
	if err := primary.Err(); err != nil {
		return err
	}
	if expectedCount != build.EntryCount {
		return fmt.Errorf("%w: shadow semantic count %d/%d", ErrCorrupt, expectedCount, build.EntryCount)
	}
	return nil
}

func (walker *reachabilityWalker) overflowValue(stored []byte, pageType PageType, inlineLimit, maximum int) ([]byte, error) {
	if len(stored) == 0 {
		return nil, ErrCorrupt
	}
	if stored[0] == 0 {
		return append([]byte(nil), stored[1:]...), nil
	}
	if stored[0] != 1 || len(stored) != 49 {
		return nil, ErrCorrupt
	}
	total, pageID := binary.LittleEndian.Uint64(stored[1:9]), binary.LittleEndian.Uint64(stored[9:17])
	if total < uint64(inlineLimit) || total > uint64(maximum) || pageID < 2 {
		return nil, ErrCorrupt
	}
	result := make([]byte, 0, int(total))
	var chunks uint32
	for index := uint32(0); uint64(len(result)) < total; index++ {
		_, page, fresh, err := walker.page(pageID, pageType)
		if err != nil {
			return nil, err
		}
		if !fresh {
			// A shared overflow chain was already fully validated. Load it through
			// the normal bounded decoder to recover bytes for logical validation.
			tx := &WriteTxn{file: walker.file, nextPage: walker.file.nextPage, byID: make(map[uint64][]byte)}
			if pageType == PageDocumentOverflow {
				return tx.loadDocumentValue(stored)
			}
			return tx.loadCommitValue(stored)
		}
		if len(page.Payload) < 16 || binary.LittleEndian.Uint64(page.Payload[:8]) != total || binary.LittleEndian.Uint32(page.Payload[8:12]) != index {
			return nil, ErrCorrupt
		}
		currentChunks := binary.LittleEndian.Uint32(page.Payload[12:16])
		if currentChunks == 0 || (index > 0 && currentChunks != chunks) || index >= currentChunks || uint64(len(result)+len(page.Payload)-16) > total {
			return nil, ErrCorrupt
		}
		chunks = currentChunks
		result = append(result, page.Payload[16:]...)
		if index+1 == chunks {
			if page.Link != 0 || uint64(len(result)) != total {
				return nil, ErrCorrupt
			}
			break
		}
		pageID = page.Link
		if pageID < 2 {
			return nil, ErrCorrupt
		}
	}
	checksum := sha256.Sum256(result)
	if !equalBytes(stored[17:], checksum[:]) {
		return nil, ErrCorrupt
	}
	return result, nil
}

func (walker *reachabilityWalker) page(pageID uint64, expected PageType) ([]byte, Page, bool, error) {
	return walker.pageEither(pageID, expected, expected)
}

func (walker *reachabilityWalker) pageEither(pageID uint64, first, second PageType) ([]byte, Page, bool, error) {
	if pageID < 2 || pageID >= walker.file.nextPage {
		return nil, Page{}, false, ErrCorrupt
	}
	if existing, found := walker.pages[pageID]; found {
		if existing != first && existing != second {
			return nil, Page{}, false, ErrCorrupt
		}
		return nil, Page{Type: existing}, false, nil
	}
	raw := make([]byte, PageSize)
	if _, err := walker.file.file.ReadAt(raw, int64(pageID)*PageSize); err != nil {
		return nil, Page{}, false, err
	}
	page, err := DecodePage(raw, pageID)
	if err != nil || (page.Type != first && page.Type != second) {
		return nil, Page{}, false, ErrCorrupt
	}
	walker.pages[pageID] = page.Type
	return raw, page, true, nil
}

func treePageTypes(kind TreeKind) (PageType, PageType) {
	switch kind {
	case TreeCatalog:
		return PageCatalogLeaf, PageCatalogBranch
	case TreePrimary:
		return PagePrimaryLeaf, PagePrimaryBranch
	case TreeSecondary:
		return PageSecondaryLeaf, PageSecondaryBranch
	case TreeCommitLog:
		return PageCommitLogLeaf, PageCommitLogBranch
	case TreeIndexCatalog:
		return PageIndexCatalogLeaf, PageIndexCatalogBranch
	case TreeOrder:
		return PageOrderLeaf, PageOrderBranch
	case TreeSystem:
		return PageSystemLeaf, PageSystemBranch
	case TreeFreeSpace:
		return PageFreeSpaceLeaf, PageFreeSpaceBranch
	case TreeIndexBuildCatalog:
		return PageIndexBuildCatalogLeaf, PageIndexBuildCatalogBranch
	default:
		return 0, 0
	}
}
