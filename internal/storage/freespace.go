package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
)

var freeSpaceExtentMagic = [8]byte{'M', 'E', 'L', 'D', 'F', 'R', '3', '1'}

func freeSpaceKey(pageID uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, pageID)
	return key
}

func encodeFreeSpaceExtent(end, safePhysicalPages, auditGeneration uint64) ([]byte, error) {
	if end <= 2 || safePhysicalPages < end || auditGeneration == 0 {
		return nil, ErrCorrupt
	}
	value := make([]byte, 32)
	copy(value[:8], freeSpaceExtentMagic[:])
	binary.LittleEndian.PutUint64(value[8:16], end)
	binary.LittleEndian.PutUint64(value[16:24], safePhysicalPages)
	binary.LittleEndian.PutUint64(value[24:32], auditGeneration)
	return value, nil
}

func decodeFreeSpaceExtent(key, value []byte, physicalPages uint64) (uint64, uint64, uint64, error) {
	if len(key) != 8 || len(value) != 32 || !bytes.Equal(value[:8], freeSpaceExtentMagic[:]) {
		return 0, 0, 0, ErrCorrupt
	}
	start := binary.BigEndian.Uint64(key)
	end := binary.LittleEndian.Uint64(value[8:16])
	safePhysicalPages := binary.LittleEndian.Uint64(value[16:24])
	auditGeneration := binary.LittleEndian.Uint64(value[24:32])
	if start < 2 || end <= start || safePhysicalPages < end || safePhysicalPages > physicalPages || auditGeneration == 0 {
		return 0, 0, 0, ErrCorrupt
	}
	return start, end, auditGeneration, nil
}

func (f *File) restoreFreeSpace(root DatabaseRoot, meta Meta) error {
	return f.restoreFreeSpaceContext(context.Background(), root, meta)
}

func (f *File) restoreFreeSpaceContext(ctx context.Context, root DatabaseRoot, meta Meta) error {
	if f == nil || meta.OptionalFeatures&OptionalFeaturePersistentFreeSpace == 0 {
		return nil
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	if root.FreeSpaceRoot == 0 {
		f.freePages = []uint64{}
		f.freePoolVersion++
		f.freeSpaceTracked = true
		return nil
	}
	walker := &reachabilityWalker{file: f, ctx: ctx, pages: make(map[uint64]PageType), visiting: make(map[uint64]struct{})}
	if err := walker.tree(root.FreeSpaceRoot, TreeFreeSpace); err != nil {
		return fmt.Errorf("free-space tree: %w", err)
	}
	rootRaw := make([]byte, PageSize)
	if _, err := f.file.ReadAt(rootRaw, int64(root.FreeSpaceRoot)*PageSize); err != nil {
		return err
	}
	rootPage, err := DecodePage(rootRaw, root.FreeSpaceRoot)
	if err != nil {
		return err
	}
	iterator, err := newTreeIterator(f, root.FreeSpaceRoot, TreeFreeSpace, nil, nil, 0)
	if err != nil {
		return fmt.Errorf("free-space scan: %w", err)
	}
	defer iterator.Close()
	free := make([]uint64, 0)
	previousEnd := uint64(0)
	auditGeneration := uint64(0)
	entries := 0
	for iterator.Next() {
		if err := contextErr(ctx); err != nil {
			return err
		}
		entries++
		start, end, generation, err := decodeFreeSpaceExtent(iterator.Key(), iterator.Value(), meta.PhysicalPageCount)
		if err != nil || (previousEnd != 0 && start <= previousEnd) {
			return fmt.Errorf("free-space extent %x after %d: %w", iterator.Key(), previousEnd, ErrCorrupt)
		}
		if auditGeneration == 0 {
			auditGeneration = generation
		} else if generation != auditGeneration {
			return fmt.Errorf("free-space mixed audit generation: %w", ErrCorrupt)
		}
		if generation != rootPage.Generation {
			return fmt.Errorf("free-space root generation %d/%d: %w", generation, rootPage.Generation, ErrCorrupt)
		}
		if end-start > uint64(int(^uint(0)>>1))-uint64(len(free)) {
			return ErrCorrupt
		}
		for pageID := start; pageID < end; pageID++ {
			if (pageID-start)&1023 == 0 {
				if err := contextErr(ctx); err != nil {
					return err
				}
			}
			free = append(free, pageID)
		}
		previousEnd = end
	}
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("free-space scan: %w", err)
	}
	if entries == 0 {
		return ErrCorrupt
	}
	if auditGeneration > meta.Generation {
		return fmt.Errorf("free-space audit generation %d after meta %d: %w", auditGeneration, meta.Generation, ErrCorrupt)
	}
	// The allocator always consumes the greatest candidate ID. Reuse after the
	// immutable audit snapshot is therefore one contiguous suffix. Inspect only
	// that suffix (plus the first untouched sentinel), rather than reading every
	// free page on open.
	cutoff := len(free)
	if auditGeneration < meta.Generation {
		for cutoff > 0 {
			reused, err := f.pageReusedAfterGeneration(free[cutoff-1], auditGeneration)
			if err != nil {
				return err
			}
			if !reused {
				break
			}
			cutoff--
		}
	}
	free = free[:cutoff]
	for pageID := range walker.pages {
		position := sort.Search(len(free), func(index int) bool { return free[index] >= pageID })
		if position < len(free) && free[position] == pageID {
			return fmt.Errorf("free-space self-reference %d: %w", pageID, ErrCorrupt)
		}
	}
	position := sort.Search(len(free), func(index int) bool { return free[index] >= meta.RootPage })
	if position < len(free) && free[position] == meta.RootPage {
		return fmt.Errorf("free-space current root %d: %w", meta.RootPage, ErrCorrupt)
	}
	f.freePages = free
	f.freePoolVersion++
	f.freeSpaceTracked = true
	return nil
}

func (f *File) pageReusedAfterGeneration(pageID, auditGeneration uint64) (bool, error) {
	if f == nil || f.file == nil || pageID < 2 || pageID >= f.nextPage || auditGeneration == 0 {
		return false, ErrCorrupt
	}
	f.freeSpaceCandidateChecks.Add(1)
	raw := make([]byte, PageSize)
	if _, err := f.file.ReadAt(raw, int64(pageID)*PageSize); err != nil {
		return false, err
	}
	page, err := DecodePage(raw, pageID)
	if err != nil {
		// Reclamation snapshots may legitimately contain stale or partially
		// overwritten unreachable pages. Only a valid later-generation page is
		// evidence that a candidate was subsequently reused.
		return false, nil
	}
	return page.Generation > auditGeneration, nil
}

func (tx *WriteTxn) finalizeFreeSpace(root *DatabaseRoot) error {
	if tx == nil || root == nil {
		return ErrCorrupt
	}
	if !tx.freeSpaceTracked {
		root.FreeSpaceRoot = 0
		return nil
	}
	if !tx.freeSpaceRebuild {
		root.FreeSpaceRoot = tx.baseRoot.FreeSpaceRoot
		return nil
	}
	if len(tx.freePages) == 0 {
		root.FreeSpaceRoot = 0
		return nil
	}
	tree, err := tx.OpenTree(0, TreeFreeSpace)
	if err != nil {
		return err
	}
	for index := 0; index < len(tx.freePages); {
		start := tx.freePages[index]
		end := start + 1
		index++
		for index < len(tx.freePages) && tx.freePages[index] == end {
			end++
			index++
		}
		value, err := encodeFreeSpaceExtent(end, tx.nextPage, tx.generation)
		if err != nil {
			return err
		}
		if err := tree.Put(freeSpaceKey(start), value); err != nil {
			return err
		}
	}
	tx.metadataAllocation = true
	root.FreeSpaceRoot, err = tree.Flush()
	tx.metadataAllocation = false
	return err
}

// PersistFreeSpace publishes the current audited pool as an optional physical
// maintenance generation without advancing the logical commit sequence.
func (f *File) PersistFreeSpace() error {
	return f.PersistFreeSpaceContext(context.Background())
}

func (f *File) PersistFreeSpaceContext(ctx context.Context) error {
	if f == nil {
		return ErrCorrupt
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.freeSpaceTracked || !f.freeSpaceNeedsRebuild {
		return nil
	}
	if f.meta.CommitSequence == 0 {
		return nil
	}
	return f.updateUnlocked(false, func(tx *WriteTxn) (DatabaseRoot, error) {
		if err := contextErr(ctx); err != nil {
			return DatabaseRoot{}, err
		}
		return tx.BaseRoot(), nil
	})
}
