package storage

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"syscall"
)

const (
	PageSize                   = 16 * 1024
	pageHeaderSize             = 80
	formatVersion       uint16 = 1
	metaPageType        uint8  = 1
	snapshotPageType    uint8  = 2
	catalogPageType     uint8  = 4
	catalogHeaderSize          = 24
	catalogEntrySize           = 14
	catalogV2HeaderSize        = 28
	catalogV2EntrySize         = 27
)

var (
	pageMagic    = [8]byte{'M', 'E', 'L', 'D', 'P', 'A', 'G', 'E'}
	catalogMagic = [8]byte{'M', 'E', 'L', 'D', 'C', 'A', 'T', '1'}
	ErrCorrupt   = errors.New("meldbase storage: corrupt database")
	ErrLocked    = errors.New("meldbase storage: database is locked")
)

type Meta struct {
	DatabaseID      [16]byte
	Generation      uint64
	RootPage        uint64
	PageCount       uint32
	CheckpointToken uint64
}

type Blob struct {
	Kind  uint8
	Class BlobClass
	Data  []byte
}

type BlobClass uint8

const (
	BlobClassRecord BlobClass = iota
	BlobClassIndex
)

type catalogEntry struct {
	blob, chunk, chunks uint32
	kind                uint8
	record              RecordID
}

type File struct {
	file      *os.File
	meta      Meta
	metaSlot  int
	nextPage  uint64
	metas     [2]Meta
	metaValid [2]bool
	reachable [2]map[uint64]struct{}
	freePages map[uint64]struct{}
}

func Open(path string) (*File, []byte, Meta, error) {
	file, blobs, meta, err := OpenBlobs(path)
	if err != nil {
		return nil, nil, Meta{}, err
	}
	if len(blobs) == 0 {
		return file, nil, meta, nil
	}
	if len(blobs) != 1 {
		_ = file.Close()
		return nil, nil, Meta{}, fmt.Errorf("%w: legacy snapshot API cannot read typed blobs", ErrCorrupt)
	}
	return file, blobs[0].Data, meta, nil
}

func OpenBlobs(path string) (*File, []Blob, Meta, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, Meta{}, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, nil, Meta{}, fmt.Errorf("%w: %v", ErrLocked, err)
	}
	cleanup := func(err error) (*File, []Blob, Meta, error) {
		syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		file.Close()
		return nil, nil, Meta{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return cleanup(err)
	}
	if info.Size() == 0 {
		if err := file.Truncate(2 * PageSize); err != nil {
			return cleanup(err)
		}
		meta := Meta{Generation: 1}
		if _, err := rand.Read(meta.DatabaseID[:]); err != nil {
			return cleanup(err)
		}
		page, err := encodeMetaPage(0, meta)
		if err != nil {
			return cleanup(err)
		}
		if _, err := file.WriteAt(page, 0); err != nil {
			return cleanup(err)
		}
		if err := file.Sync(); err != nil {
			return cleanup(err)
		}
		return &File{
			file: file, meta: meta, metaSlot: 0, nextPage: 2,
			metas: [2]Meta{0: meta}, metaValid: [2]bool{true, false},
			reachable: [2]map[uint64]struct{}{0: map[uint64]struct{}{}}, freePages: map[uint64]struct{}{},
		}, nil, meta, nil
	}
	if info.Size() < 2*PageSize {
		return cleanup(fmt.Errorf("%w: invalid file length", ErrCorrupt))
	}
	effectiveSize := info.Size() - info.Size()%PageSize
	metas := [2]Meta{}
	valid := [2]bool{}
	for slot := 0; slot < 2; slot++ {
		page := make([]byte, PageSize)
		if _, err := file.ReadAt(page, int64(slot*PageSize)); err != nil {
			return cleanup(err)
		}
		meta, err := decodeMetaPage(page, uint64(slot))
		if err == nil {
			metas[slot], valid[slot] = meta, true
		}
	}
	if !valid[0] && !valid[1] {
		return cleanup(fmt.Errorf("%w: both meta pages invalid", ErrCorrupt))
	}
	if valid[0] && valid[1] && metas[0].DatabaseID != metas[1].DatabaseID {
		return cleanup(fmt.Errorf("%w: meta pages identify different databases", ErrCorrupt))
	}
	slot := 0
	if !valid[0] || (valid[1] && metas[1].Generation > metas[0].Generation) {
		slot = 1
	}
	meta := metas[slot]
	blobs, err := readSnapshotBlobs(file, meta)
	if err != nil {
		return cleanup(err)
	}
	if effectiveSize != info.Size() {
		if err := file.Truncate(effectiveSize); err != nil {
			return cleanup(err)
		}
		if err := file.Sync(); err != nil {
			return cleanup(err)
		}
	}
	pageCount := uint64(effectiveSize / PageSize)
	result := &File{
		file: file, meta: meta, metaSlot: slot, nextPage: pageCount,
		metas: metas, metaValid: valid, freePages: map[uint64]struct{}{},
	}
	reuseSafe := true
	for metaSlot := 0; metaSlot < 2; metaSlot++ {
		if !valid[metaSlot] {
			continue
		}
		pages, reachableErr := collectReachablePages(file, metas[metaSlot])
		if reachableErr != nil {
			if metaSlot == slot {
				return cleanup(reachableErr)
			}
			reuseSafe = false
			continue
		}
		result.reachable[metaSlot] = pages
	}
	if reuseSafe {
		result.rebuildFreePages()
	}
	return result, blobs, meta, nil
}

func (f *File) Checkpoint(token uint64, snapshot []byte) error {
	return f.CheckpointBlobs(token, []Blob{{Kind: 1, Data: snapshot}})
}

func (f *File) CheckpointBlobs(token uint64, blobs []Blob) error {
	if token < f.meta.CheckpointToken {
		return errors.New("checkpoint token moved backwards")
	}
	if len(blobs) == 0 || len(blobs) > 10_000_000 {
		return errors.New("checkpoint requires bounded blobs")
	}
	maxRecord := PageSize - recordPageHeaderSize - recordSlotSize
	totalRecords := 0
	for _, blob := range blobs {
		if blob.Kind == 0 || blob.Class > BlobClassIndex {
			return errors.New("checkpoint blob kind must be non-zero")
		}
		chunks := (len(blob.Data) + maxRecord - 1) / maxRecord
		if chunks == 0 {
			chunks = 1
		}
		if chunks > math.MaxInt-totalRecords {
			return errors.New("checkpoint is too large")
		}
		totalRecords += chunks
	}
	if uint64(totalRecords) > math.MaxUint32 {
		return errors.New("checkpoint has too many records")
	}
	generation := f.meta.Generation + 1
	if generation == 0 || generation > math.MaxUint32 {
		return errors.New("checkpoint generation exceeds record format")
	}
	entries := make([]catalogEntry, 0, totalRecords)
	catalogPages := (totalRecords + catalogEntriesPerPageV2() - 1) / catalogEntriesPerPageV2()
	allocated := f.allocatePages(totalRecords + catalogPages)
	recordPageIDs := allocated[:totalRecords]
	catalogPageIDs := allocated[totalRecords:]
	recordIndex := 0
	for blobIndex, blob := range blobs {
		chunkCount := (len(blob.Data) + maxRecord - 1) / maxRecord
		if chunkCount == 0 {
			chunkCount = 1
		}
		for chunkIndex := 0; chunkIndex < chunkCount; chunkIndex++ {
			start := chunkIndex * maxRecord
			end := start + maxRecord
			if end > len(blob.Data) {
				end = len(blob.Data)
			}
			pageID := recordPageIDs[recordIndex]
			page := NewRecordPageAt(pageID, generation, token)
			if blob.Class == BlobClassIndex {
				page = NewIndexPageAt(pageID, generation, token)
			}
			rid, err := page.Insert(blob.Data[start:end])
			if err != nil {
				return err
			}
			encoded, err := page.MarshalBinary()
			if err != nil {
				return err
			}
			if _, err := f.file.WriteAt(encoded, int64(pageID)*PageSize); err != nil {
				return err
			}
			entries = append(entries, catalogEntry{blob: uint32(blobIndex), kind: blob.Kind, chunk: uint32(chunkIndex), chunks: uint32(chunkCount), record: rid})
			recordIndex++
		}
	}
	catalogRoot := catalogPageIDs[0]
	for index := 0; index < catalogPages; index++ {
		start := index * catalogEntriesPerPageV2()
		end := start + catalogEntriesPerPageV2()
		if end > len(entries) {
			end = len(entries)
		}
		next := uint64(0)
		if index+1 < catalogPages {
			next = catalogPageIDs[index+1]
		}
		payload := encodeCatalogPayloadV2(uint32(len(entries)), uint32(len(blobs)), next, entries[start:end])
		pageID := catalogPageIDs[index]
		page := encodePage(pageID, catalogPageType, generation, token, uint32(index), uint32(catalogPages), payload)
		if _, err := f.file.WriteAt(page, int64(pageID)*PageSize); err != nil {
			return err
		}
	}
	if err := f.file.Sync(); err != nil {
		return err
	}
	meta := Meta{DatabaseID: f.meta.DatabaseID, Generation: generation, RootPage: catalogRoot, PageCount: uint32(totalRecords), CheckpointToken: token}
	slot := 1 - f.metaSlot
	page, err := encodeMetaPage(uint64(slot), meta)
	if err != nil {
		return err
	}
	if _, err := f.file.WriteAt(page, int64(slot*PageSize)); err != nil {
		return err
	}
	if err := f.file.Sync(); err != nil {
		return err
	}
	newReachable := make(map[uint64]struct{}, len(allocated))
	for _, pageID := range allocated {
		newReachable[pageID] = struct{}{}
	}
	f.meta, f.metaSlot = meta, slot
	f.metas[slot], f.metaValid[slot], f.reachable[slot] = meta, true, newReachable
	f.rebuildFreePages()
	return nil
}

func (f *File) allocatePages(count int) []uint64 {
	result := make([]uint64, 0, count)
	free := make([]uint64, 0, len(f.freePages))
	for pageID := range f.freePages {
		free = append(free, pageID)
	}
	sort.Slice(free, func(i, j int) bool { return free[i] < free[j] })
	for _, pageID := range free {
		if len(result) == count {
			break
		}
		result = append(result, pageID)
		delete(f.freePages, pageID)
	}
	for len(result) < count {
		result = append(result, f.nextPage)
		f.nextPage++
	}
	return result
}

func (f *File) rebuildFreePages() {
	protected := map[uint64]struct{}{}
	for slot := 0; slot < 2; slot++ {
		if !f.metaValid[slot] {
			continue
		}
		for pageID := range f.reachable[slot] {
			protected[pageID] = struct{}{}
		}
	}
	f.freePages = make(map[uint64]struct{})
	for pageID := uint64(2); pageID < f.nextPage; pageID++ {
		if _, used := protected[pageID]; !used {
			f.freePages[pageID] = struct{}{}
		}
	}
}

func collectReachablePages(file *os.File, meta Meta) (map[uint64]struct{}, error) {
	result := map[uint64]struct{}{}
	if meta.PageCount == 0 {
		if meta.RootPage != 0 {
			return nil, ErrCorrupt
		}
		return result, nil
	}
	if meta.RootPage < 2 {
		return nil, ErrCorrupt
	}
	root := make([]byte, PageSize)
	if _, err := file.ReadAt(root, int64(meta.RootPage)*PageSize); err != nil {
		return nil, ErrCorrupt
	}
	if root[10] == snapshotPageType {
		for index := uint32(0); index < meta.PageCount; index++ {
			result[meta.RootPage+uint64(index)] = struct{}{}
		}
		return result, nil
	}
	if root[10] != catalogPageType {
		return nil, ErrCorrupt
	}
	_, firstPayload, err := decodePage(root, meta.RootPage, catalogPageType)
	if err != nil || len(firstPayload) < 10 || !equal8(firstPayload[:8], catalogMagic[:]) {
		return nil, ErrCorrupt
	}
	version := binary.LittleEndian.Uint16(firstPayload[8:10])
	var headerSize, entrySize, entriesPerPage, recordOffset, nextOffset, countOffset int
	switch version {
	case 1:
		headerSize, entrySize, entriesPerPage = catalogHeaderSize, catalogEntrySize, catalogEntriesPerPage()
		recordOffset, nextOffset, countOffset = 0, 14, 22
	case 2:
		headerSize, entrySize, entriesPerPage = catalogV2HeaderSize, catalogV2EntrySize, catalogEntriesPerPageV2()
		recordOffset, nextOffset, countOffset = 13, 18, 26
	default:
		return nil, ErrCorrupt
	}
	expectedPages := (int(meta.PageCount) + entriesPerPage - 1) / entriesPerPage
	catalogPages := map[uint64]struct{}{}
	recordPages := map[uint64]struct{}{}
	pageID := meta.RootPage
	totalEntries := 0
	for index := 0; index < expectedPages; index++ {
		if pageID < 2 {
			return nil, ErrCorrupt
		}
		if _, duplicate := catalogPages[pageID]; duplicate {
			return nil, ErrCorrupt
		}
		catalogPages[pageID] = struct{}{}
		page := make([]byte, PageSize)
		if _, err := file.ReadAt(page, int64(pageID)*PageSize); err != nil {
			return nil, ErrCorrupt
		}
		header, payload, err := decodePage(page, pageID, catalogPageType)
		if err != nil || header.generation != meta.Generation || header.lsn != meta.CheckpointToken ||
			header.chunkIndex != uint32(index) || header.chunkCount != uint32(expectedPages) || len(payload) < headerSize ||
			!equal8(payload[:8], catalogMagic[:]) || binary.LittleEndian.Uint16(payload[8:10]) != version ||
			binary.LittleEndian.Uint32(payload[10:14]) != meta.PageCount {
			return nil, ErrCorrupt
		}
		count := int(binary.LittleEndian.Uint16(payload[countOffset : countOffset+2]))
		if count == 0 || count > entriesPerPage || len(payload) != headerSize+count*entrySize {
			return nil, ErrCorrupt
		}
		for entryIndex := 0; entryIndex < count; entryIndex++ {
			offset := headerSize + entryIndex*entrySize + recordOffset
			recordPage := binary.LittleEndian.Uint64(payload[offset : offset+8])
			if recordPage < 2 {
				return nil, ErrCorrupt
			}
			recordPages[recordPage] = struct{}{}
		}
		totalEntries += count
		next := binary.LittleEndian.Uint64(payload[nextOffset : nextOffset+8])
		if index+1 == expectedPages {
			if next != 0 {
				return nil, ErrCorrupt
			}
		} else if next < 2 {
			return nil, ErrCorrupt
		}
		pageID = next
	}
	if totalEntries != int(meta.PageCount) {
		return nil, ErrCorrupt
	}
	for pageID := range catalogPages {
		if _, collision := recordPages[pageID]; collision {
			return nil, ErrCorrupt
		}
		result[pageID] = struct{}{}
	}
	for pageID := range recordPages {
		result[pageID] = struct{}{}
	}
	return result, nil
}

func (f *File) Close() error {
	if f == nil || f.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(f.file.Fd()), syscall.LOCK_UN)
	closeErr := f.file.Close()
	f.file = nil
	return errors.Join(unlockErr, closeErr)
}

func encodeMetaPage(pageID uint64, meta Meta) ([]byte, error) {
	payload := make([]byte, 52)
	copy(payload[:16], meta.DatabaseID[:])
	binary.LittleEndian.PutUint64(payload[16:24], meta.Generation)
	binary.LittleEndian.PutUint64(payload[24:32], meta.RootPage)
	binary.LittleEndian.PutUint32(payload[32:36], meta.PageCount)
	binary.LittleEndian.PutUint64(payload[36:44], meta.CheckpointToken)
	binary.LittleEndian.PutUint64(payload[44:52], PageSize)
	return encodePage(pageID, metaPageType, meta.Generation, meta.CheckpointToken, 0, 1, payload), nil
}

func decodeMetaPage(page []byte, pageID uint64) (Meta, error) {
	header, payload, err := decodePage(page, pageID, metaPageType)
	if err != nil {
		return Meta{}, err
	}
	if len(payload) != 52 || binary.LittleEndian.Uint64(payload[44:52]) != PageSize {
		return Meta{}, ErrCorrupt
	}
	var meta Meta
	copy(meta.DatabaseID[:], payload[:16])
	meta.Generation = binary.LittleEndian.Uint64(payload[16:24])
	meta.RootPage = binary.LittleEndian.Uint64(payload[24:32])
	meta.PageCount = binary.LittleEndian.Uint32(payload[32:36])
	meta.CheckpointToken = binary.LittleEndian.Uint64(payload[36:44])
	if header.generation != meta.Generation || header.lsn != meta.CheckpointToken {
		return Meta{}, ErrCorrupt
	}
	return meta, nil
}

func readSnapshotBlobs(file *os.File, meta Meta) ([]Blob, error) {
	if meta.PageCount == 0 {
		if meta.RootPage != 0 {
			return nil, ErrCorrupt
		}
		return nil, nil
	}
	if meta.RootPage < 2 {
		return nil, ErrCorrupt
	}
	root := make([]byte, PageSize)
	if _, err := file.ReadAt(root, int64(meta.RootPage)*PageSize); err != nil {
		return nil, ErrCorrupt
	}
	if root[10] == catalogPageType {
		return readCatalogBlobs(file, meta, root)
	}
	if root[10] != snapshotPageType {
		return nil, ErrCorrupt
	}
	result := make([]byte, 0, int(meta.PageCount)*(PageSize-pageHeaderSize))
	for index := uint32(0); index < meta.PageCount; index++ {
		pageID := meta.RootPage + uint64(index)
		page := make([]byte, PageSize)
		if _, err := file.ReadAt(page, int64(pageID)*PageSize); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, ErrCorrupt
			}
			return nil, err
		}
		header, payload, err := decodePage(page, pageID, snapshotPageType)
		if err != nil {
			return nil, err
		}
		if header.generation != meta.Generation || header.lsn != meta.CheckpointToken || header.chunkIndex != index || header.chunkCount != meta.PageCount {
			return nil, ErrCorrupt
		}
		result = append(result, payload...)
	}
	return []Blob{{Kind: 1, Data: result}}, nil
}

func catalogEntriesPerPage() int {
	return (PageSize - pageHeaderSize - catalogHeaderSize) / catalogEntrySize
}

func catalogEntriesPerPageV2() int {
	return (PageSize - pageHeaderSize - catalogV2HeaderSize) / catalogV2EntrySize
}

func encodeCatalogPayload(total uint32, next uint64, records []RecordID) []byte {
	payload := make([]byte, catalogHeaderSize+len(records)*catalogEntrySize)
	copy(payload[:8], catalogMagic[:])
	binary.LittleEndian.PutUint16(payload[8:10], 1)
	binary.LittleEndian.PutUint32(payload[10:14], total)
	binary.LittleEndian.PutUint64(payload[14:22], next)
	binary.LittleEndian.PutUint16(payload[22:24], uint16(len(records)))
	for index, record := range records {
		offset := catalogHeaderSize + index*catalogEntrySize
		binary.LittleEndian.PutUint64(payload[offset:offset+8], record.Page)
		binary.LittleEndian.PutUint16(payload[offset+8:offset+10], record.Slot)
		binary.LittleEndian.PutUint32(payload[offset+10:offset+14], record.Generation)
	}
	return payload
}

func encodeCatalogPayloadV2(total, blobCount uint32, next uint64, entries []catalogEntry) []byte {
	payload := make([]byte, catalogV2HeaderSize+len(entries)*catalogV2EntrySize)
	copy(payload[:8], catalogMagic[:])
	binary.LittleEndian.PutUint16(payload[8:10], 2)
	binary.LittleEndian.PutUint32(payload[10:14], total)
	binary.LittleEndian.PutUint32(payload[14:18], blobCount)
	binary.LittleEndian.PutUint64(payload[18:26], next)
	binary.LittleEndian.PutUint16(payload[26:28], uint16(len(entries)))
	for index, entry := range entries {
		offset := catalogV2HeaderSize + index*catalogV2EntrySize
		binary.LittleEndian.PutUint32(payload[offset:offset+4], entry.blob)
		payload[offset+4] = entry.kind
		binary.LittleEndian.PutUint32(payload[offset+5:offset+9], entry.chunk)
		binary.LittleEndian.PutUint32(payload[offset+9:offset+13], entry.chunks)
		binary.LittleEndian.PutUint64(payload[offset+13:offset+21], entry.record.Page)
		binary.LittleEndian.PutUint16(payload[offset+21:offset+23], entry.record.Slot)
		binary.LittleEndian.PutUint32(payload[offset+23:offset+27], entry.record.Generation)
	}
	return payload
}

func readCatalogBlobs(file *os.File, meta Meta, root []byte) ([]Blob, error) {
	_, payload, err := decodePage(root, meta.RootPage, catalogPageType)
	if err != nil || len(payload) < 10 || !equal8(payload[:8], catalogMagic[:]) {
		return nil, ErrCorrupt
	}
	switch binary.LittleEndian.Uint16(payload[8:10]) {
	case 1:
		data, err := readCatalogSnapshot(file, meta)
		if err != nil {
			return nil, err
		}
		return []Blob{{Kind: 1, Data: data}}, nil
	case 2:
		return readCatalogV2(file, meta)
	default:
		return nil, ErrCorrupt
	}
}

func readCatalogV2(file *os.File, meta Meta) ([]Blob, error) {
	if meta.PageCount == 0 || meta.PageCount > 10_000_000 {
		return nil, ErrCorrupt
	}
	expectedPages := (int(meta.PageCount) + catalogEntriesPerPageV2() - 1) / catalogEntriesPerPageV2()
	entries := make([]catalogEntry, 0, meta.PageCount)
	pageID := meta.RootPage
	seenCatalogPages := make(map[uint64]struct{}, expectedPages)
	var blobCount uint32
	for index := 0; index < expectedPages; index++ {
		if pageID < 2 {
			return nil, ErrCorrupt
		}
		if _, duplicate := seenCatalogPages[pageID]; duplicate {
			return nil, ErrCorrupt
		}
		seenCatalogPages[pageID] = struct{}{}
		page := make([]byte, PageSize)
		if _, err := file.ReadAt(page, int64(pageID)*PageSize); err != nil {
			return nil, ErrCorrupt
		}
		header, payload, err := decodePage(page, pageID, catalogPageType)
		if err != nil || header.generation != meta.Generation || header.lsn != meta.CheckpointToken ||
			header.chunkIndex != uint32(index) || header.chunkCount != uint32(expectedPages) || len(payload) < catalogV2HeaderSize ||
			!equal8(payload[:8], catalogMagic[:]) || binary.LittleEndian.Uint16(payload[8:10]) != 2 ||
			binary.LittleEndian.Uint32(payload[10:14]) != meta.PageCount {
			return nil, ErrCorrupt
		}
		pageBlobCount := binary.LittleEndian.Uint32(payload[14:18])
		if pageBlobCount == 0 || pageBlobCount > meta.PageCount || (index > 0 && pageBlobCount != blobCount) {
			return nil, ErrCorrupt
		}
		blobCount = pageBlobCount
		next := binary.LittleEndian.Uint64(payload[18:26])
		count := int(binary.LittleEndian.Uint16(payload[26:28]))
		if count == 0 || count > catalogEntriesPerPageV2() || len(payload) != catalogV2HeaderSize+count*catalogV2EntrySize {
			return nil, ErrCorrupt
		}
		for entryIndex := 0; entryIndex < count; entryIndex++ {
			offset := catalogV2HeaderSize + entryIndex*catalogV2EntrySize
			entry := catalogEntry{
				blob:   binary.LittleEndian.Uint32(payload[offset : offset+4]),
				kind:   payload[offset+4],
				chunk:  binary.LittleEndian.Uint32(payload[offset+5 : offset+9]),
				chunks: binary.LittleEndian.Uint32(payload[offset+9 : offset+13]),
				record: RecordID{
					Page: binary.LittleEndian.Uint64(payload[offset+13 : offset+21]), Slot: binary.LittleEndian.Uint16(payload[offset+21 : offset+23]),
					Generation: binary.LittleEndian.Uint32(payload[offset+23 : offset+27]),
				},
			}
			if entry.blob >= blobCount || entry.kind == 0 || entry.chunks == 0 || entry.chunk >= entry.chunks ||
				entry.record.Page < 2 || entry.record.Generation == 0 {
				return nil, ErrCorrupt
			}
			entries = append(entries, entry)
		}
		if index+1 == expectedPages {
			if next != 0 {
				return nil, ErrCorrupt
			}
		} else if next < 2 {
			return nil, ErrCorrupt
		}
		pageID = next
	}
	if len(entries) != int(meta.PageCount) {
		return nil, ErrCorrupt
	}
	blobs := make([]Blob, blobCount)
	seenRecords := make(map[RecordID]struct{}, len(entries))
	for position, entry := range entries {
		if position == 0 {
			if entry.blob != 0 || entry.chunk != 0 {
				return nil, ErrCorrupt
			}
		} else {
			previous := entries[position-1]
			if entry.blob == previous.blob {
				if entry.kind != previous.kind || entry.chunks != previous.chunks || entry.chunk != previous.chunk+1 {
					return nil, ErrCorrupt
				}
			} else if entry.blob != previous.blob+1 || entry.chunk != 0 || previous.chunk+1 != previous.chunks {
				return nil, ErrCorrupt
			}
		}
		if _, duplicate := seenRecords[entry.record]; duplicate {
			return nil, ErrCorrupt
		}
		seenRecords[entry.record] = struct{}{}
		page := make([]byte, PageSize)
		if _, err := file.ReadAt(page, int64(entry.record.Page)*PageSize); err != nil {
			return nil, ErrCorrupt
		}
		var decoded *RecordPage
		var class BlobClass
		var decodeErr error
		switch page[10] {
		case recordPageType:
			decoded, decodeErr = DecodeRecordPage(page, entry.record.Page)
		case indexPageType:
			class = BlobClassIndex
			decoded, decodeErr = DecodeIndexPage(page, entry.record.Page)
		default:
			return nil, ErrCorrupt
		}
		if decodeErr != nil || decoded.LSN() != meta.CheckpointToken {
			return nil, ErrCorrupt
		}
		chunk, err := decoded.Get(entry.record)
		if err != nil {
			return nil, ErrCorrupt
		}
		blob := &blobs[entry.blob]
		if entry.chunk == 0 {
			blob.Kind, blob.Class = entry.kind, class
		} else if blob.Class != class {
			return nil, ErrCorrupt
		}
		blob.Data = append(blob.Data, chunk...)
	}
	last := entries[len(entries)-1]
	if last.blob+1 != blobCount || last.chunk+1 != last.chunks {
		return nil, ErrCorrupt
	}
	return blobs, nil
}

func readCatalogSnapshot(file *os.File, meta Meta) ([]byte, error) {
	if meta.PageCount == 0 || meta.PageCount > 10_000_000 {
		return nil, ErrCorrupt
	}
	expectedCatalogPages := (int(meta.PageCount) + catalogEntriesPerPage() - 1) / catalogEntriesPerPage()
	records := make([]RecordID, 0, meta.PageCount)
	pageID := meta.RootPage
	for index := 0; index < expectedCatalogPages; index++ {
		if pageID != meta.RootPage+uint64(index) {
			return nil, ErrCorrupt
		}
		page := make([]byte, PageSize)
		if _, err := file.ReadAt(page, int64(pageID)*PageSize); err != nil {
			return nil, ErrCorrupt
		}
		header, payload, err := decodePage(page, pageID, catalogPageType)
		if err != nil || header.generation != meta.Generation || header.lsn != meta.CheckpointToken ||
			header.chunkIndex != uint32(index) || header.chunkCount != uint32(expectedCatalogPages) || len(payload) < catalogHeaderSize {
			return nil, ErrCorrupt
		}
		if !equal8(payload[:8], catalogMagic[:]) || binary.LittleEndian.Uint16(payload[8:10]) != 1 ||
			binary.LittleEndian.Uint32(payload[10:14]) != meta.PageCount {
			return nil, ErrCorrupt
		}
		next := binary.LittleEndian.Uint64(payload[14:22])
		count := int(binary.LittleEndian.Uint16(payload[22:24]))
		if count == 0 || count > catalogEntriesPerPage() || len(payload) != catalogHeaderSize+count*catalogEntrySize {
			return nil, ErrCorrupt
		}
		for recordIndex := 0; recordIndex < count; recordIndex++ {
			offset := catalogHeaderSize + recordIndex*catalogEntrySize
			record := RecordID{
				Page:       binary.LittleEndian.Uint64(payload[offset : offset+8]),
				Slot:       binary.LittleEndian.Uint16(payload[offset+8 : offset+10]),
				Generation: binary.LittleEndian.Uint32(payload[offset+10 : offset+14]),
			}
			if record.Page < 2 || record.Page >= meta.RootPage || record.Generation == 0 {
				return nil, ErrCorrupt
			}
			records = append(records, record)
		}
		if index+1 == expectedCatalogPages {
			if next != 0 {
				return nil, ErrCorrupt
			}
		} else if next != pageID+1 {
			return nil, ErrCorrupt
		}
		pageID = next
	}
	if len(records) != int(meta.PageCount) {
		return nil, ErrCorrupt
	}
	result := make([]byte, 0, len(records)*(PageSize-recordPageHeaderSize-recordSlotSize))
	seen := make(map[RecordID]struct{}, len(records))
	for _, record := range records {
		if _, duplicate := seen[record]; duplicate {
			return nil, ErrCorrupt
		}
		seen[record] = struct{}{}
		page := make([]byte, PageSize)
		if _, err := file.ReadAt(page, int64(record.Page)*PageSize); err != nil {
			return nil, ErrCorrupt
		}
		decoded, err := DecodeRecordPage(page, record.Page)
		if err != nil || decoded.LSN() != meta.CheckpointToken {
			return nil, ErrCorrupt
		}
		chunk, err := decoded.Get(record)
		if err != nil {
			return nil, ErrCorrupt
		}
		result = append(result, chunk...)
	}
	return result, nil
}

type pageHeader struct {
	generation, lsn        uint64
	chunkIndex, chunkCount uint32
}

func encodePage(pageID uint64, pageType uint8, generation, lsn uint64, chunkIndex, chunkCount uint32, payload []byte) []byte {
	page := make([]byte, PageSize)
	copy(page[0:8], pageMagic[:])
	binary.LittleEndian.PutUint16(page[8:10], formatVersion)
	page[10] = pageType
	binary.LittleEndian.PutUint64(page[12:20], pageID)
	binary.LittleEndian.PutUint64(page[20:28], generation)
	binary.LittleEndian.PutUint64(page[28:36], lsn)
	binary.LittleEndian.PutUint32(page[36:40], chunkIndex)
	binary.LittleEndian.PutUint32(page[40:44], chunkCount)
	binary.LittleEndian.PutUint32(page[44:48], uint32(len(payload)))
	copy(page[pageHeaderSize:], payload)
	checksum := sha256.Sum256(append(append([]byte(nil), page[:48]...), payload...))
	copy(page[48:80], checksum[:])
	return page
}

func decodePage(page []byte, expectedID uint64, expectedType uint8) (pageHeader, []byte, error) {
	if len(page) != PageSize || !equal8(page[:8], pageMagic[:]) || binary.LittleEndian.Uint16(page[8:10]) != formatVersion || page[10] != expectedType || binary.LittleEndian.Uint64(page[12:20]) != expectedID {
		return pageHeader{}, nil, ErrCorrupt
	}
	length := binary.LittleEndian.Uint32(page[44:48])
	if int(length) > PageSize-pageHeaderSize {
		return pageHeader{}, nil, ErrCorrupt
	}
	payload := page[pageHeaderSize : pageHeaderSize+int(length)]
	checksum := sha256.Sum256(append(append([]byte(nil), page[:48]...), payload...))
	if !equal8(checksum[:], page[48:80]) {
		return pageHeader{}, nil, ErrCorrupt
	}
	return pageHeader{generation: binary.LittleEndian.Uint64(page[20:28]), lsn: binary.LittleEndian.Uint64(page[28:36]), chunkIndex: binary.LittleEndian.Uint32(page[36:40]), chunkCount: binary.LittleEndian.Uint32(page[40:44])}, append([]byte(nil), payload...), nil
}

func equal8(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var difference byte
	for i := range a {
		difference |= a[i] ^ b[i]
	}
	return difference == 0
}
