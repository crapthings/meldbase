package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"time"
)

const (
	maxSystemKeyBytes      = 256
	inlineSystemValueLimit = 8 * 1024
	maxSystemValueBytes    = 16*1024*1024 + 64*1024
	systemDirectoryBytes   = 32
)

var (
	systemCatalogKey     = []byte{0, 'm', 'e', 'l', 'd', 'b', 'a', 's', 'e', '.', 's', 'y', 's', 't', 'e', 'm'}
	systemDirectoryMagic = [8]byte{'M', 'E', 'L', 'D', 'S', 'Y', 'S', '3'}
	errSystemCASMismatch = errors.New("meldbase storage v2: system record compare-and-swap mismatch")
)

type systemDirectory struct {
	Root  uint64
	Count uint64
}

// SystemRecordMutation is one compare-and-set change in the private system
// tree. It is also reusable by composite storage transactions that publish
// business and control-plane state under one DatabaseRoot.
type SystemRecordMutation struct {
	Key            []byte
	ExpectedExists bool
	ExpectedHash   [32]byte
	NewValue       []byte
	Delete         bool
	Unconditional  bool
}

// SystemRecordTransaction performs one compare-and-swap in the private system
// tree. ExpectedHash is SHA-256 over the decoded current value when
// ExpectedExists is true. NewValue must be non-empty unless Delete is set.
type SystemRecordTransaction struct {
	TransactionID  [16]byte
	CommittedAt    time.Time
	Key            []byte
	ExpectedExists bool
	ExpectedHash   [32]byte
	NewValue       []byte
	Delete         bool
	Unconditional  bool
}

type SystemRecordResult struct {
	Sequence uint64
	Applied  bool
	Current  []byte
}

func (f *File) ApplySystemRecordTransaction(transaction SystemRecordTransaction) (SystemRecordResult, error) {
	mutation := SystemRecordMutation{
		Key: transaction.Key, ExpectedExists: transaction.ExpectedExists, ExpectedHash: transaction.ExpectedHash,
		NewValue: transaction.NewValue, Delete: transaction.Delete, Unconditional: transaction.Unconditional,
	}
	if f == nil || allZero(transaction.TransactionID[:]) || !validSystemRecordMutation(mutation) {
		return SystemRecordResult{}, ErrCorrupt
	}
	if transaction.CommittedAt.IsZero() {
		transaction.CommittedAt = time.Now()
	}
	var result SystemRecordResult
	err := f.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		catalog, err := tx.OpenTree(base.CatalogRoot, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		current, applied, err := tx.applySystemRecordMutation(catalog, mutation)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if !applied {
			result.Current = current
			return DatabaseRoot{}, errSystemCASMismatch
		}
		encodedDirectory, exists, err := catalog.Get(systemCatalogKey)
		if err != nil || !exists {
			return DatabaseRoot{}, ErrCorrupt
		}
		catalogRoot, err := catalog.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		commitLogRoot, oldest, err := tx.AppendCommitRetained(base.CommitLogRoot, base.OldestRetainedSequence, CommitBatch{
			Sequence: tx.Sequence(), TransactionID: transaction.TransactionID, CommittedAt: transaction.CommittedAt,
			CatalogRoot: catalogRoot,
			Changes:     []CommitChange{{CollectionID: math.MaxUint32, Operation: CommitCatalog, ChangedPaths: []string{"_system.records"}, After: encodedDirectory}},
		})
		if err != nil {
			return DatabaseRoot{}, err
		}
		result.Sequence, result.Applied = tx.Sequence(), true
		return DatabaseRoot{
			CommitSequence: tx.Sequence(), CatalogRoot: catalogRoot, CommitLogRoot: commitLogRoot,
			FreeSpaceRoot: base.FreeSpaceRoot, OldestRetainedSequence: oldest,
			CatalogGeneration: base.CatalogGeneration, DocumentCount: base.DocumentCount, CollectionCount: base.CollectionCount,
		}, nil
	})
	if errors.Is(err, errSystemCASMismatch) {
		return result, nil
	}
	return result, err
}

func validSystemRecordMutation(mutation SystemRecordMutation) bool {
	return len(mutation.Key) > 0 && len(mutation.Key) <= maxSystemKeyBytes &&
		(!mutation.Unconditional || (!mutation.ExpectedExists && allZero(mutation.ExpectedHash[:]))) &&
		(mutation.ExpectedExists || allZero(mutation.ExpectedHash[:])) &&
		(!mutation.Unconditional || !mutation.Delete) &&
		(!mutation.Delete || len(mutation.NewValue) == 0) &&
		(mutation.Delete || (len(mutation.NewValue) > 0 && len(mutation.NewValue) <= maxSystemValueBytes))
}

// applySystemRecordMutation stages one private record CAS in catalog. A false
// applied result is not an error and leaves the write transaction unchanged.
func (tx *WriteTxn) applySystemRecordMutation(catalog *MutableTree, mutation SystemRecordMutation) ([]byte, bool, error) {
	if tx == nil || catalog == nil || !validSystemRecordMutation(mutation) {
		return nil, false, ErrCorrupt
	}
	directory := systemDirectory{}
	encodedDirectory, directoryExists, err := catalog.Get(systemCatalogKey)
	if err != nil {
		return nil, false, err
	}
	if directoryExists {
		directory, err = decodeSystemDirectory(encodedDirectory)
		if err != nil {
			return nil, false, err
		}
	}
	tree, err := tx.OpenTree(directory.Root, TreeSystem)
	if err != nil || tree.root.count != directory.Count {
		return nil, false, ErrCorrupt
	}
	stored, exists, err := tree.Get(mutation.Key)
	if err != nil {
		return nil, false, err
	}
	var current []byte
	if exists {
		current, err = tx.loadSystemValue(stored)
		if err != nil {
			return nil, false, err
		}
	}
	matches := mutation.Unconditional || exists == mutation.ExpectedExists
	if matches && exists && !mutation.Unconditional {
		actual := sha256.Sum256(current)
		matches = equalBytes(actual[:], mutation.ExpectedHash[:])
	}
	if !matches {
		return append([]byte(nil), current...), false, nil
	}
	if mutation.Delete {
		if !exists {
			return nil, false, ErrCorrupt
		}
		removed, err := tree.Delete(mutation.Key)
		if err != nil || !removed || directory.Count == 0 {
			return nil, false, ErrCorrupt
		}
		directory.Count--
	} else {
		descriptor, err := tx.storeSystemValue(mutation.NewValue)
		if err != nil {
			return nil, false, err
		}
		if err := tree.Put(mutation.Key, descriptor); err != nil {
			return nil, false, err
		}
		if !exists {
			if directory.Count == math.MaxUint64 {
				return nil, false, ErrCorrupt
			}
			directory.Count++
		}
	}
	directory.Root, err = tree.Flush()
	if err != nil || tree.root.count != directory.Count {
		return nil, false, ErrCorrupt
	}
	encodedDirectory, err = encodeSystemDirectory(directory)
	if err != nil {
		return nil, false, err
	}
	if err := catalog.Put(systemCatalogKey, encodedDirectory); err != nil {
		return nil, false, err
	}
	return nil, true, nil
}

func (f *File) GetSystemRecord(key []byte) ([]byte, bool, error) {
	if f == nil || len(key) == 0 || len(key) > maxSystemKeyBytes {
		return nil, false, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return nil, false, errors.New("meldbase storage v2: file is closed")
	}
	return f.getSystemRecordUnlocked(f.root.CatalogRoot, key)
}

// SystemRecords returns an isolated key/value snapshot for maintenance such as
// compact-to-new-file. It is not exposed through the public collection API.
func (snapshot *ReadSnapshot) SystemRecords() ([]KeyValue, error) {
	return snapshot.ScanSystemRecords(nil, nil, 0)
}

// ScanSystemRecords returns private records in bytewise [start,end) order. A
// non-positive limit is unbounded. It exists for bounded first-party retention
// maintenance, not application queries.
func (snapshot *ReadSnapshot) ScanSystemRecords(start, end []byte, limit int) ([]KeyValue, error) {
	if snapshot == nil {
		return nil, ErrCorrupt
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.file == nil {
		return nil, ErrCursorClosed
	}
	file := snapshot.file
	file.mu.RLock()
	defer file.mu.RUnlock()
	if file.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	encodedDirectory, exists, err := file.treeGetUnlocked(snapshot.root.CatalogRoot, TreeCatalog, systemCatalogKey)
	if err != nil || !exists {
		return nil, err
	}
	directory, err := decodeSystemDirectory(encodedDirectory)
	if err != nil {
		return nil, err
	}
	tx := &WriteTxn{file: file, generation: file.meta.Generation, sequence: snapshot.root.CommitSequence, nextPage: file.nextPage, byID: make(map[uint64][]byte)}
	tree, err := tx.OpenTree(directory.Root, TreeSystem)
	if err != nil || tree.root.count != directory.Count {
		return nil, ErrCorrupt
	}
	stored, err := tree.Scan(start, end, limit)
	if err != nil {
		return nil, err
	}
	if len(start) == 0 && len(end) == 0 && limit <= 0 && uint64(len(stored)) != directory.Count {
		return nil, ErrCorrupt
	}
	result := make([]KeyValue, len(stored))
	for index, pair := range stored {
		value, err := tx.loadSystemValue(pair.Value)
		if err != nil {
			return nil, err
		}
		result[index] = KeyValue{Key: append([]byte(nil), pair.Key...), Value: value}
	}
	return result, nil
}

func (f *File) getSystemRecordUnlocked(catalogRoot uint64, key []byte) ([]byte, bool, error) {
	encodedDirectory, exists, err := f.treeGetUnlocked(catalogRoot, TreeCatalog, systemCatalogKey)
	if err != nil || !exists {
		return nil, false, err
	}
	directory, err := decodeSystemDirectory(encodedDirectory)
	if err != nil {
		return nil, false, err
	}
	stored, exists, err := f.treeGetUnlocked(directory.Root, TreeSystem, key)
	if err != nil || !exists {
		return nil, false, err
	}
	tx := &WriteTxn{file: f, nextPage: f.nextPage, byID: make(map[uint64][]byte)}
	value, err := tx.loadSystemValue(stored)
	return value, err == nil, err
}

func encodeSystemDirectory(directory systemDirectory) ([]byte, error) {
	if directory.Root < 2 {
		return nil, ErrCorrupt
	}
	encoded := make([]byte, systemDirectoryBytes)
	copy(encoded[:8], systemDirectoryMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(encoded[10:12], systemDirectoryBytes)
	binary.LittleEndian.PutUint64(encoded[16:24], directory.Root)
	binary.LittleEndian.PutUint64(encoded[24:32], directory.Count)
	return encoded, nil
}

func decodeSystemDirectory(encoded []byte) (systemDirectory, error) {
	if len(encoded) != systemDirectoryBytes || string(encoded[:8]) != string(systemDirectoryMagic[:]) ||
		binary.LittleEndian.Uint16(encoded[8:10]) != FormatVersion || binary.LittleEndian.Uint16(encoded[10:12]) != systemDirectoryBytes ||
		!allZero(encoded[12:16]) {
		return systemDirectory{}, ErrCorrupt
	}
	directory := systemDirectory{Root: binary.LittleEndian.Uint64(encoded[16:24]), Count: binary.LittleEndian.Uint64(encoded[24:32])}
	if directory.Root < 2 {
		return systemDirectory{}, ErrCorrupt
	}
	return directory, nil
}

func (tx *WriteTxn) storeSystemValue(value []byte) ([]byte, error) {
	return tx.storeOverflowValue(value, inlineSystemValueLimit, maxSystemValueBytes, PageSystemOverflow)
}

func (tx *WriteTxn) loadSystemValue(stored []byte) ([]byte, error) {
	return tx.loadOverflowValue(stored, inlineSystemValueLimit, maxSystemValueBytes, PageSystemOverflow)
}

func (tx *WriteTxn) storeOverflowValue(value []byte, inlineLimit, maximum int, pageType PageType) ([]byte, error) {
	if len(value) == 0 || len(value) > maximum {
		return nil, ErrCorrupt
	}
	if len(value)+1 <= inlineLimit {
		return append([]byte{0}, value...), nil
	}
	const overflowHeader = 16
	chunkBytes := PageSize - PageHeaderSize - overflowHeader
	chunks := (len(value) + chunkBytes - 1) / chunkBytes
	pageIDs, err := tx.allocatePageIDs(chunks)
	if err != nil {
		return nil, err
	}
	for index := range chunks {
		start, end := index*chunkBytes, (index+1)*chunkBytes
		if end > len(value) {
			end = len(value)
		}
		payload := make([]byte, overflowHeader+end-start)
		binary.LittleEndian.PutUint64(payload[0:8], uint64(len(value)))
		binary.LittleEndian.PutUint32(payload[8:12], uint32(index))
		binary.LittleEndian.PutUint32(payload[12:16], uint32(chunks))
		copy(payload[16:], value[start:end])
		link := uint64(0)
		if index+1 < chunks {
			link = pageIDs[index+1]
		}
		if err := tx.appendPageAt(pageIDs[index], pageType, 0, 1, link, payload); err != nil {
			return nil, err
		}
	}
	descriptor := make([]byte, 49)
	descriptor[0] = 1
	binary.LittleEndian.PutUint64(descriptor[1:9], uint64(len(value)))
	binary.LittleEndian.PutUint64(descriptor[9:17], pageIDs[0])
	checksum := sha256.Sum256(value)
	copy(descriptor[17:], checksum[:])
	return descriptor, nil
}

func (tx *WriteTxn) loadOverflowValue(stored []byte, inlineLimit, maximum int, pageType PageType) ([]byte, error) {
	if len(stored) == 0 {
		return nil, ErrCorrupt
	}
	if stored[0] == 0 {
		if len(stored) == 1 {
			return nil, ErrCorrupt
		}
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
	seen := make(map[uint64]struct{})
	var expectedChunks uint32
	for index := uint32(0); uint64(len(result)) < total; index++ {
		if _, duplicate := seen[pageID]; duplicate {
			return nil, ErrCorrupt
		}
		seen[pageID] = struct{}{}
		raw, err := tx.readPage(pageID)
		if err != nil {
			return nil, err
		}
		page, err := DecodePage(raw, pageID)
		if err != nil || page.Type != pageType || page.Flags != 0 || page.ItemCount != 1 || len(page.Payload) < 16 ||
			binary.LittleEndian.Uint64(page.Payload[0:8]) != total || binary.LittleEndian.Uint32(page.Payload[8:12]) != index {
			return nil, ErrCorrupt
		}
		chunks := binary.LittleEndian.Uint32(page.Payload[12:16])
		if chunks == 0 || (index > 0 && chunks != expectedChunks) || index >= chunks || uint64(len(result)+len(page.Payload)-16) > total {
			return nil, ErrCorrupt
		}
		expectedChunks = chunks
		result = append(result, page.Payload[16:]...)
		if index+1 == chunks {
			if page.Link != 0 || uint64(len(result)) != total {
				return nil, ErrCorrupt
			}
			break
		}
		if page.Link < 2 {
			return nil, ErrCorrupt
		}
		pageID = page.Link
	}
	checksum := sha256.Sum256(result)
	if !equalBytes(stored[17:], checksum[:]) {
		return nil, ErrCorrupt
	}
	return result, nil
}
