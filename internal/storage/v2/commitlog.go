package v2

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"time"
	"unicode/utf8"
)

const (
	CommitInsert CommitOperation = 1 + iota
	CommitUpdate
	CommitDelete
	CommitCatalog
)

const (
	inlineCommitValueLimit = 8 * 1024
	maxCommitValueBytes    = 64 * 1024 * 1024
)

var commitMagic = [8]byte{'M', 'E', 'L', 'D', 'C', 'M', 'T', '2'}

const commitHeaderBytes = 96

type CommitOperation uint8

type CommitChange struct {
	CollectionID uint32
	// CollectionName is present only for named catalog changes. Ordinary
	// document events use the compact stable CollectionID exclusively.
	CollectionName string
	DocumentID     [16]byte
	Operation      CommitOperation
	ChangedPaths   []string
	Before         []byte
	After          []byte
	BeforeRef      *DocumentVersionRef
	AfterRef       *DocumentVersionRef
}

type DocumentVersionRef struct {
	PrimaryRoot uint64
	DocumentID  [16]byte
}

type CommitBatch struct {
	Sequence      uint64
	TransactionID [16]byte
	CommittedAt   time.Time
	// CatalogRoot is the immutable post-commit catalog snapshot. Retaining it
	// in the Commit Log makes an exact historical Snapshot N reconstructible.
	CatalogRoot uint64
	Changes     []CommitChange
}

// AppendCommit writes one complete logical batch into the Commit Log tree. It
// must be called inside the File.Update callback for the same sequence.
func (tx *WriteTxn) AppendCommit(rootPage uint64, batch CommitBatch) (uint64, error) {
	if tx == nil {
		return 0, ErrCorrupt
	}
	tree, err := tx.OpenTree(rootPage, TreeCommitLog)
	if err != nil {
		return 0, err
	}
	logicalBytes, err := tx.appendCommitToTree(tree, batch)
	if err != nil {
		return 0, err
	}
	if logicalBytes > math.MaxUint64-tx.retainedCommitBytes {
		return 0, ErrCorrupt
	}
	tx.retainedCommitBytes += logicalBytes
	tx.retentionEvaluated = true
	return tree.Flush()
}

// AppendCommitRetained appends a business commit and advances the logical
// retention watermark in the same COW publication. Active replay pins cap the
// watermark; they never lose history to satisfy the configured window.
func (tx *WriteTxn) AppendCommitRetained(rootPage, oldest uint64, batch CommitBatch) (uint64, uint64, error) {
	if tx == nil || tx.file == nil {
		return 0, 0, ErrCorrupt
	}
	tree, err := tx.OpenTree(rootPage, TreeCommitLog)
	if err != nil {
		return 0, 0, err
	}
	logicalBytes, err := tx.appendCommitToTree(tree, batch)
	if err != nil {
		return 0, 0, err
	}
	if logicalBytes > math.MaxUint64-tx.retainedCommitBytes {
		return 0, 0, ErrCorrupt
	}
	tx.retainedCommitBytes += logicalBytes
	if oldest == 0 {
		oldest = batch.Sequence
	}
	if oldest > batch.Sequence {
		return 0, 0, ErrCorrupt
	}
	keepFrom, err := tx.pruneCommitTree(tree, oldest, batch.Sequence)
	if err != nil {
		return 0, 0, err
	}
	root, err := tree.Flush()
	return root, keepFrom, err
}

func (tx *WriteTxn) pruneCommitTree(tree *MutableTree, oldest, latest uint64) (uint64, error) {
	if tx == nil || tx.file == nil || tree == nil || oldest == 0 || oldest > latest {
		return 0, ErrCorrupt
	}
	desired := uint64(1)
	window := tx.file.commitRetentionMax
	if window == 0 {
		window = DefaultCommitRetentionMaxCommits
	}
	if latest >= window {
		desired = latest - window + 1
	}
	pinnedLimit := latest
	for _, pin := range tx.file.readers {
		if !pin.replay {
			continue
		}
		protected := pin.sequence + 1
		if protected == 0 {
			protected = pin.sequence
		}
		if pinnedLimit > protected {
			pinnedLimit = protected
		}
	}
	if tx.baseRoot.IndexBuildCatalogRoot != 0 {
		builds, err := tx.OpenTree(tx.baseRoot.IndexBuildCatalogRoot, TreeIndexBuildCatalog)
		if err != nil {
			return 0, err
		}
		items, err := builds.Scan(nil, nil, MaxConcurrentIndexBuilds+1)
		if err != nil || len(items) > MaxConcurrentIndexBuilds {
			return 0, ErrCorrupt
		}
		for _, item := range items {
			build, err := decodeIndexBuildMeta(item.Key, item.Value)
			if err != nil {
				return 0, err
			}
			if build.Phase == IndexBuildFailed || build.AppliedSequence >= latest {
				continue
			}
			protected := build.AppliedSequence + 1
			if protected == 0 || protected < oldest {
				return 0, ErrCorrupt
			}
			if pinnedLimit > protected {
				pinnedLimit = protected
			}
		}
	}
	// Durable consumers persist their acknowledgement directory in the private
	// System tree. Unlike a process-local replay pin, it survives close/reopen;
	// its smallest acknowledged+1 position caps pruning until an operator
	// explicitly removes that consumer. Reading one bounded directory here keeps
	// ordinary write publication O(number of durable consumers), never O(history).
	durableFloor, err := tx.durableConsumerRetentionFloor(tx.baseRoot.CatalogRoot, oldest, latest)
	if err != nil {
		return 0, err
	}
	if durableFloor != 0 && pinnedLimit > durableFloor {
		pinnedLimit = durableFloor
	}
	if pinnedLimit < oldest {
		pinnedLimit = oldest
	}
	tx.retentionEvaluated = true
	keepFrom := oldest
	maxBytes := tx.file.commitRetentionMaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultCommitRetentionMaxBytes
	}
	for keepFrom < latest && keepFrom < pinnedLimit && (keepFrom < desired || tx.retainedCommitBytes > maxBytes) {
		stored, err := tx.readCommitFromTree(tree, keepFrom)
		if err != nil {
			return 0, err
		}
		removedBytes, err := commitBatchLogicalBytes(stored)
		if err != nil || removedBytes > tx.retainedCommitBytes {
			return 0, ErrCorrupt
		}
		for ordinal := uint32(0); ordinal <= uint32(len(stored.Changes)); ordinal++ {
			removed, err := tree.Delete(commitKey(keepFrom, ordinal))
			if err != nil || !removed {
				return 0, ErrCorrupt
			}
		}
		tx.retainedCommitBytes -= removedBytes
		tx.retentionPruned++
		keepFrom++
	}
	tx.retentionBlocked = keepFrom < desired || tx.retainedCommitBytes > maxBytes
	return keepFrom, nil
}

func (tx *WriteTxn) appendCommitToTree(tree *MutableTree, batch CommitBatch) (uint64, error) {
	if tx == nil || tree == nil || batch.Sequence != tx.Sequence() || batch.Sequence == 0 || allZero(batch.TransactionID[:]) || len(batch.Changes) == 0 || len(batch.Changes) > math.MaxUint32-1 {
		return 0, ErrCorrupt
	}
	encodedChanges := make([][]byte, len(batch.Changes))
	logicalBytes := uint64(commitHeaderBytes)
	hash := sha256.New()
	for index, change := range batch.Changes {
		encoded, err := encodeCommitChange(change)
		if err != nil {
			return 0, err
		}
		if uint64(len(encoded)) > math.MaxUint64-logicalBytes {
			return 0, ErrCorrupt
		}
		logicalBytes += uint64(len(encoded))
		encodedChanges[index] = encoded
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(len(encoded)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(encoded)
	}
	if batch.CatalogRoot != 0 && batch.CatalogRoot < 2 {
		return 0, ErrCorrupt
	}
	if batch.CatalogRoot != 0 {
		if _, err := tx.OpenTree(batch.CatalogRoot, TreeCatalog); err != nil {
			return 0, err
		}
	}
	header := make([]byte, commitHeaderBytes)
	copy(header[:8], commitMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], FormatVersion)
	binary.LittleEndian.PutUint64(header[16:24], batch.Sequence)
	copy(header[24:40], batch.TransactionID[:])
	binary.LittleEndian.PutUint64(header[40:48], uint64(batch.CommittedAt.UnixNano()))
	binary.LittleEndian.PutUint32(header[48:52], uint32(len(batch.Changes)))
	binary.LittleEndian.PutUint64(header[56:64], batch.CatalogRoot)
	copy(header[64:96], hash.Sum(nil))
	storedHeader, err := tx.storeCommitValue(header)
	if err != nil {
		return 0, err
	}
	if err := tree.Put(commitKey(batch.Sequence, 0), storedHeader); err != nil {
		return 0, err
	}
	for index, encoded := range encodedChanges {
		stored, err := tx.storeCommitValue(encoded)
		if err != nil {
			return 0, err
		}
		if err := tree.Put(commitKey(batch.Sequence, uint32(index+1)), stored); err != nil {
			return 0, err
		}
	}
	return logicalBytes, nil
}

func commitBatchLogicalBytes(batch CommitBatch) (uint64, error) {
	total := uint64(commitHeaderBytes)
	for _, change := range batch.Changes {
		size := uint64(52 + len(change.CollectionName) + len(change.Before) + len(change.After))
		for _, path := range change.ChangedPaths {
			if uint64(2+len(path)) > math.MaxUint64-size {
				return 0, ErrCorrupt
			}
			size += uint64(2 + len(path))
		}
		if size > maxCommitValueBytes || size > math.MaxUint64-total {
			return 0, ErrCorrupt
		}
		total += size
	}
	return total, nil
}

func storedCommitLogicalBytes(stored []byte) (uint64, error) {
	if len(stored) == 0 {
		return 0, ErrCorrupt
	}
	if stored[0] == 0 {
		return uint64(len(stored) - 1), nil
	}
	if stored[0] != 1 || len(stored) != 49 {
		return 0, ErrCorrupt
	}
	total := binary.LittleEndian.Uint64(stored[1:9])
	if total < inlineCommitValueLimit || total > maxCommitValueBytes || binary.LittleEndian.Uint64(stored[9:17]) < 2 {
		return 0, ErrCorrupt
	}
	return total, nil
}

func (f *File) calculateRetainedCommitBytes(rootPage uint64) (uint64, error) {
	iterator, err := newTreeIterator(f, rootPage, TreeCommitLog, nil, nil, 0)
	if err != nil {
		return 0, err
	}
	defer iterator.Close()
	var total uint64
	for iterator.Next() {
		size, err := storedCommitLogicalBytes(iterator.Value())
		if err != nil || size > math.MaxUint64-total {
			return 0, ErrCorrupt
		}
		total += size
	}
	if err := iterator.Err(); err != nil {
		return 0, err
	}
	return total, nil
}

func (f *File) ReadCommit(rootPage, sequence uint64) (CommitBatch, error) {
	if f == nil || sequence == 0 {
		return CommitBatch{}, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return CommitBatch{}, errors.New("meldbase storage v2: file is closed")
	}
	return f.readCommitUnlocked(rootPage, sequence)
}

func (f *File) readCommitUnlocked(rootPage, sequence uint64) (CommitBatch, error) {
	tx := &WriteTxn{file: f, generation: f.meta.Generation, sequence: f.meta.CommitSequence, nextPage: f.nextPage, byID: make(map[uint64][]byte)}
	batch, err := tx.readCommitUsing(sequence, func(key []byte) ([]byte, bool, error) {
		return f.treeGetUnlocked(rootPage, TreeCommitLog, key)
	})
	if err != nil {
		return CommitBatch{}, err
	}
	if err := f.validateTreeRootUnlocked(batch.CatalogRoot, TreeCatalog); err != nil {
		return CommitBatch{}, err
	}
	return batch, nil
}

func (tx *WriteTxn) readCommitFromTree(tree *MutableTree, sequence uint64) (CommitBatch, error) {
	if tx == nil || tree == nil || sequence == 0 {
		return CommitBatch{}, ErrCorrupt
	}
	return tx.readCommitUsing(sequence, tree.Get)
}

func (tx *WriteTxn) readCommitUsing(sequence uint64, get func([]byte) ([]byte, bool, error)) (CommitBatch, error) {
	if tx == nil || sequence == 0 || get == nil {
		return CommitBatch{}, ErrCorrupt
	}
	storedHeader, ok, err := get(commitKey(sequence, 0))
	if err != nil || !ok {
		if err == nil {
			err = ErrCorrupt
		}
		return CommitBatch{}, err
	}
	header, err := tx.loadCommitValue(storedHeader)
	if err != nil || len(header) != commitHeaderBytes || string(header[:8]) != string(commitMagic[:]) || binary.LittleEndian.Uint16(header[8:10]) != FormatVersion ||
		!allZero(header[10:16]) || binary.LittleEndian.Uint64(header[16:24]) != sequence || !allZero(header[52:56]) {
		return CommitBatch{}, ErrCorrupt
	}
	count := binary.LittleEndian.Uint32(header[48:52])
	if count == 0 {
		return CommitBatch{}, ErrCorrupt
	}
	catalogRoot := binary.LittleEndian.Uint64(header[56:64])
	if catalogRoot != 0 && catalogRoot < 2 {
		return CommitBatch{}, ErrCorrupt
	}
	batch := CommitBatch{
		Sequence: sequence, CommittedAt: time.Unix(0, int64(binary.LittleEndian.Uint64(header[40:48]))),
		CatalogRoot: catalogRoot, Changes: make([]CommitChange, count),
	}
	copy(batch.TransactionID[:], header[24:40])
	if allZero(batch.TransactionID[:]) {
		return CommitBatch{}, ErrCorrupt
	}
	hash := sha256.New()
	for index := range batch.Changes {
		stored, ok, err := get(commitKey(sequence, uint32(index+1)))
		if err != nil || !ok {
			return CommitBatch{}, ErrCorrupt
		}
		encoded, err := tx.loadCommitValue(stored)
		if err != nil {
			return CommitBatch{}, err
		}
		change, err := decodeCommitChange(encoded)
		if err != nil {
			return CommitBatch{}, err
		}
		batch.Changes[index] = change
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(len(encoded)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(encoded)
	}
	if !equalBytes(header[64:96], hash.Sum(nil)) {
		return CommitBatch{}, ErrCorrupt
	}
	return batch, nil
}

func encodeCommitChange(change CommitChange) ([]byte, error) {
	if change.CollectionID == 0 || change.Operation < CommitInsert || change.Operation > CommitCatalog ||
		len(change.Before) > maxCommitValueBytes || len(change.After) > maxCommitValueBytes {
		return nil, ErrCorrupt
	}
	if change.Operation != CommitCatalog && allZero(change.DocumentID[:]) {
		return nil, ErrCorrupt
	}
	if (change.Operation != CommitCatalog && change.CollectionName != "") ||
		(change.CollectionName != "" && !validCollectionName(change.CollectionName)) {
		return nil, ErrCorrupt
	}
	if (len(change.Before) > 0 && change.BeforeRef != nil) || (len(change.After) > 0 && change.AfterRef != nil) ||
		(change.BeforeRef != nil && (change.BeforeRef.PrimaryRoot < 2 || change.BeforeRef.DocumentID != change.DocumentID)) ||
		(change.AfterRef != nil && (change.AfterRef.PrimaryRoot < 2 || change.AfterRef.DocumentID != change.DocumentID)) {
		return nil, ErrCorrupt
	}
	paths, err := normalizeChangedPaths(change.ChangedPaths)
	if err != nil {
		return nil, err
	}
	size := 52 + len(change.CollectionName) + len(change.Before) + len(change.After)
	for _, path := range paths {
		if size > maxCommitValueBytes-2-len(path) {
			return nil, ErrCorrupt
		}
		size += 2 + len(path)
	}
	if size > maxCommitValueBytes {
		return nil, ErrCorrupt
	}
	result := make([]byte, size)
	binary.LittleEndian.PutUint32(result[0:4], change.CollectionID)
	result[4] = byte(change.Operation)
	if len(change.Before) > 0 {
		result[5] |= 1
	}
	if len(change.After) > 0 {
		result[5] |= 2
	}
	if change.BeforeRef != nil {
		result[5] |= 4
		binary.LittleEndian.PutUint64(result[32:40], change.BeforeRef.PrimaryRoot)
	}
	if change.AfterRef != nil {
		result[5] |= 8
		binary.LittleEndian.PutUint64(result[40:48], change.AfterRef.PrimaryRoot)
	}
	binary.LittleEndian.PutUint16(result[6:8], uint16(len(paths)))
	copy(result[8:24], change.DocumentID[:])
	binary.LittleEndian.PutUint32(result[24:28], uint32(len(change.Before)))
	binary.LittleEndian.PutUint32(result[28:32], uint32(len(change.After)))
	binary.LittleEndian.PutUint16(result[48:50], uint16(len(change.CollectionName)))
	offset := 52
	copy(result[offset:], change.CollectionName)
	offset += len(change.CollectionName)
	for _, path := range paths {
		binary.LittleEndian.PutUint16(result[offset:offset+2], uint16(len(path)))
		offset += 2
		copy(result[offset:], path)
		offset += len(path)
	}
	copy(result[offset:], change.Before)
	offset += len(change.Before)
	copy(result[offset:], change.After)
	return result, nil
}

func normalizeChangedPaths(paths []string) ([]string, error) {
	if len(paths) > math.MaxUint16 {
		return nil, ErrCorrupt
	}
	result := append([]string(nil), paths...)
	for _, path := range result {
		if len(path) == 0 || len(path) > 1024 || !utf8.ValidString(path) {
			return nil, ErrCorrupt
		}
		for _, value := range []byte(path) {
			if value == 0 {
				return nil, ErrCorrupt
			}
		}
	}
	sort.Strings(result)
	write := 0
	for _, path := range result {
		if write > 0 && result[write-1] == path {
			continue
		}
		result[write] = path
		write++
	}
	return result[:write], nil
}

func decodeCommitChange(encoded []byte) (CommitChange, error) {
	if len(encoded) < 52 || len(encoded) > maxCommitValueBytes || encoded[5]&0xf0 != 0 || !allZero(encoded[50:52]) {
		return CommitChange{}, ErrCorrupt
	}
	change := CommitChange{
		CollectionID: binary.LittleEndian.Uint32(encoded[0:4]),
		Operation:    CommitOperation(encoded[4]),
	}
	copy(change.DocumentID[:], encoded[8:24])
	if change.CollectionID == 0 || change.Operation < CommitInsert || change.Operation > CommitCatalog {
		return CommitChange{}, ErrCorrupt
	}
	if change.Operation != CommitCatalog && allZero(change.DocumentID[:]) {
		return CommitChange{}, ErrCorrupt
	}
	flags := encoded[5]
	pathCount := int(binary.LittleEndian.Uint16(encoded[6:8]))
	beforeLength := uint64(binary.LittleEndian.Uint32(encoded[24:28]))
	afterLength := uint64(binary.LittleEndian.Uint32(encoded[28:32]))
	beforeRoot := binary.LittleEndian.Uint64(encoded[32:40])
	afterRoot := binary.LittleEndian.Uint64(encoded[40:48])
	nameLength := int(binary.LittleEndian.Uint16(encoded[48:50]))
	if ((flags&1) == 0) != (beforeLength == 0) || ((flags&2) == 0) != (afterLength == 0) ||
		(flags&4 != 0 && (beforeRoot < 2 || beforeLength != 0)) || (flags&4 == 0 && beforeRoot != 0) ||
		(flags&8 != 0 && (afterRoot < 2 || afterLength != 0)) || (flags&8 == 0 && afterRoot != 0) {
		return CommitChange{}, ErrCorrupt
	}
	if flags&4 != 0 {
		change.BeforeRef = &DocumentVersionRef{PrimaryRoot: beforeRoot, DocumentID: change.DocumentID}
	}
	if flags&8 != 0 {
		change.AfterRef = &DocumentVersionRef{PrimaryRoot: afterRoot, DocumentID: change.DocumentID}
	}
	offset := 52
	if nameLength > 128 || offset+nameLength > len(encoded) {
		return CommitChange{}, ErrCorrupt
	}
	if nameLength > 0 {
		change.CollectionName = string(encoded[offset : offset+nameLength])
		if change.Operation != CommitCatalog || !validCollectionName(change.CollectionName) {
			return CommitChange{}, ErrCorrupt
		}
	}
	offset += nameLength
	change.ChangedPaths = make([]string, pathCount)
	for index := range pathCount {
		if offset+2 > len(encoded) {
			return CommitChange{}, ErrCorrupt
		}
		length := int(binary.LittleEndian.Uint16(encoded[offset : offset+2]))
		offset += 2
		if length == 0 || length > 1024 || offset+length > len(encoded) || !utf8.Valid(encoded[offset:offset+length]) {
			return CommitChange{}, ErrCorrupt
		}
		change.ChangedPaths[index] = string(encoded[offset : offset+length])
		if index > 0 && change.ChangedPaths[index-1] >= change.ChangedPaths[index] {
			return CommitChange{}, ErrCorrupt
		}
		offset += length
	}
	if beforeLength+afterLength != uint64(len(encoded)-offset) {
		return CommitChange{}, ErrCorrupt
	}
	change.Before = append([]byte(nil), encoded[offset:offset+int(beforeLength)]...)
	offset += int(beforeLength)
	change.After = append([]byte(nil), encoded[offset:]...)
	return change, nil
}

func (tx *WriteTxn) storeCommitValue(value []byte) ([]byte, error) {
	if len(value) > maxCommitValueBytes {
		return nil, ErrCorrupt
	}
	if len(value)+1 <= inlineCommitValueLimit {
		return append([]byte{0}, value...), nil
	}
	const overflowHeader = 16
	chunkBytes := PageSize - PageHeaderSize - overflowHeader
	chunks := (len(value) + chunkBytes - 1) / chunkBytes
	pageIDs, err := tx.allocatePageIDs(chunks)
	if err != nil {
		return nil, err
	}
	firstPage := pageIDs[0]
	for index := 0; index < chunks; index++ {
		start := index * chunkBytes
		end := start + chunkBytes
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
		if err := tx.appendPageAt(pageIDs[index], PageCommitOverflow, 0, 1, link, payload); err != nil {
			return nil, err
		}
	}
	descriptor := make([]byte, 49)
	descriptor[0] = 1
	binary.LittleEndian.PutUint64(descriptor[1:9], uint64(len(value)))
	binary.LittleEndian.PutUint64(descriptor[9:17], firstPage)
	checksum := sha256.Sum256(value)
	copy(descriptor[17:], checksum[:])
	return descriptor, nil
}

func (tx *WriteTxn) loadCommitValue(stored []byte) ([]byte, error) {
	if len(stored) == 0 {
		return nil, ErrCorrupt
	}
	if stored[0] == 0 {
		return append([]byte(nil), stored[1:]...), nil
	}
	if stored[0] != 1 || len(stored) != 49 {
		return nil, ErrCorrupt
	}
	total := binary.LittleEndian.Uint64(stored[1:9])
	pageID := binary.LittleEndian.Uint64(stored[9:17])
	if total < inlineCommitValueLimit || total > maxCommitValueBytes || pageID < 2 {
		return nil, ErrCorrupt
	}
	result := make([]byte, 0, int(total))
	seen := map[uint64]struct{}{}
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
		if err != nil || page.Type != PageCommitOverflow || page.Flags != 0 || page.ItemCount != 1 || len(page.Payload) < 16 ||
			binary.LittleEndian.Uint64(page.Payload[0:8]) != total || binary.LittleEndian.Uint32(page.Payload[8:12]) != index {
			return nil, ErrCorrupt
		}
		chunks := binary.LittleEndian.Uint32(page.Payload[12:16])
		if chunks == 0 || (index > 0 && chunks != expectedChunks) || index >= chunks {
			return nil, ErrCorrupt
		}
		expectedChunks = chunks
		if uint64(len(result)+len(page.Payload)-16) > total {
			return nil, ErrCorrupt
		}
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

func commitKey(sequence uint64, ordinal uint32) []byte {
	key := make([]byte, 12)
	binary.BigEndian.PutUint64(key[:8], sequence)
	binary.BigEndian.PutUint32(key[8:], ordinal)
	return key
}
