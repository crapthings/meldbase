package v2

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
)

type DocumentRecord struct {
	DocumentID        [16]byte
	InsertionPosition uint64
	Document          []byte
}

type CollectionRecord struct {
	Name string
	Meta CollectionMeta
}

// IndexIterator streams one Secondary tree from a pinned immutable snapshot.
// It owns an independent reader pin and is not safe for concurrent use.
type IndexIterator struct {
	file   *File
	pinID  uint64
	tree   *TreeIterator
	entry  IndexEntry
	err    error
	closed bool
}

// DocumentIterator streams one collection from a pinned immutable snapshot.
// It owns an independent reader pin, so the ReadSnapshot used to create it may
// be closed immediately. DocumentIterator is not safe for concurrent use.
type DocumentIterator struct {
	file        *File
	pinID       uint64
	tree        *TreeIterator
	primaryRoot uint64
	ordered     bool
	record      DocumentRecord
	err         error
	closed      bool
}

// ReadSnapshot pins one immutable DatabaseRoot. Reclamation must retain every
// page reachable through the snapshot until Close.
type ReadSnapshot struct {
	mu     sync.Mutex
	file   *File
	pinID  uint64
	root   DatabaseRoot
	closed bool
}

// LiveCommitStream tails durable commits after a sequence. It does not keep a
// per-subscriber event queue: waiters wake, read the current root, and replay
// their own next sequence from the durable Commit Log.
type LiveCommitStream struct {
	mu                sync.Mutex
	file              *File
	pinID             uint64
	after             uint64
	deliveredSequence uint64
	closed            chan struct{}
	once              sync.Once
}

// OpenSnapshot pins only the current DatabaseRoot. It is the query/read path;
// callers that also need an N+1 stream use OpenSnapshotAndStream instead.
func (f *File) OpenSnapshot() (*ReadSnapshot, error) {
	if f == nil {
		return nil, ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return nil, err
	}
	pinID, err := f.addReaderPinUnlocked(root.CommitSequence, f.meta.RootPage, false)
	if err != nil {
		return nil, err
	}
	return &ReadSnapshot{file: f, pinID: pinID, root: root}, nil
}

// OpenSnapshotAndStream atomically pins Snapshot N and creates a stream whose
// first possible result is N+1, eliminating the query/watch registration gap.
func (f *File) OpenSnapshotAndStream() (*ReadSnapshot, *LiveCommitStream, error) {
	if f == nil {
		return nil, nil, ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil, nil, errors.New("meldbase storage v2: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return nil, nil, err
	}
	snapshotPin, err := f.addReaderPinUnlocked(root.CommitSequence, f.meta.RootPage, false)
	if err != nil {
		return nil, nil, ErrCorrupt
	}
	streamPin, err := f.addReaderPinUnlocked(root.CommitSequence, f.meta.RootPage, true)
	if err != nil {
		delete(f.readers, snapshotPin)
		return nil, nil, err
	}
	snapshot := &ReadSnapshot{file: f, pinID: snapshotPin, root: root}
	stream := &LiveCommitStream{file: f, pinID: streamPin, after: root.CommitSequence, closed: make(chan struct{})}
	return snapshot, stream, nil
}

func (f *File) OpenLiveCommitStream(after uint64) (*LiveCommitStream, error) {
	if f == nil {
		return nil, ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return nil, err
	}
	if after > root.CommitSequence {
		return nil, ErrCorrupt
	}
	if after < root.CommitSequence && (after == math.MaxUint64 || after+1 < root.OldestRetainedSequence) {
		return nil, ErrHistoryLost
	}
	pinID, err := f.addReaderPinUnlocked(after, f.meta.RootPage, true)
	if err != nil {
		return nil, err
	}
	return &LiveCommitStream{file: f, pinID: pinID, after: after, closed: make(chan struct{})}, nil
}

// OpenSnapshotAndStreamAt atomically reconstructs historical Snapshot N and
// opens a durable stream whose first result is N+1. The snapshot is available
// only while N remains in the retained Commit Log window. Sequence zero denotes
// the known empty state before commit 1.
func (f *File) OpenSnapshotAndStreamAt(sequence uint64) (*ReadSnapshot, *LiveCommitStream, error) {
	if f == nil {
		return nil, nil, ErrCorrupt
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil, nil, errors.New("meldbase storage v2: file is closed")
	}
	current, err := f.databaseRootUnlocked()
	if err != nil {
		return nil, nil, err
	}
	historical, err := f.historicalRootUnlocked(current, sequence)
	if err != nil {
		return nil, nil, err
	}
	snapshotPin, err := f.addReaderPinUnlocked(sequence, f.meta.RootPage, false)
	if err != nil {
		return nil, nil, err
	}
	streamPin, err := f.addReaderPinUnlocked(sequence, f.meta.RootPage, true)
	if err != nil {
		delete(f.readers, snapshotPin)
		return nil, nil, err
	}
	return &ReadSnapshot{file: f, pinID: snapshotPin, root: historical},
		&LiveCommitStream{file: f, pinID: streamPin, after: sequence, closed: make(chan struct{})}, nil
}

func (f *File) historicalRootUnlocked(current DatabaseRoot, sequence uint64) (DatabaseRoot, error) {
	if sequence > current.CommitSequence {
		return DatabaseRoot{}, ErrCorrupt
	}
	if sequence == current.CommitSequence {
		return current, nil
	}
	if sequence == 0 {
		if current.OldestRetainedSequence > 1 {
			return DatabaseRoot{}, ErrHistoryLost
		}
		return DatabaseRoot{CommitSequence: 0}, nil
	}
	if current.OldestRetainedSequence == 0 || sequence < current.OldestRetainedSequence {
		return DatabaseRoot{}, ErrHistoryLost
	}
	batch, err := f.readCommitUnlocked(current.CommitLogRoot, sequence)
	if err != nil {
		return DatabaseRoot{}, err
	}
	return DatabaseRoot{
		CommitSequence: sequence, CatalogRoot: batch.CatalogRoot,
		OldestRetainedSequence: current.OldestRetainedSequence,
	}, nil
}

func (f *File) addReaderPinUnlocked(sequence, rootPage uint64, replay bool) (uint64, error) {
	f.nextPin++
	if f.nextPin == 0 {
		return 0, ErrCorrupt
	}
	pinID := f.nextPin
	f.readers[pinID] = readerPin{
		generation: f.meta.Generation, sequence: sequence, rootPage: rootPage, replay: replay,
	}
	return pinID, nil
}

func (stream *LiveCommitStream) Next(ctx context.Context) (CommitBatch, error) {
	if stream == nil {
		return CommitBatch{}, ErrCursorClosed
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	for {
		select {
		case <-stream.closed:
			return CommitBatch{}, ErrCursorClosed
		default:
		}
		if stream.file == nil {
			return CommitBatch{}, ErrCursorClosed
		}
		stream.file.mu.Lock()
		if stream.file.file == nil {
			stream.file.mu.Unlock()
			return CommitBatch{}, errors.New("meldbase storage v2: file is closed")
		}
		root, err := stream.file.databaseRootUnlocked()
		if err != nil {
			stream.file.mu.Unlock()
			return CommitBatch{}, err
		}
		if stream.after < root.CommitSequence && (stream.after == math.MaxUint64 || stream.after+1 < root.OldestRetainedSequence) {
			stream.file.mu.Unlock()
			return CommitBatch{}, ErrHistoryLost
		}
		if stream.after < root.CommitSequence {
			sequence := stream.after + 1
			batch, err := stream.file.readCommitUnlocked(root.CommitLogRoot, sequence)
			if err != nil {
				stream.file.mu.Unlock()
				return CommitBatch{}, err
			}
			stream.after = sequence
			stream.deliveredSequence = sequence
			pin, exists := stream.file.readers[stream.pinID]
			if !exists {
				stream.file.mu.Unlock()
				return CommitBatch{}, ErrCorrupt
			}
			pin.generation, pin.sequence, pin.rootPage = stream.file.meta.Generation, sequence, stream.file.meta.RootPage
			stream.file.readers[stream.pinID] = pin
			stream.file.mu.Unlock()
			return batch, nil
		}
		changed := stream.file.changed
		stream.file.mu.Unlock()
		select {
		case <-ctx.Done():
			return CommitBatch{}, ctx.Err()
		case <-stream.closed:
			return CommitBatch{}, ErrCursorClosed
		case <-changed:
		}
	}
}

func (stream *LiveCommitStream) Close() error {
	if stream == nil {
		return nil
	}
	stream.once.Do(func() { close(stream.closed) })
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.file != nil {
		stream.file.mu.Lock()
		delete(stream.file.readers, stream.pinID)
		stream.file.mu.Unlock()
	}
	stream.file = nil
	return nil
}

func (snapshot *ReadSnapshot) Sequence() uint64 {
	if snapshot == nil {
		return 0
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	return snapshot.root.CommitSequence
}

// ReadCommit resolves one commit from this snapshot's pinned CommitLogRoot.
// DocumentVersionRef values returned by it remain protected until Close.
func (snapshot *ReadSnapshot) ReadCommit(sequence uint64) (CommitBatch, error) {
	if snapshot == nil || sequence == 0 {
		return CommitBatch{}, ErrCorrupt
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.file == nil {
		return CommitBatch{}, ErrCursorClosed
	}
	if sequence > snapshot.root.CommitSequence || sequence < snapshot.root.OldestRetainedSequence {
		return CommitBatch{}, ErrHistoryLost
	}
	snapshot.file.mu.RLock()
	defer snapshot.file.mu.RUnlock()
	if snapshot.file.file == nil {
		return CommitBatch{}, errors.New("meldbase storage v2: file is closed")
	}
	return snapshot.file.readCommitUnlocked(snapshot.root.CommitLogRoot, sequence)
}

// ReadDocumentVersion reads a version reference obtained from a commit in this
// same pinned snapshot. The reader pin prevents reclamation/reuse of its Primary
// root while the version is decoded.
func (snapshot *ReadSnapshot) ReadDocumentVersion(reference DocumentVersionRef) ([]byte, error) {
	if snapshot == nil || reference.PrimaryRoot < 2 || allZero(reference.DocumentID[:]) {
		return nil, ErrCorrupt
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.file == nil {
		return nil, ErrCursorClosed
	}
	snapshot.file.mu.RLock()
	defer snapshot.file.mu.RUnlock()
	if snapshot.file.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	value, exists, err := snapshot.file.readDocumentUnlocked(reference.PrimaryRoot, reference.DocumentID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrCorrupt
	}
	return value, nil
}

// CollectionMeta returns metadata from this snapshot's immutable CatalogRoot,
// including the stable numeric collection ID used by Commit Log changes.
func (snapshot *ReadSnapshot) CollectionMeta(collection string) (CollectionMeta, bool, error) {
	if snapshot == nil || !validCollectionName(collection) {
		return CollectionMeta{}, false, ErrCorrupt
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.file == nil {
		return CollectionMeta{}, false, ErrCursorClosed
	}
	snapshot.file.mu.RLock()
	defer snapshot.file.mu.RUnlock()
	if snapshot.file.file == nil {
		return CollectionMeta{}, false, errors.New("meldbase storage v2: file is closed")
	}
	encoded, exists, err := snapshot.file.treeGetUnlocked(snapshot.root.CatalogRoot, TreeCatalog, []byte(collection))
	if err != nil || !exists {
		return CollectionMeta{}, false, err
	}
	meta, err := decodeCollectionMeta(encoded)
	if err != nil {
		return CollectionMeta{}, false, err
	}
	return meta, true, nil
}

func (snapshot *ReadSnapshot) Collections() ([]CollectionRecord, error) {
	if snapshot == nil {
		return nil, ErrCorrupt
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.file == nil {
		return nil, ErrCursorClosed
	}
	snapshot.file.mu.RLock()
	defer snapshot.file.mu.RUnlock()
	if snapshot.file.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	tx := &WriteTxn{file: snapshot.file, generation: snapshot.file.meta.Generation, sequence: snapshot.root.CommitSequence, nextPage: snapshot.file.nextPage, byID: make(map[uint64][]byte)}
	tree, err := tx.OpenTree(snapshot.root.CatalogRoot, TreeCatalog)
	if err != nil {
		return nil, err
	}
	pairs, err := tree.Scan(nil, nil, 0)
	if err != nil {
		return nil, err
	}
	result := make([]CollectionRecord, 0, snapshot.root.CollectionCount)
	seenIDs := make(map[uint32]struct{}, snapshot.root.CollectionCount)
	for _, pair := range pairs {
		if bytes.Equal(pair.Key, systemCatalogKey) {
			if _, err := decodeSystemDirectory(pair.Value); err != nil {
				return nil, err
			}
			continue
		}
		name := string(pair.Key)
		if !validCollectionName(name) {
			return nil, ErrCorrupt
		}
		meta, err := decodeCollectionMeta(pair.Value)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seenIDs[meta.ID]; duplicate {
			return nil, ErrCorrupt
		}
		seenIDs[meta.ID] = struct{}{}
		result = append(result, CollectionRecord{Name: name, Meta: meta})
	}
	if uint64(len(result)) != snapshot.root.CollectionCount {
		return nil, ErrCorrupt
	}
	return result, nil
}

func (snapshot *ReadSnapshot) GetDocument(collection string, documentID [16]byte) ([]byte, bool, error) {
	record, exists, err := snapshot.GetDocumentRecord(collection, documentID)
	return record.Document, exists, err
}

// GetDocumentRecord resolves one primary record including its stable insertion
// position, which query execution uses as the deterministic sort tie breaker.
func (snapshot *ReadSnapshot) GetDocumentRecord(collection string, documentID [16]byte) (DocumentRecord, bool, error) {
	if snapshot == nil || !validCollectionName(collection) || allZero(documentID[:]) {
		return DocumentRecord{}, false, ErrCorrupt
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.file == nil {
		return DocumentRecord{}, false, ErrCursorClosed
	}
	snapshot.file.mu.RLock()
	defer snapshot.file.mu.RUnlock()
	if snapshot.file.file == nil {
		return DocumentRecord{}, false, errors.New("meldbase storage v2: file is closed")
	}
	encodedMeta, exists, err := snapshot.file.treeGetUnlocked(snapshot.root.CatalogRoot, TreeCatalog, []byte(collection))
	if err != nil || !exists {
		return DocumentRecord{}, false, err
	}
	meta, err := decodeCollectionMeta(encodedMeta)
	if err != nil {
		return DocumentRecord{}, false, err
	}
	stored, exists, err := snapshot.file.treeGetUnlocked(meta.PrimaryRoot, TreePrimary, documentID[:])
	if err != nil || !exists {
		return DocumentRecord{}, false, err
	}
	tx := &WriteTxn{file: snapshot.file, nextPage: snapshot.file.nextPage, byID: make(map[uint64][]byte)}
	position, document, err := tx.loadDocumentRecord(stored)
	if err != nil {
		return DocumentRecord{}, false, err
	}
	return DocumentRecord{DocumentID: documentID, InsertionPosition: position, Document: document}, true, nil
}

func (snapshot *ReadSnapshot) ScanCollection(collection string, start, end *[16]byte, limit int) ([]DocumentRecord, error) {
	iterator, err := snapshot.OpenCollectionIterator(collection, start, end, limit)
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	result := make([]DocumentRecord, 0)
	for iterator.Next() {
		result = append(result, iterator.Record())
	}
	return result, iterator.Err()
}

// OpenCollectionIterator creates a bounded-memory scan over [start, end). A
// nil bound is open and a non-positive limit is unbounded.
func (snapshot *ReadSnapshot) OpenCollectionIterator(collection string, start, end *[16]byte, limit int) (*DocumentIterator, error) {
	if snapshot == nil || !validCollectionName(collection) {
		return nil, ErrCorrupt
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.file == nil {
		return nil, ErrCursorClosed
	}
	file := snapshot.file
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	encodedMeta, exists, err := file.treeGetUnlocked(snapshot.root.CatalogRoot, TreeCatalog, []byte(collection))
	if err != nil {
		return nil, err
	}
	primaryRoot := uint64(0)
	if exists {
		meta, err := decodeCollectionMeta(encodedMeta)
		if err != nil {
			return nil, err
		}
		primaryRoot = meta.PrimaryRoot
	}
	var startKey, endKey []byte
	if start != nil {
		startKey = start[:]
	}
	if end != nil {
		endKey = end[:]
	}
	tree, err := newTreeIterator(file, primaryRoot, TreePrimary, startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	iterator := &DocumentIterator{file: file, tree: tree}
	if primaryRoot == 0 {
		return iterator, nil
	}
	pin, exists := file.readers[snapshot.pinID]
	if !exists {
		return nil, ErrCorrupt
	}
	file.nextPin++
	if file.nextPin == 0 {
		return nil, ErrCorrupt
	}
	iterator.pinID = file.nextPin
	file.readers[iterator.pinID] = pin
	return iterator, nil
}

// OpenInsertionOrderIterator streams documents by their durable insertion
// position over [start, end). It resolves every Order entry through the Primary
// tree from the same snapshot and owns an independent reader pin.
func (snapshot *ReadSnapshot) OpenInsertionOrderIterator(collection string, start, end *uint64, limit int) (*DocumentIterator, error) {
	if snapshot == nil || !validCollectionName(collection) || (start != nil && *start == 0) || (end != nil && *end == 0) {
		return nil, ErrCorrupt
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.file == nil {
		return nil, ErrCursorClosed
	}
	file := snapshot.file
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.file == nil {
		return nil, errors.New("meldbase storage v2: file is closed")
	}
	encodedMeta, exists, err := file.treeGetUnlocked(snapshot.root.CatalogRoot, TreeCatalog, []byte(collection))
	if err != nil {
		return nil, err
	}
	var orderRoot, primaryRoot uint64
	if exists {
		meta, err := decodeCollectionMeta(encodedMeta)
		if err != nil {
			return nil, err
		}
		orderRoot, primaryRoot = meta.OrderRoot, meta.PrimaryRoot
	}
	var startKey, endKey []byte
	if start != nil {
		startKey = insertionPositionKey(*start)
	}
	if end != nil {
		endKey = insertionPositionKey(*end)
	}
	tree, err := newTreeIterator(file, orderRoot, TreeOrder, startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	iterator := &DocumentIterator{file: file, tree: tree, primaryRoot: primaryRoot, ordered: true}
	if orderRoot == 0 {
		return iterator, nil
	}
	pin, exists := file.readers[snapshot.pinID]
	if !exists {
		return nil, ErrCorrupt
	}
	file.nextPin++
	if file.nextPin == 0 {
		return nil, ErrCorrupt
	}
	iterator.pinID = file.nextPin
	file.readers[iterator.pinID] = pin
	return iterator, nil
}

func (iterator *DocumentIterator) Next() bool {
	if iterator == nil || iterator.closed || iterator.err != nil || iterator.tree == nil || iterator.tree.done {
		return false
	}
	iterator.record = DocumentRecord{}
	iterator.file.mu.RLock()
	if iterator.file.file == nil {
		iterator.err = errors.New("meldbase storage v2: file is closed")
		iterator.file.mu.RUnlock()
		iterator.releasePin()
		return false
	}
	if !iterator.tree.nextUnlocked() {
		iterator.err = iterator.tree.Err()
		iterator.file.mu.RUnlock()
		iterator.releasePin()
		return false
	}
	key, value := iterator.tree.Key(), iterator.tree.Value()
	stored := value
	if iterator.ordered {
		if len(key) != 8 || binary.BigEndian.Uint64(key) == 0 || len(value) != len(iterator.record.DocumentID) || iterator.primaryRoot < 2 {
			iterator.err = ErrCorrupt
		} else {
			iterator.record.InsertionPosition = binary.BigEndian.Uint64(key)
			copy(iterator.record.DocumentID[:], value)
			if allZero(iterator.record.DocumentID[:]) {
				iterator.err = ErrCorrupt
			} else {
				var exists bool
				stored, exists, iterator.err = iterator.file.treeGetUnlocked(iterator.primaryRoot, TreePrimary, iterator.record.DocumentID[:])
				if iterator.err == nil && !exists {
					iterator.err = ErrCorrupt
				}
			}
		}
	} else if len(key) != len(iterator.record.DocumentID) {
		iterator.err = ErrCorrupt
	} else {
		copy(iterator.record.DocumentID[:], key)
	}
	if iterator.err == nil {
		tx := &WriteTxn{file: iterator.file, nextPage: iterator.file.nextPage, byID: make(map[uint64][]byte)}
		position, document, err := tx.loadDocumentRecord(stored)
		if iterator.ordered && err == nil && position != iterator.record.InsertionPosition {
			err = ErrCorrupt
		}
		iterator.record.InsertionPosition, iterator.record.Document, iterator.err = position, document, err
	}
	iterator.file.mu.RUnlock()
	if iterator.err != nil {
		iterator.releasePin()
		return false
	}
	if iterator.tree.done {
		iterator.releasePin()
	}
	return true
}

func (iterator *DocumentIterator) Record() DocumentRecord {
	if iterator == nil {
		return DocumentRecord{}
	}
	return iterator.record
}

func (iterator *DocumentIterator) Err() error {
	if iterator == nil {
		return ErrCorrupt
	}
	return iterator.err
}

func (iterator *DocumentIterator) Close() error {
	if iterator == nil || iterator.closed {
		return nil
	}
	iterator.closed = true
	if iterator.tree != nil {
		_ = iterator.tree.Close()
	}
	iterator.releasePin()
	iterator.file = nil
	return nil
}

func (iterator *DocumentIterator) releasePin() {
	if iterator == nil || iterator.file == nil || iterator.pinID == 0 {
		return
	}
	iterator.file.mu.Lock()
	delete(iterator.file.readers, iterator.pinID)
	iterator.file.mu.Unlock()
	iterator.pinID = 0
}

func (snapshot *ReadSnapshot) Close() error {
	if snapshot == nil {
		return nil
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed {
		return nil
	}
	snapshot.closed = true
	if snapshot.file != nil {
		snapshot.file.mu.Lock()
		delete(snapshot.file.readers, snapshot.pinID)
		snapshot.file.mu.Unlock()
	}
	snapshot.file = nil
	return nil
}

func collectionMetaFromTree(tx *WriteTxn, catalogRoot uint64, collection string) (CollectionMeta, bool, error) {
	catalog, err := tx.OpenTree(catalogRoot, TreeCatalog)
	if err != nil {
		return CollectionMeta{}, false, err
	}
	encoded, exists, err := catalog.Get([]byte(collection))
	if err != nil || !exists {
		return CollectionMeta{}, false, err
	}
	meta, err := decodeCollectionMeta(encoded)
	return meta, err == nil, err
}
