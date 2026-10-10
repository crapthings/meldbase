package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"time"
)

const (
	// DocumentInsert identifies a document insertion.
	DocumentInsert CommitOperation = CommitInsert
	// DocumentUpdate identifies a document replacement.
	DocumentUpdate CommitOperation = CommitUpdate
	// DocumentDelete identifies a document deletion.
	DocumentDelete CommitOperation = CommitDelete
)

const (
	// inlineDocumentLimit bounds the bytes stored directly in a document descriptor.
	inlineDocumentLimit = 8 * 1024
	// maxDocumentBytes bounds the encoded size of a stored document.
	maxDocumentBytes = 64 * 1024 * 1024
)

// catalogValueMagic identifies encoded collection catalog records.
var catalogValueMagic = [8]byte{'M', 'E', 'L', 'D', 'C', 'O', 'L', '2'}

// documentRecordMagic identifies stored document record headers.
var documentRecordMagic = [8]byte{'M', 'E', 'L', 'D', 'R', 'E', 'C', '2'}

// ErrDocumentConflict reports that an optimistic point precondition no longer
// matches the current root.
//
// It is a safe logical rejection, not corruption or
// a durability failure.
var ErrDocumentConflict = errors.New("meldbase storage: document precondition conflicted")

// ErrDocumentExists distinguishes an insert collision from an optimistic
// update/delete precondition conflict.
//
// The public DB maps it to ErrDuplicateID
// so a grouped candidate can fall back to ordinary per-request semantics.
var ErrDocumentExists = errors.New("meldbase storage: document already exists")

// documentRecordHeaderBytes is the byte size of a document record header.
const documentRecordHeaderBytes = 24

// CollectionMeta records a collection's tree roots, document counts, and commit versions.
type CollectionMeta struct {
	ID               uint32
	PrimaryRoot      uint64
	OrderRoot        uint64
	IndexCatalogRoot uint64
	DocumentCount    uint64
	CreatedSequence  uint64
	UpdatedSequence  uint64
	// NextDocumentPosition is the greatest insertion position ever assigned in
	// this collection. Positions are never reused: update preserves one, while
	// delete followed by insert receives a newer position.
	NextDocumentPosition uint64
}

// DocumentMutation describes one document change and its index key changes.
type DocumentMutation struct {
	Collection   string
	DocumentID   [16]byte
	Operation    CommitOperation
	Document     []byte
	ChangedPaths []string
	// Indexes must contain exactly one entry for every index currently defined
	// on the collection. BeforeKey/AfterKey are encoded scalar keys; an empty key
	// means the indexed field is absent.
	Indexes []IndexMutation
}

// DocumentPrecondition binds an optimistic transaction to the canonical
// document bytes it actually read.
//
// ExpectedHash is SHA-256 over the decoded
// storage document body when ExpectedExists is true and must otherwise be zero.
type DocumentPrecondition struct {
	Collection     string
	DocumentID     [16]byte
	ExpectedExists bool
	ExpectedHash   [32]byte
}

// CollectionPrecondition records the collection version that a transaction requires at commit.
//
// Any document or published-index change to the collection invalidates it.
// Checking the whole collection prevents new matching documents from appearing
// between a range read and commit without relying on an in-memory query plan.
//
// ExpectedID and ExpectedUpdatedSequence are required only when ExpectedExists
// is true. A missing collection is a valid read state and conflicts if another
// transaction creates it before publication.
type CollectionPrecondition struct {
	Collection              string
	ExpectedExists          bool
	ExpectedID              uint32
	ExpectedUpdatedSequence uint64
}

// DocumentTransaction groups document changes with the read conditions required to commit them.
type DocumentTransaction struct {
	TransactionID           [16]byte
	CommittedAt             time.Time
	Preconditions           []DocumentPrecondition
	CollectionPreconditions []CollectionPrecondition
	Mutations               []DocumentMutation
}

// DocumentSystemTransaction atomically publishes document mutations and a
// bounded set of private system-record changes.
//
// Every CAS must match or no
// document/system mutation is published.
type DocumentSystemTransaction struct {
	DocumentTransaction DocumentTransaction
	SystemRecords       []SystemRecordMutation
}

// collectionWriteState holds the mutable trees and metadata for a collection being changed.
type collectionWriteState struct {
	name         string
	meta         CollectionMeta
	originalRoot uint64
	tree         *MutableTree
	order        *MutableTree
	indexCatalog *MutableTree
	indexes      map[string]*indexWriteState
	created      bool
}

// indexWriteState tracks one mutable index and whether its contents changed.
type indexWriteState struct {
	meta    IndexMeta
	tree    *MutableTree
	changed bool
}

// pendingIndexMutation records the index keys to replace for a document.
type pendingIndexMutation struct {
	state      *indexWriteState
	documentID [16]byte
	position   uint64
	beforeKey  []byte
	afterKey   []byte
}

// pendingDocumentChange links a commit event to its collection write state.
type pendingDocumentChange struct {
	change CommitChange
	state  *collectionWriteState
}

// ApplyDocumentTransaction atomically publishes document roots, catalog roots,
// and one matching durable CommitBatch.
//
// Duplicate document mutations inside one
// transaction are rejected so each logical change has unambiguous before/after
// version roots.
func (f *File) ApplyDocumentTransaction(transaction DocumentTransaction) (uint64, error) {
	result, err := f.applyDocumentSystemTransaction(transaction, nil)
	return result.Sequence, err
}

// ApplyDocumentTransactionGroup commits ordered document transactions in one physical generation.
//
// Callers receive one sequence per logical member, while recovery observes
// either the whole group or none of it.
// System-record CAS is intentionally excluded until its separate idempotency
// and rollback-anchor matrix has group evidence.
func (f *File) ApplyDocumentTransactionGroup(transactions []DocumentTransaction) ([]uint64, error) {
	if f == nil || len(transactions) < 2 || len(transactions) > 256 {
		return nil, ErrCorrupt
	}
	seen := make(map[[16]byte]struct{}, len(transactions))
	for index := range transactions {
		transaction := &transactions[index]
		if allZero(transaction.TransactionID[:]) || len(transaction.Mutations) == 0 {
			return nil, ErrCorrupt
		}
		if _, duplicate := seen[transaction.TransactionID]; duplicate {
			return nil, ErrCorrupt
		}
		seen[transaction.TransactionID] = struct{}{}
		if transaction.CommittedAt.IsZero() {
			transaction.CommittedAt = time.Now()
		}
	}
	sequences := make([]uint64, len(transactions))
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil || f.fatalErr != nil {
		if f.fatalErr != nil {
			return nil, f.fatalErr
		}
		return nil, ErrCorrupt
	}
	if uint64(len(transactions)) > math.MaxUint64-f.meta.CommitSequence {
		return nil, ErrCorrupt
	}
	err := f.updateUnlockedAdvance(uint64(len(transactions)), func(tx *WriteTxn) (DatabaseRoot, error) {
		base := tx.BaseRoot()
		for index, transaction := range transactions {
			if base.CommitSequence == math.MaxUint64 {
				return DatabaseRoot{}, ErrCorrupt
			}
			tx.sequence = base.CommitSequence + 1
			result := SystemRecordResult{}
			next, err := applyDocumentSystemTransactionInWriteTxn(tx, base, transaction, nil, &result)
			if err != nil {
				return DatabaseRoot{}, err
			}
			if !result.Applied || result.Sequence != tx.sequence || next.CommitSequence != tx.sequence {
				return DatabaseRoot{}, ErrCorrupt
			}
			sequences[index] = tx.sequence
			base = next
		}
		return base, nil
	})
	if err != nil {
		return nil, err
	}
	return sequences, nil
}

// ApplyDocumentSystemTransaction applies all document mutations only if every
// system-record precondition matches.
//
// A mismatch returns Applied=false and the
// isolated current value of the first mismatch without publishing any change.
func (f *File) ApplyDocumentSystemTransaction(transaction DocumentSystemTransaction) (SystemRecordResult, error) {
	if len(transaction.SystemRecords) == 0 || len(transaction.SystemRecords) > 256 {
		return SystemRecordResult{}, ErrCorrupt
	}
	seen := make(map[string]struct{}, len(transaction.SystemRecords))
	for _, mutation := range transaction.SystemRecords {
		if !validSystemRecordMutation(mutation) {
			return SystemRecordResult{}, ErrCorrupt
		}
		key := string(mutation.Key)
		if _, duplicate := seen[key]; duplicate {
			return SystemRecordResult{}, ErrCorrupt
		}
		seen[key] = struct{}{}
	}
	return f.applyDocumentSystemTransaction(transaction.DocumentTransaction, transaction.SystemRecords)
}

// applyDocumentSystemTransaction commits one document transaction with optional system record changes.
func (f *File) applyDocumentSystemTransaction(transaction DocumentTransaction, systemRecords []SystemRecordMutation) (SystemRecordResult, error) {
	if f == nil || allZero(transaction.TransactionID[:]) || len(transaction.Mutations) == 0 {
		return SystemRecordResult{}, ErrCorrupt
	}
	if transaction.CommittedAt.IsZero() {
		transaction.CommittedAt = time.Now()
	}
	var result SystemRecordResult
	err := f.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		return applyDocumentSystemTransactionInWriteTxn(tx, tx.BaseRoot(), transaction, systemRecords, &result)
	})
	if len(systemRecords) > 0 && errors.Is(err, errSystemCASMismatch) {
		return result, nil
	}
	return result, err
}

// applyDocumentSystemTransactionInWriteTxn builds one logical document commit in a caller-owned write transaction.
//
// The caller can build several logical commits before publishing the resulting
// physical generation, as the group commit path does.
func applyDocumentSystemTransactionInWriteTxn(
	tx *WriteTxn,
	base DatabaseRoot,
	transaction DocumentTransaction,
	systemRecords []SystemRecordMutation,
	result *SystemRecordResult,
) (DatabaseRoot, error) {
	if tx == nil || result == nil {
		return DatabaseRoot{}, ErrCorrupt
	}
	catalog, err := tx.OpenTree(base.CatalogRoot, TreeCatalog)
	if err != nil {
		return DatabaseRoot{}, err
	}
	states := make(map[string]*collectionWriteState)
	seenDocuments := make(map[string]struct{}, len(transaction.Mutations))
	pending := make([]pendingDocumentChange, 0, len(transaction.Mutations))
	pendingIndexes := make([]pendingIndexMutation, 0)
	newCollections := uint64(0)
	documentDelta := int64(0)
	if err := validateCollectionPreconditions(catalog, transaction.CollectionPreconditions); err != nil {
		return DatabaseRoot{}, err
	}
	if err := validateDocumentPreconditions(tx, catalog, transaction.Preconditions); err != nil {
		return DatabaseRoot{}, err
	}

	for _, mutation := range transaction.Mutations {
		if !validCollectionName(mutation.Collection) || allZero(mutation.DocumentID[:]) ||
			mutation.Operation < DocumentInsert || mutation.Operation > DocumentDelete || len(mutation.Document) > maxDocumentBytes {
			return DatabaseRoot{}, ErrCorrupt
		}
		if mutation.Operation == DocumentDelete {
			if len(mutation.Document) != 0 {
				return DatabaseRoot{}, ErrCorrupt
			}
		} else if len(mutation.Document) == 0 {
			return DatabaseRoot{}, ErrCorrupt
		}
		identity := mutation.Collection + "\x00" + string(mutation.DocumentID[:])
		if _, duplicate := seenDocuments[identity]; duplicate {
			return DatabaseRoot{}, errors.New("meldbase storage: duplicate document mutation")
		}
		seenDocuments[identity] = struct{}{}

		state := states[mutation.Collection]
		if state == nil {
			state = &collectionWriteState{name: mutation.Collection}
			encoded, exists, err := catalog.Get([]byte(mutation.Collection))
			if err != nil {
				return DatabaseRoot{}, err
			}
			if exists {
				state.meta, err = decodeCollectionMeta(encoded)
				if err != nil {
					return DatabaseRoot{}, err
				}
			} else {
				newCollections++
				if base.CollectionCount+newCollections > math.MaxUint32 {
					return DatabaseRoot{}, ErrCorrupt
				}
				state.created = true
				state.meta = CollectionMeta{ID: uint32(base.CollectionCount + newCollections), CreatedSequence: tx.Sequence()}
			}
			state.originalRoot = state.meta.PrimaryRoot
			state.tree, err = tx.OpenTree(state.meta.PrimaryRoot, TreePrimary)
			if err != nil {
				return DatabaseRoot{}, err
			}
			state.order, err = tx.OpenTree(state.meta.OrderRoot, TreeOrder)
			if err != nil {
				return DatabaseRoot{}, err
			}
			if state.tree.root.count != state.meta.DocumentCount || state.order.root.count != state.meta.DocumentCount {
				return DatabaseRoot{}, ErrCorrupt
			}
			if err := tx.loadCollectionIndexes(state); err != nil {
				return DatabaseRoot{}, err
			}
			states[mutation.Collection] = state
		}

		stored, exists, err := state.tree.Get(mutation.DocumentID[:])
		if err != nil {
			return DatabaseRoot{}, err
		}
		if mutation.Operation == DocumentInsert && exists {
			return DatabaseRoot{}, ErrDocumentExists
		}
		if mutation.Operation != DocumentInsert && !exists {
			return DatabaseRoot{}, ErrDocumentConflict
		}
		position := uint64(0)
		if exists {
			position, _, err = tx.loadDocumentRecord(stored)
			if err != nil {
				return DatabaseRoot{}, err
			}
			owner, orderExists, err := state.order.Get(insertionPositionKey(position))
			if err != nil || !orderExists || !bytes.Equal(owner, mutation.DocumentID[:]) {
				return DatabaseRoot{}, ErrCorrupt
			}
		}
		if mutation.Operation == DocumentInsert {
			if state.meta.NextDocumentPosition == math.MaxUint64 {
				return DatabaseRoot{}, ErrCorrupt
			}
			position = state.meta.NextDocumentPosition + 1
		}
		indexChanges, err := prepareIndexMutations(state, mutation, position)
		if err != nil {
			return DatabaseRoot{}, err
		}
		pendingIndexes = append(pendingIndexes, indexChanges...)
		change := CommitChange{
			CollectionID: state.meta.ID, DocumentID: mutation.DocumentID, Operation: mutation.Operation,
			ChangedPaths: append([]string(nil), mutation.ChangedPaths...),
		}
		if mutation.Operation != DocumentInsert {
			change.BeforeRef = &DocumentVersionRef{PrimaryRoot: state.originalRoot, DocumentID: mutation.DocumentID}
		}
		if mutation.Operation == DocumentDelete {
			removed, err := state.tree.Delete(mutation.DocumentID[:])
			if err != nil || !removed {
				return DatabaseRoot{}, ErrCorrupt
			}
			state.meta.DocumentCount--
			orderRemoved, err := state.order.Delete(insertionPositionKey(position))
			if err != nil || !orderRemoved {
				return DatabaseRoot{}, ErrCorrupt
			}
			documentDelta--
		} else {
			if mutation.Operation == DocumentInsert {
				state.meta.NextDocumentPosition++
				if position != state.meta.NextDocumentPosition {
					return DatabaseRoot{}, ErrCorrupt
				}
				key := insertionPositionKey(position)
				if _, duplicate, err := state.order.Get(key); err != nil || duplicate {
					if err != nil {
						return DatabaseRoot{}, err
					}
					return DatabaseRoot{}, ErrCorrupt
				}
				if err := state.order.Put(key, mutation.DocumentID[:]); err != nil {
					return DatabaseRoot{}, err
				}
			}
			descriptor, err := tx.storeDocumentRecord(position, mutation.Document)
			if err != nil {
				return DatabaseRoot{}, err
			}
			if err := state.tree.Put(mutation.DocumentID[:], descriptor); err != nil {
				return DatabaseRoot{}, err
			}
			if mutation.Operation == DocumentInsert {
				state.meta.DocumentCount++
				documentDelta++
			}
		}
		pending = append(pending, pendingDocumentChange{change: change, state: state})
	}
	if err := applyIndexMutations(pendingIndexes); err != nil {
		return DatabaseRoot{}, err
	}

	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	sort.Strings(names)
	catalogChanges := make([]CommitChange, 0, newCollections)
	for _, name := range names {
		state := states[name]
		indexNames := make([]string, 0, len(state.indexes))
		for indexName, index := range state.indexes {
			if index.changed {
				indexNames = append(indexNames, indexName)
			}
		}
		sort.Strings(indexNames)
		for _, indexName := range indexNames {
			index := state.indexes[indexName]
			index.meta.Root, err = index.tree.Flush()
			if err != nil {
				return DatabaseRoot{}, err
			}
			index.meta.UpdatedSequence = tx.Sequence()
			encoded, err := encodeIndexMeta(index.meta)
			if err != nil {
				return DatabaseRoot{}, err
			}
			if err := state.indexCatalog.Put([]byte(indexName), encoded); err != nil {
				return DatabaseRoot{}, err
			}
		}
		if len(indexNames) > 0 {
			state.meta.IndexCatalogRoot, err = state.indexCatalog.Flush()
			if err != nil {
				return DatabaseRoot{}, err
			}
		}
		state.meta.PrimaryRoot, err = state.tree.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		state.meta.OrderRoot, err = state.order.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		if state.tree.root.count != state.meta.DocumentCount || state.order.root.count != state.meta.DocumentCount {
			return DatabaseRoot{}, ErrCorrupt
		}
		state.meta.UpdatedSequence = tx.Sequence()
		encoded, err := encodeCollectionMeta(state.meta)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := catalog.Put([]byte(name), encoded); err != nil {
			return DatabaseRoot{}, err
		}
		if state.created {
			catalogChanges = append(catalogChanges, CommitChange{CollectionID: state.meta.ID, CollectionName: name, Operation: CommitCatalog, ChangedPaths: []string{"_catalog"}, After: encoded})
		}
	}
	for _, systemRecord := range systemRecords {
		current, applied, err := tx.applySystemRecordMutation(catalog, systemRecord)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if !applied {
			result.Current = current
			return DatabaseRoot{}, errSystemCASMismatch
		}
	}
	catalogRoot, err := catalog.Flush()
	if err != nil {
		return DatabaseRoot{}, err
	}
	changes := make([]CommitChange, 0, len(pending)+len(catalogChanges))
	changes = append(changes, catalogChanges...)
	for _, item := range pending {
		if item.change.Operation != DocumentDelete {
			item.change.AfterRef = &DocumentVersionRef{PrimaryRoot: item.state.meta.PrimaryRoot, DocumentID: item.change.DocumentID}
		}
		changes = append(changes, item.change)
	}
	commitLogRoot, oldest, err := tx.AppendCommitRetained(base.CommitLogRoot, base.OldestRetainedSequence, CommitBatch{
		Sequence: tx.Sequence(), TransactionID: transaction.TransactionID, CommittedAt: transaction.CommittedAt,
		CatalogRoot: catalogRoot, Changes: changes,
	})
	if err != nil {
		return DatabaseRoot{}, err
	}
	if documentDelta < 0 && uint64(-documentDelta) > base.DocumentCount {
		return DatabaseRoot{}, ErrCorrupt
	}
	documentCount := base.DocumentCount
	if documentDelta >= 0 {
		documentCount += uint64(documentDelta)
	} else {
		documentCount -= uint64(-documentDelta)
	}
	result.Sequence, result.Applied = tx.Sequence(), true
	return DatabaseRoot{
		CommitSequence: tx.Sequence(), CatalogRoot: catalogRoot, CommitLogRoot: commitLogRoot,
		OldestRetainedSequence: oldest, CatalogGeneration: base.CatalogGeneration + newCollections,
		DocumentCount: documentCount, CollectionCount: base.CollectionCount + newCollections,
	}, nil
}

// insertionPositionKey encodes a document position in sortable byte order.
func insertionPositionKey(position uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, position)
	return key
}

// GetDocument reads a document from the current committed collection.
func (f *File) GetDocument(collection string, documentID [16]byte) ([]byte, bool, error) {
	if f == nil || !validCollectionName(collection) || allZero(documentID[:]) {
		return nil, false, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return nil, false, errors.New("meldbase storage: file is closed")
	}
	root, err := f.databaseRootUnlocked()
	if err != nil {
		return nil, false, err
	}
	encodedMeta, exists, err := f.treeGetUnlocked(root.CatalogRoot, TreeCatalog, []byte(collection))
	if err != nil || !exists {
		return nil, false, err
	}
	meta, err := decodeCollectionMeta(encodedMeta)
	if err != nil {
		return nil, false, err
	}
	return f.readDocumentUnlocked(meta.PrimaryRoot, documentID)
}

// ReadDocumentVersion reads a document from the stored tree named by its version reference.
func (f *File) ReadDocumentVersion(reference DocumentVersionRef) ([]byte, error) {
	if f == nil || reference.PrimaryRoot < 2 || allZero(reference.DocumentID[:]) {
		return nil, ErrCorrupt
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.file == nil {
		return nil, errors.New("meldbase storage: file is closed")
	}
	value, exists, err := f.readDocumentUnlocked(reference.PrimaryRoot, reference.DocumentID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrCorrupt
	}
	return value, nil
}

// readDocumentUnlocked reads document bytes from a stored tree while the caller holds the file lock.
func (f *File) readDocumentUnlocked(primaryRoot uint64, documentID [16]byte) ([]byte, bool, error) {
	stored, exists, err := f.treeGetUnlocked(primaryRoot, TreePrimary, documentID[:])
	if err != nil || !exists {
		return nil, false, err
	}
	tx := &WriteTxn{file: f, generation: f.meta.Generation, sequence: f.meta.CommitSequence, nextPage: f.nextPage, byID: make(map[uint64][]byte)}
	_, value, err := tx.loadDocumentRecord(stored)
	return value, err == nil, err
}

// readDocument reads document bytes from a tree within a write transaction.
func (tx *WriteTxn) readDocument(primaryRoot uint64, documentID [16]byte) ([]byte, bool, error) {
	tree, err := tx.OpenTree(primaryRoot, TreePrimary)
	if err != nil {
		return nil, false, err
	}
	stored, exists, err := tree.Get(documentID[:])
	if err != nil || !exists {
		return nil, false, err
	}
	_, value, err := tx.loadDocumentRecord(stored)
	return value, err == nil, err
}

// validCollectionName checks the supported collection name length and characters.
func validCollectionName(name string) bool {
	if len(name) == 0 || len(name) > 128 || !((name[0] >= 'A' && name[0] <= 'Z') || (name[0] >= 'a' && name[0] <= 'z')) {
		return false
	}
	for _, value := range []byte(name[1:]) {
		if !((value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '_' || value == '-') {
			return false
		}
	}
	return true
}
