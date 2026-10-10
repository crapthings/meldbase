package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"time"
	"unicode/utf8"
)

const (
	// indexMetaHeaderBytes is the byte size of a stored index definition header.
	indexMetaHeaderBytes = 64
	// indexKeyCodec identifies the ascending single-field index key format.
	indexKeyCodec = 2
	// indexKeyCodecV3 identifies the compound and descending index key format.
	indexKeyCodecV3 = 3
	// maxIndexFields bounds the number of ordered fields in one index.
	maxIndexFields = 4
	// maxIndexFieldBytes bounds the encoded byte length of an index field path.
	maxIndexFieldBytes = 1024
	// MaxSecondaryScalarKeyBytes bounds index value bytes before the position and document ID suffix.
	MaxSecondaryScalarKeyBytes = 4096 - 24
)

// indexMetaMagic identifies encoded index catalog records.
var indexMetaMagic = [8]byte{'M', 'E', 'L', 'D', 'I', 'D', 'X', '2'}

var (
	// ErrIndexExists reports an attempt to create an existing index name.
	ErrIndexExists = errors.New("meldbase storage: index name exists")
	// ErrIndexNotFound reports a missing index name.
	ErrIndexNotFound = errors.New("meldbase storage: index name not found")
	// ErrUniqueConflict reports duplicate values in a unique index.
	ErrUniqueConflict = errors.New("meldbase storage: unique index conflict")
	// ErrIndexKeyTooLarge reports an index value exceeding the stored key size limit.
	ErrIndexKeyTooLarge = errors.New("meldbase storage: index key is too large")
)

// IndexMeta records an index definition, stored tree root, and commit versions.
type IndexMeta struct {
	Name            string
	FieldPath       string
	Fields          []IndexField
	Unique          bool
	Root            uint64
	EntryCount      uint64
	CreatedSequence uint64
	UpdatedSequence uint64
	KeyCodecVersion uint16
}

// IndexField describes a field path and its sort direction in an index key.
//
// FieldPath on the surrounding metadata mirrors Fields[0].Path for compatibility.
type IndexField struct {
	Path      string
	Direction int8
}

// IndexEntry links an encoded index value to a document and its insertion position.
type IndexEntry struct {
	Key               []byte
	InsertionPosition uint64
	DocumentID        [16]byte
}

// CreateIndexTransaction describes an index to create or replace with supplied entries.
type CreateIndexTransaction struct {
	TransactionID   [16]byte
	CommittedAt     time.Time
	Collection      string
	Name            string
	FieldPath       string
	Fields          []IndexField
	Unique          bool
	ReplaceExisting bool
	// Entries is consumed and may be reordered/populated by ApplyCreateIndex.
	// Callers must not reuse or mutate it once the call begins.
	Entries []IndexEntry
}

// normalizeIndexFields validates and copies ordered fields, using the legacy field path when needed.
func normalizeIndexFields(fieldPath string, fields []IndexField) ([]IndexField, bool) {
	if len(fields) == 0 {
		if !validIndexFieldPath(fieldPath) {
			return nil, false
		}
		return []IndexField{{Path: fieldPath, Direction: 1}}, true
	}
	if len(fields) > maxIndexFields || (fieldPath != "" && fieldPath != fields[0].Path) {
		return nil, false
	}
	result := append([]IndexField(nil), fields...)
	seen := make(map[string]struct{}, len(result))
	for _, field := range result {
		if !validIndexFieldPath(field.Path) || (field.Direction != 1 && field.Direction != -1) {
			return nil, false
		}
		if _, duplicate := seen[field.Path]; duplicate {
			return nil, false
		}
		seen[field.Path] = struct{}{}
	}
	return result, true
}

// compoundIndexFields reports whether fields require the compound or descending key format.
func compoundIndexFields(fields []IndexField) bool {
	return len(fields) != 1 || fields[0].Direction != 1
}

// validateIndexMetaFeatures checks that the database declares the feature required by an index codec.
func validateIndexMetaFeatures(required uint64, meta IndexMeta) error {
	if meta.KeyCodecVersion == indexKeyCodecV3 && required&RequiredFeatureCompoundIndexes == 0 {
		return ErrCorrupt
	}
	return nil
}

// IndexMutation describes the old and new index values for one document.
type IndexMutation struct {
	Name      string
	BeforeKey []byte
	AfterKey  []byte
}

// loadCollectionIndexes opens the defined indexes for a collection write.
func (tx *WriteTxn) loadCollectionIndexes(state *collectionWriteState) error {
	if tx == nil || state == nil {
		return ErrCorrupt
	}
	indexCatalog, err := tx.OpenTree(state.meta.IndexCatalogRoot, TreeIndexCatalog)
	if err != nil {
		return err
	}
	state.indexCatalog = indexCatalog
	state.indexes = make(map[string]*indexWriteState)
	entries, err := indexCatalog.Scan(nil, nil, 0)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := string(entry.Key)
		meta, err := decodeIndexMeta(name, entry.Value)
		if err != nil {
			return err
		}
		if err := validateIndexMetaFeatures(tx.requiredFeatures, meta); err != nil {
			return err
		}
		tree, err := tx.OpenTree(meta.Root, TreeSecondary)
		if err != nil {
			return err
		}
		state.indexes[name] = &indexWriteState{meta: meta, tree: tree}
	}
	return nil
}

// prepareIndexMutations validates and copies a document change for every defined index.
func prepareIndexMutations(state *collectionWriteState, mutation DocumentMutation, position uint64) ([]pendingIndexMutation, error) {
	if state == nil || position == 0 || len(mutation.Indexes) != len(state.indexes) {
		return nil, ErrCorrupt
	}
	provided := make(map[string]IndexMutation, len(mutation.Indexes))
	for _, item := range mutation.Indexes {
		if !validIndexName(item.Name) {
			return nil, ErrCorrupt
		}
		if len(item.BeforeKey) > MaxSecondaryScalarKeyBytes || len(item.AfterKey) > MaxSecondaryScalarKeyBytes {
			return nil, ErrIndexKeyTooLarge
		}
		if _, duplicate := provided[item.Name]; duplicate {
			return nil, ErrCorrupt
		}
		if mutation.Operation == DocumentInsert && len(item.BeforeKey) != 0 {
			return nil, ErrCorrupt
		}
		if mutation.Operation == DocumentDelete && len(item.AfterKey) != 0 {
			return nil, ErrCorrupt
		}
		provided[item.Name] = item
	}
	result := make([]pendingIndexMutation, 0, len(state.indexes))
	names := make([]string, 0, len(state.indexes))
	for name := range state.indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		item, exists := provided[name]
		if !exists {
			return nil, ErrCorrupt
		}
		result = append(result, pendingIndexMutation{
			state: state.indexes[name], documentID: mutation.DocumentID, position: position,
			beforeKey: append([]byte(nil), item.BeforeKey...), afterKey: append([]byte(nil), item.AfterKey...),
		})
	}
	return result, nil
}

// applyIndexMutations replaces index entries while allowing unique values to swap within a batch.
func applyIndexMutations(pending []pendingIndexMutation) error {
	// Remove every old key first so one atomic batch can swap unique values.
	for _, mutation := range pending {
		if bytes.Equal(mutation.beforeKey, mutation.afterKey) {
			if len(mutation.beforeKey) == 0 {
				continue
			}
			key, err := secondaryKey(mutation.beforeKey, mutation.position, mutation.documentID)
			if err != nil {
				return err
			}
			if _, exists, err := mutation.state.tree.Get(key); err != nil {
				return err
			} else if !exists {
				return ErrCorrupt
			}
			continue
		}
		if len(mutation.beforeKey) == 0 {
			continue
		}
		key, err := secondaryKey(mutation.beforeKey, mutation.position, mutation.documentID)
		if err != nil {
			return err
		}
		removed, err := mutation.state.tree.Delete(key)
		if err != nil || !removed || mutation.state.meta.EntryCount == 0 {
			return ErrCorrupt
		}
		mutation.state.meta.EntryCount--
		mutation.state.changed = true
	}
	for _, mutation := range pending {
		if bytes.Equal(mutation.beforeKey, mutation.afterKey) {
			continue
		}
		if len(mutation.afterKey) == 0 {
			continue
		}
		key, err := secondaryKey(mutation.afterKey, mutation.position, mutation.documentID)
		if err != nil {
			return err
		}
		if _, exists, err := mutation.state.tree.Get(key); err != nil || exists {
			if err != nil {
				return err
			}
			return errors.New("meldbase storage: duplicate secondary entry")
		}
		if mutation.state.meta.Unique {
			conflict, err := uniqueIndexConflict(mutation.state.tree, mutation.afterKey)
			if err != nil {
				return err
			}
			if conflict {
				return ErrUniqueConflict
			}
		}
		if err := mutation.state.tree.Put(key, []byte{0}); err != nil {
			return err
		}
		mutation.state.meta.EntryCount++
		mutation.state.changed = true
	}
	return nil
}

// uniqueIndexConflict checks whether a stored entry already uses an index value.
func uniqueIndexConflict(tree *MutableTree, scalarKey []byte) (bool, error) {
	start := append(append([]byte(nil), scalarKey...), make([]byte, 24)...)
	end := lexicographicPrefixEnd(scalarKey)
	entries, err := tree.Scan(start, end, 1)
	if err != nil || len(entries) == 0 {
		return false, err
	}
	key, _, _, err := secondaryKeyParts(entries[0].Key)
	if err != nil {
		return false, err
	}
	return bytes.Equal(key, scalarKey), nil
}

// lexicographicPrefixEnd returns the exclusive upper bound for a byte prefix, or nil if unbounded.
func lexicographicPrefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] != 0xff {
			end[index]++
			return end[:index+1]
		}
	}
	return nil
}

// validIndexName checks an index name against the collection name rules.
func validIndexName(name string) bool { return validCollectionName(name) }

// validIndexFieldPath checks field path length, UTF-8 validity, and the absence of zero bytes.
func validIndexFieldPath(path string) bool {
	if len(path) == 0 || len(path) > maxIndexFieldBytes || !utf8.ValidString(path) {
		return false
	}
	for _, value := range []byte(path) {
		if value == 0 {
			return false
		}
	}
	return true
}

// secondaryKey appends insertion position and document identity to an encoded index value.
func secondaryKey(key []byte, position uint64, documentID [16]byte) ([]byte, error) {
	if len(key) > MaxSecondaryScalarKeyBytes {
		return nil, ErrIndexKeyTooLarge
	}
	if len(key) == 0 || position == 0 || allZero(documentID[:]) {
		return nil, ErrCorrupt
	}
	result := make([]byte, len(key)+8+len(documentID))
	copy(result, key)
	binary.BigEndian.PutUint64(result[len(key):len(key)+8], position)
	copy(result[len(key)+8:], documentID[:])
	return result, nil
}

// secondaryKeyParts splits a stored index key into its value, position, and document identity.
func secondaryKeyParts(encoded []byte) ([]byte, uint64, [16]byte, error) {
	var id [16]byte
	if len(encoded) <= len(id)+8 || len(encoded) > 4096 {
		return nil, 0, id, ErrCorrupt
	}
	copy(id[:], encoded[len(encoded)-len(id):])
	positionOffset := len(encoded) - len(id) - 8
	position := binary.BigEndian.Uint64(encoded[positionOffset : positionOffset+8])
	if position == 0 || allZero(id[:]) {
		return nil, 0, id, ErrCorrupt
	}
	return encoded[:positionOffset], position, id, nil
}

// normalizeIndexEntries validates and sorts index entries while enforcing requested uniqueness.
func normalizeIndexEntries(entries []IndexEntry, unique bool) ([]IndexEntry, error) {
	seenDocuments := make(map[[16]byte]struct{}, len(entries))
	for _, entry := range entries {
		if len(entry.Key) > MaxSecondaryScalarKeyBytes {
			return nil, ErrIndexKeyTooLarge
		}
		if len(entry.Key) == 0 || allZero(entry.DocumentID[:]) {
			return nil, ErrCorrupt
		}
		if _, duplicate := seenDocuments[entry.DocumentID]; duplicate {
			return nil, errors.New("meldbase storage: document has multiple entries in one index")
		}
		seenDocuments[entry.DocumentID] = struct{}{}
	}
	// The transaction owns Entries after ApplyCreateIndex begins. Sorting the
	// slice in place avoids a second O(n) entry/key copy before the bulk loader.
	sort.Slice(entries, func(left, right int) bool {
		if comparison := bytes.Compare(entries[left].Key, entries[right].Key); comparison != 0 {
			return comparison < 0
		}
		return bytes.Compare(entries[left].DocumentID[:], entries[right].DocumentID[:]) < 0
	})
	for index := 1; index < len(entries); index++ {
		if bytes.Equal(entries[index-1].Key, entries[index].Key) {
			if entries[index-1].DocumentID == entries[index].DocumentID {
				return nil, errors.New("meldbase storage: duplicate index entry")
			}
			if unique {
				return nil, ErrUniqueConflict
			}
		}
	}
	return entries, nil
}

// ApplyCreateIndex atomically publishes an immutable secondary tree, its index
// catalog entry, the owning collection metadata, and one matching CommitBatch.
func (f *File) ApplyCreateIndex(transaction CreateIndexTransaction) (uint64, error) {
	fields, validFields := normalizeIndexFields(transaction.FieldPath, transaction.Fields)
	if f == nil || allZero(transaction.TransactionID[:]) || !validCollectionName(transaction.Collection) ||
		!validIndexName(transaction.Name) || !validFields {
		return 0, ErrCorrupt
	}
	entries, err := normalizeIndexEntries(transaction.Entries, transaction.Unique)
	if err != nil {
		return 0, err
	}
	if transaction.CommittedAt.IsZero() {
		transaction.CommittedAt = time.Now()
	}
	var sequence uint64
	err = f.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		if compoundIndexFields(fields) {
			if err := tx.requireFeature(RequiredFeatureCompoundIndexes); err != nil {
				return DatabaseRoot{}, err
			}
		}
		sequence = tx.Sequence()
		base := tx.BaseRoot()
		catalog, err := tx.OpenTree(base.CatalogRoot, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encodedCollection, exists, err := catalog.Get([]byte(transaction.Collection))
		if err != nil {
			return DatabaseRoot{}, err
		}
		createdCollection := !exists
		var collection CollectionMeta
		if exists {
			collection, err = decodeCollectionMeta(encodedCollection)
			if err != nil {
				return DatabaseRoot{}, err
			}
		} else {
			if base.CollectionCount == math.MaxUint32 {
				return DatabaseRoot{}, ErrCorrupt
			}
			primary, err := tx.OpenTree(0, TreePrimary)
			if err != nil {
				return DatabaseRoot{}, err
			}
			primaryRoot, err := primary.Flush()
			if err != nil {
				return DatabaseRoot{}, err
			}
			order, err := tx.OpenTree(0, TreeOrder)
			if err != nil {
				return DatabaseRoot{}, err
			}
			orderRoot, err := order.Flush()
			if err != nil {
				return DatabaseRoot{}, err
			}
			collection = CollectionMeta{
				ID: uint32(base.CollectionCount + 1), PrimaryRoot: primaryRoot, OrderRoot: orderRoot,
				CreatedSequence: tx.Sequence(), UpdatedSequence: tx.Sequence(),
			}
		}
		indexCatalog, err := tx.OpenTree(collection.IndexCatalogRoot, TreeIndexCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		encodedPrevious, duplicate, err := indexCatalog.Get([]byte(transaction.Name))
		if err != nil {
			return DatabaseRoot{}, err
		}
		if duplicate != transaction.ReplaceExisting {
			if duplicate {
				return DatabaseRoot{}, ErrIndexExists
			}
			return DatabaseRoot{}, ErrIndexNotFound
		}
		primary, err := tx.OpenTree(collection.PrimaryRoot, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		order, err := tx.OpenTree(collection.OrderRoot, TreeOrder)
		if err != nil || primary.root.count != collection.DocumentCount || order.root.count != collection.DocumentCount {
			return DatabaseRoot{}, ErrCorrupt
		}
		builder, err := tx.NewSortedTreeBuilder(TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		for index := range entries {
			entry := &entries[index]
			stored, documentExists, err := primary.getBorrowed(entry.DocumentID[:])
			if err != nil || !documentExists {
				return DatabaseRoot{}, ErrCorrupt
			}
			position, _, err := decodeDocumentRecordDescriptor(stored)
			if err != nil {
				return DatabaseRoot{}, err
			}
			if entry.InsertionPosition != 0 && entry.InsertionPosition != position {
				return DatabaseRoot{}, ErrCorrupt
			}
			owner, exists, err := order.Get(insertionPositionKey(position))
			if err != nil || !exists || !bytes.Equal(owner, entry.DocumentID[:]) {
				return DatabaseRoot{}, ErrCorrupt
			}
			entry.InsertionPosition = position
		}
		// Equal scalar keys are ordered by the durable insertion-position suffix.
		// normalizeIndexEntries has already made scalar-key groups contiguous.
		for start := 0; start < len(entries); {
			end := start + 1
			for end < len(entries) && bytes.Equal(entries[start].Key, entries[end].Key) {
				end++
			}
			sort.Slice(entries[start:end], func(left, right int) bool {
				leftEntry, rightEntry := entries[start+left], entries[start+right]
				if leftEntry.InsertionPosition != rightEntry.InsertionPosition {
					return leftEntry.InsertionPosition < rightEntry.InsertionPosition
				}
				return bytes.Compare(leftEntry.DocumentID[:], rightEntry.DocumentID[:]) < 0
			})
			start = end
		}
		for _, entry := range entries {
			key, err := secondaryKey(entry.Key, entry.InsertionPosition, entry.DocumentID)
			if err != nil {
				return DatabaseRoot{}, err
			}
			if err := builder.Add(key, []byte{0}); err != nil {
				return DatabaseRoot{}, err
			}
		}
		secondaryRoot, err := builder.Finish()
		if err != nil {
			return DatabaseRoot{}, err
		}
		createdSequence := tx.Sequence()
		if transaction.ReplaceExisting {
			previous, err := decodeIndexMeta(transaction.Name, encodedPrevious)
			if err != nil {
				return DatabaseRoot{}, err
			}
			createdSequence = previous.CreatedSequence
		}
		meta := IndexMeta{
			Name: transaction.Name, FieldPath: fields[0].Path, Fields: fields, Unique: transaction.Unique,
			Root: secondaryRoot, EntryCount: uint64(len(entries)), CreatedSequence: createdSequence,
			UpdatedSequence: tx.Sequence(),
		}
		encodedIndex, err := encodeIndexMeta(meta)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := indexCatalog.Put([]byte(transaction.Name), encodedIndex); err != nil {
			return DatabaseRoot{}, err
		}
		collection.IndexCatalogRoot, err = indexCatalog.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		collection.UpdatedSequence = tx.Sequence()
		encodedCollection, err = encodeCollectionMeta(collection)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := catalog.Put([]byte(transaction.Collection), encodedCollection); err != nil {
			return DatabaseRoot{}, err
		}
		catalogRoot, err := catalog.Flush()
		if err != nil {
			return DatabaseRoot{}, err
		}
		changes := make([]CommitChange, 0, 2)
		if createdCollection {
			changes = append(changes, CommitChange{
				CollectionID: collection.ID, CollectionName: transaction.Collection, Operation: CommitCatalog,
				ChangedPaths: []string{"_catalog"}, After: encodedCollection,
			})
		}
		changes = append(changes, CommitChange{
			CollectionID: collection.ID, Operation: CommitCatalog,
			ChangedPaths: []string{"_indexes." + transaction.Name}, Before: encodedPrevious, After: encodedIndex,
		})
		commitLogRoot, oldest, err := tx.AppendCommitRetained(base.CommitLogRoot, base.OldestRetainedSequence, CommitBatch{
			Sequence: tx.Sequence(), TransactionID: transaction.TransactionID, CommittedAt: transaction.CommittedAt,
			CatalogRoot: catalogRoot, Changes: changes,
		})
		if err != nil {
			return DatabaseRoot{}, err
		}
		collectionCount := base.CollectionCount
		if createdCollection {
			collectionCount++
		}
		return DatabaseRoot{
			CommitSequence: tx.Sequence(), CatalogRoot: catalogRoot, CommitLogRoot: commitLogRoot,
			FreeSpaceRoot: base.FreeSpaceRoot, OldestRetainedSequence: oldest,
			CatalogGeneration: base.CatalogGeneration + 1, DocumentCount: base.DocumentCount, CollectionCount: collectionCount,
		}, nil
	})
	return sequence, err
}

// IndexMeta reads one index definition from the pinned database version.
func (snapshot *ReadSnapshot) IndexMeta(collection, name string) (IndexMeta, bool, error) {
	if snapshot == nil || !validCollectionName(collection) || !validIndexName(name) {
		return IndexMeta{}, false, ErrCorrupt
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.file == nil {
		return IndexMeta{}, false, ErrCursorClosed
	}
	snapshot.file.mu.RLock()
	defer snapshot.file.mu.RUnlock()
	if snapshot.file.file == nil {
		return IndexMeta{}, false, errors.New("meldbase storage: file is closed")
	}
	encodedCollection, exists, err := snapshot.file.treeGetUnlocked(snapshot.root.CatalogRoot, TreeCatalog, []byte(collection))
	if err != nil || !exists {
		return IndexMeta{}, false, err
	}
	collectionMeta, err := decodeCollectionMeta(encodedCollection)
	if err != nil {
		return IndexMeta{}, false, err
	}
	encodedIndex, exists, err := snapshot.file.treeGetUnlocked(collectionMeta.IndexCatalogRoot, TreeIndexCatalog, []byte(name))
	if err != nil || !exists {
		return IndexMeta{}, false, err
	}
	meta, err := decodeIndexMeta(name, encodedIndex)
	if err != nil {
		return IndexMeta{}, false, err
	}
	if err := validateIndexMetaFeatures(snapshot.file.meta.RequiredFeatures, meta); err != nil {
		return IndexMeta{}, false, err
	}
	return meta, true, nil
}

// Indexes lists the index definitions for a collection in the pinned database version.
func (snapshot *ReadSnapshot) Indexes(collection string) ([]IndexMeta, error) {
	if snapshot == nil || !validCollectionName(collection) {
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
		return nil, errors.New("meldbase storage: file is closed")
	}
	encodedCollection, exists, err := snapshot.file.treeGetUnlocked(snapshot.root.CatalogRoot, TreeCatalog, []byte(collection))
	if err != nil || !exists {
		return nil, err
	}
	collectionMeta, err := decodeCollectionMeta(encodedCollection)
	if err != nil {
		return nil, err
	}
	tx := &WriteTxn{file: snapshot.file, generation: snapshot.file.meta.Generation, sequence: snapshot.root.CommitSequence, nextPage: snapshot.file.nextPage, byID: make(map[uint64][]byte)}
	tree, err := tx.OpenTree(collectionMeta.IndexCatalogRoot, TreeIndexCatalog)
	if err != nil {
		return nil, err
	}
	pairs, err := tree.Scan(nil, nil, 0)
	if err != nil {
		return nil, err
	}
	result := make([]IndexMeta, len(pairs))
	for index, pair := range pairs {
		result[index], err = decodeIndexMeta(string(pair.Key), pair.Value)
		if err != nil {
			return nil, err
		}
		if err := validateIndexMetaFeatures(snapshot.file.meta.RequiredFeatures, result[index]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ScanIndex returns entries whose encoded scalar key is in [start, end).
//
// Nil
// bounds are open. DocumentID remains an implicit deterministic key suffix.
func (snapshot *ReadSnapshot) ScanIndex(collection, name string, start, end []byte, limit int) ([]IndexEntry, error) {
	iterator, err := snapshot.OpenIndexIterator(collection, name, start, end, limit)
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	entries := make([]IndexEntry, 0)
	for iterator.Next() {
		entry := iterator.Entry()
		entry.Key = append([]byte(nil), entry.Key...)
		entries = append(entries, entry)
	}
	return entries, iterator.Err()
}

// OpenIndexIterator creates a bounded-memory Secondary scan over encoded scalar
// keys in [start, end).
//
// The iterator owns a snapshot pin independently.
func (snapshot *ReadSnapshot) OpenIndexIterator(collection, name string, start, end []byte, limit int) (*IndexIterator, error) {
	if snapshot == nil || !validCollectionName(collection) || !validIndexName(name) ||
		len(start) > MaxSecondaryScalarKeyBytes || len(end) > MaxSecondaryScalarKeyBytes ||
		(len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) > 0) {
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
		return nil, errors.New("meldbase storage: file is closed")
	}
	encodedCollection, collectionExists, err := file.treeGetUnlocked(snapshot.root.CatalogRoot, TreeCatalog, []byte(collection))
	if err != nil {
		return nil, err
	}
	secondaryRoot := uint64(0)
	if collectionExists {
		collectionMeta, err := decodeCollectionMeta(encodedCollection)
		if err != nil {
			return nil, err
		}
		encodedIndex, indexExists, err := file.treeGetUnlocked(collectionMeta.IndexCatalogRoot, TreeIndexCatalog, []byte(name))
		if err != nil {
			return nil, err
		}
		if indexExists {
			meta, err := decodeIndexMeta(name, encodedIndex)
			if err != nil {
				return nil, err
			}
			if err := validateIndexMetaFeatures(file.meta.RequiredFeatures, meta); err != nil {
				return nil, err
			}
			secondaryRoot = meta.Root
		}
	}
	compositeBound := func(key []byte) []byte {
		if len(key) == 0 {
			return nil
		}
		return append(append([]byte(nil), key...), make([]byte, 24)...)
	}
	tree, err := newTreeIterator(file, secondaryRoot, TreeSecondary, compositeBound(start), compositeBound(end), limit)
	if err != nil {
		return nil, err
	}
	iterator := &IndexIterator{file: file, tree: tree}
	if secondaryRoot == 0 {
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

// Next advances to the next index entry within the requested bounds.
func (iterator *IndexIterator) Next() bool {
	if iterator == nil || iterator.closed || iterator.err != nil || iterator.tree == nil || iterator.tree.done {
		return false
	}
	iterator.entry = IndexEntry{}
	iterator.file.mu.RLock()
	if iterator.file.file == nil {
		iterator.err = errors.New("meldbase storage: file is closed")
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
	key, position, id, err := secondaryKeyParts(iterator.tree.Key())
	value := iterator.tree.Value()
	if err != nil || len(value) != 1 || value[0] != 0 {
		iterator.err = ErrCorrupt
	} else {
		iterator.entry = IndexEntry{Key: append([]byte(nil), key...), InsertionPosition: position, DocumentID: id}
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

// Entry returns the current decoded index entry.
func (iterator *IndexIterator) Entry() IndexEntry {
	if iterator == nil {
		return IndexEntry{}
	}
	return iterator.entry
}

// Err returns the index iterator failure.
func (iterator *IndexIterator) Err() error {
	if iterator == nil {
		return ErrCorrupt
	}
	return iterator.err
}

// Close releases the index iterator and its snapshot pin.
func (iterator *IndexIterator) Close() error {
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

// releasePin removes the iterator's reader pin from the file.
func (iterator *IndexIterator) releasePin() {
	if iterator == nil || iterator.file == nil || iterator.pinID == 0 {
		return
	}
	iterator.file.mu.Lock()
	delete(iterator.file.readers, iterator.pinID)
	iterator.file.mu.Unlock()
	iterator.pinID = 0
}
