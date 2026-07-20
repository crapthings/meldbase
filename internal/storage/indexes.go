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
	indexMetaHeaderBytes       = 64
	indexKeyCodecV2            = 2
	indexKeyCodecV3            = 3
	maxIndexFields             = 4
	maxIndexFieldBytes         = 1024
	MaxSecondaryScalarKeyBytes = 4096 - 24
)

var indexMetaMagic = [8]byte{'M', 'E', 'L', 'D', 'I', 'D', 'X', '2'}

var (
	ErrIndexExists      = errors.New("meldbase storage v2: index name exists")
	ErrUniqueConflict   = errors.New("meldbase storage v2: unique index conflict")
	ErrIndexKeyTooLarge = errors.New("meldbase storage v2: index key is too large")
)

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

// IndexField is one ordered component of a Secondary key. FieldPath on the
// surrounding metadata remains a compatibility mirror of Fields[0].Path.
type IndexField struct {
	Path      string
	Direction int8
}

type IndexEntry struct {
	Key               []byte
	InsertionPosition uint64
	DocumentID        [16]byte
}

type CreateIndexTransaction struct {
	TransactionID [16]byte
	CommittedAt   time.Time
	Collection    string
	Name          string
	FieldPath     string
	Fields        []IndexField
	Unique        bool
	// Entries is consumed and may be reordered/populated by ApplyCreateIndex.
	// Callers must not reuse or mutate it once the call begins.
	Entries []IndexEntry
}

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

func compoundIndexFields(fields []IndexField) bool {
	return len(fields) != 1 || fields[0].Direction != 1
}

func validateIndexMetaFeatures(required uint64, meta IndexMeta) error {
	if meta.KeyCodecVersion == indexKeyCodecV3 && required&RequiredFeatureCompoundIndexes == 0 {
		return ErrCorrupt
	}
	return nil
}

type IndexMutation struct {
	Name      string
	BeforeKey []byte
	AfterKey  []byte
}

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

func applyIndexMutations(pending []pendingIndexMutation) error {
	// Remove every old key first so one atomic batch can swap unique values.
	for _, mutation := range pending {
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
			return errors.New("meldbase storage v2: duplicate secondary entry")
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

func validIndexName(name string) bool { return validCollectionName(name) }

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

func encodeIndexMeta(meta IndexMeta) ([]byte, error) {
	fields, valid := normalizeIndexFields(meta.FieldPath, meta.Fields)
	if !valid || !validIndexName(meta.Name) || meta.Root < 2 || meta.CreatedSequence == 0 || meta.UpdatedSequence < meta.CreatedSequence {
		return nil, ErrCorrupt
	}
	codec := meta.KeyCodecVersion
	if codec == 0 {
		if compoundIndexFields(fields) {
			codec = indexKeyCodecV3
		} else {
			codec = indexKeyCodecV2
		}
	}
	if (codec == indexKeyCodecV2 && compoundIndexFields(fields)) || (codec != indexKeyCodecV2 && codec != indexKeyCodecV3) {
		return nil, ErrCorrupt
	}
	payload := []byte(nil)
	if codec == indexKeyCodecV2 {
		payload = append(payload, fields[0].Path...)
	} else {
		for _, field := range fields {
			payload = append(payload, byte(field.Direction), 0, 0)
			binary.LittleEndian.PutUint16(payload[len(payload)-2:], uint16(len(field.Path)))
			payload = append(payload, field.Path...)
		}
	}
	if len(payload) == 0 || len(payload) > math.MaxUint16 {
		return nil, ErrCorrupt
	}
	encoded := make([]byte, indexMetaHeaderBytes+len(payload))
	copy(encoded[:8], indexMetaMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(encoded[10:12], indexMetaHeaderBytes)
	if meta.Unique {
		encoded[12] = 1
	}
	binary.LittleEndian.PutUint64(encoded[16:24], meta.Root)
	binary.LittleEndian.PutUint64(encoded[24:32], meta.EntryCount)
	binary.LittleEndian.PutUint64(encoded[32:40], meta.CreatedSequence)
	binary.LittleEndian.PutUint64(encoded[40:48], meta.UpdatedSequence)
	binary.LittleEndian.PutUint16(encoded[48:50], uint16(len(payload)))
	binary.LittleEndian.PutUint16(encoded[50:52], codec)
	if codec == indexKeyCodecV3 {
		binary.LittleEndian.PutUint16(encoded[52:54], uint16(len(fields)))
	}
	copy(encoded[indexMetaHeaderBytes:], payload)
	return encoded, nil
}

func decodeIndexMeta(name string, encoded []byte) (IndexMeta, error) {
	if !validIndexName(name) || len(encoded) < indexMetaHeaderBytes || string(encoded[:8]) != string(indexMetaMagic[:]) ||
		binary.LittleEndian.Uint16(encoded[8:10]) != FormatVersion || binary.LittleEndian.Uint16(encoded[10:12]) != indexMetaHeaderBytes ||
		encoded[12] > 1 || !allZero(encoded[13:16]) {
		return IndexMeta{}, ErrCorrupt
	}
	payloadLength := int(binary.LittleEndian.Uint16(encoded[48:50]))
	codec := binary.LittleEndian.Uint16(encoded[50:52])
	if payloadLength == 0 || len(encoded) != indexMetaHeaderBytes+payloadLength {
		return IndexMeta{}, ErrCorrupt
	}
	meta := IndexMeta{
		Name: name, Unique: encoded[12] == 1,
		Root: binary.LittleEndian.Uint64(encoded[16:24]), EntryCount: binary.LittleEndian.Uint64(encoded[24:32]),
		CreatedSequence: binary.LittleEndian.Uint64(encoded[32:40]), UpdatedSequence: binary.LittleEndian.Uint64(encoded[40:48]),
		KeyCodecVersion: codec,
	}
	payload := encoded[indexMetaHeaderBytes:]
	switch codec {
	case indexKeyCodecV2:
		if !allZero(encoded[52:64]) || payloadLength > maxIndexFieldBytes {
			return IndexMeta{}, ErrCorrupt
		}
		meta.FieldPath = string(payload)
		meta.Fields = []IndexField{{Path: meta.FieldPath, Direction: 1}}
	case indexKeyCodecV3:
		fieldCount := int(binary.LittleEndian.Uint16(encoded[52:54]))
		if fieldCount == 0 || fieldCount > maxIndexFields || !allZero(encoded[54:64]) {
			return IndexMeta{}, ErrCorrupt
		}
		meta.Fields = make([]IndexField, 0, fieldCount)
		for offset := 0; offset < len(payload); {
			if len(payload)-offset < 3 {
				return IndexMeta{}, ErrCorrupt
			}
			direction := int8(payload[offset])
			length := int(binary.LittleEndian.Uint16(payload[offset+1 : offset+3]))
			offset += 3
			if length == 0 || length > maxIndexFieldBytes || offset+length > len(payload) {
				return IndexMeta{}, ErrCorrupt
			}
			meta.Fields = append(meta.Fields, IndexField{Path: string(payload[offset : offset+length]), Direction: direction})
			offset += length
		}
		if len(meta.Fields) != fieldCount {
			return IndexMeta{}, ErrCorrupt
		}
		meta.FieldPath = meta.Fields[0].Path
	default:
		return IndexMeta{}, ErrCorrupt
	}
	fields, valid := normalizeIndexFields(meta.FieldPath, meta.Fields)
	if !valid || (codec == indexKeyCodecV2 && compoundIndexFields(fields)) || meta.Root < 2 || meta.CreatedSequence == 0 ||
		meta.UpdatedSequence < meta.CreatedSequence {
		return IndexMeta{}, ErrCorrupt
	}
	meta.Fields = fields
	return meta, nil
}

// DecodeIndexMeta decodes an immutable index catalog value carried in a
// Commit Log catalog event. It is intentionally a narrow export: callers get
// semantic index metadata, never an invitation to depend on the physical
// catalog layout.
func DecodeIndexMeta(name string, encoded []byte) (IndexMeta, error) {
	return decodeIndexMeta(name, encoded)
}

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
			return nil, errors.New("meldbase storage v2: document has multiple entries in one index")
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
				return nil, errors.New("meldbase storage v2: duplicate index entry")
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
		if _, duplicate, err := indexCatalog.Get([]byte(transaction.Name)); err != nil || duplicate {
			if err != nil {
				return DatabaseRoot{}, err
			}
			return DatabaseRoot{}, ErrIndexExists
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
		meta := IndexMeta{
			Name: transaction.Name, FieldPath: fields[0].Path, Fields: fields, Unique: transaction.Unique,
			Root: secondaryRoot, EntryCount: uint64(len(entries)), CreatedSequence: tx.Sequence(),
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
			ChangedPaths: []string{"_indexes." + transaction.Name}, After: encodedIndex,
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
		return IndexMeta{}, false, errors.New("meldbase storage v2: file is closed")
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
		return nil, errors.New("meldbase storage v2: file is closed")
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

// ScanIndex returns entries whose encoded scalar key is in [start, end). Nil
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
// keys in [start, end). The iterator owns a snapshot pin independently.
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
		return nil, errors.New("meldbase storage v2: file is closed")
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

func (iterator *IndexIterator) Next() bool {
	if iterator == nil || iterator.closed || iterator.err != nil || iterator.tree == nil || iterator.tree.done {
		return false
	}
	iterator.entry = IndexEntry{}
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

func (iterator *IndexIterator) Entry() IndexEntry {
	if iterator == nil {
		return IndexEntry{}
	}
	return iterator.entry
}

func (iterator *IndexIterator) Err() error {
	if iterator == nil {
		return ErrCorrupt
	}
	return iterator.err
}

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

func (iterator *IndexIterator) releasePin() {
	if iterator == nil || iterator.file == nil || iterator.pinID == 0 {
		return
	}
	iterator.file.mu.Lock()
	delete(iterator.file.readers, iterator.pinID)
	iterator.file.mu.Unlock()
	iterator.pinID = 0
}
