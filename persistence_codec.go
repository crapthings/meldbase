package meldbase

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	btree "github.com/crapthings/meldbase/internal/index"
	storagefile "github.com/crapthings/meldbase/internal/storage"
)

var snapshotMagic = [8]byte{'M', 'E', 'L', 'D', 'S', 'N', 'P', '1'}
var transactionMagic = [8]byte{'M', 'E', 'L', 'D', 'T', 'X', 'N', '1'}

const (
	checkpointCatalogBlob   uint8 = 1
	checkpointDocumentBlob  uint8 = 2
	checkpointIndexBlob     uint8 = 3
	checkpointIndexNodeBlob uint8 = 4
)

func encodeSnapshotBlobs(collections map[string]*collectionData) ([]storagefile.Blob, error) {
	return encodeCheckpointBlobs(collections, nil)
}

func encodeCheckpointBlobs(collections map[string]*collectionData, history []ChangeBatch) ([]storagefile.Blob, error) {
	catalog := bytes.NewBuffer(nil)
	catalog.Write(snapshotMagic[:])
	writeU16(catalog, 6)
	names := make([]string, 0, len(collections))
	for name := range collections {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) > math.MaxUint32 {
		return nil, ErrCorrupt
	}
	writeU32(catalog, uint32(len(names)))
	blobs := []storagefile.Blob{{Kind: checkpointCatalogBlob}}
	for _, name := range names {
		if !collectionNamePattern.MatchString(name) {
			return nil, ErrInvalidCollection
		}
		if err := writeString16(catalog, name); err != nil {
			return nil, err
		}
		data := collections[name]
		if len(data.documents) != len(data.order) {
			return nil, ErrCorrupt
		}
		writeU64(catalog, uint64(len(data.order)))
		seen := make(map[DocumentID]struct{}, len(data.order))
		for _, id := range data.order {
			document, ok := data.documents[id]
			if !ok {
				return nil, ErrCorrupt
			}
			if _, duplicate := seen[id]; duplicate {
				return nil, ErrCorrupt
			}
			seen[id] = struct{}{}
			encoded, err := encodeDocumentBinary(document)
			if err != nil || len(encoded) > 64<<20 || len(blobs) == math.MaxUint32 {
				return nil, fmt.Errorf("%w: encode document blob", ErrCorrupt)
			}
			writeU32(catalog, uint32(len(blobs)))
			blobs = append(blobs, storagefile.Blob{Kind: checkpointDocumentBlob, Data: encoded})
		}
		indexNames := make([]string, 0, len(data.indexes))
		for indexName := range data.indexes {
			indexNames = append(indexNames, indexName)
		}
		sort.Strings(indexNames)
		if len(indexNames) > math.MaxUint16 {
			return nil, ErrInvalidIndex
		}
		writeU16(catalog, uint16(len(indexNames)))
		for _, indexName := range indexNames {
			state := data.indexes[indexName]
			definition := state.definition
			if err := writeString16(catalog, definition.Name); err != nil {
				return nil, err
			}
			if err := writeString16(catalog, definition.Field); err != nil {
				return nil, err
			}
			catalog.WriteByte(byte(definition.Order))
			if definition.Unique {
				catalog.WriteByte(1)
			} else {
				catalog.WriteByte(0)
			}
			header, nodePages, err := state.tree.EncodeNodePages()
			if err != nil || len(nodePages) == 0 || uint64(len(blobs))+uint64(len(nodePages)) > math.MaxUint32 {
				return nil, fmt.Errorf("%w: encode index node pages", ErrCorrupt)
			}
			writeU16(catalog, header.MaxKeys)
			writeU64(catalog, header.Size)
			writeU32(catalog, header.Root)
			writeU32(catalog, uint32(len(nodePages)))
			for _, nodePage := range nodePages {
				writeU32(catalog, uint32(len(blobs)))
				blobs = append(blobs, storagefile.Blob{Kind: checkpointIndexNodeBlob, Class: storagefile.BlobClassIndex, Data: nodePage})
			}
		}
	}
	if len(history) > 1024 {
		return nil, ErrCorrupt
	}
	writeU32(catalog, uint32(len(history)))
	var previous uint64
	for index, batch := range history {
		if batch.Token == 0 || (index > 0 && batch.Token != previous+1) {
			return nil, ErrCorrupt
		}
		writeU64(catalog, batch.Token)
		previous = batch.Token
	}
	blobs[0].Data = catalog.Bytes()
	return blobs, nil
}

func decodeSnapshotBlobs(blobs []storagefile.Blob) (map[string]*collectionData, error) {
	collections, _, err := decodeCheckpointBlobs(blobs)
	return collections, err
}

func decodeCheckpointBlobs(blobs []storagefile.Blob) (map[string]*collectionData, []ChangeBatch, error) {
	if len(blobs) == 0 {
		return make(map[string]*collectionData), nil, nil
	}
	if blobs[0].Kind != checkpointCatalogBlob || blobs[0].Class != storagefile.BlobClassRecord || len(blobs[0].Data) < 10 || !bytes.Equal(blobs[0].Data[:8], snapshotMagic[:]) {
		return nil, nil, ErrCorrupt
	}
	version := binary.LittleEndian.Uint16(blobs[0].Data[8:10])
	if version != 4 && version != 5 && version != 6 {
		if len(blobs) != 1 {
			return nil, nil, ErrCorrupt
		}
		collections, err := decodeSnapshot(blobs[0].Data)
		return collections, nil, err
	}
	reader := bytes.NewReader(blobs[0].Data[10:])
	collectionCount, err := readU32(reader)
	if err != nil || collectionCount > 65_535 {
		return nil, nil, ErrCorrupt
	}
	collections := make(map[string]*collectionData, collectionCount)
	referenced := make([]bool, len(blobs))
	referenced[0] = true
	for range collectionCount {
		name, err := readString16(reader)
		if err != nil || !collectionNamePattern.MatchString(name) {
			return nil, nil, ErrCorrupt
		}
		if _, duplicate := collections[name]; duplicate {
			return nil, nil, ErrCorrupt
		}
		documentCount, err := readU64(reader)
		if err != nil || documentCount > 10_000_000 {
			return nil, nil, ErrCorrupt
		}
		collection := &collectionData{documents: make(map[DocumentID]Document, int(documentCount)), order: make([]DocumentID, 0, int(documentCount)), indexes: make(map[string]*indexState)}
		for range documentCount {
			blobIndex, err := readU32(reader)
			if err != nil || blobIndex == 0 || int(blobIndex) >= len(blobs) || referenced[blobIndex] || blobs[blobIndex].Kind != checkpointDocumentBlob || blobs[blobIndex].Class != storagefile.BlobClassRecord {
				return nil, nil, ErrCorrupt
			}
			referenced[blobIndex] = true
			document, err := decodeDocumentBinary(blobs[blobIndex].Data)
			if err != nil {
				return nil, nil, ErrCorrupt
			}
			id, ok := document.ID()
			if !ok || id.IsZero() {
				return nil, nil, ErrCorrupt
			}
			if _, duplicate := collection.documents[id]; duplicate {
				return nil, nil, ErrCorrupt
			}
			collection.documents[id] = document
			collection.order = append(collection.order, id)
		}
		indexCount, err := readU16(reader)
		if err != nil {
			return nil, nil, ErrCorrupt
		}
		for range indexCount {
			indexName, err := readString16(reader)
			if err != nil || !indexNamePattern.MatchString(indexName) {
				return nil, nil, ErrCorrupt
			}
			field, err := readString16(reader)
			if err != nil || validatePath(field) != nil {
				return nil, nil, ErrCorrupt
			}
			order, err := reader.ReadByte()
			if err != nil || order != 1 {
				return nil, nil, ErrCorrupt
			}
			unique, err := reader.ReadByte()
			if err != nil || unique > 1 {
				return nil, nil, ErrCorrupt
			}
			if _, duplicate := collection.indexes[indexName]; duplicate {
				return nil, nil, ErrCorrupt
			}
			definition := IndexDefinition{Name: indexName, Field: field, Order: 1, Unique: unique == 1}
			var state *indexState
			if version == 4 {
				blobIndex, readErr := readU32(reader)
				if readErr != nil || blobIndex == 0 || int(blobIndex) >= len(blobs) || referenced[blobIndex] || blobs[blobIndex].Kind != checkpointIndexBlob || blobs[blobIndex].Class != storagefile.BlobClassRecord {
					return nil, nil, ErrCorrupt
				}
				referenced[blobIndex] = true
				state, err = decodePersistedIndex(definition, collection, blobs[blobIndex].Data)
			} else {
				maxKeys, readErr := readU16(reader)
				size, sizeErr := readU64(reader)
				root, rootErr := readU32(reader)
				nodeCount, countErr := readU32(reader)
				if readErr != nil || sizeErr != nil || rootErr != nil || countErr != nil || nodeCount == 0 || nodeCount > 10_000_000 || root >= nodeCount {
					return nil, nil, ErrCorrupt
				}
				nodePages := make([][]byte, nodeCount)
				for nodeIndex := range nodePages {
					blobIndex, readErr := readU32(reader)
					if readErr != nil || blobIndex == 0 || int(blobIndex) >= len(blobs) || referenced[blobIndex] || blobs[blobIndex].Kind != checkpointIndexNodeBlob || blobs[blobIndex].Class != storagefile.BlobClassIndex {
						return nil, nil, ErrCorrupt
					}
					referenced[blobIndex] = true
					nodePages[nodeIndex] = blobs[blobIndex].Data
				}
				tree, decodeErr := btree.DecodeNodePages(btree.TreePageHeader{MaxKeys: maxKeys, Size: size, Root: root}, nodePages)
				if decodeErr != nil {
					return nil, nil, ErrCorrupt
				}
				state, err = validatePersistedIndex(definition, collection, tree)
			}
			if err != nil {
				return nil, nil, ErrCorrupt
			}
			collection.indexes[indexName] = state
		}
		collections[name] = collection
	}
	var history []ChangeBatch
	if version == 6 {
		historyCount, err := readU32(reader)
		if err != nil || historyCount > 1024 {
			return nil, nil, ErrCorrupt
		}
		history = make([]ChangeBatch, 0, historyCount)
		var previous uint64
		for index := uint32(0); index < historyCount; index++ {
			token, tokenErr := readU64(reader)
			if tokenErr != nil || token == 0 || (index > 0 && token != previous+1) {
				return nil, nil, ErrCorrupt
			}
			history = append(history, ChangeBatch{Token: token})
			previous = token
		}
	}
	if reader.Len() != 0 {
		return nil, nil, ErrCorrupt
	}
	for _, used := range referenced {
		if !used {
			return nil, nil, ErrCorrupt
		}
	}
	return collections, history, nil
}

func encodeSnapshot(collections map[string]*collectionData) ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	buffer.Write(snapshotMagic[:])
	writeU16(buffer, 3)
	names := make([]string, 0, len(collections))
	for name := range collections {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) > math.MaxUint32 {
		return nil, ErrCorrupt
	}
	writeU32(buffer, uint32(len(names)))
	for _, name := range names {
		if !collectionNamePattern.MatchString(name) {
			return nil, ErrInvalidCollection
		}
		if err := writeString16(buffer, name); err != nil {
			return nil, err
		}
		data := collections[name]
		writeU64(buffer, uint64(len(data.documents)))
		for _, id := range data.order {
			document, ok := data.documents[id]
			if !ok {
				continue
			}
			encoded, err := encodeDocumentBinary(document)
			if err != nil {
				return nil, err
			}
			if len(encoded) > 64<<20 {
				return nil, fmt.Errorf("document exceeds storage limit")
			}
			writeU32(buffer, uint32(len(encoded)))
			buffer.Write(encoded)
		}
		indexNames := make([]string, 0, len(data.indexes))
		for indexName := range data.indexes {
			indexNames = append(indexNames, indexName)
		}
		sort.Strings(indexNames)
		if len(indexNames) > math.MaxUint16 {
			return nil, ErrInvalidIndex
		}
		writeU16(buffer, uint16(len(indexNames)))
		for _, indexName := range indexNames {
			definition := data.indexes[indexName].definition
			if err := writeString16(buffer, definition.Name); err != nil {
				return nil, err
			}
			if err := writeString16(buffer, definition.Field); err != nil {
				return nil, err
			}
			buffer.WriteByte(byte(definition.Order))
			if definition.Unique {
				buffer.WriteByte(1)
			} else {
				buffer.WriteByte(0)
			}
			encodedTree, err := data.indexes[indexName].tree.MarshalBinary()
			if err != nil || len(encodedTree) > math.MaxUint32 {
				return nil, fmt.Errorf("%w: encode index tree", ErrCorrupt)
			}
			writeU32(buffer, uint32(len(encodedTree)))
			buffer.Write(encodedTree)
		}
	}
	return buffer.Bytes(), nil
}

func decodeSnapshot(data []byte) (map[string]*collectionData, error) {
	collections := make(map[string]*collectionData)
	if len(data) == 0 {
		return collections, nil
	}
	reader := bytes.NewReader(data)
	magic := make([]byte, 8)
	if _, err := io.ReadFull(reader, magic); err != nil || !bytes.Equal(magic, snapshotMagic[:]) {
		return nil, ErrCorrupt
	}
	version, err := readU16(reader)
	if err != nil || (version != 2 && version != 3) {
		return nil, ErrCorrupt
	}
	count, err := readU32(reader)
	if err != nil || count > 65_535 {
		return nil, ErrCorrupt
	}
	for range count {
		name, err := readString16(reader)
		if err != nil || !collectionNamePattern.MatchString(name) {
			return nil, ErrCorrupt
		}
		if _, exists := collections[name]; exists {
			return nil, ErrCorrupt
		}
		documentCount, err := readU64(reader)
		if err != nil || documentCount > 10_000_000 {
			return nil, ErrCorrupt
		}
		collection := &collectionData{documents: make(map[DocumentID]Document, int(documentCount)), order: make([]DocumentID, 0, int(documentCount)), indexes: make(map[string]*indexState)}
		for range documentCount {
			length, err := readU32(reader)
			if err != nil || length > 64<<20 || uint64(length) > uint64(reader.Len()) {
				return nil, ErrCorrupt
			}
			encoded := make([]byte, length)
			if _, err := io.ReadFull(reader, encoded); err != nil {
				return nil, ErrCorrupt
			}
			document, err := decodeDocumentBinary(encoded)
			if err != nil {
				return nil, err
			}
			id, ok := document.ID()
			if !ok || id.IsZero() {
				return nil, ErrCorrupt
			}
			if _, exists := collection.documents[id]; exists {
				return nil, ErrCorrupt
			}
			collection.documents[id] = document
			collection.order = append(collection.order, id)
		}
		indexCount, err := readU16(reader)
		if err != nil {
			return nil, ErrCorrupt
		}
		for range indexCount {
			indexName, err := readString16(reader)
			if err != nil || !indexNamePattern.MatchString(indexName) {
				return nil, ErrCorrupt
			}
			field, err := readString16(reader)
			if err != nil || validatePath(field) != nil {
				return nil, ErrCorrupt
			}
			order, err := reader.ReadByte()
			if err != nil || order != 1 {
				return nil, ErrCorrupt
			}
			unique, err := reader.ReadByte()
			if err != nil || unique > 1 {
				return nil, ErrCorrupt
			}
			if _, exists := collection.indexes[indexName]; exists {
				return nil, ErrCorrupt
			}
			definition := IndexDefinition{Name: indexName, Field: field, Order: 1, Unique: unique == 1}
			var state *indexState
			if version == 2 {
				state, err = buildIndex(definition, collection)
			} else {
				length, readErr := readU32(reader)
				if readErr != nil || length > 512<<20 || uint64(length) > uint64(reader.Len()) {
					return nil, ErrCorrupt
				}
				encodedTree := make([]byte, length)
				if _, readErr := io.ReadFull(reader, encodedTree); readErr != nil {
					return nil, ErrCorrupt
				}
				state, err = decodePersistedIndex(definition, collection, encodedTree)
			}
			if err != nil {
				return nil, ErrCorrupt
			}
			collection.indexes[indexName] = state
		}
		collections[name] = collection
	}
	if reader.Len() != 0 {
		return nil, ErrCorrupt
	}
	return collections, nil
}

func decodePersistedIndex(definition IndexDefinition, collection *collectionData, encoded []byte) (*indexState, error) {
	tree, err := btree.Decode(encoded)
	if err != nil {
		return nil, err
	}
	return validatePersistedIndex(definition, collection, tree)
}

func validatePersistedIndex(definition IndexDefinition, collection *collectionData, tree *btree.Tree) (*indexState, error) {
	pairs := tree.Scan(nil, nil, false)
	expected := 0
	for _, id := range collection.order {
		if document, exists := collection.documents[id]; exists {
			if _, found := lookupInternal(document, definition.Field); found {
				expected++
			}
		}
	}
	if len(pairs) != expected || tree.Len() != expected {
		return nil, ErrCorrupt
	}
	seen := make(map[DocumentID]struct{}, len(pairs))
	var previousKey []byte
	for _, pair := range pairs {
		if len(pair.Value) != len(DocumentID{}) {
			return nil, ErrCorrupt
		}
		var id DocumentID
		copy(id[:], pair.Value)
		if _, duplicate := seen[id]; duplicate {
			return nil, ErrCorrupt
		}
		seen[id] = struct{}{}
		document, exists := collection.documents[id]
		if !exists {
			return nil, ErrCorrupt
		}
		value, found := lookupInternal(document, definition.Field)
		if !found {
			return nil, ErrCorrupt
		}
		key, err := encodeIndexKey(value)
		if err != nil || !bytes.Equal(key, pair.Key) {
			return nil, ErrCorrupt
		}
		if definition.Unique && previousKey != nil && bytes.Equal(previousKey, pair.Key) {
			return nil, ErrCorrupt
		}
		previousKey = pair.Key
	}
	return &indexState{definition: definition, tree: tree}, nil
}

func encodeTransaction(changes []Change) ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	buffer.Write(transactionMagic[:])
	writeU16(buffer, 1)
	if len(changes) == 0 || len(changes) > math.MaxUint32 {
		return nil, errors.New("invalid empty transaction")
	}
	writeU32(buffer, uint32(len(changes)))
	transactionCollection, transactionOperation := changes[0].Collection, changes[0].Operation
	for _, change := range changes {
		if change.Collection != transactionCollection || change.Operation != transactionOperation {
			return nil, errors.New("transaction changes must share collection and operation")
		}
		if !collectionNamePattern.MatchString(change.Collection) {
			return nil, ErrCorrupt
		}
		if err := writeString16(buffer, change.Collection); err != nil {
			return nil, err
		}
		var operation byte
		switch change.Operation {
		case InsertOperation:
			operation = 1
		case UpdateOperation:
			operation = 2
		case DeleteOperation:
			operation = 3
		case CreateIndexOperation:
			operation = 4
		default:
			return nil, ErrCorrupt
		}
		buffer.WriteByte(operation)
		if operation == 4 {
			if change.Index == nil || !indexNamePattern.MatchString(change.Index.Name) || validatePath(change.Index.Field) != nil || change.Index.Order != 1 {
				return nil, ErrCorrupt
			}
			if err := writeString16(buffer, change.Index.Name); err != nil {
				return nil, err
			}
			if err := writeString16(buffer, change.Index.Field); err != nil {
				return nil, err
			}
			buffer.WriteByte(1)
			if change.Index.Unique {
				buffer.WriteByte(1)
			} else {
				buffer.WriteByte(0)
			}
			continue
		}
		if change.DocumentID.IsZero() {
			return nil, ErrCorrupt
		}
		buffer.Write(change.DocumentID[:])
		if operation != 3 {
			if change.After == nil {
				return nil, ErrCorrupt
			}
			documentID, ok := change.After.ID()
			if !ok || documentID != change.DocumentID {
				return nil, ErrCorrupt
			}
			encoded, err := encodeDocumentBinary(*change.After)
			if err != nil {
				return nil, err
			}
			writeU32(buffer, uint32(len(encoded)))
			buffer.Write(encoded)
		}
	}
	return buffer.Bytes(), nil
}

func decodeTransaction(data []byte) ([]Change, error) {
	reader := bytes.NewReader(data)
	magic := make([]byte, 8)
	if _, err := io.ReadFull(reader, magic); err != nil || !bytes.Equal(magic, transactionMagic[:]) {
		return nil, ErrCorrupt
	}
	version, err := readU16(reader)
	if err != nil || version != 1 {
		return nil, ErrCorrupt
	}
	count, err := readU32(reader)
	if err != nil || count == 0 || count > 1_000_000 {
		return nil, ErrCorrupt
	}
	changes := make([]Change, count)
	for i := range changes {
		collection, err := readString16(reader)
		if err != nil || !collectionNamePattern.MatchString(collection) {
			return nil, ErrCorrupt
		}
		operation, err := reader.ReadByte()
		if err != nil || operation < 1 || operation > 4 {
			return nil, ErrCorrupt
		}
		if operation == 4 {
			indexName, err := readString16(reader)
			if err != nil || !indexNamePattern.MatchString(indexName) {
				return nil, ErrCorrupt
			}
			field, err := readString16(reader)
			if err != nil || validatePath(field) != nil {
				return nil, ErrCorrupt
			}
			order, err := reader.ReadByte()
			if err != nil || order != 1 {
				return nil, ErrCorrupt
			}
			unique, err := reader.ReadByte()
			if err != nil || unique > 1 {
				return nil, ErrCorrupt
			}
			definition := IndexDefinition{Name: indexName, Field: field, Order: 1, Unique: unique == 1}
			changes[i] = Change{Collection: collection, Operation: CreateIndexOperation, Index: &definition}
			continue
		}
		var id DocumentID
		if _, err := io.ReadFull(reader, id[:]); err != nil || id.IsZero() {
			return nil, ErrCorrupt
		}
		change := Change{Collection: collection, DocumentID: id}
		if operation == 1 {
			change.Operation = InsertOperation
		} else if operation == 2 {
			change.Operation = UpdateOperation
		} else {
			change.Operation = DeleteOperation
		}
		if operation != 3 {
			length, err := readU32(reader)
			if err != nil || length > 64<<20 || uint64(length) > uint64(reader.Len()) {
				return nil, ErrCorrupt
			}
			encoded := make([]byte, length)
			if _, err := io.ReadFull(reader, encoded); err != nil {
				return nil, ErrCorrupt
			}
			document, err := decodeDocumentBinary(encoded)
			if err != nil {
				return nil, err
			}
			documentID, ok := document.ID()
			if !ok || documentID != id {
				return nil, ErrCorrupt
			}
			change.After = &document
		}
		changes[i] = change
	}
	if reader.Len() != 0 {
		return nil, ErrCorrupt
	}
	return changes, nil
}

func encodeDocumentBinary(document Document) ([]byte, error) {
	if err := document.Validate(); err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(nil)
	if err := encodeObjectBinary(buffer, document, 0); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
func decodeDocumentBinary(data []byte) (Document, error) {
	reader := bytes.NewReader(data)
	document, err := decodeObjectBinary(reader, 0)
	if err != nil || reader.Len() != 0 {
		return nil, ErrCorrupt
	}
	return document, nil
}

func encodeValueBinary(buffer *bytes.Buffer, value Value, depth int) error {
	if depth > 64 {
		return ErrInvalidDocument
	}
	buffer.WriteByte(byte(value.kind))
	switch value.kind {
	case NullKind:
	case BoolKind:
		if value.b {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
	case Int64Kind:
		writeU64(buffer, uint64(value.i))
	case Float64Kind:
		if math.IsNaN(value.f) || math.IsInf(value.f, 0) {
			return ErrInvalidDocument
		}
		writeU64(buffer, math.Float64bits(value.f))
	case StringKind:
		if err := writeBytes32(buffer, []byte(value.s)); err != nil {
			return err
		}
	case BinaryKind:
		if err := writeBytes32(buffer, value.bin); err != nil {
			return err
		}
	case TimeKind:
		writeU64(buffer, uint64(value.t.UnixMilli()))
	case IDKind:
		buffer.Write(value.id[:])
	case ArrayKind:
		if len(value.arr) > math.MaxUint32 {
			return ErrInvalidDocument
		}
		writeU32(buffer, uint32(len(value.arr)))
		for _, item := range value.arr {
			if err := encodeValueBinary(buffer, item, depth+1); err != nil {
				return err
			}
		}
	case ObjectKind:
		return encodeObjectBinary(buffer, value.obj, depth+1)
	default:
		return ErrInvalidDocument
	}
	return nil
}
func encodeObjectBinary(buffer *bytes.Buffer, document Document, depth int) error {
	if depth > 64 || len(document) > math.MaxUint32 {
		return ErrInvalidDocument
	}
	keys := make([]string, 0, len(document))
	for key := range document {
		if err := validField(key); err != nil {
			return err
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	writeU32(buffer, uint32(len(keys)))
	for _, key := range keys {
		if err := writeString16(buffer, key); err != nil {
			return err
		}
		if err := encodeValueBinary(buffer, document[key], depth+1); err != nil {
			return err
		}
	}
	return nil
}

func decodeValueBinary(reader *bytes.Reader, depth int) (Value, error) {
	if depth > 64 {
		return Value{}, ErrCorrupt
	}
	kind, err := reader.ReadByte()
	if err != nil || Kind(kind) > IDKind {
		return Value{}, ErrCorrupt
	}
	switch Kind(kind) {
	case NullKind:
		return Null(), nil
	case BoolKind:
		value, err := reader.ReadByte()
		if err != nil || value > 1 {
			return Value{}, ErrCorrupt
		}
		return Bool(value == 1), nil
	case Int64Kind:
		value, err := readU64(reader)
		return Int(int64(value)), corruptIf(err)
	case Float64Kind:
		bits, err := readU64(reader)
		value := math.Float64frombits(bits)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return Value{}, ErrCorrupt
		}
		return Float(value), nil
	case StringKind:
		value, err := readBytes32(reader, 64<<20)
		if err != nil {
			return Value{}, err
		}
		return String(string(value)), nil
	case BinaryKind:
		value, err := readBytes32(reader, 64<<20)
		if err != nil {
			return Value{}, err
		}
		return Binary(value), nil
	case TimeKind:
		value, err := readU64(reader)
		if err != nil {
			return Value{}, ErrCorrupt
		}
		return Time(time.UnixMilli(int64(value))), nil
	case IDKind:
		var id DocumentID
		if _, err := io.ReadFull(reader, id[:]); err != nil || id.IsZero() {
			return Value{}, ErrCorrupt
		}
		return ID(id), nil
	case ArrayKind:
		count, err := readU32(reader)
		if err != nil || count > 10_000_000 {
			return Value{}, ErrCorrupt
		}
		values := make([]Value, count)
		for i := range values {
			values[i], err = decodeValueBinary(reader, depth+1)
			if err != nil {
				return Value{}, err
			}
		}
		return Array(values...), nil
	case ObjectKind:
		document, err := decodeObjectBinary(reader, depth+1)
		if err != nil {
			return Value{}, err
		}
		return Object(document), nil
	}
	return Value{}, ErrCorrupt
}
func decodeObjectBinary(reader *bytes.Reader, depth int) (Document, error) {
	if depth > 64 {
		return nil, ErrCorrupt
	}
	count, err := readU32(reader)
	if err != nil || count > 1_000_000 {
		return nil, ErrCorrupt
	}
	document := make(Document, count)
	for range count {
		key, err := readString16(reader)
		if err != nil || validField(key) != nil {
			return nil, ErrCorrupt
		}
		if _, exists := document[key]; exists {
			return nil, ErrCorrupt
		}
		value, err := decodeValueBinary(reader, depth+1)
		if err != nil {
			return nil, err
		}
		document[key] = value
	}
	return document, nil
}

func writeU16(w io.Writer, value uint16) {
	var data [2]byte
	binary.LittleEndian.PutUint16(data[:], value)
	_, _ = w.Write(data[:])
}
func writeU32(w io.Writer, value uint32) {
	var data [4]byte
	binary.LittleEndian.PutUint32(data[:], value)
	_, _ = w.Write(data[:])
}
func writeU64(w io.Writer, value uint64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], value)
	_, _ = w.Write(data[:])
}
func readU16(r io.Reader) (uint16, error) {
	var data [2]byte
	_, err := io.ReadFull(r, data[:])
	return binary.LittleEndian.Uint16(data[:]), err
}
func readU32(r io.Reader) (uint32, error) {
	var data [4]byte
	_, err := io.ReadFull(r, data[:])
	return binary.LittleEndian.Uint32(data[:]), err
}
func readU64(r io.Reader) (uint64, error) {
	var data [8]byte
	_, err := io.ReadFull(r, data[:])
	return binary.LittleEndian.Uint64(data[:]), err
}
func writeString16(w io.Writer, value string) error {
	if len(value) > math.MaxUint16 {
		return ErrCorrupt
	}
	writeU16(w, uint16(len(value)))
	_, err := io.WriteString(w, value)
	return err
}
func readString16(r io.Reader) (string, error) {
	length, err := readU16(r)
	if err != nil {
		return "", ErrCorrupt
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", ErrCorrupt
	}
	return string(data), nil
}
func writeBytes32(w io.Writer, value []byte) error {
	if len(value) > math.MaxUint32 {
		return ErrInvalidDocument
	}
	writeU32(w, uint32(len(value)))
	_, err := w.Write(value)
	return err
}
func readBytes32(r *bytes.Reader, max uint32) ([]byte, error) {
	length, err := readU32(r)
	if err != nil || length > max || uint64(length) > uint64(r.Len()) {
		return nil, ErrCorrupt
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, ErrCorrupt
	}
	return data, nil
}
func corruptIf(err error) error {
	if err != nil {
		return ErrCorrupt
	}
	return nil
}
