package database

import "github.com/crapthings/meldbase/internal/storage"

// openQuerySnapshot pins a stored database version for query reads.
func (store *durableStore) openQuerySnapshot() (queryStorageSnapshot, error) {
	if store == nil || store.file == nil {
		return nil, ErrClosed
	}
	snapshot, err := store.file.OpenSnapshot()
	if err != nil {
		return nil, mapStorageError(err)
	}
	if store.testQuerySnapshotHook != nil {
		store.testQuerySnapshotHook()
	}
	return &durableQueryStorageSnapshot{snapshot: snapshot, documents: store.documents}, nil
}

// durableQueryStorageSnapshot adapts a pinned storage snapshot for database queries.
type durableQueryStorageSnapshot struct {
	snapshot  *storage.ReadSnapshot
	documents *documentCache
}

// Sequence returns the commit number of the pinned snapshot.
func (snapshot *durableQueryStorageSnapshot) Sequence() uint64 {
	if snapshot == nil || snapshot.snapshot == nil {
		return 0
	}
	return snapshot.snapshot.Sequence()
}

// CollectionVersion returns the identity and change version of a stored collection.
func (snapshot *durableQueryStorageSnapshot) CollectionVersion(collection string) (queryStorageCollectionVersion, bool, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return queryStorageCollectionVersion{}, false, ErrClosed
	}
	meta, exists, err := snapshot.snapshot.CollectionMeta(collection)
	if err != nil {
		return queryStorageCollectionVersion{}, false, mapStorageError(err)
	}
	if !exists {
		return queryStorageCollectionVersion{}, false, nil
	}
	if meta.ID == 0 || meta.UpdatedSequence == 0 {
		return queryStorageCollectionVersion{}, false, ErrCorrupt
	}
	return queryStorageCollectionVersion{ID: meta.ID, UpdatedSequence: meta.UpdatedSequence, NextDocumentPosition: meta.NextDocumentPosition}, true, nil
}

// GetDocumentRecord reads and decodes a document from the pinned snapshot.
func (snapshot *durableQueryStorageSnapshot) GetDocumentRecord(collection string, id DocumentID) (queryStorageDocument, bool, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return queryStorageDocument{}, false, ErrClosed
	}
	record, exists, err := snapshot.snapshot.GetDocumentRecord(collection, [16]byte(id))
	if err != nil {
		return queryStorageDocument{}, false, mapStorageError(err)
	}
	if !exists {
		snapshot.documents.remove(collection, id)
		return queryStorageDocument{}, false, nil
	}
	document, err := snapshot.documents.decode(collection, id, record.Document)
	if err != nil {
		return queryStorageDocument{}, false, err
	}
	return queryStorageDocument{
		ID: DocumentID(record.DocumentID), Position: record.InsertionPosition,
		Encoded: record.Document, Decoded: document,
	}, true, nil
}

// Indexes returns the query index definitions for a stored collection.
func (snapshot *durableQueryStorageSnapshot) Indexes(collection string) ([]queryStorageIndex, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil, ErrClosed
	}
	metas, err := snapshot.snapshot.Indexes(collection)
	if err != nil {
		return nil, mapStorageError(err)
	}
	result := make([]queryStorageIndex, len(metas))
	for index, meta := range metas {
		fields, err := publicIndexFields(meta.FieldPath, meta.Fields)
		if err != nil {
			return nil, err
		}
		result[index] = queryStorageIndex{Name: meta.Name, Field: fields[0].Field, Fields: fields, Unique: meta.Unique}
	}
	return result, nil
}

// OpenIndexIterator opens a bounded scan of stored index entries.
func (snapshot *durableQueryStorageSnapshot) OpenIndexIterator(collection, index string, start, end []byte, limit int) (queryStorageIndexIterator, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil, ErrClosed
	}
	iterator, err := snapshot.snapshot.OpenIndexIterator(collection, index, start, end, limit)
	if err != nil {
		return nil, mapStorageError(err)
	}
	return &durableQueryStorageIndexIterator{iterator: iterator}, nil
}

// OpenCollectionIterator opens a document scan in insertion order.
func (snapshot *durableQueryStorageSnapshot) OpenCollectionIterator(collection string) (queryStorageDocumentIterator, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil, ErrClosed
	}
	iterator, err := snapshot.snapshot.OpenInsertionOrderIterator(collection, nil, nil, 0)
	if err != nil {
		return nil, mapStorageError(err)
	}
	return &durableQueryStorageDocumentIterator{iterator: iterator, collection: collection, documents: snapshot.documents}, nil
}

// Close releases the pinned storage snapshot.
func (snapshot *durableQueryStorageSnapshot) Close() error {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil
	}
	err := snapshot.snapshot.Close()
	snapshot.snapshot = nil
	return mapStorageError(err)
}

// durableQueryStorageIndexIterator adapts a stored index scan for database queries.
type durableQueryStorageIndexIterator struct {
	iterator *storage.IndexIterator
}

// Next advances to the next stored index entry.
func (iterator *durableQueryStorageIndexIterator) Next() bool {
	return iterator != nil && iterator.iterator != nil && iterator.iterator.Next()
}

// Entry returns the current index key and document location.
func (iterator *durableQueryStorageIndexIterator) Entry() queryStorageIndexEntry {
	if iterator == nil || iterator.iterator == nil {
		return queryStorageIndexEntry{}
	}
	entry := iterator.iterator.Entry()
	return queryStorageIndexEntry{Key: entry.Key, Position: entry.InsertionPosition, ID: DocumentID(entry.DocumentID)}
}

// Err returns the index scan error in database error form.
func (iterator *durableQueryStorageIndexIterator) Err() error {
	if iterator == nil || iterator.iterator == nil {
		return ErrCorrupt
	}
	return mapStorageError(iterator.iterator.Err())
}

// Close releases the stored index scan.
func (iterator *durableQueryStorageIndexIterator) Close() error {
	if iterator == nil || iterator.iterator == nil {
		return nil
	}
	err := iterator.iterator.Close()
	iterator.iterator = nil
	return mapStorageError(err)
}

// durableQueryStorageDocumentIterator decodes documents during a stored collection scan.
type durableQueryStorageDocumentIterator struct {
	iterator   *storage.DocumentIterator
	collection string
	documents  *documentCache
	err        error
}

// Next advances the document scan unless decoding has failed.
func (iterator *durableQueryStorageDocumentIterator) Next() bool {
	return iterator != nil && iterator.iterator != nil && iterator.err == nil && iterator.iterator.Next()
}

// Record decodes the current document and saves any decoding error.
func (iterator *durableQueryStorageDocumentIterator) Record() queryStorageDocument {
	if iterator == nil || iterator.iterator == nil {
		return queryStorageDocument{}
	}
	record := iterator.iterator.Record()
	id := DocumentID(record.DocumentID)
	document, err := iterator.documents.decode(iterator.collection, id, record.Document)
	if err != nil {
		iterator.err = err
		return queryStorageDocument{}
	}
	return queryStorageDocument{ID: id, Position: record.InsertionPosition, Encoded: record.Document, Decoded: document}
}

// Err returns the first decoding error or the underlying document scan error.
func (iterator *durableQueryStorageDocumentIterator) Err() error {
	if iterator == nil || iterator.iterator == nil {
		return ErrCorrupt
	}
	if iterator.err != nil {
		return iterator.err
	}
	return mapStorageError(iterator.iterator.Err())
}

// Close releases the stored document scan.
func (iterator *durableQueryStorageDocumentIterator) Close() error {
	if iterator == nil || iterator.iterator == nil {
		return nil
	}
	err := iterator.iterator.Close()
	iterator.iterator = nil
	return mapStorageError(err)
}
