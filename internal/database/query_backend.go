package database

// querySnapshotSource opens an immutable storage view for query execution.
//
// Planners must read document records and index entries from the same snapshot.
type querySnapshotSource interface {
	openQuerySnapshot() (queryStorageSnapshot, error)
}

// queryStorageSnapshot reads collection records and indexes from one immutable commit.
type queryStorageSnapshot interface {
	Sequence() uint64
	CollectionVersion(collection string) (queryStorageCollectionVersion, bool, error)
	GetDocumentRecord(collection string, id DocumentID) (queryStorageDocument, bool, error)
	Indexes(collection string) ([]queryStorageIndex, error)
	OpenIndexIterator(collection, index string, start, end []byte, limit int) (queryStorageIndexIterator, error)
	OpenCollectionIterator(collection string) (queryStorageDocumentIterator, error)
	Close() error
}

// queryStorageCollectionVersion identifies a collection version for optimistic validation.
//
// It exposes no page roots or other storage topology.
type queryStorageCollectionVersion struct {
	ID                   uint32
	UpdatedSequence      uint64
	NextDocumentPosition uint64
}

// queryStorageDocument carries a stored document and its insertion position.
type queryStorageDocument struct {
	ID       DocumentID
	Position uint64
	Encoded  []byte
	Decoded  Document
}

// queryStorageIndex describes an index available in a storage snapshot.
type queryStorageIndex struct {
	Name   string
	Field  string
	Fields []IndexField
	Unique bool
}

// queryStorageIndexEntry links an encoded index key to a document and its insertion position.
type queryStorageIndexEntry struct {
	Key      []byte
	Position uint64
	ID       DocumentID
}

// queryStorageIndexIterator reads index entries from a pinned storage view.
type queryStorageIndexIterator interface {
	Next() bool
	Entry() queryStorageIndexEntry
	Err() error
	Close() error
}

// queryStorageDocumentIterator reads document records from a pinned storage view.
type queryStorageDocumentIterator interface {
	Next() bool
	Record() queryStorageDocument
	Err() error
	Close() error
}
