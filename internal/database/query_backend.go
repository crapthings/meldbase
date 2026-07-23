package database

// querySnapshotSource is the storage-backed read boundary. Implementations pin
// one immutable commit sequence; planners must never combine entries or primary
// records from different snapshots.
type querySnapshotSource interface {
	openQuerySnapshot() (queryStorageSnapshot, error)
}

type queryStorageSnapshot interface {
	Sequence() uint64
	CollectionVersion(collection string) (queryStorageCollectionVersion, bool, error)
	GetDocumentRecord(collection string, id DocumentID) (queryStorageDocument, bool, error)
	Indexes(collection string) ([]queryStorageIndex, error)
	OpenIndexIterator(collection, index string, start, end []byte, limit int) (queryStorageIndexIterator, error)
	OpenCollectionIterator(collection string) (queryStorageDocumentIterator, error)
	Close() error
}

// queryStorageCollectionVersion is the immutable collection fence exposed by
// a snapshot. It intentionally contains no page root: callers use it only for
// optimistic validation, while page topology remains storage-private.
type queryStorageCollectionVersion struct {
	ID                   uint32
	UpdatedSequence      uint64
	NextDocumentPosition uint64
}

type queryStorageDocument struct {
	ID       DocumentID
	Position uint64
	Encoded  []byte
	Decoded  Document
}

type queryStorageIndex struct {
	Name   string
	Field  string
	Fields []IndexField
	Unique bool
}

type queryStorageIndexEntry struct {
	Key      []byte
	Position uint64
	ID       DocumentID
}

type queryStorageIndexIterator interface {
	Next() bool
	Entry() queryStorageIndexEntry
	Err() error
	Close() error
}

type queryStorageDocumentIterator interface {
	Next() bool
	Record() queryStorageDocument
	Err() error
	Close() error
}
