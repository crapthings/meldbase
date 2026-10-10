package database

import (
	"bytes"
	"container/list"
	"sync"
	"sync/atomic"
)

const (
	// defaultDocumentCacheEntries sets the default decoded-document entry limit.
	defaultDocumentCacheEntries = 4096
	// defaultDocumentCacheBytes sets the default estimated decoded-document byte limit.
	defaultDocumentCacheBytes = 16 * 1024 * 1024
)

// documentCacheKey identifies a document by collection and ID.
type documentCacheKey struct {
	collection string
	id         DocumentID
}

// documentCacheEntry retains a decoded document, its source bytes, and its estimated cost.
type documentCacheEntry struct {
	key      documentCacheKey
	encoded  []byte
	document Document
	cost     uint64
}

// documentCache retains recently used decoded documents within fixed limits.
//
// Every lookup compares immutable record bytes to prevent stale reads without
// requiring invalidation on the write path.
type documentCache struct {
	mu         sync.Mutex
	entries    map[documentCacheKey]*list.Element
	lru        list.List
	bytes      uint64
	maxEntries uint64
	maxBytes   uint64
	hits       atomic.Uint64
	misses     atomic.Uint64
	evictions  atomic.Uint64
}

// newDocumentCache creates a decoded-document cache bounded by entry count and estimated bytes.
func newDocumentCache(maxEntries, maxBytes uint64) *documentCache {
	return &documentCache{
		entries: make(map[documentCacheKey]*list.Element), maxEntries: maxEntries, maxBytes: maxBytes,
	}
}

// decode reuses a matching cached document or decodes and caches the supplied record.
func (cache *documentCache) decode(collection string, id DocumentID, encoded []byte) (Document, error) {
	if cache == nil || cache.maxEntries == 0 || cache.maxBytes == 0 {
		return decodeStoredDocument(encoded)
	}
	key := documentCacheKey{collection: collection, id: id}
	cache.mu.Lock()
	if element := cache.entries[key]; element != nil {
		entry := element.Value.(*documentCacheEntry)
		if bytes.Equal(entry.encoded, encoded) {
			cache.lru.MoveToFront(element)
			document := entry.document
			cache.mu.Unlock()
			cache.hits.Add(1)
			return document, nil
		}
		cache.removeElementLocked(element, false)
	}
	cache.mu.Unlock()
	cache.misses.Add(1)

	document, err := decodeStoredDocument(encoded)
	if err != nil {
		return nil, err
	}
	actualID, exists := document.ID()
	if !exists || actualID != id {
		return nil, ErrCorrupt
	}
	cost := estimateCachedDocumentBytes(collection, encoded, document)
	if cost > cache.maxBytes {
		return document, nil
	}
	entry := &documentCacheEntry{
		key: key, encoded: append([]byte(nil), encoded...), document: document, cost: cost,
	}
	cache.mu.Lock()
	if element := cache.entries[key]; element != nil {
		existing := element.Value.(*documentCacheEntry)
		if bytes.Equal(existing.encoded, encoded) {
			cache.lru.MoveToFront(element)
			document = existing.document
			cache.mu.Unlock()
			return document, nil
		}
		cache.removeElementLocked(element, false)
	}
	element := cache.lru.PushFront(entry)
	cache.entries[key] = element
	cache.bytes += cost
	for uint64(len(cache.entries)) > cache.maxEntries || cache.bytes > cache.maxBytes {
		cache.removeElementLocked(cache.lru.Back(), true)
	}
	cache.mu.Unlock()
	return document, nil
}

// remove drops a document from the cache by collection and ID.
func (cache *documentCache) remove(collection string, id DocumentID) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	if element := cache.entries[documentCacheKey{collection: collection, id: id}]; element != nil {
		cache.removeElementLocked(element, false)
	}
	cache.mu.Unlock()
}

// removeElementLocked drops an entry and its byte charge while the caller holds the cache lock.
func (cache *documentCache) removeElementLocked(element *list.Element, eviction bool) {
	if element == nil {
		return
	}
	entry := element.Value.(*documentCacheEntry)
	delete(cache.entries, entry.key)
	cache.lru.Remove(element)
	if entry.cost > cache.bytes {
		cache.bytes = 0
	} else {
		cache.bytes -= entry.cost
	}
	if eviction {
		cache.evictions.Add(1)
	}
}

// stats samples document-cache occupancy and access counters.
func (cache *documentCache) stats() DocumentCacheStats {
	if cache == nil {
		return DocumentCacheStats{}
	}
	cache.mu.Lock()
	stats := DocumentCacheStats{
		CapacityEntries: cache.maxEntries, CapacityBytes: cache.maxBytes,
		Entries: uint64(len(cache.entries)), Bytes: cache.bytes,
		Hits: cache.hits.Load(), Misses: cache.misses.Load(), Evictions: cache.evictions.Load(),
	}
	cache.mu.Unlock()
	return stats
}

// estimateCachedDocumentBytes estimates the memory retained by a cached document and its source bytes.
func estimateCachedDocumentBytes(collection string, encoded []byte, document Document) uint64 {
	// Include encoded bytes used for exact version comparison plus a conservative
	// estimate of Go map/value overhead. Entry count provides a second hard bound
	// when many tiny fields make allocator overhead dominate encoded size.
	return uint64(len(collection)+len(encoded)) + estimateDocumentHeapBytes(document) + 192
}

// estimateDocumentHeapBytes estimates memory used by a document and its nested values.
func estimateDocumentHeapBytes(document Document) uint64 {
	result := uint64(64 + len(document)*48)
	for key, value := range document {
		result += uint64(len(key)) + estimateValueHeapBytes(value)
	}
	return result
}

// estimateValueHeapBytes estimates memory used by a value and its nested contents.
func estimateValueHeapBytes(value Value) uint64 {
	switch value.kind {
	case StringKind:
		return uint64(len(value.s)) + 16
	case BinaryKind:
		return uint64(len(value.bin)) + 24
	case ArrayKind:
		result := uint64(24 + len(value.arr)*64)
		for _, item := range value.arr {
			result += estimateValueHeapBytes(item)
		}
		return result
	case ObjectKind:
		return estimateDocumentHeapBytes(value.obj)
	default:
		return 32
	}
}
