package meldbase

import (
	"bytes"
	"container/list"
	"sync"
	"sync/atomic"
)

const (
	defaultV2DocumentCacheEntries = 4096
	defaultV2DocumentCacheBytes   = 16 * 1024 * 1024
)

type v2DocumentCacheKey struct {
	collection string
	id         DocumentID
}

type v2DocumentCacheEntry struct {
	key      v2DocumentCacheKey
	encoded  []byte
	document Document
	cost     uint64
}

// v2DocumentCache is a strictly bounded decoded-document LRU. Entries are
// validated against the current immutable record bytes on every lookup, so an
// update or delete cannot return a stale version even without invalidation on
// the write path.
type v2DocumentCache struct {
	mu         sync.Mutex
	entries    map[v2DocumentCacheKey]*list.Element
	lru        list.List
	bytes      uint64
	maxEntries uint64
	maxBytes   uint64
	hits       atomic.Uint64
	misses     atomic.Uint64
	evictions  atomic.Uint64
}

func newV2DocumentCache(maxEntries, maxBytes uint64) *v2DocumentCache {
	return &v2DocumentCache{
		entries: make(map[v2DocumentCacheKey]*list.Element), maxEntries: maxEntries, maxBytes: maxBytes,
	}
}

func (cache *v2DocumentCache) decode(collection string, id DocumentID, encoded []byte) (Document, error) {
	if cache == nil || cache.maxEntries == 0 || cache.maxBytes == 0 {
		return decodeStoredDocument(encoded)
	}
	key := v2DocumentCacheKey{collection: collection, id: id}
	cache.mu.Lock()
	if element := cache.entries[key]; element != nil {
		entry := element.Value.(*v2DocumentCacheEntry)
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
	entry := &v2DocumentCacheEntry{
		key: key, encoded: append([]byte(nil), encoded...), document: document, cost: cost,
	}
	cache.mu.Lock()
	if element := cache.entries[key]; element != nil {
		existing := element.Value.(*v2DocumentCacheEntry)
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

func (cache *v2DocumentCache) remove(collection string, id DocumentID) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	if element := cache.entries[v2DocumentCacheKey{collection: collection, id: id}]; element != nil {
		cache.removeElementLocked(element, false)
	}
	cache.mu.Unlock()
}

func (cache *v2DocumentCache) removeElementLocked(element *list.Element, eviction bool) {
	if element == nil {
		return
	}
	entry := element.Value.(*v2DocumentCacheEntry)
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

func (cache *v2DocumentCache) stats() DocumentCacheStats {
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

func estimateCachedDocumentBytes(collection string, encoded []byte, document Document) uint64 {
	// Include encoded bytes used for exact version comparison plus a conservative
	// estimate of Go map/value overhead. Entry count provides a second hard bound
	// when many tiny fields make allocator overhead dominate encoded size.
	return uint64(len(collection)+len(encoded)) + estimateDocumentHeapBytes(document) + 192
}

func estimateDocumentHeapBytes(document Document) uint64 {
	result := uint64(64 + len(document)*48)
	for key, value := range document {
		result += uint64(len(key)) + estimateValueHeapBytes(value)
	}
	return result
}

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
