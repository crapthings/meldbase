package v2

import (
	"sync"
	"sync/atomic"
)

const (
	defaultCachedPages = 1024
	pageCacheShards    = 16
)

type PageCacheStats struct {
	CapacityPages uint64
	ResidentPages uint64
	Hits          uint64
	Misses        uint64
	Evictions     uint64
}

// StorageStats is a bounded point-in-time view of physical storage health. It
// contains no keys, document identifiers, paths, or user values.
type StorageStats struct {
	PageSize                   uint64
	Generation                 uint64
	PhysicalPages              uint64
	CommitSequence             uint64
	OldestRetainedSequence     uint64
	RetainedCommits            uint64
	CommitRetentionMax         uint64
	CommitRetentionOverage     uint64
	RetainedCommitBytes        uint64
	CommitRetentionMaxBytes    uint64
	CommitRetentionByteOverage uint64
	RetentionPrunedCommits     uint64
	RetentionPressureEvents    uint64
	RetentionPressure          bool
	StorageUsedBytes           uint64
	StorageMaxBytes            uint64
	StorageByteOverage         uint64
	StorageLimitRejections     uint64
	StorageQuotaExhausted      bool
	ActiveReaders              uint64
	ActiveReplayLeases         uint64
	DocumentCount              uint64
	CollectionCount            uint64
	ReusablePages              uint64
	TreeSplits                 uint64
	TreeMerges                 uint64
	PersistentFreeSpace        bool
	FreeSpaceLoads             uint64
	FreeSpaceLoadFailures      uint64
	FreeSpacePublishes         uint64
	FreeSpaceCandidateChecks   uint64
	PageCache                  PageCacheStats
}

type cachedPage struct {
	raw  []byte
	page Page
}

type pageCacheShard struct {
	mu      sync.RWMutex
	entries map[uint64]*cachedPage
	order   []uint64
	hand    int
	limit   int
}

type pageCache struct {
	shards    [pageCacheShards]pageCacheShard
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

func newPageCache(capacity int) *pageCache {
	if capacity < pageCacheShards {
		capacity = pageCacheShards
	}
	cache := &pageCache{}
	base, remainder := capacity/pageCacheShards, capacity%pageCacheShards
	for index := range cache.shards {
		limit := base
		if index < remainder {
			limit++
		}
		cache.shards[index] = pageCacheShard{entries: make(map[uint64]*cachedPage, limit), order: make([]uint64, 0, limit), limit: limit}
	}
	return cache
}

func (cache *pageCache) get(pageID uint64) (*cachedPage, bool) {
	if cache == nil {
		return nil, false
	}
	shard := &cache.shards[pageID%pageCacheShards]
	shard.mu.RLock()
	page := shard.entries[pageID]
	shard.mu.RUnlock()
	if page == nil {
		cache.misses.Add(1)
		return nil, false
	}
	cache.hits.Add(1)
	return page, true
}

func (cache *pageCache) put(pageID uint64, raw []byte, decoded Page) *cachedPage {
	entry := &cachedPage{raw: raw, page: decoded}
	shard := &cache.shards[pageID%pageCacheShards]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if existing := shard.entries[pageID]; existing != nil {
		return existing
	}
	if len(shard.entries) >= shard.limit {
		victim := shard.order[shard.hand]
		delete(shard.entries, victim)
		shard.order[shard.hand] = pageID
		shard.hand = (shard.hand + 1) % shard.limit
		cache.evictions.Add(1)
	} else {
		shard.order = append(shard.order, pageID)
	}
	shard.entries[pageID] = entry
	return entry
}

func (cache *pageCache) invalidate(pageIDs []uint64) {
	if cache == nil {
		return
	}
	for _, pageID := range pageIDs {
		shard := &cache.shards[pageID%pageCacheShards]
		shard.mu.Lock()
		if _, exists := shard.entries[pageID]; exists {
			delete(shard.entries, pageID)
			for index, candidate := range shard.order {
				if candidate != pageID {
					continue
				}
				shard.order = append(shard.order[:index], shard.order[index+1:]...)
				if index < shard.hand {
					shard.hand--
				}
				if len(shard.order) == 0 || shard.hand >= len(shard.order) {
					shard.hand = 0
				}
				break
			}
		}
		shard.mu.Unlock()
	}
}

func (cache *pageCache) stats() PageCacheStats {
	if cache == nil {
		return PageCacheStats{}
	}
	stats := PageCacheStats{Hits: cache.hits.Load(), Misses: cache.misses.Load(), Evictions: cache.evictions.Load()}
	for index := range cache.shards {
		shard := &cache.shards[index]
		shard.mu.RLock()
		stats.CapacityPages += uint64(shard.limit)
		stats.ResidentPages += uint64(len(shard.entries))
		shard.mu.RUnlock()
	}
	return stats
}
