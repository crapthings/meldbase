package storage

import (
	"sync"
	"sync/atomic"
)

const (
	// defaultCachedPages sets the default page-cache capacity.
	defaultCachedPages = 1024
	// pageCacheShards sets the number of independently locked cache partitions.
	pageCacheShards = 16
)

// PageCacheStats reports cache occupancy, hits, misses, and evictions.
type PageCacheStats struct {
	CapacityPages uint64
	ResidentPages uint64
	Hits          uint64
	Misses        uint64
	Evictions     uint64
}

// StorageStats reports physical storage health without exposing application data.
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

// cachedPage holds a decoded page and the raw bytes backing its values.
type cachedPage struct {
	raw  []byte
	page Page
}

// pageCacheShard owns a locked cache partition with round-robin eviction.
type pageCacheShard struct {
	mu      sync.RWMutex
	entries map[uint64]*cachedPage
	order   []uint64
	hand    int
	limit   int
}

// pageCache distributes cached pages across independently locked partitions.
type pageCache struct {
	shards    [pageCacheShards]pageCacheShard
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// newPageCache creates a bounded cache with capacity shared across its partitions.
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

// get looks up a cached page and counts the hit or miss.
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

// put retains a page in its partition and evicts an older entry when full.
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

// invalidate removes cached pages whose IDs may be reused.
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

// stats samples cache occupancy and cumulative access counters.
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
