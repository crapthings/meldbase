package database

import "github.com/crapthings/meldbase/internal/storage"

// indexBuildStatsBackend exposes cached index build statistics.
type indexBuildStatsBackend interface {
	indexBuildDBStats() IndexBuildStats
}

// indexBuildDBStats returns the latest cached index build statistics.
func (store *durableStore) indexBuildDBStats() IndexBuildStats {
	if store == nil {
		return IndexBuildStats{}
	}
	stats := store.indexBuildStats.Load()
	if stats == nil {
		return IndexBuildStats{}
	}
	return *stats
}

// persistentIndexBuildStats summarizes the progress and size of stored index builds.
func persistentIndexBuildStats(builds []storage.IndexBuildMeta) IndexBuildStats {
	stats := IndexBuildStats{}
	stats.Persistent = uint64(len(builds))
	for _, build := range builds {
		stats.PersistentEntries += build.EntryCount
		stats.PersistentBytes += build.CanonicalBytes
		switch build.Phase {
		case storage.IndexBuildScan:
			stats.Scanning++
		case storage.IndexBuildCatchUp:
			stats.CatchingUp++
		case storage.IndexBuildReady:
			stats.Ready++
		case storage.IndexBuildFailed:
			stats.PersistentFailed++
		}
		if build.Phase != storage.IndexBuildFailed &&
			(stats.oldestActiveAppliedSequence == 0 || build.AppliedSequence < stats.oldestActiveAppliedSequence) {
			stats.oldestActiveAppliedSequence = build.AppliedSequence
		}
	}
	return stats
}

// refreshIndexBuildStats replaces cached statistics with the current stored index build state.
func (store *durableStore) refreshIndexBuildStats() {
	if store == nil || store.file == nil {
		return
	}
	store.indexBuildStatsMu.Lock()
	defer store.indexBuildStatsMu.Unlock()
	builds, err := store.file.IndexBuilds()
	if err != nil {
		return
	}
	stats := persistentIndexBuildStats(builds)
	store.indexBuildStats.Store(&stats)
}
