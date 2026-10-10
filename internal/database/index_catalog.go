package database

import (
	"context"
	"sort"
)

// IndexCatalogEntry describes a published index without exposing document data.
//
// Index management remains a deployment concern; this catalog supports CLIs
// and protected operator views.
type IndexCatalogEntry struct {
	Collection string
	Definition IndexDefinition
}

// IndexCatalog returns copied index definitions sorted by collection and index name.
func (db *DB) IndexCatalog(ctx context.Context) ([]IndexCatalogEntry, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	entries := make([]IndexCatalogEntry, 0)
	for collection, data := range db.collections {
		if data == nil {
			continue
		}
		for _, state := range data.indexes {
			entries = append(entries, IndexCatalogEntry{
				Collection: collection,
				Definition: cloneIndexDefinition(state.definition),
			})
		}
	}
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].Collection != entries[right].Collection {
			return entries[left].Collection < entries[right].Collection
		}
		return entries[left].Definition.Name < entries[right].Definition.Name
	})
	return entries, nil
}
