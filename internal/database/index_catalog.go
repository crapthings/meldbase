package database

import (
	"context"
	"sort"
)

// IndexCatalogEntry is an immutable operator-facing description of one
// published index. It contains no document keys, values, or cardinalities.
// Index management remains a deployment concern; this is intentionally a
// read-only catalog for CLIs and protected operator surfaces.
type IndexCatalogEntry struct {
	Collection string
	Definition IndexDefinition
}

// IndexCatalog returns every published index in canonical collection/name
// order. The returned definitions and field slices are independent copies.
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
