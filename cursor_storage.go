package meldbase

import "context"

// openStorageCollectionStreamLocked returns a lazy insertion-order COLLSCAN
// only when the normal planner would not select Primary point lookup or a
// Secondary index. db.mu must be read- or write-locked by the caller.
func (c *Collection) openStorageCollectionStreamLocked(ctx context.Context, query QuerySpec) (_ queryStorageDocumentIterator, streamed bool, resultErr error) {
	if c == nil || c.db == nil || c.db.querySource == nil || len(query.sort) != 0 {
		return nil, false, nil
	}
	if value, ok := equalityCandidate(query.where, "_id"); ok && value.kind == IDKind {
		return nil, false, nil
	}
	snapshot, err := c.db.querySource.openQuerySnapshot()
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if closeErr := snapshot.Close(); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
	}()
	if snapshot.Sequence() != c.db.token {
		return nil, false, ErrCorrupt
	}
	indexes, err := snapshot.Indexes(c.name)
	if err != nil {
		return nil, false, err
	}
	for _, index := range indexes {
		value, ok := equalityCandidate(query.where, index.Field)
		if !ok {
			continue
		}
		if _, err := encodeIndexKey(value); err == nil {
			return nil, false, nil
		}
	}
	for _, index := range indexes {
		lower, upper, ok := rangeCandidate(query.where, index.Field)
		if !ok {
			continue
		}
		if _, _, _, err := storageIndexBounds(lower, upper); err == nil {
			return nil, false, nil
		}
	}
	if err := contextError(ctx); err != nil {
		return nil, false, err
	}
	iterator, err := snapshot.OpenCollectionIterator(c.name)
	if err != nil {
		return nil, false, err
	}
	return iterator, true, nil
}
