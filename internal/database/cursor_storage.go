package database

import "context"

// openStorageCollectionStreamLocked opens a lazy collection scan when no index plan applies.
//
// It returns the selected plan for FindQuery to reuse when an index applies.
// The caller must hold db.mu for reading or writing.
func (c *Collection) openStorageCollectionStreamLocked(ctx context.Context, query QuerySpec) (_ queryStorageDocumentIterator, access *queryAccessPlan, streamed bool, resultErr error) {
	if c == nil || c.db == nil || c.db.querySource == nil || len(query.sort) != 0 {
		return nil, nil, false, nil
	}
	snapshot, err := c.db.querySource.openQuerySnapshot()
	if err != nil {
		return nil, nil, false, err
	}
	defer func() {
		if closeErr := snapshot.Close(); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
	}()
	if snapshot.Sequence() != c.db.token {
		return nil, nil, false, ErrCorrupt
	}
	indexes, err := snapshot.Indexes(c.name)
	if err != nil {
		return nil, nil, false, err
	}
	definitions := make([]IndexDefinition, len(indexes))
	for index := range indexes {
		definitions[index] = IndexDefinition{
			Name: indexes[index].Name, Field: indexes[index].Field, Order: 1,
			Unique: indexes[index].Unique, Fields: cloneIndexFields(indexes[index].Fields),
		}
	}
	selected := selectQueryAccessPlan(query.where, definitions, query)
	if selected.usable {
		return nil, &selected, false, nil
	}
	if err := contextError(ctx); err != nil {
		return nil, nil, false, err
	}
	iterator, err := snapshot.OpenCollectionIterator(c.name)
	if err != nil {
		return nil, nil, false, err
	}
	return iterator, &selected, true, nil
}
