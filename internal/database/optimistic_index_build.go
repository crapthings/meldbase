package database

import (
	"context"
	"errors"
	"fmt"
)

const maxOptimisticIndexBuildAttempts = 3

// createDurableIndexOptimistic performs the document decode/key extraction phase on
// a pinned immutable snapshot without holding db.mu. Publication still takes
// the writer lock and succeeds only if the source sequence remains current.
// A later persisted shadow-build protocol can replace bounded retries without
// changing ApplyCreateIndex's final atomic catalog publication.
func (c *Collection) createDurableIndexOptimistic(ctx context.Context, definition IndexDefinition, store *durableStore, budget *indexBuildBudget) error {
	reservation := c.name + "\x00" + definition.Name
	c.db.mu.Lock()
	if err := c.indexBuildPreconditionLocked(definition); err != nil {
		c.db.mu.Unlock()
		return err
	}
	if c.db.indexBuildReservations == nil {
		c.db.indexBuildReservations = make(map[string]struct{})
	}
	if _, exists := c.db.indexBuildReservations[reservation]; exists {
		c.db.metrics.indexBuildConflicts.Add(1)
		c.db.mu.Unlock()
		return ErrWriteConflict
	}
	c.db.indexBuildReservations[reservation] = struct{}{}
	c.db.mu.Unlock()
	defer func() {
		c.db.mu.Lock()
		delete(c.db.indexBuildReservations, reservation)
		c.db.mu.Unlock()
	}()

	for attempt := 0; attempt < maxOptimisticIndexBuildAttempts; attempt++ {
		if err := contextError(ctx); err != nil {
			return err
		}
		c.db.mu.RLock()
		if err := c.indexBuildPreconditionLocked(definition); err != nil {
			c.db.mu.RUnlock()
			return err
		}
		sequence := c.db.token
		c.db.mu.RUnlock()

		budget.reset()
		entries, err := store.collectIndexEntries(ctx, sequence, c.name, definition, budget)
		if errors.Is(err, ErrWriteConflict) {
			c.db.recordIndexBuildConflict(attempt)
			continue
		}
		if err != nil {
			c.db.mu.RLock()
			closed := c.db.closed
			c.db.mu.RUnlock()
			if closed {
				return ErrClosed
			}
			return err
		}

		c.db.mu.Lock()
		if err := c.indexBuildPreconditionLocked(definition); err != nil {
			c.db.mu.Unlock()
			return err
		}
		if c.db.token != sequence {
			c.db.mu.Unlock()
			c.db.recordIndexBuildConflict(attempt)
			continue
		}
		prepared := &store.preparedIndexBuild
		prepared.active, prepared.sequence = true, sequence
		prepared.collection, prepared.name, prepared.entries = c.name, definition.Name, entries
		c.db.activeIndexBuild = budget
		copyDefinition := cloneIndexDefinition(definition)
		change := Change{Collection: c.name, Operation: CreateIndexOperation, Index: &copyDefinition}
		token := sequence + 1
		err = c.db.appendCommit(ctx, token, []Change{change})
		prepared.active, prepared.entries = false, nil
		c.db.activeIndexBuild = nil
		if err == nil {
			data := c.db.collections[c.name]
			if data == nil {
				data = newCollectionData()
				c.db.collections[c.name] = data
			}
			if data.indexes == nil {
				data.indexes = make(map[string]*indexState)
			}
			data.indexes[definition.Name] = &indexState{definition: definition}
			c.db.token = token
			c.db.recordLiveCommit(ChangeBatch{Token: token, Changes: []Change{change}})
		}
		c.db.mu.Unlock()
		return err
	}
	return fmt.Errorf("%w: index build overlapped concurrent writes", ErrWriteConflict)
}

func (db *DB) recordIndexBuildConflict(attempt int) {
	db.metrics.indexBuildConflicts.Add(1)
	if attempt+1 < maxOptimisticIndexBuildAttempts {
		db.metrics.indexBuildRetries.Add(1)
	}
}

// indexBuildPreconditionLocked accepts either db.mu mode.
func (c *Collection) indexBuildPreconditionLocked(definition IndexDefinition) error {
	if c.db.closed {
		return ErrClosed
	}
	if c.db.fatalErr != nil {
		return c.db.fatalErr
	}
	if data := c.db.collections[c.name]; data != nil && data.indexes != nil {
		if _, exists := data.indexes[definition.Name]; exists {
			return fmt.Errorf("%w: index name exists", ErrInvalidIndex)
		}
	}
	return nil
}
