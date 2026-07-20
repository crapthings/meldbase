package meldbase

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

// coordinatorPreparedMutation is an immutable selection made at admission.
// Its point read set pins every document whose before image will enter the V2
// group. The original query remains on the request solely for conflict fallback
// under db.mu; it is never evaluated by storage.
type coordinatorPreparedMutation struct {
	changes []Change
	readSet []storagev2.DocumentPrecondition
	update  UpdateResult
	delete  DeleteResult
}

func (coordinator *v2CommitCoordinator) submitUpdate(ctx context.Context, collection string, query QuerySpec, mutation MutationSpec, one bool, maxAffected int) (UpdateResult, error) {
	if coordinator == nil || coordinator.db == nil {
		return UpdateResult{}, ErrCorrupt
	}
	c := &Collection{db: coordinator.db, name: collection}
	coordinator.db.mu.Lock()
	prepared, err := c.prepareCoordinatorUpdateLocked(ctx, query, mutation, one, maxAffected)
	coordinator.db.mu.Unlock()
	if err != nil || len(prepared.changes) == 0 {
		return prepared.update, err
	}
	request := &v2CommitRequest{
		collection: collection, changes: prepared.changes, readSet: prepared.readSet,
		success: v2CommitResult{update: prepared.update}, result: make(chan v2CommitResult, 1),
	}
	request.fallback = func() v2CommitResult {
		result, err := c.updateQueryLocked(context.Background(), query, mutation, one, maxAffected)
		return v2CommitResult{update: result, err: err}
	}
	result, err := coordinator.admit(ctx, request)
	return result.update, err
}

func (coordinator *v2CommitCoordinator) submitDelete(ctx context.Context, collection string, query QuerySpec, one bool, maxAffected int) (DeleteResult, error) {
	if coordinator == nil || coordinator.db == nil {
		return DeleteResult{}, ErrCorrupt
	}
	c := &Collection{db: coordinator.db, name: collection}
	coordinator.db.mu.Lock()
	prepared, err := c.prepareCoordinatorDeleteLocked(ctx, query, one, maxAffected)
	coordinator.db.mu.Unlock()
	if err != nil || len(prepared.changes) == 0 {
		return prepared.delete, err
	}
	request := &v2CommitRequest{
		collection: collection, changes: prepared.changes, readSet: prepared.readSet,
		success: v2CommitResult{delete: prepared.delete}, result: make(chan v2CommitResult, 1),
	}
	request.fallback = func() v2CommitResult {
		result, err := c.deleteQueryLocked(context.Background(), query, one, maxAffected)
		return v2CommitResult{delete: result, err: err}
	}
	result, err := coordinator.admit(ctx, request)
	return result.delete, err
}

func (c *Collection) prepareCoordinatorUpdateLocked(ctx context.Context, query QuerySpec, mutation MutationSpec, one bool, maxAffected int) (coordinatorPreparedMutation, error) {
	if c == nil || c.db == nil || c.db.closed {
		return coordinatorPreparedMutation{}, ErrClosed
	}
	if c.db.fatalErr != nil {
		return coordinatorPreparedMutation{}, c.db.fatalErr
	}
	data := c.db.collections[c.name]
	selectionLimit, resourceBounded := c.db.boundedMutationSelection(maxAffected, one)
	selected, err := c.selectMutationDocumentsLocked(ctx, query, one, selectionLimit)
	if err != nil {
		if resourceBounded && errors.Is(err, ErrMutationLimit) {
			c.db.metrics.resourceLimitRejections.Add(1)
			return coordinatorPreparedMutation{}, fmt.Errorf("%w: transaction changes exceed limit %d", ErrResourceLimit, c.db.resourceLimits.MaxTransactionChanges)
		}
		return coordinatorPreparedMutation{}, err
	}
	if len(selected) > 0 && data == nil {
		return coordinatorPreparedMutation{}, ErrCorrupt
	}
	prepared := coordinatorPreparedMutation{update: UpdateResult{MatchedCount: int64(len(selected))}}
	pending := make([]pendingUpdate, 0, len(selected))
	for _, document := range selected {
		id, exists := document.ID()
		if !exists || id.IsZero() {
			return coordinatorPreparedMutation{}, ErrCorrupt
		}
		after := document.Clone()
		for _, operation := range mutation.operations {
			if err := applyUpdateOperation(after, operation); err != nil {
				return coordinatorPreparedMutation{}, err
			}
		}
		if err := after.Validate(); err != nil {
			return coordinatorPreparedMutation{}, err
		}
		if !after.Equal(document) {
			prepared.update.ModifiedCount++
			pending = append(pending, pendingUpdate{id: id, before: document.Clone(), after: after})
		}
	}
	prepared.changes = make([]Change, len(pending))
	changedPaths := mutation.Paths()
	for index, change := range pending {
		before, after := change.before.Clone(), change.after.Clone()
		prepared.changes[index] = Change{
			Collection: c.name, Operation: UpdateOperation, DocumentID: change.id, Before: &before, After: &after,
			ChangedPaths: append([]string(nil), changedPaths...),
		}
	}
	if err := c.db.validateTransactionResource(prepared.changes); err != nil {
		return coordinatorPreparedMutation{}, err
	}
	if err := data.validateIndexUpdates(pending); err != nil {
		return coordinatorPreparedMutation{}, err
	}
	readSet, err := coordinatorReadSet(prepared.changes)
	if err != nil {
		return coordinatorPreparedMutation{}, err
	}
	prepared.readSet = readSet
	return prepared, nil
}

func (c *Collection) prepareCoordinatorDeleteLocked(ctx context.Context, query QuerySpec, one bool, maxAffected int) (coordinatorPreparedMutation, error) {
	if c == nil || c.db == nil || c.db.closed {
		return coordinatorPreparedMutation{}, ErrClosed
	}
	if c.db.fatalErr != nil {
		return coordinatorPreparedMutation{}, c.db.fatalErr
	}
	data := c.db.collections[c.name]
	selectionLimit, resourceBounded := c.db.boundedMutationSelection(maxAffected, one)
	selected, err := c.selectMutationDocumentsLocked(ctx, query, one, selectionLimit)
	if err != nil {
		if resourceBounded && errors.Is(err, ErrMutationLimit) {
			c.db.metrics.resourceLimitRejections.Add(1)
			return coordinatorPreparedMutation{}, fmt.Errorf("%w: transaction changes exceed limit %d", ErrResourceLimit, c.db.resourceLimits.MaxTransactionChanges)
		}
		return coordinatorPreparedMutation{}, err
	}
	if len(selected) > 0 && data == nil {
		return coordinatorPreparedMutation{}, ErrCorrupt
	}
	prepared := coordinatorPreparedMutation{delete: DeleteResult{DeletedCount: int64(len(selected))}, changes: make([]Change, len(selected))}
	for index, document := range selected {
		id, exists := document.ID()
		if !exists || id.IsZero() {
			return coordinatorPreparedMutation{}, ErrCorrupt
		}
		before := document.Clone()
		prepared.changes[index] = Change{Collection: c.name, Operation: DeleteOperation, DocumentID: id, Before: &before}
	}
	if err := c.db.validateTransactionResource(prepared.changes); err != nil {
		return coordinatorPreparedMutation{}, err
	}
	readSet, err := coordinatorReadSet(prepared.changes)
	if err != nil {
		return coordinatorPreparedMutation{}, err
	}
	prepared.readSet = readSet
	return prepared, nil
}

func coordinatorReadSet(changes []Change) ([]storagev2.DocumentPrecondition, error) {
	readSet := make([]storagev2.DocumentPrecondition, 0, len(changes))
	for _, change := range changes {
		if change.Before == nil || change.DocumentID.IsZero() || change.Operation == InsertOperation {
			return nil, ErrCorrupt
		}
		encoded, err := encodeStoredDocument(*change.Before)
		if err != nil {
			return nil, err
		}
		readSet = append(readSet, storagev2.DocumentPrecondition{
			Collection: change.Collection, DocumentID: [16]byte(change.DocumentID), ExpectedExists: true, ExpectedHash: sha256.Sum256(encoded),
		})
	}
	return readSet, nil
}
