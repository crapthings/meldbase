package meldbase

import (
	"context"

	storage "github.com/crapthings/meldbase/internal/storage"
)

// submitWriteTransaction admits one completed public WriteTransaction to the
//
//	group publisher. Its callback has already returned and its point read set
//
// is frozen, so a failed speculative group can safely replay only this exact
// storage operation in admission order; it never invokes application code a
// second time.
func (coordinator *commitCoordinator) submitWriteTransaction(
	ctx context.Context,
	changes []Change,
	preconditions []storage.DocumentPrecondition,
	collectionPreconditions []storage.CollectionPrecondition,
) error {
	if coordinator == nil || coordinator.db == nil || coordinator.store == nil || len(changes) == 0 {
		return ErrCorrupt
	}
	request := &commitRequest{
		changes:           append([]Change(nil), changes...),
		readSet:           append([]storage.DocumentPrecondition(nil), preconditions...),
		collectionReadSet: append([]storage.CollectionPrecondition(nil), collectionPreconditions...),
		result:            make(chan commitResult, 1), waitForOutcome: true,
	}
	request.fallback = func() commitResult {
		_, _, err := coordinator.db.commitPreparedWriteTransactionLocked(
			context.Background(), coordinator.store, request.changes, nil, request.readSet, request.collectionReadSet, nil,
		)
		return commitResult{err: err}
	}
	_, err := coordinator.admit(ctx, request)
	return err
}
