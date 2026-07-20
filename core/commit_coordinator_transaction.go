package meldbase

import (
	"context"

	storagev2 "github.com/crapthings/meldbase/internal/storage"
)

// submitWriteTransaction admits one completed public WriteTransaction to the
// V2 group publisher. Its callback has already returned and its point read set
// is frozen, so a failed speculative group can safely replay only this exact
// storage operation in admission order; it never invokes application code a
// second time.
func (coordinator *v2CommitCoordinator) submitWriteTransaction(
	ctx context.Context,
	changes []Change,
	preconditions []storagev2.DocumentPrecondition,
	collectionPreconditions []storagev2.CollectionPrecondition,
) error {
	if coordinator == nil || coordinator.db == nil || coordinator.store == nil || len(changes) == 0 {
		return ErrCorrupt
	}
	request := &v2CommitRequest{
		changes:           append([]Change(nil), changes...),
		readSet:           append([]storagev2.DocumentPrecondition(nil), preconditions...),
		collectionReadSet: append([]storagev2.CollectionPrecondition(nil), collectionPreconditions...),
		result:            make(chan v2CommitResult, 1), waitForOutcome: true,
	}
	request.fallback = func() v2CommitResult {
		_, _, err := coordinator.db.commitPreparedWriteTransactionLocked(
			context.Background(), coordinator.store, request.changes, nil, request.readSet, request.collectionReadSet, nil,
		)
		return v2CommitResult{err: err}
	}
	_, err := coordinator.admit(ctx, request)
	return err
}
