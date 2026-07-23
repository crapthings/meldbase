package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	storage "github.com/crapthings/meldbase/internal/storage"
)

// CreateCollection creates an empty durable collection. CRUD operations still
// create collections implicitly; this explicit form exists for schema tools
// that must preserve an otherwise-empty collection, such as logical import.
func (db *DB) CreateCollection(ctx context.Context, name string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if db == nil || !collectionNamePattern.MatchString(name) {
		return ErrInvalidCollection
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	if db.replicaReadOnly {
		return ErrReplicaReadOnly
	}
	if _, exists := db.collections[name]; exists {
		return fmt.Errorf("%w: collection exists", ErrInvalidCollection)
	}
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		return ErrLogicalArchiveUnsupported
	}
	if err := db.validatePrimaryWriteFence(db.token + 1); err != nil {
		return err
	}
	transactionID, err := newSnapshotTransactionID()
	if err != nil {
		return err
	}
	sequence, err := store.file.ApplyCreateCollection(storage.CreateCollectionTransaction{
		TransactionID: transactionID, Collection: name, CommittedAt: time.Now(),
	})
	if err != nil {
		if errors.Is(err, storage.ErrCollectionExists) {
			return fmt.Errorf("%w: collection exists", ErrInvalidCollection)
		}
		mapped := mapStorageError(err)
		db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, mapped)
		return db.fatalErr
	}
	if sequence != db.token+1 {
		db.fatalErr = fmt.Errorf("%w: collection sequence mismatch", ErrDurability)
		return db.fatalErr
	}
	if err := db.advanceRollbackAnchorLocked(ctx, store, sequence); err != nil {
		return err
	}
	db.collections[name] = newCollectionData()
	db.token = sequence
	batch := ChangeBatch{Token: sequence, Changes: []Change{{Collection: name, Operation: CreateCollectionOperation, ChangedPaths: []string{"_catalog"}}}}
	db.recordLiveCommit(batch)
	db.publish(batch)
	return nil
}
