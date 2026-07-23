package database

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"

	storage "github.com/crapthings/meldbase/internal/storage"
)

// DurableChangeBatch is one globally ordered Commit Log position projected to
// one collection. Changes is empty when another collection or private catalog
// change advanced the durable position; callers must still Ack that Token after
// processing it so the checkpoint can advance without pinning history forever.
//
// This is deliberately a document-change feed, not a full replication protocol:
// it does not expose private System records, index definitions, collection
// lifecycle or raw storage bytes.
type DurableChangeBatch struct {
	Token   uint64
	Changes []Change
}

// DurableChangeSubscription is a pull/acknowledge bridge over a  durable
// checkpoint. Batches remain ordered. Ack must be called only after the
// consumer's external side effect for that token is durable.
type DurableChangeSubscription struct {
	Batches <-chan DurableChangeBatch
	Errors  <-chan error

	ack    func(uint64) error
	cancel context.CancelFunc
	done   <-chan struct{}
	once   sync.Once
}

func (subscription *DurableChangeSubscription) Ack(token uint64) error {
	if subscription == nil || subscription.ack == nil {
		return ErrClosed
	}
	return subscription.ack(token)
}

func (subscription *DurableChangeSubscription) Close() {
	if subscription != nil {
		subscription.once.Do(func() {
			if subscription.cancel != nil {
				subscription.cancel()
			}
			if subscription.done != nil {
				<-subscription.done
			}
		})
	}
}

// CreateDurableCollectionChanges creates a durable, collection-scoped document
// feed at afterToken. The requested position must still be retained. A logical
// name may be reused for a different collection without collision; the storage
// checkpoint identity is derived from both values.
func (db *DB) CreateDurableCollectionChanges(ctx context.Context, name, collection string, afterToken uint64, buffer int) (*DurableChangeSubscription, error) {
	if err := validateDurableCollectionConsumer(name, collection, buffer); err != nil {
		return nil, err
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, ErrClosed
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		return nil, ErrDurableConsumerUnsupported
	}
	// An empty database establishes its private consumer directory through a
	// sequence-one control commit. It is not a user-data event, but it changes
	// the source history and must not become a stale-primary side door.
	if db.token == 0 {
		if err := db.validatePrimaryWriteFence(1); err != nil {
			return nil, err
		}
	}
	consumer, err := store.file.CreateDurableCommitConsumer(durableCollectionConsumerKey(name, collection), afterToken)
	if err != nil {
		return nil, mapDurableConsumerError(err)
	}
	// A brand-new empty file establishes its private System tree through one
	// initialization commit. Keep DB's in-memory sequence aligned with that
	// storage fact before any subsequent business write computes token+1.
	if sequence := store.file.Meta().CommitSequence; sequence != db.token {
		if sequence < db.token {
			_ = consumer.Close()
			return nil, ErrCorrupt
		}
		db.token = sequence
	}
	return newDurableCollectionSubscription(ctx, store, consumer, collection, buffer)
}

// OpenDurableCollectionChanges reopens an existing durable collection feed at
// its stored checkpoint. If retention cannot satisfy that position it returns
// ErrHistoryLost; it never silently starts from a newer token.
func (db *DB) OpenDurableCollectionChanges(ctx context.Context, name, collection string, buffer int) (*DurableChangeSubscription, error) {
	if err := validateDurableCollectionConsumer(name, collection, buffer); err != nil {
		return nil, err
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, ErrClosed
	}
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		db.mu.RUnlock()
		return nil, ErrDurableConsumerUnsupported
	}
	consumer, err := store.file.OpenDurableCommitConsumer(durableCollectionConsumerKey(name, collection))
	db.mu.RUnlock()
	if err != nil {
		return nil, mapDurableConsumerError(err)
	}
	return newDurableCollectionSubscription(ctx, store, consumer, collection, buffer)
}

// DeleteDurableCollectionChanges explicitly removes one retained checkpoint.
// Callers should stop every owner of that logical consumer first; an already
// open stream retains only its temporary process-local pin until it closes.
func (db *DB) DeleteDurableCollectionChanges(ctx context.Context, name, collection string) error {
	if err := validateDurableCollectionConsumer(name, collection, 1); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if db == nil {
		return ErrClosed
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		return ErrDurableConsumerUnsupported
	}
	return mapDurableConsumerError(store.file.DeleteDurableCommitConsumer(durableCollectionConsumerKey(name, collection)))
}

func newDurableCollectionSubscription(ctx context.Context, store *durableStore, consumer *storage.DurableCommitConsumer, collection string, buffer int) (*DurableChangeSubscription, error) {
	if store == nil || store.file == nil || consumer == nil || !collectionNamePattern.MatchString(collection) || buffer <= 0 || buffer > 1024 {
		if consumer != nil {
			_ = consumer.Close()
		}
		return nil, ErrCorrupt
	}
	collectionID, err := durableCollectionID(store.file, collection)
	if err != nil {
		_ = consumer.Close()
		return nil, mapDurableConsumerError(err)
	}
	child, cancel := context.WithCancel(ctx)
	batches := make(chan DurableChangeBatch, buffer)
	errorsOut := make(chan error, 1)
	done := make(chan struct{})
	subscription := &DurableChangeSubscription{
		Batches: batches, Errors: errorsOut, ack: consumer.Ack, cancel: cancel, done: done,
	}
	go func() {
		defer close(done)
		defer cancel()
		defer consumer.Close()
		defer close(batches)
		defer close(errorsOut)
		for {
			batch, err := consumer.Next(child)
			if err != nil {
				if child.Err() == nil {
					errorsOut <- mapDurableConsumerError(err)
				}
				return
			}
			converted, nextCollectionID, err := convertDurableCollectionBatch(consumer, batch, collection, collectionID)
			if err != nil {
				if child.Err() == nil {
					errorsOut <- mapDurableConsumerError(err)
				}
				return
			}
			collectionID = nextCollectionID
			select {
			case <-child.Done():
				return
			case batches <- converted:
			}
		}
	}()
	return subscription, nil
}

func convertDurableCollectionBatch(consumer *storage.DurableCommitConsumer, batch storage.CommitBatch, collection string, collectionID uint32) (DurableChangeBatch, uint32, error) {
	if consumer == nil || batch.Sequence == 0 || !collectionNamePattern.MatchString(collection) {
		return DurableChangeBatch{}, 0, ErrCorrupt
	}
	result := DurableChangeBatch{Token: batch.Sequence}
	seen := make(map[DocumentID]struct{})
	for _, change := range batch.Changes {
		if change.Operation == storage.CommitCatalog {
			if change.CollectionName == collection {
				if collectionID != 0 && collectionID != change.CollectionID {
					return DurableChangeBatch{}, 0, ErrCorrupt
				}
				collectionID = change.CollectionID
			}
			continue
		}
		if collectionID == 0 || change.CollectionID != collectionID {
			continue
		}
		resolved, err := consumer.ResolveChange(change)
		if err != nil {
			return DurableChangeBatch{}, 0, err
		}
		converted, err := convertReplayChange(resolved)
		if err != nil {
			return DurableChangeBatch{}, 0, err
		}
		converted.Collection = collection
		if _, duplicate := seen[converted.DocumentID]; duplicate {
			return DurableChangeBatch{}, 0, ErrCorrupt
		}
		seen[converted.DocumentID] = struct{}{}
		result.Changes = append(result.Changes, converted)
	}
	return result, collectionID, nil
}

func durableCollectionID(file *storage.File, collection string) (uint32, error) {
	if file == nil || !collectionNamePattern.MatchString(collection) {
		return 0, ErrCorrupt
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		return 0, err
	}
	defer snapshot.Close()
	meta, exists, err := snapshot.CollectionMeta(collection)
	if err != nil || !exists {
		return 0, err
	}
	return meta.ID, nil
}

func validateDurableCollectionConsumer(name, collection string, buffer int) error {
	if !validPublicDurableConsumerName(name) || !collectionNamePattern.MatchString(collection) || buffer <= 0 || buffer > 1024 {
		return ErrInvalidDocument
	}
	return nil
}

func validPublicDurableConsumerName(name string) bool {
	if len(name) == 0 || len(name) > 128 {
		return false
	}
	for index := range len(name) {
		value := name[index]
		if !((value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || value == '_' || value == '-') {
			return false
		}
	}
	return true
}

func durableCollectionConsumerKey(name, collection string) string {
	digest := sha256.Sum256([]byte(name + "\x00" + collection))
	return "c_" + hex.EncodeToString(digest[:20])
}

func mapDurableConsumerError(err error) error {
	if err == nil {
		return nil
	}
	mapped := mapStorageError(err)
	if errors.Is(mapped, storage.ErrCursorClosed) {
		return ErrClosed
	}
	return mapped
}
