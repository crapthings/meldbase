package meldbase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

// DurableDatabaseChangeBatch is one globally ordered V2 Commit Log position
// projected into public document and catalog events. It is the semantic source
// for archive and single-writer-follower protocols: callers must Ack only
// after the externally applied effect for Token is durable.
//
// Private System records, raw pages, index-build progress and retention
// control records are deliberately excluded. A batch can therefore be empty
// when a private record advanced a retained position; it must still be Acked.
type DurableDatabaseChangeBatch struct {
	Token         uint64
	TransactionID [16]byte
	CommittedAt   time.Time
	Changes       []Change
}

// DurableDatabaseChangeSubscription is a crash-resumable pull/acknowledge
// feed over the complete public V2 database. It exposes collection creation,
// index publication and document changes in exact Commit Log order. It does
// not itself copy a bootstrap snapshot or apply changes to a follower; those
// transport and ownership contracts are intentionally separate.
type DurableDatabaseChangeSubscription struct {
	Batches <-chan DurableDatabaseChangeBatch
	Errors  <-chan error

	ack        func(uint64) error
	checkpoint func() (uint64, error)
	cancel     context.CancelFunc
	done       <-chan struct{}
	once       sync.Once
}

// Checkpoint returns the consumer's last durable acknowledgement. Delivered
// batches do not move it; callers may use it to bind a replication hello to
// the exact retained position, rather than to a process-local queue position.
func (subscription *DurableDatabaseChangeSubscription) Checkpoint() (uint64, error) {
	if subscription == nil || subscription.checkpoint == nil {
		return 0, ErrClosed
	}
	return subscription.checkpoint()
}

func (subscription *DurableDatabaseChangeSubscription) Ack(token uint64) error {
	if subscription == nil || subscription.ack == nil {
		return ErrClosed
	}
	return subscription.ack(token)
}

func (subscription *DurableDatabaseChangeSubscription) Close() {
	if subscription == nil {
		return
	}
	subscription.once.Do(func() {
		if subscription.cancel != nil {
			subscription.cancel()
		}
		if subscription.done != nil {
			<-subscription.done
		}
	})
}

// CreateDurableDatabaseChanges creates a named durable feed after afterToken.
// The requested position must still be retained. Names share no namespace with
// collection-scoped consumers, so one application can use the same logical
// name for both an outbox and a database archive without a checkpoint clash.
func (db *DB) CreateDurableDatabaseChanges(ctx context.Context, name string, afterToken uint64, buffer int) (*DurableDatabaseChangeSubscription, error) {
	if !validPublicDurableConsumerName(name) || buffer <= 0 || buffer > 1024 {
		return nil, ErrInvalidDocument
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
	store, ok := db.durability.(*v2DurableStore)
	if !ok || store == nil || store.file == nil {
		return nil, ErrDurableConsumerUnsupported
	}
	// See CreateDurableCollectionChanges: the first private consumer record on
	// an empty V2 file is a sequence-one control commit and requires current
	// primary authority.
	if db.token == 0 {
		if err := db.validateV2PrimaryWriteFence(1); err != nil {
			return nil, err
		}
	}
	consumer, err := store.file.CreateDurableCommitConsumer(durableDatabaseConsumerKey(name), afterToken)
	if err != nil {
		return nil, mapDurableConsumerError(err)
	}
	// See CreateDurableCollectionChanges: the first private durable-consumer
	// record initializes an otherwise empty V2 file at logical sequence one.
	if sequence := store.file.Meta().CommitSequence; sequence != db.token {
		if sequence < db.token {
			_ = consumer.Close()
			return nil, ErrCorrupt
		}
		db.token = sequence
	}
	return newDurableDatabaseChangeSubscription(ctx, store, consumer, buffer)
}

// OpenDurableDatabaseChanges resumes a named durable feed from its persisted
// checkpoint. ErrHistoryLost is returned instead of silently starting at a
// later token when the required Commit Log window is gone.
func (db *DB) OpenDurableDatabaseChanges(ctx context.Context, name string, buffer int) (*DurableDatabaseChangeSubscription, error) {
	if !validPublicDurableConsumerName(name) || buffer <= 0 || buffer > 1024 {
		return nil, ErrInvalidDocument
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
	store, ok := db.durability.(*v2DurableStore)
	if !ok || store == nil || store.file == nil {
		db.mu.RUnlock()
		return nil, ErrDurableConsumerUnsupported
	}
	consumer, err := store.file.OpenDurableCommitConsumer(durableDatabaseConsumerKey(name))
	db.mu.RUnlock()
	if err != nil {
		return nil, mapDurableConsumerError(err)
	}
	return newDurableDatabaseChangeSubscription(ctx, store, consumer, buffer)
}

// DeleteDurableDatabaseChanges removes a named database-wide checkpoint. Stop
// all active owners first; deletion deliberately makes future resume fail.
func (db *DB) DeleteDurableDatabaseChanges(ctx context.Context, name string) error {
	if !validPublicDurableConsumerName(name) {
		return ErrInvalidDocument
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
	store, ok := db.durability.(*v2DurableStore)
	if !ok || store == nil || store.file == nil {
		return ErrDurableConsumerUnsupported
	}
	return mapDurableConsumerError(store.file.DeleteDurableCommitConsumer(durableDatabaseConsumerKey(name)))
}

func newDurableDatabaseChangeSubscription(ctx context.Context, store *v2DurableStore, consumer *storagev2.DurableCommitConsumer, buffer int) (*DurableDatabaseChangeSubscription, error) {
	if store == nil || store.file == nil || consumer == nil || buffer <= 0 || buffer > 1024 {
		if consumer != nil {
			_ = consumer.Close()
		}
		return nil, ErrCorrupt
	}
	collections, err := durableDatabaseCollectionNames(store.file)
	if err != nil {
		_ = consumer.Close()
		return nil, mapDurableConsumerError(err)
	}
	child, cancel := context.WithCancel(ctx)
	batches := make(chan DurableDatabaseChangeBatch, buffer)
	errorsOut := make(chan error, 1)
	done := make(chan struct{})
	subscription := &DurableDatabaseChangeSubscription{Batches: batches, Errors: errorsOut, ack: consumer.Ack, checkpoint: func() (uint64, error) {
		return consumer.Checkpoint(), nil
	}, cancel: cancel, done: done}
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
			converted, err := convertDurableDatabaseBatch(consumer, batch, collections)
			if err != nil {
				if child.Err() == nil {
					errorsOut <- mapDurableConsumerError(err)
				}
				return
			}
			select {
			case <-child.Done():
				return
			case batches <- converted:
			}
		}
	}()
	return subscription, nil
}

func durableDatabaseCollectionNames(file *storagev2.File) (map[uint32]string, error) {
	if file == nil {
		return nil, ErrCorrupt
	}
	snapshot, err := file.OpenSnapshot()
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()
	collections, err := snapshot.Collections()
	if err != nil {
		return nil, err
	}
	result := make(map[uint32]string, len(collections))
	for _, collection := range collections {
		if collection.Meta.ID == 0 || !collectionNamePattern.MatchString(collection.Name) {
			return nil, ErrCorrupt
		}
		if _, duplicate := result[collection.Meta.ID]; duplicate {
			return nil, ErrCorrupt
		}
		result[collection.Meta.ID] = collection.Name
	}
	return result, nil
}

func convertDurableDatabaseBatch(consumer *storagev2.DurableCommitConsumer, batch storagev2.CommitBatch, collections map[uint32]string) (DurableDatabaseChangeBatch, error) {
	if consumer == nil || batch.Sequence == 0 || collections == nil {
		return DurableDatabaseChangeBatch{}, ErrCorrupt
	}
	result := DurableDatabaseChangeBatch{Token: batch.Sequence, TransactionID: batch.TransactionID, CommittedAt: batch.CommittedAt}
	seenDocuments := make(map[string]struct{})
	for _, change := range batch.Changes {
		if change.Operation == storagev2.CommitCatalog {
			converted, visible, err := convertDurableCatalogChange(change, collections)
			if err != nil {
				return DurableDatabaseChangeBatch{}, err
			}
			if visible {
				result.Changes = append(result.Changes, converted)
			}
			continue
		}
		collection, exists := collections[change.CollectionID]
		if !exists {
			return DurableDatabaseChangeBatch{}, ErrCorrupt
		}
		resolved, err := consumer.ResolveChange(change)
		if err != nil {
			return DurableDatabaseChangeBatch{}, err
		}
		converted, err := convertV2ReplayChange(resolved)
		if err != nil {
			return DurableDatabaseChangeBatch{}, err
		}
		converted.Collection = collection
		identity := collection + "\x00" + string(converted.DocumentID[:])
		if _, duplicate := seenDocuments[identity]; duplicate {
			return DurableDatabaseChangeBatch{}, ErrCorrupt
		}
		seenDocuments[identity] = struct{}{}
		result.Changes = append(result.Changes, converted)
	}
	return result, nil
}

func convertDurableCatalogChange(change storagev2.CommitChange, collections map[uint32]string) (Change, bool, error) {
	if change.CollectionID == 0 {
		return Change{}, false, ErrCorrupt
	}
	if change.CollectionID == ^uint32(0) {
		// Durable checkpoints, retention and RPC/idempotency records are private
		// control-plane details. Their sequence is still delivered as an empty
		// public batch so a consumer can release its retention pin.
		return Change{}, false, nil
	}
	if change.CollectionName != "" {
		if !collectionNamePattern.MatchString(change.CollectionName) || (len(change.ChangedPaths) != 1 || change.ChangedPaths[0] != "_catalog") {
			return Change{}, false, ErrCorrupt
		}
		if existing, exists := collections[change.CollectionID]; exists && existing != change.CollectionName {
			return Change{}, false, ErrCorrupt
		}
		collections[change.CollectionID] = change.CollectionName
		return Change{Collection: change.CollectionName, Operation: CreateCollectionOperation, ChangedPaths: append([]string(nil), change.ChangedPaths...)}, true, nil
	}
	collection, exists := collections[change.CollectionID]
	if !exists {
		return Change{}, false, ErrCorrupt
	}
	if len(change.ChangedPaths) != 1 || !strings.HasPrefix(change.ChangedPaths[0], "_indexes.") || len(change.After) == 0 || len(change.Before) != 0 {
		return Change{}, false, ErrCorrupt
	}
	indexName := strings.TrimPrefix(change.ChangedPaths[0], "_indexes.")
	if !indexNamePattern.MatchString(indexName) {
		return Change{}, false, ErrCorrupt
	}
	meta, err := storagev2.DecodeIndexMeta(indexName, change.After)
	if err != nil {
		return Change{}, false, err
	}
	fields, err := publicV2IndexFields(meta.FieldPath, meta.Fields)
	if err != nil || len(fields) == 0 {
		return Change{}, false, ErrCorrupt
	}
	definition := IndexDefinition{Name: indexName, Field: fields[0].Field, Order: fields[0].Order, Fields: fields, Unique: meta.Unique}
	return Change{Collection: collection, Operation: CreateIndexOperation, Index: &definition, ChangedPaths: append([]string(nil), change.ChangedPaths...)}, true, nil
}

func durableDatabaseConsumerKey(name string) string {
	digest := sha256.Sum256([]byte("database\x00" + name))
	return "d_" + hex.EncodeToString(digest[:20])
}
