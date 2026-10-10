package database

import (
	"context"
	"errors"
	"fmt"
	"sync"

	storage "github.com/crapthings/meldbase/internal/storage"
)

// Follower owns a read-only local database advanced by validated source change batches.
//
// Transport authentication and snapshot transfer are managed outside this type;
// promotion requires the external authority and write guard described below.
type Follower struct {
	mu       sync.Mutex
	db       *DB
	promoted bool
}

// FollowerPromotionRequest identifies the database version requesting permission to become writable.
type FollowerPromotionRequest struct {
	DatabaseID     [16]byte
	CommitSequence uint64
}

// FollowerPromotionFence proves that the former primary lost write authority for this database version.
//
// Epoch is non-empty and
// deliberately opaque to Meldbase: a controller may use it as an epoch ID or
// a compact signed lease certificate (as integrations/primarylease does).
// Meldbase does not invent a local substitute for that distributed safety
// decision.
type FollowerPromotionFence struct {
	DatabaseID     [16]byte
	CommitSequence uint64
	Epoch          string
}

// FollowerPromotionAuthority grants promotion after durably revoking the former primary's write authority.
//
// Implementations normally revoke a primary lease through a quorum
// controller or external consensus store.
type FollowerPromotionAuthority interface {
	AuthorizeFollowerPromotion(context.Context, FollowerPromotionRequest) (FollowerPromotionFence, error)
}

// FollowerPromotionFenceBinder connects a promotion certificate to the local write protection guard.
//
// The binder may update caller-owned local lease/epoch state, but must not enable
// writes until it has accepted the exact fence. It runs on the promotion
// control path, outside the DB writer lock; unlike ValidatePrimaryWrite it
// may coordinate with the controller if the implementation needs to.
//
// A promoted follower requires this interface in addition to
// PrimaryWriteFence. Otherwise an unrelated always-allow guard could make a
// one-time promotion certificate appear to grant permanent write authority.
type FollowerPromotionFenceBinder interface {
	BindFollowerPromotion(context.Context, FollowerPromotionFence) error
}

// OpenFollower opens a database copy as a read-only replica.
//
// Normal public mutations on DB return ErrReplicaReadOnly; use Apply to advance the
// next source token. The returned DB remains fully queryable and reactive.
func OpenFollower(path string, options OpenOptions) (*Follower, error) {
	options.Follower = true
	options.CommitCoordinator = CommitCoordinatorOptions{}
	db, err := OpenWithOptions(path, options)
	if err != nil {
		return nil, err
	}
	return &Follower{db: db}, nil
}

// DB returns the follower's database handle for queries and subscriptions.
func (follower *Follower) DB() *DB {
	if follower == nil {
		return nil
	}
	return follower.db
}

// Close closes the follower's database while serializing follower operations.
func (follower *Follower) Close() error {
	if follower == nil {
		return nil
	}
	follower.mu.Lock()
	defer follower.mu.Unlock()
	if follower.db == nil {
		return nil
	}
	return follower.db.Close()
}

// Promote enables writes after an external authority revokes the former primary's write access.
//
// The authority must certify this exact database identity and commit token. There is no
// default or best-effort implementation: promoting without a durable external
// fence would turn a network partition into split-brain data loss.
func (follower *Follower) Promote(ctx context.Context, authority FollowerPromotionAuthority) (FollowerPromotionFence, error) {
	if follower == nil || follower.db == nil {
		return FollowerPromotionFence{}, ErrClosed
	}
	if authority == nil {
		return FollowerPromotionFence{}, ErrReplicaPromotionAuthority
	}
	if err := contextError(ctx); err != nil {
		return FollowerPromotionFence{}, err
	}
	follower.mu.Lock()
	defer follower.mu.Unlock()
	if follower.promoted {
		return FollowerPromotionFence{}, ErrReplicaPromoted
	}
	db := follower.db
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return FollowerPromotionFence{}, ErrClosed
	}
	if !db.replicaReadOnly || db.fatalErr != nil {
		err := db.fatalErr
		db.mu.RUnlock()
		if err != nil {
			return FollowerPromotionFence{}, err
		}
		return FollowerPromotionFence{}, ErrCorrupt
	}
	// The authority's promotion certificate proves this transition, but it is
	// not a substitute for ongoing local admission checks. Requiring a guard
	// before the controller is contacted prevents a promoted follower from
	// becoming permanently writable after its next lease/epoch change.
	if db.primaryWriteFence == nil {
		db.mu.RUnlock()
		return FollowerPromotionFence{}, ErrReplicaPromotionWriteFence
	}
	binder, ok := db.primaryWriteFence.(FollowerPromotionFenceBinder)
	if !ok || binder == nil {
		db.mu.RUnlock()
		return FollowerPromotionFence{}, ErrReplicaPromotionWriteFence
	}
	request := FollowerPromotionRequest{DatabaseID: db.databaseID, CommitSequence: db.token}
	db.mu.RUnlock()
	fence, err := authority.AuthorizeFollowerPromotion(ctx, request)
	if err != nil {
		return FollowerPromotionFence{}, err
	}
	if fence.DatabaseID != request.DatabaseID || fence.CommitSequence != request.CommitSequence || fence.Epoch == "" || len(fence.Epoch) > 256 {
		return FollowerPromotionFence{}, ErrReplicaPromotionFence
	}
	if err := binder.BindFollowerPromotion(ctx, fence); err != nil {
		return FollowerPromotionFence{}, fmt.Errorf("%w: %w", ErrReplicaPromotionFence, err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return FollowerPromotionFence{}, ErrClosed
	}
	if !db.replicaReadOnly || db.databaseID != request.DatabaseID || db.token != request.CommitSequence {
		return FollowerPromotionFence{}, ErrReplicaPromotionFence
	}
	db.replicaReadOnly = false
	follower.promoted = true
	return fence, nil
}

// ApplyFrame checks a replication frame's database identity before applying its batch.
//
// A transport handles hello/ack/resync;
// only a validated batch frame belongs at the follower mutation boundary.
func (follower *Follower) ApplyFrame(ctx context.Context, frame ReplicationFrame) error {
	if follower == nil || follower.db == nil {
		return ErrClosed
	}
	follower.mu.Lock()
	defer follower.mu.Unlock()
	if follower.promoted {
		return ErrReplicaPromoted
	}
	if frame.Type != ReplicationBatchFrame || frame.Batch == nil {
		return fmt.Errorf("%w: follower requires a batch frame", ErrReplicaProtocol)
	}
	follower.db.mu.RLock()
	identity, closed := follower.db.databaseID, follower.db.closed
	follower.db.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if frame.DatabaseID != identity {
		return fmt.Errorf("%w: replication source identity", ErrDatabaseIdentity)
	}
	return follower.applyLocked(ctx, *frame.Batch)
}

// Apply saves exactly the next source change batch as one atomic commit.
//
// A gap, duplicate or locally diverged token returns ErrReplicaSequence; it never
// guesses, retries business logic or advances past missing history.
func (follower *Follower) Apply(ctx context.Context, source DurableDatabaseChangeBatch) error {
	if follower == nil || follower.db == nil {
		return ErrClosed
	}
	follower.mu.Lock()
	defer follower.mu.Unlock()
	if follower.promoted {
		return ErrReplicaPromoted
	}
	return follower.applyLocked(ctx, source)
}

// applyLocked validates and applies the next source batch while the caller holds the follower lock.
func (follower *Follower) applyLocked(ctx context.Context, source DurableDatabaseChangeBatch) error {
	if follower == nil || follower.db == nil {
		return ErrClosed
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if source.Token == 0 || source.TransactionID == [16]byte{} {
		return ErrCorrupt
	}
	changes := make([]Change, len(source.Changes))
	for index, change := range source.Changes {
		changes[index] = cloneChange(change)
	}
	db := follower.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if !db.replicaReadOnly {
		return ErrCorrupt
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	if source.Token != db.token+1 {
		return fmt.Errorf("%w: have %d, received %d", ErrReplicaSequence, db.token, source.Token)
	}
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		return ErrReplicaReadOnly
	}
	return applyFollowerBatchLocked(ctx, db, store, source, changes)
}

// applyFollowerBatchLocked validates and stores a source batch while the caller holds the database lock.
func applyFollowerBatchLocked(ctx context.Context, db *DB, store *durableStore, source DurableDatabaseChangeBatch, changes []Change) error {
	documents := make([]Change, 0, len(changes))
	collections := make(map[string]struct{})
	indexes := make([]Change, 0, 1)
	for _, change := range changes {
		switch change.Operation {
		case CreateCollectionOperation:
			if !collectionNamePattern.MatchString(change.Collection) || change.DocumentID != (DocumentID{}) || change.Before != nil || change.After != nil || change.Index != nil {
				return ErrCorrupt
			}
			if _, duplicate := collections[change.Collection]; duplicate {
				return ErrCorrupt
			}
			collections[change.Collection] = struct{}{}
		case CreateIndexOperation, ReplaceIndexOperation:
			if change.Index == nil || !collectionNamePattern.MatchString(change.Collection) || change.Before != nil || change.After != nil {
				return ErrCorrupt
			}
			if _, err := validateIndexDefinition(change.Index.Name, indexDefinitionFields(*change.Index), IndexOptions{Unique: change.Index.Unique}); err != nil {
				return ErrCorrupt
			}
			indexes = append(indexes, change)
		case InsertOperation, UpdateOperation, DeleteOperation:
			if !collectionNamePattern.MatchString(change.Collection) || change.DocumentID.IsZero() {
				return ErrCorrupt
			}
			documents = append(documents, change)
		default:
			return ErrCorrupt
		}
	}

	if len(indexes) > 0 {
		if len(indexes) != 1 || len(documents) != 0 || (len(collections) > 1) {
			return ErrCorrupt
		}
		index := indexes[0]
		if len(collections) == 1 {
			if _, exists := collections[index.Collection]; !exists {
				return ErrCorrupt
			}
		}
		if err := store.appendDBCommit(ctx, db, source.Token, []Change{index}); err != nil {
			return err
		}
		data := db.collections[index.Collection]
		if data == nil {
			data = newCollectionData()
			db.collections[index.Collection] = data
		}
		if data.indexes == nil {
			data.indexes = make(map[string]*indexState)
		}
		definition := cloneIndexDefinition(*index.Index)
		data.indexes[definition.Name] = &indexState{definition: definition}
		return finishFollowerBatchLocked(db, source, changes)
	}

	if len(documents) > 0 {
		seenCollections := make(map[string]struct{})
		for _, change := range documents {
			seenCollections[change.Collection] = struct{}{}
		}
		for collection := range collections {
			if _, exists := seenCollections[collection]; !exists {
				return ErrCorrupt
			}
		}
		if err := db.validateTransactionResource(documents); err != nil {
			return err
		}
		if err := store.appendDBCommit(ctx, db, source.Token, documents); err != nil {
			return err
		}
		for collection := range seenCollections {
			if db.collections[collection] == nil {
				db.collections[collection] = newCollectionData()
			}
		}
		return finishFollowerBatchLocked(db, source, changes)
	}

	if len(collections) > 0 {
		if len(collections) != 1 {
			return ErrCorrupt
		}
		var collection string
		for collection = range collections {
		}
		if db.collections[collection] != nil {
			return ErrCorrupt
		}
		sequence, err := store.file.ApplyCreateCollection(storage.CreateCollectionTransaction{TransactionID: source.TransactionID, Collection: collection, CommittedAt: source.CommittedAt})
		if err != nil {
			return followerStorageError(db, err)
		}
		if sequence != source.Token {
			return followerSequenceFault(db, sequence, source.Token)
		}
		db.collections[collection] = newCollectionData()
		return finishFollowerBatchLocked(db, source, changes)
	}

	sequence, err := store.file.ApplyReplicationNoop(source.TransactionID, source.CommittedAt)
	if err != nil {
		return followerStorageError(db, err)
	}
	if sequence != source.Token {
		return followerSequenceFault(db, sequence, source.Token)
	}
	return finishFollowerBatchLocked(db, source, nil)
}

// finishFollowerBatchLocked advances the local token and publishes the applied changes under the database lock.
func finishFollowerBatchLocked(db *DB, source DurableDatabaseChangeBatch, changes []Change) error {
	if db == nil || source.Token != db.token+1 {
		return ErrCorrupt
	}
	db.token = source.Token
	batch := ChangeBatch{Token: source.Token, Changes: changes}
	db.recordLiveCommit(batch)
	db.publish(batch)
	return nil
}

// followerStorageError maps a storage rejection and stops the database for unrecoverable apply failures.
func followerStorageError(db *DB, err error) error {
	mapped := mapStorageError(err)
	if db != nil && !errors.Is(mapped, ErrResourceLimit) && !errors.Is(mapped, ErrDuplicateID) && !errors.Is(mapped, ErrDuplicateKey) && !errors.Is(mapped, ErrInvalidIndex) {
		db.fatalErr = fmt.Errorf("%w: follower apply: %v", ErrDurability, mapped)
		return db.fatalErr
	}
	return mapped
}

// followerSequenceFault stops the database when a stored commit sequence differs from the expected source sequence.
func followerSequenceFault(db *DB, actual, expected uint64) error {
	if db != nil {
		db.fatalErr = fmt.Errorf("%w: follower applied %d, expected %d", ErrDurability, actual, expected)
		return db.fatalErr
	}
	return ErrCorrupt
}
