package database

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	storage "github.com/crapthings/meldbase/internal/storage"
	"github.com/crapthings/meldbase/internal/systemrecord"
)

// durableStore adapts the page/Commit Log format to the public database API.
type durableStore struct {
	file                     *storage.File
	documents                *documentCache
	path                     string
	rollbackAnchor           RollbackAnchorStore
	databaseID               [16]byte
	rollbackAnchorTimeout    time.Duration
	rollbackAnchorGate       chan struct{}
	rollbackAnchorSequence   atomic.Uint64
	rollbackAnchorGeneration atomic.Uint64
	rollbackAnchorFailures   atomic.Uint64
	rollbackAnchorNanos      atomic.Uint64
	rollbackAnchorMaxNanos   atomic.Uint64
	compactMu                sync.Mutex
	// testCompactionSnapshotHook pauses a compaction after it has pinned its
	// immutable source snapshot. It is package-test-only proof that the long
	// copy/verification phase does not hold db.mu and block writers.
	testCompactionSnapshotHook func()
	// testQuerySnapshotHook pauses a  query snapshot after its immutable
	// storage root is pinned. It proves cold reactive construction scans without
	// retaining db.mu.
	testQuerySnapshotHook func()
	preparedIndexBuild    struct {
		active     bool
		sequence   uint64
		collection string
		name       string
		entries    []storage.IndexEntry
	}
	testIndexBuildSnapshotHook        func()
	testPersistentIndexBuildReadyHook func()
	testPersistentIndexBuildBatchHook func(context.Context, IndexBuildID)
	indexBuildStatsMu                 sync.Mutex
	indexBuildStats                   atomic.Pointer[IndexBuildStats]
}

// Open creates or opens a Meldbase durable database in the current format.
func Open(path string) (*DB, error) {
	return OpenWithOptions(path, OpenOptions{})
}

// OpenWithOptions creates or opens a durable database with explicit storage and resource options.
func OpenWithOptions(path string, options OpenOptions) (*DB, error) {
	if err := validateRecoveryMode(options.Recovery); err != nil {
		return nil, err
	}
	replayDeliveryTimeout, err := normalizeReplayDeliveryTimeout(options.ReplayDeliveryTimeout)
	if err != nil {
		return nil, err
	}
	coordinatorOptions, err := normalizeCommitCoordinatorOptions(options.CommitCoordinator)
	if err != nil {
		return nil, err
	}
	resourceLimits, err := normalizeResourceLimits(options.ResourceLimits)
	if err != nil {
		return nil, err
	}
	protection := options.RollbackProtection
	if protection.OperationTimeout < 0 || (protection.AnchorStore == nil && (protection.InitializeAnchor || protection.OperationTimeout != 0)) {
		return nil, ErrInvalidRollbackProtection
	}
	anchorTimeout := protection.OperationTimeout
	if protection.AnchorStore != nil && anchorTimeout == 0 {
		anchorTimeout = DefaultRollbackAnchorOperationTimeout
	}
	anchor := RollbackAnchor{}
	anchorExists := false
	if protection.AnchorStore != nil {
		anchorContext, cancel := context.WithTimeout(context.Background(), anchorTimeout)
		anchor, anchorExists, err = protection.AnchorStore.Load(anchorContext)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrRollbackAnchor, err)
		}
		if !anchorExists && !protection.InitializeAnchor {
			return nil, ErrRollbackAnchorRequired
		}
		if anchorExists && protection.InitializeAnchor {
			return nil, ErrInvalidRollbackProtection
		}
		if anchorExists && !validRollbackAnchor(anchor) {
			return nil, ErrRollbackAnchor
		}
	}
	expectedID := protection.ExpectedDatabaseID
	minimumSequence := protection.MinimumCommitSequence
	minimumGeneration := protection.MinimumGeneration
	if anchorExists {
		if !zeroDatabaseID(expectedID) && expectedID != anchor.DatabaseID {
			return nil, ErrDatabaseIdentity
		}
		expectedID = anchor.DatabaseID
		if anchor.MinimumCommitSequence > minimumSequence {
			minimumSequence = anchor.MinimumCommitSequence
		}
		if anchor.MinimumGeneration > minimumGeneration {
			minimumGeneration = anchor.MinimumGeneration
		}
	}
	file, meta, recovery, err := storage.OpenWithOptions(path, storage.OpenOptions{
		RequireClean:              options.Recovery == RecoveryRequireClean,
		RequireGraphAudit:         options.RequireGraphAudit,
		RequirePrivateFileMode:    options.RequirePrivateFileMode,
		ExpectedDatabaseID:        expectedID,
		MinimumCommitSequence:     minimumSequence,
		MinimumGeneration:         minimumGeneration,
		CommitRetentionMaxCommits: options.CommitRetention.MaxCommits,
		CommitRetentionMaxBytes:   options.CommitRetention.MaxBytes,
		MaxFileBytes:              options.StorageLimits.MaxFileBytes,
	})
	if err != nil {
		return nil, mapStorageError(err)
	}
	selectedStorageLimits := StorageLimits{MaxFileBytes: file.StorageStats().StorageMaxBytes}
	fail := func(err error) (*DB, error) {
		_ = file.Close()
		return nil, err
	}
	snapshot, stream, err := file.OpenSnapshotAndStream()
	if err != nil {
		return fail(mapStorageError(err))
	}
	collections, err := loadCollections(snapshot)
	closeErr := errors.Join(snapshot.Close(), stream.Close())
	if err != nil {
		return fail(err)
	}
	if closeErr != nil {
		return fail(mapStorageError(closeErr))
	}
	absolutePath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return fail(err)
	}
	if protection.AnchorStore != nil && (!anchorExists || anchor.MinimumCommitSequence != meta.CommitSequence || anchor.MinimumGeneration != meta.Generation) {
		anchorContext, cancel := context.WithTimeout(context.Background(), anchorTimeout)
		requested := RollbackAnchor{DatabaseID: meta.DatabaseID, MinimumCommitSequence: meta.CommitSequence, MinimumGeneration: meta.Generation}
		retained, err := persistRollbackAnchor(anchorContext, protection.AnchorStore, requested)
		cancel()
		if err != nil || retained != requested {
			if err == nil {
				err = fmt.Errorf("%w: anchor is ahead of the opened database", ErrRollbackAnchor)
			}
			return fail(err)
		}
	}
	store := &durableStore{file: file, documents: newDocumentCache(defaultDocumentCacheEntries, defaultDocumentCacheBytes), path: absolutePath, rollbackAnchor: protection.AnchorStore, databaseID: meta.DatabaseID, rollbackAnchorTimeout: anchorTimeout}
	if protection.AnchorStore != nil {
		store.rollbackAnchorGate = make(chan struct{}, 1)
		store.rollbackAnchorGate <- struct{}{}
		store.rollbackAnchorSequence.Store(meta.CommitSequence)
		store.rollbackAnchorGeneration.Store(meta.Generation)
	}
	source := &queryReplaySource{file: file, deliveryTimeout: replayDeliveryTimeout, resourceLimits: resourceLimits}
	db := &DB{
		startedAt: time.Now(), closedCh: make(chan struct{}), collections: collections, watchers: make(map[uint64]*changeWatcher),
		token: meta.CommitSequence, durability: store, replaySource: source, querySource: store,
		databaseID: meta.DatabaseID, historyLimit: 0, resourceLimits: resourceLimits, storageLimits: selectedStorageLimits,
		recovery: finalizeRecoveryReport(RecoveryReport{
			Engine: "current", Created: recovery.Created, CommitSequenceBefore: meta.CommitSequence,
			CommitSequenceAfter: meta.CommitSequence, SelectedMetaSlot: recovery.SelectedMetaSlot,
			ChecksumValidMetaSlots: recovery.ChecksumValidMetaSlots, RootValidMetaSlots: recovery.RootValidMetaSlots,
			FallbackToOlderRoot: recovery.FallbackToOlderRoot, MainTailBytesRemoved: recovery.TrailingBytesRemoved,
			MetaRedundancyDegraded: recovery.MetaRedundancyDegraded,
			AccelerationDegraded:   recovery.FreeSpaceLoadDegraded,
		}),
		replicaReadOnly:   options.Follower,
		primaryWriteFence: options.PrimaryWriteFence,
	}
	source.onSlowConsumer = func() { db.metrics.slowConsumers.Add(1) }
	source.onResourceLimit = func() { db.metrics.resourceLimitRejections.Add(1) }
	builds, err := file.IndexBuilds()
	if err != nil {
		return fail(mapStorageError(err))
	}
	if len(builds) > 0 {
		db.indexBuildReservations = make(map[string]struct{}, len(builds))
		for _, build := range builds {
			data := db.collections[build.Collection]
			if data == nil || data.indexes == nil {
				if data == nil {
					return fail(fmt.Errorf("%w: index build collection metadata", ErrCorrupt))
				}
			} else if _, exists := data.indexes[build.Name]; exists != build.ReplaceExisting {
				return fail(fmt.Errorf("%w: index build publication state", ErrCorrupt))
			}
			reservation := indexBuildReservation(build.Collection, build.Name)
			if _, exists := db.indexBuildReservations[reservation]; exists {
				return fail(fmt.Errorf("%w: duplicate index build reservation", ErrCorrupt))
			}
			db.indexBuildReservations[reservation] = struct{}{}
		}
	}
	initialIndexBuildStats := persistentIndexBuildStats(builds)
	store.indexBuildStats.Store(&initialIndexBuildStats)
	persistentDocuments := file.StorageStats().DocumentCount
	db.initializeLogicalStats(&persistentDocuments)
	db.reactive = newReactiveHub(db)
	db.dispatcher = newChangeDispatcher(db)
	if coordinatorOptions.Enabled && !options.Follower {
		db.commitCoordinator = newCommitCoordinator(db, store, coordinatorOptions)
	}
	return db, nil
}

// loadCollections restores collection and index definitions from a storage snapshot.
func loadCollections(snapshot *storage.ReadSnapshot) (map[string]*collectionData, error) {
	records, err := snapshot.Collections()
	if err != nil {
		return nil, mapStorageError(err)
	}
	collections := make(map[string]*collectionData, len(records))
	for _, collection := range records {
		data := newCollectionData()
		indexMetas, err := snapshot.Indexes(collection.Name)
		if err != nil {
			return nil, mapStorageError(err)
		}
		for _, meta := range indexMetas {
			fields, fieldsErr := publicIndexFields(meta.FieldPath, meta.Fields)
			if !indexNamePattern.MatchString(meta.Name) || fieldsErr != nil {
				return nil, ErrCorrupt
			}
			definition := newIndexDefinition(meta.Name, fields, meta.Unique)
			data.indexes[meta.Name] = &indexState{definition: definition}
		}
		collections[collection.Name] = data
	}
	return collections, nil
}

// appendDBCommit persists a document or catalog change batch.
func (store *durableStore) appendDBCommit(ctx context.Context, db *DB, token uint64, changes []Change) error {
	_, err := store.appendDBCommitWithSystem(ctx, db, token, changes, nil, nil, nil)
	return err
}

// appendDBCommitWithSystem commits database changes and system records together.
func (store *durableStore) appendDBCommitWithSystem(
	ctx context.Context,
	db *DB,
	token uint64,
	changes []Change,
	systems []systemrecord.Mutation,
	preconditions []storage.DocumentPrecondition,
	collectionPreconditions []storage.CollectionPrecondition,
) (systemrecord.Result, error) {
	if store == nil || store.file == nil || len(changes) == 0 {
		return systemrecord.Result{}, ErrCorrupt
	}
	if err := contextError(ctx); err != nil {
		return systemrecord.Result{}, err
	}
	if err := db.validatePrimaryWriteFence(token); err != nil {
		return systemrecord.Result{}, err
	}
	var transactionID [16]byte
	if _, err := rand.Read(transactionID[:]); err != nil {
		return systemrecord.Result{}, err
	}
	db.metrics.durableCommitAttempts.Add(1)
	started := time.Now()
	var sequence uint64
	result := systemrecord.Result{}
	var err error
	if len(changes) == 1 && (changes[0].Operation == CreateIndexOperation || changes[0].Operation == ReplaceIndexOperation) {
		if len(systems) > 0 {
			return systemrecord.Result{}, ErrInvalidIndex
		}
		change := changes[0]
		if change.Index == nil {
			return systemrecord.Result{}, ErrCorrupt
		}
		budget := db.activeIndexBuild
		if budget == nil {
			budget = db.newIndexBuildBudget(db.resourceLimits)
		}
		var entries []storage.IndexEntry
		if prepared := &store.preparedIndexBuild; prepared.active && prepared.sequence == db.token &&
			prepared.collection == change.Collection && prepared.name == change.Index.Name {
			entries = prepared.entries
		} else {
			var collectErr error
			entries, collectErr = store.collectIndexEntries(ctx, db.token, change.Collection, *change.Index, budget)
			if collectErr != nil {
				db.metrics.durableRejectedTransactions.Add(1)
				return systemrecord.Result{}, collectErr
			}
		}
		sequence, err = store.file.ApplyCreateIndex(storage.CreateIndexTransaction{
			TransactionID: transactionID, Collection: change.Collection, Name: change.Index.Name,
			FieldPath: change.Index.Field, Fields: storageIndexFields(*change.Index), Unique: change.Index.Unique,
			ReplaceExisting: change.Operation == ReplaceIndexOperation, Entries: entries,
		})
	} else {
		documentTransaction, prepareErr := store.prepareDocumentTransaction(ctx, db, transactionID, changes, preconditions, collectionPreconditions)
		if prepareErr != nil {
			return systemrecord.Result{}, prepareErr
		}
		if err = contextError(ctx); err == nil {
			if len(systems) == 0 {
				sequence, err = store.file.ApplyDocumentTransaction(documentTransaction)
				result.Applied = err == nil
			} else {
				storageMutations := make([]storage.SystemRecordMutation, len(systems))
				for index, system := range systems {
					storageMutations[index] = storage.SystemRecordMutation{
						Key: append([]byte(nil), system.Key...), ExpectedExists: system.ExpectedExists,
						ExpectedHash: system.ExpectedHash, NewValue: append([]byte(nil), system.NewValue...),
						Delete: system.Delete, Unconditional: system.Unconditional,
					}
				}
				storageResult, applyErr := store.file.ApplyDocumentSystemTransaction(storage.DocumentSystemTransaction{
					DocumentTransaction: documentTransaction, SystemRecords: storageMutations,
				})
				sequence, err = storageResult.Sequence, applyErr
				result = systemrecord.Result{Applied: storageResult.Applied, Current: append([]byte(nil), storageResult.Current...)}
				if err == nil && !result.Applied {
					db.metrics.durableRejectedTransactions.Add(1)
					return result, nil
				}
			}
		}
	}
	if err != nil {
		db.metrics.durableRejectedTransactions.Add(1)
		mapped := mapStorageError(err)
		if !errors.Is(mapped, ErrDuplicateID) && !errors.Is(mapped, ErrDuplicateKey) && !errors.Is(mapped, ErrInvalidIndex) &&
			!errors.Is(mapped, ErrResourceLimit) && !errors.Is(mapped, ErrWriteConflict) {
			db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, mapped)
			return systemrecord.Result{}, db.fatalErr
		}
		return systemrecord.Result{}, mapped
	}
	if sequence != token {
		db.metrics.durableRejectedTransactions.Add(1)
		db.fatalErr = fmt.Errorf("%w: commit sequence mismatch", ErrDurability)
		return systemrecord.Result{}, db.fatalErr
	}
	if store.rollbackAnchor != nil {
		if anchorErr := store.advanceRollbackAnchor(ctx, sequence); anchorErr != nil {
			db.metrics.durableRejectedTransactions.Add(1)
			db.fatalErr = fmt.Errorf("%w: committed sequence %d but %w", ErrDurability, sequence, anchorErr)
			return systemrecord.Result{}, db.fatalErr
		}
	}
	elapsed := uint64(time.Since(started))
	db.metrics.durableCommittedTransactions.Add(1)
	db.metrics.durableCommitNanos.Add(elapsed)
	updateAtomicMax(&db.metrics.durableCommitMaxNanos, elapsed)
	result.Applied = true
	return result, nil
}

// prepareDocumentTransaction encodes an authorized document change batch for storage.
//
// Schema and index definitions come from db.collections; document state comes
// from the storage snapshot, and grouped commits reuse this conversion.
func (store *durableStore) prepareDocumentTransaction(
	ctx context.Context,
	db *DB,
	transactionID [16]byte,
	changes []Change,
	preconditions []storage.DocumentPrecondition,
	collectionPreconditions []storage.CollectionPrecondition,
) (storage.DocumentTransaction, error) {
	if store == nil || db == nil || transactionID == ([16]byte{}) || len(changes) == 0 {
		return storage.DocumentTransaction{}, ErrCorrupt
	}
	mutations := make([]storage.DocumentMutation, len(changes))
	for index, change := range changes {
		if err := contextError(ctx); err != nil {
			return storage.DocumentTransaction{}, err
		}
		operation := storage.DocumentInsert
		switch change.Operation {
		case InsertOperation:
			operation = storage.DocumentInsert
		case UpdateOperation:
			operation = storage.DocumentUpdate
		case DeleteOperation:
			operation = storage.DocumentDelete
		default:
			return storage.DocumentTransaction{}, ErrCorrupt
		}
		mutation := storage.DocumentMutation{
			Collection: change.Collection, DocumentID: [16]byte(change.DocumentID), Operation: operation,
			ChangedPaths: append([]string(nil), change.ChangedPaths...),
		}
		var err error
		if change.After != nil {
			mutation.Document, err = encodeStoredDocument(*change.After)
			if err != nil {
				return storage.DocumentTransaction{}, err
			}
		}
		data := db.collections[change.Collection]
		if data != nil {
			names := make([]string, 0, len(data.indexes))
			for name := range data.indexes {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				definition := data.indexes[name].definition
				item := storage.IndexMutation{Name: name}
				if change.Before != nil {
					item.BeforeKey, _, err = indexDocumentKey(definition, *change.Before)
					if err != nil {
						return storage.DocumentTransaction{}, err
					}
				}
				if change.After != nil {
					item.AfterKey, _, err = indexDocumentKey(definition, *change.After)
					if err != nil {
						return storage.DocumentTransaction{}, err
					}
				}
				mutation.Indexes = append(mutation.Indexes, item)
			}
		}
		mutations[index] = mutation
	}
	return storage.DocumentTransaction{TransactionID: transactionID, Preconditions: preconditions, CollectionPreconditions: collectionPreconditions, Mutations: mutations}, nil
}

// commitDurableChangeBatchesLocked commits ordered document batches without read preconditions.
//
// The caller holds db.mu and supplies authorized, immutable batches in admission order.
func (db *DB) commitDurableChangeBatchesLocked(ctx context.Context, store *durableStore, batches []ChangeBatch) error {
	return db.commitChangeBatchesWithPreconditionsLocked(ctx, store, batches, nil, nil)
}

// commitChangeBatchesWithPreconditionsLocked commits ordered batches after checking each member's read preconditions.
//
// Each member's reads are checked against the preceding logical catalog root
// within the same storage transaction, preventing stale selections from
// overwriting changes made by an earlier member.
// The caller holds db.mu; non-nil precondition lists must match the batch count.
func (db *DB) commitChangeBatchesWithPreconditionsLocked(
	ctx context.Context,
	store *durableStore,
	batches []ChangeBatch,
	preconditions [][]storage.DocumentPrecondition,
	collectionPreconditions [][]storage.CollectionPrecondition,
) error {
	if db == nil || store == nil || store.file == nil || len(batches) < 2 || len(batches) > 256 {
		return ErrCorrupt
	}
	if preconditions != nil && len(preconditions) != len(batches) {
		return ErrCorrupt
	}
	if collectionPreconditions != nil && len(collectionPreconditions) != len(batches) {
		return ErrCorrupt
	}
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	currentStore, ok := db.durability.(*durableStore)
	if !ok || currentStore != store {
		return ErrWriteConflict
	}
	if uint64(len(batches)) > ^uint64(0)-db.token {
		return ErrCorrupt
	}
	for index := range batches {
		if err := db.validatePrimaryWriteFence(db.token + uint64(index) + 1); err != nil {
			return err
		}
	}
	transactions := make([]storage.DocumentTransaction, len(batches))
	for index, batch := range batches {
		if err := contextError(ctx); err != nil {
			return err
		}
		if batch.Token != db.token+uint64(index)+1 || len(batch.Changes) == 0 {
			return ErrCorrupt
		}
		var transactionID [16]byte
		if _, err := rand.Read(transactionID[:]); err != nil {
			return err
		}
		var readSet []storage.DocumentPrecondition
		var collectionReadSet []storage.CollectionPrecondition
		if preconditions != nil {
			readSet = preconditions[index]
		}
		if collectionPreconditions != nil {
			collectionReadSet = collectionPreconditions[index]
		}
		transaction, err := store.prepareDocumentTransaction(ctx, db, transactionID, batch.Changes, readSet, collectionReadSet)
		if err != nil {
			return err
		}
		transactions[index] = transaction
	}
	db.metrics.durableCommitAttempts.Add(uint64(len(batches)))
	started := time.Now()
	sequences, err := store.file.ApplyDocumentTransactionGroup(transactions)
	if err != nil {
		mapped := mapStorageError(err)
		if !errors.Is(mapped, ErrDuplicateID) && !errors.Is(mapped, ErrDuplicateKey) && !errors.Is(mapped, ErrInvalidIndex) && !errors.Is(mapped, ErrResourceLimit) && !errors.Is(mapped, ErrWriteConflict) {
			db.metrics.durableRejectedTransactions.Add(uint64(len(batches)))
			db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, mapped)
			return db.fatalErr
		}
		// The coordinator now replays this all-or-nothing candidate through the
		// original per-request path. Count the final individual outcome there;
		// otherwise one duplicate would appear as every group member rejected.
		return mapped
	}
	if len(sequences) != len(batches) {
		db.fatalErr = fmt.Errorf("%w: grouped commit sequence count", ErrDurability)
		return db.fatalErr
	}
	for index, sequence := range sequences {
		if sequence != db.token+uint64(index)+1 {
			db.fatalErr = fmt.Errorf("%w: grouped commit sequence mismatch", ErrDurability)
			return db.fatalErr
		}
	}
	if store.rollbackAnchor != nil {
		if err := store.advanceRollbackAnchor(ctx, sequences[len(sequences)-1]); err != nil {
			db.metrics.durableRejectedTransactions.Add(uint64(len(batches)))
			db.fatalErr = fmt.Errorf("%w: grouped commit sequence %d but %w", ErrDurability, sequences[len(sequences)-1], err)
			return db.fatalErr
		}
	}
	for index, batch := range batches {
		for _, change := range batch.Changes {
			if db.collections[change.Collection] == nil {
				db.collections[change.Collection] = newCollectionData()
			}
		}
		db.token = sequences[index]
		batch.Token = sequences[index]
		db.recordLiveCommit(batch)
		db.publish(batch)
	}
	elapsed := uint64(time.Since(started))
	db.metrics.durableCommittedTransactions.Add(uint64(len(batches)))
	db.metrics.durableCommitNanos.Add(elapsed)
	updateAtomicMax(&db.metrics.durableCommitMaxNanos, elapsed)
	return nil
}

// advanceRollbackAnchor records the current durable version in the external rollback protection store.
func (store *durableStore) advanceRollbackAnchor(ctx context.Context, minimumSequence uint64) error {
	if store == nil || store.rollbackAnchor == nil || zeroDatabaseID(store.databaseID) {
		return ErrRollbackAnchor
	}
	if ctx == nil {
		return ErrRollbackAnchor
	}
	started := time.Now()
	defer func() {
		elapsed := uint64(time.Since(started))
		store.rollbackAnchorNanos.Add(elapsed)
		updateAtomicMax(&store.rollbackAnchorMaxNanos, elapsed)
	}()
	select {
	case <-ctx.Done():
		store.rollbackAnchorFailures.Add(1)
		return ctx.Err()
	case <-store.rollbackAnchorGate:
	}
	defer func() { store.rollbackAnchorGate <- struct{}{} }()
	meta := store.file.Meta()
	if meta.DatabaseID != store.databaseID || meta.CommitSequence < minimumSequence || meta.Generation == 0 {
		store.rollbackAnchorFailures.Add(1)
		return ErrRollbackAnchor
	}
	operationContext, cancel := context.WithTimeout(ctx, store.rollbackAnchorTimeout)
	defer cancel()
	retained, err := persistRollbackAnchor(operationContext, store.rollbackAnchor, RollbackAnchor{DatabaseID: store.databaseID, MinimumCommitSequence: meta.CommitSequence, MinimumGeneration: meta.Generation})
	if err != nil {
		store.rollbackAnchorFailures.Add(1)
		return err
	}
	latest := store.file.Meta()
	if retained.DatabaseID != store.databaseID || retained.MinimumCommitSequence > latest.CommitSequence || retained.MinimumGeneration > latest.Generation {
		store.rollbackAnchorFailures.Add(1)
		return fmt.Errorf("%w: anchor advanced beyond the database", ErrRollbackAnchor)
	}
	store.rollbackAnchorSequence.Store(retained.MinimumCommitSequence)
	store.rollbackAnchorGeneration.Store(retained.MinimumGeneration)
	return nil
}

// advanceRollbackAnchorLocked stops the database if rollback protection cannot record a committed version.
func (db *DB) advanceRollbackAnchorLocked(ctx context.Context, store *durableStore, minimumSequence uint64) error {
	if store == nil || store.rollbackAnchor == nil {
		return nil
	}
	if err := store.advanceRollbackAnchor(ctx, minimumSequence); err != nil {
		meta := store.file.Meta()
		db.fatalErr = fmt.Errorf("%w: committed generation %d sequence %d but rollback anchor failed: %w", ErrDurability, meta.Generation, meta.CommitSequence, err)
		return db.fatalErr
	}
	return nil
}

// advanceRollbackAnchor checks database health under its lock before updating rollback protection.
func (db *DB) advanceRollbackAnchor(ctx context.Context, store *durableStore, minimumSequence uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	return db.advanceRollbackAnchorLocked(ctx, store, minimumSequence)
}

// collectIndexEntries builds index entries from a collection snapshot at the requested commit.
//
// This can run outside db.mu; a changed sequence is an optimistic conflict.
// Document contents come from storage rather than the in-memory collection metadata.
func (store *durableStore) collectIndexEntries(ctx context.Context, sequence uint64, collection string, definition IndexDefinition, budget *indexBuildBudget) (_ []storage.IndexEntry, resultErr error) {
	if store == nil || store.file == nil {
		return nil, ErrClosed
	}
	snapshot, err := store.file.OpenSnapshot()
	if err != nil {
		return nil, mapStorageError(err)
	}
	defer func() {
		if closeErr := mapStorageError(snapshot.Close()); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
	}()
	if snapshot.Sequence() != sequence {
		return nil, ErrWriteConflict
	}
	if store.testIndexBuildSnapshotHook != nil {
		store.testIndexBuildSnapshotHook()
	}
	iterator, err := snapshot.OpenCollectionIterator(collection, nil, nil, 0)
	if err != nil {
		return nil, mapStorageError(err)
	}
	defer func() {
		if closeErr := mapStorageError(iterator.Close()); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
	}()
	entries := make([]storage.IndexEntry, 0)
	fields := indexDefinitionFields(definition)
	paths := make([][][]byte, len(fields))
	for index, field := range fields {
		paths[index] = indexBuildPath(field.Field)
	}
	for iterator.Next() {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		record := iterator.Record()
		if record.InsertionPosition == 0 {
			return nil, fmt.Errorf("%w: stored document", ErrCorrupt)
		}
		values := make([]Value, len(fields))
		present := 0
		for index, path := range paths {
			value, exists, scalar, decodeErr := projectStoredDocumentScalar(record.Document, path, DocumentID(record.DocumentID))
			if decodeErr != nil {
				return nil, fmt.Errorf("%w: stored document", ErrCorrupt)
			}
			if !exists {
				break
			}
			if !scalar {
				return nil, fmt.Errorf("%w: indexed field is not scalar", ErrInvalidIndex)
			}
			values[index] = value
			present++
		}
		if present == 0 {
			continue
		}
		var key []byte
		var keyErr error
		if usesCompoundIndexCodec(definition) {
			if present < len(fields) {
				key, keyErr = encodeCompoundPartialIndexKey(values[:present], fields[:present], DocumentID(record.DocumentID))
			} else {
				key, keyErr = encodeCompoundIndexKey(values, fields)
			}
		} else {
			key, keyErr = encodeIndexKey(values[0])
		}
		if keyErr != nil {
			return nil, fmt.Errorf("%w: indexed field is not scalar", ErrInvalidIndex)
		}
		if err := budget.add(key); err != nil {
			return nil, err
		}
		entries = append(entries, storage.IndexEntry{Key: key, InsertionPosition: record.InsertionPosition, DocumentID: record.DocumentID})
	}
	if err := iterator.Err(); err != nil {
		return nil, mapStorageError(err)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return entries, nil
}

// storageIndexFields converts an index definition to stored field descriptions.
func storageIndexFields(definition IndexDefinition) []storage.IndexField {
	fields := indexDefinitionFields(definition)
	result := make([]storage.IndexField, len(fields))
	for index, field := range fields {
		result[index] = storage.IndexField{Path: field.Field, Direction: int8(field.Order)}
	}
	return result
}

// publicIndexFields validates stored index fields and converts them to database field descriptions.
func publicIndexFields(fieldPath string, fields []storage.IndexField) ([]IndexField, error) {
	if len(fields) == 0 {
		fields = []storage.IndexField{{Path: fieldPath, Direction: 1}}
	}
	result := make([]IndexField, len(fields))
	for index, field := range fields {
		result[index] = IndexField{Field: field.Path, Order: int(field.Direction)}
	}
	definition, err := validateIndexDefinition("validation", result, IndexOptions{})
	if err != nil {
		return nil, ErrCorrupt
	}
	return cloneIndexFields(definition.Fields), nil
}

// storageDBStats reports storage usage, cache activity, and rollback protection status.
func (store *durableStore) storageDBStats() StorageStats {
	stats := StorageStats{Engine: "current"}
	if store == nil || store.file == nil {
		return stats
	}
	physical := store.file.StorageStats()
	stats.RollbackProtected = store.rollbackAnchor != nil
	stats.RollbackAnchorSequence = store.rollbackAnchorSequence.Load()
	stats.RollbackAnchorGeneration = store.rollbackAnchorGeneration.Load()
	stats.RollbackAnchorFailures = store.rollbackAnchorFailures.Load()
	stats.RollbackAnchorTimeout = store.rollbackAnchorTimeout
	stats.RollbackAnchorNanos = store.rollbackAnchorNanos.Load()
	stats.RollbackAnchorMaxLatency = time.Duration(store.rollbackAnchorMaxNanos.Load())
	if provider, ok := store.rollbackAnchor.(RollbackAnchorStatusProvider); ok {
		stats.RollbackAnchorStore = provider.RollbackAnchorStatus()
	}
	stats.PageSize = physical.PageSize
	stats.Generation = physical.Generation
	stats.PhysicalPages = physical.PhysicalPages
	stats.CommitSequence = physical.CommitSequence
	stats.OldestRetainedSequence = physical.OldestRetainedSequence
	stats.RetainedCommits = physical.RetainedCommits
	stats.CommitRetentionMax = physical.CommitRetentionMax
	stats.CommitRetentionOverage = physical.CommitRetentionOverage
	stats.RetainedCommitBytes = physical.RetainedCommitBytes
	stats.CommitRetentionMaxBytes = physical.CommitRetentionMaxBytes
	stats.CommitRetentionByteOverage = physical.CommitRetentionByteOverage
	stats.RetentionPrunedCommits = physical.RetentionPrunedCommits
	stats.RetentionPressureEvents = physical.RetentionPressureEvents
	stats.RetentionPressure = physical.RetentionPressure
	stats.StorageUsedBytes = physical.StorageUsedBytes
	stats.StorageMaxBytes = physical.StorageMaxBytes
	stats.StorageByteOverage = physical.StorageByteOverage
	stats.StorageLimitRejections = physical.StorageLimitRejections
	stats.StorageQuotaExhausted = physical.StorageQuotaExhausted
	stats.ActiveReaders = physical.ActiveReaders
	stats.ActiveReplayLeases = physical.ActiveReplayLeases
	stats.Documents = physical.DocumentCount
	stats.Collections = physical.CollectionCount
	stats.ReusablePages = physical.ReusablePages
	stats.TreeSplits = physical.TreeSplits
	stats.TreeMerges = physical.TreeMerges
	stats.PersistentFreeSpace = physical.PersistentFreeSpace
	stats.FreeSpaceLoads = physical.FreeSpaceLoads
	stats.FreeSpaceLoadFailures = physical.FreeSpaceLoadFailures
	stats.FreeSpacePublishes = physical.FreeSpacePublishes
	stats.FreeSpaceCandidateChecks = physical.FreeSpaceCandidateChecks
	stats.PageCache = PageCacheStats{
		CapacityPages: physical.PageCache.CapacityPages,
		ResidentPages: physical.PageCache.ResidentPages,
		Hits:          physical.PageCache.Hits,
		Misses:        physical.PageCache.Misses,
		Evictions:     physical.PageCache.Evictions,
	}
	if store.documents != nil {
		stats.DocumentCache = store.documents.stats()
	}
	return stats
}

// syncDB reports database health because each successful commit is already synced.
func (store *durableStore) syncDB(db *DB) error {
	if db.fatalErr != nil {
		return db.fatalErr
	}
	return nil // every commit is already data+meta fsynced
}

// closeDB closes the storage file and returns any database or close error.
func (store *durableStore) closeDB(db *DB) error {
	if store == nil || store.file == nil {
		return db.fatalErr
	}
	err := store.file.Close()
	store.file = nil
	return errors.Join(db.fatalErr, err)
}

// OpenQueryReplay opens a query change stream after the supplied commit token.
func (db *DB) OpenQueryReplay(ctx context.Context, collection string, query QuerySpec, afterToken uint64, buffer int) (*QueryReplaySubscription, error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if db.replaySource == nil {
		return nil, ErrHistoryLost
	}
	return db.replaySource.OpenQueryReplay(ctx, collection, query, afterToken, buffer)
}

// mapStorageError translates storage errors into database errors.
func mapStorageError(err error) error {
	switch {
	case errors.Is(err, storage.ErrLocked):
		return fmt.Errorf("%w: %v", ErrDatabaseLocked, err)
	case errors.Is(err, storage.ErrStaleSnapshot):
		return fmt.Errorf("%w: %v", ErrRollbackDetected, err)
	case errors.Is(err, storage.ErrDatabaseIdentity):
		return fmt.Errorf("%w: %v", ErrDatabaseIdentity, err)
	case errors.Is(err, storage.ErrInsecureFileMode):
		return ErrInsecureFileMode
	case errors.Is(err, storage.ErrUnsupportedFormat), errors.Is(err, storage.ErrUnsupportedFeature):
		return fmt.Errorf("%w: %v", ErrUnsupportedFormat, err)
	case errors.Is(err, storage.ErrReclamationConflict):
		return fmt.Errorf("%w: %v", ErrReclamationConflict, err)
	case errors.Is(err, storage.ErrUniqueConflict):
		return ErrDuplicateKey
	case errors.Is(err, storage.ErrDocumentExists):
		return ErrDuplicateID
	case errors.Is(err, storage.ErrIndexExists), errors.Is(err, storage.ErrIndexNotFound):
		return ErrInvalidIndex
	case errors.Is(err, storage.ErrIndexBuildExists):
		return ErrIndexBuildExists
	case errors.Is(err, storage.ErrIndexBuildNotFound):
		return ErrIndexBuildNotFound
	case errors.Is(err, storage.ErrIndexBuildState):
		return ErrWriteConflict
	case errors.Is(err, storage.ErrDocumentConflict):
		return ErrWriteConflict
	case errors.Is(err, storage.ErrIndexBuildHistoryLost):
		return ErrHistoryLost
	case errors.Is(err, storage.ErrHistoryLost):
		return ErrHistoryLost
	case errors.Is(err, storage.ErrDurableConsumerExists):
		return ErrDurableConsumerExists
	case errors.Is(err, storage.ErrDurableConsumerNotFound):
		return ErrDurableConsumerNotFound
	case errors.Is(err, storage.ErrIndexKeyTooLarge):
		return ErrInvalidIndex
	case errors.Is(err, storage.ErrStorageLimit):
		return fmt.Errorf("%w: physical storage quota", ErrResourceLimit)
	case errors.Is(err, storage.ErrInvalidStorageLimit):
		return ErrInvalidResourceLimits
	case errors.Is(err, storage.ErrCorrupt):
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	case errors.Is(err, storage.ErrRecoveryRequired):
		return ErrRecoveryRequired
	default:
		return err
	}
}
