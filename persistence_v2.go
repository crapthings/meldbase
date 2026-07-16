package meldbase

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

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
	"github.com/crapthings/meldbase/internal/systemrecord"
)

// v2DurableStore adapts the page/Commit Log format to the public database API.
type v2DurableStore struct {
	file               *storagev2.File
	documents          *v2DocumentCache
	path               string
	compactMu          sync.Mutex
	preparedIndexBuild struct {
		active     bool
		sequence   uint64
		collection string
		name       string
		entries    []storagev2.IndexEntry
	}
	testIndexBuildSnapshotHook        func()
	testPersistentIndexBuildReadyHook func()
	testPersistentIndexBuildBatchHook func(context.Context, IndexBuildID)
	indexBuildStatsMu                 sync.Mutex
	indexBuildStats                   atomic.Pointer[IndexBuildStats]
}

type indexBuildStatsBackend interface {
	indexBuildDBStats() IndexBuildStats
}

func (store *v2DurableStore) indexBuildDBStats() IndexBuildStats {
	if store == nil {
		return IndexBuildStats{}
	}
	stats := store.indexBuildStats.Load()
	if stats == nil {
		return IndexBuildStats{}
	}
	return *stats
}

func persistentIndexBuildStats(builds []storagev2.IndexBuildMeta) IndexBuildStats {
	stats := IndexBuildStats{}
	stats.Persistent = uint64(len(builds))
	for _, build := range builds {
		stats.PersistentEntries += build.EntryCount
		stats.PersistentBytes += build.CanonicalBytes
		switch build.Phase {
		case storagev2.IndexBuildScan:
			stats.Scanning++
		case storagev2.IndexBuildCatchUp:
			stats.CatchingUp++
		case storagev2.IndexBuildReady:
			stats.Ready++
		case storagev2.IndexBuildFailed:
			stats.PersistentFailed++
		}
		if build.Phase != storagev2.IndexBuildFailed &&
			(stats.oldestActiveAppliedSequence == 0 || build.AppliedSequence < stats.oldestActiveAppliedSequence) {
			stats.oldestActiveAppliedSequence = build.AppliedSequence
		}
	}
	return stats
}

func (store *v2DurableStore) refreshIndexBuildStats() {
	if store == nil || store.file == nil {
		return
	}
	store.indexBuildStatsMu.Lock()
	defer store.indexBuildStatsMu.Unlock()
	builds, err := store.file.IndexBuilds()
	if err != nil {
		return
	}
	stats := persistentIndexBuildStats(builds)
	store.indexBuildStats.Store(&stats)
}

// OpenV2 explicitly creates or opens Storage V2. It never interprets or
// migrates a V1 file. Open performs read-only format selection when callers
// want to support both generations.
func OpenV2(path string) (*DB, error) {
	return OpenV2WithOptions(path, V2Options{})
}

func OpenV2WithOptions(path string, options V2Options) (*DB, error) {
	if err := validateRecoveryMode(options.Recovery); err != nil {
		return nil, err
	}
	resourceLimits, err := normalizeResourceLimits(options.ResourceLimits)
	if err != nil {
		return nil, err
	}
	file, meta, recovery, err := storagev2.OpenWithOptions(path, storagev2.OpenOptions{
		RequireClean:              options.Recovery == RecoveryRequireClean,
		CommitRetentionMaxCommits: options.CommitRetention.MaxCommits,
		CommitRetentionMaxBytes:   options.CommitRetention.MaxBytes,
		MaxFileBytes:              options.StorageLimits.MaxFileBytes,
	})
	if err != nil {
		return nil, mapStorageV2Error(err)
	}
	selectedStorageLimits := V2StorageLimits{MaxFileBytes: file.StorageStats().StorageMaxBytes}
	fail := func(err error) (*DB, error) {
		_ = file.Close()
		return nil, err
	}
	snapshot, stream, err := file.OpenSnapshotAndStream()
	if err != nil {
		return fail(mapStorageV2Error(err))
	}
	collections, err := loadV2Collections(snapshot)
	closeErr := errors.Join(snapshot.Close(), stream.Close())
	if err != nil {
		return fail(err)
	}
	if closeErr != nil {
		return fail(mapStorageV2Error(closeErr))
	}
	absolutePath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return fail(err)
	}
	store := &v2DurableStore{file: file, documents: newV2DocumentCache(defaultV2DocumentCacheEntries, defaultV2DocumentCacheBytes), path: absolutePath}
	source := &v2QueryReplaySource{file: file}
	db := &DB{
		startedAt: time.Now(), closedCh: make(chan struct{}), collections: collections, watchers: make(map[uint64]*changeWatcher),
		token: meta.CommitSequence, durability: store, replaySource: source, querySource: store,
		databaseID: meta.DatabaseID, historyLimit: 0, resourceLimits: resourceLimits, v2StorageLimits: selectedStorageLimits,
		recovery: finalizeRecoveryReport(RecoveryReport{
			Engine: "v2", Created: recovery.Created, CommitSequenceBefore: meta.CommitSequence,
			CommitSequenceAfter: meta.CommitSequence, SelectedMetaSlot: recovery.SelectedMetaSlot,
			ChecksumValidMetaSlots: recovery.ChecksumValidMetaSlots, RootValidMetaSlots: recovery.RootValidMetaSlots,
			FallbackToOlderRoot: recovery.FallbackToOlderRoot, MainTailBytesRemoved: recovery.TrailingBytesRemoved,
			MetaRedundancyDegraded: recovery.MetaRedundancyDegraded,
			AccelerationDegraded:   recovery.FreeSpaceLoadDegraded,
		}),
	}
	builds, err := file.IndexBuilds()
	if err != nil {
		return fail(mapStorageV2Error(err))
	}
	if len(builds) > 0 {
		db.indexBuildReservations = make(map[string]struct{}, len(builds))
		for _, build := range builds {
			data := db.collections[build.Collection]
			if data == nil || data.indexes == nil {
				if data == nil {
					return fail(fmt.Errorf("%w: index build collection metadata", ErrCorrupt))
				}
			} else if _, exists := data.indexes[build.Name]; exists {
				return fail(fmt.Errorf("%w: index build is already published", ErrCorrupt))
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
	return db, nil
}

func (store *v2DurableStore) openQuerySnapshot() (queryStorageSnapshot, error) {
	if store == nil || store.file == nil {
		return nil, ErrClosed
	}
	snapshot, err := store.file.OpenSnapshot()
	if err != nil {
		return nil, mapStorageV2Error(err)
	}
	return &v2QueryStorageSnapshot{snapshot: snapshot, documents: store.documents}, nil
}

type v2QueryStorageSnapshot struct {
	snapshot  *storagev2.ReadSnapshot
	documents *v2DocumentCache
}

func (snapshot *v2QueryStorageSnapshot) Sequence() uint64 {
	if snapshot == nil || snapshot.snapshot == nil {
		return 0
	}
	return snapshot.snapshot.Sequence()
}

func (snapshot *v2QueryStorageSnapshot) GetDocumentRecord(collection string, id DocumentID) (queryStorageDocument, bool, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return queryStorageDocument{}, false, ErrClosed
	}
	record, exists, err := snapshot.snapshot.GetDocumentRecord(collection, [16]byte(id))
	if err != nil {
		return queryStorageDocument{}, false, mapStorageV2Error(err)
	}
	if !exists {
		snapshot.documents.remove(collection, id)
		return queryStorageDocument{}, false, nil
	}
	document, err := snapshot.documents.decode(collection, id, record.Document)
	if err != nil {
		return queryStorageDocument{}, false, err
	}
	return queryStorageDocument{
		ID: DocumentID(record.DocumentID), Position: record.InsertionPosition,
		Encoded: record.Document, Decoded: document,
	}, true, nil
}

func (snapshot *v2QueryStorageSnapshot) Indexes(collection string) ([]queryStorageIndex, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil, ErrClosed
	}
	metas, err := snapshot.snapshot.Indexes(collection)
	if err != nil {
		return nil, mapStorageV2Error(err)
	}
	result := make([]queryStorageIndex, len(metas))
	for index, meta := range metas {
		fields, err := publicV2IndexFields(meta.FieldPath, meta.Fields)
		if err != nil {
			return nil, err
		}
		result[index] = queryStorageIndex{Name: meta.Name, Field: fields[0].Field, Fields: fields, Unique: meta.Unique}
	}
	return result, nil
}

func (snapshot *v2QueryStorageSnapshot) OpenIndexIterator(collection, index string, start, end []byte, limit int) (queryStorageIndexIterator, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil, ErrClosed
	}
	iterator, err := snapshot.snapshot.OpenIndexIterator(collection, index, start, end, limit)
	if err != nil {
		return nil, mapStorageV2Error(err)
	}
	return &v2QueryStorageIndexIterator{iterator: iterator}, nil
}

func (snapshot *v2QueryStorageSnapshot) OpenCollectionIterator(collection string) (queryStorageDocumentIterator, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil, ErrClosed
	}
	iterator, err := snapshot.snapshot.OpenInsertionOrderIterator(collection, nil, nil, 0)
	if err != nil {
		return nil, mapStorageV2Error(err)
	}
	return &v2QueryStorageDocumentIterator{iterator: iterator, collection: collection, documents: snapshot.documents}, nil
}

func (snapshot *v2QueryStorageSnapshot) Close() error {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil
	}
	err := snapshot.snapshot.Close()
	snapshot.snapshot = nil
	return mapStorageV2Error(err)
}

type v2QueryStorageIndexIterator struct {
	iterator *storagev2.IndexIterator
}

func (iterator *v2QueryStorageIndexIterator) Next() bool {
	return iterator != nil && iterator.iterator != nil && iterator.iterator.Next()
}

func (iterator *v2QueryStorageIndexIterator) Entry() queryStorageIndexEntry {
	if iterator == nil || iterator.iterator == nil {
		return queryStorageIndexEntry{}
	}
	entry := iterator.iterator.Entry()
	return queryStorageIndexEntry{Key: entry.Key, Position: entry.InsertionPosition, ID: DocumentID(entry.DocumentID)}
}

func (iterator *v2QueryStorageIndexIterator) Err() error {
	if iterator == nil || iterator.iterator == nil {
		return ErrCorrupt
	}
	return mapStorageV2Error(iterator.iterator.Err())
}

func (iterator *v2QueryStorageIndexIterator) Close() error {
	if iterator == nil || iterator.iterator == nil {
		return nil
	}
	err := iterator.iterator.Close()
	iterator.iterator = nil
	return mapStorageV2Error(err)
}

type v2QueryStorageDocumentIterator struct {
	iterator   *storagev2.DocumentIterator
	collection string
	documents  *v2DocumentCache
	err        error
}

func (iterator *v2QueryStorageDocumentIterator) Next() bool {
	return iterator != nil && iterator.iterator != nil && iterator.err == nil && iterator.iterator.Next()
}

func (iterator *v2QueryStorageDocumentIterator) Record() queryStorageDocument {
	if iterator == nil || iterator.iterator == nil {
		return queryStorageDocument{}
	}
	record := iterator.iterator.Record()
	id := DocumentID(record.DocumentID)
	document, err := iterator.documents.decode(iterator.collection, id, record.Document)
	if err != nil {
		iterator.err = err
		return queryStorageDocument{}
	}
	return queryStorageDocument{ID: id, Position: record.InsertionPosition, Encoded: record.Document, Decoded: document}
}

func (iterator *v2QueryStorageDocumentIterator) Err() error {
	if iterator == nil || iterator.iterator == nil {
		return ErrCorrupt
	}
	if iterator.err != nil {
		return iterator.err
	}
	return mapStorageV2Error(iterator.iterator.Err())
}

func (iterator *v2QueryStorageDocumentIterator) Close() error {
	if iterator == nil || iterator.iterator == nil {
		return nil
	}
	err := iterator.iterator.Close()
	iterator.iterator = nil
	return mapStorageV2Error(err)
}

func loadV2Collections(snapshot *storagev2.ReadSnapshot) (map[string]*collectionData, error) {
	records, err := snapshot.Collections()
	if err != nil {
		return nil, mapStorageV2Error(err)
	}
	collections := make(map[string]*collectionData, len(records))
	for _, collection := range records {
		data := newCollectionData()
		indexMetas, err := snapshot.Indexes(collection.Name)
		if err != nil {
			return nil, mapStorageV2Error(err)
		}
		for _, meta := range indexMetas {
			fields, fieldsErr := publicV2IndexFields(meta.FieldPath, meta.Fields)
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

func (store *v2DurableStore) appendDBCommit(ctx context.Context, db *DB, token uint64, changes []Change) error {
	_, err := store.appendDBCommitWithSystem(ctx, db, token, changes, nil, nil)
	return err
}

func (store *v2DurableStore) appendDBCommitWithSystem(
	ctx context.Context,
	db *DB,
	token uint64,
	changes []Change,
	systems []systemrecord.Mutation,
	preconditions []storagev2.DocumentPrecondition,
) (systemrecord.Result, error) {
	if store == nil || store.file == nil || len(changes) == 0 {
		return systemrecord.Result{}, ErrCorrupt
	}
	if err := contextError(ctx); err != nil {
		return systemrecord.Result{}, err
	}
	var transactionID [16]byte
	if _, err := rand.Read(transactionID[:]); err != nil {
		return systemrecord.Result{}, err
	}
	db.metrics.v2CommitAttempts.Add(1)
	started := time.Now()
	var sequence uint64
	result := systemrecord.Result{}
	var err error
	if len(changes) == 1 && changes[0].Operation == CreateIndexOperation {
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
		var entries []storagev2.IndexEntry
		if prepared := &store.preparedIndexBuild; prepared.active && prepared.sequence == db.token &&
			prepared.collection == change.Collection && prepared.name == change.Index.Name {
			entries = prepared.entries
		} else {
			var collectErr error
			entries, collectErr = store.collectIndexEntries(ctx, db.token, change.Collection, *change.Index, budget)
			if collectErr != nil {
				db.metrics.v2RejectedTransactions.Add(1)
				return systemrecord.Result{}, collectErr
			}
		}
		sequence, err = store.file.ApplyCreateIndex(storagev2.CreateIndexTransaction{
			TransactionID: transactionID, Collection: change.Collection, Name: change.Index.Name,
			FieldPath: change.Index.Field, Fields: storageV2IndexFields(*change.Index), Unique: change.Index.Unique, Entries: entries,
		})
	} else {
		mutations := make([]storagev2.DocumentMutation, len(changes))
		for index, change := range changes {
			if err := contextError(ctx); err != nil {
				return systemrecord.Result{}, err
			}
			operation := storagev2.DocumentInsert
			switch change.Operation {
			case InsertOperation:
				operation = storagev2.DocumentInsert
			case UpdateOperation:
				operation = storagev2.DocumentUpdate
			case DeleteOperation:
				operation = storagev2.DocumentDelete
			default:
				return systemrecord.Result{}, ErrCorrupt
			}
			mutation := storagev2.DocumentMutation{Collection: change.Collection, DocumentID: [16]byte(change.DocumentID), Operation: operation}
			if change.After != nil {
				mutation.Document, err = encodeStoredDocument(*change.After)
				if err != nil {
					return systemrecord.Result{}, err
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
					item := storagev2.IndexMutation{Name: name}
					if change.Before != nil {
						item.BeforeKey, _, err = indexDocumentKey(definition, *change.Before)
						if err != nil {
							return systemrecord.Result{}, err
						}
					}
					if change.After != nil {
						item.AfterKey, _, err = indexDocumentKey(definition, *change.After)
						if err != nil {
							return systemrecord.Result{}, err
						}
					}
					mutation.Indexes = append(mutation.Indexes, item)
				}
			}
			mutations[index] = mutation
		}
		if err = contextError(ctx); err == nil {
			documentTransaction := storagev2.DocumentTransaction{
				TransactionID: transactionID, Preconditions: preconditions, Mutations: mutations,
			}
			if len(systems) == 0 {
				sequence, err = store.file.ApplyDocumentTransaction(documentTransaction)
				result.Applied = err == nil
			} else {
				storageMutations := make([]storagev2.SystemRecordMutation, len(systems))
				for index, system := range systems {
					storageMutations[index] = storagev2.SystemRecordMutation{
						Key: append([]byte(nil), system.Key...), ExpectedExists: system.ExpectedExists,
						ExpectedHash: system.ExpectedHash, NewValue: append([]byte(nil), system.NewValue...),
						Delete: system.Delete, Unconditional: system.Unconditional,
					}
				}
				storageResult, applyErr := store.file.ApplyDocumentSystemTransaction(storagev2.DocumentSystemTransaction{
					DocumentTransaction: documentTransaction, SystemRecords: storageMutations,
				})
				sequence, err = storageResult.Sequence, applyErr
				result = systemrecord.Result{Applied: storageResult.Applied, Current: append([]byte(nil), storageResult.Current...)}
				if err == nil && !result.Applied {
					db.metrics.v2RejectedTransactions.Add(1)
					return result, nil
				}
			}
		}
	}
	if err != nil {
		db.metrics.v2RejectedTransactions.Add(1)
		mapped := mapStorageV2Error(err)
		if !errors.Is(mapped, ErrDuplicateKey) && !errors.Is(mapped, ErrInvalidIndex) &&
			!errors.Is(mapped, ErrResourceLimit) && !errors.Is(mapped, ErrWriteConflict) {
			db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, mapped)
			return systemrecord.Result{}, db.fatalErr
		}
		return systemrecord.Result{}, mapped
	}
	if sequence != token {
		db.metrics.v2RejectedTransactions.Add(1)
		db.fatalErr = fmt.Errorf("%w: V2 commit sequence mismatch", ErrDurability)
		return systemrecord.Result{}, db.fatalErr
	}
	elapsed := uint64(time.Since(started))
	db.metrics.v2CommittedTransactions.Add(1)
	db.metrics.v2CommitNanos.Add(elapsed)
	updateAtomicMax(&db.metrics.v2CommitMaxNanos, elapsed)
	result.Applied = true
	return result, nil
}

// collectIndexEntries streams the Primary B+Tree at one exact sequence. It may
// run outside db.mu; a changed current sequence is an optimistic conflict, not
// a durability fault. It deliberately does not consult db.collections: V2 keeps only
// collection/index metadata in memory and treats the immutable storage roots as
// the source of truth for document contents.
func (store *v2DurableStore) collectIndexEntries(ctx context.Context, sequence uint64, collection string, definition IndexDefinition, budget *indexBuildBudget) (_ []storagev2.IndexEntry, resultErr error) {
	if store == nil || store.file == nil {
		return nil, ErrClosed
	}
	snapshot, err := store.file.OpenSnapshot()
	if err != nil {
		return nil, mapStorageV2Error(err)
	}
	defer func() {
		if closeErr := mapStorageV2Error(snapshot.Close()); resultErr == nil && closeErr != nil {
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
		return nil, mapStorageV2Error(err)
	}
	defer func() {
		if closeErr := mapStorageV2Error(iterator.Close()); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
	}()
	entries := make([]storagev2.IndexEntry, 0)
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
			return nil, fmt.Errorf("%w: V2 stored document", ErrCorrupt)
		}
		values := make([]Value, len(fields))
		present := 0
		for index, path := range paths {
			value, exists, scalar, decodeErr := projectStoredDocumentScalar(record.Document, path, DocumentID(record.DocumentID))
			if decodeErr != nil {
				return nil, fmt.Errorf("%w: V2 stored document", ErrCorrupt)
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
		entries = append(entries, storagev2.IndexEntry{Key: key, InsertionPosition: record.InsertionPosition, DocumentID: record.DocumentID})
	}
	if err := iterator.Err(); err != nil {
		return nil, mapStorageV2Error(err)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return entries, nil
}

func storageV2IndexFields(definition IndexDefinition) []storagev2.IndexField {
	fields := indexDefinitionFields(definition)
	result := make([]storagev2.IndexField, len(fields))
	for index, field := range fields {
		result[index] = storagev2.IndexField{Path: field.Field, Direction: int8(field.Order)}
	}
	return result
}

func publicV2IndexFields(fieldPath string, fields []storagev2.IndexField) ([]IndexField, error) {
	if len(fields) == 0 {
		fields = []storagev2.IndexField{{Path: fieldPath, Direction: 1}}
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

func (store *v2DurableStore) storageDBStats() StorageStats {
	stats := StorageStats{Engine: "v2"}
	if store == nil || store.file == nil {
		return stats
	}
	physical := store.file.StorageStats()
	stats.PageSize = physical.PageSize
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

func (store *v2DurableStore) syncDB(db *DB) error {
	if db.fatalErr != nil {
		return db.fatalErr
	}
	return nil // every V2 commit is already data+meta fsynced
}

func (store *v2DurableStore) closeDB(db *DB) error {
	if store == nil || store.file == nil {
		return db.fatalErr
	}
	err := store.file.Close()
	store.file = nil
	return errors.Join(db.fatalErr, err)
}

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

func mapStorageV2Error(err error) error {
	switch {
	case errors.Is(err, storagev2.ErrLocked):
		return fmt.Errorf("%w: %v", ErrDatabaseLocked, err)
	case errors.Is(err, storagev2.ErrUnsupportedFormat), errors.Is(err, storagev2.ErrUnsupportedFeature):
		return fmt.Errorf("%w: %v", ErrUnsupportedFormat, err)
	case errors.Is(err, storagev2.ErrReclamationConflict):
		return fmt.Errorf("%w: %v", ErrReclamationConflict, err)
	case errors.Is(err, storagev2.ErrUniqueConflict):
		return ErrDuplicateKey
	case errors.Is(err, storagev2.ErrIndexExists):
		return ErrInvalidIndex
	case errors.Is(err, storagev2.ErrIndexBuildExists):
		return ErrIndexBuildExists
	case errors.Is(err, storagev2.ErrIndexBuildNotFound):
		return ErrIndexBuildNotFound
	case errors.Is(err, storagev2.ErrIndexBuildState):
		return ErrWriteConflict
	case errors.Is(err, storagev2.ErrDocumentConflict):
		return ErrWriteConflict
	case errors.Is(err, storagev2.ErrIndexBuildHistoryLost):
		return ErrHistoryLost
	case errors.Is(err, storagev2.ErrIndexKeyTooLarge):
		return ErrInvalidIndex
	case errors.Is(err, storagev2.ErrStorageLimit):
		return fmt.Errorf("%w: V2 physical storage quota", ErrResourceLimit)
	case errors.Is(err, storagev2.ErrInvalidStorageLimit):
		return ErrInvalidResourceLimits
	case errors.Is(err, storagev2.ErrCorrupt):
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	case errors.Is(err, storagev2.ErrRecoveryRequired):
		return ErrRecoveryRequired
	default:
		return err
	}
}
