package meldbase

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	storagefile "github.com/crapthings/meldbase/internal/storage"
	"github.com/crapthings/meldbase/internal/wal"
)

type durableStore struct {
	pages            *storagefile.File
	log              *wal.Log
	path             string
	checkpointPolicy V1CheckpointPolicy
	checkpointFault  func(v1CheckpointFaultPoint) error
	commitsSinceSync uint64
}

const (
	defaultV1CheckpointWALBytes   int64  = 64 << 20
	defaultV1CheckpointWALCommits uint64 = 10_000
)

// V1CheckpointPolicy bounds the legacy sidecar WAL. Either enabled threshold
// triggers a synchronous physical checkpoint after the triggering logical
// commit is already durable. Zero values select production defaults.
type V1CheckpointPolicy struct {
	MaxWALBytes   int64
	MaxWALCommits uint64
	Disabled      bool
}

// V1Options configures the explicitly selected legacy V1 storage engine.
// Storage V2 does not use this policy because every V2 commit publishes a COW
// database root and inactive Meta page atomically.
type V1Options struct {
	Checkpoint     V1CheckpointPolicy
	Recovery       RecoveryMode
	ResourceLimits ResourceLimits
}

type v1CheckpointFaultPoint uint8

const (
	v1CheckpointBeforePages v1CheckpointFaultPoint = 1 + iota
	v1CheckpointAfterPages
	v1CheckpointBeforeWALReset
	v1CheckpointAfterWALReset
)

func (s *durableStore) storageDBStats() StorageStats {
	return StorageStats{Engine: "v1"}
}

type durabilityBackend interface {
	appendDBCommit(ctx context.Context, db *DB, token uint64, changes []Change) error
	syncDB(db *DB) error
	closeDB(db *DB) error
}

type storageStatsBackend interface {
	storageDBStats() StorageStats
}

// Open opens an existing V1 or V2 database after read-only format detection.
// A missing or zero-length path creates V2, the current default format. It
// never migrates or rewrites an existing V1 database implicitly.
func Open(path string) (*DB, error) {
	return OpenWithOptions(path, OpenOptions{})
}

func OpenWithOptions(path string, options OpenOptions) (*DB, error) {
	if err := validateRecoveryMode(options.Recovery); err != nil {
		return nil, err
	}
	format, err := DetectStorageFormat(path)
	if err != nil {
		return nil, err
	}
	switch format {
	case StorageFormatV1:
		return OpenV1WithOptions(path, V1Options{Checkpoint: options.V1Checkpoint, Recovery: options.Recovery, ResourceLimits: options.ResourceLimits})
	case StorageFormatV2:
		return OpenV2WithOptions(path, V2Options{Recovery: options.Recovery, CommitRetention: options.V2CommitRetention, ResourceLimits: options.ResourceLimits, StorageLimits: options.V2StorageLimits})
	case StorageFormatUnknown:
		// A missing main file beside any V1 WAL may represent an incomplete
		// restore or operator mistake. Never create a V2 main file that would
		// silently strand that sidecar.
		if _, statErr := os.Stat(path + ".wal"); statErr == nil {
			return nil, fmt.Errorf("%w: legacy WAL exists without a V1 database", ErrCorrupt)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return nil, statErr
		}
		return OpenV2WithOptions(path, V2Options{Recovery: options.Recovery, CommitRetention: options.V2CommitRetention, ResourceLimits: options.ResourceLimits, StorageLimits: options.V2StorageLimits})
	default:
		return nil, ErrCorrupt
	}
}

// OpenV1 explicitly creates or opens the legacy page-checkpoint plus WAL
// format. Existing applications normally use Open, which still recognizes V1
// without migrating it. New databases should use Open or OpenV2.
func OpenV1(path string) (*DB, error) {
	return OpenV1WithOptions(path, V1Options{})
}

// OpenV1WithOptions explicitly opens the legacy page-checkpoint plus WAL
// format with a bounded automatic checkpoint policy.
func OpenV1WithOptions(path string, options V1Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("meldbase: empty database path")
	}
	policy, err := normalizeV1CheckpointPolicy(options.Checkpoint)
	if err != nil {
		return nil, err
	}
	if err := validateRecoveryMode(options.Recovery); err != nil {
		return nil, err
	}
	resourceLimits, err := normalizeResourceLimits(options.ResourceLimits)
	if err != nil {
		return nil, err
	}
	requireClean := options.Recovery == RecoveryRequireClean
	pages, blobs, meta, pageRecovery, err := storagefile.OpenBlobsWithOptions(path, storagefile.OpenOptions{RequireClean: requireClean})
	if err != nil {
		return nil, mapStorageError(err)
	}
	collections, history, err := decodeCheckpointBlobs(blobs)
	if err != nil {
		pages.Close()
		return nil, fmt.Errorf("%w: snapshot: %v", ErrCorrupt, err)
	}
	if history != nil && ((meta.CheckpointToken > 0 && len(history) == 0) || (len(history) > 0 && history[len(history)-1].Token != meta.CheckpointToken)) {
		pages.Close()
		return nil, fmt.Errorf("%w: checkpoint history position", ErrCorrupt)
	}
	log, records, walRecovery, err := wal.OpenWithOptions(path+".wal", meta.CheckpointToken, wal.OpenOptions{RequireClean: requireClean})
	if err != nil {
		pages.Close()
		return nil, mapStorageError(err)
	}
	store := &durableStore{pages: pages, log: log, path: path, checkpointPolicy: policy, commitsSinceSync: uint64(len(records))}
	db := &DB{
		startedAt: time.Now(), closedCh: make(chan struct{}), collections: collections, watchers: make(map[uint64]*changeWatcher),
		token: meta.CheckpointToken, store: store, durability: store, databaseID: meta.DatabaseID, history: history, historyLimit: 1024,
		resourceLimits: resourceLimits,
		recovery: RecoveryReport{
			Engine: "v1", Created: pageRecovery.Created, CommitSequenceBefore: meta.CheckpointToken,
			SelectedMetaSlot: pageRecovery.SelectedMetaSlot, ChecksumValidMetaSlots: pageRecovery.ValidMetaSlots,
			RootValidMetaSlots: pageRecovery.ValidMetaSlots, MainTailBytesRemoved: pageRecovery.TrailingBytesRemoved,
			MetaRedundancyDegraded: pageRecovery.MetaRedundancyDegraded,
			WALRecordsReplayed:     walRecovery.RecordsReplayed, WALTailBytesRemoved: walRecovery.BytesDiscarded,
			AccelerationDegraded: pageRecovery.FreePageReuseDegraded,
		},
	}
	db.metrics.walCurrentBytes.Store(uint64(max(log.Size(), 0)))
	db.metrics.walCurrentCommits.Store(uint64(len(records)))
	for _, record := range records {
		if record.Token != db.token+1 {
			db.store.close()
			return nil, fmt.Errorf("%w: WAL token gap", ErrCorrupt)
		}
		changes, err := decodeTransaction(record.Payload)
		if err != nil {
			db.store.close()
			return nil, fmt.Errorf("%w: WAL transaction", ErrCorrupt)
		}
		if err := db.applyRecovered(changes); err != nil {
			db.store.close()
			return nil, err
		}
		db.token = record.Token
		db.recordCommittedBatch(ChangeBatch{Token: record.Token, Changes: changes})
	}
	db.recovery.CommitSequenceAfter = db.token
	db.recovery = finalizeRecoveryReport(db.recovery)
	db.initializeLogicalStats(nil)
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		db.store.close()
		return nil, err
	}
	db.reactive = newReactiveHub(db)
	return db, nil
}

func normalizeV1CheckpointPolicy(policy V1CheckpointPolicy) (V1CheckpointPolicy, error) {
	if policy.MaxWALBytes < 0 {
		return V1CheckpointPolicy{}, errors.New("meldbase: V1 checkpoint MaxWALBytes must not be negative")
	}
	if policy.Disabled {
		return V1CheckpointPolicy{Disabled: true}, nil
	}
	if policy.MaxWALBytes == 0 {
		policy.MaxWALBytes = defaultV1CheckpointWALBytes
	}
	if policy.MaxWALCommits == 0 {
		policy.MaxWALCommits = defaultV1CheckpointWALCommits
	}
	return policy, nil
}

func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	if db.durability == nil {
		return nil
	}
	return db.durability.syncDB(db)
}

func (db *DB) appendCommit(ctx context.Context, token uint64, changes []Change) error {
	if db.durability == nil {
		return nil
	}
	diagnostic := db.beginDiagnostic(DiagnosticCommit)
	if err := contextError(ctx); err != nil {
		db.finishCommitDiagnostic(diagnostic, len(changes), err)
		return err
	}
	err := db.durability.appendDBCommit(ctx, db, token, changes)
	db.finishCommitDiagnostic(diagnostic, len(changes), err)
	return err
}

func (s *durableStore) appendDBCommit(ctx context.Context, db *DB, token uint64, changes []Change) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	payload, err := encodeTransaction(changes)
	if err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	started := time.Now()
	if err := db.store.log.Append(token, payload); err != nil {
		db.metrics.walAppendFailures.Add(1)
		db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, err)
		return db.fatalErr
	}
	elapsed := uint64(time.Since(started))
	db.metrics.walAppends.Add(1)
	db.metrics.walPayloadBytes.Add(uint64(len(payload)))
	db.metrics.walAppendNanos.Add(elapsed)
	updateAtomicMax(&db.metrics.walAppendMaxNanos, elapsed)
	s.commitsSinceSync++
	db.metrics.walCurrentBytes.Store(uint64(max(s.log.Size(), 0)))
	db.metrics.walCurrentCommits.Store(s.commitsSinceSync)
	return nil
}

func (s *durableStore) syncDB(db *DB) error {
	return s.checkpointDB(db, false)
}

func (s *durableStore) checkpointDB(db *DB, automatic bool) error {
	db.metrics.checkpointAttempts.Add(1)
	if automatic {
		db.metrics.automaticCheckpoints.Add(1)
	}
	started := time.Now()
	fail := func(err error) error {
		db.metrics.checkpointFailures.Add(1)
		db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, err)
		return db.fatalErr
	}
	if err := s.injectCheckpoint(v1CheckpointBeforePages); err != nil {
		return fail(err)
	}
	blobs, err := encodeCheckpointBlobs(db.collections, db.history)
	if err != nil {
		return fail(err)
	}
	if err := s.pages.CheckpointBlobs(db.token, blobs); err != nil {
		return fail(err)
	}
	if err := s.injectCheckpoint(v1CheckpointAfterPages); err != nil {
		return fail(err)
	}
	if err := s.injectCheckpoint(v1CheckpointBeforeWALReset); err != nil {
		return fail(err)
	}
	if err := s.log.Reset(db.token); err != nil {
		return fail(err)
	}
	s.commitsSinceSync = 0
	db.metrics.walCurrentBytes.Store(0)
	db.metrics.walCurrentCommits.Store(0)
	if err := s.injectCheckpoint(v1CheckpointAfterWALReset); err != nil {
		return fail(err)
	}
	db.metrics.checkpointsCompleted.Add(1)
	elapsed := uint64(time.Since(started))
	db.metrics.checkpointNanos.Add(elapsed)
	updateAtomicMax(&db.metrics.checkpointMaxNanos, elapsed)
	return nil
}

func (s *durableStore) maybeCheckpointDB(db *DB) {
	if s == nil || db == nil || s.checkpointPolicy.Disabled || db.fatalErr != nil {
		return
	}
	bytesReached := s.checkpointPolicy.MaxWALBytes > 0 && s.log.Size() >= s.checkpointPolicy.MaxWALBytes
	commitsReached := s.checkpointPolicy.MaxWALCommits > 0 && s.commitsSinceSync >= s.checkpointPolicy.MaxWALCommits
	if bytesReached || commitsReached {
		// The logical commit has already been WAL-fsynced and applied. A
		// maintenance failure poisons later writes, but must not turn that
		// successful commit into an ambiguous error for its caller.
		_ = s.checkpointDB(db, true)
	}
}

func (s *durableStore) injectCheckpoint(point v1CheckpointFaultPoint) error {
	if s == nil || s.checkpointFault == nil {
		return nil
	}
	return s.checkpointFault(point)
}

func (s *durableStore) closeDB(db *DB) error {
	err := db.fatalErr
	if err == nil {
		err = s.syncDB(db)
	}
	return errors.Join(err, s.close())
}

func (db *DB) applyRecovered(changes []Change) error {
	if len(changes) == 0 {
		return ErrCorrupt
	}
	collection, operation := changes[0].Collection, changes[0].Operation
	for _, change := range changes {
		if change.Collection != collection || change.Operation != operation {
			return fmt.Errorf("%w: mixed transaction batch", ErrCorrupt)
		}
	}
	data := db.collections[collection]
	if data == nil {
		data = newCollectionData()
		db.collections[collection] = data
	}
	switch operation {
	case InsertOperation:
		for _, change := range changes {
			if _, exists := data.documents[change.DocumentID]; exists || change.After == nil {
				return fmt.Errorf("%w: invalid recovered insert", ErrCorrupt)
			}
			if err := data.validateIndexInsert(change.DocumentID, *change.After); err != nil {
				return fmt.Errorf("%w: recovered index insert", ErrCorrupt)
			}
			data.documents[change.DocumentID] = change.After.Clone()
			data.positions[change.DocumentID] = uint64(len(data.order))
			data.order = append(data.order, change.DocumentID)
			data.insertIndexes(change.DocumentID, *change.After)
		}
	case UpdateOperation:
		pending := make([]pendingUpdate, len(changes))
		for i, change := range changes {
			before, exists := data.documents[change.DocumentID]
			if !exists || change.After == nil {
				return fmt.Errorf("%w: invalid recovered update", ErrCorrupt)
			}
			pending[i] = pendingUpdate{id: change.DocumentID, before: before, after: *change.After}
		}
		if err := data.validateIndexUpdates(pending); err != nil {
			return fmt.Errorf("%w: recovered index update", ErrCorrupt)
		}
		for _, change := range pending {
			data.deleteIndexes(change.id, change.before)
		}
		for _, change := range pending {
			data.documents[change.id] = change.after.Clone()
			data.insertIndexes(change.id, change.after)
		}
	case DeleteOperation:
		for _, change := range changes {
			document, exists := data.documents[change.DocumentID]
			if !exists {
				return fmt.Errorf("%w: invalid recovered delete", ErrCorrupt)
			}
			data.deleteIndexes(change.DocumentID, document)
			delete(data.documents, change.DocumentID)
		}
		kept := data.order[:0]
		deleted := make(map[DocumentID]struct{}, len(changes))
		for _, change := range changes {
			deleted[change.DocumentID] = struct{}{}
		}
		for _, id := range data.order {
			if _, remove := deleted[id]; !remove {
				kept = append(kept, id)
			}
		}
		data.order = kept
		data.rebuildPositions()
	case CreateIndexOperation:
		if len(changes) != 1 || changes[0].Index == nil || data.indexes[changes[0].Index.Name] != nil {
			return fmt.Errorf("%w: invalid recovered index", ErrCorrupt)
		}
		state, err := buildIndex(*changes[0].Index, data, nil)
		if err != nil {
			return fmt.Errorf("%w: recovered index", ErrCorrupt)
		}
		data.indexes[changes[0].Index.Name] = state
	default:
		return ErrCorrupt
	}
	return nil
}

func (s *durableStore) close() error {
	if s == nil {
		return nil
	}
	return errors.Join(s.log.Close(), s.pages.Close())
}
func mapStorageError(err error) error {
	if errors.Is(err, storagefile.ErrLocked) {
		return fmt.Errorf("%w: %v", ErrDatabaseLocked, err)
	}
	if errors.Is(err, storagefile.ErrCorrupt) || errors.Is(err, wal.ErrCorrupt) {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if errors.Is(err, storagefile.ErrRecoveryRequired) || errors.Is(err, wal.ErrRecoveryRequired) {
		return ErrRecoveryRequired
	}
	return err
}
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
