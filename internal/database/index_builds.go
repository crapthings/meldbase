package database

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	storage "github.com/crapthings/meldbase/internal/storage"
)

// IndexBuildID identifies one durable, resumable Storage  index build.
type IndexBuildID [16]byte

func (id IndexBuildID) String() string               { return hex.EncodeToString(id[:]) }
func (id IndexBuildID) IsZero() bool                 { return id == IndexBuildID{} }
func (id IndexBuildID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

func (id *IndexBuildID) UnmarshalText(value []byte) error {
	if id == nil {
		return ErrIndexBuildNotFound
	}
	parsed, err := ParseIndexBuildID(string(value))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

func ParseIndexBuildID(value string) (IndexBuildID, error) {
	var id IndexBuildID
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(id) {
		return id, fmt.Errorf("%w: malformed index build id", ErrIndexBuildNotFound)
	}
	copy(id[:], decoded)
	if id.IsZero() {
		return IndexBuildID{}, fmt.Errorf("%w: malformed index build id", ErrIndexBuildNotFound)
	}
	return id, nil
}

type IndexBuildPhase string
type IndexBuildFailure string

const (
	IndexBuildPhaseScan    IndexBuildPhase = "scan"
	IndexBuildPhaseCatchUp IndexBuildPhase = "catch_up"
	IndexBuildPhaseReady   IndexBuildPhase = "ready"
	IndexBuildPhaseFailed  IndexBuildPhase = "failed"

	IndexBuildFailureNone           IndexBuildFailure = ""
	IndexBuildFailureUniqueConflict IndexBuildFailure = "unique_conflict"
	IndexBuildFailureResourceLimit  IndexBuildFailure = "resource_limit"
	IndexBuildFailureHistoryLost    IndexBuildFailure = "history_lost"
	IndexBuildFailureCanceled       IndexBuildFailure = "canceled"
	IndexBuildFailureInvalidIndex   IndexBuildFailure = "invalid_index"
)

// IndexBuildStatus is durable progress. EntryCount and CanonicalBytes describe
// the current private Secondary tree, not transient Go heap usage.
type IndexBuildStatus struct {
	ID              IndexBuildID      `json:"id"`
	Collection      string            `json:"collection"`
	Name            string            `json:"name"`
	Field           string            `json:"field"`
	Fields          []IndexField      `json:"fields"`
	Unique          bool              `json:"unique"`
	Phase           IndexBuildPhase   `json:"phase"`
	Failure         IndexBuildFailure `json:"failure,omitempty"`
	SourceSequence  uint64            `json:"sourceSequence"`
	AppliedSequence uint64            `json:"appliedSequence"`
	EntryCount      uint64            `json:"entryCount"`
	CanonicalBytes  uint64            `json:"canonicalBytes"`
	CreatedAt       time.Time         `json:"createdAt"`
	UpdatedAt       time.Time         `json:"updatedAt"`
}

// StartIndexBuild creates a durable private shadow index. It is Storage
// only and does not scan documents or make the index query-visible.
func (c *Collection) StartIndexBuild(ctx context.Context, name string, fields []IndexField, options IndexOptions) (IndexBuildID, error) {
	if err := contextError(ctx); err != nil {
		return IndexBuildID{}, err
	}
	if err := c.validate(); err != nil {
		return IndexBuildID{}, err
	}
	definition, err := validateIndexDefinition(name, fields, options)
	if err != nil {
		return IndexBuildID{}, err
	}
	var id IndexBuildID
	if _, err := rand.Read(id[:]); err != nil {
		return IndexBuildID{}, err
	}
	reservation := indexBuildReservation(c.name, definition.Name)
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	if c.db.replicaReadOnly {
		return IndexBuildID{}, ErrReplicaReadOnly
	}
	store, ok := c.db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		return IndexBuildID{}, ErrIndexBuildUnsupported
	}
	if err := contextError(ctx); err != nil {
		return IndexBuildID{}, err
	}
	if err := c.indexBuildPreconditionLocked(definition); err != nil {
		return IndexBuildID{}, err
	}
	if c.db.indexBuildReservations == nil {
		c.db.indexBuildReservations = make(map[string]struct{})
	}
	if _, exists := c.db.indexBuildReservations[reservation]; exists {
		return IndexBuildID{}, ErrIndexBuildExists
	}
	_, err = store.file.BeginIndexBuild(storage.BeginIndexBuildTransaction{
		BuildID: [16]byte(id), Collection: c.name, Name: definition.Name,
		FieldPath: definition.Field, Fields: storageIndexFields(definition), Unique: definition.Unique,
	})
	if err != nil {
		return IndexBuildID{}, c.db.handleIndexBuildErrorLocked(err)
	}
	if err := c.db.advanceRollbackAnchorLocked(ctx, store, c.db.token); err != nil {
		return IndexBuildID{}, err
	}
	store.refreshIndexBuildStats()
	c.db.indexBuildReservations[reservation] = struct{}{}
	return id, nil
}

// CreateIndexOnline starts and runs one durable build to publication. A
// context cancellation leaves the returned build discoverable through
// IndexBuilds; use StartIndexBuild when the ID must be known before execution.
func (c *Collection) CreateIndexOnline(ctx context.Context, name string, fields []IndexField, options IndexOptions) (IndexBuildID, error) {
	id, err := c.StartIndexBuild(ctx, name, fields, options)
	if err != nil {
		return IndexBuildID{}, err
	}
	return id, c.db.ResumeIndexBuild(ctx, id)
}

// IndexBuilds returns all durable unfinished builds. The result survives a
// clean close, process crash, and reopen.
func (db *DB) IndexBuilds() ([]IndexBuildStatus, error) {
	store, err := db.durableIndexBuildStore()
	if err != nil {
		return nil, err
	}
	metas, err := store.file.IndexBuilds()
	if err != nil {
		return nil, db.handleIndexBuildError(err)
	}
	result := make([]IndexBuildStatus, len(metas))
	for index := range metas {
		result[index] = publicIndexBuildStatus(metas[index])
	}
	return result, nil
}

func (db *DB) IndexBuild(id IndexBuildID) (IndexBuildStatus, error) {
	store, err := db.durableIndexBuildStore()
	if err != nil {
		return IndexBuildStatus{}, err
	}
	meta, exists, err := store.file.IndexBuild([16]byte(id))
	if err != nil {
		return IndexBuildStatus{}, db.handleIndexBuildError(err)
	}
	if !exists {
		return IndexBuildStatus{}, ErrIndexBuildNotFound
	}
	return publicIndexBuildStatus(meta), nil
}

// ResumeIndexBuild scans bounded batches, catches up retained commits, and
// atomically publishes the index. Only one caller should resume a given build;
// stale concurrent callers receive ErrWriteConflict from durable CAS checks.
func (db *DB) ResumeIndexBuild(ctx context.Context, id IndexBuildID) error {
	store, err := db.durableIndexBuildStore()
	if err != nil {
		return err
	}
	if id.IsZero() {
		return ErrIndexBuildNotFound
	}
	meta, exists, err := store.file.IndexBuild([16]byte(id))
	if err != nil {
		return db.handleIndexBuildError(err)
	}
	if !exists {
		return ErrIndexBuildNotFound
	}
	budget := db.newIndexBuildBudget(db.resourceLimits)
	budget.entries, budget.bytes = meta.EntryCount, meta.CanonicalBytes
	return db.observeIndexBuild(budget, func() error {
		for {
			if err := contextError(ctx); err != nil {
				return err
			}
			if store.testPersistentIndexBuildBatchHook != nil {
				store.testPersistentIndexBuildBatchHook(ctx, id)
				if err := contextError(ctx); err != nil {
					return err
				}
			}
			meta, exists, err = store.file.IndexBuild([16]byte(id))
			if err != nil {
				return db.handleIndexBuildError(err)
			}
			if !exists {
				if db.indexBuildPublished(meta) {
					return nil
				}
				return ErrIndexBuildNotFound
			}
			budget.entries, budget.bytes = meta.EntryCount, meta.CanonicalBytes
			finished := false
			switch meta.Phase {
			case storage.IndexBuildScan:
				err = db.resumeIndexBuildScan(ctx, store, meta, budget)
			case storage.IndexBuildCatchUp:
				err = db.resumeIndexBuildCatchUp(ctx, store, meta, budget)
			case storage.IndexBuildReady:
				if store.testPersistentIndexBuildReadyHook != nil {
					store.testPersistentIndexBuildReadyHook()
				}
				root, rootErr := store.file.DatabaseRoot()
				if rootErr != nil {
					err = db.handleIndexBuildError(rootErr)
				} else if root.CommitSequence > meta.AppliedSequence {
					err = db.resumeIndexBuildCatchUp(ctx, store, meta, budget)
				} else {
					err = db.finalizeIndexBuild(ctx, store, meta)
					finished = err == nil
				}
			case storage.IndexBuildFailed:
				return fmt.Errorf("%w: %s", ErrIndexBuildFailed, publicIndexBuildStatus(meta).Failure)
			default:
				return ErrCorrupt
			}
			if err != nil {
				if errors.Is(err, ErrWriteConflict) {
					db.metrics.indexBuildConflicts.Add(1)
					db.metrics.indexBuildRetries.Add(1)
					continue
				}
				return err
			}
			if finished {
				return nil
			}
		}
	})
}

func (db *DB) indexBuildPublished(meta storage.IndexBuildMeta) bool {
	if db == nil || meta.Name == "" {
		return false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	data := db.collections[meta.Collection]
	if data == nil || data.indexes == nil {
		return false
	}
	state := data.indexes[meta.Name]
	fields, err := publicIndexFields(meta.FieldPath, meta.Fields)
	if err != nil {
		return false
	}
	return state != nil && equalIndexDefinitions(state.definition, newIndexDefinition(meta.Name, fields, meta.Unique))
}

func (db *DB) resumeIndexBuildScan(ctx context.Context, store *durableStore, meta storage.IndexBuildMeta, budget *indexBuildBudget) (resultErr error) {
	opened, iterator, err := store.file.OpenIndexBuildScanIterator(meta.BuildID, storage.MaxIndexBuildBatchEntries)
	if err != nil {
		return db.handleIndexBuildError(err)
	}
	defer func() {
		if closeErr := iterator.Close(); resultErr == nil && closeErr != nil {
			resultErr = db.handleIndexBuildError(closeErr)
		}
	}()
	fields, err := publicIndexFields(opened.FieldPath, opened.Fields)
	if err != nil {
		return err
	}
	definition := newIndexDefinition(opened.Name, fields, opened.Unique)
	entries := make([]storage.IndexEntry, 0, storage.MaxIndexBuildBatchEntries)
	last := opened.ScanAfter
	sourceCount := 0
	batchBytes := uint64(0)
	hitBatchLimit := false
	for iterator.Next() {
		if err := contextError(ctx); err != nil {
			return err
		}
		record := iterator.Record()
		key, found, err := projectedIndexBuildKey(record.Document, definition, DocumentID(record.DocumentID))
		if errors.Is(err, ErrCorrupt) || record.InsertionPosition == 0 {
			return fmt.Errorf("%w: stored document", ErrCorrupt)
		}
		if err != nil {
			return err
		}
		if !found {
			sourceCount++
			last = record.DocumentID
			continue
		}
		entryBytes := uint64(len(key) + 8 + 16)
		if sourceCount > 0 && entryBytes > storage.MaxIndexBuildBatchBytes-batchBytes {
			hitBatchLimit = true
			break
		}
		if err := budget.add(key); err != nil {
			return err
		}
		entries = append(entries, storage.IndexEntry{Key: key, InsertionPosition: record.InsertionPosition, DocumentID: record.DocumentID})
		batchBytes += entryBytes
		sourceCount++
		last = record.DocumentID
	}
	if err := iterator.Err(); err != nil {
		return db.handleIndexBuildError(err)
	}
	// The iterator limit counts source documents, while entries omit documents
	// lacking the field. An exact full batch uses one cheap follow-up batch to
	// prove EOF; a shorter batch can transition immediately.
	complete := !hitBatchLimit && sourceCount < storage.MaxIndexBuildBatchEntries
	_, err = store.file.ApplyIndexBuildScanBatch(storage.IndexBuildScanBatch{
		BuildID: meta.BuildID, ExpectedScanAfter: opened.ScanAfter, ScanAfter: last,
		Entries: entries, Complete: complete,
	})
	if err != nil {
		return db.handleIndexBuildError(err)
	}
	if err := db.advanceRollbackAnchor(ctx, store, meta.AppliedSequence); err != nil {
		return err
	}
	store.refreshIndexBuildStats()
	return nil
}

func (db *DB) resumeIndexBuildCatchUp(ctx context.Context, store *durableStore, meta storage.IndexBuildMeta, budget *indexBuildBudget) error {
	opened, snapshot, err := store.file.OpenIndexBuildCatchUpSnapshot(meta.BuildID)
	if err != nil {
		return db.handleIndexBuildError(err)
	}
	defer snapshot.Close()
	budget.entries, budget.bytes = opened.EntryCount, opened.CanonicalBytes
	through := snapshot.Sequence()
	if through > opened.AppliedSequence+storage.MaxIndexBuildCatchUpCommits {
		through = opened.AppliedSequence + storage.MaxIndexBuildCatchUpCommits
	}
	if through <= opened.AppliedSequence {
		return ErrWriteConflict
	}
	fields, err := publicIndexFields(opened.FieldPath, opened.Fields)
	if err != nil {
		return err
	}
	definition := newIndexDefinition(opened.Name, fields, opened.Unique)
	mutations := make([]storage.IndexBuildCatchUpMutation, 0)
	for sequence := opened.AppliedSequence + 1; sequence <= through; sequence++ {
		if err := contextError(ctx); err != nil {
			return err
		}
		commit, err := snapshot.ReadCommit(sequence)
		if err != nil {
			return db.handleIndexBuildError(err)
		}
		relevant := 0
		for _, change := range commit.Changes {
			if change.CollectionID == opened.CollectionID && change.Operation != storage.CommitCatalog {
				relevant++
			}
		}
		if len(mutations) > 0 && relevant > storage.MaxIndexBuildCatchUpMutations-len(mutations) {
			through = sequence - 1
			break
		}
		for _, change := range commit.Changes {
			if change.CollectionID != opened.CollectionID || change.Operation == storage.CommitCatalog {
				continue
			}
			mutation := storage.IndexBuildCatchUpMutation{Sequence: sequence, DocumentID: change.DocumentID, Operation: change.Operation}
			if change.BeforeRef != nil {
				encoded, err := snapshot.ReadDocumentVersion(*change.BeforeRef)
				if err != nil {
					return db.handleIndexBuildError(err)
				}
				mutation.BeforeKey, _, err = projectedIndexBuildKey(encoded, definition, DocumentID(change.DocumentID))
				if err != nil {
					return err
				}
			}
			if change.AfterRef != nil {
				encoded, err := snapshot.ReadDocumentVersion(*change.AfterRef)
				if err != nil {
					return db.handleIndexBuildError(err)
				}
				mutation.AfterKey, _, err = projectedIndexBuildKey(encoded, definition, DocumentID(change.DocumentID))
				if err != nil {
					return err
				}
			}
			if len(mutation.BeforeKey) > 0 {
				if err := budget.remove(mutation.BeforeKey); err != nil {
					return err
				}
			}
			if len(mutation.AfterKey) > 0 {
				if err := budget.add(mutation.AfterKey); err != nil {
					return err
				}
			}
			mutations = append(mutations, mutation)
		}
	}
	_, err = store.file.ApplyIndexBuildCatchUpBatch(storage.IndexBuildCatchUpBatch{
		BuildID: opened.BuildID, ExpectedAppliedSequence: opened.AppliedSequence,
		ThroughSequence: through, Mutations: mutations,
	})
	if err != nil {
		return db.handleIndexBuildError(err)
	}
	if err := db.advanceRollbackAnchor(ctx, store, through); err != nil {
		return err
	}
	store.refreshIndexBuildStats()
	return nil
}

func (db *DB) finalizeIndexBuild(ctx context.Context, store *durableStore, meta storage.IndexBuildMeta) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := contextError(ctx); err != nil {
		return err
	}
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	if db.token != meta.AppliedSequence {
		return ErrWriteConflict
	}
	// Finalization is a logical catalog commit: it makes an index visible to
	// queries and replication. A stale primary must not be able to publish that
	// visibility point after its lease/epoch was revoked.
	if err := db.validatePrimaryWriteFence(db.token + 1); err != nil {
		return err
	}
	var transactionID [16]byte
	if _, err := rand.Read(transactionID[:]); err != nil {
		return err
	}
	sequence, err := store.file.FinalizeIndexBuild(storage.FinalizeIndexBuildTransaction{
		BuildID: meta.BuildID, TransactionID: transactionID,
		ExpectedAppliedSequence: meta.AppliedSequence,
	})
	if err != nil {
		return db.handleIndexBuildErrorLocked(err)
	}
	if sequence != db.token+1 {
		db.fatalErr = fmt.Errorf("%w: index build sequence mismatch", ErrDurability)
		return db.fatalErr
	}
	if err := db.advanceRollbackAnchorLocked(ctx, store, sequence); err != nil {
		return err
	}
	store.refreshIndexBuildStats()
	fields, err := publicIndexFields(meta.FieldPath, meta.Fields)
	if err != nil {
		return err
	}
	definition := newIndexDefinition(meta.Name, fields, meta.Unique)
	data := db.collections[meta.Collection]
	if data == nil {
		data = newCollectionData()
		db.collections[meta.Collection] = data
	}
	if data.indexes == nil {
		data.indexes = make(map[string]*indexState)
	}
	data.indexes[meta.Name] = &indexState{definition: definition}
	db.token = sequence
	delete(db.indexBuildReservations, indexBuildReservation(meta.Collection, meta.Name))
	copyDefinition := definition
	db.recordLiveCommit(ChangeBatch{Token: sequence, Changes: []Change{{Collection: meta.Collection, Operation: CreateIndexOperation, Index: &copyDefinition}}})
	return nil
}

func (db *DB) AbortIndexBuild(ctx context.Context, id IndexBuildID) error {
	store, err := db.durableIndexBuildStore()
	if err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	meta, exists, err := store.file.IndexBuild([16]byte(id))
	if err != nil {
		return db.handleIndexBuildError(err)
	}
	if !exists {
		return ErrIndexBuildNotFound
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if err := store.file.AbortIndexBuild([16]byte(id)); err != nil {
		return db.handleIndexBuildErrorLocked(err)
	}
	if err := db.advanceRollbackAnchorLocked(ctx, store, db.token); err != nil {
		return err
	}
	store.refreshIndexBuildStats()
	delete(db.indexBuildReservations, indexBuildReservation(meta.Collection, meta.Name))
	return nil
}

func (db *DB) failIndexBuild(ctx context.Context, id IndexBuildID, failure storage.IndexBuildFailure) (IndexBuildStatus, error) {
	if err := contextError(ctx); err != nil {
		return IndexBuildStatus{}, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return IndexBuildStatus{}, ErrClosed
	}
	if db.fatalErr != nil {
		return IndexBuildStatus{}, db.fatalErr
	}
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		return IndexBuildStatus{}, ErrIndexBuildUnsupported
	}
	meta, err := store.file.FailIndexBuild(storage.FailIndexBuildTransaction{BuildID: [16]byte(id), Failure: failure})
	if err != nil {
		return IndexBuildStatus{}, db.handleIndexBuildErrorLocked(err)
	}
	if err := db.advanceRollbackAnchorLocked(ctx, store, db.token); err != nil {
		return IndexBuildStatus{}, err
	}
	store.refreshIndexBuildStats()
	return publicIndexBuildStatus(meta), nil
}

func (db *DB) durableIndexBuildStore() (*durableStore, error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	store, ok := db.durability.(*durableStore)
	if !ok || store == nil || store.file == nil {
		return nil, ErrIndexBuildUnsupported
	}
	return store, nil
}

func (db *DB) handleIndexBuildError(err error) error {
	mapped := mapStorageError(err)
	if safeIndexBuildError(mapped) {
		return mapped
	}
	if db != nil {
		db.mu.Lock()
		mapped = db.poisonIndexBuildLocked(mapped)
		db.mu.Unlock()
	}
	return mapped
}

// handleIndexBuildErrorLocked is for storage calls deliberately serialized
// by db.mu, including begin/finalize/abort publication coordination.
func (db *DB) handleIndexBuildErrorLocked(err error) error {
	mapped := mapStorageError(err)
	if safeIndexBuildError(mapped) {
		return mapped
	}
	return db.poisonIndexBuildLocked(mapped)
}

func safeIndexBuildError(err error) bool {
	return err == nil || errors.Is(err, ErrIndexBuildNotFound) || errors.Is(err, ErrIndexBuildExists) ||
		errors.Is(err, ErrWriteConflict) || errors.Is(err, ErrHistoryLost) || errors.Is(err, ErrDuplicateKey) ||
		errors.Is(err, ErrInvalidIndex) || errors.Is(err, ErrResourceLimit) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrClosed)
}

func (db *DB) poisonIndexBuildLocked(err error) error {
	if db == nil {
		return err
	}
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr == nil {
		db.fatalErr = fmt.Errorf("%w: %v", ErrDurability, err)
	}
	return db.fatalErr
}

func validateIndexDefinition(name string, fields []IndexField, options IndexOptions) (IndexDefinition, error) {
	if !indexNamePattern.MatchString(name) || len(fields) == 0 || len(fields) > maxCompoundIndexFields {
		return IndexDefinition{}, fmt.Errorf("%w: indexes require one to %d fields", ErrInvalidIndex, maxCompoundIndexFields)
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if (field.Order != 1 && field.Order != -1) || validatePath(field.Field) != nil {
			return IndexDefinition{}, fmt.Errorf("%w: invalid index field", ErrInvalidIndex)
		}
		if _, duplicate := seen[field.Field]; duplicate {
			return IndexDefinition{}, fmt.Errorf("%w: duplicate index field", ErrInvalidIndex)
		}
		seen[field.Field] = struct{}{}
	}
	return newIndexDefinition(name, fields, options.Unique), nil
}

func indexBuildPath(field string) [][]byte {
	parts := strings.Split(field, ".")
	result := make([][]byte, len(parts))
	for index := range parts {
		result[index] = []byte(parts[index])
	}
	return result
}

func projectedIndexBuildKey(encoded []byte, definition IndexDefinition, id DocumentID) ([]byte, bool, error) {
	fields := indexDefinitionFields(definition)
	values := make([]Value, len(fields))
	for index, field := range fields {
		value, found, scalar, err := projectStoredDocumentScalar(encoded, indexBuildPath(field.Field), id)
		if err != nil {
			return nil, false, fmt.Errorf("%w: stored document", ErrCorrupt)
		}
		if !found {
			if index == 0 || !usesCompoundIndexCodec(definition) {
				return nil, false, nil
			}
			key, err := encodeCompoundPartialIndexKey(values[:index], fields[:index], id)
			if err != nil {
				return nil, true, fmt.Errorf("%w: indexed tuple is invalid", ErrInvalidIndex)
			}
			return key, true, nil
		}
		if !scalar {
			return nil, true, fmt.Errorf("%w: indexed field is not scalar", ErrInvalidIndex)
		}
		values[index] = value
	}
	if usesCompoundIndexCodec(definition) {
		key, err := encodeCompoundIndexKey(values, fields)
		if err != nil {
			return nil, true, fmt.Errorf("%w: indexed field is not scalar", ErrInvalidIndex)
		}
		return key, true, nil
	}
	key, err := encodeIndexKey(values[0])
	if err != nil {
		return nil, true, fmt.Errorf("%w: indexed field is not scalar", ErrInvalidIndex)
	}
	return key, true, nil
}

func indexBuildReservation(collection, name string) string { return collection + "\x00" + name }

func publicIndexBuildStatus(meta storage.IndexBuildMeta) IndexBuildStatus {
	phase := IndexBuildPhaseFailed
	failure := IndexBuildFailureNone
	switch meta.Phase {
	case storage.IndexBuildScan:
		phase = IndexBuildPhaseScan
	case storage.IndexBuildCatchUp:
		phase = IndexBuildPhaseCatchUp
	case storage.IndexBuildReady:
		phase = IndexBuildPhaseReady
	}
	switch meta.Failure {
	case storage.IndexBuildFailureUniqueConflict:
		failure = IndexBuildFailureUniqueConflict
	case storage.IndexBuildFailureResourceLimit:
		failure = IndexBuildFailureResourceLimit
	case storage.IndexBuildFailureHistoryLost:
		failure = IndexBuildFailureHistoryLost
	case storage.IndexBuildFailureCanceled:
		failure = IndexBuildFailureCanceled
	case storage.IndexBuildFailureInvalidIndex:
		failure = IndexBuildFailureInvalidIndex
	}
	fields, _ := publicIndexFields(meta.FieldPath, meta.Fields)
	return IndexBuildStatus{
		ID: IndexBuildID(meta.BuildID), Collection: meta.Collection, Name: meta.Name, Field: meta.FieldPath,
		Fields: fields,
		Unique: meta.Unique, Phase: phase, Failure: failure, SourceSequence: meta.SourceSequence, AppliedSequence: meta.AppliedSequence,
		EntryCount: meta.EntryCount, CanonicalBytes: meta.CanonicalBytes, CreatedAt: meta.CreatedAt, UpdatedAt: meta.UpdatedAt,
	}
}
