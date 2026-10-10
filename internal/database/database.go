package database

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

// collectionNamePattern defines the accepted collection name syntax.
var collectionNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,127}$`)

// fallbackDatabaseIdentityCounter distinguishes memory databases when random identity generation fails.
var fallbackDatabaseIdentityCounter atomic.Uint64

// Operation identifies a document or catalog change.
type Operation string

const (
	// InsertOperation marks a document insertion.
	InsertOperation Operation = "insert"
	// UpdateOperation marks a document update.
	UpdateOperation Operation = "update"
	// DeleteOperation marks a document deletion.
	DeleteOperation Operation = "delete"
	// CreateCollectionOperation marks collection creation in the durable database change feed.
	//
	// Ordinary collection creation remains implicit for CRUD callers.
	CreateCollectionOperation Operation = "create_collection"
	// CreateIndexOperation marks publication of a new index.
	CreateIndexOperation Operation = "create_index"
	// ReplaceIndexOperation marks replacement of an existing index.
	ReplaceIndexOperation Operation = "replace_index"
)

// IndexDefinition describes the ordered fields and uniqueness of an index.
type IndexDefinition struct {
	Name, Field string
	Order       int
	Unique      bool
	// Fields is the ordered definition for compound/descending indexes. Field
	// and Order remain the compatibility mirror of Fields[0] for existing
	// callers; new code must use indexDefinitionFields.
	Fields []IndexField
}

// Change records a document or catalog edit at a commit.
type Change struct {
	Collection string
	Operation  Operation
	DocumentID DocumentID
	Before     *Document
	After      *Document
	Index      *IndexDefinition

	// ChangedPaths is the sorted, deduplicated set of document paths changed by
	// an Update operation when the writer can prove it. A nil value means the
	// changed field set is unknown and consumers must conservatively treat the
	// whole document as affected. Insert, delete and catalog changes intentionally
	// use that conservative form: their membership and visible payload may change
	// through any query path.
	//
	// The slice is immutable once a Change enters the dispatcher. Public watcher
	// deliveries receive an independent copy via cloneChange.
	ChangedPaths []string

	// dispatchBytes is an internal, canonical accounting weight for the data
	// retained by the asynchronous change-dispatch boundary. It is populated
	// while the write path already validates document resource limits, avoiding
	// a second document walk after a successful durable commit.
	dispatchBytes      uint64
	dispatchBytesKnown bool

	// afterCanonicalBytes is populated alongside dispatchBytes. Shared reactive
	// views use it to avoid re-encoding the same immutable After document once
	// per matching view. Replay and private test changes retain a safe canonical
	// fallback when this write-path cache is unavailable.
	afterCanonicalBytes      uint64
	afterCanonicalBytesKnown bool
}

// ChangeBatch groups changes under one ordered commit token.
type ChangeBatch struct {
	Token   uint64
	Changes []Change
}

// DB owns an in-memory or durable document database.
type DB struct {
	mu                        sync.RWMutex
	startedAt                 time.Time
	closed                    bool
	closedCh                  chan struct{}
	collections               map[string]*collectionData
	token                     uint64
	feedMu                    sync.Mutex
	nextWatcher               uint64
	watchers                  map[uint64]*changeWatcher
	pendingWatcherBytes       uint64 // protected by feedMu
	durability                durabilityBackend
	replaySource              QueryReplaySource
	querySource               querySnapshotSource
	fatalErr                  error
	databaseID                [16]byte
	history                   []ChangeBatch
	historyLimit              int
	metrics                   dbMetrics
	diagnostics               atomic.Pointer[Diagnostics]
	diagnosticSession         atomic.Uint64
	reactive                  *reactiveHub
	recovery                  RecoveryReport
	resourceLimits            ResourceLimits
	activeIndexBuild          *indexBuildBudget
	indexBuildReservations    map[string]struct{}
	indexBuildSchedulerActive bool
	storageLimits             StorageLimits
	dispatcher                *changeDispatcher
	commitCoordinator         *commitCoordinator
	// replicationSourceLeases provides process-local single-active ownership for
	// an authenticated primary-side replica identity. It is not a distributed
	// leader lease; it only prevents two transport connections in this DB from
	// racing one durable consumer checkpoint.
	replicationSourceLeases map[string]struct{}
	// replicaReadOnly is set only by OpenFollower. Application mutations are
	// rejected at the common durable commit boundary; follower.Apply bypasses
	// that boundary only after it validates the next source token under db.mu.
	replicaReadOnly   bool
	primaryWriteFence PrimaryWriteFence
}

// collectionData holds a collection's in-memory documents, insertion order, and indexes.
type collectionData struct {
	documents map[DocumentID]Document
	order     []DocumentID
	positions map[DocumentID]uint64
	indexes   map[string]*indexState
}

// newCollectionData creates empty document, position, and index maps.
func newCollectionData() *collectionData {
	return &collectionData{
		documents: make(map[DocumentID]Document),
		positions: make(map[DocumentID]uint64),
		indexes:   make(map[string]*indexState),
	}
}

// rebuildPositions refreshes document positions after the insertion order changes.
func (d *collectionData) rebuildPositions() {
	d.positions = make(map[DocumentID]uint64, len(d.order))
	for position, id := range d.order {
		d.positions[id] = uint64(position)
	}
}

// New creates an in-memory database with default limits.
func New() *DB {
	db, _ := NewWithOptions(DatabaseOptions{})
	return db
}

// NewWithOptions creates an in-memory database with explicit resource limits.
func NewWithOptions(options DatabaseOptions) (*DB, error) {
	resourceLimits, err := normalizeResourceLimits(options.ResourceLimits)
	if err != nil {
		return nil, err
	}
	db := &DB{
		startedAt: time.Now(), closedCh: make(chan struct{}), collections: make(map[string]*collectionData),
		watchers: make(map[uint64]*changeWatcher), historyLimit: 1024,
		recovery:       finalizeRecoveryReport(RecoveryReport{Engine: "memory", Created: true}),
		resourceLimits: resourceLimits,
	}
	db.reactive = newReactiveHub(db)
	db.dispatcher = newChangeDispatcher(db)
	db.initializeLogicalStats(nil)
	if _, err := rand.Read(db.databaseID[:]); err != nil {
		// The identity is a namespace binding, not a secret. Keep the infallible
		// in-memory constructor usable if the OS entropy source is unavailable,
		// while avoiding a shared all-zero namespace.
		var seed [16]byte
		binary.LittleEndian.PutUint64(seed[:8], uint64(time.Now().UnixNano()))
		binary.LittleEndian.PutUint64(seed[8:], fallbackDatabaseIdentityCounter.Add(1))
		digest := sha256.Sum256(seed[:])
		copy(db.databaseID[:], digest[:16])
	}
	return db, nil
}

// Close stops background work and closes the database's storage and subscriptions.
func (db *DB) Close() error {
	// Stop  admission before taking db.mu: a running coordinator owns work
	// that will shortly need that mutex to publish, so reversing this order
	// would deadlock Close against an in-flight group.
	if db != nil && db.commitCoordinator != nil {
		db.commitCoordinator.close()
	}
	// A  compaction retains an immutable storage snapshot after it releases
	// db.mu so ordinary writes can continue. Closing the underlying file during
	// that copy would invalidate its pinned readers, therefore Close joins the
	// same gate before it closes the durable store. Compaction takes this gate
	// before its short db.mu read section, so this ordering cannot deadlock.
	var compactionStore *durableStore
	if db != nil {
		compactionStore, _ = db.durability.(*durableStore)
		if compactionStore != nil {
			compactionStore.compactMu.Lock()
			defer compactionStore.compactMu.Unlock()
		}
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	var closeErr error
	if db.durability != nil {
		closeErr = db.durability.closeDB(db)
	}
	db.closed = true
	close(db.closedCh)
	if diagnostics := db.diagnostics.Swap(nil); diagnostics != nil {
		diagnostics.closed.Store(true)
	}
	db.mu.Unlock()
	if db.dispatcher != nil {
		db.dispatcher.close()
	}
	if db.reactive != nil {
		db.reactive.close()
	}
	db.feedMu.Lock()
	stopping := make([]*changeWatcher, 0, len(db.watchers))
	for id, watcher := range db.watchers {
		db.finishChangeWatcherLocked(id, watcher, nil)
		stopping = append(stopping, watcher)
	}
	db.feedMu.Unlock()
	for _, watcher := range stopping {
		<-watcher.stopped
	}
	return closeErr
}

// Collection returns an operation handle for the named collection.
func (db *DB) Collection(name string) *Collection { return &Collection{db: db, name: name} }

// DatabaseID returns the stable, non-secret database namespace used to
// bind resume and replication protocols.
//
// It is not an authentication token.
func (db *DB) DatabaseID() [16]byte {
	if db == nil {
		return [16]byte{}
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.databaseID
}

// CommitCoordinatorStats returns a bounded snapshot of the optional
// admission scheduler.
//
// It performs no I/O. DBStats includes the same snapshot
// in the versioned admin schema; this method is convenient for direct callers.
func (db *DB) CommitCoordinatorStats() CommitCoordinatorStats {
	if db == nil {
		return CommitCoordinatorStats{}
	}
	db.mu.RLock()
	coordinator := db.commitCoordinator
	db.mu.RUnlock()
	if coordinator == nil {
		return CommitCoordinatorStats{}
	}
	return coordinator.stats()
}

// Collection provides document operations for one named collection.
type Collection struct {
	db   *DB
	name string
}

// InsertOne inserts one document and returns its assigned ID.
func (c *Collection) InsertOne(ctx context.Context, document Document) (DocumentID, error) {
	ids, err := c.InsertMany(ctx, []Document{document})
	if err != nil {
		// A coordinator admission may race caller cancellation after the
		// document ID is assigned. Preserve that ID with ErrCommitOutcomeUnknown
		// so InsertOne callers can reconcile the durable outcome too.
		if errors.Is(err, ErrCommitOutcomeUnknown) && len(ids) == 1 {
			return ids[0], err
		}
		return DocumentID{}, err
	}
	return ids[0], nil
}

// InsertMany validates and atomically inserts a batch of copied documents.
func (c *Collection) InsertMany(ctx context.Context, documents []Document) ([]DocumentID, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	if len(documents) == 0 {
		return nil, fmt.Errorf("%w: empty insert batch", ErrInvalidDocument)
	}
	if uint64(len(documents)) > c.db.resourceLimits.MaxTransactionChanges {
		c.db.metrics.resourceLimitRejections.Add(1)
		return nil, fmt.Errorf("%w: transaction changes %d exceed limit %d", ErrResourceLimit, len(documents), c.db.resourceLimits.MaxTransactionChanges)
	}
	copies := make([]Document, len(documents))
	ids := make([]DocumentID, len(documents))
	seenIDs := make(map[DocumentID]struct{}, len(documents))
	for index, document := range documents {
		copy := document.Clone()
		if copy == nil {
			copy = Document{}
		}
		if err := copy.Validate(); err != nil {
			return nil, err
		}
		id, hasID := copy.ID()
		if _, exists := copy["_id"]; exists && !hasID {
			return nil, fmt.Errorf("%w: _id must be DocumentID", ErrInvalidDocument)
		}
		if !hasID {
			var err error
			id, err = NewDocumentID()
			if err != nil {
				return nil, err
			}
			copy["_id"] = ID(id)
		}
		if id.IsZero() {
			return nil, fmt.Errorf("%w: zero _id", ErrInvalidDocument)
		}
		if _, duplicate := seenIDs[id]; duplicate {
			return nil, ErrDuplicateID
		}
		seenIDs[id] = struct{}{}
		copies[index], ids[index] = copy, id
	}
	changes := make([]Change, len(copies))
	for index, copy := range copies {
		after := copy.Clone()
		changes[index] = Change{Collection: c.name, Operation: InsertOperation, DocumentID: ids[index], After: &after}
	}
	if err := c.db.validateTransactionResource(changes); err != nil {
		return nil, err
	}
	if coordinator := c.db.commitCoordinator; coordinator != nil {
		return coordinator.submit(ctx, c.name, ids, copies, changes)
	}
	c.db.mu.Lock()
	err := c.commitInsertManyLocked(ctx, ids, copies, changes)
	c.db.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return append([]DocumentID(nil), ids...), nil
}

// commitInsertManyLocked commits one prepared insertion batch atomically.
//
// The coordinator reuses this path after a group conflict so each request keeps
// its own success or duplicate-key outcome.
//
// The caller holds db.mu and has already validated immutable input/resource
// bounds. ctx is intentionally supplied by the caller: direct CRUD preserves
// normal context behavior, while an admitted coordinator request uses a
// background context because cancellation after admission cannot prove absence.
func (c *Collection) commitInsertManyLocked(ctx context.Context, ids []DocumentID, copies []Document, changes []Change) error {
	if c == nil || c.db == nil || len(ids) == 0 || len(ids) != len(copies) || len(changes) != len(copies) {
		return ErrCorrupt
	}
	if c.db.closed {
		return ErrClosed
	}
	if c.db.fatalErr != nil {
		return c.db.fatalErr
	}
	data := c.db.collections[c.name]
	if data != nil && c.db.querySource == nil {
		for _, id := range ids {
			if _, exists := data.documents[id]; exists {
				return ErrDuplicateID
			}
		}
		if err := data.validateIndexBatchInsert(ids, copies); err != nil {
			return err
		}
	}
	token := c.db.token + 1
	if err := c.db.appendCommit(ctx, token, changes); err != nil {
		return err
	}
	if data == nil {
		data = newCollectionData()
		c.db.collections[c.name] = data
	}
	if c.db.querySource == nil {
		for index, copy := range copies {
			id := ids[index]
			data.documents[id] = copy
			data.positions[id] = uint64(len(data.order))
			data.order = append(data.order, id)
			data.insertIndexes(id, copy)
		}
	}
	c.db.token = token
	batch := ChangeBatch{Token: token, Changes: changes}
	c.db.recordLiveCommit(batch)
	c.db.publish(batch)
	return nil
}

// Find compiles a filter and optional pagination settings into a result cursor.
func (c *Collection) Find(ctx context.Context, filter Filter, options ...QueryOptions) (*Cursor, error) {
	queryOptions := QueryOptions{}
	if len(options) > 1 {
		return nil, fmt.Errorf("%w: multiple query options", ErrInvalidFilter)
	}
	if len(options) == 1 {
		queryOptions = options[0]
	}
	query, err := CompileQuery(filter, queryOptions)
	if err != nil {
		return nil, err
	}
	return c.FindQuery(ctx, query)
}

// FindQuery opens a result cursor for a validated compiled query.
func (c *Collection) FindQuery(ctx context.Context, query QuerySpec) (*Cursor, error) {
	if err := query.Validate(); err != nil {
		return nil, err
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	diagnostic := c.db.beginDiagnostic(DiagnosticQuery)
	budget, err := c.db.newQueryBudget(query)
	if err != nil {
		explain := enrichExplain(ExplainResult{
			PlanReason: "not_planned", FallbackReason: "budget_rejected",
		}, query, budget)
		c.db.recordQuery(explain, 0, err, diagnostic)
		return nil, err
	}
	var plannedAccess *queryAccessPlan
	if c.db.querySource != nil {
		c.db.mu.RLock()
		if c.db.closed {
			c.db.mu.RUnlock()
			c.db.recordQuery(ExplainResult{Stage: "COLLSCAN"}, 0, ErrClosed, diagnostic)
			return nil, ErrClosed
		}
		stream, access, streamed, err := c.openStorageCollectionStreamLocked(ctx, query)
		c.db.mu.RUnlock()
		if err != nil {
			c.db.recordQuery(ExplainResult{Stage: "COLLSCAN"}, 0, err, diagnostic)
			return nil, err
		}
		if streamed {
			limit := -1
			if value, exists := query.Limit(); exists {
				limit = value
			}
			cursor := &Cursor{
				stream: stream, query: query, skip: query.Skip(), remaining: limit, budget: budget,
				db: c.db, explain: explainCollectionPlan(query, *access), active: true, diagnostic: diagnostic,
			}
			c.db.metrics.activeCursors.Add(1)
			stop := context.AfterFunc(ctx, func() { _ = cursor.closeWithError(ctx.Err()) })
			cursor.mu.Lock()
			if cursor.closed {
				cursor.mu.Unlock()
				stop()
			} else {
				cursor.stop = stop
				cursor.mu.Unlock()
			}
			if limit == 0 {
				_ = cursor.Close()
			}
			return cursor, nil
		}
		if access != nil && access.usable {
			plannedAccess = access
		}
	}
	documents, explain, err := c.planWithBudgetAccess(ctx, query, budget, plannedAccess)
	if c != nil && c.db != nil {
		c.db.recordQuery(explain, len(documents), err, diagnostic)
	}
	if err != nil {
		return nil, err
	}
	return &Cursor{documents: documents}, nil
}

// FindOne returns the first matching document or ErrNotFound.
func (c *Collection) FindOne(ctx context.Context, filter Filter) (Document, error) {
	one := 1
	cursor, err := c.Find(ctx, filter, QueryOptions{Limit: &one})
	if err != nil {
		return nil, err
	}
	documents, err := cursor.All(ctx)
	if err != nil {
		return nil, err
	}
	if len(documents) == 0 {
		return nil, ErrNotFound
	}
	return documents[0], nil
}

// DeleteOne removes the first document matching the filter.
func (c *Collection) DeleteOne(ctx context.Context, filter Filter) (DeleteResult, error) {
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		return DeleteResult{}, err
	}
	return c.DeleteOneQuery(ctx, query)
}

// DeleteMany atomically removes all documents matching the filter.
func (c *Collection) DeleteMany(ctx context.Context, filter Filter) (DeleteResult, error) {
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		return DeleteResult{}, err
	}
	return c.DeleteManyQuery(ctx, query)
}

// DeleteOneQuery removes the first document matching a compiled query.
func (c *Collection) DeleteOneQuery(ctx context.Context, query QuerySpec) (DeleteResult, error) {
	return c.deleteQuery(ctx, query, true, 1)
}

// DeleteManyQuery atomically removes all documents matching a compiled query.
func (c *Collection) DeleteManyQuery(ctx context.Context, query QuerySpec) (DeleteResult, error) {
	return c.deleteQuery(ctx, query, false, 0)
}

// DeleteManyQueryLimited deletes matches only when their count stays within maxAffected.
func (c *Collection) DeleteManyQueryLimited(ctx context.Context, query QuerySpec, maxAffected int) (DeleteResult, error) {
	if maxAffected <= 0 {
		return DeleteResult{}, fmt.Errorf("%w: maxAffected must be positive", ErrMutationLimit)
	}
	return c.deleteQuery(ctx, query, false, maxAffected)
}

// deleteQuery validates a deletion request and sends it to the active commit path.
func (c *Collection) deleteQuery(ctx context.Context, query QuerySpec, one bool, maxAffected int) (DeleteResult, error) {
	if err := query.Validate(); err != nil {
		return DeleteResult{}, err
	}
	if err := contextError(ctx); err != nil {
		return DeleteResult{}, err
	}
	if err := c.validate(); err != nil {
		return DeleteResult{}, err
	}
	if query.HasModifiers() {
		return DeleteResult{}, fmt.Errorf("%w: mutation query cannot sort, skip, or limit", ErrInvalidFilter)
	}
	if coordinator := c.db.commitCoordinator; coordinator != nil {
		return coordinator.submitDelete(ctx, c.name, query, one, maxAffected)
	}
	c.db.mu.Lock()
	result, err := c.deleteQueryLocked(ctx, query, one, maxAffected)
	c.db.mu.Unlock()
	return result, err
}

// deleteQueryLocked commits one query-selected deletion while the caller holds db.mu.
//
// The coordinator reuses this path after a grouped request conflicts.
func (c *Collection) deleteQueryLocked(ctx context.Context, query QuerySpec, one bool, maxAffected int) (DeleteResult, error) {
	if c.db.closed {
		return DeleteResult{}, ErrClosed
	}
	if c.db.fatalErr != nil {
		return DeleteResult{}, c.db.fatalErr
	}
	data := c.db.collections[c.name]
	selectionLimit, resourceBounded := c.db.boundedMutationSelection(maxAffected, one)
	selected, err := c.selectMutationDocumentsLocked(ctx, query, one, selectionLimit)
	if err != nil {
		if resourceBounded && errors.Is(err, ErrMutationLimit) {
			c.db.metrics.resourceLimitRejections.Add(1)
			return DeleteResult{}, fmt.Errorf("%w: transaction changes exceed limit %d", ErrResourceLimit, c.db.resourceLimits.MaxTransactionChanges)
		}
		return DeleteResult{}, err
	}
	if len(selected) > 0 && data == nil {
		return DeleteResult{}, ErrCorrupt
	}
	changes := make([]Change, len(selected))
	deleted := make(map[DocumentID]struct{}, len(selected))
	for index, document := range selected {
		id, exists := document.ID()
		if !exists || id.IsZero() {
			return DeleteResult{}, ErrCorrupt
		}
		before := document.Clone()
		changes[index] = Change{Collection: c.name, Operation: DeleteOperation, DocumentID: id, Before: &before}
		deleted[id] = struct{}{}
	}
	if err := c.db.validateTransactionResource(changes); err != nil {
		return DeleteResult{}, err
	}
	var token uint64
	if len(changes) > 0 {
		token = c.db.token + 1
		if err := c.db.appendCommit(ctx, token, changes); err != nil {
			return DeleteResult{}, err
		}
		if c.db.querySource == nil {
			for _, change := range changes {
				data.deleteIndexes(change.DocumentID, *change.Before)
				delete(data.documents, change.DocumentID)
			}
			kept := data.order[:0]
			for _, id := range data.order {
				if _, remove := deleted[id]; !remove {
					kept = append(kept, id)
				}
			}
			data.order = kept
			data.rebuildPositions()
		}
		c.db.token = token
		c.db.recordLiveCommit(ChangeBatch{Token: token, Changes: changes})
	}
	if len(changes) > 0 {
		c.db.publish(ChangeBatch{Token: token, Changes: changes})
	}
	return DeleteResult{DeletedCount: int64(len(changes))}, nil
}

// DatabaseIdentity returns the database namespace identifier.
func (db *DB) DatabaseIdentity() [16]byte {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.databaseID
}

// validatePrimaryWriteFence checks external write authority under the writer admission lock.
//
// It performs no I/O itself; see PrimaryWriteFence.
func (db *DB) validatePrimaryWriteFence(nextCommitSequence uint64) error {
	if db == nil || db.replicaReadOnly || db.primaryWriteFence == nil {
		return nil
	}
	if nextCommitSequence == 0 {
		return ErrCorrupt
	}
	db.metrics.primaryWriteFenceChecks.Add(1)
	if err := db.primaryWriteFence.ValidatePrimaryWrite(PrimaryWriteFenceRequest{
		DatabaseID: db.databaseID, NextCommitSequence: nextCommitSequence,
	}); err != nil {
		db.metrics.primaryWriteFenceRejected.Add(1)
		return fmt.Errorf("%w: %w", ErrPrimaryWriteFence, err)
	}
	return nil
}

// CanResumeFrom reports whether retained in-memory history covers the requested token.
func (db *DB) CanResumeFrom(token uint64) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed || token > db.token {
		return false
	}
	if token == db.token {
		return true
	}
	if len(db.history) == 0 || token == ^uint64(0) {
		return false
	}
	return db.history[0].Token <= token+1
}

// recordCommittedBatch retains commit positions for in-memory resume checks.
func (db *DB) recordCommittedBatch(batch ChangeBatch) {
	if db.historyLimit <= 0 || len(batch.Changes) == 0 {
		return
	}
	// Resume validation needs continuity positions only. Persisting Before/After
	// images here would retain deleted or redacted data beyond the WAL lifetime.
	db.history = append(db.history, ChangeBatch{Token: batch.Token})
	if overflow := len(db.history) - db.historyLimit; overflow > 0 {
		copy(db.history, db.history[overflow:])
		db.history = db.history[:db.historyLimit]
	}
}

// validate checks that a collection handle has a database and a valid name.
func (c *Collection) validate() error {
	if c == nil || c.db == nil || !collectionNamePattern.MatchString(c.name) {
		return ErrInvalidCollection
	}
	return nil
}

// DeleteResult reports how many documents were deleted.
type DeleteResult struct{ DeletedCount int64 }

// publish sends committed changes to the dispatcher or direct consumers.
func (db *DB) publish(batch ChangeBatch) {
	if db == nil || len(batch.Changes) == 0 {
		return
	}
	if db.dispatcher != nil {
		db.metrics.publishedBatches.Add(1)
		db.metrics.publishedChanges.Add(uint64(len(batch.Changes)))
		db.dispatcher.enqueue(batch)
		return
	}
	// Constructors initialize the dispatcher. Retain the direct path only for
	// defensive compatibility with private test fixtures built before it.
	db.metrics.publishedBatches.Add(1)
	db.metrics.publishedChanges.Add(uint64(len(batch.Changes)))
	if db.reactive != nil {
		db.reactive.notify(batch)
	}
	db.deliverChangeWatchers(batch)
}

// contextError returns the cancellation error if the context is done.
func contextError(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
