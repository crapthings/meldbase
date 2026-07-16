package meldbase

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

var collectionNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,127}$`)
var fallbackDatabaseIdentityCounter atomic.Uint64

type Operation string

const (
	InsertOperation      Operation = "insert"
	UpdateOperation      Operation = "update"
	DeleteOperation      Operation = "delete"
	CreateIndexOperation Operation = "create_index"
)

type IndexDefinition struct {
	Name, Field string
	Order       int
	Unique      bool
	// Fields is the ordered definition for compound/descending indexes. Field
	// and Order remain the compatibility mirror of Fields[0] for V1 records and
	// existing callers; new code must use indexDefinitionFields.
	Fields []IndexField
}

type Change struct {
	Collection string
	Operation  Operation
	DocumentID DocumentID
	Before     *Document
	After      *Document
	Index      *IndexDefinition
}

type ChangeBatch struct {
	Token   uint64
	Changes []Change
}

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
	store                     *durableStore
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
	v2StorageLimits           V2StorageLimits
}

type collectionData struct {
	documents map[DocumentID]Document
	order     []DocumentID
	positions map[DocumentID]uint64
	indexes   map[string]*indexState
}

func newCollectionData() *collectionData {
	return &collectionData{
		documents: make(map[DocumentID]Document),
		positions: make(map[DocumentID]uint64),
		indexes:   make(map[string]*indexState),
	}
}

func (d *collectionData) rebuildPositions() {
	d.positions = make(map[DocumentID]uint64, len(d.order))
	for position, id := range d.order {
		d.positions[id] = uint64(position)
	}
}

type changeWatcher struct {
	collection string
	events     chan ChangeBatch
	done       chan error
}

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
func (db *DB) Close() error {
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
	if db.reactive != nil {
		db.reactive.close()
	}
	db.feedMu.Lock()
	for id, watcher := range db.watchers {
		delete(db.watchers, id)
		close(watcher.events)
		close(watcher.done)
	}
	db.feedMu.Unlock()
	return closeErr
}
func (db *DB) Collection(name string) *Collection { return &Collection{db: db, name: name} }

type Collection struct {
	db   *DB
	name string
}

func (c *Collection) InsertOne(ctx context.Context, document Document) (DocumentID, error) {
	ids, err := c.InsertMany(ctx, []Document{document})
	if err != nil {
		return DocumentID{}, err
	}
	return ids[0], nil
}

// InsertMany validates IDs, documents, and all unique-index keys before writing
// one WAL record. Either the entire batch becomes visible or none of it does.
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
	c.db.mu.Lock()
	if c.db.closed {
		c.db.mu.Unlock()
		return nil, ErrClosed
	}
	if c.db.fatalErr != nil {
		c.db.mu.Unlock()
		return nil, c.db.fatalErr
	}
	data := c.db.collections[c.name]
	if data != nil && c.db.querySource == nil {
		for _, id := range ids {
			if _, exists := data.documents[id]; exists {
				c.db.mu.Unlock()
				return nil, ErrDuplicateID
			}
		}
		if err := data.validateIndexBatchInsert(ids, copies); err != nil {
			c.db.mu.Unlock()
			return nil, err
		}
	}
	changes := make([]Change, len(copies))
	for index, copy := range copies {
		after := copy.Clone()
		changes[index] = Change{Collection: c.name, Operation: InsertOperation, DocumentID: ids[index], After: &after}
	}
	if err := c.db.validateTransactionResource(changes); err != nil {
		c.db.mu.Unlock()
		return nil, err
	}
	token := c.db.token + 1
	if err := c.db.appendCommit(ctx, token, changes); err != nil {
		c.db.mu.Unlock()
		return nil, err
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
	c.db.mu.Unlock()
	return append([]DocumentID(nil), ids...), nil
}

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

func (c *Collection) FindQuery(ctx context.Context, query QuerySpec) (*Cursor, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	diagnostic := c.db.beginDiagnostic(DiagnosticQuery)
	if c.db.querySource != nil {
		c.db.mu.RLock()
		if c.db.closed {
			c.db.mu.RUnlock()
			c.db.recordQuery(ExplainResult{Stage: "COLLSCAN"}, 0, ErrClosed, diagnostic)
			return nil, ErrClosed
		}
		stream, streamed, err := c.openStorageCollectionStreamLocked(ctx, query)
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
				stream: stream, query: query, skip: query.Skip(), remaining: limit,
				db: c.db, explain: ExplainResult{Stage: "COLLSCAN"}, active: true, diagnostic: diagnostic,
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
	}
	documents, explain, err := c.plan(ctx, query)
	if c != nil && c.db != nil {
		c.db.recordQuery(explain, len(documents), err, diagnostic)
	}
	if err != nil {
		return nil, err
	}
	return &Cursor{documents: documents}, nil
}

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

func (c *Collection) DeleteOne(ctx context.Context, filter Filter) (DeleteResult, error) {
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		return DeleteResult{}, err
	}
	return c.DeleteOneQuery(ctx, query)
}
func (c *Collection) DeleteMany(ctx context.Context, filter Filter) (DeleteResult, error) {
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		return DeleteResult{}, err
	}
	return c.DeleteManyQuery(ctx, query)
}
func (c *Collection) DeleteOneQuery(ctx context.Context, query QuerySpec) (DeleteResult, error) {
	return c.deleteQuery(ctx, query, true, 1)
}
func (c *Collection) DeleteManyQuery(ctx context.Context, query QuerySpec) (DeleteResult, error) {
	return c.deleteQuery(ctx, query, false, 0)
}

// DeleteManyQueryLimited atomically rejects the whole mutation when more than
// maxAffected documents match.
func (c *Collection) DeleteManyQueryLimited(ctx context.Context, query QuerySpec, maxAffected int) (DeleteResult, error) {
	if maxAffected <= 0 {
		return DeleteResult{}, fmt.Errorf("%w: maxAffected must be positive", ErrMutationLimit)
	}
	return c.deleteQuery(ctx, query, false, maxAffected)
}
func (c *Collection) deleteQuery(ctx context.Context, query QuerySpec, one bool, maxAffected int) (DeleteResult, error) {
	if err := contextError(ctx); err != nil {
		return DeleteResult{}, err
	}
	if err := c.validate(); err != nil {
		return DeleteResult{}, err
	}
	if query.HasModifiers() {
		return DeleteResult{}, fmt.Errorf("%w: mutation query cannot sort, skip, or limit", ErrInvalidFilter)
	}
	c.db.mu.Lock()
	if c.db.closed {
		c.db.mu.Unlock()
		return DeleteResult{}, ErrClosed
	}
	if c.db.fatalErr != nil {
		c.db.mu.Unlock()
		return DeleteResult{}, c.db.fatalErr
	}
	data := c.db.collections[c.name]
	selectionLimit, resourceBounded := c.db.boundedMutationSelection(maxAffected, one)
	selected, err := c.selectMutationDocumentsLocked(ctx, query, one, selectionLimit)
	if err != nil {
		c.db.mu.Unlock()
		if resourceBounded && errors.Is(err, ErrMutationLimit) {
			c.db.metrics.resourceLimitRejections.Add(1)
			return DeleteResult{}, fmt.Errorf("%w: transaction changes exceed limit %d", ErrResourceLimit, c.db.resourceLimits.MaxTransactionChanges)
		}
		return DeleteResult{}, err
	}
	if len(selected) > 0 && data == nil {
		c.db.mu.Unlock()
		return DeleteResult{}, ErrCorrupt
	}
	changes := make([]Change, len(selected))
	deleted := make(map[DocumentID]struct{}, len(selected))
	for index, document := range selected {
		id, exists := document.ID()
		if !exists || id.IsZero() {
			c.db.mu.Unlock()
			return DeleteResult{}, ErrCorrupt
		}
		before := document.Clone()
		changes[index] = Change{Collection: c.name, Operation: DeleteOperation, DocumentID: id, Before: &before}
		deleted[id] = struct{}{}
	}
	if err := c.db.validateTransactionResource(changes); err != nil {
		c.db.mu.Unlock()
		return DeleteResult{}, err
	}
	var token uint64
	if len(changes) > 0 {
		token = c.db.token + 1
		if err := c.db.appendCommit(ctx, token, changes); err != nil {
			c.db.mu.Unlock()
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
	c.db.mu.Unlock()
	return DeleteResult{DeletedCount: int64(len(changes))}, nil
}

func (db *DB) DatabaseIdentity() [16]byte {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.databaseID
}

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

func (c *Collection) validate() error {
	if c == nil || c.db == nil || !collectionNamePattern.MatchString(c.name) {
		return ErrInvalidCollection
	}
	return nil
}

type DeleteResult struct{ DeletedCount int64 }

type Cursor struct {
	mu         sync.Mutex
	documents  []Document
	position   int
	stream     queryStorageDocumentIterator
	query      QuerySpec
	skip       int
	remaining  int
	db         *DB
	explain    ExplainResult
	recorded   bool
	returned   int
	closed     bool
	active     bool
	diagnostic diagnosticSpan
	stop       func() bool
}

func (c *Cursor) Next(ctx context.Context) (Document, bool, error) {
	if c == nil {
		return nil, false, ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := contextError(ctx); err != nil {
		_ = c.finishLocked(err)
		return nil, false, err
	}
	if c.closed {
		return nil, false, nil
	}
	if c.stream != nil {
		if c.remaining == 0 {
			return nil, false, c.finishLocked(nil)
		}
		for c.stream.Next() {
			record := c.stream.Record()
			candidate, err := decodeQueryStorageCandidate(record)
			if err != nil {
				_ = c.finishLocked(err)
				return nil, false, err
			}
			c.explain.DocumentsExamined++
			if !c.query.Match(candidate.document) {
				continue
			}
			if c.skip > 0 {
				c.skip--
				continue
			}
			document := candidate.document.Clone()
			c.returned++
			if c.remaining > 0 {
				c.remaining--
				if c.remaining == 0 {
					if err := c.finishLocked(nil); err != nil {
						return nil, false, err
					}
				}
			}
			return document, true, nil
		}
		err := c.stream.Err()
		if finishErr := c.finishLocked(err); err == nil {
			err = finishErr
		}
		return nil, false, err
	}
	if c.position >= len(c.documents) {
		c.closed = true
		return nil, false, nil
	}
	// Cursor owns already-detached query results. Transfer this document to the
	// caller and clear our reference instead of cloning a second time.
	document := c.documents[c.position]
	c.documents[c.position] = nil
	c.position++
	return document, true, nil
}
func (c *Cursor) All(ctx context.Context) ([]Document, error) {
	if c == nil {
		return nil, ErrClosed
	}
	c.mu.Lock()
	streamed := c.stream != nil
	if !streamed {
		defer c.mu.Unlock()
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		result := append([]Document(nil), c.documents[c.position:]...)
		for index := c.position; index < len(c.documents); index++ {
			c.documents[index] = nil
		}
		c.position = len(c.documents)
		c.closed = true
		return result, nil
	}
	c.mu.Unlock()
	result := make([]Document, 0)
	for {
		document, exists, err := c.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !exists {
			return result, nil
		}
		result = append(result, document)
	}
}

// Close releases a pinned storage snapshot held by a lazy cursor. It is safe to
// call repeatedly. Exhaustion, limit completion, errors and context cancellation
// close automatically; callers that stop early must close explicitly.
func (c *Cursor) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finishLocked(nil)
}

func (c *Cursor) closeWithError(err error) error {
	if c == nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finishLocked(err)
}

func (c *Cursor) finishLocked(operationErr error) error {
	if c.closed {
		return operationErr
	}
	c.closed = true
	closeErr := error(nil)
	if c.stream != nil {
		closeErr = c.stream.Close()
		c.stream = nil
	} else if c.position < len(c.documents) {
		for index := c.position; index < len(c.documents); index++ {
			c.documents[index] = nil
		}
		c.position = len(c.documents)
	}
	if c.stop != nil {
		c.stop()
		c.stop = nil
	}
	resultErr := operationErr
	if resultErr == nil {
		resultErr = closeErr
	}
	if c.db != nil && !c.recorded {
		c.recorded = true
		c.db.recordQuery(c.explain, c.returned, resultErr, c.diagnostic)
	}
	if c.db != nil && c.active {
		c.active = false
		c.db.metrics.activeCursors.Add(^uint64(0))
	}
	return resultErr
}

func (db *DB) WatchChanges(ctx context.Context, collection string, buffer int) (<-chan ChangeBatch, <-chan error, error) {
	if !collectionNamePattern.MatchString(collection) {
		return nil, nil, ErrInvalidCollection
	}
	if buffer <= 0 || buffer > 4096 {
		return nil, nil, fmt.Errorf("invalid change buffer")
	}
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return nil, nil, ErrClosed
	}
	db.feedMu.Lock()
	db.nextWatcher++
	id := db.nextWatcher
	watcher := &changeWatcher{collection: collection, events: make(chan ChangeBatch, buffer), done: make(chan error, 1)}
	db.watchers[id] = watcher
	db.feedMu.Unlock()
	go func() {
		<-ctx.Done()
		db.feedMu.Lock()
		if current, ok := db.watchers[id]; ok {
			delete(db.watchers, id)
			current.done <- ctx.Err()
			close(current.events)
			close(current.done)
		}
		db.feedMu.Unlock()
	}()
	return watcher.events, watcher.done, nil
}

func (db *DB) publish(batch ChangeBatch) {
	db.metrics.publishedBatches.Add(1)
	db.metrics.publishedChanges.Add(uint64(len(batch.Changes)))
	if db.reactive != nil {
		db.reactive.notify(batch)
	}
	db.feedMu.Lock()
	defer db.feedMu.Unlock()
	for id, watcher := range db.watchers {
		filtered := make([]Change, 0, len(batch.Changes))
		for _, change := range batch.Changes {
			if watcher.collection == change.Collection {
				filtered = append(filtered, cloneChange(change))
			}
		}
		if len(filtered) == 0 {
			continue
		}
		select {
		case watcher.events <- ChangeBatch{Token: batch.Token, Changes: filtered}:
			db.metrics.watcherDeliveries.Add(1)
		default:
			db.metrics.slowConsumers.Add(1)
			delete(db.watchers, id)
			watcher.done <- ErrSlowConsumer
			close(watcher.events)
			close(watcher.done)
		}
	}
}

func cloneChange(change Change) Change {
	if change.Before != nil {
		value := change.Before.Clone()
		change.Before = &value
	}
	if change.After != nil {
		value := change.After.Clone()
		change.After = &value
	}
	if change.Index != nil {
		value := *change.Index
		change.Index = &value
	}
	return change
}
func contextError(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
