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
	mu           sync.RWMutex
	closed       bool
	collections  map[string]*collectionData
	token        uint64
	feedMu       sync.Mutex
	nextWatcher  uint64
	watchers     map[uint64]*changeWatcher
	store        *durableStore
	fatalErr     error
	databaseID   [16]byte
	history      []ChangeBatch
	historyLimit int
}

type collectionData struct {
	documents map[DocumentID]Document
	order     []DocumentID
	indexes   map[string]*indexState
}

func newCollectionData() *collectionData {
	return &collectionData{documents: make(map[DocumentID]Document), indexes: make(map[string]*indexState)}
}

type changeWatcher struct {
	collection string
	events     chan ChangeBatch
	done       chan error
}

func New() *DB {
	db := &DB{collections: make(map[string]*collectionData), watchers: make(map[uint64]*changeWatcher), historyLimit: 1024}
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
	return db
}
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	var closeErr error
	if db.store != nil {
		err := db.fatalErr
		if err == nil {
			blobs, snapshotErr := encodeCheckpointBlobs(db.collections, db.history)
			err = snapshotErr
			if err == nil {
				err = db.store.pages.CheckpointBlobs(db.token, blobs)
			}
			if err == nil {
				err = db.store.log.Reset(db.token)
			}
		}
		closeErr = errors.Join(err, db.store.close())
	}
	db.closed = true
	db.mu.Unlock()
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
	if data != nil {
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
	token := c.db.token + 1
	if err := c.db.appendCommit(token, changes); err != nil {
		c.db.mu.Unlock()
		return nil, err
	}
	if data == nil {
		data = newCollectionData()
		c.db.collections[c.name] = data
	}
	for index, copy := range copies {
		id := ids[index]
		data.documents[id] = copy
		data.order = append(data.order, id)
		data.insertIndexes(id, copy)
	}
	c.db.token = token
	batch := ChangeBatch{Token: token, Changes: changes}
	c.db.recordCommittedBatch(batch)
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
	documents, _, err := c.plan(ctx, query)
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
	if data == nil {
		c.db.mu.Unlock()
		return DeleteResult{}, nil
	}
	changes := []Change{}
	kept := make([]DocumentID, 0, len(data.order))
	for _, id := range data.order {
		document, exists := data.documents[id]
		if !exists {
			continue
		}
		if query.Match(document) && (!one || len(changes) == 0) {
			before := document.Clone()
			changes = append(changes, Change{Collection: c.name, Operation: DeleteOperation, DocumentID: id, Before: &before})
			if maxAffected > 0 && len(changes) > maxAffected {
				c.db.mu.Unlock()
				return DeleteResult{}, ErrMutationLimit
			}
			continue
		}
		kept = append(kept, id)
	}
	var token uint64
	if len(changes) > 0 {
		token = c.db.token + 1
		if err := c.db.appendCommit(token, changes); err != nil {
			c.db.mu.Unlock()
			return DeleteResult{}, err
		}
		for _, change := range changes {
			data.deleteIndexes(change.DocumentID, *change.Before)
			delete(data.documents, change.DocumentID)
		}
		data.order = kept
		c.db.token = token
		c.db.recordCommittedBatch(ChangeBatch{Token: token, Changes: changes})
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
	documents []Document
	position  int
}

func (c *Cursor) Next(ctx context.Context) (Document, bool, error) {
	if err := contextError(ctx); err != nil {
		return nil, false, err
	}
	if c.position >= len(c.documents) {
		return nil, false, nil
	}
	document := c.documents[c.position].Clone()
	c.position++
	return document, true, nil
}
func (c *Cursor) All(ctx context.Context) ([]Document, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	result := make([]Document, len(c.documents)-c.position)
	for i := range result {
		result[i] = c.documents[c.position+i].Clone()
	}
	c.position = len(c.documents)
	return result, nil
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
		default:
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
