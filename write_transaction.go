package meldbase

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"

	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
	"github.com/crapthings/meldbase/internal/systemrecord"
)

// WriteTransaction is a short-lived snapshot write view. It provides point
// operations with optimistic serializable commit validation. Values returned
// from it are isolated clones.
//
// A transaction is active only during its handler callback. Handlers must not
// retain it or call normal DB/Collection methods from inside the callback.
type WriteTransaction struct {
	db              *DB
	ctx             context.Context
	active          bool
	snapshot        queryStorageSnapshot
	baseToken       uint64
	entries         map[writeTransactionKey]*writeTransactionEntry
	collections     map[string]storagev2.CollectionPrecondition
	systemMutations []systemrecord.Mutation
	commitHooks     []func(uint64)
	systemBytes     uint64
	overlayBytes    uint64
}

// MeldbaseStageSystemMutation is first-party composite-transaction plumbing.
// Its internal parameter type prevents external applications from accessing the
// private System tree. onCommit runs synchronously after the durable root is
// published and before the matching business ChangeBatch becomes visible; it
// must be bounded, non-blocking and must not call back into the database.
func (tx *WriteTransaction) MeldbaseStageSystemMutation(mutation systemrecord.Mutation, onCommit func(uint64)) error {
	if tx == nil || !tx.active || tx.db == nil {
		return ErrClosed
	}
	if err := contextError(tx.ctx); err != nil {
		return err
	}
	if len(tx.systemMutations) >= 255 || len(mutation.Key) == 0 || len(mutation.Key) > 256 ||
		(!mutation.Delete && (len(mutation.NewValue) == 0 || len(mutation.NewValue) > 16*1024*1024+64*1024)) ||
		(mutation.Delete && len(mutation.NewValue) != 0) ||
		mutation.TransactionID != [16]byte{} ||
		(!mutation.ExpectedExists && mutation.ExpectedHash != [32]byte{}) ||
		(mutation.Unconditional && (mutation.ExpectedExists || mutation.Delete)) {
		return ErrInvalidDocument
	}
	for _, existing := range tx.systemMutations {
		if string(existing.Key) == string(mutation.Key) {
			return ErrInvalidDocument
		}
	}
	added := uint64(len(mutation.Key)) + uint64(len(mutation.NewValue))
	retained := tx.overlayBytes + tx.systemBytes
	if retained < tx.overlayBytes || retained > tx.db.resourceLimits.MaxTransactionBytes || added > tx.db.resourceLimits.MaxTransactionBytes-retained {
		tx.db.metrics.resourceLimitRejections.Add(1)
		return fmt.Errorf("%w: transaction overlay bytes exceed limit %d", ErrResourceLimit, tx.db.resourceLimits.MaxTransactionBytes)
	}
	mutation.Key = append([]byte(nil), mutation.Key...)
	mutation.NewValue = append([]byte(nil), mutation.NewValue...)
	tx.systemMutations = append(tx.systemMutations, mutation)
	tx.systemBytes += added
	if onCommit != nil {
		tx.commitHooks = append(tx.commitHooks, onCommit)
	}
	return nil
}

type writeTransactionKey struct {
	collection string
	id         DocumentID
}

type writeTransactionEntry struct {
	base         Document
	current      Document
	baseHash     [32]byte
	baseBytes    uint64
	currentBytes uint64
	exists       bool
}

// Find evaluates query against the transaction's immutable V2 snapshot plus
// its own staged point writes. It installs a collection snapshot fence, so any
// concurrent document or published-index change in that collection causes the
// eventual commit to return ErrWriteConflict rather than admitting a phantom.
//
// The first range-read primitive intentionally uses a collection-wide fence.
// It is serializable and independent of a process-local index plan; a future
// narrower predicate-fence implementation can refine conflicts without
// weakening this API's correctness contract.
func (tx *WriteTransaction) Find(collection string, query QuerySpec) (QuerySnapshot, error) {
	if err := tx.validateCollection(collection); err != nil {
		return QuerySnapshot{}, err
	}
	if _, err := MarshalQuerySpecJSON(query); err != nil {
		return QuerySnapshot{}, err
	}
	version, exists, err := tx.snapshot.CollectionVersion(collection)
	if err != nil {
		return QuerySnapshot{}, err
	}
	if err := tx.recordCollectionRead(collection, exists, version); err != nil {
		return QuerySnapshot{}, err
	}
	iterator, err := tx.snapshot.OpenCollectionIterator(collection)
	if err != nil {
		return QuerySnapshot{}, err
	}
	defer iterator.Close()
	candidates := make([]queryCandidate, 0)
	seen := make(map[DocumentID]struct{})
	var candidateBytes uint64
	add := func(document Document, position uint64) error {
		if document == nil || position == 0 || !query.Match(document) {
			return nil
		}
		if uint64(len(candidates)) >= tx.db.resourceLimits.MaxTransactionChanges {
			tx.db.metrics.resourceLimitRejections.Add(1)
			return fmt.Errorf("%w: transaction query candidates exceed limit %d", ErrResourceLimit, tx.db.resourceLimits.MaxTransactionChanges)
		}
		size, err := canonicalDocumentSize(document)
		if err != nil {
			return err
		}
		if candidateBytes > tx.db.resourceLimits.MaxTransactionBytes || size > tx.db.resourceLimits.MaxTransactionBytes-candidateBytes {
			tx.db.metrics.resourceLimitRejections.Add(1)
			return fmt.Errorf("%w: transaction query candidates exceed byte limit %d", ErrResourceLimit, tx.db.resourceLimits.MaxTransactionBytes)
		}
		candidateBytes += size
		candidates = append(candidates, queryCandidate{document: document, position: position})
		return nil
	}
	for iterator.Next() {
		record := iterator.Record()
		candidate, err := decodeQueryStorageCandidate(record)
		if err != nil {
			return QuerySnapshot{}, err
		}
		id := candidate.documentID()
		if _, duplicate := seen[id]; duplicate {
			return QuerySnapshot{}, ErrCorrupt
		}
		seen[id] = struct{}{}
		if entry := tx.entries[writeTransactionKey{collection: collection, id: id}]; entry != nil {
			if err := add(entry.current, candidate.position); err != nil {
				return QuerySnapshot{}, err
			}
			continue
		}
		if err := add(candidate.document, candidate.position); err != nil {
			return QuerySnapshot{}, err
		}
	}
	if err := iterator.Err(); err != nil {
		return QuerySnapshot{}, err
	}
	if err := iterator.Close(); err != nil {
		return QuerySnapshot{}, err
	}

	inserted := make([]writeTransactionKey, 0)
	for key, entry := range tx.entries {
		if key.collection == collection && !entry.exists && entry.current != nil {
			inserted = append(inserted, key)
		}
	}
	sort.Slice(inserted, func(left, right int) bool { return string(inserted[left].id[:]) < string(inserted[right].id[:]) })
	position := uint64(0)
	if exists {
		position = version.NextDocumentPosition
	}
	for _, key := range inserted {
		if position == ^uint64(0) {
			return QuerySnapshot{}, ErrCorrupt
		}
		position++
		if err := add(tx.entries[key].current, position); err != nil {
			return QuerySnapshot{}, err
		}
	}
	return QuerySnapshot{Token: tx.baseToken, Documents: query.executeMatched(candidates)}, nil
}

// GetOne returns one document by intrinsic ID from the transaction's current
// view, including earlier point mutations in the same callback.
func (tx *WriteTransaction) GetOne(collection string, id DocumentID) (Document, error) {
	entry, err := tx.load(collection, id)
	if err != nil {
		return nil, err
	}
	if entry.current == nil {
		return nil, ErrNotFound
	}
	return entry.current.Clone(), nil
}

// InsertOne stages one insert and returns its generated or supplied ID.
func (tx *WriteTransaction) InsertOne(collection string, document Document) (DocumentID, error) {
	if err := tx.validateCollection(collection); err != nil {
		return DocumentID{}, err
	}
	copy := document.Clone()
	if copy == nil {
		copy = Document{}
	}
	if err := copy.Validate(); err != nil {
		return DocumentID{}, err
	}
	id, hasID := copy.ID()
	if _, present := copy["_id"]; present && !hasID {
		return DocumentID{}, fmt.Errorf("%w: _id must be DocumentID", ErrInvalidDocument)
	}
	if !hasID {
		var err error
		id, err = NewDocumentID()
		if err != nil {
			return DocumentID{}, err
		}
		copy["_id"] = ID(id)
	}
	if id.IsZero() {
		return DocumentID{}, fmt.Errorf("%w: zero _id", ErrInvalidDocument)
	}
	entry, err := tx.load(collection, id)
	if err != nil {
		return DocumentID{}, err
	}
	if entry.current != nil {
		return DocumentID{}, ErrDuplicateID
	}
	if err := tx.setCurrent(entry, copy); err != nil {
		return DocumentID{}, err
	}
	return id, nil
}

// ReplaceOne stages a full replacement for an existing document. The supplied
// document may omit _id; if present it must equal id.
func (tx *WriteTransaction) ReplaceOne(collection string, id DocumentID, document Document) error {
	entry, err := tx.load(collection, id)
	if err != nil {
		return err
	}
	if entry.current == nil {
		return ErrNotFound
	}
	copy := document.Clone()
	if copy == nil {
		copy = Document{}
	}
	if existing, present := copy["_id"]; present {
		value, ok := existing.IDValue()
		if !ok || value != id {
			return ErrImmutableID
		}
	} else {
		copy["_id"] = ID(id)
	}
	if err := copy.Validate(); err != nil {
		return err
	}
	return tx.setCurrent(entry, copy)
}

// UpdateOne applies one already compiled, data-only mutation to the
// transaction's current document view. Earlier writes in the same transaction
// are visible and the intrinsic _id remains immutable.
func (tx *WriteTransaction) UpdateOne(collection string, id DocumentID, mutation MutationSpec) error {
	entry, err := tx.load(collection, id)
	if err != nil {
		return err
	}
	if entry.current == nil {
		return ErrNotFound
	}
	after, err := mutation.Apply(entry.current)
	if err != nil {
		return err
	}
	currentID, ok := after.ID()
	if !ok || currentID != id {
		return ErrImmutableID
	}
	return tx.setCurrent(entry, after)
}

// DeleteOne stages a point delete. Deleting a document inserted earlier in the
// same callback cancels that insert without producing a storage mutation.
func (tx *WriteTransaction) DeleteOne(collection string, id DocumentID) error {
	entry, err := tx.load(collection, id)
	if err != nil {
		return err
	}
	if entry.current == nil {
		return ErrNotFound
	}
	return tx.setCurrent(entry, nil)
}

func (tx *WriteTransaction) validateCollection(collection string) error {
	if tx == nil || !tx.active || tx.db == nil {
		return ErrClosed
	}
	if err := contextError(tx.ctx); err != nil {
		return err
	}
	if !collectionNamePattern.MatchString(collection) {
		return ErrInvalidCollection
	}
	return nil
}

func (tx *WriteTransaction) load(collection string, id DocumentID) (*writeTransactionEntry, error) {
	if err := tx.validateCollection(collection); err != nil {
		return nil, err
	}
	if id.IsZero() {
		return nil, ErrInvalidDocument
	}
	key := writeTransactionKey{collection: collection, id: id}
	if entry := tx.entries[key]; entry != nil {
		return entry, nil
	}
	if uint64(len(tx.entries)) >= tx.db.resourceLimits.MaxTransactionChanges {
		tx.db.metrics.resourceLimitRejections.Add(1)
		return nil, fmt.Errorf("%w: transaction overlay entries exceed limit %d", ErrResourceLimit, tx.db.resourceLimits.MaxTransactionChanges)
	}
	if tx.snapshot == nil || tx.snapshot.Sequence() != tx.baseToken {
		return nil, ErrCorrupt
	}
	record, exists, err := tx.snapshot.GetDocumentRecord(collection, id)
	if err != nil {
		return nil, err
	}
	entry := &writeTransactionEntry{exists: exists}
	if exists {
		if len(record.Encoded) == 0 {
			return nil, ErrCorrupt
		}
		size, err := tx.db.validateDocumentResource(record.Decoded)
		if err != nil {
			return nil, err
		}
		if size > tx.db.resourceLimits.MaxTransactionBytes/2 || tx.overlayBytes > tx.db.resourceLimits.MaxTransactionBytes-2*size {
			tx.db.metrics.resourceLimitRejections.Add(1)
			return nil, fmt.Errorf("%w: transaction overlay bytes exceed limit %d", ErrResourceLimit, tx.db.resourceLimits.MaxTransactionBytes)
		}
		// The storage document/cache value is immutable. Retain it as the base
		// and clone only the callback's mutable overlay value, avoiding a third
		// full document copy during admission.
		entry.base, entry.current = record.Decoded, record.Decoded.Clone()
		entry.baseHash = sha256.Sum256(record.Encoded)
		entry.baseBytes, entry.currentBytes = size, size
		tx.overlayBytes += 2 * size
	}
	tx.entries[key] = entry
	return entry, nil
}

func (tx *WriteTransaction) recordCollectionRead(collection string, exists bool, version queryStorageCollectionVersion) error {
	if tx == nil || !tx.active || tx.collections == nil {
		return ErrClosed
	}
	precondition := storagev2.CollectionPrecondition{Collection: collection, ExpectedExists: exists}
	if exists {
		if version.ID == 0 || version.UpdatedSequence == 0 {
			return ErrCorrupt
		}
		precondition.ExpectedID = version.ID
		precondition.ExpectedUpdatedSequence = version.UpdatedSequence
	}
	if current, recorded := tx.collections[collection]; recorded {
		if current != precondition {
			return ErrCorrupt
		}
		return nil
	}
	tx.collections[collection] = precondition
	return nil
}

// setCurrent replaces one retained overlay value without cumulative accounting
// across repeated updates. The immutable base copy remains charged until the
// callback finishes because changes and point preconditions still need it.
func (tx *WriteTransaction) setCurrent(entry *writeTransactionEntry, document Document) error {
	if tx == nil || tx.db == nil || entry == nil || tx.overlayBytes < entry.currentBytes {
		return ErrCorrupt
	}
	size := uint64(0)
	if document != nil {
		var err error
		size, err = tx.db.validateDocumentResource(document)
		if err != nil {
			return err
		}
	}
	retained := tx.overlayBytes - entry.currentBytes
	if retained > tx.db.resourceLimits.MaxTransactionBytes || size > tx.db.resourceLimits.MaxTransactionBytes-retained {
		tx.db.metrics.resourceLimitRejections.Add(1)
		return fmt.Errorf("%w: transaction overlay bytes exceed limit %d", ErrResourceLimit, tx.db.resourceLimits.MaxTransactionBytes)
	}
	entry.current = document
	entry.currentBytes = size
	tx.overlayBytes = retained + size
	return nil
}

func (tx *WriteTransaction) preconditions() []storagev2.DocumentPrecondition {
	keys := make([]writeTransactionKey, 0, len(tx.entries))
	for key := range tx.entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].collection != keys[j].collection {
			return keys[i].collection < keys[j].collection
		}
		return string(keys[i].id[:]) < string(keys[j].id[:])
	})
	result := make([]storagev2.DocumentPrecondition, len(keys))
	for index, key := range keys {
		entry := tx.entries[key]
		result[index] = storagev2.DocumentPrecondition{
			Collection: key.collection, DocumentID: [16]byte(key.id),
			ExpectedExists: entry.exists, ExpectedHash: entry.baseHash,
		}
	}
	return result
}

func (tx *WriteTransaction) collectionPreconditions() []storagev2.CollectionPrecondition {
	collections := make([]string, 0, len(tx.collections))
	for collection := range tx.collections {
		collections = append(collections, collection)
	}
	sort.Strings(collections)
	result := make([]storagev2.CollectionPrecondition, len(collections))
	for index, collection := range collections {
		result[index] = tx.collections[collection]
	}
	return result
}

func (tx *WriteTransaction) changes() []Change {
	keys := make([]writeTransactionKey, 0, len(tx.entries))
	for key := range tx.entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].collection != keys[j].collection {
			return keys[i].collection < keys[j].collection
		}
		return string(keys[i].id[:]) < string(keys[j].id[:])
	})
	changes := make([]Change, 0, len(keys))
	for _, key := range keys {
		entry := tx.entries[key]
		switch {
		case !entry.exists && entry.current != nil:
			after := entry.current.Clone()
			changes = append(changes, Change{Collection: key.collection, Operation: InsertOperation, DocumentID: key.id, After: &after})
		case entry.exists && entry.current == nil:
			before := entry.base.Clone()
			changes = append(changes, Change{Collection: key.collection, Operation: DeleteOperation, DocumentID: key.id, Before: &before})
		case entry.exists && entry.current != nil && !entry.base.Equal(entry.current):
			before, after := entry.base.Clone(), entry.current.Clone()
			changes = append(changes, Change{Collection: key.collection, Operation: UpdateOperation, DocumentID: key.id, Before: &before, After: &after})
		}
	}
	return changes
}

func (db *DB) beginWriteTransaction(ctx context.Context) (*WriteTransaction, *v2DurableStore, error) {
	if db == nil || ctx == nil {
		return nil, nil, ErrClosed
	}
	if err := contextError(ctx); err != nil {
		return nil, nil, err
	}
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, nil, ErrClosed
	}
	if db.fatalErr != nil {
		db.mu.RUnlock()
		return nil, nil, db.fatalErr
	}
	store, ok := db.durability.(*v2DurableStore)
	if !ok || store.file == nil || db.querySource == nil {
		db.mu.RUnlock()
		return nil, nil, ErrWriteTransactionUnsupported
	}
	baseToken := db.token
	snapshot, err := db.querySource.openQuerySnapshot()
	db.mu.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	if snapshot.Sequence() != baseToken {
		_ = snapshot.Close()
		return nil, nil, ErrWriteConflict
	}
	tx := &WriteTransaction{
		db: db, ctx: ctx, active: true, snapshot: snapshot, baseToken: baseToken,
		entries: make(map[writeTransactionKey]*writeTransactionEntry), collections: make(map[string]storagev2.CollectionPrecondition),
	}
	return tx, store, nil
}

func (tx *WriteTransaction) finish() error {
	if tx == nil {
		return nil
	}
	tx.active = false
	if tx.snapshot == nil {
		return nil
	}
	err := tx.snapshot.Close()
	tx.snapshot = nil
	return err
}

// commitWriteTransaction is the single V2 business/System publication path
// shared by public transactions and first-party transactional RPC. The bool is
// false when optimistic validation failed before storage publication began.
func (db *DB) commitWriteTransaction(
	ctx context.Context,
	tx *WriteTransaction,
	store *v2DurableStore,
	changes []Change,
	systemMutations []systemrecord.Mutation,
) (systemrecord.Result, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return systemrecord.Result{}, false, ErrClosed
	}
	if db.fatalErr != nil {
		return systemrecord.Result{}, false, db.fatalErr
	}
	currentStore, ok := db.durability.(*v2DurableStore)
	if !ok || currentStore != store || tx == nil {
		return systemrecord.Result{}, false, ErrWriteConflict
	}
	return db.commitPreparedWriteTransactionLocked(ctx, store, changes, systemMutations, tx.preconditions(), tx.collectionPreconditions(), tx.commitHooks)
}

// commitPreparedWriteTransactionLocked publishes already-frozen V2 point
// writes while db.mu is held. It is shared by the direct transaction path and
// the CommitCoordinator's conflict fallback: neither path reevaluates a user
// callback after its snapshot has closed.
func (db *DB) commitPreparedWriteTransactionLocked(
	ctx context.Context,
	store *v2DurableStore,
	changes []Change,
	systemMutations []systemrecord.Mutation,
	preconditions []storagev2.DocumentPrecondition,
	collectionPreconditions []storagev2.CollectionPrecondition,
	commitHooks []func(uint64),
) (systemrecord.Result, bool, error) {
	if db == nil || store == nil || len(changes) == 0 {
		return systemrecord.Result{}, false, ErrCorrupt
	}
	if db.closed {
		return systemrecord.Result{}, false, ErrClosed
	}
	if db.fatalErr != nil {
		return systemrecord.Result{}, false, db.fatalErr
	}
	currentStore, ok := db.durability.(*v2DurableStore)
	if !ok || currentStore != store {
		return systemrecord.Result{}, false, ErrWriteConflict
	}
	token := db.token + 1
	if token == 0 {
		return systemrecord.Result{}, true, ErrCorrupt
	}
	diagnostic := db.beginDiagnostic(DiagnosticCommit)
	result, err := store.appendDBCommitWithSystem(ctx, db, token, changes, systemMutations, preconditions, collectionPreconditions)
	db.finishCommitDiagnostic(diagnostic, len(changes), err)
	if err != nil || !result.Applied {
		return result, true, err
	}
	for _, change := range changes {
		if db.collections[change.Collection] == nil {
			db.collections[change.Collection] = newCollectionData()
		}
	}
	db.token = token
	for _, hook := range commitHooks {
		hook(token)
	}
	batch := ChangeBatch{Token: token, Changes: changes}
	db.recordLiveCommit(batch)
	db.publish(batch)
	return result, true, nil
}

func (db *DB) validateWriteTransactionSnapshot(tx *WriteTransaction, store *v2DurableStore) error {
	if db == nil || tx == nil || store == nil {
		return ErrClosed
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	if db.fatalErr != nil {
		return db.fatalErr
	}
	currentStore, ok := db.durability.(*v2DurableStore)
	if !ok || currentStore != store {
		return ErrWriteConflict
	}
	preconditions := tx.preconditions()
	collectionPreconditions := tx.collectionPreconditions()
	if len(preconditions) == 0 && len(collectionPreconditions) == 0 {
		return nil
	}
	if err := mapStorageV2Error(store.file.ValidateCollectionPreconditions(collectionPreconditions)); err != nil {
		return err
	}
	return mapStorageV2Error(store.file.ValidateDocumentPreconditions(preconditions))
}

// RunWriteTransaction executes build against one immutable Storage V2
// snapshot and atomically publishes all staged point mutations if every
// document read by the callback still matches. The callback runs without
// the database writer lock. A conflicting point write returns ErrWriteConflict;
// callbacks are never retried because they may contain application side effects.
//
// A successful callback with no effective changes is a successful no-op and
// does not advance the commit sequence. The transaction is invalid as soon as
// the callback returns. Normal DB and Collection methods must not be called
// from inside build.
func (db *DB) RunWriteTransaction(ctx context.Context, build func(*WriteTransaction) error) error {
	if build == nil {
		return ErrClosed
	}
	tx, store, err := db.beginWriteTransaction(ctx)
	if err != nil {
		return err
	}
	db.metrics.writeTransactionsStarted.Add(1)
	db.metrics.writeTransactionsActive.Add(1)
	outcome := writeTransactionAborted
	defer func() {
		db.metrics.writeTransactionsActive.Add(^uint64(0))
		switch outcome {
		case writeTransactionCommitted:
			db.metrics.writeTransactionsCommitted.Add(1)
		case writeTransactionNoop:
			db.metrics.writeTransactionsNoops.Add(1)
		case writeTransactionConflict:
			db.metrics.writeTransactionsConflicts.Add(1)
		default:
			db.metrics.writeTransactionsAborted.Add(1)
		}
	}()
	defer tx.finish()
	buildErr := build(tx)
	closeErr := tx.finish()
	if buildErr != nil {
		return buildErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if len(tx.systemMutations) != 0 || len(tx.commitHooks) != 0 {
		return ErrInvalidDocument
	}
	changes := tx.changes()
	if len(changes) == 0 {
		if err := db.validateWriteTransactionSnapshot(tx, store); err != nil {
			if errors.Is(err, ErrWriteConflict) {
				outcome = writeTransactionConflict
			}
			return err
		}
		outcome = writeTransactionNoop
		return nil
	}
	if err := db.validateTransactionResource(changes); err != nil {
		return err
	}

	var result systemrecord.Result
	if coordinator := db.commitCoordinator; coordinator != nil {
		err = coordinator.submitWriteTransaction(ctx, changes, tx.preconditions(), tx.collectionPreconditions())
		result.Applied = err == nil
	} else {
		result, _, err = db.commitWriteTransaction(ctx, tx, store, changes, nil)
	}
	if err == nil && !result.Applied {
		return ErrCorrupt
	}
	if errors.Is(err, ErrWriteConflict) {
		outcome = writeTransactionConflict
	} else if err == nil {
		outcome = writeTransactionCommitted
	}
	return err
}

type writeTransactionOutcome uint8

const (
	writeTransactionAborted writeTransactionOutcome = iota
	writeTransactionCommitted
	writeTransactionNoop
	writeTransactionConflict
)

// MeldbaseSystemWrite runs build against one immutable V2 snapshot without
// holding the database writer lock. If build succeeds and its point read set is
// still valid, its business changes and systemMutation commit in one V2 generation.
// The bool reports whether a composite commit was attempted; false means build
// produced no business change or lost optimistic validation.
//
// This method is first-party plumbing: the systemrecord parameter is internal,
// preventing external applications from using the private keyspace.
func (db *DB) MeldbaseSystemWrite(ctx context.Context, systemMutation systemrecord.Mutation, build func(*WriteTransaction) ([]byte, error)) (systemrecord.Result, bool, error) {
	if build == nil {
		return systemrecord.Result{}, false, ErrClosed
	}
	tx, store, err := db.beginWriteTransaction(ctx)
	if err != nil {
		return systemrecord.Result{}, false, err
	}
	defer tx.finish()
	terminal, err := build(tx)
	closeErr := tx.finish()
	if err != nil {
		return systemrecord.Result{}, false, err
	}
	if closeErr != nil {
		return systemrecord.Result{}, false, closeErr
	}
	if len(terminal) == 0 {
		return systemrecord.Result{}, false, ErrInvalidDocument
	}
	if err := contextError(ctx); err != nil {
		return systemrecord.Result{}, false, err
	}
	changes := tx.changes()
	if len(changes) == 0 {
		if len(tx.systemMutations) > 0 {
			return systemrecord.Result{}, false, ErrInvalidDocument
		}
		if err := db.validateWriteTransactionSnapshot(tx, store); err != nil {
			return systemrecord.Result{}, false, err
		}
		return systemrecord.Result{}, false, nil
	}
	extraBytes := tx.systemBytes + uint64(len(systemMutation.Key)) + uint64(len(terminal))
	if extraBytes < tx.systemBytes {
		db.metrics.resourceLimitRejections.Add(1)
		return systemrecord.Result{}, false, ErrResourceLimit
	}
	if err := db.validateTransactionResourceExtra(changes, extraBytes); err != nil {
		return systemrecord.Result{}, false, err
	}
	systemMutation.NewValue = append([]byte(nil), terminal...)
	systemMutations := make([]systemrecord.Mutation, 1, 1+len(tx.systemMutations))
	systemMutations[0] = systemMutation
	for _, mutation := range tx.systemMutations {
		if string(mutation.Key) == string(systemMutation.Key) {
			return systemrecord.Result{}, false, ErrInvalidDocument
		}
		systemMutations = append(systemMutations, mutation)
	}
	return db.commitWriteTransaction(ctx, tx, store, changes, systemMutations)
}
