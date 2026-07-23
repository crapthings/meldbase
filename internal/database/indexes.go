package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	btree "github.com/crapthings/meldbase/internal/index"
)

var indexNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,127}$`)

// IndexField is one ordered component of an index definition. Order must be 1
// (ascending) or -1 (descending); fields are evaluated left to right.
type IndexField struct {
	Field string
	Order int
}

// IndexOptions controls complete-tuple uniqueness.
type IndexOptions struct{ Unique bool }
type indexState struct {
	definition IndexDefinition
	tree       *btree.Tree
}

func (c *Collection) CreateIndex(ctx context.Context, name string, fields []IndexField, options IndexOptions) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := c.validate(); err != nil {
		return err
	}
	definition, err := validateIndexDefinition(name, fields, options)
	if err != nil {
		return err
	}
	if usesCompoundIndexCodec(definition) && c.db.durability != nil {
		if _, storage := c.db.durability.(*durableStore); !storage {
			return ErrCompoundIndexUnsupported
		}
	}
	budget := c.db.newIndexBuildBudget(c.db.resourceLimits)
	if store, ok := c.db.durability.(*durableStore); ok && store != nil {
		return c.db.observeIndexBuild(budget, func() error {
			return c.createDurableIndexOptimistic(ctx, definition, store, budget)
		})
	}
	return c.db.observeIndexBuild(budget, func() error {
		return c.createLockedIndex(ctx, definition, budget)
	})
}

func (c *Collection) createLockedIndex(ctx context.Context, definition IndexDefinition, budget *indexBuildBudget) error {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	if c.db.closed {
		return ErrClosed
	}
	if c.db.fatalErr != nil {
		return c.db.fatalErr
	}
	data := c.db.collections[c.name]
	if data == nil {
		data = newCollectionData()
	}
	if data.indexes == nil {
		data.indexes = make(map[string]*indexState)
	}
	if _, exists := data.indexes[definition.Name]; exists {
		return fmt.Errorf("%w: index name exists", ErrInvalidIndex)
	}
	c.db.activeIndexBuild = budget
	defer func() { c.db.activeIndexBuild = nil }()
	var state *indexState
	if c.db.querySource != nil {
		// The durable  transaction scans one pinned Primary snapshot and owns
		// uniqueness validation. Keeping a second decoded-document scan here
		// would both double the work and make CreateIndex depend on the
		// compatibility mirror that  is progressively removing.
		state = &indexState{definition: definition}
	} else {
		var err error
		state, err = buildIndex(definition, data, budget)
		if err != nil {
			return err
		}
	}
	copyDefinition := cloneIndexDefinition(definition)
	change := Change{Collection: c.name, Operation: CreateIndexOperation, Index: &copyDefinition}
	token := c.db.token + 1
	if err := c.db.appendCommit(ctx, token, []Change{change}); err != nil {
		return err
	}
	if c.db.collections[c.name] == nil {
		c.db.collections[c.name] = data
	}
	data.indexes[definition.Name] = state
	c.db.token = token
	c.db.recordLiveCommit(ChangeBatch{Token: token, Changes: []Change{change}})
	return nil
}

func (db *DB) observeIndexBuild(budget *indexBuildBudget, build func() error) (resultErr error) {
	db.metrics.indexBuildAttempts.Add(1)
	db.metrics.indexBuildActive.Add(1)
	started := time.Now()
	defer func() {
		db.metrics.indexBuildActive.Add(^uint64(0))
		elapsed := uint64(time.Since(started))
		db.metrics.indexBuildLastMu.Lock()
		db.metrics.indexBuildLastEntries = budget.entries
		db.metrics.indexBuildLastBytes = budget.bytes
		db.metrics.indexBuildLastNanos = elapsed
		db.metrics.indexBuildLastMu.Unlock()
		updateAtomicMax(&db.metrics.indexBuildMaxNanos, elapsed)
		if resultErr == nil {
			db.metrics.indexBuildCompleted.Add(1)
		} else if !errors.Is(resultErr, context.Canceled) && !errors.Is(resultErr, context.DeadlineExceeded) {
			db.metrics.indexBuildFailed.Add(1)
		}
	}()
	return build()
}

func buildIndex(definition IndexDefinition, data *collectionData, budget *indexBuildBudget) (*indexState, error) {
	state := &indexState{definition: definition, tree: btree.New()}
	for _, id := range data.order {
		document, ok := data.documents[id]
		if !ok {
			return nil, ErrCorrupt
		}
		key, found, err := indexDocumentKey(definition, document)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		if err := budget.add(key); err != nil {
			return nil, err
		}
		if definition.Unique && len(state.tree.Get(key)) > 0 {
			return nil, ErrDuplicateKey
		}
		state.tree.Insert(key, id[:])
	}
	return state, nil
}

func (d *collectionData) validateIndexInsert(id DocumentID, document Document) error {
	for _, state := range d.indexes {
		if state.tree == nil {
			continue
		}
		key, found, err := indexDocumentKey(state.definition, document)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if state.definition.Unique {
			values := state.tree.Get(key)
			if len(values) > 0 && !bytes.Equal(values[0], id[:]) {
				return ErrDuplicateKey
			}
		}
	}
	return nil
}

func (d *collectionData) validateIndexBatchInsert(ids []DocumentID, documents []Document) error {
	for _, state := range d.indexes {
		if state.tree == nil {
			continue
		}
		batchOwners := map[string]DocumentID{}
		for index, document := range documents {
			key, found, err := indexDocumentKey(state.definition, document)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			if !state.definition.Unique {
				continue
			}
			if len(state.tree.Get(key)) > 0 {
				return ErrDuplicateKey
			}
			encoded := string(key)
			if owner, exists := batchOwners[encoded]; exists && owner != ids[index] {
				return ErrDuplicateKey
			}
			batchOwners[encoded] = ids[index]
		}
	}
	return nil
}

func (d *collectionData) validateIndexUpdates(changes []pendingUpdate) error {
	for _, state := range d.indexes {
		if state.tree == nil {
			continue
		}
		if !state.definition.Unique {
			for _, change := range changes {
				if _, _, err := indexDocumentKey(state.definition, change.before); err != nil {
					return ErrInvalidIndex
				}
				if _, _, err := indexDocumentKey(state.definition, change.after); err != nil {
					return ErrInvalidIndex
				}
			}
			continue
		}
		changedIDs := make(map[DocumentID]struct{}, len(changes))
		for _, change := range changes {
			changedIDs[change.id] = struct{}{}
			if _, _, err := indexDocumentKey(state.definition, change.before); err != nil {
				return ErrInvalidIndex
			}
		}
		afterOwners := make(map[string]DocumentID, len(changes))
		for _, change := range changes {
			if key, found, err := indexDocumentKey(state.definition, change.after); found {
				if err != nil {
					return ErrInvalidIndex
				}
				for _, rawOwner := range state.tree.Get(key) {
					var owner DocumentID
					if len(rawOwner) != len(owner) {
						return ErrDuplicateKey
					}
					copy(owner[:], rawOwner)
					if _, moving := changedIDs[owner]; !moving {
						return ErrDuplicateKey
					}
				}
				encoded := string(key)
				if owner, exists := afterOwners[encoded]; exists && owner != change.id {
					return ErrDuplicateKey
				}
				afterOwners[encoded] = change.id
			} else if err != nil {
				return ErrInvalidIndex
			}
		}
	}
	return nil
}

func (d *collectionData) insertIndexes(id DocumentID, document Document) {
	for _, state := range d.indexes {
		if state.tree == nil {
			continue
		}
		if key, found, err := indexDocumentKey(state.definition, document); found && err == nil {
			state.tree.Insert(key, id[:])
		}
	}
}
func (d *collectionData) deleteIndexes(id DocumentID, document Document) {
	for _, state := range d.indexes {
		if state.tree == nil {
			continue
		}
		if key, found, err := indexDocumentKey(state.definition, document); found && err == nil {
			state.tree.Delete(key, id[:])
		}
	}
}

// updateIndexes keeps an unchanged indexed value in place. Rebuilding the
// same secondary entry is unnecessary secondary-tree work.
func (d *collectionData) updateIndexes(id DocumentID, before, after Document) {
	for _, state := range d.indexes {
		if state.tree == nil {
			continue
		}
		beforeKey, beforeFound, beforeErr := indexDocumentKey(state.definition, before)
		afterKey, afterFound, afterErr := indexDocumentKey(state.definition, after)
		if beforeErr != nil || afterErr != nil {
			continue
		}
		if beforeFound && afterFound && bytes.Equal(beforeKey, afterKey) {
			continue
		}
		if beforeFound {
			state.tree.Delete(beforeKey, id[:])
		}
		if afterFound {
			state.tree.Insert(afterKey, id[:])
		}
	}
}

type ExplainResult struct {
	Stage, IndexName                string
	DocumentsExamined, KeysExamined int64
}

func (c *Collection) Explain(ctx context.Context, filter Filter) (ExplainResult, error) {
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		return ExplainResult{}, err
	}
	_, explain, err := c.plan(ctx, query)
	return explain, err
}

func (c *Collection) plan(ctx context.Context, query QuerySpec) ([]Document, ExplainResult, error) {
	if err := contextError(ctx); err != nil {
		return nil, ExplainResult{}, err
	}
	if err := c.validate(); err != nil {
		return nil, ExplainResult{}, err
	}
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	if c.db.closed {
		return nil, ExplainResult{}, ErrClosed
	}
	if c.db.querySource != nil {
		return c.planStorageLocked(ctx, query)
	}
	data := c.db.collections[c.name]
	if data == nil {
		return nil, ExplainResult{Stage: "COLLSCAN"}, nil
	}
	if value, ok := equalityCandidate(query.where, "_id"); ok && value.kind == IDKind {
		document, exists := data.documents[value.id]
		documents := []Document{}
		examined := int64(0)
		if exists {
			documents, examined = []Document{document}, 1
		}
		return query.Execute(documents), ExplainResult{Stage: "ID_LOOKUP", IndexName: "_id", DocumentsExamined: examined, KeysExamined: 1}, nil
	}
	names := make([]string, 0, len(data.indexes))
	for name := range data.indexes {
		names = append(names, name)
	}
	sort.Slice(names, func(left, right int) bool {
		leftScore := indexQueryScore(data.indexes[names[left]].definition, query.where)
		rightScore := indexQueryScore(data.indexes[names[right]].definition, query.where)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return names[left] < names[right]
	})
	for _, name := range names {
		state := data.indexes[name]
		if start, end, ok, exact := compoundIndexQueryBounds(state.definition, query.where); ok {
			var pairs []btree.Pair
			if exact {
				for _, raw := range state.tree.Get(start) {
					pairs = append(pairs, btree.Pair{Key: start, Value: raw})
				}
			} else if end == nil || len(start) == 0 || bytes.Compare(start, end) < 0 {
				pairs = state.tree.Scan(start, end, false)
			}
			rawIDs := make([][]byte, 0, len(pairs))
			for _, pair := range pairs {
				rawIDs = append(rawIDs, pair.Value)
			}
			matched, examined := executeMemoryIndexCandidates(data, rawIDs, query)
			return matched, ExplainResult{Stage: "IXSCAN", IndexName: name, DocumentsExamined: examined, KeysExamined: int64(len(pairs))}, nil
		}
		if usesCompoundIndexCodec(state.definition) {
			continue
		}
		value, ok := equalityCandidate(query.where, state.definition.Field)
		if !ok {
			continue
		}
		key, err := encodeIndexKey(value)
		if err != nil {
			continue
		}
		rawIDs := state.tree.Get(key)
		matched, examined := executeMemoryIndexCandidates(data, rawIDs, query)
		return matched, ExplainResult{Stage: "IXSCAN", IndexName: name, DocumentsExamined: examined, KeysExamined: int64(len(rawIDs))}, nil
	}
	for _, name := range names {
		state := data.indexes[name]
		if usesCompoundIndexCodec(state.definition) {
			continue
		}
		lower, upper, ok := rangeCandidate(query.where, state.definition.Field)
		if !ok {
			continue
		}
		start, end, valid, err := storageIndexBounds(lower, upper)
		if err != nil {
			continue
		}
		if !valid {
			return []Document{}, ExplainResult{Stage: "IXSCAN", IndexName: name}, nil
		}
		pairs := state.tree.Scan(start, end, false)
		rawIDs := make([][]byte, 0, len(pairs))
		for _, pair := range pairs {
			rawIDs = append(rawIDs, pair.Value)
		}
		matched, examined := executeMemoryIndexCandidates(data, rawIDs, query)
		return matched, ExplainResult{Stage: "IXSCAN", IndexName: name, DocumentsExamined: examined, KeysExamined: int64(len(pairs))}, nil
	}
	documents := make([]Document, 0, len(data.order))
	for _, id := range data.order {
		if document, ok := data.documents[id]; ok {
			documents = append(documents, document)
		}
	}
	return query.Execute(documents), ExplainResult{Stage: "COLLSCAN", DocumentsExamined: int64(len(documents))}, nil
}

func executeMemoryIndexCandidates(data *collectionData, rawIDs [][]byte, query QuerySpec) ([]Document, int64) {
	candidates := make([]queryCandidate, 0, len(rawIDs))
	var examined int64
	for _, raw := range rawIDs {
		if len(raw) != len(DocumentID{}) {
			continue
		}
		var id DocumentID
		copy(id[:], raw)
		document, exists := data.documents[id]
		if !exists {
			continue
		}
		examined++
		if !query.Match(document) {
			continue
		}
		position, exists := data.positions[id]
		if !exists {
			// Compatibility for deliberately hand-built collectionData values in
			// package tests. All constructor and recovery paths own a position map.
			for index, candidate := range data.order {
				if candidate == id {
					position, exists = uint64(index), true
					break
				}
			}
		}
		if !exists {
			continue
		}
		candidates = append(candidates, queryCandidate{document: document, position: position})
	}
	return query.executeMatched(candidates), examined
}

func (c *Collection) planStorageLocked(ctx context.Context, query QuerySpec) (documents []Document, explain ExplainResult, resultErr error) {
	snapshot, err := c.db.querySource.openQuerySnapshot()
	if err != nil {
		return nil, ExplainResult{}, err
	}
	defer func() {
		if err := snapshot.Close(); resultErr == nil && err != nil {
			documents, explain, resultErr = nil, ExplainResult{}, err
		}
	}()
	if snapshot.Sequence() != c.db.token {
		return nil, ExplainResult{}, ErrCorrupt
	}
	if value, ok := equalityCandidate(query.where, "_id"); ok && value.kind == IDKind {
		record, exists, err := snapshot.GetDocumentRecord(c.name, value.id)
		if err != nil {
			return nil, ExplainResult{}, err
		}
		candidates := []queryCandidate{}
		if exists {
			candidate, err := decodeQueryStorageCandidate(record)
			if err != nil {
				return nil, ExplainResult{}, err
			}
			if candidate.documentID() != value.id {
				return nil, ExplainResult{}, ErrCorrupt
			}
			if query.Match(candidate.document) {
				candidates = append(candidates, candidate)
			}
		}
		return query.executeMatched(candidates), ExplainResult{
			Stage: "ID_LOOKUP", IndexName: "_id", DocumentsExamined: boolCount(exists), KeysExamined: 1,
		}, nil
	}
	indexes, err := snapshot.Indexes(c.name)
	if err != nil {
		return nil, ExplainResult{}, err
	}
	sort.Slice(indexes, func(left, right int) bool {
		leftDefinition := IndexDefinition{Name: indexes[left].Name, Field: indexes[left].Field, Order: 1, Unique: indexes[left].Unique, Fields: indexes[left].Fields}
		rightDefinition := IndexDefinition{Name: indexes[right].Name, Field: indexes[right].Field, Order: 1, Unique: indexes[right].Unique, Fields: indexes[right].Fields}
		leftScore, rightScore := indexQueryScore(leftDefinition, query.where), indexQueryScore(rightDefinition, query.where)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return indexes[left].Name < indexes[right].Name
	})
	for _, index := range indexes {
		definition := IndexDefinition{Name: index.Name, Field: index.Field, Order: 1, Unique: index.Unique, Fields: cloneIndexFields(index.Fields)}
		if start, end, ok, exact := compoundIndexQueryBounds(definition, query.where); ok {
			return executeStorageIndexQuery(ctx, snapshot, c.name, definition, query, start, end, exact)
		}
		if usesCompoundIndexCodec(definition) {
			continue
		}
		value, ok := equalityCandidate(query.where, index.Field)
		if !ok {
			continue
		}
		key, err := encodeIndexKey(value)
		if err != nil {
			continue
		}
		end := indexKeyPrefixEnd(key)
		if end == nil {
			return nil, ExplainResult{}, ErrCorrupt
		}
		return executeStorageIndexQuery(ctx, snapshot, c.name, definition, query, key, end, true)
	}
	for _, index := range indexes {
		definition := IndexDefinition{Name: index.Name, Field: index.Field, Order: 1, Unique: index.Unique, Fields: cloneIndexFields(index.Fields)}
		if usesCompoundIndexCodec(definition) {
			continue
		}
		lower, upper, ok := rangeCandidate(query.where, index.Field)
		if !ok {
			continue
		}
		start, end, valid, err := storageIndexBounds(lower, upper)
		if err != nil {
			continue
		}
		if !valid {
			return []Document{}, ExplainResult{Stage: "IXSCAN", IndexName: index.Name}, nil
		}
		return executeStorageIndexQuery(ctx, snapshot, c.name, definition, query, start, end, false)
	}
	iterator, err := snapshot.OpenCollectionIterator(c.name)
	if err != nil {
		return nil, ExplainResult{}, err
	}
	defer iterator.Close()
	candidates := make([]queryCandidate, 0)
	examined := int64(0)
	seen := make(map[DocumentID]struct{})
	for iterator.Next() {
		if err := contextError(ctx); err != nil {
			return nil, ExplainResult{}, err
		}
		candidate, err := decodeQueryStorageCandidate(iterator.Record())
		if err != nil {
			return nil, ExplainResult{}, err
		}
		id := candidate.documentID()
		if _, duplicate := seen[id]; duplicate {
			return nil, ExplainResult{}, ErrCorrupt
		}
		seen[id] = struct{}{}
		examined++
		if query.Match(candidate.document) {
			candidates = append(candidates, candidate)
		}
	}
	if err := iterator.Err(); err != nil {
		return nil, ExplainResult{}, err
	}
	if err := iterator.Close(); err != nil {
		return nil, ExplainResult{}, err
	}
	return query.executeMatched(candidates), ExplainResult{Stage: "COLLSCAN", DocumentsExamined: examined}, nil
}

func executeStorageIndexQuery(ctx context.Context, snapshot queryStorageSnapshot, collection string, definition IndexDefinition, query QuerySpec, start, end []byte, ordered bool) ([]Document, ExplainResult, error) {
	stopAfter := -1
	if ordered && len(query.sort) == 0 && query.limit != nil {
		if *query.limit == 0 {
			return []Document{}, ExplainResult{Stage: "IXSCAN", IndexName: definition.Name}, nil
		}
		maxInt := int(^uint(0) >> 1)
		if query.skip <= maxInt-*query.limit {
			stopAfter = query.skip + *query.limit
		}
	}
	iterator, err := snapshot.OpenIndexIterator(collection, definition.Name, start, end, 0)
	if err != nil {
		return nil, ExplainResult{}, err
	}
	defer iterator.Close()
	candidates := make([]queryCandidate, 0)
	seen := make(map[DocumentID]struct{})
	keys := int64(0)
	for iterator.Next() {
		if err := contextError(ctx); err != nil {
			return nil, ExplainResult{}, err
		}
		entry := iterator.Entry()
		if entry.ID.IsZero() || entry.Position == 0 {
			return nil, ExplainResult{}, ErrCorrupt
		}
		if _, duplicate := seen[entry.ID]; duplicate {
			return nil, ExplainResult{}, ErrCorrupt
		}
		seen[entry.ID] = struct{}{}
		keys++
		record, exists, err := snapshot.GetDocumentRecord(collection, entry.ID)
		if err != nil {
			return nil, ExplainResult{}, err
		}
		if !exists {
			return nil, ExplainResult{}, ErrCorrupt
		}
		candidate, err := decodeQueryStorageCandidate(record)
		if err != nil || candidate.documentID() != entry.ID || record.Position != entry.Position {
			return nil, ExplainResult{}, ErrCorrupt
		}
		actualKey, exists, err := indexDocumentKey(definition, candidate.document)
		if err != nil || !exists || !bytes.Equal(actualKey, entry.Key) {
			return nil, ExplainResult{}, ErrCorrupt
		}
		if query.Match(candidate.document) {
			candidates = append(candidates, candidate)
			if stopAfter >= 0 && len(candidates) >= stopAfter {
				break
			}
		}
	}
	if err := iterator.Err(); err != nil {
		return nil, ExplainResult{}, err
	}
	if err := iterator.Close(); err != nil {
		return nil, ExplainResult{}, err
	}
	return query.executeMatched(candidates), ExplainResult{
		Stage: "IXSCAN", IndexName: definition.Name, DocumentsExamined: keys, KeysExamined: keys,
	}, nil
}

func decodeQueryStorageCandidate(record queryStorageDocument) (queryCandidate, error) {
	if record.ID.IsZero() || record.Position == 0 || (record.Decoded == nil && len(record.Encoded) == 0) {
		return queryCandidate{}, ErrCorrupt
	}
	document := record.Decoded
	if document == nil {
		var err error
		document, err = decodeStoredDocument(record.Encoded)
		if err != nil {
			return queryCandidate{}, ErrCorrupt
		}
	}
	id, exists := document.ID()
	if !exists || id != record.ID {
		return queryCandidate{}, ErrCorrupt
	}
	return queryCandidate{document: document, position: record.Position}, nil
}

func (candidate queryCandidate) documentID() DocumentID {
	id, _ := candidate.document.ID()
	return id
}

func boolCount(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func indexKeyPrefixEnd(key []byte) []byte {
	end := append([]byte(nil), key...)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] != 0xff {
			end[index]++
			return end[:index+1]
		}
	}
	return nil
}

func storageIndexBounds(lower, upper *indexBound) ([]byte, []byte, bool, error) {
	var start, end []byte
	if lower != nil {
		var err error
		start, err = encodeIndexKey(lower.value)
		if err != nil {
			return nil, nil, false, err
		}
		if !lower.inclusive {
			start = indexKeyPrefixEnd(start)
			if start == nil {
				return nil, nil, false, nil
			}
		}
	}
	if upper != nil {
		var err error
		end, err = encodeIndexKey(upper.value)
		if err != nil {
			return nil, nil, false, err
		}
		if upper.inclusive {
			end = indexKeyPrefixEnd(end)
		}
	}
	if len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) >= 0 {
		return nil, nil, false, nil
	}
	return start, end, true, nil
}

func equalityCandidate(expression expr, path string) (Value, bool) {
	switch value := expression.(type) {
	case compareExpr:
		if value.path == path && value.cmp == "eq" {
			return value.value, true
		}
	case logicalExpr:
		if value.op == "and" {
			for _, child := range value.args {
				if candidate, ok := equalityCandidate(child, path); ok {
					return candidate, true
				}
			}
		}
	}
	return Value{}, false
}

type indexBound struct {
	value     Value
	inclusive bool
}

func rangeCandidate(expression expr, path string) (*indexBound, *indexBound, bool) {
	var lower, upper *indexBound
	found := false
	var visit func(expr) bool
	visit = func(expression expr) bool {
		switch value := expression.(type) {
		case logicalExpr:
			if value.op != "and" {
				return true
			}
			for _, child := range value.args {
				if !visit(child) {
					return false
				}
			}
		case compareExpr:
			if value.path != path || (value.cmp != "gt" && value.cmp != "gte" && value.cmp != "lt" && value.cmp != "lte") {
				return true
			}
			if _, err := encodeIndexKey(value.value); err != nil {
				return false
			}
			found = true
			candidate := &indexBound{value: value.value, inclusive: value.cmp == "gte" || value.cmp == "lte"}
			if value.cmp == "gt" || value.cmp == "gte" {
				if lower == nil {
					lower = candidate
				} else if comparison, ok := compareValues(candidate.value, lower.value); ok && (comparison > 0 || (comparison == 0 && !candidate.inclusive)) {
					lower = candidate
				}
			} else {
				if upper == nil {
					upper = candidate
				} else if comparison, ok := compareValues(candidate.value, upper.value); ok && (comparison < 0 || (comparison == 0 && !candidate.inclusive)) {
					upper = candidate
				}
			}
		}
		return true
	}
	if !visit(expression) {
		return nil, nil, false
	}
	return lower, upper, found
}
