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

// ExplainBound describes the logical values used to constrain one selected
// index component. Values is used for equality and membership unions; range
// bounds use Lower and Upper. A nil range endpoint is unbounded.
type ExplainBound struct {
	Path                           string
	Values                         []Value
	Lower, Upper                   *Value
	LowerInclusive, UpperInclusive bool
}

// ExplainResult separates plan facts from observed work. Estimated fields are
// conservative candidate estimates when the selected backend can provide them;
// DocumentsExamined and KeysExamined are the actual completed scan counts.
// ResidualPredicate means the complete compiled predicate is rechecked after
// index admission, including when the index appears to cover every condition.
type ExplainResult struct {
	Stage, IndexName                  string
	Bounds                            []ExplainBound
	ResidualPredicate, SortRequired   bool
	SortIndexCompatible               bool
	EstimatedDocuments, EstimatedKeys int64
	DocumentsExamined, KeysExamined   int64
	CandidatesRetained, SortBytes     uint64
}

func (c *Collection) Explain(ctx context.Context, filter Filter) (ExplainResult, error) {
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		return ExplainResult{}, err
	}
	return c.ExplainQuery(ctx, query)
}

// ExplainWithOptions compiles a filter and its ordering/window options before
// returning the selected plan. It is the convenience counterpart to
// ExplainQuery for callers that do not already hold a compiled QuerySpec.
func (c *Collection) ExplainWithOptions(ctx context.Context, filter Filter, options QueryOptions) (ExplainResult, error) {
	query, err := CompileQuery(filter, options)
	if err != nil {
		return ExplainResult{}, err
	}
	return c.ExplainQuery(ctx, query)
}

// ExplainQuery accepts the same validated compiled query used by FindQuery,
// so Explain cannot silently omit sort, skip, limit, or seek options.
func (c *Collection) ExplainQuery(ctx context.Context, query QuerySpec) (ExplainResult, error) {
	if err := query.Validate(); err != nil {
		return ExplainResult{}, err
	}
	_, explain, err := c.plan(ctx, query)
	return explain, err
}

func (c *Collection) plan(ctx context.Context, query QuerySpec) ([]Document, ExplainResult, error) {
	if c == nil || c.db == nil {
		return nil, ExplainResult{}, ErrInvalidCollection
	}
	budget, err := c.db.newQueryBudget(query)
	if err != nil {
		return nil, ExplainResult{}, err
	}
	return c.planWithBudget(ctx, query, budget)
}

func explainIndex(definition IndexDefinition, query QuerySpec) ExplainResult {
	return ExplainResult{
		Stage:               "IXSCAN",
		IndexName:           definition.Name,
		Bounds:              explainIndexBounds(definition, query.where),
		ResidualPredicate:   true,
		SortRequired:        len(query.sort) > 0,
		SortIndexCompatible: indexSortCompatible(definition, query),
	}
}

func explainCollection(query QuerySpec) ExplainResult {
	return ExplainResult{Stage: "COLLSCAN", SortRequired: len(query.sort) > 0}
}

func explainIndexBounds(definition IndexDefinition, expression expr) []ExplainBound {
	fields := indexDefinitionFields(definition)
	bounds := make([]ExplainBound, 0, len(fields))
	for _, field := range fields {
		if values, ok := indexValuesCandidate(expression, field.Field); ok {
			bound := ExplainBound{Path: field.Field, Values: make([]Value, len(values))}
			for index := range values {
				bound.Values[index] = values[index].Clone()
			}
			bounds = append(bounds, bound)
			if len(values) != 1 {
				break
			}
			continue
		}
		lower, upper, ok := rangeCandidate(expression, field.Field)
		if !ok {
			break
		}
		bound := ExplainBound{Path: field.Field}
		if lower != nil {
			value := lower.value.Clone()
			bound.Lower, bound.LowerInclusive = &value, lower.inclusive
		}
		if upper != nil {
			value := upper.value.Clone()
			bound.Upper, bound.UpperInclusive = &value, upper.inclusive
		}
		bounds = append(bounds, bound)
		break
	}
	return bounds
}

// indexSortCompatible recognizes the common safe compound case: an equality
// constrained leading prefix followed by an ascending requested suffix. The
// physical scan still applies the documented insertion-position tie-breaker,
// but this preference makes the sort-compatible index win when predicate
// selectivity is otherwise the same.
func indexSortCompatible(definition IndexDefinition, query QuerySpec) bool {
	if len(query.sort) == 0 {
		return false
	}
	fields := indexDefinitionFields(definition)
	prefix := 0
	for prefix < len(fields) {
		value, ok := equalityCandidate(query.where, fields[prefix].Field)
		if !ok {
			break
		}
		if _, err := encodeIndexKey(value); err != nil {
			return false
		}
		prefix++
	}
	if prefix == 0 || prefix+len(query.sort) > len(fields) {
		return false
	}
	for index, sortField := range query.sort {
		field := fields[prefix+index]
		if sortField.Path != field.Field || sortField.Direction != 1 || field.Order != 1 {
			return false
		}
	}
	return true
}

func enrichExplain(explain ExplainResult, query QuerySpec, budget *queryBudget) ExplainResult {
	explain.SortRequired = len(query.sort) > 0
	if budget == nil {
		return explain
	}
	if explain.EstimatedDocuments == 0 && explain.DocumentsExamined > 0 {
		explain.EstimatedDocuments = explain.DocumentsExamined
	}
	if explain.EstimatedKeys == 0 && explain.KeysExamined > 0 {
		explain.EstimatedKeys = explain.KeysExamined
	}
	explain.CandidatesRetained = budget.candidates
	explain.SortBytes = budget.sortBytes
	return explain
}

func (c *Collection) planWithBudget(ctx context.Context, query QuerySpec, budget *queryBudget) (documents []Document, explain ExplainResult, resultErr error) {
	defer func() { explain = enrichExplain(explain, query, budget) }()
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
		return c.planStorageLocked(ctx, query, budget)
	}
	data := c.db.collections[c.name]
	if data == nil {
		return nil, explainCollection(query), nil
	}
	if value, ok := equalityCandidate(query.where, "_id"); ok && value.kind == IDKind {
		if err := budget.key(); err != nil {
			return nil, ExplainResult{}, err
		}
		document, exists := data.documents[value.id]
		documents := []Document{}
		examined := int64(0)
		if exists {
			if err := budget.document(); err != nil {
				return nil, ExplainResult{}, err
			}
			documents, examined = []Document{document}, 1
		}
		if len(documents) > 0 && query.Match(documents[0]) {
			if err := budget.candidate(documents[0]); err != nil {
				return nil, ExplainResult{}, err
			}
		}
		explain := explainIndex(IndexDefinition{Name: "_id", Field: "_id", Order: 1, Fields: []IndexField{{Field: "_id", Order: 1}}}, query)
		explain.Stage, explain.DocumentsExamined, explain.KeysExamined = "ID_LOOKUP", examined, 1
		return query.Execute(documents), explain, nil
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
		leftOrder := indexSortCompatible(data.indexes[names[left]].definition, query)
		rightOrder := indexSortCompatible(data.indexes[names[right]].definition, query)
		if leftOrder != rightOrder {
			return leftOrder
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
			matched, examined, err := executeMemoryIndexCandidates(ctx, data, rawIDs, query, budget)
			if err != nil {
				return nil, ExplainResult{}, err
			}
			explain := explainIndex(state.definition, query)
			explain.DocumentsExamined, explain.KeysExamined = examined, int64(len(pairs))
			return matched, explain, nil
		}
		if spans, ok := compoundIndexValueSpans(state.definition, query.where); ok {
			pairs := make([]btree.Pair, 0)
			for _, span := range spans {
				pairs = append(pairs, state.tree.Scan(span.start, span.end, false)...)
			}
			rawIDs := make([][]byte, 0, len(pairs))
			for _, pair := range pairs {
				rawIDs = append(rawIDs, pair.Value)
			}
			matched, examined, err := executeMemoryIndexCandidates(ctx, data, rawIDs, query, budget)
			if err != nil {
				return nil, ExplainResult{}, err
			}
			explain := explainIndex(state.definition, query)
			explain.DocumentsExamined, explain.KeysExamined = examined, int64(len(pairs))
			return matched, explain, nil
		}
		if usesCompoundIndexCodec(state.definition) {
			continue
		}
		values, ok := indexValuesCandidate(query.where, state.definition.Field)
		if !ok {
			continue
		}
		rawIDs := make([][]byte, 0)
		for _, value := range values {
			key, err := encodeIndexKey(value)
			if err != nil {
				continue
			}
			rawIDs = append(rawIDs, state.tree.Get(key)...)
		}
		matched, examined, err := executeMemoryIndexCandidates(ctx, data, rawIDs, query, budget)
		if err != nil {
			return nil, ExplainResult{}, err
		}
		explain := explainIndex(state.definition, query)
		explain.DocumentsExamined, explain.KeysExamined = examined, int64(len(rawIDs))
		return matched, explain, nil
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
			return []Document{}, explainIndex(state.definition, query), nil
		}
		pairs := state.tree.Scan(start, end, false)
		rawIDs := make([][]byte, 0, len(pairs))
		for _, pair := range pairs {
			rawIDs = append(rawIDs, pair.Value)
		}
		matched, examined, err := executeMemoryIndexCandidates(ctx, data, rawIDs, query, budget)
		if err != nil {
			return nil, ExplainResult{}, err
		}
		explain := explainIndex(state.definition, query)
		explain.DocumentsExamined, explain.KeysExamined = examined, int64(len(pairs))
		return matched, explain, nil
	}
	collector := newQueryCandidateCollector(query)
	for _, id := range data.order {
		if err := contextError(ctx); err != nil {
			return nil, ExplainResult{}, err
		}
		if document, ok := data.documents[id]; ok {
			if err := budget.document(); err != nil {
				return nil, ExplainResult{}, err
			}
			if query.Match(document) {
				if err := retainQueryCandidate(&collector, budget, queryCandidate{document: document, position: data.positions[id]}); err != nil {
					return nil, ExplainResult{}, err
				}
			}
		}
	}
	explain = explainCollection(query)
	explain.DocumentsExamined = int64(budget.documents)
	return collector.Documents(), explain, nil
}

func executeMemoryIndexCandidates(ctx context.Context, data *collectionData, rawIDs [][]byte, query QuerySpec, budget *queryBudget) ([]Document, int64, error) {
	collector := newQueryCandidateCollector(query)
	seen := make(map[DocumentID]struct{}, len(rawIDs))
	var examined int64
	for _, raw := range rawIDs {
		if err := contextError(ctx); err != nil {
			return nil, examined, err
		}
		if err := budget.key(); err != nil {
			return nil, examined, err
		}
		if len(raw) != len(DocumentID{}) {
			continue
		}
		var id DocumentID
		copy(id[:], raw)
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		document, exists := data.documents[id]
		if !exists {
			continue
		}
		examined++
		if err := budget.document(); err != nil {
			return nil, examined, err
		}
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
		if err := retainQueryCandidate(&collector, budget, queryCandidate{document: document, position: position}); err != nil {
			return nil, examined, err
		}
	}
	return collector.Documents(), examined, nil
}

func (c *Collection) planStorageLocked(ctx context.Context, query QuerySpec, budget *queryBudget) (documents []Document, explain ExplainResult, resultErr error) {
	defer func() { explain = enrichExplain(explain, query, budget) }()
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
		if err := budget.key(); err != nil {
			return nil, ExplainResult{}, err
		}
		record, exists, err := snapshot.GetDocumentRecord(c.name, value.id)
		if err != nil {
			return nil, ExplainResult{}, err
		}
		collector := newQueryCandidateCollector(query)
		if exists {
			if err := budget.document(); err != nil {
				return nil, ExplainResult{}, err
			}
			candidate, err := decodeQueryStorageCandidate(record)
			if err != nil {
				return nil, ExplainResult{}, err
			}
			if candidate.documentID() != value.id {
				return nil, ExplainResult{}, ErrCorrupt
			}
			if query.Match(candidate.document) {
				if err := retainQueryCandidate(&collector, budget, candidate); err != nil {
					return nil, ExplainResult{}, err
				}
			}
		}
		explain := explainIndex(IndexDefinition{Name: "_id", Field: "_id", Order: 1, Fields: []IndexField{{Field: "_id", Order: 1}}}, query)
		explain.Stage, explain.DocumentsExamined, explain.KeysExamined = "ID_LOOKUP", boolCount(exists), 1
		return collector.Documents(), explain, nil
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
		leftOrder := indexSortCompatible(leftDefinition, query)
		rightOrder := indexSortCompatible(rightDefinition, query)
		if leftOrder != rightOrder {
			return leftOrder
		}
		return indexes[left].Name < indexes[right].Name
	})
	for _, index := range indexes {
		definition := IndexDefinition{Name: index.Name, Field: index.Field, Order: 1, Unique: index.Unique, Fields: cloneIndexFields(index.Fields)}
		if start, end, ok, exact := compoundIndexQueryBounds(definition, query.where); ok {
			return executeStorageIndexQuery(ctx, snapshot, c.name, definition, query, budget, start, end, exact)
		}
		if spans, ok := compoundIndexValueSpans(definition, query.where); ok {
			return executeStorageIndexSpans(ctx, snapshot, c.name, definition, query, budget, spans, false)
		}
		if usesCompoundIndexCodec(definition) {
			continue
		}
		values, ok := indexValuesCandidate(query.where, index.Field)
		if !ok {
			continue
		}
		spans := make([]indexScanSpan, 0, len(values))
		for _, value := range values {
			key, err := encodeIndexKey(value)
			if err != nil {
				continue
			}
			end := indexKeyPrefixEnd(key)
			if end == nil {
				return nil, ExplainResult{}, ErrCorrupt
			}
			spans = append(spans, indexScanSpan{start: key, end: end})
		}
		return executeStorageIndexSpans(ctx, snapshot, c.name, definition, query, budget, spans, len(spans) == 1)
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
			return []Document{}, explainIndex(definition, query), nil
		}
		return executeStorageIndexQuery(ctx, snapshot, c.name, definition, query, budget, start, end, false)
	}
	iterator, err := snapshot.OpenCollectionIterator(c.name)
	if err != nil {
		return nil, ExplainResult{}, err
	}
	defer iterator.Close()
	collector := newQueryCandidateCollector(query)
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
		if err := budget.document(); err != nil {
			return nil, ExplainResult{}, err
		}
		if query.Match(candidate.document) {
			if err := retainQueryCandidate(&collector, budget, candidate); err != nil {
				return nil, ExplainResult{}, err
			}
		}
	}
	if err := iterator.Err(); err != nil {
		return nil, ExplainResult{}, err
	}
	if err := iterator.Close(); err != nil {
		return nil, ExplainResult{}, err
	}
	explain = explainCollection(query)
	explain.DocumentsExamined = examined
	return collector.Documents(), explain, nil
}

type indexScanSpan struct{ start, end []byte }

func executeStorageIndexQuery(ctx context.Context, snapshot queryStorageSnapshot, collection string, definition IndexDefinition, query QuerySpec, budget *queryBudget, start, end []byte, ordered bool) ([]Document, ExplainResult, error) {
	return executeStorageIndexSpans(ctx, snapshot, collection, definition, query, budget, []indexScanSpan{{start: start, end: end}}, ordered)
}

func executeStorageIndexSpans(ctx context.Context, snapshot queryStorageSnapshot, collection string, definition IndexDefinition, query QuerySpec, budget *queryBudget, spans []indexScanSpan, ordered bool) ([]Document, ExplainResult, error) {
	stopAfter := -1
	// Exact equality postings retain their collection insertion order. A limit
	// can therefore stop there without changing unsorted query semantics; range
	// scans still feed the bounded collector through to completion.
	if ordered && len(spans) == 1 && len(query.sort) == 0 && query.limit != nil {
		if *query.limit == 0 {
			return []Document{}, explainIndex(definition, query), nil
		}
		maxInt := int(^uint(0) >> 1)
		if query.skip <= maxInt-*query.limit {
			stopAfter = query.skip + *query.limit
		}
	}
	collector := newQueryCandidateCollector(query)
	seen := make(map[DocumentID]struct{})
	keys := int64(0)
	for _, span := range spans {
		stop, err := func() (stop bool, resultErr error) {
			iterator, err := snapshot.OpenIndexIterator(collection, definition.Name, span.start, span.end, 0)
			if err != nil {
				return false, err
			}
			defer func() {
				if err := iterator.Close(); resultErr == nil && err != nil {
					resultErr = err
				}
			}()
			for iterator.Next() {
				if err := contextError(ctx); err != nil {
					return false, err
				}
				entry := iterator.Entry()
				if entry.ID.IsZero() || entry.Position == 0 {
					return false, ErrCorrupt
				}
				keys++
				if err := budget.key(); err != nil {
					return false, err
				}
				if _, duplicate := seen[entry.ID]; duplicate {
					// A multi-value membership or $or union can encounter one ID
					// more than once. Account every physical key, but recheck one
					// logical document only once.
					continue
				}
				seen[entry.ID] = struct{}{}
				record, exists, err := snapshot.GetDocumentRecord(collection, entry.ID)
				if err != nil {
					return false, err
				}
				if !exists {
					return false, ErrCorrupt
				}
				candidate, err := decodeQueryStorageCandidate(record)
				if err != nil || candidate.documentID() != entry.ID || record.Position != entry.Position {
					return false, ErrCorrupt
				}
				actualKey, exists, err := indexDocumentKey(definition, candidate.document)
				if err != nil || !exists || !bytes.Equal(actualKey, entry.Key) {
					return false, ErrCorrupt
				}
				if err := budget.document(); err != nil {
					return false, err
				}
				if query.Match(candidate.document) {
					if err := retainQueryCandidate(&collector, budget, candidate); err != nil {
						return false, err
					}
					if stopAfter >= 0 && len(collector.heap.items) >= stopAfter {
						return true, nil
					}
				}
			}
			return false, iterator.Err()
		}()
		if err != nil {
			return nil, ExplainResult{}, err
		}
		if stop {
			break
		}
	}
	explain := explainIndex(definition, query)
	explain.DocumentsExamined, explain.KeysExamined = keys, keys
	return collector.Documents(), explain, nil
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

// indexValuesCandidate extracts a sound union of scalar equality keys for one
// single-field index. A conjunction needs only one restrictive child; a
// disjunction is usable only when every branch supplies keys for the same
// field. The executor always rechecks the complete predicate, so unrelated
// conjuncts remain a residual filter rather than becoming planner assumptions.
func indexValuesCandidate(expression expr, path string) ([]Value, bool) {
	values, ok := indexValuesCandidateExpr(expression, path)
	if !ok {
		return nil, false
	}
	unique := make([]Value, 0, len(values))
	for _, value := range values {
		if _, err := encodeIndexKey(value); err != nil {
			return nil, false
		}
		duplicate := false
		for _, existing := range unique {
			if existing.Equal(value) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			unique = append(unique, value.Clone())
		}
	}
	return unique, true
}

func indexValuesCandidateExpr(expression expr, path string) ([]Value, bool) {
	switch value := expression.(type) {
	case compareExpr:
		if value.path == path && value.cmp == "eq" {
			return []Value{value.value}, true
		}
	case membershipExpr:
		if value.path == path && value.op == "in" {
			return append([]Value(nil), value.values...), true
		}
	case logicalExpr:
		switch value.op {
		case "and":
			for _, child := range value.args {
				if values, ok := indexValuesCandidateExpr(child, path); ok {
					return values, true
				}
			}
		case "or":
			values := make([]Value, 0, len(value.args))
			for _, child := range value.args {
				childValues, ok := indexValuesCandidateExpr(child, path)
				if !ok {
					return nil, false
				}
				values = append(values, childValues...)
			}
			return values, true
		}
	}
	return nil, false
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
