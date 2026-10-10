package database

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	btree "github.com/crapthings/meldbase/internal/index"
)

// indexNamePattern defines the allowed spelling and length of index names.
var indexNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,127}$`)

// IndexField selects a field and its sort direction within an index.
//
// Order must be 1 (ascending) or -1 (descending); fields are evaluated left to right.
type IndexField struct {
	Field string
	Order int
}

// IndexOptions controls complete-tuple uniqueness.
type IndexOptions struct{ Unique bool }

// indexState pairs an index definition with its optional in-memory tree.
type indexState struct {
	definition IndexDefinition
	tree       *btree.Tree
}

// CreateIndex builds and commits a named index over the collection.
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

// createLockedIndex builds and publishes an index while holding the database write lock.
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
		// The durable transaction scans a pinned primary snapshot and checks
		// uniqueness, so this path only needs to keep the index definition.
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

// observeIndexBuild records resource use, duration, and outcome for an index build.
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

// buildIndex creates an in-memory index from existing documents within the build budget.
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

// validateIndexInsert checks a document's index keys and rejects conflicts with another document.
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

// validateIndexBatchInsert checks index keys and uniqueness across existing documents and the insert batch.
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

// validateIndexUpdates checks index keys and uniqueness for the final state of an update batch.
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

// insertIndexes adds a document's valid index entries to the in-memory trees.
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

// deleteIndexes removes a document's valid index entries from the in-memory trees.
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

// updateIndexes replaces changed in-memory index entries and preserves unchanged keys.
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

// ExplainBound describes the values that restrict one indexed field.
//
// Values is used for equality and membership unions; range
// bounds use Lower and Upper. A nil range endpoint is unbounded.
type ExplainBound struct {
	Path                           string
	Values                         []Value
	Lower, Upper                   *Value
	LowerInclusive, UpperInclusive bool
}

// ExplainAccessSource records the query work performed through one index or primary key path.
//
// CandidateIDs counts document IDs processed by the
// deduplicator. Read-ahead needed for an ordered union remains visible in
// KeysExamined even when execution stops before those IDs are consumed.
type ExplainAccessSource struct {
	IndexName                                 string
	Primary                                   bool
	Bounds                                    []ExplainBound
	Spans, ExactSpans                         int
	KeysExamined, CandidateIDs                int64
	UniqueCandidateIDs, DuplicateCandidateIDs int64
	DocumentsExamined                         int64
}

// ExplainBudget is the final resource-budget snapshot for one execution.
//
// Pressure and Exceeded are fixed reason codes naming one of "documents",
// "keys", "candidates", "sort_bytes", "skip", or "predicate_steps".
type ExplainBudget struct {
	DocumentsUsed, DocumentsLimit           uint64
	KeysUsed, KeysLimit                     uint64
	CandidatesUsed, CandidatesLimit         uint64
	SortBytesUsed, SortBytesLimit           uint64
	SkipUsed, SkipLimit                     uint64
	PredicateStepsUsed, PredicateStepsLimit uint64
	Pressure, Exceeded                      string
}

// ExplainAdvice records a possible query improvement to check against the workload.
//
// It is not an instruction to create an index. Paths and Sort contain schema facts but
// never query values. Callers should validate selectivity against workload
// measurements.
type ExplainAdvice struct {
	Code  string
	Paths []string
	Sort  []SortField
}

// ExplainResult describes the query plan and the work measured during execution.
//
// Estimated fields are
// conservative candidate estimates when the selected backend can provide them;
// DocumentsExamined and KeysExamined are the actual completed scan counts.
// ResidualPredicate means the complete compiled predicate is rechecked after
// index admission, including when the index appears to cover every condition.
type ExplainResult struct {
	Stage, IndexName string
	// IndexNames contains every access path used by an index union. IndexName
	// remains populated for the common single-index and primary-key plans.
	IndexNames                        []string
	Bounds                            []ExplainBound
	ResidualPredicate, SortRequired   bool
	SortIndexCompatible               bool
	EstimatedDocuments, EstimatedKeys int64
	DocumentsExamined, KeysExamined   int64
	CandidatesRetained, SortBytes     uint64
	PlanReason, FallbackReason        string
	UnindexedPaths                    []string
	// IndexableConjunctPaths lists distinct AND predicate paths that each have
	// an independently usable access path. It is populated only by explicit
	// Explain calls and does not imply that an index intersection was selected.
	IndexableConjunctPaths []string
	// CompoundIndexOpportunity is a structural signal: multiple independently
	// indexable AND paths exist, while the selected non-unique source constrains
	// only a subset. Advice still requires observed amplification.
	CompoundIndexOpportunity         bool
	Sources                          []ExplainAccessSource
	CandidateIDs, UniqueCandidateIDs int64
	DuplicateCandidateIDs            int64
	EarlyStopEligible, EarlyStopped  bool
	EarlyStopScope, EarlyStopReason  string
	Budget                           ExplainBudget
	Advice                           []ExplainAdvice
}

// Explain executes a filter query and reports its access plan and observed work.
func (c *Collection) Explain(ctx context.Context, filter Filter) (ExplainResult, error) {
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		return ExplainResult{}, err
	}
	return c.ExplainQuery(ctx, query)
}

// ExplainWithOptions executes a filter with query options and reports its plan and measured work.
func (c *Collection) ExplainWithOptions(ctx context.Context, filter Filter, options QueryOptions) (ExplainResult, error) {
	query, err := CompileQuery(filter, options)
	if err != nil {
		return ExplainResult{}, err
	}
	return c.ExplainQuery(ctx, query)
}

// ExplainQuery executes a compiled query and reports its plan and measured work.
//
// It validates the same query used by FindQuery, including sort, skip, limit, and seek options.
func (c *Collection) ExplainQuery(ctx context.Context, query QuerySpec) (ExplainResult, error) {
	if err := query.Validate(); err != nil {
		return ExplainResult{}, err
	}
	_, explain, err := c.plan(ctx, query)
	return finalizeExplainAdvice(query, explain), err
}

// plan executes a query with a fresh budget and detailed work counters.
func (c *Collection) plan(ctx context.Context, query QuerySpec) ([]Document, ExplainResult, error) {
	if c == nil || c.db == nil {
		return nil, ExplainResult{}, ErrInvalidCollection
	}
	budget, err := c.db.newQueryBudget(query)
	if err != nil {
		explain := ExplainResult{PlanReason: "not_planned", FallbackReason: "budget_rejected"}
		return nil, enrichExplain(explain, query, budget), err
	}
	budget.detailed = true
	return c.planWithBudget(ctx, query, budget)
}

// explainCollection describes a collection scan with no usable index.
func explainCollection(query QuerySpec) ExplainResult {
	return explainCollectionPlan(query, queryAccessPlan{fallbackReason: "no_usable_index"})
}

// explainIndexBounds describes equality and range limits on the usable leading index fields.
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

// indexSortCompatible checks whether ascending sort fields follow an equality-constrained index prefix.
//
// The
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

// enrichExplain adds ordering requirements and measured budget use to an execution report.
func enrichExplain(explain ExplainResult, query QuerySpec, budget *queryBudget) ExplainResult {
	explain.SortRequired = len(query.sort) > 0
	if budget == nil {
		return explain
	}
	if explain.DocumentsExamined < int64(budget.documents) {
		explain.DocumentsExamined = int64(budget.documents)
	}
	if explain.KeysExamined < int64(budget.keys) {
		explain.KeysExamined = int64(budget.keys)
	}
	if explain.EstimatedDocuments == 0 && explain.DocumentsExamined > 0 {
		explain.EstimatedDocuments = explain.DocumentsExamined
	}
	if explain.EstimatedKeys == 0 && explain.KeysExamined > 0 {
		explain.EstimatedKeys = explain.KeysExamined
	}
	explain.CandidatesRetained = budget.candidates
	explain.SortBytes = budget.sortBytes
	explain.Budget = explainBudgetSnapshot(budget)
	return explain
}

// planWithBudget selects and executes an access plan within the supplied query budget.
func (c *Collection) planWithBudget(ctx context.Context, query QuerySpec, budget *queryBudget) (documents []Document, explain ExplainResult, resultErr error) {
	return c.planWithBudgetAccess(ctx, query, budget, nil)
}

// planWithBudgetAccess executes a supplied or newly selected access plan and records its work.
func (c *Collection) planWithBudgetAccess(ctx context.Context, query QuerySpec, budget *queryBudget, plannedAccess *queryAccessPlan) (documents []Document, explain ExplainResult, resultErr error) {
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
		return c.planStorageLockedAccess(ctx, query, budget, plannedAccess)
	}
	data := c.db.collections[c.name]
	if data == nil {
		data = newCollectionData()
	}
	definitions := make([]IndexDefinition, 0, len(data.indexes))
	for _, state := range data.indexes {
		definitions = append(definitions, state.definition)
	}
	access := selectQueryAccessPlan(query.where, definitions, query)
	if budget.detailed {
		annotateQueryAccessPlan(&access, query.where, definitions, query)
	}
	if access.usable {
		return executeMemoryAccessPlan(ctx, data, access, query, budget)
	}
	explain = explainCollectionPlan(query, access)
	if query.limit != nil && *query.limit == 0 {
		return []Document{}, explain, nil
	}
	collector := newQueryCandidateCollector(query)
	stopAfter := accessPlanWindow(query)
	for _, id := range data.order {
		if err := contextError(ctx); err != nil {
			return nil, explain, err
		}
		if document, ok := data.documents[id]; ok {
			if err := budget.document(); err != nil {
				return nil, explain, err
			}
			explain.DocumentsExamined++
			matched, err := query.matchWithBudget(document, budget)
			if err != nil {
				return nil, explain, err
			}
			if matched {
				if err := retainQueryCandidate(&collector, budget, queryCandidate{document: document, position: data.positions[id]}); err != nil {
					return nil, explain, err
				}
				if explain.EarlyStopEligible && stopAfter >= 0 && len(collector.heap.items) >= stopAfter {
					explain.EarlyStopped = true
					break
				}
			}
		}
	}
	return collector.Documents(), explain, nil
}

// planStorageLocked selects and executes a stored query plan while the caller holds the database read lock.
func (c *Collection) planStorageLocked(ctx context.Context, query QuerySpec, budget *queryBudget) (documents []Document, explain ExplainResult, resultErr error) {
	return c.planStorageLockedAccess(ctx, query, budget, nil)
}

// planStorageLockedAccess executes a supplied or newly selected plan against a pinned storage snapshot.
func (c *Collection) planStorageLockedAccess(ctx context.Context, query QuerySpec, budget *queryBudget, plannedAccess *queryAccessPlan) (documents []Document, explain ExplainResult, resultErr error) {
	defer func() { explain = enrichExplain(explain, query, budget) }()
	snapshot, err := c.db.querySource.openQuerySnapshot()
	if err != nil {
		return nil, ExplainResult{}, err
	}
	defer func() {
		if err := snapshot.Close(); resultErr == nil && err != nil {
			documents, resultErr = nil, err
		}
	}()
	if snapshot.Sequence() != c.db.token {
		return nil, ExplainResult{}, ErrCorrupt
	}
	if plannedAccess != nil {
		return executeStorageAccessPlan(ctx, snapshot, c.name, *plannedAccess, query, budget)
	}
	indexes, err := snapshot.Indexes(c.name)
	if err != nil {
		return nil, ExplainResult{}, err
	}
	definitions := make([]IndexDefinition, len(indexes))
	for index := range indexes {
		definitions[index] = IndexDefinition{
			Name: indexes[index].Name, Field: indexes[index].Field, Order: 1,
			Unique: indexes[index].Unique, Fields: cloneIndexFields(indexes[index].Fields),
		}
	}
	access := selectQueryAccessPlan(query.where, definitions, query)
	if budget.detailed {
		annotateQueryAccessPlan(&access, query.where, definitions, query)
	}
	if access.usable {
		return executeStorageAccessPlan(ctx, snapshot, c.name, access, query, budget)
	}
	explain = explainCollectionPlan(query, access)
	if query.limit != nil && *query.limit == 0 {
		return []Document{}, explain, nil
	}
	iterator, err := snapshot.OpenCollectionIterator(c.name)
	if err != nil {
		return nil, explain, err
	}
	defer iterator.Close()
	collector := newQueryCandidateCollector(query)
	stopAfter := accessPlanWindow(query)
	seen := make(map[DocumentID]struct{})
	for iterator.Next() {
		if err := contextError(ctx); err != nil {
			return nil, explain, err
		}
		candidate, err := decodeQueryStorageCandidate(iterator.Record())
		if err != nil {
			return nil, explain, err
		}
		id := candidate.documentID()
		if _, duplicate := seen[id]; duplicate {
			return nil, explain, ErrCorrupt
		}
		seen[id] = struct{}{}
		if err := budget.document(); err != nil {
			return nil, explain, err
		}
		explain.DocumentsExamined++
		matched, err := query.matchWithBudget(candidate.document, budget)
		if err != nil {
			return nil, explain, err
		}
		if matched {
			if err := retainQueryCandidate(&collector, budget, candidate); err != nil {
				return nil, explain, err
			}
			if explain.EarlyStopEligible && stopAfter >= 0 && len(collector.heap.items) >= stopAfter {
				explain.EarlyStopped = true
				break
			}
		}
	}
	if err := iterator.Err(); err != nil {
		return nil, explain, err
	}
	if err := iterator.Close(); err != nil {
		return nil, explain, err
	}
	return collector.Documents(), explain, nil
}

// executeStorageAccessPlan evaluates distinct candidates from stored access sources within the query budget.
func executeStorageAccessPlan(ctx context.Context, snapshot queryStorageSnapshot, collection string, access queryAccessPlan, query QuerySpec, budget *queryBudget) ([]Document, ExplainResult, error) {
	if query.limit != nil && *query.limit == 0 {
		explain := explainQueryAccessPlan(access, query, budget.detailed)
		configureStorageAccessEarlyStop(&explain, access, query)
		return []Document{}, explain, nil
	}
	if canMergeStorageAccessByPosition(access, query) {
		return executeStorageOrderedAccessPlan(ctx, snapshot, collection, access, query, budget)
	}
	explain := explainQueryAccessPlan(access, query, budget.detailed)
	configureStorageAccessEarlyStop(&explain, access, query)
	collector := newQueryCandidateCollector(query)
	seen := make(map[DocumentID]struct{})
	stopAfter := accessPlanStopAfter(access, query)
	stopped := false
	admit := func(source int, id DocumentID, expectedPosition uint64, expectedKey []byte, definition *IndexDefinition, missingOK bool) error {
		if _, duplicate := seen[id]; duplicate {
			observeExplainCandidate(&explain, source, true)
			return nil
		}
		observeExplainCandidate(&explain, source, false)
		record, exists, err := snapshot.GetDocumentRecord(collection, id)
		if err != nil {
			return err
		}
		if !exists {
			if missingOK {
				return nil
			}
			return ErrCorrupt
		}
		candidate, err := decodeQueryStorageCandidate(record)
		if err != nil || candidate.documentID() != id || (expectedPosition != 0 && record.Position != expectedPosition) {
			return ErrCorrupt
		}
		if definition != nil {
			actualKey, indexed, err := indexDocumentKey(*definition, candidate.document)
			if err != nil || !indexed || !bytes.Equal(actualKey, expectedKey) {
				return ErrCorrupt
			}
		}
		seen[id] = struct{}{}
		if err := budget.document(); err != nil {
			return err
		}
		observeExplainDocument(&explain, source)
		matched, err := query.matchWithBudget(candidate.document, budget)
		if err != nil {
			return err
		}
		if matched {
			if err := retainQueryCandidate(&collector, budget, candidate); err != nil {
				return err
			}
			if stopAfter >= 0 && len(collector.heap.items) >= stopAfter {
				stopped = true
			}
		}
		return nil
	}

	for sourceIndex, source := range access.sources {
		if source.primary {
			for _, id := range source.ids {
				if err := budget.key(); err != nil {
					return nil, explain, err
				}
				observeExplainKey(&explain, sourceIndex)
				if err := admit(sourceIndex, id, 0, nil, nil, true); err != nil {
					return nil, explain, err
				}
				if stopped {
					break
				}
			}
			if stopped {
				break
			}
			continue
		}
		for _, span := range source.spans {
			iterator, err := snapshot.OpenIndexIterator(collection, source.definition.Name, span.start, span.end, 0)
			if err != nil {
				return nil, explain, err
			}
			for iterator.Next() {
				if err := contextError(ctx); err != nil {
					_ = iterator.Close()
					return nil, explain, err
				}
				entry := iterator.Entry()
				if entry.ID.IsZero() || entry.Position == 0 {
					_ = iterator.Close()
					return nil, explain, ErrCorrupt
				}
				if err := budget.key(); err != nil {
					_ = iterator.Close()
					return nil, explain, err
				}
				observeExplainKey(&explain, sourceIndex)
				if err := admit(sourceIndex, entry.ID, entry.Position, entry.Key, &source.definition, false); err != nil {
					_ = iterator.Close()
					return nil, explain, err
				}
				if stopped {
					break
				}
			}
			iteratorErr := iterator.Err()
			closeErr := iterator.Close()
			if iteratorErr != nil {
				return nil, explain, iteratorErr
			}
			if closeErr != nil {
				return nil, explain, closeErr
			}
			if stopped {
				break
			}
		}
		if stopped {
			break
		}
	}
	explain.EarlyStopped = stopped
	return collector.Documents(), explain, nil
}

// accessPlanStopAfter returns the match count for safe single-source early stopping, or -1 when unavailable.
func accessPlanStopAfter(access queryAccessPlan, query QuerySpec) int {
	if len(query.sort) != 0 || query.limit == nil || len(access.sources) != 1 {
		return -1
	}
	source := access.sources[0]
	if source.primary {
		if len(source.ids) != 1 {
			return -1
		}
	} else if len(source.spans) != 1 || !source.spans[0].exact {
		return -1
	}
	if *query.limit == 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if query.skip > maxInt-*query.limit {
		return -1
	}
	return query.skip + *query.limit
}

// maxOrderedUnionIterators limits the open scans used to merge an index union in insertion order.
const maxOrderedUnionIterators = 256

// canMergeStorageAccessByPosition checks whether exact stored index spans can be merged for a limited query.
func canMergeStorageAccessByPosition(access queryAccessPlan, query QuerySpec) bool {
	if len(query.sort) != 0 || query.limit == nil || accessPlanWindow(query) < 0 {
		return false
	}
	spans := 0
	for _, source := range access.sources {
		if source.primary {
			return false
		}
		for _, span := range source.spans {
			if !span.exact {
				return false
			}
			spans++
		}
	}
	return spans > 1 && spans <= maxOrderedUnionIterators
}

// executeStorageOrderedAccessPlan merges exact index scans in insertion order until the result window is filled.
func executeStorageOrderedAccessPlan(ctx context.Context, snapshot queryStorageSnapshot, collection string, access queryAccessPlan, query QuerySpec, budget *queryBudget) (documents []Document, explain ExplainResult, resultErr error) {
	explain = explainQueryAccessPlan(access, query, budget.detailed)
	configureStorageAccessEarlyStop(&explain, access, query)
	if query.limit != nil && *query.limit == 0 {
		return []Document{}, explain, nil
	}
	stopAfter := accessPlanWindow(query)
	scans := make([]*orderedStorageScan, 0, queryPlanWork(access))
	defer func() {
		for _, scan := range scans {
			if scan.iterator == nil {
				continue
			}
			if err := scan.iterator.Close(); resultErr == nil && err != nil {
				documents, resultErr = nil, err
			}
		}
	}()
	queue := orderedStorageScanHeap{}
	ordinal := 0
	for sourceIndex, source := range access.sources {
		for _, span := range source.spans {
			iterator, err := snapshot.OpenIndexIterator(collection, source.definition.Name, span.start, span.end, 0)
			if err != nil {
				return nil, explain, err
			}
			scan := &orderedStorageScan{
				iterator: iterator, definition: source.definition,
				expectedKey: span.start, ordinal: ordinal, source: sourceIndex,
			}
			ordinal++
			scans = append(scans, scan)
			next, err := advanceOrderedStorageScan(ctx, scan, budget, &explain)
			if err != nil {
				return nil, explain, err
			}
			if next {
				heap.Push(&queue, scan)
			}
		}
	}
	collector := newQueryCandidateCollector(query)
	seen := make(map[DocumentID]struct{})
	for queue.Len() > 0 {
		scan := heap.Pop(&queue).(*orderedStorageScan)
		entry := scan.entry
		if _, duplicate := seen[entry.ID]; duplicate {
			observeExplainCandidate(&explain, scan.source, true)
		} else {
			observeExplainCandidate(&explain, scan.source, false)
			record, exists, err := snapshot.GetDocumentRecord(collection, entry.ID)
			if err != nil {
				return nil, explain, err
			}
			if !exists {
				return nil, explain, ErrCorrupt
			}
			candidate, err := decodeQueryStorageCandidate(record)
			if err != nil || candidate.documentID() != entry.ID || record.Position != entry.Position {
				return nil, explain, ErrCorrupt
			}
			actualKey, indexed, err := indexDocumentKey(scan.definition, candidate.document)
			if err != nil || !indexed || !bytes.Equal(actualKey, entry.Key) {
				return nil, explain, ErrCorrupt
			}
			seen[entry.ID] = struct{}{}
			if err := budget.document(); err != nil {
				return nil, explain, err
			}
			observeExplainDocument(&explain, scan.source)
			matched, err := query.matchWithBudget(candidate.document, budget)
			if err != nil {
				return nil, explain, err
			}
			if matched {
				if err := retainQueryCandidate(&collector, budget, candidate); err != nil {
					return nil, explain, err
				}
				if len(collector.heap.items) >= stopAfter {
					explain.EarlyStopped = true
					return collector.Documents(), explain, nil
				}
			}
		}
		next, err := advanceOrderedStorageScan(ctx, scan, budget, &explain)
		if err != nil {
			return nil, explain, err
		}
		if next {
			heap.Push(&queue, scan)
		}
	}
	return collector.Documents(), explain, nil
}

// accessPlanWindow returns skip plus limit, or -1 when the query has no representable finite window.
func accessPlanWindow(query QuerySpec) int {
	if query.limit == nil {
		return -1
	}
	maxInt := int(^uint(0) >> 1)
	if query.skip > maxInt-*query.limit {
		return -1
	}
	return query.skip + *query.limit
}

// orderedStorageScan holds an exact index iterator and its current entry for an insertion-order merge.
type orderedStorageScan struct {
	iterator    queryStorageIndexIterator
	definition  IndexDefinition
	expectedKey []byte
	entry       queryStorageIndexEntry
	ordinal     int
	source      int
}

// advanceOrderedStorageScan reads and validates the next exact-key entry while charging the query budget.
func advanceOrderedStorageScan(ctx context.Context, scan *orderedStorageScan, budget *queryBudget, explain *ExplainResult) (bool, error) {
	if err := contextError(ctx); err != nil {
		return false, err
	}
	if !scan.iterator.Next() {
		return false, scan.iterator.Err()
	}
	entry := scan.iterator.Entry()
	if entry.ID.IsZero() || entry.Position == 0 || !bytes.Equal(entry.Key, scan.expectedKey) {
		return false, ErrCorrupt
	}
	if err := budget.key(); err != nil {
		return false, err
	}
	observeExplainKey(explain, scan.source)
	scan.entry = entry
	return true, nil
}

// orderedStorageScanHeap orders active scans by insertion position, document ID, and scan ordinal.
type orderedStorageScanHeap []*orderedStorageScan

// Len returns the number of active scans in the heap.
func (heap orderedStorageScanHeap) Len() int { return len(heap) }

// Less compares current entries by insertion position with stable tie breakers.
func (heap orderedStorageScanHeap) Less(left, right int) bool {
	if heap[left].entry.Position != heap[right].entry.Position {
		return heap[left].entry.Position < heap[right].entry.Position
	}
	if comparison := bytes.Compare(heap[left].entry.ID[:], heap[right].entry.ID[:]); comparison != 0 {
		return comparison < 0
	}
	return heap[left].ordinal < heap[right].ordinal
}

// Swap exchanges two scans in the heap.
func (heap orderedStorageScanHeap) Swap(left, right int) {
	heap[left], heap[right] = heap[right], heap[left]
}

// Push appends a scan for the heap package to place in order.
func (heap *orderedStorageScanHeap) Push(value any) {
	*heap = append(*heap, value.(*orderedStorageScan))
}

// Pop removes the last scan after the heap package has moved it there.
func (heap *orderedStorageScanHeap) Pop() any {
	last := len(*heap) - 1
	value := (*heap)[last]
	(*heap)[last] = nil
	*heap = (*heap)[:last]
	return value
}

// indexScanSpan describes an encoded index interval and whether it selects one complete key.
type indexScanSpan struct {
	start, end []byte
	// exact means start is one complete logical index key. Entries inside the
	// span are therefore ordered by insertion position and can participate in
	// an insertion-order union merge.
	exact bool
}

// decodeQueryStorageCandidate decodes a stored document and checks its identity and insertion position.
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

// documentID returns the ID stored in a query candidate's document.
func (candidate queryCandidate) documentID() DocumentID {
	id, _ := candidate.document.ID()
	return id
}

// indexKeyPrefixEnd returns the exclusive end of an encoded key prefix, or nil when no end exists.
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

// storageIndexBounds converts logical range endpoints into a nonempty half-open encoded interval.
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

// equalityCandidate finds an equality condition on a field within a conjunction.
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

// indexValuesCandidate returns distinct query values that safely restrict a single-field index search.
//
// A conjunction needs only one restrictive child; a
// disjunction is usable only when every branch supplies keys for the same
// field. The executor always rechecks the complete predicate, so unrelated
// conjuncts remain a residual filter rather than becoming planner assumptions.
func indexValuesCandidate(expression expr, path string) ([]Value, bool) {
	candidates, ok := encodedIndexValuesCandidate(expression, path)
	if !ok {
		return nil, false
	}
	values := make([]Value, len(candidates))
	for index := range candidates {
		values[index] = candidates[index].value
	}
	return values, true
}

// encodedIndexValueCandidate pairs a query value with its encoded index key.
type encodedIndexValueCandidate struct {
	value Value
	key   []byte
}

// encodedIndexValuesCandidate encodes and deduplicates equality or membership values usable by an index.
func encodedIndexValuesCandidate(expression expr, path string) ([]encodedIndexValueCandidate, bool) {
	values, ok := indexValuesCandidateExpr(expression, path)
	if !ok {
		return nil, false
	}
	unique := make([]encodedIndexValueCandidate, 0, len(values))
	encoded := make(map[string]struct{}, len(values))
	for _, value := range values {
		key, err := encodeIndexKey(value)
		if err != nil {
			return nil, false
		}
		canonical := string(key)
		if _, duplicate := encoded[canonical]; duplicate {
			continue
		}
		encoded[canonical] = struct{}{}
		unique = append(unique, encodedIndexValueCandidate{value: value.Clone(), key: key})
	}
	return unique, true
}

// indexValuesCandidateExpr finds equality or membership values that safely restrict a field's candidates.
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

// indexBound pairs a range endpoint value with its inclusion rule.
type indexBound struct {
	value     Value
	inclusive bool
}

// rangeCandidate finds the strongest encodable lower and upper bounds on a field within a conjunction.
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
