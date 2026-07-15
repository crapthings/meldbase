package meldbase

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"

	btree "github.com/crapthings/meldbase/internal/index"
)

var indexNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,127}$`)

type IndexField struct {
	Field string
	Order int
}
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
	if !indexNamePattern.MatchString(name) || len(fields) != 1 || fields[0].Order != 1 {
		return fmt.Errorf("%w: V1 indexes require one ascending field", ErrInvalidIndex)
	}
	if err := validatePath(fields[0].Field); err != nil {
		return fmt.Errorf("%w: invalid field", ErrInvalidIndex)
	}
	definition := IndexDefinition{Name: name, Field: fields[0].Field, Order: 1, Unique: options.Unique}
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
	if _, exists := data.indexes[name]; exists {
		return fmt.Errorf("%w: index name exists", ErrInvalidIndex)
	}
	state, err := buildIndex(definition, data)
	if err != nil {
		return err
	}
	copyDefinition := definition
	change := Change{Collection: c.name, Operation: CreateIndexOperation, Index: &copyDefinition}
	token := c.db.token + 1
	if err := c.db.appendCommit(token, []Change{change}); err != nil {
		return err
	}
	if c.db.collections[c.name] == nil {
		c.db.collections[c.name] = data
	}
	data.indexes[name] = state
	c.db.token = token
	c.db.recordCommittedBatch(ChangeBatch{Token: token, Changes: []Change{change}})
	return nil
}

func buildIndex(definition IndexDefinition, data *collectionData) (*indexState, error) {
	state := &indexState{definition: definition, tree: btree.New()}
	for _, id := range data.order {
		document, ok := data.documents[id]
		if !ok {
			return nil, ErrCorrupt
		}
		value, found := lookupInternal(document, definition.Field)
		if !found {
			continue
		}
		key, err := encodeIndexKey(value)
		if err != nil {
			return nil, fmt.Errorf("%w: field %s is not scalar", ErrInvalidIndex, definition.Field)
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
		value, found := lookupInternal(document, state.definition.Field)
		if !found {
			continue
		}
		key, err := encodeIndexKey(value)
		if err != nil {
			return fmt.Errorf("%w: indexed field is not scalar", ErrInvalidIndex)
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
		batchOwners := map[string]DocumentID{}
		for index, document := range documents {
			value, found := lookupInternal(document, state.definition.Field)
			if !found {
				continue
			}
			key, err := encodeIndexKey(value)
			if err != nil {
				return fmt.Errorf("%w: indexed field is not scalar", ErrInvalidIndex)
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
		if !state.definition.Unique {
			continue
		}
		owners := make(map[string]DocumentID)
		for _, id := range d.order {
			document := d.documents[id]
			value, found := lookupInternal(document, state.definition.Field)
			if !found {
				continue
			}
			key, err := encodeIndexKey(value)
			if err != nil {
				return ErrInvalidIndex
			}
			owners[string(key)] = id
		}
		for _, change := range changes {
			if value, found := lookupInternal(change.before, state.definition.Field); found {
				key, err := encodeIndexKey(value)
				if err != nil {
					return ErrInvalidIndex
				}
				delete(owners, string(key))
			}
		}
		for _, change := range changes {
			if value, found := lookupInternal(change.after, state.definition.Field); found {
				key, err := encodeIndexKey(value)
				if err != nil {
					return ErrInvalidIndex
				}
				if owner, exists := owners[string(key)]; exists && owner != change.id {
					return ErrDuplicateKey
				}
				owners[string(key)] = change.id
			}
		}
	}
	return nil
}

func (d *collectionData) insertIndexes(id DocumentID, document Document) {
	for _, state := range d.indexes {
		if value, found := lookupInternal(document, state.definition.Field); found {
			if key, err := encodeIndexKey(value); err == nil {
				state.tree.Insert(key, id[:])
			}
		}
	}
}
func (d *collectionData) deleteIndexes(id DocumentID, document Document) {
	for _, state := range d.indexes {
		if value, found := lookupInternal(document, state.definition.Field); found {
			if key, err := encodeIndexKey(value); err == nil {
				state.tree.Delete(key, id[:])
			}
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
	sort.Strings(names)
	for _, name := range names {
		state := data.indexes[name]
		value, ok := equalityCandidate(query.where, state.definition.Field)
		if !ok {
			continue
		}
		key, err := encodeIndexKey(value)
		if err != nil {
			continue
		}
		rawIDs := state.tree.Get(key)
		documents := make([]Document, 0, len(rawIDs))
		for _, raw := range rawIDs {
			if len(raw) != 16 {
				continue
			}
			var id DocumentID
			copy(id[:], raw)
			if document, exists := data.documents[id]; exists {
				documents = append(documents, document)
			}
		}
		return query.Execute(documents), ExplainResult{Stage: "IXSCAN", IndexName: name, DocumentsExamined: int64(len(documents)), KeysExamined: int64(len(rawIDs))}, nil
	}
	for _, name := range names {
		state := data.indexes[name]
		lower, upper, ok := rangeCandidate(query.where, state.definition.Field)
		if !ok {
			continue
		}
		var start, end []byte
		var err error
		if lower != nil {
			start, err = encodeIndexKey(lower.value)
			if err != nil {
				continue
			}
		}
		if upper != nil {
			end, err = encodeIndexKey(upper.value)
			if err != nil {
				continue
			}
		}
		pairs := state.tree.Scan(start, end, upper == nil || upper.inclusive)
		documents := make([]Document, 0, len(pairs))
		for _, pair := range pairs {
			if len(pair.Value) != 16 {
				continue
			}
			var id DocumentID
			copy(id[:], pair.Value)
			if document, exists := data.documents[id]; exists {
				documents = append(documents, document)
			}
		}
		return query.Execute(documents), ExplainResult{Stage: "IXSCAN", IndexName: name, DocumentsExamined: int64(len(documents)), KeysExamined: int64(len(pairs))}, nil
	}
	documents := make([]Document, 0, len(data.order))
	for _, id := range data.order {
		if document, ok := data.documents[id]; ok {
			documents = append(documents, document)
		}
	}
	return query.Execute(documents), ExplainResult{Stage: "COLLSCAN", DocumentsExamined: int64(len(documents))}, nil
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
