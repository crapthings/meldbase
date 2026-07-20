package meldbase

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

type Update map[string]any
type UpdateResult struct{ MatchedCount, ModifiedCount int64 }
type updateOperation struct {
	kind, path string
	value      Value
}
type MutationSpec struct{ operations []updateOperation }
type pendingUpdate struct {
	id            DocumentID
	before, after Document
}

func (c *Collection) UpdateOne(ctx context.Context, filter Filter, update Update) (UpdateResult, error) {
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		return UpdateResult{}, err
	}
	mutation, err := CompileUpdate(update)
	if err != nil {
		return UpdateResult{}, err
	}
	return c.UpdateOneQuery(ctx, query, mutation)
}
func (c *Collection) UpdateMany(ctx context.Context, filter Filter, update Update) (UpdateResult, error) {
	query, err := CompileQuery(filter, QueryOptions{})
	if err != nil {
		return UpdateResult{}, err
	}
	mutation, err := CompileUpdate(update)
	if err != nil {
		return UpdateResult{}, err
	}
	return c.UpdateManyQuery(ctx, query, mutation)
}
func (c *Collection) UpdateOneQuery(ctx context.Context, query QuerySpec, mutation MutationSpec) (UpdateResult, error) {
	return c.updateQuery(ctx, query, mutation, true, 1)
}
func (c *Collection) UpdateManyQuery(ctx context.Context, query QuerySpec, mutation MutationSpec) (UpdateResult, error) {
	return c.updateQuery(ctx, query, mutation, false, 0)
}

// UpdateManyQueryLimited atomically rejects the whole mutation when more than
// maxAffected documents match. A non-positive limit is invalid here so callers
// cannot accidentally disable a server-owned safety bound.
func (c *Collection) UpdateManyQueryLimited(ctx context.Context, query QuerySpec, mutation MutationSpec, maxAffected int) (UpdateResult, error) {
	if maxAffected <= 0 {
		return UpdateResult{}, fmt.Errorf("%w: maxAffected must be positive", ErrMutationLimit)
	}
	return c.updateQuery(ctx, query, mutation, false, maxAffected)
}
func (c *Collection) updateQuery(ctx context.Context, query QuerySpec, mutation MutationSpec, one bool, maxAffected int) (UpdateResult, error) {
	if err := contextError(ctx); err != nil {
		return UpdateResult{}, err
	}
	if err := c.validate(); err != nil {
		return UpdateResult{}, err
	}
	if len(mutation.operations) == 0 {
		return UpdateResult{}, fmt.Errorf("%w: empty compiled update", ErrInvalidUpdate)
	}
	if query.HasModifiers() {
		return UpdateResult{}, fmt.Errorf("%w: mutation query cannot sort, skip, or limit", ErrInvalidFilter)
	}
	if coordinator := c.db.commitCoordinator; coordinator != nil {
		return coordinator.submitUpdate(ctx, c.name, query, mutation, one, maxAffected)
	}
	c.db.mu.Lock()
	result, err := c.updateQueryLocked(ctx, query, mutation, one, maxAffected)
	c.db.mu.Unlock()
	return result, err
}

// updateQueryLocked preserves the original filter-selection and publication
// semantics for one request. The V2 coordinator uses it for single requests
// and logical-conflict fallback after a speculative group has been rejected.
// The caller holds db.mu.
func (c *Collection) updateQueryLocked(ctx context.Context, query QuerySpec, mutation MutationSpec, one bool, maxAffected int) (UpdateResult, error) {
	if c.db.closed {
		return UpdateResult{}, ErrClosed
	}
	if c.db.fatalErr != nil {
		return UpdateResult{}, c.db.fatalErr
	}
	data := c.db.collections[c.name]
	selectionLimit, resourceBounded := c.db.boundedMutationSelection(maxAffected, one)
	selected, err := c.selectMutationDocumentsLocked(ctx, query, one, selectionLimit)
	if err != nil {
		if resourceBounded && errors.Is(err, ErrMutationLimit) {
			c.db.metrics.resourceLimitRejections.Add(1)
			return UpdateResult{}, fmt.Errorf("%w: transaction changes exceed limit %d", ErrResourceLimit, c.db.resourceLimits.MaxTransactionChanges)
		}
		return UpdateResult{}, err
	}
	if len(selected) > 0 && data == nil {
		return UpdateResult{}, ErrCorrupt
	}
	changes := []pendingUpdate{}
	result := UpdateResult{MatchedCount: int64(len(selected))}
	for _, document := range selected {
		id, exists := document.ID()
		if !exists {
			return UpdateResult{}, ErrCorrupt
		}
		after := document.Clone()
		for _, operation := range mutation.operations {
			if err := applyUpdateOperation(after, operation); err != nil {
				return UpdateResult{}, err
			}
		}
		if err := after.Validate(); err != nil {
			return UpdateResult{}, err
		}
		if !after.Equal(document) {
			result.ModifiedCount++
			changes = append(changes, pendingUpdate{id: id, before: document.Clone(), after: after})
		}
	}
	events := make([]Change, len(changes))
	changedPaths := mutation.Paths()
	var token uint64
	if len(changes) > 0 {
		token = c.db.token + 1
	}
	for i, change := range changes {
		before, after := change.before.Clone(), change.after.Clone()
		events[i] = Change{
			Collection: c.name, Operation: UpdateOperation, DocumentID: change.id, Before: &before, After: &after,
			ChangedPaths: append([]string(nil), changedPaths...),
		}
	}
	if err := c.db.validateTransactionResource(events); err != nil {
		return UpdateResult{}, err
	}
	if err := data.validateIndexUpdates(changes); err != nil {
		return UpdateResult{}, err
	}
	if len(events) > 0 {
		if err := c.db.appendCommit(ctx, token, events); err != nil {
			return UpdateResult{}, err
		}
		c.db.token = token
	}
	if c.db.querySource == nil {
		for _, change := range changes {
			data.deleteIndexes(change.id, change.before)
			data.documents[change.id] = change.after
			data.insertIndexes(change.id, change.after)
		}
	}
	if len(events) > 0 {
		batch := ChangeBatch{Token: token, Changes: events}
		c.db.recordLiveCommit(batch)
		c.db.publish(batch)
	}
	return result, nil
}

func CompileUpdate(update Update) (MutationSpec, error) {
	if len(update) == 0 {
		return MutationSpec{}, fmt.Errorf("%w: empty update", ErrInvalidUpdate)
	}
	operators := make([]string, 0, len(update))
	for operator := range update {
		operators = append(operators, operator)
	}
	sort.Strings(operators)
	result := []updateOperation{}
	seen := map[string]string{}
	for _, operator := range operators {
		if operator != "$set" && operator != "$unset" && operator != "$inc" && operator != "$push" && operator != "$pull" {
			return MutationSpec{}, fmt.Errorf("%w: unknown operator %q", ErrInvalidUpdate, operator)
		}
		fields, ok := asStringAnyMap(update[operator])
		if !ok || len(fields) == 0 {
			return MutationSpec{}, fmt.Errorf("%w: %s expects a non-empty object", ErrInvalidUpdate, operator)
		}
		paths := make([]string, 0, len(fields))
		for path := range fields {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			if err := validatePath(path); err != nil {
				return MutationSpec{}, fmt.Errorf("%w: invalid update path", ErrInvalidUpdate)
			}
			if path == "_id" || strings.HasPrefix(path, "_id.") {
				return MutationSpec{}, ErrImmutableID
			}
			for previousPath, previousOperator := range seen {
				if previousPath == path || strings.HasPrefix(previousPath, path+".") || strings.HasPrefix(path, previousPath+".") {
					return MutationSpec{}, fmt.Errorf("%w: conflicting paths %q (%s) and %q (%s)", ErrInvalidUpdate, previousPath, previousOperator, path, operator)
				}
			}
			seen[path] = operator
			value := Null()
			if operator != "$unset" {
				var err error
				value, err = ValueOf(fields[path])
				if err != nil {
					return MutationSpec{}, fmt.Errorf("%w: %v", ErrInvalidUpdate, err)
				}
			}
			if operator == "$inc" && value.kind != Int64Kind && value.kind != Float64Kind {
				return MutationSpec{}, fmt.Errorf("%w: $inc requires numeric value", ErrInvalidUpdate)
			}
			result = append(result, updateOperation{kind: operator[1:], path: path, value: value})
		}
	}
	return MutationSpec{operations: result}, nil
}

func (m MutationSpec) Paths() []string {
	paths := make([]string, len(m.operations))
	for i, operation := range m.operations {
		paths[i] = operation.path
	}
	sort.Strings(paths)
	return paths
}
func (m MutationSpec) Apply(document Document) (Document, error) {
	if len(m.operations) == 0 {
		return nil, fmt.Errorf("%w: empty compiled update", ErrInvalidUpdate)
	}
	result := document.Clone()
	for _, operation := range m.operations {
		if err := applyUpdateOperation(result, operation); err != nil {
			return nil, err
		}
	}
	if err := result.Validate(); err != nil {
		return nil, err
	}
	return result, nil
}

func applyUpdateOperation(document Document, operation updateOperation) error {
	switch operation.kind {
	case "set":
		return setPath(document, operation.path, operation.value)
	case "unset":
		unsetPath(document, operation.path)
		return nil
	case "inc":
		current, found := lookupInternal(document, operation.path)
		if !found {
			return setPath(document, operation.path, operation.value)
		}
		value, err := addNumbers(current, operation.value)
		if err != nil {
			return err
		}
		return setPath(document, operation.path, value)
	case "push":
		current, found := lookupInternal(document, operation.path)
		if !found {
			return setPath(document, operation.path, Array(operation.value))
		}
		if current.kind != ArrayKind {
			return fmt.Errorf("%w: $push target is not array", ErrInvalidUpdate)
		}
		values := cloneValues(current.arr)
		values = append(values, operation.value.Clone())
		return setPath(document, operation.path, Array(values...))
	case "pull":
		current, found := lookupInternal(document, operation.path)
		if !found {
			return nil
		}
		if current.kind != ArrayKind {
			return fmt.Errorf("%w: $pull target is not array", ErrInvalidUpdate)
		}
		values := make([]Value, 0, len(current.arr))
		for _, item := range current.arr {
			if !item.Equal(operation.value) {
				values = append(values, item)
			}
		}
		return setPath(document, operation.path, Array(values...))
	default:
		return fmt.Errorf("%w: unknown compiled operation", ErrInvalidUpdate)
	}
}

func setPath(document Document, path string, value Value) error {
	parts := strings.Split(path, ".")
	current := document
	for _, part := range parts[:len(parts)-1] {
		existing, ok := current[part]
		if !ok {
			child := Document{}
			current[part] = Value{kind: ObjectKind, obj: child}
			existing = current[part]
		}
		if existing.kind != ObjectKind {
			return fmt.Errorf("%w: path traverses non-object", ErrInvalidUpdate)
		}
		child := existing.obj.Clone()
		current[part] = Value{kind: ObjectKind, obj: child}
		current = child
	}
	current[parts[len(parts)-1]] = value.Clone()
	return nil
}

func unsetPath(document Document, path string) {
	parts := strings.Split(path, ".")
	current := document
	for _, part := range parts[:len(parts)-1] {
		existing, ok := current[part]
		if !ok || existing.kind != ObjectKind {
			return
		}
		child := existing.obj.Clone()
		current[part] = Value{kind: ObjectKind, obj: child}
		current = child
	}
	delete(current, parts[len(parts)-1])
}

func addNumbers(left, right Value) (Value, error) {
	if left.kind == Int64Kind && right.kind == Int64Kind {
		sum := left.i + right.i
		if (right.i > 0 && sum < left.i) || (right.i < 0 && sum > left.i) {
			return Value{}, fmt.Errorf("%w: integer overflow", ErrInvalidUpdate)
		}
		return Int(sum), nil
	}
	var a, b float64
	if left.kind == Int64Kind {
		var ok bool
		a, ok = exactFloat64(left.i)
		if !ok {
			return Value{}, fmt.Errorf("%w: mixed Int64/Float64 increment would lose precision", ErrInvalidUpdate)
		}
	} else if left.kind == Float64Kind {
		a = left.f
	} else {
		return Value{}, fmt.Errorf("%w: $inc target is not numeric", ErrInvalidUpdate)
	}
	if right.kind == Int64Kind {
		var ok bool
		b, ok = exactFloat64(right.i)
		if !ok {
			return Value{}, fmt.Errorf("%w: mixed Int64/Float64 increment would lose precision", ErrInvalidUpdate)
		}
	} else {
		b = right.f
	}
	sum := a + b
	if math.IsInf(sum, 0) || math.IsNaN(sum) {
		return Value{}, fmt.Errorf("%w: non-finite increment", ErrInvalidUpdate)
	}
	return Float(sum), nil
}

func exactFloat64(value int64) (float64, bool) {
	converted := float64(value)
	const two63 = float64(1 << 63)
	if converted >= two63 || converted < -two63 {
		return 0, false
	}
	return converted, int64(converted) == value
}
