package database

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

// QueryLimits bounds the size and complexity of accepted queries.
type QueryLimits struct {
	MaxWireBytes  int
	MaxDepth      int
	MaxNodes      int
	MaxArrayItems int
	MaxValueBytes int
	MaxSortFields int
	MaxLimit      int
}

// DefaultQueryLimits provides the default query input limits.
var DefaultQueryLimits = QueryLimits{
	MaxWireBytes: 1 << 20, MaxDepth: 16, MaxNodes: 128,
	MaxArrayItems: 256, MaxValueBytes: 16_384, MaxSortFields: 4,
	MaxLimit: 10_000,
}

// maxQueryArraySize limits array-size operands to integers exactly representable in JavaScript.
const maxQueryArraySize = int64(1<<53 - 1)

// canonicalQueryTypes defines supported query type names in canonical order.
var canonicalQueryTypes = []struct {
	name string
	kind Kind
}{
	{name: "null", kind: NullKind},
	{name: "boolean", kind: BoolKind},
	{name: "int64", kind: Int64Kind},
	{name: "float64", kind: Float64Kind},
	{name: "string", kind: StringKind},
	{name: "date", kind: TimeKind},
	{name: "id", kind: IDKind},
	{name: "binary", kind: BinaryKind},
	{name: "array", kind: ArrayKind},
	{name: "object", kind: ObjectKind},
}

// SortField specifies a document path and its ascending or descending sort direction.
type SortField struct {
	Path      string `json:"path"`
	Direction int    `json:"direction"`
}

// QuerySpec holds a compiled filter with sorting and pagination.
type QuerySpec struct {
	where expr
	sort  []SortField
	skip  int
	limit *int
	seek  bool
}

// FilterCapability identifies a field and predicate operation for authorization.
//
// Policies can grant equality lookup independently of range scans and sorting.
type FilterCapability struct {
	Path     string
	Operator string
}

// Validate checks that the compiled query is safe to execute under default input limits.
//
// A zero QuerySpec is invalid because it has no filter expression.
func (q QuerySpec) Validate() error { return validateQuerySpec(q, DefaultQueryLimits) }

// Match reports whether a document satisfies the query's filter.
func (q QuerySpec) Match(document Document) bool {
	matched, _ := q.matchWithBudget(document, nil)
	return matched
}

// matchWithBudget evaluates the filter while charging predicate work to an optional budget.
//
// Execute passes no budget when evaluating application-supplied memory data.
func (q QuerySpec) matchWithBudget(document Document, budget *queryBudget) (bool, error) {
	if q.where == nil {
		return false, nil
	}
	return matchExpression(q.where, document, budget)
}

// Sort returns a copy of the query's ordered sort fields.
func (q QuerySpec) Sort() []SortField { return append([]SortField(nil), q.sort...) }

// Skip returns the requested result offset.
func (q QuerySpec) Skip() int { return q.skip }

// Limit returns the requested result count and whether a limit is set.
func (q QuerySpec) Limit() (int, bool) {
	if q.limit == nil {
		return 0, false
	}
	return *q.limit, true
}

// HasModifiers reports whether sorting, an offset, or a limit is set.
func (q QuerySpec) HasModifiers() bool { return len(q.sort) > 0 || q.skip != 0 || q.limit != nil }

// UsesSeekPagination reports whether the query was compiled with seek pagination.
func (q QuerySpec) UsesSeekPagination() bool { return q.seek }

// Constrain adds a required row predicate while preserving sorting and pagination.
func (q QuerySpec) Constrain(policy QuerySpec) QuerySpec {
	return QuerySpec{where: logicalExpr{op: "and", args: []expr{policy.where, q.where}}, sort: q.Sort(), skip: q.skip, limit: cloneInt(q.limit), seek: q.seek}
}

// Capped applies a nonnegative result cap to a copy of the query.
func (q QuerySpec) Capped(max int) QuerySpec {
	result := QuerySpec{where: q.where, sort: q.Sort(), skip: q.skip, limit: cloneInt(q.limit), seek: q.seek}
	if max >= 0 && (result.limit == nil || *result.limit > max) {
		result.limit = &max
	}
	return result
}

// Paths returns distinct filter and sort paths in sorted order.
func (q QuerySpec) Paths() []string {
	seen := map[string]struct{}{}
	collectExprPaths(q.where, seen)
	for _, field := range q.sort {
		seen[field.Path] = struct{}{}
	}
	paths := make([]string, 0, len(seen))
	for path := range seen {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// FilterCapabilities returns distinct field and predicate-operation pairs in sorted order.
func (q QuerySpec) FilterCapabilities() []FilterCapability {
	seen := map[FilterCapability]struct{}{}
	collectFilterCapabilities(q.where, seen)
	result := make([]FilterCapability, 0, len(seen))
	for capability := range seen {
		result = append(result, capability)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Path != result[right].Path {
			return result[left].Path < result[right].Path
		}
		return result[left].Operator < result[right].Operator
	})
	return result
}

// SortPaths returns field paths in the query's sort order.
func (q QuerySpec) SortPaths() []string {
	paths := make([]string, len(q.sort))
	for index, field := range q.sort {
		paths[index] = field.Path
	}
	return paths
}

// cloneInt copies an optional integer.
func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// collectExprPaths gathers the document paths referenced by a filter.
func collectExprPaths(expression expr, seen map[string]struct{}) {
	switch value := expression.(type) {
	case logicalExpr:
		for _, child := range value.args {
			collectExprPaths(child, seen)
		}
	case notExpr:
		collectExprPaths(value.arg, seen)
	case compareExpr:
		seen[value.path] = struct{}{}
	case membershipExpr:
		seen[value.path] = struct{}{}
	case existsExpr:
		seen[value.path] = struct{}{}
	case sizeExpr:
		seen[value.path] = struct{}{}
	case typeExpr:
		seen[value.path] = struct{}{}
	case allExpr:
		seen[value.path] = struct{}{}
	case elemMatchExpr:
		seen[value.path] = struct{}{}
	}
}

// collectFilterCapabilities gathers field and predicate-operation pairs for authorization.
func collectFilterCapabilities(expression expr, seen map[FilterCapability]struct{}) {
	switch value := expression.(type) {
	case logicalExpr:
		for _, child := range value.args {
			collectFilterCapabilities(child, seen)
		}
	case notExpr:
		collectFilterCapabilities(value.arg, seen)
	case compareExpr:
		seen[FilterCapability{Path: value.path, Operator: value.cmp}] = struct{}{}
	case membershipExpr:
		seen[FilterCapability{Path: value.path, Operator: value.op}] = struct{}{}
	case existsExpr:
		seen[FilterCapability{Path: value.path, Operator: "exists"}] = struct{}{}
	case sizeExpr:
		seen[FilterCapability{Path: value.path, Operator: "size"}] = struct{}{}
	case typeExpr:
		seen[FilterCapability{Path: value.path, Operator: "type"}] = struct{}{}
	case allExpr:
		seen[FilterCapability{Path: value.path, Operator: "all"}] = struct{}{}
	case elemMatchExpr:
		seen[FilterCapability{Path: value.path, Operator: "elem_match"}] = struct{}{}
	}
}

// expr defines the closed set of document query expressions.
type expr interface{ queryExpression() }

// trueExpr represents an unconditional document match.
type trueExpr struct{}

// queryExpression marks trueExpr as a document query expression.
func (trueExpr) queryExpression() {}

// logicalExpr combines document conditions with AND or OR.
type logicalExpr struct {
	op   string
	args []expr
}

// queryExpression marks logicalExpr as a document query expression.
func (logicalExpr) queryExpression() {}

// matchExpression evaluates a document condition and charges expression visits to its budget.
func matchExpression(expression expr, d Document, budget *queryBudget) (bool, error) {
	if budget != nil {
		if err := budget.predicate(); err != nil {
			return false, err
		}
	}
	switch e := expression.(type) {
	case trueExpr:
		return true, nil
	case logicalExpr:
		return e.match(d, budget)
	case notExpr:
		matched, err := matchExpression(e.arg, d, budget)
		return !matched, err
	case compareExpr:
		return e.match(d, budget)
	case membershipExpr:
		return e.match(d, budget)
	case existsExpr:
		return e.match(d, budget)
	case sizeExpr:
		return e.match(d, budget)
	case typeExpr:
		return e.match(d, budget)
	case allExpr:
		return e.match(d, budget)
	case elemMatchExpr:
		return e.match(d, budget)
	default:
		return false, ErrCorrupt
	}
}

// match evaluates AND or OR conditions with short-circuiting.
func (e logicalExpr) match(d Document, budget *queryBudget) (bool, error) {
	if e.op == "and" {
		for _, arg := range e.args {
			matched, err := matchExpression(arg, d, budget)
			if err != nil || !matched {
				return false, err
			}
		}
		return true, nil
	}
	for _, arg := range e.args {
		matched, err := matchExpression(arg, d, budget)
		if err != nil || matched {
			return matched, err
		}
	}
	return false, nil
}

// notExpr negates one document condition.
type notExpr struct{ arg expr }

// queryExpression marks notExpr as a document query expression.
func (notExpr) queryExpression() {}

// compareExpr compares a document field with one value.
type compareExpr struct {
	cmp, path string
	value     Value
}

// queryExpression marks compareExpr as a document query expression.
func (compareExpr) queryExpression() {}

// match compares a field with a value, with missing fields matching only not-equal conditions.
func (e compareExpr) match(d Document, budget *queryBudget) (bool, error) {
	value, found := lookupInternal(d, e.path)
	if !found {
		return e.cmp == "ne", nil
	}
	if e.cmp == "eq" {
		return fieldEqualsWithBudget(value, e.value, budget)
	}
	if e.cmp == "ne" {
		matched, err := fieldEqualsWithBudget(value, e.value, budget)
		return !matched, err
	}
	comparison, comparable := compareValues(value, e.value)
	if !comparable {
		return false, nil
	}
	switch e.cmp {
	case "gt":
		return comparison > 0, nil
	case "gte":
		return comparison >= 0, nil
	case "lt":
		return comparison < 0, nil
	default:
		return comparison <= 0, nil
	}
}

// membershipExpr tests a document field against allowed or excluded values.
type membershipExpr struct {
	op, path string
	values   []Value
}

// queryExpression marks membershipExpr as a document query expression.
func (membershipExpr) queryExpression() {}

// match tests field membership and charges attempted comparisons to its budget.
func (e membershipExpr) match(d Document, budget *queryBudget) (bool, error) {
	value, found := lookupInternal(d, e.path)
	hit := false
	if found {
		for _, candidate := range e.values {
			if budget != nil {
				if err := budget.predicate(); err != nil {
					return false, err
				}
			}
			matched, err := fieldEqualsWithBudget(value, candidate, budget)
			if err != nil {
				return false, err
			}
			if matched {
				hit = true
				break
			}
		}
	}
	if e.op == "in" {
		return hit, nil
	}
	return !hit, nil
}

// existsExpr tests whether a document field is present.
type existsExpr struct {
	path  string
	value bool
}

// queryExpression marks existsExpr as a document query expression.
func (existsExpr) queryExpression() {}

// match checks whether field presence equals the requested condition.
func (e existsExpr) match(d Document, _ *queryBudget) (bool, error) {
	_, found := lookupInternal(d, e.path)
	return found == e.value, nil
}

// sizeExpr tests a document array's length.
type sizeExpr struct {
	path string
	size int64
}

// queryExpression marks sizeExpr as a document query expression.
func (sizeExpr) queryExpression() {}

// match checks the length of an existing array field.
func (e sizeExpr) match(d Document, _ *queryBudget) (bool, error) {
	value, found := lookupInternal(d, e.path)
	return found && value.kind == ArrayKind && int64(len(value.arr)) == e.size, nil
}

// typeExpr tests a document field against selected value types.
type typeExpr struct {
	path  string
	types []Kind
}

// allExpr requires a document array to contain every specified value.
type allExpr struct {
	path   string
	values []Value
}

// elemMatchExpr requires one array element to satisfy a scalar or object condition.
type elemMatchExpr struct {
	path   string
	mode   string
	scalar elementExpr
	object expr
}

// queryExpression marks elemMatchExpr as a document query expression.
func (elemMatchExpr) queryExpression() {}

// match finds one array element satisfying the full element condition within its budget.
func (e elemMatchExpr) match(d Document, budget *queryBudget) (bool, error) {
	value, found := lookupInternal(d, e.path)
	if !found || value.kind != ArrayKind {
		return false, nil
	}
	for _, item := range value.arr {
		if budget != nil {
			if err := budget.predicate(); err != nil {
				return false, err
			}
		}
		var matched bool
		var err error
		switch e.mode {
		case "scalar":
			matched, err = matchElementExpression(e.scalar, item, budget)
		case "object":
			if item.kind == ObjectKind {
				matched, err = matchExpression(e.object, item.obj, budget)
			}
		default:
			return false, ErrCorrupt
		}
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

// elementExpr defines the closed set of scalar array-element conditions.
type elementExpr interface{ elementExpression() }

// elementLogicalExpr combines conditions on one array element with AND or OR.
type elementLogicalExpr struct {
	op   string
	args []elementExpr
}

// elementExpression marks elementLogicalExpr as a scalar array-element condition.
func (elementLogicalExpr) elementExpression() {}

// elementNotExpr negates one array-element condition.
type elementNotExpr struct{ arg elementExpr }

// elementExpression marks elementNotExpr as a scalar array-element condition.
func (elementNotExpr) elementExpression() {}

// elementCompareExpr compares an array element with one value.
type elementCompareExpr struct {
	cmp   string
	value Value
}

// elementExpression marks elementCompareExpr as a scalar array-element condition.
func (elementCompareExpr) elementExpression() {}

// elementMembershipExpr tests an array element against allowed or excluded values.
type elementMembershipExpr struct {
	op     string
	values []Value
}

// elementExpression marks elementMembershipExpr as a scalar array-element condition.
func (elementMembershipExpr) elementExpression() {}

// matchElementExpression evaluates a condition on one array element within its budget.
func matchElementExpression(expression elementExpr, value Value, budget *queryBudget) (bool, error) {
	if budget != nil {
		if err := budget.predicate(); err != nil {
			return false, err
		}
	}
	switch e := expression.(type) {
	case elementLogicalExpr:
		if e.op == "and" {
			for _, child := range e.args {
				matched, err := matchElementExpression(child, value, budget)
				if err != nil || !matched {
					return false, err
				}
			}
			return true, nil
		}
		for _, child := range e.args {
			matched, err := matchElementExpression(child, value, budget)
			if err != nil || matched {
				return matched, err
			}
		}
		return false, nil
	case elementNotExpr:
		matched, err := matchElementExpression(e.arg, value, budget)
		return !matched, err
	case elementCompareExpr:
		if e.cmp == "eq" {
			return fieldEqualsWithBudget(value, e.value, budget)
		}
		if e.cmp == "ne" {
			matched, err := fieldEqualsWithBudget(value, e.value, budget)
			return !matched, err
		}
		comparison, comparable := compareValues(value, e.value)
		if !comparable {
			return false, nil
		}
		switch e.cmp {
		case "gt":
			return comparison > 0, nil
		case "gte":
			return comparison >= 0, nil
		case "lt":
			return comparison < 0, nil
		default:
			return comparison <= 0, nil
		}
	case elementMembershipExpr:
		hit := false
		for _, candidate := range e.values {
			if budget != nil {
				if err := budget.predicate(); err != nil {
					return false, err
				}
			}
			matched, err := fieldEqualsWithBudget(value, candidate, budget)
			if err != nil {
				return false, err
			}
			if matched {
				hit = true
				break
			}
		}
		if e.op == "in" {
			return hit, nil
		}
		return !hit, nil
	default:
		return false, ErrCorrupt
	}
}

// queryExpression marks allExpr as a document query expression.
func (allExpr) queryExpression() {}

// match checks that an array contains every required value within its comparison budget.
func (e allExpr) match(d Document, budget *queryBudget) (bool, error) {
	value, found := lookupInternal(d, e.path)
	if !found || value.kind != ArrayKind {
		return false, nil
	}
	for _, required := range e.values {
		if budget != nil {
			if err := budget.predicate(); err != nil {
				return false, err
			}
		}
		matched := false
		for _, item := range value.arr {
			if budget != nil {
				if err := budget.predicate(); err != nil {
					return false, err
				}
			}
			if item.Equal(required) {
				matched = true
				break
			}
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

// queryExpression marks typeExpr as a document query expression.
func (typeExpr) queryExpression() {}

// match checks an existing field's type against the allowed types within its budget.
func (e typeExpr) match(d Document, budget *queryBudget) (bool, error) {
	value, found := lookupInternal(d, e.path)
	if !found {
		return false, nil
	}
	for _, kind := range e.types {
		if budget != nil {
			if err := budget.predicate(); err != nil {
				return false, err
			}
		}
		if value.kind == kind {
			return true, nil
		}
	}
	return false, nil
}

// normalizeQueryTypes validates type names and returns distinct types in canonical order.
func normalizeQueryTypes(names []string, maxItems int) ([]Kind, error) {
	if len(names) == 0 || len(names) > maxItems {
		return nil, fmt.Errorf("%w: $type expects a non-empty bounded type list", ErrInvalidFilter)
	}
	selected := make(map[Kind]struct{}, len(names))
	for _, name := range names {
		found := false
		for _, candidate := range canonicalQueryTypes {
			if name == candidate.name {
				selected[candidate.kind] = struct{}{}
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: unknown query type %q", ErrInvalidFilter, name)
		}
	}
	result := make([]Kind, 0, len(selected))
	for _, candidate := range canonicalQueryTypes {
		if _, ok := selected[candidate.kind]; ok {
			result = append(result, candidate.kind)
		}
	}
	return result, nil
}

// queryTypeNames converts recognized value types into their query names.
func queryTypeNames(types []Kind) []string {
	result := make([]string, 0, len(types))
	for _, kind := range types {
		for _, candidate := range canonicalQueryTypes {
			if kind == candidate.kind {
				result = append(result, candidate.name)
				break
			}
		}
	}
	return result
}

// fieldEquals compares a field with a value using array membership for non-array operands.
func fieldEquals(field, candidate Value) bool {
	matched, _ := fieldEqualsWithBudget(field, candidate, nil)
	return matched
}

// fieldEqualsWithBudget compares field values and charges array-element comparisons to its budget.
func fieldEqualsWithBudget(field, candidate Value, budget *queryBudget) (bool, error) {
	if field.kind == ArrayKind && candidate.kind != ArrayKind {
		for _, item := range field.arr {
			if budget != nil {
				if err := budget.predicate(); err != nil {
					return false, err
				}
			}
			if item.Equal(candidate) {
				return true, nil
			}
		}
		return false, nil
	}
	return field.Equal(candidate), nil
}

// compareValues orders scalar values and reports whether range comparison is supported.
func compareValues(a, b Value) (int, bool) {
	if comparison, numeric := compareNumeric(a, b); numeric {
		return comparison, true
	}
	if !scalarKind(a.kind) || !scalarKind(b.kind) {
		return 0, false
	}
	if a.kind != b.kind {
		return compareValueKinds(a.kind, b.kind), true
	}
	switch a.kind {
	case NullKind:
		return 0, true
	case Int64Kind:
		if a.i < b.i {
			return -1, true
		}
		if a.i > b.i {
			return 1, true
		}
		return 0, true
	case Float64Kind:
		if a.f < b.f {
			return -1, true
		}
		if a.f > b.f {
			return 1, true
		}
		return 0, true
	case StringKind:
		return strings.Compare(a.s, b.s), true
	case BoolKind:
		if a.b == b.b {
			return 0, true
		}
		if !a.b {
			return -1, true
		}
		return 1, true
	case TimeKind:
		if a.t.Before(b.t) {
			return -1, true
		}
		if a.t.After(b.t) {
			return 1, true
		}
		return 0, true
	case IDKind:
		return bytes.Compare(a.id[:], b.id[:]), true
	case BinaryKind:
		return bytes.Compare(a.bin, b.bin), true
	default:
		return 0, false
	}
}

// compareSortValues compares values for sorting across scalar and complex types.
//
// Scalars share comparison rules with range predicates and index bounds.
// Arrays and objects are intentionally not range-comparable, but receive a
// stable type position for sorting; values of the same complex type retain the
// collection insertion-order tie-breaker.
func compareSortValues(a, b Value) int {
	if a.kind != b.kind {
		if comparison, numeric := compareNumeric(a, b); numeric {
			return comparison
		}
		return compareValueKinds(a.kind, b.kind)
	}
	if comparison, comparable := compareValues(a, b); comparable {
		return comparison
	}
	return 0
}

// scalarKind reports whether a value type supports scalar range comparison.
func scalarKind(kind Kind) bool {
	switch kind {
	case NullKind, BoolKind, Int64Kind, Float64Kind, StringKind, TimeKind, IDKind, BinaryKind:
		return true
	default:
		return false
	}
}

// valueKindRank assigns type positions matching scalar index-key order.
//
// Query and index ordering must agree for correct range bounds and pagination.
func valueKindRank(kind Kind) int {
	switch kind {
	case NullKind:
		return 0
	case BoolKind:
		return 1
	case Int64Kind, Float64Kind:
		return 2
	case StringKind:
		return 3
	case TimeKind:
		return 4
	case IDKind:
		return 5
	case BinaryKind:
		return 6
	case ArrayKind:
		return 7
	case ObjectKind:
		return 8
	default:
		return 9
	}
}

// compareValueKinds compares value types in the database's ordering.
func compareValueKinds(a, b Kind) int {
	left, right := valueKindRank(a), valueKindRank(b)
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

// lookupInternal follows a dotted object path without copying the stored value.
func lookupInternal(d Document, path string) (Value, bool) {
	current := d
	parts := strings.Split(path, ".")
	for i, part := range parts {
		value, ok := current[part]
		if !ok {
			return Value{}, false
		}
		if i == len(parts)-1 {
			return value, true
		}
		if value.kind != ObjectKind {
			return Value{}, false
		}
		current = value.obj
	}
	return Value{}, false
}

// validatePath rejects oversized paths and invalid field-name components.
func validatePath(path string) error {
	if len(path) == 0 || len(path) > 512 {
		return fmt.Errorf("%w: invalid field path", ErrInvalidFilter)
	}
	for _, part := range strings.Split(path, ".") {
		if err := validField(part); err != nil {
			return fmt.Errorf("%w: invalid field path %q", ErrInvalidFilter, path)
		}
	}
	return nil
}
