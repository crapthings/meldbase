package database

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

type QueryLimits struct {
	MaxWireBytes  int
	MaxDepth      int
	MaxNodes      int
	MaxArrayItems int
	MaxValueBytes int
	MaxSortFields int
	MaxLimit      int
}

var DefaultQueryLimits = QueryLimits{
	MaxWireBytes: 1 << 20, MaxDepth: 16, MaxNodes: 128,
	MaxArrayItems: 256, MaxValueBytes: 16_384, MaxSortFields: 4,
	MaxLimit: 10_000,
}

type SortField struct {
	Path      string `json:"path"`
	Direction int    `json:"direction"`
}

func validateSortFields(fields []SortField, limits QueryLimits) error {
	if len(fields) > limits.MaxSortFields {
		return fmt.Errorf("%w: too many sort fields", ErrInvalidFilter)
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if err := validatePath(field.Path); err != nil {
			return err
		}
		if field.Direction != 1 && field.Direction != -1 {
			return fmt.Errorf("%w: invalid sort direction", ErrInvalidFilter)
		}
		if _, duplicate := seen[field.Path]; duplicate {
			return fmt.Errorf("%w: duplicate sort path %q", ErrInvalidFilter, field.Path)
		}
		seen[field.Path] = struct{}{}
	}
	return nil
}

type QuerySpec struct {
	where expr
	sort  []SortField
	skip  int
	limit *int
	seek  bool
}

// FilterCapability identifies one field-level predicate operation used by a
// compiled query. It is exposed separately from sort and result paths so
// authorization can grant equality lookup without granting range scans.
type FilterCapability struct {
	Path     string
	Operator string
}

// Validate verifies that a compiled query can safely enter any public query
// execution API. QuerySpec intentionally has private fields, but its zero value
// is still constructible by callers and must never reach a nil expression.
func (q QuerySpec) Validate() error { return validateQuerySpec(q, DefaultQueryLimits) }

func (q QuerySpec) Match(document Document) bool {
	if q.where == nil {
		return false
	}
	return q.where.match(document)
}
func (q QuerySpec) Sort() []SortField { return append([]SortField(nil), q.sort...) }
func (q QuerySpec) Skip() int         { return q.skip }
func (q QuerySpec) Limit() (int, bool) {
	if q.limit == nil {
		return 0, false
	}
	return *q.limit, true
}
func (q QuerySpec) HasModifiers() bool       { return len(q.sort) > 0 || q.skip != 0 || q.limit != nil }
func (q QuerySpec) UsesSeekPagination() bool { return q.seek }

// Constrain applies a server-owned row predicate before the caller's sort and
// pagination. This is the safe composition point for authorization policies.
func (q QuerySpec) Constrain(policy QuerySpec) QuerySpec {
	return QuerySpec{where: logicalExpr{op: "and", args: []expr{policy.where, q.where}}, sort: q.Sort(), skip: q.skip, limit: cloneInt(q.limit), seek: q.seek}
}

func (q QuerySpec) Capped(max int) QuerySpec {
	result := QuerySpec{where: q.where, sort: q.Sort(), skip: q.skip, limit: cloneInt(q.limit), seek: q.seek}
	if max >= 0 && (result.limit == nil || *result.limit > max) {
		result.limit = &max
	}
	return result
}

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

func (q QuerySpec) SortPaths() []string {
	paths := make([]string, len(q.sort))
	for index, field := range q.sort {
		paths[index] = field.Path
	}
	return paths
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

type querySpecValidator struct {
	limits QueryLimits
	nodes  int
}

func validateQuerySpec(query QuerySpec, limits QueryLimits) error {
	limits = normalizedLimits(limits)
	if query.where == nil {
		return fmt.Errorf("%w: missing where expression", ErrInvalidFilter)
	}
	if err := validateSortFields(query.sort, limits); err != nil {
		return err
	}
	if query.skip < 0 {
		return fmt.Errorf("%w: invalid skip", ErrInvalidFilter)
	}
	if query.limit != nil && (*query.limit < 0 || *query.limit > limits.MaxLimit) {
		return fmt.Errorf("%w: invalid limit", ErrInvalidFilter)
	}
	if query.seek && (len(query.sort) == 0 || query.limit == nil) {
		return fmt.Errorf("%w: seek pagination requires sort and limit", ErrInvalidFilter)
	}
	validator := querySpecValidator{limits: limits}
	if err := validator.expression(query.where, 0); err != nil {
		return err
	}
	encoded, err := marshalQuerySpecJSONUnchecked(query)
	if err != nil {
		return err
	}
	if len(encoded) > limits.MaxWireBytes {
		return fmt.Errorf("%w: query exceeds wire limit", ErrInvalidFilter)
	}
	return nil
}

func (v *querySpecValidator) expression(expression expr, depth int) error {
	if depth > v.limits.MaxDepth {
		return fmt.Errorf("%w: query nesting is too deep", ErrInvalidFilter)
	}
	v.nodes++
	if v.nodes > v.limits.MaxNodes {
		return fmt.Errorf("%w: too many query nodes", ErrInvalidFilter)
	}
	switch value := expression.(type) {
	case trueExpr:
		return nil
	case logicalExpr:
		if (value.op != "and" && value.op != "or") || len(value.args) == 0 || len(value.args) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: invalid logical expression", ErrInvalidFilter)
		}
		for _, child := range value.args {
			if child == nil {
				return fmt.Errorf("%w: missing logical expression", ErrInvalidFilter)
			}
			if err := v.expression(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	case notExpr:
		if value.arg == nil {
			return fmt.Errorf("%w: missing not expression", ErrInvalidFilter)
		}
		return v.expression(value.arg, depth+1)
	case compareExpr:
		if value.cmp != "eq" && value.cmp != "ne" && value.cmp != "gt" && value.cmp != "gte" && value.cmp != "lt" && value.cmp != "lte" {
			return fmt.Errorf("%w: invalid comparison %q", ErrInvalidFilter, value.cmp)
		}
		return v.operand(value.path, value.value)
	case membershipExpr:
		if (value.op != "in" && value.op != "nin") || len(value.values) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: invalid membership expression", ErrInvalidFilter)
		}
		for _, item := range value.values {
			if err := v.operand(value.path, item); err != nil {
				return err
			}
		}
		return nil
	case existsExpr:
		return validatePath(value.path)
	default:
		return fmt.Errorf("%w: unknown compiled expression", ErrInvalidFilter)
	}
}

func (v *querySpecValidator) operand(path string, value Value) error {
	if err := validatePath(path); err != nil {
		return err
	}
	if path == "_id" && value.kind != IDKind {
		return fmt.Errorf("%w: _id requires a document id value", ErrInvalidFilter)
	}
	if err := v.value(value, 0); err != nil {
		return err
	}
	if wireValueBytes(value) > v.limits.MaxValueBytes {
		return fmt.Errorf("%w: query value is too large", ErrInvalidFilter)
	}
	return nil
}

func (v *querySpecValidator) value(value Value, depth int) error {
	if depth > v.limits.MaxDepth {
		return fmt.Errorf("%w: query value nesting is too deep", ErrInvalidFilter)
	}
	switch value.kind {
	case Float64Kind:
		if math.IsNaN(value.f) || math.IsInf(value.f, 0) {
			return fmt.Errorf("%w: non-finite query number", ErrInvalidFilter)
		}
	case StringKind:
		if !utf8.ValidString(value.s) {
			return fmt.Errorf("%w: invalid UTF-8 query string", ErrInvalidFilter)
		}
	case ArrayKind:
		if len(value.arr) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: query array has too many items", ErrInvalidFilter)
		}
		for _, item := range value.arr {
			if err := v.value(item, depth+1); err != nil {
				return err
			}
		}
	case ObjectKind:
		if len(value.obj) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: query object has too many fields", ErrInvalidFilter)
		}
		for key, item := range value.obj {
			if err := validField(key); err != nil {
				return fmt.Errorf("%w: invalid query object field", ErrInvalidFilter)
			}
			if err := v.value(item, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

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
	}
}

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
	}
}

type queryCandidate struct {
	document Document
	position uint64
}

func (q QuerySpec) Execute(documents []Document) []Document {
	result := make([]queryCandidate, 0, len(documents))
	for i, document := range documents {
		if q.Match(document) {
			result = append(result, queryCandidate{document: document, position: uint64(i)})
		}
	}
	return q.executeMatched(result)
}

// executeMatched applies ordering and window modifiers to documents whose
// predicate membership is already known. Position is the stable collection
// insertion order used to break sort ties.
func (q QuerySpec) executeMatched(result []queryCandidate) []Document {
	if len(q.sort) > 0 {
		sort.SliceStable(result, func(i, j int) bool {
			return q.compareCandidates(result[i], result[j]) < 0
		})
	} else {
		// Storage trees are keyed by DocumentID or encoded index value, while the
		// public unsorted order is stable collection insertion order.
		ordered := true
		for index := 1; index < len(result); index++ {
			if result[index-1].position > result[index].position {
				ordered = false
				break
			}
		}
		if !ordered {
			sort.SliceStable(result, func(i, j int) bool { return result[i].position < result[j].position })
		}
	}
	start := q.skip
	if start > len(result) {
		start = len(result)
	}
	end := len(result)
	if q.limit != nil && start+*q.limit < end {
		end = start + *q.limit
	}
	out := make([]Document, end-start)
	for i := start; i < end; i++ {
		out[i-start] = result[i].document.Clone()
	}
	return out
}

func (q QuerySpec) compareCandidates(left, right queryCandidate) int {
	for _, field := range q.sort {
		lv, lok := lookupInternal(left.document, field.Path)
		rv, rok := lookupInternal(right.document, field.Path)
		if lok != rok {
			if field.Direction == 1 {
				if !lok {
					return -1
				}
				return 1
			}
			if lok {
				return -1
			}
			return 1
		}
		if !lok {
			continue
		}
		comparison := compareSortValues(lv, rv)
		if comparison == 0 {
			continue
		}
		if field.Direction == -1 {
			comparison = -comparison
		}
		return comparison
	}
	if left.position < right.position {
		return -1
	}
	if left.position > right.position {
		return 1
	}
	return 0
}

// queryCandidateCollector retains only the window a query can return. It is
// used by scan plans so sorted/limited queries do not allocate one candidate
// per matching document before applying skip and limit.
type queryCandidateCollector struct {
	query       QuerySpec
	maxRetained int
	heap        queryCandidateHeap
}

func newQueryCandidateCollector(query QuerySpec) queryCandidateCollector {
	collector := queryCandidateCollector{query: query, maxRetained: -1, heap: queryCandidateHeap{query: query}}
	if query.limit == nil {
		return collector
	}
	if query.skip > int(^uint(0)>>1)-*query.limit {
		return collector
	}
	collector.maxRetained = query.skip + *query.limit
	return collector
}

func (c *queryCandidateCollector) Add(candidate queryCandidate) (retained bool, evicted *queryCandidate) {
	if c.maxRetained < 0 {
		c.heap.items = append(c.heap.items, candidate)
		return true, nil
	}
	if c.maxRetained == 0 {
		return false, nil
	}
	if len(c.heap.items) < c.maxRetained {
		heap.Push(&c.heap, candidate)
		return true, nil
	}
	if c.query.compareCandidates(candidate, c.heap.items[0]) >= 0 {
		return false, nil
	}
	removed := c.heap.items[0]
	c.heap.items[0] = candidate
	heap.Fix(&c.heap, 0)
	return true, &removed
}

func (c *queryCandidateCollector) Documents() []Document { return c.query.executeMatched(c.heap.items) }

// queryCandidateHeap keeps the currently worst retained candidate at its root.
// Replacing that root makes bounded Top-K admission O(log(skip+limit)) rather
// than a linear scan over every retained candidate.
type queryCandidateHeap struct {
	query QuerySpec
	items []queryCandidate
}

func (h queryCandidateHeap) Len() int { return len(h.items) }
func (h queryCandidateHeap) Less(left, right int) bool {
	return h.query.compareCandidates(h.items[left], h.items[right]) > 0
}
func (h queryCandidateHeap) Swap(left, right int) {
	h.items[left], h.items[right] = h.items[right], h.items[left]
}
func (h *queryCandidateHeap) Push(value any) { h.items = append(h.items, value.(queryCandidate)) }
func (h *queryCandidateHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items = h.items[:last]
	return value
}

type expr interface{ match(Document) bool }
type trueExpr struct{}

func (trueExpr) match(Document) bool { return true }

type logicalExpr struct {
	op   string
	args []expr
}

func (e logicalExpr) match(d Document) bool {
	if e.op == "and" {
		for _, arg := range e.args {
			if !arg.match(d) {
				return false
			}
		}
		return true
	}
	for _, arg := range e.args {
		if arg.match(d) {
			return true
		}
	}
	return false
}

type notExpr struct{ arg expr }

func (e notExpr) match(d Document) bool { return !e.arg.match(d) }

type compareExpr struct {
	cmp, path string
	value     Value
}

func (e compareExpr) match(d Document) bool {
	value, found := lookupInternal(d, e.path)
	if !found {
		return e.cmp == "ne"
	}
	if e.cmp == "eq" {
		return fieldEquals(value, e.value)
	}
	if e.cmp == "ne" {
		return !fieldEquals(value, e.value)
	}
	comparison, comparable := compareValues(value, e.value)
	if !comparable {
		return false
	}
	switch e.cmp {
	case "gt":
		return comparison > 0
	case "gte":
		return comparison >= 0
	case "lt":
		return comparison < 0
	default:
		return comparison <= 0
	}
}

type membershipExpr struct {
	op, path string
	values   []Value
}

func (e membershipExpr) match(d Document) bool {
	value, found := lookupInternal(d, e.path)
	hit := false
	if found {
		for _, candidate := range e.values {
			if fieldEquals(value, candidate) {
				hit = true
				break
			}
		}
	}
	if e.op == "in" {
		return hit
	}
	return !hit
}

type existsExpr struct {
	path  string
	value bool
}

func (e existsExpr) match(d Document) bool {
	_, found := lookupInternal(d, e.path)
	return found == e.value
}

func fieldEquals(field, candidate Value) bool {
	if field.kind == ArrayKind && candidate.kind != ArrayKind {
		for _, item := range field.arr {
			if item.Equal(candidate) {
				return true
			}
		}
		return false
	}
	return field.Equal(candidate)
}

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

// compareSortValues defines Meldbase's total sort order. Scalar values use
// compareValues, which is also used by range predicates and index bounds.
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

func scalarKind(kind Kind) bool {
	switch kind {
	case NullKind, BoolKind, Int64Kind, Float64Kind, StringKind, TimeKind, IDKind, BinaryKind:
		return true
	default:
		return false
	}
}

// valueKindRank deliberately matches the scalar tag order used by
// appendIndexKey. Keeping the query comparator and index-key order aligned is
// necessary for correct range bounds and index-backed pagination.
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
