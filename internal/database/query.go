package database

import (
	"fmt"
	"sort"
	"strings"
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

type QuerySpec struct {
	where expr
	sort  []SortField
	skip  int
	limit *int
}

func (q QuerySpec) Match(document Document) bool { return q.where.match(document) }
func (q QuerySpec) Sort() []SortField            { return append([]SortField(nil), q.sort...) }
func (q QuerySpec) Skip() int                    { return q.skip }
func (q QuerySpec) Limit() (int, bool) {
	if q.limit == nil {
		return 0, false
	}
	return *q.limit, true
}
func (q QuerySpec) HasModifiers() bool { return len(q.sort) > 0 || q.skip != 0 || q.limit != nil }

// Constrain applies a server-owned row predicate before the caller's sort and
// pagination. This is the safe composition point for authorization policies.
func (q QuerySpec) Constrain(policy QuerySpec) QuerySpec {
	return QuerySpec{where: logicalExpr{op: "and", args: []expr{policy.where, q.where}}, sort: q.Sort(), skip: q.skip, limit: cloneInt(q.limit)}
}

func (q QuerySpec) Capped(max int) QuerySpec {
	result := QuerySpec{where: q.where, sort: q.Sort(), skip: q.skip, limit: cloneInt(q.limit)}
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

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
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
			left, right := result[i].document, result[j].document
			for _, field := range q.sort {
				lv, lok := lookupInternal(left, field.Path)
				rv, rok := lookupInternal(right, field.Path)
				if lok != rok {
					if field.Direction == 1 {
						return !lok
					}
					return lok
				}
				if !lok {
					continue
				}
				comparison, comparable := compareValues(lv, rv)
				if comparable && comparison != 0 {
					if field.Direction == 1 {
						return comparison < 0
					}
					return comparison > 0
				}
			}
			return result[i].position < result[j].position
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
	if a.kind != b.kind {
		return compareNumeric(a, b)
	}
	switch a.kind {
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
	default:
		return 0, false
	}
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
