package database

import (
	"bytes"
	"sort"
	"strings"
)

const (
	primaryAccessScore = 1 << 20
	emptyAccessScore   = 1 << 21
)

// queryAccessPlan is a storage-independent candidate admission plan. Every
// source is a complete candidate source for one disjunct; the executor always
// rechecks the full predicate after the sources have been unioned.
type queryAccessPlan struct {
	usable         bool
	empty          bool
	score          int
	sortCompatible bool
	sources        []queryPlanSource
	fallbackReason string
	unindexedPaths []string

	// Detailed Explain-only analysis. Ordinary Find planning deliberately leaves
	// these empty so detecting competing conjunction paths cannot add work to the
	// query hot path.
	indexableConjunctPaths   []string
	compoundIndexOpportunity bool
}

type queryPlanSource struct {
	primary    bool
	definition IndexDefinition
	ids        []DocumentID
	spans      []indexScanSpan
	bounds     []ExplainBound
}

func selectQueryAccessPlan(expression expr, definitions []IndexDefinition, query QuerySpec) queryAccessPlan {
	cloned := make([]IndexDefinition, len(definitions))
	for index := range definitions {
		cloned[index] = cloneIndexDefinition(definitions[index])
	}
	sort.Slice(cloned, func(left, right int) bool { return cloned[left].Name < cloned[right].Name })
	selected := bestQueryAccessPlan(expression, cloned, query)
	if !selected.usable {
		selected.fallbackReason, selected.unindexedPaths = classifyQueryPlanFallback(expression, cloned, query)
	}
	return selected
}

func bestQueryAccessPlan(expression expr, definitions []IndexDefinition, query QuerySpec) queryAccessPlan {
	best := directQueryAccessPlan(expression, definitions, query)
	logical, ok := expression.(logicalExpr)
	if !ok {
		return best
	}
	switch logical.op {
	case "and":
		for _, child := range logical.args {
			candidate := bestQueryAccessPlan(child, definitions, query)
			if candidate.usable && candidate.empty {
				return candidate
			}
			best = betterQueryAccessPlan(best, candidate)
		}
	case "or":
		if directPlanFullyCoversOr(best, definitions) {
			return best
		}
		union := queryAccessPlan{usable: true, score: emptyAccessScore}
		for _, child := range logical.args {
			candidate := bestQueryAccessPlan(child, definitions, query)
			if !candidate.usable {
				union = queryAccessPlan{}
				break
			}
			if candidate.empty {
				continue
			}
			if union.score == emptyAccessScore || candidate.score < union.score {
				union.score = candidate.score
			}
			union.sources = append(union.sources, candidate.sources...)
		}
		if union.usable {
			if len(union.sources) == 0 {
				union.empty = true
				union.score = emptyAccessScore
			} else {
				union = normalizeQueryAccessPlan(union, query)
			}
			best = betterQueryAccessPlan(best, union)
		}
	}
	return best
}

func directQueryAccessPlan(expression expr, definitions []IndexDefinition, query QuerySpec) queryAccessPlan {
	best := primaryQueryAccessPlan(expression)
	for _, definition := range definitions {
		best = betterQueryAccessPlan(best, indexQueryAccessPlan(expression, definition, query))
	}
	return best
}

func primaryQueryAccessPlan(expression expr) queryAccessPlan {
	values, ok := indexValuesCandidate(expression, "_id")
	if !ok {
		return queryAccessPlan{}
	}
	source := queryPlanSource{
		primary:    true,
		definition: IndexDefinition{Name: "_id", Field: "_id", Order: 1, Fields: []IndexField{{Field: "_id", Order: 1}}},
		bounds:     []ExplainBound{{Path: "_id", Values: cloneExplainValues(values)}},
	}
	for _, value := range values {
		if value.kind != IDKind {
			return queryAccessPlan{}
		}
		source.ids = append(source.ids, value.id)
	}
	return queryAccessPlan{
		usable:  true,
		empty:   len(source.ids) == 0,
		score:   chooseAccessScore(len(source.ids) == 0, primaryAccessScore),
		sources: []queryPlanSource{source},
	}
}

func indexQueryAccessPlan(expression expr, definition IndexDefinition, query QuerySpec) queryAccessPlan {
	if usesCompoundIndexCodec(definition) {
		score := indexQueryScore(definition, expression)
		if start, end, ok, exact := compoundIndexQueryBounds(definition, expression); ok {
			// A nil/nil non-exact compound range is the conservative encoding of
			// an empty or unusable range. Do not turn it into a full index scan.
			if !exact && len(start) == 0 && len(end) == 0 {
				return queryAccessPlan{}
			}
			source := queryPlanSource{
				definition: definition,
				spans:      []indexScanSpan{{start: cloneBytes(start), end: cloneBytes(end), exact: exact}},
				bounds:     explainIndexBounds(definition, expression),
			}
			return normalizedSingleSourcePlan(source, score, query)
		}
		if spans, ok := compoundIndexValueSpans(definition, expression); ok {
			source := queryPlanSource{
				definition: definition,
				spans:      spans,
				bounds:     explainIndexBounds(definition, expression),
			}
			return normalizedSingleSourcePlan(source, score, query)
		}
		return queryAccessPlan{}
	}
	if candidates, ok := encodedIndexValuesCandidate(expression, definition.Field); ok {
		values := make([]Value, len(candidates))
		source := queryPlanSource{
			definition: definition,
			bounds:     []ExplainBound{{Path: definition.Field, Values: values}},
		}
		for index, candidate := range candidates {
			values[index] = candidate.value
			key := candidate.key
			end := indexKeyPrefixEnd(key)
			if end == nil {
				return queryAccessPlan{}
			}
			source.spans = append(source.spans, indexScanSpan{start: key, end: end, exact: true})
		}
		return normalizedSingleSourcePlan(source, 2, query)
	}
	lower, upper, ok := rangeCandidate(expression, definition.Field)
	if !ok {
		return queryAccessPlan{}
	}
	start, end, valid, err := storageIndexBounds(lower, upper)
	if err != nil {
		return queryAccessPlan{}
	}
	source := queryPlanSource{
		definition: definition,
		bounds:     []ExplainBound{explainRangeBound(definition.Field, lower, upper)},
	}
	if valid {
		source.spans = []indexScanSpan{{start: start, end: end}}
	}
	plan := normalizedSingleSourcePlan(source, 1, query)
	if !valid {
		plan.empty = true
		plan.score = emptyAccessScore
	}
	return plan
}

func normalizedSingleSourcePlan(source queryPlanSource, score int, query QuerySpec) queryAccessPlan {
	plan := queryAccessPlan{
		usable:  true,
		empty:   len(source.ids) == 0 && len(source.spans) == 0,
		score:   score,
		sources: []queryPlanSource{source},
	}
	if plan.empty {
		plan.score = emptyAccessScore
	}
	plan.sortCompatible = !source.primary && indexSortCompatible(source.definition, query)
	return plan
}

func normalizeQueryAccessPlan(plan queryAccessPlan, query QuerySpec) queryAccessPlan {
	if !plan.usable {
		return queryAccessPlan{}
	}
	primary := queryPlanSource{
		primary:    true,
		definition: IndexDefinition{Name: "_id", Field: "_id", Order: 1, Fields: []IndexField{{Field: "_id", Order: 1}}},
	}
	hasPrimary := false
	secondary := map[string]queryPlanSource{}
	for _, source := range plan.sources {
		if source.primary {
			hasPrimary = true
			primary.ids = append(primary.ids, source.ids...)
			primary.bounds = append(primary.bounds, source.bounds...)
			continue
		}
		existing, found := secondary[source.definition.Name]
		if !found {
			existing.definition = cloneIndexDefinition(source.definition)
		}
		existing.spans = append(existing.spans, source.spans...)
		existing.bounds = append(existing.bounds, source.bounds...)
		secondary[source.definition.Name] = existing
	}
	sources := make([]queryPlanSource, 0, len(secondary)+1)
	if hasPrimary {
		seen := make(map[DocumentID]struct{}, len(primary.ids))
		ids := primary.ids[:0]
		for _, id := range primary.ids {
			if _, duplicate := seen[id]; duplicate {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
		primary.ids = ids
		primary.bounds = coalesceExplainBounds(primary.bounds)
		sources = append(sources, primary)
	}
	names := make([]string, 0, len(secondary))
	for name := range secondary {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		source := secondary[name]
		source.spans = normalizeIndexScanSpans(source.spans)
		source.bounds = coalesceExplainBounds(source.bounds)
		sources = append(sources, source)
	}
	plan.sources = sources
	plan.empty = plan.empty || queryPlanHasNoCandidates(plan)
	plan.sortCompatible = len(sources) == 1 && !sources[0].primary && indexSortCompatible(sources[0].definition, query)
	return plan
}

func directPlanFullyCoversOr(plan queryAccessPlan, definitions []IndexDefinition) bool {
	if !plan.usable || plan.empty || len(plan.sources) != 1 {
		return plan.usable && plan.empty
	}
	source := plan.sources[0]
	if source.primary {
		return true
	}
	if len(indexDefinitionFields(source.definition)) != 1 {
		return false
	}
	for _, definition := range definitions {
		if len(indexDefinitionFields(definition)) > 1 {
			return false
		}
	}
	if len(source.spans) == 0 {
		return false
	}
	for _, span := range source.spans {
		if !span.exact {
			return false
		}
	}
	return true
}

func betterQueryAccessPlan(current, candidate queryAccessPlan) queryAccessPlan {
	if !candidate.usable {
		return current
	}
	if !current.usable {
		return candidate
	}
	if current.empty != candidate.empty {
		if candidate.empty {
			return candidate
		}
		return current
	}
	if current.score != candidate.score {
		if candidate.score > current.score {
			return candidate
		}
		return current
	}
	if current.sortCompatible != candidate.sortCompatible {
		if candidate.sortCompatible {
			return candidate
		}
		return current
	}
	currentWork, candidateWork := queryPlanWork(current), queryPlanWork(candidate)
	if currentWork != candidateWork {
		if candidateWork < currentWork {
			return candidate
		}
		return current
	}
	if len(current.sources) != len(candidate.sources) {
		if len(candidate.sources) < len(current.sources) {
			return candidate
		}
		return current
	}
	if queryPlanSignature(candidate) < queryPlanSignature(current) {
		return candidate
	}
	return current
}

func queryPlanHasNoCandidates(plan queryAccessPlan) bool {
	if len(plan.sources) == 0 {
		return true
	}
	for _, source := range plan.sources {
		if len(source.ids) > 0 || len(source.spans) > 0 {
			return false
		}
	}
	return true
}

func queryPlanWork(plan queryAccessPlan) int {
	work := 0
	for _, source := range plan.sources {
		work += len(source.ids) + len(source.spans)
	}
	return work
}

func queryPlanSignature(plan queryAccessPlan) string {
	parts := make([]string, 0, len(plan.sources))
	for _, source := range plan.sources {
		parts = append(parts, source.definition.Name)
	}
	return strings.Join(parts, "\x00")
}

// annotateQueryAccessPlan records a narrow structural fact for detailed
// Explain: multiple distinct AND predicates each have a usable index path, but
// the selected non-unique source does not constrain all of them. It does not
// build or cost an intersection; observed residual amplification determines
// later whether Explain emits compound-index advice.
func annotateQueryAccessPlan(plan *queryAccessPlan, expression expr, definitions []IndexDefinition, query QuerySpec) {
	if plan == nil || !plan.usable || plan.empty {
		return
	}
	paths := independentlyIndexableConjunctPaths(expression, definitions, query)
	if len(paths) < 2 {
		return
	}
	plan.indexableConjunctPaths = paths
	if len(plan.sources) != 1 {
		return
	}
	source := plan.sources[0]
	if source.primary || source.definition.Unique {
		return
	}
	covered := selectedSourceConstrainedPaths(source, expression)
	for _, path := range paths {
		if _, exists := covered[path]; !exists {
			plan.compoundIndexOpportunity = true
			return
		}
	}
}

func independentlyIndexableConjunctPaths(expression expr, definitions []IndexDefinition, query QuerySpec) []string {
	conjuncts := make([]expr, 0)
	flattenConjuncts(expression, &conjuncts)
	paths := make(map[string]struct{})
	for _, conjunct := range conjuncts {
		path, ok := directIndexablePredicatePath(conjunct)
		if !ok || path == "_id" {
			continue
		}
		candidate := directQueryAccessPlan(conjunct, definitions, query)
		if !candidate.usable || candidate.empty || len(candidate.sources) != 1 {
			continue
		}
		paths[path] = struct{}{}
	}
	result := make([]string, 0, len(paths))
	for path := range paths {
		result = append(result, path)
	}
	sort.Strings(result)
	return result
}

func flattenConjuncts(expression expr, result *[]expr) {
	if logical, ok := expression.(logicalExpr); ok && logical.op == "and" {
		for _, child := range logical.args {
			flattenConjuncts(child, result)
		}
		return
	}
	*result = append(*result, expression)
}

func directIndexablePredicatePath(expression expr) (string, bool) {
	switch value := expression.(type) {
	case compareExpr:
		switch value.cmp {
		case "eq", "gt", "gte", "lt", "lte":
			return value.path, true
		}
	case membershipExpr:
		if value.op == "in" {
			return value.path, true
		}
	}
	return "", false
}

func selectedSourceConstrainedPaths(source queryPlanSource, expression expr) map[string]struct{} {
	result := make(map[string]struct{})
	if source.primary {
		result["_id"] = struct{}{}
		return result
	}
	for _, field := range indexDefinitionFields(source.definition) {
		if values, ok := indexValuesCandidate(expression, field.Field); ok {
			result[field.Field] = struct{}{}
			if len(values) != 1 {
				break
			}
			continue
		}
		if _, _, ok := rangeCandidate(expression, field.Field); ok {
			result[field.Field] = struct{}{}
		}
		break
	}
	return result
}

func classifyQueryPlanFallback(expression expr, definitions []IndexDefinition, query QuerySpec) (string, []string) {
	if _, unfiltered := expression.(trueExpr); unfiltered {
		return "unfiltered", nil
	}
	if paths, unindexedOR := unindexedOrBranchPaths(expression, definitions, query); unindexedOR {
		return "unindexed_or_branch", paths
	}
	paths := missingPotentialIndexPaths(expression, definitions)
	if len(definitions) == 0 {
		return "no_secondary_indexes", paths
	}
	return "no_usable_index", paths
}

func unindexedOrBranchPaths(expression expr, definitions []IndexDefinition, query QuerySpec) ([]string, bool) {
	logical, ok := expression.(logicalExpr)
	if !ok {
		return nil, false
	}
	if logical.op == "or" {
		missing := map[string]struct{}{}
		unindexed := false
		for _, child := range logical.args {
			if candidate := bestQueryAccessPlan(child, definitions, query); candidate.usable {
				continue
			}
			unindexed = true
			collectPotentialIndexPaths(child, missing, false)
		}
		if unindexed {
			return filterMissingLeadingIndexPaths(missing, definitions), true
		}
	}
	for _, child := range logical.args {
		if paths, found := unindexedOrBranchPaths(child, definitions, query); found {
			return paths, true
		}
	}
	return nil, false
}

func missingPotentialIndexPaths(expression expr, definitions []IndexDefinition) []string {
	paths := map[string]struct{}{}
	collectPotentialIndexPaths(expression, paths, false)
	return filterMissingLeadingIndexPaths(paths, definitions)
}

func collectPotentialIndexPaths(expression expr, paths map[string]struct{}, negated bool) {
	switch value := expression.(type) {
	case logicalExpr:
		for _, child := range value.args {
			collectPotentialIndexPaths(child, paths, negated)
		}
	case notExpr:
		collectPotentialIndexPaths(value.arg, paths, true)
	case compareExpr:
		if !negated && value.path != "_id" &&
			(value.cmp == "eq" || value.cmp == "gt" || value.cmp == "gte" || value.cmp == "lt" || value.cmp == "lte") {
			paths[value.path] = struct{}{}
		}
	case membershipExpr:
		if !negated && value.path != "_id" && value.op == "in" {
			paths[value.path] = struct{}{}
		}
	}
}

func filterMissingLeadingIndexPaths(paths map[string]struct{}, definitions []IndexDefinition) []string {
	for _, definition := range definitions {
		fields := indexDefinitionFields(definition)
		if len(fields) > 0 {
			delete(paths, fields[0].Field)
		}
	}
	result := make([]string, 0, len(paths))
	for path := range paths {
		result = append(result, path)
	}
	sort.Strings(result)
	return result
}

func explainQueryAccessPlan(plan queryAccessPlan, query QuerySpec, detailed bool) ExplainResult {
	if !plan.usable {
		return explainCollectionPlan(query, plan)
	}
	names := make([]string, 0, len(plan.sources))
	bounds := make([]ExplainBound, 0, len(plan.sources))
	onlyPrimary := len(plan.sources) > 0
	for _, source := range plan.sources {
		names = append(names, source.definition.Name)
		bounds = append(bounds, cloneExplainBounds(source.bounds)...)
		if !source.primary {
			onlyPrimary = false
		}
	}
	sort.Strings(names)
	names = compactStrings(names)
	stage := "IXSCAN"
	if onlyPrimary {
		stage = "ID_LOOKUP"
	}
	explain := ExplainResult{
		Stage:                    stage,
		IndexNames:               append([]string(nil), names...),
		Bounds:                   coalesceExplainBounds(bounds),
		ResidualPredicate:        true,
		SortRequired:             len(query.sort) > 0,
		SortIndexCompatible:      plan.sortCompatible,
		PlanReason:               queryAccessPlanReason(plan),
		IndexableConjunctPaths:   append([]string(nil), plan.indexableConjunctPaths...),
		CompoundIndexOpportunity: plan.compoundIndexOpportunity,
	}
	if detailed {
		explain.Sources = explainAccessSources(plan)
	}
	if len(names) == 1 {
		explain.IndexName = names[0]
	}
	return explain
}

func queryAccessPlanReason(plan queryAccessPlan) string {
	if plan.empty {
		return "empty_index_result"
	}
	if len(plan.sources) > 1 {
		return "multi_index_union"
	}
	if len(plan.sources) == 0 {
		return "empty_index_result"
	}
	source := plan.sources[0]
	if source.primary {
		if len(source.ids) > 1 {
			return "primary_union"
		}
		return "primary_lookup"
	}
	if len(source.spans) > 1 {
		return "index_union"
	}
	return "secondary_index"
}

func explainAccessSources(plan queryAccessPlan) []ExplainAccessSource {
	sources := make([]ExplainAccessSource, len(plan.sources))
	for index, source := range plan.sources {
		exact := 0
		for _, span := range source.spans {
			if span.exact {
				exact++
			}
		}
		sources[index] = ExplainAccessSource{
			IndexName:  source.definition.Name,
			Primary:    source.primary,
			Bounds:     cloneExplainBounds(source.bounds),
			Spans:      len(source.spans) + len(source.ids),
			ExactSpans: exact + len(source.ids),
		}
	}
	return sources
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

func normalizeIndexScanSpans(spans []indexScanSpan) []indexScanSpan {
	if len(spans) == 0 {
		return nil
	}
	sort.Slice(spans, func(left, right int) bool {
		if comparison := compareLowerBounds(spans[left].start, spans[right].start); comparison != 0 {
			return comparison < 0
		}
		return compareUpperBounds(spans[left].end, spans[right].end) < 0
	})
	result := spans[:0]
	for _, span := range spans {
		if len(result) == 0 {
			result = append(result, span)
			continue
		}
		last := &result[len(result)-1]
		if bytes.Equal(last.start, span.start) && bytes.Equal(last.end, span.end) {
			last.exact = last.exact && span.exact
			continue
		}
		if len(last.end) == 0 || len(span.start) == 0 || bytes.Compare(span.start, last.end) < 0 {
			if len(last.end) != 0 && (len(span.end) == 0 || bytes.Compare(span.end, last.end) > 0) {
				last.end = span.end
			}
			last.exact = false
			continue
		}
		result = append(result, span)
	}
	return result
}

func compareLowerBounds(left, right []byte) int {
	if len(left) == 0 {
		if len(right) == 0 {
			return 0
		}
		return -1
	}
	if len(right) == 0 {
		return 1
	}
	return bytes.Compare(left, right)
}

func compareUpperBounds(left, right []byte) int {
	if len(left) == 0 {
		if len(right) == 0 {
			return 0
		}
		return 1
	}
	if len(right) == 0 {
		return -1
	}
	return bytes.Compare(left, right)
}

func chooseAccessScore(empty bool, score int) int {
	if empty {
		return emptyAccessScore
	}
	return score
}

func explainRangeBound(path string, lower, upper *indexBound) ExplainBound {
	bound := ExplainBound{Path: path}
	if lower != nil {
		value := lower.value.Clone()
		bound.Lower, bound.LowerInclusive = &value, lower.inclusive
	}
	if upper != nil {
		value := upper.value.Clone()
		bound.Upper, bound.UpperInclusive = &value, upper.inclusive
	}
	return bound
}

func cloneExplainValues(values []Value) []Value {
	result := make([]Value, len(values))
	for index := range values {
		result[index] = values[index].Clone()
	}
	return result
}

func cloneExplainBounds(bounds []ExplainBound) []ExplainBound {
	result := make([]ExplainBound, len(bounds))
	for index, bound := range bounds {
		result[index] = ExplainBound{
			Path:           bound.Path,
			Values:         cloneExplainValues(bound.Values),
			LowerInclusive: bound.LowerInclusive,
			UpperInclusive: bound.UpperInclusive,
		}
		if bound.Lower != nil {
			value := bound.Lower.Clone()
			result[index].Lower = &value
		}
		if bound.Upper != nil {
			value := bound.Upper.Clone()
			result[index].Upper = &value
		}
	}
	return result
}

func coalesceExplainBounds(bounds []ExplainBound) []ExplainBound {
	if len(bounds) < 2 {
		return cloneExplainBounds(bounds)
	}
	result := make([]ExplainBound, 0, len(bounds))
	valueBounds := map[string]int{}
	valueKeys := map[string]map[string]struct{}{}
	for _, bound := range bounds {
		if bound.Lower != nil || bound.Upper != nil || len(bound.Values) == 0 {
			result = append(result, cloneExplainBounds([]ExplainBound{bound})[0])
			continue
		}
		position, exists := valueBounds[bound.Path]
		if !exists {
			position = len(result)
			valueBounds[bound.Path] = position
			valueKeys[bound.Path] = map[string]struct{}{}
			result = append(result, ExplainBound{Path: bound.Path})
		}
		for _, value := range bound.Values {
			key, err := encodeIndexKey(value)
			if err != nil {
				continue
			}
			canonical := string(key)
			if _, duplicate := valueKeys[bound.Path][canonical]; duplicate {
				continue
			}
			valueKeys[bound.Path][canonical] = struct{}{}
			result[position].Values = append(result[position].Values, value.Clone())
		}
	}
	return result
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
