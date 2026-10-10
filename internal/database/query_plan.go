package database

import (
	"bytes"
	"sort"
	"strings"
)

const (
	// primaryAccessScore ranks direct document ID lookups above secondary scans.
	primaryAccessScore = 1 << 20
	// emptyAccessScore ranks proven empty plans above plans that need reads.
	emptyAccessScore = 1 << 21
)

// queryAccessPlan describes the candidate sources and ranking for a query.
//
// Each source covers its logical branch; execution rechecks the full predicate
// after combining candidates.
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

// queryPlanSource describes document IDs or index intervals that produce query candidates.
type queryPlanSource struct {
	primary    bool
	definition IndexDefinition
	ids        []DocumentID
	spans      []indexScanSpan
	bounds     []ExplainBound
}

// selectQueryAccessPlan chooses a deterministic candidate plan and explains any collection-scan fallback.
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

// bestQueryAccessPlan chooses a candidate plan across direct predicates and logical branches.
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

// directQueryAccessPlan chooses the best direct ID lookup or secondary index plan.
func directQueryAccessPlan(expression expr, definitions []IndexDefinition, query QuerySpec) queryAccessPlan {
	best := primaryQueryAccessPlan(expression)
	for _, definition := range definitions {
		best = betterQueryAccessPlan(best, indexQueryAccessPlan(expression, definition, query))
	}
	return best
}

// primaryQueryAccessPlan builds an ID lookup plan from allowed _id values.
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

// indexQueryAccessPlan derives candidate scan intervals for one secondary index.
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

// normalizedSingleSourcePlan builds a single-source plan with emptiness and sort compatibility flags.
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

// normalizeQueryAccessPlan merges candidate sources by index and removes redundant IDs and intervals.
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

// directPlanFullyCoversOr reports whether a direct plan can safely replace separate OR branch planning.
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

// betterQueryAccessPlan compares candidate plans by score, sort compatibility, work, and deterministic ties.
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

// queryPlanHasNoCandidates reports whether every plan source has no IDs or scan intervals.
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

// queryPlanWork counts planned ID lookups and scan intervals for tie-breaking.
func queryPlanWork(plan queryAccessPlan) int {
	work := 0
	for _, source := range plan.sources {
		work += len(source.ids) + len(source.spans)
	}
	return work
}

// queryPlanSignature joins source names to break otherwise equal plan ties deterministically.
func queryPlanSignature(plan queryAccessPlan) string {
	parts := make([]string, 0, len(plan.sources))
	for _, source := range plan.sources {
		parts = append(parts, source.definition.Name)
	}
	return strings.Join(parts, "\x00")
}

// annotateQueryAccessPlan records opportunities for a compound index in detailed diagnostics.
//
// It detects independently indexed AND predicates left unconstrained by the
// selected non-unique source, without planning an index intersection.
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

// independentlyIndexableConjunctPaths lists AND predicate paths that each have a usable index plan.
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

// flattenConjuncts collects nested AND conditions as a flat list.
func flattenConjuncts(expression expr, result *[]expr) {
	if logical, ok := expression.(logicalExpr); ok && logical.op == "and" {
		for _, child := range logical.args {
			flattenConjuncts(child, result)
		}
		return
	}
	*result = append(*result, expression)
}

// directIndexablePredicatePath returns the field path of a supported equality, range, or membership predicate.
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

// selectedSourceConstrainedPaths lists leading index fields constrained by the selected source.
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

// classifyQueryPlanFallback identifies why no index plan applies and which useful leading index fields are missing.
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

// unindexedOrBranchPaths finds the first OR expression with a branch that lacks a usable candidate plan.
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

// missingPotentialIndexPaths lists potentially useful query fields without a leading index component.
func missingPotentialIndexPaths(expression expr, definitions []IndexDefinition) []string {
	paths := map[string]struct{}{}
	collectPotentialIndexPaths(expression, paths, false)
	return filterMissingLeadingIndexPaths(paths, definitions)
}

// collectPotentialIndexPaths collects non-negated secondary fields used by supported index predicates.
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

// filterMissingLeadingIndexPaths removes indexed leading fields and returns the remaining paths in sorted order.
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

// compactStrings removes adjacent duplicates from a string slice in place.
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

// normalizeIndexScanSpans sorts scan intervals and merges duplicate or overlapping ranges.
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

// compareLowerBounds compares lower bounds with an empty bound treated as negative infinity.
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

// compareUpperBounds compares upper bounds with an empty bound treated as positive infinity.
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

// chooseAccessScore assigns the empty-result score or keeps the supplied score.
func chooseAccessScore(empty bool, score int) int {
	if empty {
		return emptyAccessScore
	}
	return score
}

// cloneBytes copies a byte slice.
func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
