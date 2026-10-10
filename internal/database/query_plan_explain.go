package database

import "sort"

// explainQueryAccessPlan describes the selected candidate plan for query diagnostics.
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

// queryAccessPlanReason classifies the selected plan by its candidate source shape.
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

// explainAccessSources describes each source's bounds and lookup or scan counts.
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

// explainRangeBound copies range endpoints into a diagnostic bound for one field.
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

// cloneExplainValues copies diagnostic values and their nested data.
func cloneExplainValues(values []Value) []Value {
	result := make([]Value, len(values))
	for index := range values {
		result[index] = values[index].Clone()
	}
	return result
}

// cloneExplainBounds copies diagnostic bounds and their nested values.
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

// coalesceExplainBounds merges and deduplicates equality values for each diagnostic field.
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
