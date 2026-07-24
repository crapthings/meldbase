package database

const (
	explainBudgetPressurePercent             = 80
	explainCompoundAdviceMinDocuments  int64 = 32
	explainCompoundAdviceAmplification       = 4
)

func explainCollectionPlan(query QuerySpec, access queryAccessPlan) ExplainResult {
	explain := ExplainResult{
		Stage:           "COLLSCAN",
		PlanReason:      "collection_scan",
		FallbackReason:  access.fallbackReason,
		UnindexedPaths:  append([]string(nil), access.unindexedPaths...),
		SortRequired:    len(query.sort) > 0,
		EarlyStopReason: "limit_not_set",
	}
	configureCollectionEarlyStop(&explain, query)
	return explain
}

func configureCollectionEarlyStop(explain *ExplainResult, query QuerySpec) {
	if explain == nil {
		return
	}
	if query.limit == nil {
		explain.EarlyStopReason = "limit_not_set"
		return
	}
	if *query.limit == 0 {
		explain.EarlyStopEligible = true
		explain.EarlyStopped = true
		explain.EarlyStopScope = "documents"
		explain.EarlyStopReason = "zero_limit"
		return
	}
	if len(query.sort) > 0 {
		explain.EarlyStopReason = "sort_required"
		return
	}
	explain.EarlyStopEligible = true
	explain.EarlyStopScope = "documents"
	explain.EarlyStopReason = "collection_insertion_order"
}

func configureMemoryAccessEarlyStop(explain *ExplainResult, access queryAccessPlan, query QuerySpec) {
	if configureCommonAccessEarlyStop(explain, query) {
		return
	}
	for _, source := range access.sources {
		if source.primary {
			continue
		}
		for _, span := range source.spans {
			if !span.exact {
				explain.EarlyStopReason = "range_scan"
				return
			}
		}
	}
	explain.EarlyStopEligible = true
	explain.EarlyStopScope = "documents"
	explain.EarlyStopReason = "candidate_position_order"
}

func configureStorageAccessEarlyStop(explain *ExplainResult, access queryAccessPlan, query QuerySpec) {
	if configureCommonAccessEarlyStop(explain, query) {
		return
	}
	if accessPlanStopAfter(access, query) >= 0 {
		explain.EarlyStopEligible = true
		explain.EarlyStopScope = "keys_and_documents"
		explain.EarlyStopReason = "single_exact_source"
		return
	}
	if canMergeStorageAccessByPosition(access, query) {
		explain.EarlyStopEligible = true
		explain.EarlyStopScope = "keys_and_documents"
		explain.EarlyStopReason = "exact_span_position_merge"
		return
	}
	primary, spans, exact := 0, 0, 0
	for _, source := range access.sources {
		if source.primary {
			primary++
			continue
		}
		for _, span := range source.spans {
			spans++
			if span.exact {
				exact++
			}
		}
	}
	switch {
	case primary > 0 && spans > 0:
		explain.EarlyStopReason = "mixed_primary_secondary"
	case primary > 0:
		explain.EarlyStopReason = "primary_union"
	case exact != spans:
		explain.EarlyStopReason = "range_scan"
	case spans > maxOrderedUnionIterators:
		explain.EarlyStopReason = "too_many_exact_spans"
	default:
		explain.EarlyStopReason = "access_order_not_proven"
	}
}

func configureCommonAccessEarlyStop(explain *ExplainResult, query QuerySpec) bool {
	if explain == nil {
		return true
	}
	if query.limit == nil {
		explain.EarlyStopReason = "limit_not_set"
		return true
	}
	if *query.limit == 0 {
		explain.EarlyStopEligible = true
		explain.EarlyStopped = true
		explain.EarlyStopScope = "keys_and_documents"
		explain.EarlyStopReason = "zero_limit"
		return true
	}
	if len(query.sort) > 0 {
		explain.EarlyStopReason = "sort_required"
		return true
	}
	if accessPlanWindow(query) < 0 {
		explain.EarlyStopReason = "window_overflow"
		return true
	}
	return false
}

func observeExplainKey(explain *ExplainResult, source int) {
	if explain == nil {
		return
	}
	explain.KeysExamined++
	if source >= 0 && source < len(explain.Sources) {
		explain.Sources[source].KeysExamined++
	}
}

func observeExplainCandidate(explain *ExplainResult, source int, duplicate bool) {
	observeExplainCandidateID(explain, source)
	observeExplainCandidateDedup(explain, source, duplicate)
}

func observeExplainCandidateID(explain *ExplainResult, source int) {
	if explain == nil {
		return
	}
	explain.CandidateIDs++
	if source >= 0 && source < len(explain.Sources) {
		explain.Sources[source].CandidateIDs++
	}
}

func observeExplainCandidateDedup(explain *ExplainResult, source int, duplicate bool) {
	if explain == nil {
		return
	}
	if duplicate {
		explain.DuplicateCandidateIDs++
		if source >= 0 && source < len(explain.Sources) {
			explain.Sources[source].DuplicateCandidateIDs++
		}
		return
	}
	explain.UniqueCandidateIDs++
	if source >= 0 && source < len(explain.Sources) {
		explain.Sources[source].UniqueCandidateIDs++
	}
}

func observeExplainDocument(explain *ExplainResult, source int) {
	if explain == nil {
		return
	}
	explain.DocumentsExamined++
	if source >= 0 && source < len(explain.Sources) {
		explain.Sources[source].DocumentsExamined++
	}
}

func explainBudgetSnapshot(budget *queryBudget) ExplainBudget {
	if budget == nil {
		return ExplainBudget{}
	}
	result := ExplainBudget{
		DocumentsUsed: budget.documents, DocumentsLimit: budget.limits.MaxQueryDocumentsExamined,
		KeysUsed: budget.keys, KeysLimit: budget.limits.MaxQueryKeysExamined,
		CandidatesUsed: budget.candidates, CandidatesLimit: budget.limits.MaxQueryCandidates,
		SortBytesUsed: budget.sortBytes, SortBytesLimit: budget.limits.MaxQuerySortBytes,
		SkipUsed: uint64(budget.query.skip), SkipLimit: budget.limits.MaxQuerySkip,
		Exceeded: budget.exceeded,
	}
	result.Pressure = highestBudgetPressure(result)
	return result
}

func highestBudgetPressure(budget ExplainBudget) string {
	type usage struct {
		name        string
		used, limit uint64
	}
	values := []usage{
		{"documents", budget.DocumentsUsed, budget.DocumentsLimit},
		{"keys", budget.KeysUsed, budget.KeysLimit},
		{"candidates", budget.CandidatesUsed, budget.CandidatesLimit},
		{"sort_bytes", budget.SortBytesUsed, budget.SortBytesLimit},
		{"skip", budget.SkipUsed, budget.SkipLimit},
	}
	bestName := ""
	bestRatio := float64(0)
	for _, value := range values {
		if value.limit == 0 {
			continue
		}
		ratio := float64(value.used) / float64(value.limit)
		if ratio*100 < explainBudgetPressurePercent || ratio <= bestRatio {
			continue
		}
		bestName, bestRatio = value.name, ratio
	}
	return bestName
}

func finalizeExplainAdvice(query QuerySpec, explain ExplainResult) ExplainResult {
	if len(explain.UnindexedPaths) > 0 {
		explain.Advice = append(explain.Advice, ExplainAdvice{
			Code: "consider_filter_index", Paths: append([]string(nil), explain.UnindexedPaths...),
		})
	}
	if len(query.sort) > 0 && !explain.SortIndexCompatible {
		explain.Advice = append(explain.Advice, ExplainAdvice{
			Code: "consider_sort_index", Paths: query.SortPaths(), Sort: query.Sort(),
		})
	}
	if query.limit != nil && *query.limit > 0 && !explain.EarlyStopEligible {
		explain.Advice = append(explain.Advice, ExplainAdvice{Code: "limit_requires_full_scan"})
	}
	if explain.CandidateIDs > 0 && explain.DuplicateCandidateIDs >= quarterRoundedUp(explain.CandidateIDs) &&
		(len(explain.Sources) > 1 || explainSourceSpanCount(explain.Sources) > 1) {
		explain.Advice = append(explain.Advice, ExplainAdvice{Code: "high_union_overlap"})
	}
	if explain.CompoundIndexOpportunity && !query.HasModifiers() &&
		hasResidualAmplification(explain, explainCompoundAdviceAmplification) {
		explain.Advice = append(explain.Advice, ExplainAdvice{
			Code: "consider_compound_index", Paths: append([]string(nil), explain.IndexableConjunctPaths...),
		})
	}
	if explain.Budget.Pressure != "" || explain.Budget.Exceeded != "" {
		explain.Advice = append(explain.Advice, ExplainAdvice{Code: "budget_pressure"})
	}
	return explain
}

func hasResidualAmplification(explain ExplainResult, factor uint64) bool {
	if explain.DocumentsExamined < explainCompoundAdviceMinDocuments {
		return false
	}
	examined := uint64(explain.DocumentsExamined)
	if explain.CandidatesRetained == 0 {
		return true
	}
	return examined/explain.CandidatesRetained >= factor
}

func quarterRoundedUp(value int64) int64 {
	result := value / 4
	if value%4 != 0 {
		result++
	}
	return result
}

func explainSourceSpanCount(sources []ExplainAccessSource) int {
	total := 0
	for _, source := range sources {
		total += source.Spans
	}
	return total
}
