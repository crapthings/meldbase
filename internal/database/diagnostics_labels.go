package database

import (
	"context"
	"errors"
)

// safeDiagnosticStage keeps known query stages and replaces other values with UNKNOWN.
func safeDiagnosticStage(stage string) string {
	switch stage {
	case "COLLSCAN", "IXSCAN", "ID_LOOKUP":
		return stage
	default:
		return "UNKNOWN"
	}
}

// safeDiagnosticPlanReason keeps only approved query plan labels.
func safeDiagnosticPlanReason(reason string) string {
	switch reason {
	case "collection_scan", "empty_index_result", "primary_lookup", "primary_union",
		"secondary_index", "index_union", "multi_index_union", "not_planned":
		return reason
	default:
		return ""
	}
}

// safeDiagnosticFallbackReason keeps only approved collection-scan fallback labels.
func safeDiagnosticFallbackReason(reason string) string {
	switch reason {
	case "unfiltered", "unindexed_or_branch", "no_secondary_indexes",
		"no_usable_index", "budget_rejected":
		return reason
	default:
		return ""
	}
}

// safeDiagnosticEarlyStopReason keeps only approved early-stop reason labels.
func safeDiagnosticEarlyStopReason(reason string) string {
	switch reason {
	case "limit_not_set", "zero_limit", "sort_required", "collection_insertion_order",
		"candidate_position_order", "single_exact_source", "exact_span_position_merge",
		"window_overflow", "mixed_primary_secondary", "primary_union", "range_scan",
		"too_many_exact_spans", "access_order_not_proven":
		return reason
	default:
		return ""
	}
}

// safeDiagnosticEarlyStopScope keeps only approved early-stop scope labels.
func safeDiagnosticEarlyStopScope(scope string) string {
	switch scope {
	case "documents", "keys_and_documents":
		return scope
	default:
		return ""
	}
}

// safeDiagnosticBudgetReason keeps only approved query budget labels.
func safeDiagnosticBudgetReason(reason string) string {
	switch reason {
	case "documents", "keys", "candidates", "sort_bytes", "skip", "predicate_steps":
		return reason
	default:
		return ""
	}
}

// classifyDiagnosticError maps an error to a fixed diagnostic outcome and label.
func classifyDiagnosticError(err error) (DiagnosticOutcome, string) {
	if err == nil {
		return DiagnosticSuccess, ""
	}
	if errors.Is(err, context.Canceled) {
		return DiagnosticCanceled, "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return DiagnosticCanceled, "deadline"
	}
	classes := []struct {
		err  error
		name string
	}{
		{ErrClosed, "closed"}, {ErrNotFound, "not_found"},
		{ErrDuplicateID, "duplicate_id"}, {ErrDuplicateKey, "duplicate_key"},
		{ErrMutationLimit, "mutation_limit"}, {ErrDurability, "durability"},
		{ErrCorrupt, "corrupt"}, {ErrInvalidDocument, "invalid_document"},
		{ErrInvalidFilter, "invalid_filter"}, {ErrInvalidUpdate, "invalid_update"},
		{ErrInvalidIndex, "invalid_index"}, {ErrQueryBudget, "query_budget"},
	}
	for _, class := range classes {
		if errors.Is(err, class.err) {
			return DiagnosticFailure, class.name
		}
	}
	return DiagnosticFailure, "other"
}
