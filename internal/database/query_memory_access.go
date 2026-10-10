package database

import (
	"bytes"
	"context"
	"sort"
)

// memoryAccessCandidate carries an encoded document ID and the access source that supplied it.
type memoryAccessCandidate struct {
	raw    []byte
	source int
}

// executeMemoryAccessPlan gathers candidates from in-memory access sources and evaluates the query.
func executeMemoryAccessPlan(ctx context.Context, data *collectionData, access queryAccessPlan, query QuerySpec, budget *queryBudget) ([]Document, ExplainResult, error) {
	explain := explainQueryAccessPlan(access, query, budget.detailed)
	configureMemoryAccessEarlyStop(&explain, access, query)
	if query.limit != nil && *query.limit == 0 {
		return []Document{}, explain, nil
	}
	candidates := make([]memoryAccessCandidate, 0, queryPlanWork(access))
	for sourceIndex, source := range access.sources {
		if source.primary {
			for _, id := range source.ids {
				raw := make([]byte, len(id))
				copy(raw, id[:])
				candidates = append(candidates, memoryAccessCandidate{raw: raw, source: sourceIndex})
			}
			continue
		}
		state := data.indexes[source.definition.Name]
		if state == nil || state.tree == nil || !equalIndexDefinitions(state.definition, source.definition) {
			return nil, explain, ErrCorrupt
		}
		for _, span := range source.spans {
			if span.exact {
				for _, raw := range state.tree.Get(span.start) {
					candidates = append(candidates, memoryAccessCandidate{raw: raw, source: sourceIndex})
				}
				continue
			}
			for _, pair := range state.tree.Scan(span.start, span.end, false) {
				candidates = append(candidates, memoryAccessCandidate{raw: pair.Value, source: sourceIndex})
			}
		}
	}
	if canOrderMemoryAccessByPosition(access, query) {
		return executeMemoryOrderedCandidates(ctx, data, candidates, query, budget, explain)
	}
	return executeMemoryIndexCandidates(ctx, data, candidates, query, budget, explain)
}

// canOrderMemoryAccessByPosition checks whether exact access spans allow a limited query to stop in insertion order.
func canOrderMemoryAccessByPosition(access queryAccessPlan, query QuerySpec) bool {
	if len(query.sort) != 0 || query.limit == nil || accessPlanWindow(query) < 0 {
		return false
	}
	for _, source := range access.sources {
		if source.primary {
			continue
		}
		for _, span := range source.spans {
			if !span.exact {
				return false
			}
		}
	}
	return true
}

// executeMemoryOrderedCandidates checks distinct candidates in insertion order until the result window is filled.
func executeMemoryOrderedCandidates(ctx context.Context, data *collectionData, rawIDs []memoryAccessCandidate, query QuerySpec, budget *queryBudget, explain ExplainResult) ([]Document, ExplainResult, error) {
	type positionedID struct {
		id       DocumentID
		position uint64
		source   int
	}
	candidates := make([]positionedID, 0, len(rawIDs))
	seen := make(map[DocumentID]struct{}, len(rawIDs))
	for _, rawCandidate := range rawIDs {
		if err := budget.key(); err != nil {
			return nil, explain, err
		}
		observeExplainKey(&explain, rawCandidate.source)
		raw := rawCandidate.raw
		if len(raw) != len(DocumentID{}) {
			continue
		}
		var id DocumentID
		copy(id[:], raw)
		if _, duplicate := seen[id]; duplicate {
			observeExplainCandidate(&explain, rawCandidate.source, true)
			continue
		}
		observeExplainCandidate(&explain, rawCandidate.source, false)
		seen[id] = struct{}{}
		if _, exists := data.documents[id]; !exists {
			continue
		}
		position, exists := collectionDocumentPosition(data, id)
		if !exists {
			continue
		}
		candidates = append(candidates, positionedID{id: id, position: position, source: rawCandidate.source})
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].position != candidates[right].position {
			return candidates[left].position < candidates[right].position
		}
		return bytes.Compare(candidates[left].id[:], candidates[right].id[:]) < 0
	})
	collector := newQueryCandidateCollector(query)
	stopAfter := accessPlanWindow(query)
	for _, candidate := range candidates {
		if err := contextError(ctx); err != nil {
			return nil, explain, err
		}
		document := data.documents[candidate.id]
		if err := budget.document(); err != nil {
			return nil, explain, err
		}
		observeExplainDocument(&explain, candidate.source)
		matched, err := query.matchWithBudget(document, budget)
		if err != nil {
			return nil, explain, err
		}
		if !matched {
			continue
		}
		if err := retainQueryCandidate(&collector, budget, queryCandidate{document: document, position: candidate.position}); err != nil {
			return nil, explain, err
		}
		if len(collector.heap.items) >= stopAfter {
			explain.EarlyStopped = true
			break
		}
	}
	return collector.Documents(), explain, nil
}

// executeMemoryIndexCandidates checks distinct candidates and collects matches using the query ordering and window.
func executeMemoryIndexCandidates(ctx context.Context, data *collectionData, rawIDs []memoryAccessCandidate, query QuerySpec, budget *queryBudget, explain ExplainResult) ([]Document, ExplainResult, error) {
	collector := newQueryCandidateCollector(query)
	seen := make(map[DocumentID]struct{}, len(rawIDs))
	for _, rawCandidate := range rawIDs {
		if err := contextError(ctx); err != nil {
			return nil, explain, err
		}
		if err := budget.key(); err != nil {
			return nil, explain, err
		}
		observeExplainKey(&explain, rawCandidate.source)
		raw := rawCandidate.raw
		if len(raw) != len(DocumentID{}) {
			continue
		}
		var id DocumentID
		copy(id[:], raw)
		if _, duplicate := seen[id]; duplicate {
			observeExplainCandidate(&explain, rawCandidate.source, true)
			continue
		}
		observeExplainCandidate(&explain, rawCandidate.source, false)
		seen[id] = struct{}{}
		document, exists := data.documents[id]
		if !exists {
			continue
		}
		if err := budget.document(); err != nil {
			return nil, explain, err
		}
		observeExplainDocument(&explain, rawCandidate.source)
		matched, err := query.matchWithBudget(document, budget)
		if err != nil {
			return nil, explain, err
		}
		if !matched {
			continue
		}
		position, exists := collectionDocumentPosition(data, id)
		if !exists {
			continue
		}
		if err := retainQueryCandidate(&collector, budget, queryCandidate{document: document, position: position}); err != nil {
			return nil, explain, err
		}
	}
	return collector.Documents(), explain, nil
}

// collectionDocumentPosition finds a document's insertion position in the position map or legacy order slice.
func collectionDocumentPosition(data *collectionData, id DocumentID) (uint64, bool) {
	position, exists := data.positions[id]
	if exists {
		return position, true
	}
	// Compatibility for deliberately hand-built collectionData values in package
	// tests. All constructor and recovery paths own a position map.
	for index, candidate := range data.order {
		if candidate == id {
			return uint64(index), true
		}
	}
	return 0, false
}
