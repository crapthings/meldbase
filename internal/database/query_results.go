package database

import (
	"container/heap"
	"sort"
)

// queryCandidate pairs a matching document with its stable insertion position.
type queryCandidate struct {
	document Document
	position uint64
}

// Execute filters, sorts, and paginates supplied documents into copied results.
func (q QuerySpec) Execute(documents []Document) []Document {
	result := make([]queryCandidate, 0, len(documents))
	for i, document := range documents {
		if q.Match(document) {
			result = append(result, queryCandidate{document: document, position: uint64(i)})
		}
	}
	return q.executeMatched(result)
}

// executeMatched sorts known matches and returns copied documents in the requested page.
//
// Stable collection insertion positions break sort ties.
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

// compareCandidates orders matches by sort fields and then by insertion position.
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

// queryCandidateCollector retains the best matches needed to satisfy query pagination.
//
// A bounded query avoids retaining every match before applying skip and limit.
type queryCandidateCollector struct {
	query       QuerySpec
	maxRetained int
	heap        queryCandidateHeap
}

// newQueryCandidateCollector chooses a retention window from the query's skip and limit.
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

// Add retains a useful candidate and reports any candidate it replaces.
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

// Documents sorts retained matches and returns copied documents in the requested page.
func (c *queryCandidateCollector) Documents() []Document { return c.query.executeMatched(c.heap.items) }

// queryCandidateHeap keeps the worst retained candidate at its root.
//
// Replacing the root admits a better candidate without scanning all retained matches.
type queryCandidateHeap struct {
	query QuerySpec
	items []queryCandidate
}

// Len returns the number of retained candidates.
func (h queryCandidateHeap) Len() int { return len(h.items) }

// Less ranks worse candidates first so the heap root is easiest to discard.
func (h queryCandidateHeap) Less(left, right int) bool {
	return h.query.compareCandidates(h.items[left], h.items[right]) > 0
}

// Swap exchanges two retained candidates.
func (h queryCandidateHeap) Swap(left, right int) {
	h.items[left], h.items[right] = h.items[right], h.items[left]
}

// Push appends a candidate for the heap implementation.
func (h *queryCandidateHeap) Push(value any) { h.items = append(h.items, value.(queryCandidate)) }

// Pop removes the last candidate for the heap implementation.
func (h *queryCandidateHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items = h.items[:last]
	return value
}
