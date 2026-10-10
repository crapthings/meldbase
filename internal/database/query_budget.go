package database

import "fmt"

// queryBudget tracks work and retained candidates against one query's limits.
//
// The limits apply across scans, cursors, aggregates, and mutation selection
// regardless of the selected plan.
type queryBudget struct {
	db             *DB
	query          QuerySpec
	limits         ResourceLimits
	documents      uint64
	keys           uint64
	candidates     uint64
	sortBytes      uint64
	predicateSteps uint64
	rejected       bool
	exceeded       string
	detailed       bool
}

// newPredicateBudget creates a predicate-only work budget for reactive rebuilds and replay.
//
// Those paths enforce their own retained-view limits separately.
func newPredicateBudget(query QuerySpec, limits ResourceLimits) *queryBudget {
	return &queryBudget{query: query, limits: limits}
}

// newQueryBudget creates a query work budget and checks the requested skip count.
func (db *DB) newQueryBudget(query QuerySpec) (*queryBudget, error) {
	if db == nil {
		return nil, ErrClosed
	}
	budget := &queryBudget{db: db, query: query, limits: db.resourceLimits}
	if uint64(query.Skip()) > budget.limits.MaxQuerySkip {
		return budget, budget.reject("skip", "skip %d exceeds limit %d", query.Skip(), budget.limits.MaxQuerySkip)
	}
	return budget, nil
}

// document charges one examined document against the query limit.
func (b *queryBudget) document() error {
	if b.documents >= b.limits.MaxQueryDocumentsExamined {
		return b.reject("documents", "documents examined exceed limit %d", b.limits.MaxQueryDocumentsExamined)
	}
	b.documents++
	return nil
}

// key charges one examined index key against the query limit.
func (b *queryBudget) key() error {
	if b.keys >= b.limits.MaxQueryKeysExamined {
		return b.reject("keys", "index keys examined exceed limit %d", b.limits.MaxQueryKeysExamined)
	}
	b.keys++
	return nil
}

// predicate charges one predicate step against the query limit.
func (b *queryBudget) predicate() error {
	if b.predicateSteps >= b.limits.MaxQueryPredicateSteps {
		return b.reject("predicate_steps", "predicate steps exceed limit %d", b.limits.MaxQueryPredicateSteps)
	}
	b.predicateSteps++
	return nil
}

// candidate charges a retained document against candidate and sorting limits.
func (b *queryBudget) candidate(document Document) error {
	if b.candidates >= b.limits.MaxQueryCandidates {
		return b.reject("candidates", "query candidates exceed limit %d", b.limits.MaxQueryCandidates)
	}
	b.candidates++
	if len(b.query.sort) == 0 {
		return nil
	}
	size, err := canonicalDocumentSize(document)
	if err != nil {
		return err
	}
	if b.sortBytes > b.limits.MaxQuerySortBytes || size > b.limits.MaxQuerySortBytes-b.sortBytes {
		return b.reject("sort_bytes", "sort candidate bytes exceed limit %d", b.limits.MaxQuerySortBytes)
	}
	b.sortBytes += size
	return nil
}

// releaseCandidate removes a discarded document from candidate and sorting charges.
func (b *queryBudget) releaseCandidate(document Document) error {
	if b.candidates == 0 {
		return ErrCorrupt
	}
	b.candidates--
	if len(b.query.sort) == 0 {
		return nil
	}
	size, err := canonicalDocumentSize(document)
	if err != nil {
		return err
	}
	if size > b.sortBytes {
		return ErrCorrupt
	}
	b.sortBytes -= size
	return nil
}

// retainQueryCandidate updates budget charges when the collector retains or replaces a candidate.
func retainQueryCandidate(collector *queryCandidateCollector, budget *queryBudget, candidate queryCandidate) error {
	retained, evicted := collector.Add(candidate)
	if !retained {
		return nil
	}
	if evicted != nil {
		if err := budget.releaseCandidate(evicted.document); err != nil {
			return err
		}
	}
	return budget.candidate(candidate.document)
}

// reject records the first exceeded limit and returns a query budget error.
func (b *queryBudget) reject(kind, format string, values ...any) error {
	if b.exceeded == "" {
		b.exceeded = kind
	}
	if !b.rejected && b.db != nil {
		b.rejected = true
		b.db.metrics.resourceLimitRejections.Add(1)
	}
	return fmt.Errorf("%w: "+format, append([]any{ErrQueryBudget}, values...)...)
}
