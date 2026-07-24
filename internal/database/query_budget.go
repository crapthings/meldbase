package database

import "fmt"

// queryBudget bounds one logical query across collection scans, index scans,
// lazy cursors, aggregate reads, and mutation selection. It is deliberately
// independent of the selected plan so an added index cannot weaken admission.
type queryBudget struct {
	db         *DB
	query      QuerySpec
	limits     ResourceLimits
	documents  uint64
	keys       uint64
	candidates uint64
	sortBytes  uint64
	rejected   bool
	exceeded   string
	detailed   bool
}

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

func (b *queryBudget) document() error {
	if b.documents >= b.limits.MaxQueryDocumentsExamined {
		return b.reject("documents", "documents examined exceed limit %d", b.limits.MaxQueryDocumentsExamined)
	}
	b.documents++
	return nil
}

func (b *queryBudget) key() error {
	if b.keys >= b.limits.MaxQueryKeysExamined {
		return b.reject("keys", "index keys examined exceed limit %d", b.limits.MaxQueryKeysExamined)
	}
	b.keys++
	return nil
}

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
