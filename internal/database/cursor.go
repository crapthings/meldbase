package database

import (
	"context"
	"sync"
)

// Cursor reads query results and releases their resources when closed.
type Cursor struct {
	mu         sync.Mutex
	documents  []Document
	position   int
	stream     queryStorageDocumentIterator
	query      QuerySpec
	budget     *queryBudget
	skip       int
	remaining  int
	db         *DB
	explain    ExplainResult
	recorded   bool
	returned   int
	closed     bool
	active     bool
	diagnostic diagnosticSpan
	stop       func() bool
}

// Next returns the next query result and reports whether a document was returned.
func (c *Cursor) Next(ctx context.Context) (Document, bool, error) {
	if c == nil {
		return nil, false, ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := contextError(ctx); err != nil {
		_ = c.finishLocked(err)
		return nil, false, err
	}
	if c.closed {
		return nil, false, nil
	}
	if c.stream != nil {
		if c.remaining == 0 {
			return nil, false, c.finishLocked(nil)
		}
		for c.stream.Next() {
			record := c.stream.Record()
			candidate, err := decodeQueryStorageCandidate(record)
			if err != nil {
				_ = c.finishLocked(err)
				return nil, false, err
			}
			if c.budget != nil {
				if err := c.budget.document(); err != nil {
					_ = c.finishLocked(err)
					return nil, false, err
				}
			}
			c.explain.DocumentsExamined++
			matched, err := c.query.matchWithBudget(candidate.document, c.budget)
			if err != nil {
				_ = c.finishLocked(err)
				return nil, false, err
			}
			if !matched {
				continue
			}
			if c.budget != nil {
				if err := c.budget.candidate(candidate.document); err != nil {
					_ = c.finishLocked(err)
					return nil, false, err
				}
			}
			if c.skip > 0 {
				c.skip--
				continue
			}
			document := candidate.document.Clone()
			c.returned++
			if c.remaining > 0 {
				c.remaining--
				if c.remaining == 0 {
					c.explain.EarlyStopped = true
					if err := c.finishLocked(nil); err != nil {
						return nil, false, err
					}
				}
			}
			return document, true, nil
		}
		err := c.stream.Err()
		if finishErr := c.finishLocked(err); err == nil {
			err = finishErr
		}
		return nil, false, err
	}
	if c.position >= len(c.documents) {
		c.closed = true
		return nil, false, nil
	}
	// Cursor owns already-detached query results. Transfer this document to the
	// caller and clear our reference instead of cloning a second time.
	document := c.documents[c.position]
	c.documents[c.position] = nil
	c.position++
	return document, true, nil
}

// All collects and consumes the remaining cursor results.
func (c *Cursor) All(ctx context.Context) ([]Document, error) {
	if c == nil {
		return nil, ErrClosed
	}
	c.mu.Lock()
	streamed := c.stream != nil
	if !streamed {
		defer c.mu.Unlock()
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		result := append([]Document(nil), c.documents[c.position:]...)
		for index := c.position; index < len(c.documents); index++ {
			c.documents[index] = nil
		}
		c.position = len(c.documents)
		c.closed = true
		return result, nil
	}
	c.mu.Unlock()
	result := make([]Document, 0)
	for {
		document, exists, err := c.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !exists {
			return result, nil
		}
		result = append(result, document)
	}
}

// Close releases cursor resources and is safe to call repeatedly.
//
// Exhaustion, limit completion, errors, and context cancellation close lazy
// cursors automatically; callers that stop early must close explicitly.
func (c *Cursor) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finishLocked(nil)
}

// closeWithError closes the cursor while preserving the operation error.
func (c *Cursor) closeWithError(err error) error {
	if c == nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finishLocked(err)
}

// finishLocked releases cursor resources and records its outcome while the caller holds c.mu.
func (c *Cursor) finishLocked(operationErr error) error {
	if c.closed {
		return operationErr
	}
	c.closed = true
	closeErr := error(nil)
	if c.stream != nil {
		closeErr = c.stream.Close()
		c.stream = nil
	} else if c.position < len(c.documents) {
		for index := c.position; index < len(c.documents); index++ {
			c.documents[index] = nil
		}
		c.position = len(c.documents)
	}
	if c.stop != nil {
		c.stop()
		c.stop = nil
	}
	resultErr := operationErr
	if resultErr == nil {
		resultErr = closeErr
	}
	if c.db != nil && !c.recorded {
		c.recorded = true
		c.explain = enrichExplain(c.explain, c.query, c.budget)
		c.db.recordQuery(c.explain, c.returned, resultErr, c.diagnostic)
	}
	if c.db != nil && c.active {
		c.active = false
		c.db.metrics.activeCursors.Add(^uint64(0))
	}
	return resultErr
}
