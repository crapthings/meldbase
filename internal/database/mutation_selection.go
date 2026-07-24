package database

import "context"

// selectMutationDocumentsLocked runs while db.mu is write-locked. Storage-
// backed engines pin one immutable query snapshot at the same commit token;
// in-memory/V1 engines retain their insertion-order scan.
func (c *Collection) selectMutationDocumentsLocked(ctx context.Context, query QuerySpec, one bool, maxAffected int) ([]Document, error) {
	if c.db.querySource != nil {
		selection := query
		cap := 0
		if one {
			cap = 1
		} else if maxAffected > 0 {
			cap = maxAffected + 1
		}
		if cap > 0 {
			selection = selection.Capped(cap)
		}
		budget, err := c.db.newQueryBudget(selection)
		if err != nil {
			return nil, err
		}
		documents, _, err := c.planStorageLocked(ctx, selection, budget)
		if err != nil {
			return nil, err
		}
		if maxAffected > 0 && len(documents) > maxAffected {
			return nil, ErrMutationLimit
		}
		return documents, nil
	}
	data := c.db.collections[c.name]
	if data == nil {
		return nil, nil
	}
	limit := len(data.order)
	if one && limit > 1 {
		limit = 1
	}
	budget, err := c.db.newQueryBudget(query)
	if err != nil {
		return nil, err
	}
	documents := make([]Document, 0, limit)
	for _, id := range data.order {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		document, exists := data.documents[id]
		if exists {
			if err := budget.document(); err != nil {
				return nil, err
			}
		}
		if !exists {
			continue
		}
		matched, err := query.matchWithBudget(document, budget)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		if err := budget.candidate(document); err != nil {
			return nil, err
		}
		documents = append(documents, document.Clone())
		if maxAffected > 0 && len(documents) > maxAffected {
			return nil, ErrMutationLimit
		}
		if one {
			break
		}
	}
	return documents, nil
}
