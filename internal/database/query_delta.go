package database

import "fmt"

type QueryDeltaOperationKind string

const (
	QueryDeltaRemove QueryDeltaOperationKind = "remove"
	QueryDeltaAdd    QueryDeltaOperationKind = "add_before"
	QueryDeltaMove   QueryDeltaOperationKind = "move_before"
	QueryDeltaChange QueryDeltaOperationKind = "change"
)

// QueryDeltaOperation mutates an ordered query result. A zero BeforeID means
// the end of the result; database document IDs are never zero.
type QueryDeltaOperation struct {
	Kind       QueryDeltaOperationKind
	DocumentID DocumentID
	BeforeID   DocumentID
	Document   Document
}

// QueryDelta transforms exactly FromToken into Token. Operations are ordered:
// removals first, followed by reverse-order add/move anchors and document
// changes. Applying them in slice order is deterministic.
type QueryDelta struct {
	FromToken  uint64
	Token      uint64
	Operations []QueryDeltaOperation
}

type sharedQueryDelta struct {
	token      uint64
	operations []QueryDeltaOperation // immutable internal documents
}

type queryDeltaNode struct {
	id       DocumentID
	document Document
	previous *queryDeltaNode
	next     *queryDeltaNode
}

type queryDeltaList struct {
	head, tail *queryDeltaNode
	byID       map[DocumentID]*queryDeltaNode
}

func buildSharedQueryDelta(previous, next []Document, token uint64) (*sharedQueryDelta, error) {
	if len(previous) == len(next) {
		operations := make([]QueryDeltaOperation, 0)
		sameOrder := true
		for index := range previous {
			previousID, previousOK := previous[index].ID()
			nextID, nextOK := next[index].ID()
			if !previousOK || !nextOK || previousID.IsZero() || nextID.IsZero() {
				return nil, fmt.Errorf("%w: result document has no valid _id", ErrInvalidDelta)
			}
			if previousID != nextID {
				sameOrder = false
				break
			}
			if !previous[index].Equal(next[index]) {
				operations = append(operations, QueryDeltaOperation{Kind: QueryDeltaChange, DocumentID: nextID, Document: next[index]})
			}
		}
		if sameOrder {
			return &sharedQueryDelta{token: token, operations: operations}, nil
		}
	}
	oldList, oldDocuments, err := newQueryDeltaList(previous, false)
	if err != nil {
		return nil, err
	}
	_, nextDocuments, err := newQueryDeltaList(next, false)
	if err != nil {
		return nil, err
	}
	operations := make([]QueryDeltaOperation, 0)
	for node := oldList.head; node != nil; {
		following := node.next
		if _, remains := nextDocuments[node.id]; !remains {
			operations = append(operations, QueryDeltaOperation{Kind: QueryDeltaRemove, DocumentID: node.id})
			oldList.remove(node)
		}
		node = following
	}
	var anchor *queryDeltaNode
	for index := len(next) - 1; index >= 0; index-- {
		document := next[index]
		id, _ := document.ID()
		node := oldList.byID[id]
		beforeID := DocumentID{}
		if anchor != nil {
			beforeID = anchor.id
		}
		if node == nil {
			node = &queryDeltaNode{id: id, document: document}
			oldList.insertBefore(node, anchor)
			operations = append(operations, QueryDeltaOperation{
				Kind: QueryDeltaAdd, DocumentID: id, BeforeID: beforeID, Document: document,
			})
		} else {
			if node.next != anchor {
				oldList.remove(node)
				oldList.insertBefore(node, anchor)
				operations = append(operations, QueryDeltaOperation{Kind: QueryDeltaMove, DocumentID: id, BeforeID: beforeID})
			}
			if !oldDocuments[id].Equal(document) {
				operations = append(operations, QueryDeltaOperation{Kind: QueryDeltaChange, DocumentID: id, Document: document})
			}
		}
		anchor = node
	}
	return &sharedQueryDelta{token: token, operations: operations}, nil
}

func newQueryDeltaList(documents []Document, clone bool) (*queryDeltaList, map[DocumentID]Document, error) {
	list := &queryDeltaList{byID: make(map[DocumentID]*queryDeltaNode, len(documents))}
	byID := make(map[DocumentID]Document, len(documents))
	nodes := make([]queryDeltaNode, len(documents))
	for index, document := range documents {
		id, valid := document.ID()
		if !valid || id.IsZero() {
			return nil, nil, fmt.Errorf("%w: result document has no valid _id", ErrInvalidDelta)
		}
		if _, duplicate := list.byID[id]; duplicate {
			return nil, nil, fmt.Errorf("%w: duplicate result document", ErrInvalidDelta)
		}
		if clone {
			document = document.Clone()
		}
		node := &nodes[index]
		*node = queryDeltaNode{id: id, document: document}
		list.insertBefore(node, nil)
		byID[id] = document
	}
	return list, byID, nil
}

func (list *queryDeltaList) remove(node *queryDeltaNode) {
	if node.previous == nil {
		list.head = node.next
	} else {
		node.previous.next = node.next
	}
	if node.next == nil {
		list.tail = node.previous
	} else {
		node.next.previous = node.previous
	}
	delete(list.byID, node.id)
	node.previous, node.next = nil, nil
}

func (list *queryDeltaList) insertBefore(node, anchor *queryDeltaNode) {
	if anchor == nil {
		node.previous, node.next = list.tail, nil
		if list.tail == nil {
			list.head = node
		} else {
			list.tail.next = node
		}
		list.tail = node
	} else {
		node.previous, node.next = anchor.previous, anchor
		if anchor.previous == nil {
			list.head = node
		} else {
			anchor.previous.next = node
		}
		anchor.previous = node
	}
	list.byID[node.id] = node
}

// ApplyQueryDelta strictly validates and applies an ordered delta without
// mutating the input snapshot.
func ApplyQueryDelta(snapshot QuerySnapshot, delta QueryDelta) (QuerySnapshot, error) {
	if delta.FromToken != snapshot.Token || delta.Token <= delta.FromToken || len(delta.Operations) == 0 {
		return QuerySnapshot{}, fmt.Errorf("%w: token contract", ErrInvalidDelta)
	}
	list, _, err := newQueryDeltaList(snapshot.Documents, true)
	if err != nil {
		return QuerySnapshot{}, err
	}
	for _, operation := range delta.Operations {
		if operation.DocumentID.IsZero() {
			return QuerySnapshot{}, fmt.Errorf("%w: zero document id", ErrInvalidDelta)
		}
		node := list.byID[operation.DocumentID]
		anchor := (*queryDeltaNode)(nil)
		if !operation.BeforeID.IsZero() {
			anchor = list.byID[operation.BeforeID]
			if anchor == nil || operation.BeforeID == operation.DocumentID {
				return QuerySnapshot{}, fmt.Errorf("%w: invalid before anchor", ErrInvalidDelta)
			}
		}
		switch operation.Kind {
		case QueryDeltaRemove:
			if node == nil || !operation.BeforeID.IsZero() || operation.Document != nil {
				return QuerySnapshot{}, fmt.Errorf("%w: invalid remove", ErrInvalidDelta)
			}
			list.remove(node)
		case QueryDeltaAdd:
			if node != nil || !documentHasID(operation.Document, operation.DocumentID) {
				return QuerySnapshot{}, fmt.Errorf("%w: invalid add", ErrInvalidDelta)
			}
			node = &queryDeltaNode{id: operation.DocumentID, document: operation.Document.Clone()}
			list.insertBefore(node, anchor)
		case QueryDeltaMove:
			if node == nil || operation.Document != nil || node.next == anchor {
				return QuerySnapshot{}, fmt.Errorf("%w: invalid move", ErrInvalidDelta)
			}
			list.remove(node)
			list.insertBefore(node, anchor)
		case QueryDeltaChange:
			if node == nil || !operation.BeforeID.IsZero() || !documentHasID(operation.Document, operation.DocumentID) {
				return QuerySnapshot{}, fmt.Errorf("%w: invalid change", ErrInvalidDelta)
			}
			node.document = operation.Document.Clone()
		default:
			return QuerySnapshot{}, fmt.Errorf("%w: unknown operation", ErrInvalidDelta)
		}
	}
	result := QuerySnapshot{Token: delta.Token, Documents: make([]Document, 0, len(list.byID))}
	for node := list.head; node != nil; node = node.next {
		result.Documents = append(result.Documents, node.document)
	}
	return result, nil
}

func documentHasID(document Document, expected DocumentID) bool {
	id, valid := document.ID()
	return valid && !id.IsZero() && id == expected && document.Validate() == nil
}

func cloneSharedQueryDelta(delta *sharedQueryDelta, from uint64) QueryDelta {
	result := QueryDelta{FromToken: from, Token: delta.token, Operations: make([]QueryDeltaOperation, len(delta.operations))}
	for index, operation := range delta.operations {
		result.Operations[index] = operation
		if operation.Document != nil {
			result.Operations[index].Document = operation.Document.Clone()
		}
	}
	return result
}
