package server

import "github.com/crapthings/meldbase/core"

type visibilityNode struct {
	id             meldbase.DocumentID
	document       meldbase.Document
	previous, next *visibilityNode
}

// visibilityOverlay owns one connection-local projected ordered result. It is
// initialized in O(N), then applies a k-operation engine delta in O(k) without
// rebuilding or cloning the complete query result.
type visibilityOverlay struct {
	engineToken, visibleToken uint64
	head, tail                *visibilityNode
	byID                      map[meldbase.DocumentID]*visibilityNode
}

func newVisibilityOverlay(snapshot meldbase.QuerySnapshot, policy QueryPolicy) (*visibilityOverlay, []meldbase.Document, error) {
	overlay := &visibilityOverlay{
		engineToken: snapshot.Token, visibleToken: snapshot.Token,
		byID: make(map[meldbase.DocumentID]*visibilityNode, len(snapshot.Documents)),
	}
	projected := make([]meldbase.Document, len(snapshot.Documents))
	for index, document := range snapshot.Documents {
		projected[index] = project(document, policy)
		id, ok := projected[index].ID()
		if !ok || id.IsZero() || overlay.byID[id] != nil || projected[index].Validate() != nil {
			return nil, nil, meldbase.ErrCorrupt
		}
		overlay.insertBefore(&visibilityNode{id: id, document: projected[index]}, nil)
	}
	return overlay, projected, nil
}

func (overlay *visibilityOverlay) apply(delta meldbase.QueryDelta, policy QueryPolicy) (meldbase.QueryDelta, bool, error) {
	if delta.FromToken != overlay.engineToken || delta.Token <= delta.FromToken || len(delta.Operations) == 0 {
		return meldbase.QueryDelta{}, false, meldbase.ErrInvalidDelta
	}
	visible := meldbase.QueryDelta{FromToken: overlay.visibleToken, Token: delta.Token, Operations: make([]meldbase.QueryDeltaOperation, 0, len(delta.Operations))}
	for _, operation := range delta.Operations {
		node := overlay.byID[operation.DocumentID]
		var anchor *visibilityNode
		if !operation.BeforeID.IsZero() {
			anchor = overlay.byID[operation.BeforeID]
			if anchor == nil || operation.BeforeID == operation.DocumentID {
				return meldbase.QueryDelta{}, false, meldbase.ErrInvalidDelta
			}
		}
		projected := operation
		if operation.Document != nil {
			projected.Document = project(operation.Document, policy)
			id, ok := projected.Document.ID()
			if !ok || id != operation.DocumentID || projected.Document.Validate() != nil {
				return meldbase.QueryDelta{}, false, meldbase.ErrInvalidDelta
			}
		}
		switch operation.Kind {
		case meldbase.QueryDeltaRemove:
			if node == nil || !operation.BeforeID.IsZero() || operation.Document != nil {
				return meldbase.QueryDelta{}, false, meldbase.ErrInvalidDelta
			}
			overlay.remove(node)
		case meldbase.QueryDeltaAdd:
			if node != nil || projected.Document == nil {
				return meldbase.QueryDelta{}, false, meldbase.ErrInvalidDelta
			}
			overlay.insertBefore(&visibilityNode{id: operation.DocumentID, document: projected.Document}, anchor)
		case meldbase.QueryDeltaMove:
			if node == nil || operation.Document != nil || node.next == anchor {
				return meldbase.QueryDelta{}, false, meldbase.ErrInvalidDelta
			}
			overlay.remove(node)
			overlay.insertBefore(node, anchor)
		case meldbase.QueryDeltaChange:
			if node == nil || !operation.BeforeID.IsZero() || projected.Document == nil {
				return meldbase.QueryDelta{}, false, meldbase.ErrInvalidDelta
			}
			if node.document.Equal(projected.Document) {
				node.document = projected.Document
				continue
			}
			node.document = projected.Document
		default:
			return meldbase.QueryDelta{}, false, meldbase.ErrInvalidDelta
		}
		visible.Operations = append(visible.Operations, projected)
	}
	overlay.engineToken = delta.Token
	if len(visible.Operations) == 0 {
		return visible, false, nil
	}
	overlay.visibleToken = delta.Token
	return visible, true, nil
}

func (overlay *visibilityOverlay) remove(node *visibilityNode) {
	if node.previous == nil {
		overlay.head = node.next
	} else {
		node.previous.next = node.next
	}
	if node.next == nil {
		overlay.tail = node.previous
	} else {
		node.next.previous = node.previous
	}
	delete(overlay.byID, node.id)
	node.previous, node.next = nil, nil
}

func (overlay *visibilityOverlay) insertBefore(node, anchor *visibilityNode) {
	if anchor == nil {
		node.previous, node.next = overlay.tail, nil
		if overlay.tail == nil {
			overlay.head = node
		} else {
			overlay.tail.next = node
		}
		overlay.tail = node
	} else {
		node.previous, node.next = anchor.previous, anchor
		if anchor.previous == nil {
			overlay.head = node
		} else {
			anchor.previous.next = node
		}
		anchor.previous = node
	}
	overlay.byID[node.id] = node
}
