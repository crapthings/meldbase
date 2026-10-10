package database

import (
	"bytes"
	"hash/maphash"
	"sort"
)

// reactiveIDNode holds a member in an immutable tree ordered by document ID.
//
// Updates copy changed paths so readers can retain old view states without locks.
type reactiveIDNode struct {
	id          DocumentID
	member      reactiveMember
	priority    uint64
	left, right *reactiveIDNode
}

// reactiveOrderNode holds a member in an immutable result-order tree with subtree counts.
type reactiveOrderNode struct {
	id          DocumentID
	member      reactiveMember
	priority    uint64
	size        uint64
	left, right *reactiveOrderNode
}

// reactiveTreeEntry pairs a document ID with a member for bulk tree construction.
type reactiveTreeEntry struct {
	id     DocumentID
	member reactiveMember
}

// reactiveNodePriority hashes an ID with a process-local seed to choose its tree priority.
func reactiveNodePriority(seed maphash.Seed, id DocumentID) uint64 {
	// The per-database process seed prevents chosen document IDs from forcing a
	// degenerate tree. Reactive state is ephemeral, so priorities need not be
	// stable across reopen.
	return maphash.Bytes(seed, id[:])
}

// compareDocumentIDs compares document IDs in byte order.
func compareDocumentIDs(left, right DocumentID) int {
	return bytes.Compare(left[:], right[:])
}

// reactiveIDGet looks up a member by ID in an immutable tree.
func reactiveIDGet(root *reactiveIDNode, id DocumentID) (reactiveMember, bool) {
	for root != nil {
		switch comparison := compareDocumentIDs(id, root.id); {
		case comparison < 0:
			root = root.left
		case comparison > 0:
			root = root.right
		default:
			return root.member, true
		}
	}
	return reactiveMember{}, false
}

// reactiveIDPut returns a tree with the member inserted or replaced through copied paths.
func reactiveIDPut(root *reactiveIDNode, seed maphash.Seed, id DocumentID, member reactiveMember) *reactiveIDNode {
	if root == nil {
		return &reactiveIDNode{id: id, member: member, priority: reactiveNodePriority(seed, id)}
	}
	copy := *root
	switch comparison := compareDocumentIDs(id, root.id); {
	case comparison < 0:
		copy.left = reactiveIDPut(root.left, seed, id, member)
		if higherReactivePriority(copy.left.priority, copy.left.id, copy.priority, copy.id) {
			return rotateReactiveIDRight(&copy)
		}
	case comparison > 0:
		copy.right = reactiveIDPut(root.right, seed, id, member)
		if higherReactivePriority(copy.right.priority, copy.right.id, copy.priority, copy.id) {
			return rotateReactiveIDLeft(&copy)
		}
	default:
		copy.member = member
	}
	return &copy
}

// reactiveIDDelete returns a tree with the requested ID removed through copied paths.
func reactiveIDDelete(root *reactiveIDNode, id DocumentID) *reactiveIDNode {
	if root == nil {
		return nil
	}
	switch comparison := compareDocumentIDs(id, root.id); {
	case comparison < 0:
		copy := *root
		copy.left = reactiveIDDelete(root.left, id)
		return &copy
	case comparison > 0:
		copy := *root
		copy.right = reactiveIDDelete(root.right, id)
		return &copy
	default:
		return mergeReactiveIDs(root.left, root.right)
	}
}

// mergeReactiveIDs joins two ordered ID subtrees while preserving existing nodes.
func mergeReactiveIDs(left, right *reactiveIDNode) *reactiveIDNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if higherReactivePriority(left.priority, left.id, right.priority, right.id) {
		copy := *left
		copy.right = mergeReactiveIDs(left.right, right)
		return &copy
	}
	copy := *right
	copy.left = mergeReactiveIDs(left, right.left)
	return &copy
}

// rotateReactiveIDRight rotates a private root copy right while copying its left child.
func rotateReactiveIDRight(root *reactiveIDNode) *reactiveIDNode {
	left := *root.left
	root.left = left.right
	left.right = root
	return &left
}

// rotateReactiveIDLeft rotates a private root copy left while copying its right child.
func rotateReactiveIDLeft(root *reactiveIDNode) *reactiveIDNode {
	right := *root.right
	root.right = right.left
	right.left = root
	return &right
}

// higherReactivePriority compares tree priorities and breaks ties by document ID.
func higherReactivePriority(leftPriority uint64, leftID DocumentID, rightPriority uint64, rightID DocumentID) bool {
	if leftPriority != rightPriority {
		return leftPriority > rightPriority
	}
	return compareDocumentIDs(leftID, rightID) < 0
}

// compareReactiveOrder orders members by query sort fields, insertion position, and document ID.
func compareReactiveOrder(query QuerySpec, leftID DocumentID, left reactiveMember, rightID DocumentID, right reactiveMember) int {
	for _, field := range query.sort {
		leftValue, leftOK := lookupInternal(left.document, field.Path)
		rightValue, rightOK := lookupInternal(right.document, field.Path)
		if leftOK != rightOK {
			comparison := -1 // ascending order places missing values first
			if leftOK {
				comparison = 1
			}
			if field.Direction == -1 {
				comparison = -comparison
			}
			return comparison
		}
		if !leftOK {
			continue
		}
		comparison := compareSortValues(leftValue, rightValue)
		if comparison != 0 {
			if field.Direction == -1 {
				comparison = -comparison
			}
			return comparison
		}
	}
	if left.position < right.position {
		return -1
	}
	if left.position > right.position {
		return 1
	}
	return compareDocumentIDs(leftID, rightID)
}

// reactiveOrderPut returns a result-order tree with the member inserted or replaced through copied paths.
func reactiveOrderPut(root *reactiveOrderNode, seed maphash.Seed, query QuerySpec, id DocumentID, member reactiveMember) *reactiveOrderNode {
	if root == nil {
		return &reactiveOrderNode{id: id, member: member, priority: reactiveNodePriority(seed, id), size: 1}
	}
	copy := *root
	comparison := compareReactiveOrder(query, id, member, root.id, root.member)
	if comparison < 0 {
		copy.left = reactiveOrderPut(root.left, seed, query, id, member)
		if higherReactivePriority(copy.left.priority, copy.left.id, copy.priority, copy.id) {
			return rotateReactiveOrderRight(&copy)
		}
	} else if comparison > 0 {
		copy.right = reactiveOrderPut(root.right, seed, query, id, member)
		if higherReactivePriority(copy.right.priority, copy.right.id, copy.priority, copy.id) {
			return rotateReactiveOrderLeft(&copy)
		}
	} else {
		copy.member = member
	}
	recomputeReactiveOrderSize(&copy)
	return &copy
}

// reactiveOrderDelete returns a result-order tree with the member removed through copied paths.
func reactiveOrderDelete(root *reactiveOrderNode, query QuerySpec, id DocumentID, member reactiveMember) *reactiveOrderNode {
	if root == nil {
		return nil
	}
	comparison := compareReactiveOrder(query, id, member, root.id, root.member)
	if comparison < 0 {
		copy := *root
		copy.left = reactiveOrderDelete(root.left, query, id, member)
		recomputeReactiveOrderSize(&copy)
		return &copy
	}
	if comparison > 0 {
		copy := *root
		copy.right = reactiveOrderDelete(root.right, query, id, member)
		recomputeReactiveOrderSize(&copy)
		return &copy
	}
	return mergeReactiveOrder(root.left, root.right)
}

// mergeReactiveOrder joins two result-order subtrees while preserving existing nodes.
func mergeReactiveOrder(left, right *reactiveOrderNode) *reactiveOrderNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if higherReactivePriority(left.priority, left.id, right.priority, right.id) {
		copy := *left
		copy.right = mergeReactiveOrder(left.right, right)
		recomputeReactiveOrderSize(&copy)
		return &copy
	}
	copy := *right
	copy.left = mergeReactiveOrder(left, right.left)
	recomputeReactiveOrderSize(&copy)
	return &copy
}

// rotateReactiveOrderRight rotates a private root copy right and refreshes affected subtree counts.
func rotateReactiveOrderRight(root *reactiveOrderNode) *reactiveOrderNode {
	left := *root.left
	root.left = left.right
	recomputeReactiveOrderSize(root)
	left.right = root
	recomputeReactiveOrderSize(&left)
	return &left
}

// rotateReactiveOrderLeft rotates a private root copy left and refreshes affected subtree counts.
func rotateReactiveOrderLeft(root *reactiveOrderNode) *reactiveOrderNode {
	right := *root.right
	root.right = right.left
	recomputeReactiveOrderSize(root)
	right.left = root
	recomputeReactiveOrderSize(&right)
	return &right
}

// recomputeReactiveOrderSize refreshes one result-order node's count from its children.
func recomputeReactiveOrderSize(node *reactiveOrderNode) {
	node.size = 1 + reactiveOrderSize(node.left) + reactiveOrderSize(node.right)
}

// reactiveOrderSize returns a result-order subtree's member count or zero for an empty tree.
func reactiveOrderSize(node *reactiveOrderNode) uint64 {
	if node == nil {
		return 0
	}
	return node.size
}

// buildReactiveTrees builds immutable ID and result-order trees from a complete member set.
//
// Sorting and stack-based construction avoid repeated path copies during
// initial loading and resynchronization.
func buildReactiveTrees(seed maphash.Seed, query QuerySpec, entries []reactiveTreeEntry) (*reactiveIDNode, *reactiveOrderNode) {
	if len(entries) == 0 {
		return nil, nil
	}
	idEntries := append([]reactiveTreeEntry(nil), entries...)
	sort.Slice(idEntries, func(left, right int) bool {
		return compareDocumentIDs(idEntries[left].id, idEntries[right].id) < 0
	})
	sort.Slice(entries, func(left, right int) bool {
		return compareReactiveOrder(query, entries[left].id, entries[left].member, entries[right].id, entries[right].member) < 0
	})
	return bulkReactiveIDTree(seed, idEntries), bulkReactiveOrderTree(seed, entries)
}

// bulkReactiveIDTree builds an ID tree from a nonempty slice already sorted by ID.
func bulkReactiveIDTree(seed maphash.Seed, entries []reactiveTreeEntry) *reactiveIDNode {
	nodes := make([]reactiveIDNode, len(entries))
	stack := make([]*reactiveIDNode, 0, len(entries))
	for index, entry := range entries {
		node := &nodes[index]
		*node = reactiveIDNode{id: entry.id, member: entry.member, priority: reactiveNodePriority(seed, entry.id)}
		var left *reactiveIDNode
		for len(stack) > 0 && higherReactivePriority(node.priority, node.id, stack[len(stack)-1].priority, stack[len(stack)-1].id) {
			left = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
		}
		node.left = left
		if len(stack) > 0 {
			stack[len(stack)-1].right = node
		}
		stack = append(stack, node)
	}
	return stack[0]
}

// bulkReactiveOrderTree builds a result-order tree from a nonempty slice already sorted by query order.
func bulkReactiveOrderTree(seed maphash.Seed, entries []reactiveTreeEntry) *reactiveOrderNode {
	nodes := make([]reactiveOrderNode, len(entries))
	stack := make([]*reactiveOrderNode, 0, len(entries))
	for index, entry := range entries {
		node := &nodes[index]
		*node = reactiveOrderNode{id: entry.id, member: entry.member, priority: reactiveNodePriority(seed, entry.id), size: 1}
		var left *reactiveOrderNode
		for len(stack) > 0 && higherReactivePriority(node.priority, node.id, stack[len(stack)-1].priority, stack[len(stack)-1].id) {
			left = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
		}
		node.left = left
		if len(stack) > 0 {
			stack[len(stack)-1].right = node
		}
		stack = append(stack, node)
	}
	recomputeReactiveOrderSubtree(stack[0])
	return stack[0]
}

// recomputeReactiveOrderSubtree refreshes member counts throughout a result-order subtree.
func recomputeReactiveOrderSubtree(node *reactiveOrderNode) uint64 {
	if node == nil {
		return 0
	}
	node.size = 1 + recomputeReactiveOrderSubtree(node.left) + recomputeReactiveOrderSubtree(node.right)
	return node.size
}

// materializeReactiveOrder collects the requested page from a result-order tree without copying documents.
func materializeReactiveOrder(root *reactiveOrderNode, skip int, limit *int) []Document {
	start := uint64(skip)
	total := reactiveOrderSize(root)
	if start > total {
		start = total
	}
	end := total
	if limit != nil && uint64(*limit) < end-start {
		end = start + uint64(*limit)
	}
	result := make([]Document, 0, int(end-start))
	collectReactiveOrder(root, 0, start, end, &result)
	return result
}

// collectReactiveOrder collects documents in a position range while skipping irrelevant subtrees.
func collectReactiveOrder(node *reactiveOrderNode, base, start, end uint64, result *[]Document) {
	if node == nil || start >= end {
		return
	}
	leftSize := reactiveOrderSize(node.left)
	position := base + leftSize
	if start < position {
		collectReactiveOrder(node.left, base, start, end, result)
	}
	if position >= start && position < end {
		// View states and database versions are immutable. Public snapshot/delta
		// boundaries perform the deep clone; cloning here would duplicate the
		// entire result once before fan-out even begins.
		*result = append(*result, node.member.document)
	}
	if position+1 < end {
		collectReactiveOrder(node.right, position+1, start, end, result)
	}
}
