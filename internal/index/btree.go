package index

import (
	"bytes"
	"sort"
)

// defaultMaxKeys sets the default maximum number of keys in a tree node.
const defaultMaxKeys = 32

// Tree stores sorted byte keys with one or more distinct byte values per key.
type Tree struct {
	root    *node
	maxKeys int
	size    int
}

// node holds leaf entries or links to child nodes in the tree.
type node struct {
	leaf     bool
	keys     [][]byte
	values   [][][]byte
	children []*node
	next     *node
}

// split carries the right node and separator created when a node overflows.
type split struct {
	separator []byte
	right     *node
}

// New creates an empty tree with the default node size.
func New() *Tree { return &Tree{root: &node{leaf: true}, maxKeys: defaultMaxKeys} }

// NewWithOrder creates an empty tree with at least three keys allowed per node.
func NewWithOrder(maxKeys int) *Tree {
	if maxKeys < 3 {
		maxKeys = 3
	}
	return &Tree{root: &node{leaf: true}, maxKeys: maxKeys}
}

// Len returns the number of distinct key-value pairs.
func (t *Tree) Len() int { return t.size }

// Insert adds a copied key-value pair and reports whether it was new.
func (t *Tree) Insert(key, value []byte) bool {
	if len(key) == 0 {
		return false
	}
	added, result := t.insert(t.root, clone(key), clone(value))
	if !added {
		return false
	}
	t.size++
	if result != nil {
		t.root = &node{children: []*node{t.root, result.right}}
		recompute(t.root)
	}
	return true
}

// insert adds a pair to a subtree and returns any split needed by its parent.
func (t *Tree) insert(current *node, key, value []byte) (bool, *split) {
	if current.leaf {
		position := sort.Search(len(current.keys), func(i int) bool { return bytes.Compare(current.keys[i], key) >= 0 })
		if position < len(current.keys) && bytes.Equal(current.keys[position], key) {
			values := current.values[position]
			valuePosition := sort.Search(len(values), func(i int) bool { return bytes.Compare(values[i], value) >= 0 })
			if valuePosition < len(values) && bytes.Equal(values[valuePosition], value) {
				return false, nil
			}
			values = append(values, nil)
			copy(values[valuePosition+1:], values[valuePosition:])
			values[valuePosition] = value
			current.values[position] = values
			return true, nil
		}
		current.keys = insertBytes(current.keys, position, key)
		current.values = insertValues(current.values, position, [][]byte{value})
		if len(current.keys) <= t.maxKeys {
			return true, nil
		}
		middle := len(current.keys) / 2
		right := &node{leaf: true, keys: cloneKeys(current.keys[middle:]), values: cloneValueLists(current.values[middle:]), next: current.next}
		current.keys, current.values, current.next = current.keys[:middle], current.values[:middle], right
		return true, &split{separator: clone(right.keys[0]), right: right}
	}
	childIndex := sort.Search(len(current.keys), func(i int) bool { return bytes.Compare(key, current.keys[i]) < 0 })
	added, childSplit := t.insert(current.children[childIndex], key, value)
	if !added {
		return false, nil
	}
	if childSplit != nil {
		current.children = insertNode(current.children, childIndex+1, childSplit.right)
	}
	recompute(current)
	if len(current.keys) <= t.maxKeys {
		return true, nil
	}
	middle := len(current.children) / 2
	right := &node{children: append([]*node(nil), current.children[middle:]...)}
	current.children = current.children[:middle]
	recompute(current)
	recompute(right)
	return true, &split{separator: clone(minKey(right)), right: right}
}

// Get returns copies of the values stored under a key.
func (t *Tree) Get(key []byte) [][]byte {
	leaf := t.findLeaf(key)
	position := sort.Search(len(leaf.keys), func(i int) bool { return bytes.Compare(leaf.keys[i], key) >= 0 })
	if position >= len(leaf.keys) || !bytes.Equal(leaf.keys[position], key) {
		return nil
	}
	return cloneKeys(leaf.values[position])
}

// Delete removes a key-value pair and reports whether it existed.
func (t *Tree) Delete(key, value []byte) bool {
	if t == nil || t.root == nil || len(key) == 0 || !t.delete(t.root, key, value) {
		return false
	}
	t.size--
	for !t.root.leaf && len(t.root.children) == 1 {
		t.root = t.root.children[0]
	}
	if t.size == 0 {
		t.root = &node{leaf: true}
	}
	return true
}

// delete removes a pair from a subtree and rebalances its affected child.
func (t *Tree) delete(current *node, key, value []byte) bool {
	if current.leaf {
		position := sort.Search(len(current.keys), func(index int) bool { return bytes.Compare(current.keys[index], key) >= 0 })
		if position >= len(current.keys) || !bytes.Equal(current.keys[position], key) {
			return false
		}
		values := current.values[position]
		valuePosition := sort.Search(len(values), func(index int) bool { return bytes.Compare(values[index], value) >= 0 })
		if valuePosition >= len(values) || !bytes.Equal(values[valuePosition], value) {
			return false
		}
		values = removeBytes(values, valuePosition)
		if len(values) > 0 {
			current.values[position] = values
			return true
		}
		current.keys = removeBytes(current.keys, position)
		current.values = removeValueList(current.values, position)
		return true
	}
	childIndex := sort.Search(len(current.keys), func(index int) bool { return bytes.Compare(key, current.keys[index]) < 0 })
	if !t.delete(current.children[childIndex], key, value) {
		return false
	}
	t.rebalanceChild(current, childIndex)
	recompute(current)
	return true
}

// rebalanceChild restores node occupancy by borrowing from or merging with a sibling.
func (t *Tree) rebalanceChild(parent *node, childIndex int) {
	if parent == nil || parent.leaf || childIndex < 0 || childIndex >= len(parent.children) {
		return
	}
	child := parent.children[childIndex]
	if !t.underfilled(child) || len(parent.children) == 1 {
		return
	}
	if childIndex > 0 {
		left := parent.children[childIndex-1]
		if t.canLend(left) {
			t.borrowFromLeft(left, child)
			return
		}
	}
	if childIndex+1 < len(parent.children) {
		right := parent.children[childIndex+1]
		if t.canLend(right) {
			t.borrowFromRight(child, right)
			return
		}
	}
	if childIndex > 0 {
		t.mergeNodes(parent.children[childIndex-1], child)
		parent.children = removeNode(parent.children, childIndex)
		return
	}
	t.mergeNodes(child, parent.children[1])
	parent.children = removeNode(parent.children, 1)
}

// underfilled reports whether a node has fewer entries or children than required.
func (t *Tree) underfilled(current *node) bool {
	if current.leaf {
		return len(current.keys) < (t.maxKeys+1)/2
	}
	return len(current.children) < (t.maxKeys+2)/2
}

// canLend reports whether a node can give an entry or child to its sibling.
func (t *Tree) canLend(current *node) bool {
	if current.leaf {
		return len(current.keys) > (t.maxKeys+1)/2
	}
	return len(current.children) > (t.maxKeys+2)/2
}

// borrowFromLeft moves the left sibling's last entry or child to the current node.
func (t *Tree) borrowFromLeft(left, current *node) {
	if current.leaf {
		last := len(left.keys) - 1
		current.keys = insertBytes(current.keys, 0, left.keys[last])
		current.values = insertValues(current.values, 0, left.values[last])
		left.keys = removeBytes(left.keys, last)
		left.values = removeValueList(left.values, last)
		return
	}
	last := len(left.children) - 1
	current.children = insertNode(current.children, 0, left.children[last])
	left.children = removeNode(left.children, last)
	recompute(left)
	recompute(current)
}

// borrowFromRight moves the right sibling's first entry or child to the current node.
func (t *Tree) borrowFromRight(current, right *node) {
	if current.leaf {
		current.keys = append(current.keys, right.keys[0])
		current.values = append(current.values, right.values[0])
		right.keys = removeBytes(right.keys, 0)
		right.values = removeValueList(right.values, 0)
		return
	}
	current.children = append(current.children, right.children[0])
	right.children = removeNode(right.children, 0)
	recompute(current)
	recompute(right)
}

// mergeNodes appends the right node to the left node and preserves leaf links.
func (t *Tree) mergeNodes(left, right *node) {
	if left.leaf {
		left.keys = append(left.keys, right.keys...)
		left.values = append(left.values, right.values...)
		left.next = right.next
		return
	}
	left.children = append(left.children, right.children...)
	recompute(left)
}

// Pair holds one byte key and its byte value.
type Pair struct{ Key, Value []byte }

// Scan returns copied pairs in key order within the requested bounds.
func (t *Tree) Scan(start, end []byte, includeEnd bool) []Pair {
	leaf := t.findLeaf(start)
	result := []Pair{}
	for leaf != nil {
		for i, key := range leaf.keys {
			if len(start) > 0 && bytes.Compare(key, start) < 0 {
				continue
			}
			if len(end) > 0 {
				comparison := bytes.Compare(key, end)
				if comparison > 0 || (comparison == 0 && !includeEnd) {
					return result
				}
			}
			for _, value := range leaf.values[i] {
				result = append(result, Pair{Key: clone(key), Value: clone(value)})
			}
		}
		leaf = leaf.next
	}
	return result
}

// findLeaf follows separator keys to the leaf that can contain the requested key.
func (t *Tree) findLeaf(key []byte) *node {
	current := t.root
	for !current.leaf {
		index := sort.Search(len(current.keys), func(i int) bool { return bytes.Compare(key, current.keys[i]) < 0 })
		current = current.children[index]
	}
	return current
}

// recompute rebuilds an internal node's separator keys from its children.
func recompute(current *node) {
	if current.leaf {
		return
	}
	needed := len(current.children) - 1
	if needed < 0 {
		needed = 0
	}
	if cap(current.keys) < needed {
		current.keys = make([][]byte, needed)
	} else {
		for index := needed; index < len(current.keys); index++ {
			current.keys[index] = nil
		}
		current.keys = current.keys[:needed]
	}
	for i := 1; i < len(current.children); i++ {
		current.keys[i-1] = append(current.keys[i-1][:0], minKey(current.children[i])...)
	}
}

// minKey returns the first key in a nonempty subtree.
func minKey(current *node) []byte {
	for !current.leaf {
		current = current.children[0]
	}
	return current.keys[0]
}

// insertBytes inserts a byte slice at the requested position.
func insertBytes(values [][]byte, index int, value []byte) [][]byte {
	values = append(values, nil)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}

// insertValues inserts a value list at the requested position.
func insertValues(values [][][]byte, index int, value [][]byte) [][][]byte {
	values = append(values, nil)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}

// insertNode inserts a child node at the requested position.
func insertNode(values []*node, index int, value *node) []*node {
	values = append(values, nil)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}

// removeBytes removes a byte slice and clears the unused slot.
func removeBytes(values [][]byte, index int) [][]byte {
	copy(values[index:], values[index+1:])
	values[len(values)-1] = nil
	return values[:len(values)-1]
}

// removeValueList removes a value list and clears the unused slot.
func removeValueList(values [][][]byte, index int) [][][]byte {
	copy(values[index:], values[index+1:])
	values[len(values)-1] = nil
	return values[:len(values)-1]
}

// removeNode removes a child node and clears the unused slot.
func removeNode(values []*node, index int) []*node {
	copy(values[index:], values[index+1:])
	values[len(values)-1] = nil
	return values[:len(values)-1]
}

// clone copies a byte slice.
func clone(value []byte) []byte { return append([]byte(nil), value...) }

// cloneKeys copies a slice of byte slices.
func cloneKeys(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for i := range values {
		result[i] = clone(values[i])
	}
	return result
}

// cloneValueLists copies every byte slice in a set of value lists.
func cloneValueLists(values [][][]byte) [][][]byte {
	result := make([][][]byte, len(values))
	for i := range values {
		result[i] = cloneKeys(values[i])
	}
	return result
}
