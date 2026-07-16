package index

import (
	"bytes"
	"sort"
)

const defaultMaxKeys = 32

type Tree struct {
	root    *node
	maxKeys int
	size    int
}
type node struct {
	leaf     bool
	keys     [][]byte
	values   [][][]byte
	children []*node
	next     *node
}
type split struct {
	separator []byte
	right     *node
}

func New() *Tree { return &Tree{root: &node{leaf: true}, maxKeys: defaultMaxKeys} }
func NewWithOrder(maxKeys int) *Tree {
	if maxKeys < 3 {
		maxKeys = 3
	}
	return &Tree{root: &node{leaf: true}, maxKeys: maxKeys}
}
func (t *Tree) Len() int { return t.size }

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

func (t *Tree) Get(key []byte) [][]byte {
	leaf := t.findLeaf(key)
	position := sort.Search(len(leaf.keys), func(i int) bool { return bytes.Compare(leaf.keys[i], key) >= 0 })
	if position >= len(leaf.keys) || !bytes.Equal(leaf.keys[position], key) {
		return nil
	}
	return cloneKeys(leaf.values[position])
}

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

func (t *Tree) underfilled(current *node) bool {
	if current.leaf {
		return len(current.keys) < (t.maxKeys+1)/2
	}
	return len(current.children) < (t.maxKeys+2)/2
}

func (t *Tree) canLend(current *node) bool {
	if current.leaf {
		return len(current.keys) > (t.maxKeys+1)/2
	}
	return len(current.children) > (t.maxKeys+2)/2
}

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

type Pair struct{ Key, Value []byte }

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

func (t *Tree) findLeaf(key []byte) *node {
	current := t.root
	for !current.leaf {
		index := sort.Search(len(current.keys), func(i int) bool { return bytes.Compare(key, current.keys[i]) < 0 })
		current = current.children[index]
	}
	return current
}

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
func minKey(current *node) []byte {
	for !current.leaf {
		current = current.children[0]
	}
	return current.keys[0]
}
func insertBytes(values [][]byte, index int, value []byte) [][]byte {
	values = append(values, nil)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}
func insertValues(values [][][]byte, index int, value [][]byte) [][][]byte {
	values = append(values, nil)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}
func insertNode(values []*node, index int, value *node) []*node {
	values = append(values, nil)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}
func removeBytes(values [][]byte, index int) [][]byte {
	copy(values[index:], values[index+1:])
	values[len(values)-1] = nil
	return values[:len(values)-1]
}
func removeValueList(values [][][]byte, index int) [][][]byte {
	copy(values[index:], values[index+1:])
	values[len(values)-1] = nil
	return values[:len(values)-1]
}
func removeNode(values []*node, index int) []*node {
	copy(values[index:], values[index+1:])
	values[len(values)-1] = nil
	return values[:len(values)-1]
}
func clone(value []byte) []byte { return append([]byte(nil), value...) }
func cloneKeys(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for i := range values {
		result[i] = clone(values[i])
	}
	return result
}
func cloneValueLists(values [][][]byte) [][][]byte {
	result := make([][][]byte, len(values))
	for i := range values {
		result[i] = cloneKeys(values[i])
	}
	return result
}
