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
	entries := t.entries()
	removed := false
	rebuilt := NewWithOrder(t.maxKeys)
	for _, entry := range entries {
		if !removed && bytes.Equal(entry.key, key) && bytes.Equal(entry.value, value) {
			removed = true
			continue
		}
		rebuilt.Insert(entry.key, entry.value)
	}
	if removed {
		t.root, t.size = rebuilt.root, rebuilt.size
	}
	return removed
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

type entry struct{ key, value []byte }

func (t *Tree) entries() []entry {
	pairs := t.Scan(nil, nil, false)
	result := make([]entry, len(pairs))
	for i, pair := range pairs {
		result[i] = entry{pair.Key, pair.Value}
	}
	return result
}

func recompute(current *node) {
	if current.leaf {
		return
	}
	current.keys = make([][]byte, len(current.children)-1)
	for i := 1; i < len(current.children); i++ {
		current.keys[i-1] = clone(minKey(current.children[i]))
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
