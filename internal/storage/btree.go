package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
)

var (
	ErrNodeFull  = errors.New("meldbase storage: tree node is full")
	ErrKeyTooBig = errors.New("meldbase storage: tree key is too large")
)

type TreeKind uint8

const (
	TreeCatalog TreeKind = 1 + iota
	TreePrimary
	TreeSecondary
	TreeCommitLog
	TreeIndexCatalog
	TreeOrder
	TreeSystem
	TreeFreeSpace
	TreeIndexBuildCatalog
)

type KeyValue struct {
	Key, Value []byte
}

type nodeRef struct {
	pageID uint64
	count  uint64
	node   *treeNode
}

type treeNode struct {
	leaf     bool
	dirty    bool
	keys     [][]byte
	values   [][]byte
	children []*nodeRef
	count    uint64
}

type MutableTree struct {
	tx    *WriteTxn
	kind  TreeKind
	root  *nodeRef
	cache map[uint64]*nodeRef
}

func (tx *WriteTxn) OpenTree(rootPage uint64, kind TreeKind) (*MutableTree, error) {
	if tx == nil || !validTreeKind(kind) {
		return nil, ErrCorrupt
	}
	tree := &MutableTree{tx: tx, kind: kind, cache: make(map[uint64]*nodeRef)}
	if rootPage == 0 {
		node := &treeNode{leaf: true, dirty: true}
		tree.root = &nodeRef{node: node}
		return tree, nil
	}
	ref, err := tree.reference(rootPage)
	if err != nil {
		return nil, err
	}
	tree.root = ref
	return tree, nil
}

func (tree *MutableTree) Put(key, value []byte) error {
	if tree == nil || tree.root == nil || len(key) == 0 || len(key) > 4096 {
		return ErrKeyTooBig
	}
	left, separator, right, err := tree.put(tree.root, key, value)
	if err != nil {
		return err
	}
	if right == nil {
		tree.root = left
		return nil
	}
	root := &treeNode{
		dirty: true, keys: [][]byte{separator}, children: []*nodeRef{left, right},
		count: left.count + right.count,
	}
	tree.root = &nodeRef{node: root, count: root.count}
	return nil
}

func (tree *MutableTree) Get(key []byte) ([]byte, bool, error) {
	value, found, err := tree.getBorrowed(key)
	return append([]byte(nil), value...), found, err
}

// getBorrowed returns storage owned by the loaded immutable/mutable node. The
// caller must not mutate or retain it beyond the WriteTxn callback.
func (tree *MutableTree) getBorrowed(key []byte) ([]byte, bool, error) {
	if tree == nil || tree.root == nil {
		return nil, false, ErrCorrupt
	}
	ref := tree.root
	for {
		node, err := tree.load(ref)
		if err != nil {
			return nil, false, err
		}
		position := sort.Search(len(node.keys), func(index int) bool { return bytes.Compare(node.keys[index], key) >= 0 })
		if node.leaf {
			if position < len(node.keys) && bytes.Equal(node.keys[position], key) {
				return node.values[position], true, nil
			}
			return nil, false, nil
		}
		if position < len(node.keys) && bytes.Equal(node.keys[position], key) {
			position++
		}
		ref = node.children[position]
	}
}

// Delete removes a key using copy-on-write path mutation. Empty children are
// eliminated and adjacent siblings are merged whenever the combined node fits;
// it never rebuilds the complete tree.
func (tree *MutableTree) Delete(key []byte) (bool, error) {
	if tree == nil || tree.root == nil || len(key) == 0 {
		return false, ErrCorrupt
	}
	removed, left, separator, right, err := tree.delete(tree.root, key)
	if err != nil || !removed {
		return removed, err
	}
	if right == nil {
		tree.root = left
	} else {
		root := &treeNode{
			dirty: true, keys: [][]byte{separator}, children: []*nodeRef{left, right},
			count: left.count + right.count,
		}
		tree.root = &nodeRef{node: root, count: root.count}
	}
	for {
		root, err := tree.load(tree.root)
		if err != nil {
			return false, err
		}
		if root.leaf || len(root.keys) > 0 {
			break
		}
		if len(root.children) == 0 && root.count == 0 {
			tree.root = &nodeRef{node: &treeNode{leaf: true, dirty: true}}
			break
		}
		if len(root.children) != 1 {
			return false, ErrCorrupt
		}
		tree.root = root.children[0]
	}
	return true, nil
}

// Scan returns keys in bytewise order in [start, end). A nil bound is open and
// a non-positive limit is unbounded.
func (tree *MutableTree) Scan(start, end []byte, limit int) ([]KeyValue, error) {
	if tree == nil || tree.root == nil || (len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) > 0) {
		return nil, ErrCorrupt
	}
	result := []KeyValue{}
	if err := tree.scan(tree.root, start, end, limit, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (tree *MutableTree) Flush() (uint64, error) {
	if tree == nil || tree.root == nil {
		return 0, ErrCorrupt
	}
	return tree.flush(tree.root)
}

func (tree *MutableTree) put(ref *nodeRef, key, value []byte) (*nodeRef, []byte, *nodeRef, error) {
	node, err := tree.load(ref)
	if err != nil {
		return nil, nil, nil, err
	}
	node.dirty = true
	position := sort.Search(len(node.keys), func(index int) bool { return bytes.Compare(node.keys[index], key) >= 0 })
	if node.leaf {
		if position < len(node.keys) && bytes.Equal(node.keys[position], key) {
			node.values[position] = append(node.values[position][:0], value...)
		} else {
			node.keys = insertBytes(node.keys, position, key)
			node.values = insertBytes(node.values, position, value)
			node.count++
		}
		ref.count = node.count
		if tree.nodeFits(node) {
			return ref, nil, nil, nil
		}
		middle, ok := byteBalancedLeafSplit(node)
		if !ok {
			return nil, nil, nil, ErrNodeFull
		}
		leftNode := &treeNode{leaf: true, dirty: true, keys: cloneByteMatrix(node.keys[:middle]), values: cloneByteMatrix(node.values[:middle]), count: uint64(middle)}
		rightNode := &treeNode{leaf: true, dirty: true, keys: cloneByteMatrix(node.keys[middle:]), values: cloneByteMatrix(node.values[middle:]), count: uint64(len(node.keys) - middle)}
		leftRef, rightRef := &nodeRef{node: leftNode, count: leftNode.count}, &nodeRef{node: rightNode, count: rightNode.count}
		tree.recordSplit()
		return leftRef, append([]byte(nil), rightNode.keys[0]...), rightRef, nil
	}

	childIndex := position
	if position < len(node.keys) && bytes.Equal(node.keys[position], key) {
		childIndex++
	}
	oldChildCount := node.children[childIndex].count
	left, separator, right, err := tree.put(node.children[childIndex], key, value)
	if err != nil {
		return nil, nil, nil, err
	}
	node.children[childIndex] = left
	node.count += left.count - oldChildCount
	if right != nil {
		node.keys = insertBytes(node.keys, childIndex, separator)
		node.children = insertRef(node.children, childIndex+1, right)
		node.count += right.count
	}
	ref.count = node.count
	if tree.nodeFits(node) {
		return ref, nil, nil, nil
	}
	middle, ok := byteBalancedBranchSplit(node)
	if !ok {
		return nil, nil, nil, ErrNodeFull
	}
	promoted := append([]byte(nil), node.keys[middle]...)
	leftNode := &treeNode{
		dirty: true, keys: cloneByteMatrix(node.keys[:middle]),
		children: append([]*nodeRef(nil), node.children[:middle+1]...),
	}
	rightNode := &treeNode{
		dirty: true, keys: cloneByteMatrix(node.keys[middle+1:]),
		children: append([]*nodeRef(nil), node.children[middle+1:]...),
	}
	leftNode.count, rightNode.count = childCount(leftNode.children), childCount(rightNode.children)
	leftRef, rightRef := &nodeRef{node: leftNode, count: leftNode.count}, &nodeRef{node: rightNode, count: rightNode.count}
	tree.recordSplit()
	return leftRef, promoted, rightRef, nil
}

func (tree *MutableTree) scan(ref *nodeRef, start, end []byte, limit int, result *[]KeyValue) error {
	if limit > 0 && len(*result) >= limit {
		return nil
	}
	node, err := tree.load(ref)
	if err != nil {
		return err
	}
	if node.leaf {
		position := 0
		if len(start) > 0 {
			position = sort.Search(len(node.keys), func(index int) bool { return bytes.Compare(node.keys[index], start) >= 0 })
		}
		for ; position < len(node.keys); position++ {
			if len(end) > 0 && bytes.Compare(node.keys[position], end) >= 0 {
				break
			}
			*result = append(*result, KeyValue{Key: append([]byte(nil), node.keys[position]...), Value: append([]byte(nil), node.values[position]...)})
			if limit > 0 && len(*result) >= limit {
				break
			}
		}
		return nil
	}
	childIndex := 0
	if len(start) > 0 {
		childIndex = sort.Search(len(node.keys), func(index int) bool { return bytes.Compare(node.keys[index], start) > 0 })
	}
	for ; childIndex < len(node.children); childIndex++ {
		if childIndex > 0 && len(end) > 0 && bytes.Compare(node.keys[childIndex-1], end) >= 0 {
			break
		}
		if err := tree.scan(node.children[childIndex], start, end, limit, result); err != nil {
			return err
		}
		if limit > 0 && len(*result) >= limit {
			break
		}
	}
	return nil
}

func (tree *MutableTree) delete(ref *nodeRef, key []byte) (bool, *nodeRef, []byte, *nodeRef, error) {
	node, err := tree.load(ref)
	if err != nil {
		return false, nil, nil, nil, err
	}
	position := sort.Search(len(node.keys), func(index int) bool { return bytes.Compare(node.keys[index], key) >= 0 })
	if node.leaf {
		if position >= len(node.keys) || !bytes.Equal(node.keys[position], key) {
			return false, ref, nil, nil, nil
		}
		node.keys = removeBytes(node.keys, position)
		node.values = removeBytes(node.values, position)
		node.count--
		node.dirty, ref.count = true, node.count
		return true, ref, nil, nil, nil
	}
	childIndex := position
	if position < len(node.keys) && bytes.Equal(node.keys[position], key) {
		childIndex++
	}
	removed, left, separator, right, err := tree.delete(node.children[childIndex], key)
	if err != nil || !removed {
		return removed, ref, nil, nil, err
	}
	node.children[childIndex] = left
	if right != nil {
		node.keys = insertBytes(node.keys, childIndex, separator)
		node.children = insertRef(node.children, childIndex+1, right)
	}
	node.dirty = true
	node.count--
	ref.count = node.count
	child, err := tree.load(node.children[childIndex])
	if err != nil {
		return false, nil, nil, nil, err
	}
	if child.count == 0 {
		node.children = removeRef(node.children, childIndex)
		if childIndex == 0 {
			if len(node.keys) > 0 {
				node.keys = removeBytes(node.keys, 0)
			}
		} else {
			node.keys = removeBytes(node.keys, childIndex-1)
			childIndex--
		}
		if len(node.children) == 0 {
			node.leaf, node.keys, node.values, node.children = true, nil, nil, nil
		}
	} else if childIndex > 0 {
		minimum, err := tree.minimumKey(node.children[childIndex])
		if err != nil {
			return false, nil, nil, nil, err
		}
		node.keys[childIndex-1] = minimum
	}
	if !node.leaf && len(node.children) > 1 {
		mergeIndex := childIndex
		if mergeIndex >= len(node.children)-1 {
			mergeIndex--
		}
		if mergeIndex >= 0 {
			merged, err := tree.mergeChildren(node, mergeIndex)
			if err != nil {
				return false, nil, nil, nil, err
			}
			if merged && mergeIndex > 0 {
				minimum, err := tree.minimumKey(node.children[mergeIndex])
				if err != nil {
					return false, nil, nil, nil, err
				}
				node.keys[mergeIndex-1] = minimum
			}
		}
	}
	if tree.nodeFits(node) {
		return true, ref, nil, nil, nil
	}
	middle, ok := byteBalancedBranchSplit(node)
	if !ok {
		return false, nil, nil, nil, ErrNodeFull
	}
	promoted := append([]byte(nil), node.keys[middle]...)
	leftNode := &treeNode{
		dirty: true, keys: cloneByteMatrix(node.keys[:middle]),
		children: append([]*nodeRef(nil), node.children[:middle+1]...),
	}
	rightNode := &treeNode{
		dirty: true, keys: cloneByteMatrix(node.keys[middle+1:]),
		children: append([]*nodeRef(nil), node.children[middle+1:]...),
	}
	leftNode.count, rightNode.count = childCount(leftNode.children), childCount(rightNode.children)
	leftRef, rightRef := &nodeRef{node: leftNode, count: leftNode.count}, &nodeRef{node: rightNode, count: rightNode.count}
	tree.recordSplit()
	return true, leftRef, promoted, rightRef, nil
}

func (tree *MutableTree) mergeChildren(parent *treeNode, leftIndex int) (bool, error) {
	if parent == nil || parent.leaf || leftIndex < 0 || leftIndex+1 >= len(parent.children) || leftIndex >= len(parent.keys) {
		return false, ErrCorrupt
	}
	left, err := tree.load(parent.children[leftIndex])
	if err != nil {
		return false, err
	}
	right, err := tree.load(parent.children[leftIndex+1])
	if err != nil {
		return false, err
	}
	if left.leaf != right.leaf {
		return false, ErrCorrupt
	}
	merged := &treeNode{leaf: left.leaf, dirty: true, count: left.count + right.count}
	if left.leaf {
		merged.keys = append(cloneByteMatrix(left.keys), cloneByteMatrix(right.keys)...)
		merged.values = append(cloneByteMatrix(left.values), cloneByteMatrix(right.values)...)
	} else {
		merged.keys = append(cloneByteMatrix(left.keys), append([]byte(nil), parent.keys[leftIndex]...))
		merged.keys = append(merged.keys, cloneByteMatrix(right.keys)...)
		merged.children = append(append([]*nodeRef(nil), left.children...), right.children...)
	}
	if !tree.nodeFits(merged) {
		return false, nil
	}
	mergedRef := &nodeRef{node: merged, count: merged.count}
	parent.children[leftIndex] = mergedRef
	parent.children = removeRef(parent.children, leftIndex+1)
	parent.keys = removeBytes(parent.keys, leftIndex)
	tree.recordMerge()
	return true, nil
}

func (tree *MutableTree) recordSplit() {
	if tree != nil && tree.tx != nil {
		tree.tx.treeSplits++
	}
}

func (tree *MutableTree) recordMerge() {
	if tree != nil && tree.tx != nil {
		tree.tx.treeMerges++
	}
}

func (tree *MutableTree) minimumKey(ref *nodeRef) ([]byte, error) {
	for {
		node, err := tree.load(ref)
		if err != nil {
			return nil, err
		}
		if node.leaf {
			if len(node.keys) == 0 {
				return nil, ErrCorrupt
			}
			return append([]byte(nil), node.keys[0]...), nil
		}
		if len(node.children) == 0 {
			return nil, ErrCorrupt
		}
		ref = node.children[0]
	}
}

func (tree *MutableTree) nodeFits(node *treeNode) bool {
	_, err := treeNodeEncodedSize(node)
	return err == nil
}

// byteBalancedLeafSplit selects a boundary by encoded bytes rather than entry
// count. Variable-sized values can otherwise leave one count-balanced half too
// large even though isolating a large entry would produce two valid pages.
func byteBalancedLeafSplit(node *treeNode) (int, bool) {
	if node == nil || !node.leaf || len(node.keys) < 2 || len(node.values) != len(node.keys) {
		return 0, false
	}
	prefix, ok := treeNodeEntryPrefix(node)
	if !ok {
		return 0, false
	}
	capacity := PageSize - PageHeaderSize
	best, bestLargest, bestSkew := 0, int(^uint(0)>>1), int(^uint(0)>>1)
	for boundary := 1; boundary < len(node.keys); boundary++ {
		leftSize, rightSize := 32+prefix[boundary], 32+prefix[len(node.keys)]-prefix[boundary]
		if leftSize > capacity || rightSize > capacity {
			continue
		}
		largest, skew := leftSize, leftSize-rightSize
		if rightSize > largest {
			largest = rightSize
		}
		if skew < 0 {
			skew = -skew
		}
		if largest < bestLargest || (largest == bestLargest && skew < bestSkew) {
			best, bestLargest, bestSkew = boundary, largest, skew
		}
	}
	return best, best != 0
}

// byteBalancedBranchSplit returns the separator to promote. The promoted key
// is not stored in either child, so every candidate must be measured in its
// actual encoded form.
func byteBalancedBranchSplit(node *treeNode) (int, bool) {
	if node == nil || node.leaf || len(node.keys) == 0 || len(node.children) != len(node.keys)+1 {
		return 0, false
	}
	prefix, ok := treeNodeEntryPrefix(node)
	if !ok {
		return 0, false
	}
	capacity := PageSize - PageHeaderSize
	best, found := 0, false
	bestLargest, bestSkew := int(^uint(0)>>1), int(^uint(0)>>1)
	for promoted := range node.keys {
		leftSize := 32 + prefix[promoted]
		rightSize := 32 + prefix[len(node.keys)] - prefix[promoted+1]
		if leftSize > capacity || rightSize > capacity {
			continue
		}
		largest, skew := leftSize, leftSize-rightSize
		if rightSize > largest {
			largest = rightSize
		}
		if skew < 0 {
			skew = -skew
		}
		if !found || largest < bestLargest || (largest == bestLargest && skew < bestSkew) {
			best, found, bestLargest, bestSkew = promoted, true, largest, skew
		}
	}
	return best, found
}

// treeNodeEntryPrefix returns cumulative entry-plus-slot bytes. It keeps split
// selection linear in the number of entries and shares the exact entry sizing
// rules used by the encoder.
func treeNodeEntryPrefix(node *treeNode) ([]int, bool) {
	prefix := make([]int, len(node.keys)+1)
	for index := range node.keys {
		entryLength, err := treeNodeEntryEncodedSize(node, index)
		if err != nil {
			return nil, false
		}
		prefix[index+1] = prefix[index] + 8 + entryLength
	}
	return prefix, true
}

func (tree *MutableTree) reference(pageID uint64) (*nodeRef, error) {
	if existing := tree.cache[pageID]; existing != nil {
		return existing, nil
	}
	raw, err := tree.tx.readPage(pageID)
	if err != nil {
		return nil, err
	}
	page, err := DecodePage(raw, pageID)
	if err != nil || !tree.accepts(page.Type) {
		return nil, ErrCorrupt
	}
	node, err := decodeTreeNode(page)
	if err != nil {
		return nil, err
	}
	ref := &nodeRef{pageID: pageID, count: node.count, node: node}
	tree.cache[pageID] = ref
	return ref, nil
}

func (tree *MutableTree) load(ref *nodeRef) (*treeNode, error) {
	if ref == nil {
		return nil, ErrCorrupt
	}
	if ref.node == nil {
		loaded, err := tree.reference(ref.pageID)
		if err != nil {
			return nil, err
		}
		if ref.count != loaded.count {
			return nil, ErrCorrupt
		}
		ref.node, ref.count = loaded.node, loaded.count
	}
	return ref.node, nil
}

func (tree *MutableTree) flush(ref *nodeRef) (uint64, error) {
	if ref == nil {
		return 0, ErrCorrupt
	}
	if ref.pageID != 0 && (ref.node == nil || !ref.node.dirty) {
		return ref.pageID, nil
	}
	node := ref.node
	if node == nil {
		return 0, ErrCorrupt
	}
	if !node.leaf {
		for _, child := range node.children {
			pageID, err := tree.flush(child)
			if err != nil {
				return 0, err
			}
			child.pageID = pageID
		}
	}
	payload, err := encodeTreeNode(node)
	if err != nil {
		return 0, err
	}
	pageType := tree.branchType()
	if node.leaf {
		pageType = tree.leafType()
	}
	pageID, err := tree.tx.appendPage(pageType, 0, uint32(len(node.keys)), 0, payload)
	if err != nil {
		return 0, err
	}
	ref.pageID, ref.count, node.dirty = pageID, node.count, false
	return pageID, nil
}

func encodeTreeNode(node *treeNode) ([]byte, error) {
	size, err := treeNodeEncodedSize(node)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, size)
	const nodeHeader = 32
	const slotSize = 8
	offset := nodeHeader
	for index, key := range node.keys {
		entryLength := 2 + len(key) + 16
		if node.leaf {
			entryLength = 2 + 4 + len(key) + len(node.values[index])
		}
		entry := payload[offset : offset+entryLength]
		binary.LittleEndian.PutUint16(entry[:2], uint16(len(key)))
		cursor := 2
		if node.leaf {
			binary.LittleEndian.PutUint32(entry[2:6], uint32(len(node.values[index])))
			cursor = 6
		}
		copy(entry[cursor:], key)
		cursor += len(key)
		if node.leaf {
			copy(entry[cursor:], node.values[index])
		} else {
			binary.LittleEndian.PutUint64(entry[cursor:cursor+8], node.children[index+1].pageID)
			binary.LittleEndian.PutUint64(entry[cursor+8:cursor+16], node.children[index+1].count)
		}
		slot := payload[len(payload)-(index+1)*slotSize : len(payload)-index*slotSize]
		binary.LittleEndian.PutUint16(slot[0:2], uint16(offset))
		binary.LittleEndian.PutUint16(slot[2:4], uint16(entryLength))
		offset += entryLength
	}
	binary.LittleEndian.PutUint16(payload[0:2], uint16(len(node.keys)))
	binary.LittleEndian.PutUint16(payload[2:4], uint16(offset))
	binary.LittleEndian.PutUint16(payload[4:6], uint16(offset))
	if !node.leaf && len(node.children) > 0 {
		binary.LittleEndian.PutUint64(payload[8:16], node.children[0].pageID)
		binary.LittleEndian.PutUint64(payload[16:24], node.children[0].count)
	}
	binary.LittleEndian.PutUint64(payload[24:32], node.count)
	return payload, nil
}

func treeNodeEncodedSize(node *treeNode) (int, error) {
	if node == nil || len(node.keys) > 65535 || (!node.leaf && len(node.children) != len(node.keys)+1) || (node.leaf && len(node.values) != len(node.keys)) {
		return 0, ErrCorrupt
	}
	size := 32 + len(node.keys)*8
	for index := range node.keys {
		entryLength, err := treeNodeEntryEncodedSize(node, index)
		if err != nil {
			return 0, err
		}
		if entryLength > 65535 || size > PageSize-PageHeaderSize-entryLength {
			return 0, ErrNodeFull
		}
		size += entryLength
	}
	return size, nil
}

func treeNodeEntryEncodedSize(node *treeNode, index int) (int, error) {
	if node == nil || index < 0 || index >= len(node.keys) {
		return 0, ErrCorrupt
	}
	key := node.keys[index]
	if len(key) == 0 || len(key) > 4096 {
		return 0, ErrKeyTooBig
	}
	if !node.leaf {
		return 2 + len(key) + 16, nil
	}
	if index >= len(node.values) || uint64(len(node.values[index])) > uint64(^uint32(0)) {
		return 0, ErrNodeFull
	}
	return 2 + 4 + len(key) + len(node.values[index]), nil
}

func decodeTreeNode(page Page) (*treeNode, error) {
	payload := page.Payload
	if len(payload) < 32 || page.ItemCount > 65535 {
		return nil, ErrCorrupt
	}
	count := int(binary.LittleEndian.Uint16(payload[0:2]))
	freeStart := int(binary.LittleEndian.Uint16(payload[2:4]))
	freeEnd := int(binary.LittleEndian.Uint16(payload[4:6]))
	if count != int(page.ItemCount) || binary.LittleEndian.Uint16(payload[6:8]) != 0 || freeStart < 32 || freeEnd != freeStart || len(payload) != freeStart+count*8 {
		return nil, ErrCorrupt
	}
	leaf := page.Type == PageCatalogLeaf || page.Type == PagePrimaryLeaf || page.Type == PageSecondaryLeaf || page.Type == PageCommitLogLeaf || page.Type == PageIndexCatalogLeaf || page.Type == PageOrderLeaf || page.Type == PageSystemLeaf || page.Type == PageFreeSpaceLeaf || page.Type == PageIndexBuildCatalogLeaf
	node := &treeNode{leaf: leaf, keys: make([][]byte, count), count: binary.LittleEndian.Uint64(payload[24:32])}
	if leaf {
		if binary.LittleEndian.Uint64(payload[8:16]) != 0 || binary.LittleEndian.Uint64(payload[16:24]) != 0 {
			return nil, ErrCorrupt
		}
		node.values = make([][]byte, count)
	} else {
		left := binary.LittleEndian.Uint64(payload[8:16])
		leftCount := binary.LittleEndian.Uint64(payload[16:24])
		if left < 2 {
			return nil, ErrCorrupt
		}
		node.children = append(node.children, &nodeRef{pageID: left, count: leftCount})
	}
	cursor := 32
	for index := range count {
		slot := payload[len(payload)-(index+1)*8 : len(payload)-index*8]
		offset := int(binary.LittleEndian.Uint16(slot[0:2]))
		length := int(binary.LittleEndian.Uint16(slot[2:4]))
		if binary.LittleEndian.Uint32(slot[4:8]) != 0 || offset != cursor || length < 2 || offset+length > freeStart {
			return nil, ErrCorrupt
		}
		entry := payload[offset : offset+length]
		keyLength := int(binary.LittleEndian.Uint16(entry[:2]))
		entryCursor := 2
		if leaf {
			if len(entry) < 6 {
				return nil, ErrCorrupt
			}
			valueLength := int(binary.LittleEndian.Uint32(entry[2:6]))
			entryCursor = 6
			if keyLength == 0 || entryCursor+keyLength+valueLength != len(entry) {
				return nil, ErrCorrupt
			}
			node.keys[index] = append([]byte(nil), entry[entryCursor:entryCursor+keyLength]...)
			node.values[index] = append([]byte(nil), entry[entryCursor+keyLength:]...)
		} else {
			if keyLength == 0 || entryCursor+keyLength+16 != len(entry) {
				return nil, ErrCorrupt
			}
			node.keys[index] = append([]byte(nil), entry[entryCursor:entryCursor+keyLength]...)
			child := binary.LittleEndian.Uint64(entry[entryCursor+keyLength : entryCursor+keyLength+8])
			childItems := binary.LittleEndian.Uint64(entry[entryCursor+keyLength+8:])
			if child < 2 {
				return nil, ErrCorrupt
			}
			node.children = append(node.children, &nodeRef{pageID: child, count: childItems})
		}
		if index > 0 && bytes.Compare(node.keys[index-1], node.keys[index]) >= 0 {
			return nil, ErrCorrupt
		}
		cursor += length
	}
	if cursor != freeStart || (leaf && node.count != uint64(len(node.keys))) || (!leaf && node.count != childCount(node.children)) {
		return nil, ErrCorrupt
	}
	return node, nil
}

func (tree *MutableTree) accepts(pageType PageType) bool {
	return pageType == tree.leafType() || pageType == tree.branchType()
}
func (tree *MutableTree) leafType() PageType {
	leaf, _ := treePageTypes(tree.kind)
	return leaf
}
func (tree *MutableTree) branchType() PageType {
	_, branch := treePageTypes(tree.kind)
	return branch
}

func validTreeKind(kind TreeKind) bool { return kind >= TreeCatalog && kind <= TreeIndexBuildCatalog }

func insertBytes(values [][]byte, position int, value []byte) [][]byte {
	values = append(values, nil)
	copy(values[position+1:], values[position:])
	values[position] = append([]byte(nil), value...)
	return values
}

func insertRef(values []*nodeRef, position int, value *nodeRef) []*nodeRef {
	values = append(values, nil)
	copy(values[position+1:], values[position:])
	values[position] = value
	return values
}

func removeBytes(values [][]byte, position int) [][]byte {
	copy(values[position:], values[position+1:])
	values[len(values)-1] = nil
	return values[:len(values)-1]
}

func removeRef(values []*nodeRef, position int) []*nodeRef {
	copy(values[position:], values[position+1:])
	values[len(values)-1] = nil
	return values[:len(values)-1]
}

func cloneByteMatrix(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for index := range values {
		result[index] = append([]byte(nil), values[index]...)
	}
	return result
}

func childCount(children []*nodeRef) uint64 {
	var result uint64
	for _, child := range children {
		result += child.count
	}
	return result
}
