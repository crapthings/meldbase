package storage

import (
	"bytes"
	"math"
)

// SortedTreeBuilder consumes strictly increasing keys and builds immutable
// leaves eagerly. Finish constructs only the much smaller branch frontier.
// This is the storage boundary used by index construction today and by a
// future external-merge source without changing catalog publication.
//
// A builder belongs to one WriteTxn, is not concurrency-safe, and must not be
// retained after the transaction callback returns.
type SortedTreeBuilder struct {
	tx       *WriteTxn
	kind     TreeKind
	keys     [][]byte
	values   [][]byte
	leaves   []bulkTreeRef
	lastKey  []byte
	count    uint64
	leafSize int
	finished bool
}

type bulkTreeRef struct {
	pageID uint64
	count  uint64
	minKey []byte
}

// NewSortedTreeBuilder starts an empty immutable tree build.
func (tx *WriteTxn) NewSortedTreeBuilder(kind TreeKind) (*SortedTreeBuilder, error) {
	if tx == nil || !validTreeKind(kind) {
		return nil, ErrCorrupt
	}
	return &SortedTreeBuilder{tx: tx, kind: kind, leafSize: 32}, nil
}

// Add consumes one key/value pair. Keys must be globally strictly increasing.
func (builder *SortedTreeBuilder) Add(key, value []byte) error {
	if builder == nil || builder.tx == nil || builder.finished || len(key) == 0 || len(key) > 4096 {
		return ErrCorrupt
	}
	if len(builder.lastKey) > 0 && bytes.Compare(builder.lastKey, key) >= 0 {
		return ErrCorrupt
	}
	if uint64(len(value)) > math.MaxUint32 {
		return ErrNodeFull
	}
	entrySize := 8 + 2 + 4 + len(key) + len(value)
	if 32+entrySize > PageSize-PageHeaderSize {
		return ErrNodeFull
	}
	if builder.leafSize+entrySize > PageSize-PageHeaderSize {
		if err := builder.flushLeaf(); err != nil {
			return err
		}
	}
	builder.keys = append(builder.keys, append([]byte(nil), key...))
	builder.values = append(builder.values, append([]byte(nil), value...))
	builder.leafSize += entrySize
	builder.lastKey = append(builder.lastKey[:0], key...)
	builder.count++
	return nil
}

// Finish returns the immutable root page. Calling it more than once fails.
func (builder *SortedTreeBuilder) Finish() (uint64, error) {
	if builder == nil || builder.tx == nil || builder.finished {
		return 0, ErrCorrupt
	}
	builder.finished = true
	if len(builder.keys) > 0 || len(builder.leaves) == 0 {
		if err := builder.flushLeaf(); err != nil {
			return 0, err
		}
	}
	level := builder.leaves
	for len(level) > 1 {
		groups, err := partitionBulkLevel(level)
		if err != nil {
			return 0, err
		}
		next := make([]bulkTreeRef, 0, len(groups))
		for _, group := range groups {
			node := bulkBranchNode(group)
			payload, err := encodeTreeNode(node)
			if err != nil {
				return 0, err
			}
			pageID, err := builder.tx.appendPage(treePageType(builder.kind, false), 0, uint32(len(node.keys)), 0, payload)
			if err != nil {
				return 0, err
			}
			next = append(next, bulkTreeRef{pageID: pageID, count: node.count, minKey: append([]byte(nil), group[0].minKey...)})
		}
		level = next
	}
	if len(level) != 1 || level[0].count != builder.count {
		return 0, ErrCorrupt
	}
	return level[0].pageID, nil
}

func (builder *SortedTreeBuilder) flushLeaf() error {
	node := &treeNode{leaf: true, keys: builder.keys, values: builder.values, count: uint64(len(builder.keys))}
	payload, err := encodeTreeNode(node)
	if err != nil {
		return err
	}
	pageID, err := builder.tx.appendPage(treePageType(builder.kind, true), 0, uint32(len(node.keys)), 0, payload)
	if err != nil {
		return err
	}
	minimum := []byte(nil)
	if len(node.keys) > 0 {
		minimum = append(minimum, node.keys[0]...)
	}
	builder.leaves = append(builder.leaves, bulkTreeRef{pageID: pageID, count: node.count, minKey: minimum})
	builder.keys, builder.values, builder.leafSize = nil, nil, 32
	return nil
}

func partitionBulkLevel(level []bulkTreeRef) ([][]bulkTreeRef, error) {
	if len(level) < 2 {
		return nil, ErrCorrupt
	}
	groups := make([][]bulkTreeRef, 0)
	start := 0
	for start < len(level) {
		end, encodedSize := start+1, 32
		for end < len(level) {
			entrySize := 8 + 2 + len(level[end].minKey) + 16
			if encodedSize+entrySize > PageSize-PageHeaderSize {
				break
			}
			encodedSize += entrySize
			end++
		}
		groups = append(groups, level[start:end])
		start = end
	}
	if len(groups) > 1 && len(groups[len(groups)-1]) == 1 {
		previous := groups[len(groups)-2]
		if len(previous) < 3 {
			return nil, ErrNodeFull
		}
		last := groups[len(groups)-1]
		groups[len(groups)-2] = previous[:len(previous)-1]
		groups[len(groups)-1] = append(previous[len(previous)-1:], last...)
	}
	for _, group := range groups {
		if len(group) < 2 {
			return nil, ErrCorrupt
		}
		if _, err := treeNodeEncodedSize(bulkBranchNode(group)); err != nil {
			return nil, err
		}
	}
	return groups, nil
}

func bulkBranchNode(children []bulkTreeRef) *treeNode {
	node := &treeNode{children: make([]*nodeRef, len(children))}
	for index, child := range children {
		node.children[index] = &nodeRef{pageID: child.pageID, count: child.count}
		node.count += child.count
		if index > 0 {
			node.keys = append(node.keys, child.minKey)
		}
	}
	return node
}

func treePageType(kind TreeKind, leaf bool) PageType {
	leafType, branchType := treePageTypes(kind)
	if leaf {
		return leafType
	}
	return branchType
}
