package storage

import (
	"bytes"
	"errors"
	"sort"
)

const maxTreeIteratorDepth = 64

type treeIteratorFrame struct {
	view       treeNodeView
	childIndex int
}

// TreeIterator walks one immutable B+Tree root without materializing the
// result set. Key and Value point into validated immutable cached pages and
// remain valid until the iterator is closed or garbage collected. TreeIterator
// is not safe for concurrent use.
//
// A raw TreeIterator does not pin a DatabaseRoot. Callers that can run page
// reclamation must hold a snapshot pin for its lifetime. DocumentIterator does
// this automatically.
type TreeIterator struct {
	file      *File
	rootPage  uint64
	kind      TreeKind
	start     []byte
	end       []byte
	remaining int

	initialized bool
	done        bool
	closed      bool
	err         error
	branches    []treeIteratorFrame
	leaf        treeNodeView
	leafIndex   int
	key         []byte
	value       []byte
}

func newTreeIterator(file *File, rootPage uint64, kind TreeKind, start, end []byte, limit int) (*TreeIterator, error) {
	if file == nil || !validTreeKind(kind) || (len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) > 0) {
		return nil, ErrCorrupt
	}
	iterator := &TreeIterator{
		file: file, rootPage: rootPage, kind: kind,
		start: append([]byte(nil), start...), end: append([]byte(nil), end...), remaining: limit,
	}
	iterator.done = rootPage == 0 || (len(start) > 0 && len(end) > 0 && bytes.Equal(start, end))
	return iterator, nil
}

// Next advances to the next entry in bytewise key order within [start, end).
func (iterator *TreeIterator) Next() bool {
	if iterator == nil || iterator.closed || iterator.done || iterator.err != nil {
		return false
	}
	iterator.key, iterator.value = nil, nil
	iterator.file.mu.RLock()
	defer iterator.file.mu.RUnlock()
	if iterator.file.file == nil {
		iterator.err = errors.New("meldbase storage v2: file is closed")
		return false
	}
	return iterator.nextUnlocked()
}

func (iterator *TreeIterator) nextUnlocked() bool {
	if !iterator.initialized {
		iterator.initialized = true
		if err := iterator.descendUnlocked(iterator.rootPage, iterator.start, true); err != nil {
			iterator.err = err
			return false
		}
	}
	for {
		if iterator.leafIndex < iterator.leaf.count {
			key, value, _, _, err := iterator.leaf.entry(iterator.leafIndex)
			if err != nil {
				iterator.err = err
				return false
			}
			iterator.leafIndex++
			if len(iterator.end) > 0 && bytes.Compare(key, iterator.end) >= 0 {
				iterator.done = true
				return false
			}
			iterator.key, iterator.value = key, value
			if iterator.remaining > 0 {
				iterator.remaining--
				if iterator.remaining == 0 {
					iterator.done = true
				}
			}
			return true
		}
		if !iterator.advanceLeafUnlocked() {
			return false
		}
	}
}

func (iterator *TreeIterator) descendUnlocked(pageID uint64, seek []byte, useSeek bool) error {
	leafType, branchType := treePageTypes(iterator.kind)
	for {
		if len(iterator.branches) >= maxTreeIteratorDepth {
			return ErrCorrupt
		}
		cached, err := iterator.file.readDataPageUnlocked(pageID)
		if err != nil {
			return err
		}
		if cached.page.Type != leafType && cached.page.Type != branchType {
			return ErrCorrupt
		}
		view, err := decodeTreeNodeView(cached.page)
		if err != nil {
			return err
		}
		if view.leaf {
			iterator.leaf = view
			iterator.leafIndex = 0
			if useSeek && len(seek) > 0 {
				iterator.leafIndex = sort.Search(view.count, func(index int) bool {
					key, _, _, _, entryErr := view.entry(index)
					if entryErr != nil {
						iterator.err = entryErr
						return true
					}
					return bytes.Compare(key, seek) >= 0
				})
				if iterator.err != nil {
					return iterator.err
				}
			}
			return nil
		}
		childIndex := 0
		if useSeek && len(seek) > 0 {
			childIndex = sort.Search(view.count, func(index int) bool {
				key, _, _, _, entryErr := view.entry(index)
				if entryErr != nil {
					iterator.err = entryErr
					return true
				}
				return bytes.Compare(key, seek) > 0
			})
			if iterator.err != nil {
				return iterator.err
			}
		}
		childPage, _, err := view.child(childIndex)
		if err != nil {
			return err
		}
		iterator.branches = append(iterator.branches, treeIteratorFrame{view: view, childIndex: childIndex})
		pageID = childPage
	}
}

func (iterator *TreeIterator) advanceLeafUnlocked() bool {
	for len(iterator.branches) > 0 {
		last := len(iterator.branches) - 1
		frame := &iterator.branches[last]
		if frame.childIndex < frame.view.count {
			frame.childIndex++
			pageID, _, err := frame.view.child(frame.childIndex)
			if err != nil {
				iterator.err = err
				return false
			}
			if err := iterator.descendUnlocked(pageID, nil, false); err != nil {
				iterator.err = err
				return false
			}
			return true
		}
		iterator.branches = iterator.branches[:last]
	}
	iterator.done = true
	return false
}

func (iterator *TreeIterator) Key() []byte {
	if iterator == nil {
		return nil
	}
	return iterator.key
}

func (iterator *TreeIterator) Value() []byte {
	if iterator == nil {
		return nil
	}
	return iterator.value
}

func (iterator *TreeIterator) Err() error {
	if iterator == nil {
		return ErrCorrupt
	}
	return iterator.err
}

func (iterator *TreeIterator) Close() error {
	if iterator == nil || iterator.closed {
		return nil
	}
	iterator.closed, iterator.done = true, true
	iterator.file = nil
	iterator.branches = nil
	iterator.leaf = treeNodeView{}
	iterator.key, iterator.value = nil, nil
	return nil
}
