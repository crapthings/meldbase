package storage

import (
	"bytes"
	"encoding/binary"
	"sort"
)

type treeNodeView struct {
	payload   []byte
	leaf      bool
	count     int
	freeStart int
}

func (f *File) validateTreeRootUnlocked(rootPage uint64, kind TreeKind) error {
	if rootPage == 0 {
		return nil
	}
	if !validTreeKind(kind) || rootPage < 2 {
		return ErrCorrupt
	}
	cached, err := f.readDataPageUnlocked(rootPage)
	if err != nil {
		return err
	}
	leafType, branchType := treePageTypes(kind)
	if cached.page.Type != leafType && cached.page.Type != branchType {
		return ErrCorrupt
	}
	_, err = decodeTreeNodeView(cached.page)
	return err
}

func (f *File) treeGetUnlocked(rootPage uint64, kind TreeKind, key []byte) ([]byte, bool, error) {
	if rootPage == 0 {
		return nil, false, nil
	}
	if !validTreeKind(kind) || len(key) == 0 {
		return nil, false, ErrCorrupt
	}
	pageID := rootPage
	leafType, branchType := treePageTypes(kind)
	for {
		cached, err := f.readDataPageUnlocked(pageID)
		if err != nil {
			return nil, false, err
		}
		page := cached.page
		if err != nil || (page.Type != leafType && page.Type != branchType) {
			return nil, false, ErrCorrupt
		}
		view, err := decodeTreeNodeView(page)
		if err != nil {
			return nil, false, err
		}
		position := sort.Search(view.count, func(index int) bool {
			candidate, _, _, _, _ := view.entry(index)
			return bytes.Compare(candidate, key) >= 0
		})
		if view.leaf {
			if position >= view.count {
				return nil, false, nil
			}
			candidate, value, _, _, err := view.entry(position)
			if err != nil {
				return nil, false, err
			}
			if !bytes.Equal(candidate, key) {
				return nil, false, nil
			}
			return append([]byte(nil), value...), true, nil
		}
		if position < view.count {
			candidate, _, _, _, err := view.entry(position)
			if err != nil {
				return nil, false, err
			}
			if bytes.Equal(candidate, key) {
				position++
			}
		}
		pageID, _, err = view.child(position)
		if err != nil {
			return nil, false, err
		}
	}
}

func decodeTreeNodeView(page Page) (treeNodeView, error) {
	payload := page.Payload
	if len(payload) < 32 || page.ItemCount > 65535 {
		return treeNodeView{}, ErrCorrupt
	}
	view := treeNodeView{
		payload: payload, count: int(binary.LittleEndian.Uint16(payload[0:2])),
		freeStart: int(binary.LittleEndian.Uint16(payload[2:4])),
		leaf:      page.Type == PageCatalogLeaf || page.Type == PagePrimaryLeaf || page.Type == PageSecondaryLeaf || page.Type == PageCommitLogLeaf || page.Type == PageIndexCatalogLeaf || page.Type == PageOrderLeaf || page.Type == PageSystemLeaf || page.Type == PageFreeSpaceLeaf || page.Type == PageIndexBuildCatalogLeaf,
	}
	freeEnd := int(binary.LittleEndian.Uint16(payload[4:6]))
	if view.count != int(page.ItemCount) || binary.LittleEndian.Uint16(payload[6:8]) != 0 || view.freeStart < 32 || freeEnd != view.freeStart || len(payload) != view.freeStart+view.count*8 {
		return treeNodeView{}, ErrCorrupt
	}
	if view.leaf {
		if binary.LittleEndian.Uint64(payload[8:16]) != 0 || binary.LittleEndian.Uint64(payload[16:24]) != 0 || binary.LittleEndian.Uint64(payload[24:32]) != uint64(view.count) {
			return treeNodeView{}, ErrCorrupt
		}
	} else {
		left, leftCount := binary.LittleEndian.Uint64(payload[8:16]), binary.LittleEndian.Uint64(payload[16:24])
		if left < 2 {
			return treeNodeView{}, ErrCorrupt
		}
		subtree := leftCount
		for index := 0; index < view.count; index++ {
			_, _, child, childCount, err := view.entry(index)
			if err != nil || child < 2 {
				return treeNodeView{}, ErrCorrupt
			}
			subtree += childCount
		}
		if subtree != binary.LittleEndian.Uint64(payload[24:32]) {
			return treeNodeView{}, ErrCorrupt
		}
	}
	var previous []byte
	cursor := 32
	for index := 0; index < view.count; index++ {
		key, _, _, _, err := view.entry(index)
		if err != nil || (index > 0 && bytes.Compare(previous, key) >= 0) {
			return treeNodeView{}, ErrCorrupt
		}
		slot := payload[len(payload)-(index+1)*8 : len(payload)-index*8]
		if int(binary.LittleEndian.Uint16(slot[0:2])) != cursor {
			return treeNodeView{}, ErrCorrupt
		}
		cursor += int(binary.LittleEndian.Uint16(slot[2:4]))
		previous = key
	}
	if cursor != view.freeStart {
		return treeNodeView{}, ErrCorrupt
	}
	return view, nil
}

func (view treeNodeView) entry(index int) (key, value []byte, child, childCount uint64, err error) {
	if index < 0 || index >= view.count {
		return nil, nil, 0, 0, ErrCorrupt
	}
	slot := view.payload[len(view.payload)-(index+1)*8 : len(view.payload)-index*8]
	offset := int(binary.LittleEndian.Uint16(slot[0:2]))
	length := int(binary.LittleEndian.Uint16(slot[2:4]))
	if binary.LittleEndian.Uint32(slot[4:8]) != 0 || offset < 32 || length < 2 || offset+length > view.freeStart {
		return nil, nil, 0, 0, ErrCorrupt
	}
	entry := view.payload[offset : offset+length]
	keyLength := int(binary.LittleEndian.Uint16(entry[:2]))
	if keyLength == 0 || keyLength > 4096 {
		return nil, nil, 0, 0, ErrCorrupt
	}
	if view.leaf {
		if len(entry) < 6 {
			return nil, nil, 0, 0, ErrCorrupt
		}
		valueLength := int(binary.LittleEndian.Uint32(entry[2:6]))
		if 6+keyLength+valueLength != len(entry) {
			return nil, nil, 0, 0, ErrCorrupt
		}
		return entry[6 : 6+keyLength], entry[6+keyLength:], 0, 0, nil
	}
	if 2+keyLength+16 != len(entry) {
		return nil, nil, 0, 0, ErrCorrupt
	}
	return entry[2 : 2+keyLength], nil,
		binary.LittleEndian.Uint64(entry[2+keyLength : 2+keyLength+8]),
		binary.LittleEndian.Uint64(entry[2+keyLength+8:]), nil
}

func (view treeNodeView) child(index int) (uint64, uint64, error) {
	if view.leaf || index < 0 || index > view.count {
		return 0, 0, ErrCorrupt
	}
	if index == 0 {
		return binary.LittleEndian.Uint64(view.payload[8:16]), binary.LittleEndian.Uint64(view.payload[16:24]), nil
	}
	_, _, child, count, err := view.entry(index - 1)
	return child, count, err
}
