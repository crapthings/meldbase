package storage

import (
	"bytes"
	"encoding/binary"
)

// encodeTreeNode encodes a leaf or branch into a slotted page payload.
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

// treeNodeEncodedSize validates node structure and computes its encoded byte size.
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

// treeNodeEntryEncodedSize computes the bytes required for one leaf or branch entry.
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

// decodeTreeNode validates a stored node payload and copies its entries and child references.
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
