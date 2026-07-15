package index

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

var nodePageMagic = [8]byte{'M', 'E', 'L', 'D', 'I', 'D', 'X', 'N'}

type TreePageHeader struct {
	MaxKeys uint16
	Size    uint64
	Root    uint32
}

// EncodeNodePages returns one independently addressable blob per logical
// B+Tree node. Child and leaf-next links are stable node ordinals resolved by
// the checkpoint catalog.
func (t *Tree) EncodeNodePages() (TreePageHeader, [][]byte, error) {
	if t == nil || t.root == nil || t.maxKeys < 3 || t.maxKeys > math.MaxUint16 || t.size < 0 {
		return TreePageHeader{}, nil, ErrCorrupt
	}
	nodes := []*node{}
	ids := map[*node]uint32{}
	var visit func(*node) error
	visit = func(current *node) error {
		if current == nil {
			return ErrCorrupt
		}
		if _, exists := ids[current]; exists {
			return nil
		}
		if len(nodes) == math.MaxUint32 {
			return ErrCorrupt
		}
		ids[current] = uint32(len(nodes))
		nodes = append(nodes, current)
		for _, child := range current.children {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(t.root); err != nil {
		return TreePageHeader{}, nil, err
	}
	encoded := make([][]byte, len(nodes))
	for id, current := range nodes {
		buffer := bytes.NewBuffer(nil)
		buffer.Write(nodePageMagic[:])
		write16(buffer, 1)
		write32(buffer, uint32(id))
		if current.leaf {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
		if len(current.keys) > t.maxKeys || len(current.keys) > math.MaxUint16 {
			return TreePageHeader{}, nil, ErrCorrupt
		}
		write16(buffer, uint16(len(current.keys)))
		for _, key := range current.keys {
			if len(key) == 0 {
				return TreePageHeader{}, nil, ErrCorrupt
			}
			if err := writeBytes(buffer, key); err != nil {
				return TreePageHeader{}, nil, err
			}
		}
		if current.leaf {
			if len(current.values) != len(current.keys) || len(current.children) != 0 {
				return TreePageHeader{}, nil, ErrCorrupt
			}
			for _, values := range current.values {
				if len(values) == 0 || len(values) > math.MaxUint32 {
					return TreePageHeader{}, nil, ErrCorrupt
				}
				write32(buffer, uint32(len(values)))
				for _, value := range values {
					if err := writeBytes(buffer, value); err != nil {
						return TreePageHeader{}, nil, err
					}
				}
			}
			next := uint32(noNode)
			if current.next != nil {
				var ok bool
				next, ok = ids[current.next]
				if !ok {
					return TreePageHeader{}, nil, ErrCorrupt
				}
			}
			write32(buffer, next)
		} else {
			if len(current.values) != 0 || len(current.children) != len(current.keys)+1 || len(current.children) < 2 || len(current.children) > math.MaxUint16 {
				return TreePageHeader{}, nil, ErrCorrupt
			}
			write16(buffer, uint16(len(current.children)))
			for _, child := range current.children {
				childID, ok := ids[child]
				if !ok {
					return TreePageHeader{}, nil, ErrCorrupt
				}
				write32(buffer, childID)
			}
		}
		encoded[id] = buffer.Bytes()
	}
	return TreePageHeader{MaxKeys: uint16(t.maxKeys), Size: uint64(t.size), Root: ids[t.root]}, encoded, nil
}

func DecodeNodePages(header TreePageHeader, encoded [][]byte) (*Tree, error) {
	if header.MaxKeys < 3 || header.Size > math.MaxInt || len(encoded) == 0 || len(encoded) > 10_000_000 || header.Root >= uint32(len(encoded)) {
		return nil, ErrCorrupt
	}
	nodes := make([]*node, len(encoded))
	childIDs := make([][]uint32, len(encoded))
	nextIDs := make([]uint32, len(encoded))
	for index := range nextIDs {
		nextIDs[index] = noNode
	}
	for expectedID, data := range encoded {
		reader := bytes.NewReader(data)
		magic := make([]byte, 8)
		if _, err := io.ReadFull(reader, magic); err != nil || !bytes.Equal(magic, nodePageMagic[:]) {
			return nil, ErrCorrupt
		}
		version, err := read16(reader)
		if err != nil || version != 1 {
			return nil, ErrCorrupt
		}
		id, err := read32(reader)
		if err != nil || id != uint32(expectedID) {
			return nil, ErrCorrupt
		}
		leaf, err := reader.ReadByte()
		if err != nil || leaf > 1 {
			return nil, ErrCorrupt
		}
		keyCount, err := read16(reader)
		if err != nil || keyCount > header.MaxKeys {
			return nil, ErrCorrupt
		}
		current := &node{leaf: leaf == 1, keys: make([][]byte, keyCount)}
		for keyIndex := range current.keys {
			key, err := readBytes(reader)
			if err != nil || len(key) == 0 || (keyIndex > 0 && bytes.Compare(current.keys[keyIndex-1], key) >= 0) {
				return nil, ErrCorrupt
			}
			current.keys[keyIndex] = key
		}
		if current.leaf {
			current.values = make([][][]byte, keyCount)
			for keyIndex := range current.values {
				count, err := read32(reader)
				if err != nil || count == 0 || count > 10_000_000 {
					return nil, ErrCorrupt
				}
				current.values[keyIndex] = make([][]byte, count)
				for valueIndex := range current.values[keyIndex] {
					value, err := readBytes(reader)
					if err != nil || (valueIndex > 0 && bytes.Compare(current.values[keyIndex][valueIndex-1], value) >= 0) {
						return nil, ErrCorrupt
					}
					current.values[keyIndex][valueIndex] = value
				}
			}
			next, err := read32(reader)
			if err != nil || (next != noNode && next >= uint32(len(encoded))) {
				return nil, ErrCorrupt
			}
			nextIDs[expectedID] = next
		} else {
			count, err := read16(reader)
			if err != nil || int(count) != int(keyCount)+1 || count < 2 {
				return nil, ErrCorrupt
			}
			childIDs[expectedID] = make([]uint32, count)
			for childIndex := range childIDs[expectedID] {
				childID, err := read32(reader)
				if err != nil || childID >= uint32(len(encoded)) {
					return nil, ErrCorrupt
				}
				childIDs[expectedID][childIndex] = childID
			}
		}
		if reader.Len() != 0 {
			return nil, ErrCorrupt
		}
		nodes[expectedID] = current
	}
	for index, current := range nodes {
		for _, childID := range childIDs[index] {
			current.children = append(current.children, nodes[childID])
		}
		if nextIDs[index] != noNode {
			current.next = nodes[nextIDs[index]]
			if !current.leaf || !current.next.leaf {
				return nil, ErrCorrupt
			}
		}
	}
	tree := &Tree{root: nodes[header.Root], maxKeys: int(header.MaxKeys), size: int(header.Size)}
	if err := validateDecodedTree(tree, nodes); err != nil {
		return nil, err
	}
	return tree, nil
}

func nodePageID(data []byte) (uint32, bool) {
	if len(data) < 14 || !bytes.Equal(data[:8], nodePageMagic[:]) || binary.LittleEndian.Uint16(data[8:10]) != 1 {
		return 0, false
	}
	return binary.LittleEndian.Uint32(data[10:14]), true
}
