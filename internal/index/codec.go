package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

var treeMagic = [8]byte{'M', 'E', 'L', 'D', 'B', 'T', 'R', '1'}

var ErrCorrupt = errors.New("meldbase index: corrupt tree")

const noNode = math.MaxUint32

// MarshalBinary persists the actual node topology. It does not flatten the
// tree to entries and rebuild it on open.
func (t *Tree) MarshalBinary() ([]byte, error) {
	if t == nil || t.root == nil || t.maxKeys < 3 {
		return nil, ErrCorrupt
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
		return nil, err
	}
	buffer := bytes.NewBuffer(nil)
	buffer.Write(treeMagic[:])
	write16(buffer, 1)
	write16(buffer, uint16(t.maxKeys))
	write64(buffer, uint64(t.size))
	write32(buffer, uint32(len(nodes)))
	write32(buffer, ids[t.root])
	for id, current := range nodes {
		write32(buffer, uint32(id))
		if current.leaf {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
		if len(current.keys) > math.MaxUint16 {
			return nil, ErrCorrupt
		}
		write16(buffer, uint16(len(current.keys)))
		for _, key := range current.keys {
			if err := writeBytes(buffer, key); err != nil {
				return nil, err
			}
		}
		if current.leaf {
			if len(current.values) != len(current.keys) || len(current.children) != 0 {
				return nil, ErrCorrupt
			}
			for _, values := range current.values {
				if len(values) == 0 || len(values) > math.MaxUint32 {
					return nil, ErrCorrupt
				}
				write32(buffer, uint32(len(values)))
				for _, value := range values {
					if err := writeBytes(buffer, value); err != nil {
						return nil, err
					}
				}
			}
			next := uint32(noNode)
			if current.next != nil {
				var ok bool
				next, ok = ids[current.next]
				if !ok {
					return nil, ErrCorrupt
				}
			}
			write32(buffer, next)
			continue
		}
		if len(current.values) != 0 || len(current.children) != len(current.keys)+1 || len(current.children) > math.MaxUint16 {
			return nil, ErrCorrupt
		}
		write16(buffer, uint16(len(current.children)))
		for _, child := range current.children {
			childID, ok := ids[child]
			if !ok {
				return nil, ErrCorrupt
			}
			write32(buffer, childID)
		}
	}
	return buffer.Bytes(), nil
}

func Decode(data []byte) (*Tree, error) {
	reader := bytes.NewReader(data)
	magic := make([]byte, 8)
	if _, err := io.ReadFull(reader, magic); err != nil || !bytes.Equal(magic, treeMagic[:]) {
		return nil, ErrCorrupt
	}
	version, err := read16(reader)
	if err != nil || version != 1 {
		return nil, ErrCorrupt
	}
	maxKeys, err := read16(reader)
	if err != nil || maxKeys < 3 {
		return nil, ErrCorrupt
	}
	size, err := read64(reader)
	if err != nil || size > math.MaxInt {
		return nil, ErrCorrupt
	}
	count, err := read32(reader)
	if err != nil || count == 0 || count > 10_000_000 {
		return nil, ErrCorrupt
	}
	rootID, err := read32(reader)
	if err != nil || rootID >= count {
		return nil, ErrCorrupt
	}
	nodes := make([]*node, count)
	childIDs := make([][]uint32, count)
	nextIDs := make([]uint32, count)
	for index := range nextIDs {
		nextIDs[index] = noNode
	}
	for index := uint32(0); index < count; index++ {
		id, err := read32(reader)
		if err != nil || id != index {
			return nil, ErrCorrupt
		}
		leaf, err := reader.ReadByte()
		if err != nil || leaf > 1 {
			return nil, ErrCorrupt
		}
		keyCount, err := read16(reader)
		if err != nil || int(keyCount) > int(maxKeys) {
			return nil, ErrCorrupt
		}
		current := &node{leaf: leaf == 1, keys: make([][]byte, keyCount)}
		for keyIndex := range current.keys {
			current.keys[keyIndex], err = readBytes(reader)
			if err != nil || len(current.keys[keyIndex]) == 0 ||
				(keyIndex > 0 && bytes.Compare(current.keys[keyIndex-1], current.keys[keyIndex]) >= 0) {
				return nil, ErrCorrupt
			}
		}
		if current.leaf {
			current.values = make([][][]byte, keyCount)
			for keyIndex := range current.values {
				valueCount, err := read32(reader)
				if err != nil || valueCount == 0 || valueCount > 10_000_000 {
					return nil, ErrCorrupt
				}
				current.values[keyIndex] = make([][]byte, valueCount)
				for valueIndex := range current.values[keyIndex] {
					value, err := readBytes(reader)
					if err != nil || (valueIndex > 0 && bytes.Compare(current.values[keyIndex][valueIndex-1], value) >= 0) {
						return nil, ErrCorrupt
					}
					current.values[keyIndex][valueIndex] = value
				}
			}
			next, err := read32(reader)
			if err != nil || (next != noNode && next >= count) {
				return nil, ErrCorrupt
			}
			nextIDs[index] = next
		} else {
			children, err := read16(reader)
			if err != nil || int(children) != int(keyCount)+1 || children < 2 {
				return nil, ErrCorrupt
			}
			childIDs[index] = make([]uint32, children)
			for childIndex := range childIDs[index] {
				childID, err := read32(reader)
				if err != nil || childID >= count {
					return nil, ErrCorrupt
				}
				childIDs[index][childIndex] = childID
			}
		}
		nodes[index] = current
	}
	if reader.Len() != 0 {
		return nil, ErrCorrupt
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
	tree := &Tree{root: nodes[rootID], maxKeys: int(maxKeys), size: int(size)}
	if err := validateDecodedTree(tree, nodes); err != nil {
		return nil, err
	}
	return tree, nil
}

func validateDecodedTree(tree *Tree, all []*node) error {
	visiting, visited := map[*node]bool{}, map[*node]bool{}
	leaves := []*node{}
	pairs := 0
	var walk func(*node, int) (int, error)
	walk = func(current *node, depth int) (int, error) {
		if visiting[current] || visited[current] {
			return 0, ErrCorrupt
		}
		visiting[current] = true
		defer delete(visiting, current)
		visited[current] = true
		if current.leaf {
			leaves = append(leaves, current)
			for _, values := range current.values {
				pairs += len(values)
			}
			return depth, nil
		}
		leafDepth := -1
		for index, child := range current.children {
			depthFound, err := walk(child, depth+1)
			if err != nil || (leafDepth >= 0 && depthFound != leafDepth) {
				return 0, ErrCorrupt
			}
			leafDepth = depthFound
			if index > 0 {
				minimum, ok := decodedMinKey(child)
				if !ok || !bytes.Equal(current.keys[index-1], minimum) {
					return 0, ErrCorrupt
				}
			}
		}
		return leafDepth, nil
	}
	if _, err := walk(tree.root, 0); err != nil || len(visited) != len(all) || pairs != tree.size {
		return ErrCorrupt
	}
	for index, leaf := range leaves {
		expected := (*node)(nil)
		if index+1 < len(leaves) {
			expected = leaves[index+1]
		}
		if leaf.next != expected {
			return ErrCorrupt
		}
	}
	return nil
}

func decodedMinKey(current *node) ([]byte, bool) {
	seen := map[*node]struct{}{}
	for current != nil && !current.leaf {
		if _, duplicate := seen[current]; duplicate || len(current.children) == 0 {
			return nil, false
		}
		seen[current] = struct{}{}
		current = current.children[0]
	}
	if current == nil || len(current.keys) == 0 {
		return nil, false
	}
	return current.keys[0], true
}

func writeBytes(buffer *bytes.Buffer, value []byte) error {
	if len(value) > 64<<20 {
		return fmt.Errorf("%w: value too large", ErrCorrupt)
	}
	write32(buffer, uint32(len(value)))
	buffer.Write(value)
	return nil
}

func readBytes(reader *bytes.Reader) ([]byte, error) {
	length, err := read32(reader)
	if err != nil || length > 64<<20 || uint64(length) > uint64(reader.Len()) {
		return nil, ErrCorrupt
	}
	value := make([]byte, length)
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, ErrCorrupt
	}
	return value, nil
}

func write16(buffer *bytes.Buffer, value uint16) {
	_ = binary.Write(buffer, binary.LittleEndian, value)
}
func write32(buffer *bytes.Buffer, value uint32) {
	_ = binary.Write(buffer, binary.LittleEndian, value)
}
func write64(buffer *bytes.Buffer, value uint64) {
	_ = binary.Write(buffer, binary.LittleEndian, value)
}
func read16(reader *bytes.Reader) (uint16, error) {
	var v uint16
	err := binary.Read(reader, binary.LittleEndian, &v)
	return v, err
}
func read32(reader *bytes.Reader) (uint32, error) {
	var v uint32
	err := binary.Read(reader, binary.LittleEndian, &v)
	return v, err
}
func read64(reader *bytes.Reader) (uint64, error) {
	var v uint64
	err := binary.Read(reader, binary.LittleEndian, &v)
	return v, err
}
