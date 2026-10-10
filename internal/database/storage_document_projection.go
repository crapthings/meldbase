package database

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"
	"unicode/utf8"
)

// projectStoredDocumentScalar validates the complete canonical document while
// materializing only one requested path.
//
// It also proves that the top-level _id
// matches the Primary record key. The path parts must already be validated.
func projectStoredDocumentScalar(encoded []byte, path [][]byte, expectedID DocumentID) (Value, bool, bool, error) {
	body, err := storedDocumentBody(encoded)
	if err != nil || len(path) == 0 || expectedID.IsZero() {
		return Value{}, false, false, ErrCorrupt
	}
	cursor := binaryProjectionCursor{data: body}
	value, found, scalar, idFound, err := cursor.object(path, 0, true, expectedID)
	if err != nil || cursor.offset != len(cursor.data) || !idFound {
		return Value{}, false, false, ErrCorrupt
	}
	return value, found, scalar, nil
}

// binaryProjectionCursor tracks a read position while validating and projecting encoded document values.
type binaryProjectionCursor struct {
	data   []byte
	offset int
}

// take reads the requested bytes and advances the cursor if they are available.
func (cursor *binaryProjectionCursor) take(length int) ([]byte, error) {
	if cursor == nil || length < 0 || cursor.offset < 0 || length > len(cursor.data)-cursor.offset {
		return nil, ErrCorrupt
	}
	result := cursor.data[cursor.offset : cursor.offset+length]
	cursor.offset += length
	return result, nil
}

// byte reads one encoded byte.
func (cursor *binaryProjectionCursor) byte() (byte, error) {
	data, err := cursor.take(1)
	if err != nil {
		return 0, err
	}
	return data[0], nil
}

// u16 reads an unsigned little-endian 16-bit integer.
func (cursor *binaryProjectionCursor) u16() (uint16, error) {
	data, err := cursor.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(data), nil
}

// u32 reads an unsigned little-endian 32-bit integer.
func (cursor *binaryProjectionCursor) u32() (uint32, error) {
	data, err := cursor.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

// u64 reads an unsigned little-endian 64-bit integer.
func (cursor *binaryProjectionCursor) u64() (uint64, error) {
	data, err := cursor.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(data), nil
}

// object validates object fields while projecting a path and optionally checking document identity.
func (cursor *binaryProjectionCursor) object(path [][]byte, depth int, requireID bool, expectedID DocumentID) (Value, bool, bool, bool, error) {
	if depth > 64 {
		return Value{}, false, false, false, ErrCorrupt
	}
	count, err := cursor.u32()
	if err != nil || count > 1_000_000 || uint64(count) > uint64((len(cursor.data)-cursor.offset)/4) {
		return Value{}, false, false, false, ErrCorrupt
	}
	var projected Value
	found, scalar, idFound := false, false, !requireID
	var previous []byte
	for range count {
		length, err := cursor.u16()
		if err != nil {
			return Value{}, false, false, false, err
		}
		field, err := cursor.take(int(length))
		if err != nil || !validEncodedField(field) || (previous != nil && bytes.Compare(previous, field) >= 0) {
			return Value{}, false, false, false, ErrCorrupt
		}
		previous = field
		if requireID && bytes.Equal(field, []byte("_id")) {
			value, isScalar, err := cursor.scalarOrSkip(depth + 1)
			id, ok := value.IDValue()
			if err != nil || !isScalar || !ok || id != expectedID {
				return Value{}, false, false, false, ErrCorrupt
			}
			idFound = true
			if bytes.Equal(field, path[0]) && len(path) == 1 {
				projected, found, scalar = value, true, true
			}
			continue
		}
		if bytes.Equal(field, path[0]) {
			projected, found, scalar, err = cursor.projectValue(path[1:], depth+1)
			if err != nil {
				return Value{}, false, false, false, err
			}
		} else if err := cursor.skipValue(depth + 1); err != nil {
			return Value{}, false, false, false, err
		}
	}
	return projected, found, scalar, idFound, nil
}

// projectValue follows a remaining field path through an encoded value.
func (cursor *binaryProjectionCursor) projectValue(remaining [][]byte, depth int) (Value, bool, bool, error) {
	kindByte, err := cursor.byte()
	kind := Kind(kindByte)
	if err != nil || kind > IDKind || depth > 64 {
		return Value{}, false, false, ErrCorrupt
	}
	if len(remaining) == 0 {
		value, scalar, err := cursor.scalarPayloadOrSkip(kind, depth)
		return value, true, scalar, err
	}
	if kind != ObjectKind {
		if err := cursor.skipPayload(kind, depth); err != nil {
			return Value{}, false, false, err
		}
		return Value{}, false, false, nil
	}
	value, found, scalar, _, err := cursor.object(remaining, depth+1, false, DocumentID{})
	return value, found, scalar, err
}

// scalarOrSkip reads a scalar value or validates and skips a container.
func (cursor *binaryProjectionCursor) scalarOrSkip(depth int) (Value, bool, error) {
	kindByte, err := cursor.byte()
	if err != nil || Kind(kindByte) > IDKind || depth > 64 {
		return Value{}, false, ErrCorrupt
	}
	return cursor.scalarPayloadOrSkip(Kind(kindByte), depth)
}

// scalarPayloadOrSkip reads scalar bytes or skips container contents for the supplied kind.
func (cursor *binaryProjectionCursor) scalarPayloadOrSkip(kind Kind, depth int) (Value, bool, error) {
	switch kind {
	case NullKind:
		return Null(), true, nil
	case BoolKind:
		value, err := cursor.byte()
		if err != nil || value > 1 {
			return Value{}, false, ErrCorrupt
		}
		return Bool(value == 1), true, nil
	case Int64Kind:
		value, err := cursor.u64()
		return Int(int64(value)), true, err
	case Float64Kind:
		bits, err := cursor.u64()
		value := math.Float64frombits(bits)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return Value{}, false, ErrCorrupt
		}
		return Float(value), true, nil
	case StringKind, BinaryKind:
		length, err := cursor.u32()
		if err != nil || length > 64<<20 {
			return Value{}, false, ErrCorrupt
		}
		data, err := cursor.take(int(length))
		if err != nil || (kind == StringKind && !utf8.Valid(data)) {
			return Value{}, false, ErrCorrupt
		}
		if kind == StringKind {
			return String(string(data)), true, nil
		}
		return Binary(data), true, nil
	case TimeKind:
		value, err := cursor.u64()
		return Time(time.UnixMilli(int64(value))), true, err
	case IDKind:
		data, err := cursor.take(16)
		var id DocumentID
		copy(id[:], data)
		if err != nil || id.IsZero() {
			return Value{}, false, ErrCorrupt
		}
		return ID(id), true, nil
	case ArrayKind, ObjectKind:
		if err := cursor.skipPayload(kind, depth); err != nil {
			return Value{}, false, err
		}
		return Value{}, false, nil
	default:
		return Value{}, false, ErrCorrupt
	}
}

// skipValue reads a value kind and validates its payload without retaining it.
func (cursor *binaryProjectionCursor) skipValue(depth int) error {
	kindByte, err := cursor.byte()
	if err != nil || Kind(kindByte) > IDKind || depth > 64 {
		return ErrCorrupt
	}
	return cursor.skipPayload(Kind(kindByte), depth)
}

// skipPayload validates and skips the bytes belonging to a value kind.
func (cursor *binaryProjectionCursor) skipPayload(kind Kind, depth int) error {
	switch kind {
	case NullKind:
		return nil
	case BoolKind:
		value, err := cursor.byte()
		if err != nil || value > 1 {
			return ErrCorrupt
		}
		return nil
	case Int64Kind, TimeKind:
		_, err := cursor.take(8)
		return err
	case Float64Kind:
		bits, err := cursor.u64()
		value := math.Float64frombits(bits)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return ErrCorrupt
		}
		return nil
	case StringKind, BinaryKind:
		length, err := cursor.u32()
		if err != nil || length > 64<<20 {
			return ErrCorrupt
		}
		data, err := cursor.take(int(length))
		if err != nil || (kind == StringKind && !utf8.Valid(data)) {
			return ErrCorrupt
		}
		return nil
	case IDKind:
		data, err := cursor.take(16)
		var id DocumentID
		copy(id[:], data)
		if err != nil || id.IsZero() {
			return ErrCorrupt
		}
		return nil
	case ArrayKind:
		count, err := cursor.u32()
		if err != nil || count > 10_000_000 || uint64(count) > uint64(len(cursor.data)-cursor.offset) {
			return ErrCorrupt
		}
		for range count {
			if err := cursor.skipValue(depth + 1); err != nil {
				return err
			}
		}
		return nil
	case ObjectKind:
		_, _, _, _, err := cursor.object([][]byte{[]byte("\x00")}, depth+1, false, DocumentID{})
		return err
	default:
		return ErrCorrupt
	}
}

// validEncodedField checks that encoded field bytes form a supported field name.
func validEncodedField(field []byte) bool {
	if len(field) == 0 || !utf8.Valid(field) || bytes.IndexByte(field, 0) >= 0 || bytes.IndexByte(field, '.') >= 0 || field[0] == '$' {
		return false
	}
	return !bytes.Equal(field, []byte("__proto__")) && !bytes.Equal(field, []byte("prototype")) && !bytes.Equal(field, []byte("constructor"))
}
