package database

import (
	"encoding/binary"
	"errors"
	"math"
)

// numericMagnitudeBytes reserves enough bytes for every finite float magnitude.
const numericMagnitudeBytes = 263

// encodeIndexKey encodes a scalar value so byte order matches index order.
func encodeIndexKey(value Value) ([]byte, error) {
	return appendIndexKey(nil, value)
}

// appendIndexKey appends an order-preserving scalar key to the destination.
func appendIndexKey(destination []byte, value Value) ([]byte, error) {
	switch value.kind {
	case NullKind:
		return append(destination, 0x10), nil
	case BoolKind:
		if value.b {
			return append(destination, 0x21), nil
		}
		return append(destination, 0x20), nil
	case Int64Kind, Float64Kind:
		return appendNumericIndexKey(destination, value)
	case StringKind:
		destination = append(destination, 0x40)
		return appendEscapedIndexBytes(destination, []byte(value.s)), nil
	case TimeKind:
		start := len(destination)
		destination = append(destination, make([]byte, 9)...)
		destination[start] = 0x50
		binary.BigEndian.PutUint64(destination[start+1:], uint64(value.t.UnixMilli())^(1<<63))
		return destination, nil
	case IDKind:
		destination = append(destination, 0x60)
		return append(destination, value.id[:]...), nil
	case BinaryKind:
		destination = append(destination, 0x70)
		return appendEscapedIndexBytes(destination, value.bin), nil
	default:
		return nil, errors.New("meldbase: value is not scalar-indexable")
	}
}

// indexKeyCapacity returns the encoded scalar key size or zero for unsupported types.
func indexKeyCapacity(value Value) int {
	switch value.kind {
	case NullKind, BoolKind:
		return 1
	case Int64Kind, Float64Kind:
		return 2 + numericMagnitudeBytes
	case StringKind:
		return 1 + escapedIndexBytesLength([]byte(value.s))
	case TimeKind:
		return 9
	case IDKind:
		return 17
	case BinaryKind:
		return 1 + escapedIndexBytesLength(value.bin)
	default:
		return 0
	}
}

// escapedIndexBytesLength counts the bytes needed to escape zeros and add a terminator.
func escapedIndexBytesLength(value []byte) int {
	length := len(value) + 2
	for _, item := range value {
		if item == 0 {
			length++
		}
	}
	return length
}

// encodeNumericIndexKey encodes an integer or float in a shared numeric order.
func encodeNumericIndexKey(value Value) ([]byte, error) {
	return appendNumericIndexKey(nil, value)
}

// appendNumericIndexKey appends an exact, order-preserving integer or float key.
func appendNumericIndexKey(destination []byte, value Value) ([]byte, error) {
	start := len(destination)
	destination = append(destination, make([]byte, 2+numericMagnitudeBytes)...)
	result := destination[start:]
	result[0] = 0x30
	negative, magnitude, shift := false, uint64(0), uint(0)
	if value.kind == Int64Kind {
		number := value.i
		negative = number < 0
		if negative {
			magnitude = uint64(-(number + 1)) + 1
		} else {
			magnitude = uint64(number)
		}
		shift = 1074
	} else {
		if math.IsNaN(value.f) || math.IsInf(value.f, 0) {
			return nil, ErrInvalidDocument
		}
		bits := math.Float64bits(value.f)
		negative = bits>>63 == 1
		exponent := int((bits >> 52) & 0x7ff)
		fraction := bits & ((1 << 52) - 1)
		if exponent == 0 {
			magnitude = fraction
		} else {
			magnitude = (1 << 52) | fraction
			shift = uint(exponent - 1)
		}
	}
	if magnitude == 0 {
		result[1] = 0x01
		return destination, nil
	}
	if !writeShiftedIndexMagnitude(result[2:], magnitude, shift) {
		return nil, errors.New("numeric index magnitude overflow")
	}
	if negative {
		result[1] = 0x00
		for i := 2; i < len(result); i++ {
			result[i] = ^result[i]
		}
	} else {
		result[1] = 0x02
	}
	return destination, nil
}

// writeShiftedIndexMagnitude writes a shifted unsigned magnitude and reports whether it fits.
func writeShiftedIndexMagnitude(destination []byte, magnitude uint64, shift uint) bool {
	if len(destination) == 0 || magnitude == 0 {
		return magnitude == 0
	}
	byteShift, bitShift := int(shift/8), uint(shift%8)
	end := len(destination) - byteShift
	if end < 8 {
		return false
	}
	binary.BigEndian.PutUint64(destination[end-8:end], magnitude<<bitShift)
	if bitShift != 0 {
		high := magnitude >> (64 - bitShift)
		if high != 0 {
			if end < 9 {
				return false
			}
			destination[end-9] = byte(high)
		}
	}
	return true
}

// escapeIndexBytes escapes zero bytes and adds an unambiguous terminator.
func escapeIndexBytes(value []byte) []byte {
	return appendEscapedIndexBytes(make([]byte, 0, len(value)+2), value)
}

// appendEscapedIndexBytes appends escaped bytes and a terminator to the destination.
func appendEscapedIndexBytes(result, value []byte) []byte {
	for _, item := range value {
		if item == 0 {
			result = append(result, 0, 0xff)
		} else {
			result = append(result, item)
		}
	}
	return append(result, 0, 0)
}
