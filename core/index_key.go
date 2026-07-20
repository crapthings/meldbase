package meldbase

import (
	"encoding/binary"
	"errors"
	"math"
)

const numericMagnitudeBytes = 263

func encodeIndexKey(value Value) ([]byte, error) {
	return appendIndexKey(nil, value)
}

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

func escapedIndexBytesLength(value []byte) int {
	length := len(value) + 2
	for _, item := range value {
		if item == 0 {
			length++
		}
	}
	return length
}

func encodeNumericIndexKey(value Value) ([]byte, error) {
	return appendNumericIndexKey(nil, value)
}

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

func escapeIndexBytes(value []byte) []byte {
	return appendEscapedIndexBytes(make([]byte, 0, len(value)+2), value)
}

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
