package meldbase

import (
	"encoding/binary"
	"errors"
	"math"
	"math/big"
)

const numericMagnitudeBytes = 263

func encodeIndexKey(value Value) ([]byte, error) {
	switch value.kind {
	case NullKind:
		return []byte{0x10}, nil
	case BoolKind:
		if value.b {
			return []byte{0x21}, nil
		}
		return []byte{0x20}, nil
	case Int64Kind, Float64Kind:
		return encodeNumericIndexKey(value)
	case StringKind:
		return append([]byte{0x40}, escapeIndexBytes([]byte(value.s))...), nil
	case TimeKind:
		result := make([]byte, 9)
		result[0] = 0x50
		binary.BigEndian.PutUint64(result[1:], uint64(value.t.UnixMilli())^(1<<63))
		return result, nil
	case IDKind:
		return append([]byte{0x60}, value.id[:]...), nil
	case BinaryKind:
		return append([]byte{0x70}, escapeIndexBytes(value.bin)...), nil
	default:
		return nil, errors.New("meldbase: value is not scalar-indexable")
	}
}

func encodeNumericIndexKey(value Value) ([]byte, error) {
	result := make([]byte, 2+numericMagnitudeBytes)
	result[0] = 0x30
	magnitude := new(big.Int)
	negative := false
	if value.kind == Int64Kind {
		number := value.i
		negative = number < 0
		magnitude.SetInt64(number)
		magnitude.Abs(magnitude)
		magnitude.Lsh(magnitude, 1074)
	} else {
		if math.IsNaN(value.f) || math.IsInf(value.f, 0) {
			return nil, ErrInvalidDocument
		}
		bits := math.Float64bits(value.f)
		negative = bits>>63 == 1
		exponent := int((bits >> 52) & 0x7ff)
		fraction := bits & ((1 << 52) - 1)
		if exponent == 0 {
			magnitude.SetUint64(fraction)
		} else {
			magnitude.SetUint64((1 << 52) | fraction)
			magnitude.Lsh(magnitude, uint(exponent-1))
		}
	}
	if magnitude.Sign() == 0 {
		result[1] = 0x01
		return result, nil
	}
	bytes := magnitude.Bytes()
	if len(bytes) > numericMagnitudeBytes {
		return nil, errors.New("numeric index magnitude overflow")
	}
	copy(result[len(result)-len(bytes):], bytes)
	if negative {
		result[1] = 0x00
		for i := 2; i < len(result); i++ {
			result[i] = ^result[i]
		}
	} else {
		result[1] = 0x02
	}
	return result, nil
}

func escapeIndexBytes(value []byte) []byte {
	result := make([]byte, 0, len(value)+2)
	for _, item := range value {
		if item == 0 {
			result = append(result, 0, 0xff)
		} else {
			result = append(result, item)
		}
	}
	return append(result, 0, 0)
}
