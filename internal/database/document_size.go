package database

import (
	"math"
	"unicode/utf8"
)

// canonicalDocumentSize returns the exact size produced by
// encodeDocumentBinary without allocating the encoded byte slice.
func canonicalDocumentSize(document Document) (uint64, error) {
	return canonicalObjectSize(document, 0)
}

// canonicalObjectSize validates an object and counts its canonical encoded bytes.
func canonicalObjectSize(document Document, depth int) (uint64, error) {
	if depth > 64 || uint64(len(document)) > math.MaxUint32 {
		return 0, ErrInvalidDocument
	}
	total := uint64(4)
	for key, value := range document {
		if err := validField(key); err != nil {
			return 0, err
		}
		if len(key) > math.MaxUint16 {
			return 0, ErrInvalidDocument
		}
		size, err := canonicalValueSize(value, depth+1)
		if err != nil {
			return 0, err
		}
		add := uint64(2 + len(key))
		if add > math.MaxUint64-total || size > math.MaxUint64-total-add {
			return 0, ErrInvalidDocument
		}
		total += add + size
	}
	return total, nil
}

// canonicalValueSize validates a typed value and counts its canonical encoded bytes.
func canonicalValueSize(value Value, depth int) (uint64, error) {
	if depth > 64 {
		return 0, ErrInvalidDocument
	}
	switch value.kind {
	case NullKind:
		return 1, nil
	case BoolKind:
		return 2, nil
	case Int64Kind, TimeKind:
		return 9, nil
	case Float64Kind:
		if math.IsNaN(value.f) || math.IsInf(value.f, 0) {
			return 0, ErrInvalidDocument
		}
		return 9, nil
	case IDKind:
		return 17, nil
	case StringKind:
		if !utf8.ValidString(value.s) || uint64(len(value.s)) > math.MaxUint32 {
			return 0, ErrInvalidDocument
		}
		return 5 + uint64(len(value.s)), nil
	case BinaryKind:
		if uint64(len(value.bin)) > math.MaxUint32 {
			return 0, ErrInvalidDocument
		}
		return 5 + uint64(len(value.bin)), nil
	case ObjectKind:
		size, err := canonicalObjectSize(value.obj, depth+1)
		return size + 1, err
	case ArrayKind:
		if uint64(len(value.arr)) > math.MaxUint32 {
			return 0, ErrInvalidDocument
		}
		total := uint64(5)
		for _, item := range value.arr {
			size, err := canonicalValueSize(item, depth+1)
			if err != nil {
				return 0, err
			}
			if size > math.MaxUint64-total {
				return 0, ErrInvalidDocument
			}
			total += size
		}
		return total, nil
	default:
		return 0, ErrInvalidDocument
	}
}
