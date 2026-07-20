package meldbase

import "errors"

const (
	maxCompoundIndexFields      = 4
	maxCanonicalSecondaryKeyLen = 4096 - 24
	compoundPartialMarker       = byte(0x01)
	compoundComponentFloor      = byte(0x02)
)

// encodeCompoundIndexKey encodes an ordered tuple into the V3 Secondary-key
// payload. Each scalar is framed independently so tuple boundaries are
// unambiguous and an encoded equality prefix is also a byte prefix. Descending
// fields use the dual framing whose lexical order is the exact reverse of the
// scalar codec, including the otherwise troublesome proper-prefix case.
func encodeCompoundIndexKey(values []Value, fields []IndexField) ([]byte, error) {
	if len(values) == 0 || len(values) != len(fields) || len(fields) > maxCompoundIndexFields {
		return nil, errors.New("meldbase: invalid compound index tuple")
	}
	scalarCapacity := 0
	for _, value := range values {
		scalarCapacity += indexKeyCapacity(value)
	}
	scalars := make([]byte, 0, scalarCapacity)
	var ends [maxCompoundIndexFields]int
	for index, field := range fields {
		if field.Order != 1 && field.Order != -1 {
			return nil, errors.New("meldbase: invalid compound index direction")
		}
		var err error
		scalars, err = appendIndexKey(scalars, values[index])
		if err != nil {
			return nil, err
		}
		ends[index] = len(scalars)
	}
	resultCapacity := 0
	for index := range fields {
		start := 0
		if index > 0 {
			start = ends[index-1]
		}
		scalar := scalars[start:ends[index]]
		resultCapacity += len(scalar) + 2
		for _, value := range scalar {
			if value == 0 {
				resultCapacity++
			}
		}
	}
	if resultCapacity > maxCanonicalSecondaryKeyLen {
		return nil, errors.New("meldbase: compound index key is too large")
	}
	result := make([]byte, 0, resultCapacity)
	for index, field := range fields {
		start := 0
		if index > 0 {
			start = ends[index-1]
		}
		scalar := scalars[start:ends[index]]
		if field.Order == 1 {
			result = appendAscendingIndexComponent(result, scalar)
		} else {
			result = appendDescendingIndexComponent(result, scalar)
		}
	}
	return result, nil
}

// encodeCompoundPartialIndexKey indexes a document's longest present left
// prefix. The marker sorts before every scalar component tag and the ID keeps
// partial tuples non-conflicting under a unique index. It is never generated
// for a missing first component.
func encodeCompoundPartialIndexKey(values []Value, fields []IndexField, id DocumentID) ([]byte, error) {
	if len(values) == 0 || len(values) != len(fields) || id.IsZero() {
		return nil, errors.New("meldbase: invalid partial compound index tuple")
	}
	key, err := encodeCompoundIndexKey(values, fields)
	if err != nil {
		return nil, err
	}
	if len(key)+1+len(id) > maxCanonicalSecondaryKeyLen {
		return nil, errors.New("meldbase: compound index key is too large")
	}
	key = append(key, compoundPartialMarker)
	return append(key, id[:]...), nil
}

func encodeCompoundIndexPrefix(values []Value, fields []IndexField) ([]byte, error) {
	if len(values) == 0 || len(values) > len(fields) {
		return nil, errors.New("meldbase: invalid compound index prefix")
	}
	return encodeCompoundIndexKey(values, fields[:len(values)])
}

func appendAscendingIndexComponent(destination, scalar []byte) []byte {
	for _, value := range scalar {
		if value == 0 {
			destination = append(destination, 0, 0xff)
		} else {
			destination = append(destination, value)
		}
	}
	return append(destination, 0, 0)
}

func appendDescendingIndexComponent(destination, scalar []byte) []byte {
	for _, value := range scalar {
		inverted := ^value
		if inverted == 0xff {
			destination = append(destination, 0xff, 0)
		} else {
			destination = append(destination, inverted)
		}
	}
	return append(destination, 0xff, 0xff)
}
