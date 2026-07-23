package database

import (
	"bytes"
	"sort"
	"testing"
	"time"
)

func TestCompoundIndexKeyPreservesMixedTupleOrderAndPrefixes(t *testing.T) {
	values := []Value{
		Null(), Bool(false), Bool(true), Int(-1), Float(-0.5), Int(0), Float(0), Int(1),
		String(""), String("a"), String("a\x00"), String("aa"),
		Time(time.UnixMilli(0)), Binary(nil), Binary([]byte{0}), Binary([]byte{0, 1}),
		ID(DocumentID{1}),
	}
	for _, direction := range []int{1, -1} {
		fields := []IndexField{{Field: "left", Order: direction}, {Field: "right", Order: direction}}
		type tuple struct {
			values []Value
			key    []byte
		}
		tuples := make([]tuple, 0, len(values)*len(values))
		for _, left := range values {
			for _, right := range values {
				key, err := encodeCompoundIndexKey([]Value{left, right}, fields)
				if err != nil {
					t.Fatal(err)
				}
				tuples = append(tuples, tuple{values: []Value{left, right}, key: key})
			}
		}
		sort.Slice(tuples, func(left, right int) bool { return bytes.Compare(tuples[left].key, tuples[right].key) < 0 })
		for index := 1; index < len(tuples); index++ {
			comparison := compareIndexTuple(tuples[index-1].values, tuples[index].values, fields)
			if comparison > 0 || (comparison == 0 && !bytes.Equal(tuples[index-1].key, tuples[index].key)) {
				t.Fatalf("direction=%d tuple order %d comparison=%d", direction, index, comparison)
			}
		}
		prefix, err := encodeCompoundIndexPrefix([]Value{String("a\x00")}, fields)
		if err != nil {
			t.Fatal(err)
		}
		complete, err := encodeCompoundIndexKey([]Value{String("a\x00"), Binary([]byte{0, 1})}, fields)
		if err != nil || !bytes.HasPrefix(complete, prefix) {
			t.Fatalf("direction=%d prefix=%x complete=%x err=%v", direction, prefix, complete, err)
		}
	}
}

func TestCompoundIndexKeySupportsMixedDirectionsAndRejectsInvalidTuples(t *testing.T) {
	fields := []IndexField{{Field: "group", Order: 1}, {Field: "score", Order: -1}}
	tuples := [][]Value{{String("a"), Int(1)}, {String("a"), Int(3)}, {String("b"), Int(2)}}
	keys := make([][]byte, len(tuples))
	for index := range tuples {
		var err error
		keys[index], err = encodeCompoundIndexKey(tuples[index], fields)
		if err != nil {
			t.Fatal(err)
		}
	}
	if bytes.Compare(keys[1], keys[0]) >= 0 || bytes.Compare(keys[0], keys[2]) >= 0 {
		t.Fatalf("mixed direction keys=%x %x %x", keys[0], keys[1], keys[2])
	}
	for _, test := range []struct {
		values []Value
		fields []IndexField
	}{
		{nil, nil},
		{[]Value{Int(1)}, fields},
		{[]Value{Int(1)}, []IndexField{{Field: "x", Order: 0}}},
		{[]Value{Array()}, []IndexField{{Field: "x", Order: 1}}},
		{[]Value{Int(1), Int(2), Int(3), Int(4), Int(5)}, []IndexField{{Order: 1}, {Order: 1}, {Order: 1}, {Order: 1}, {Order: 1}}},
	} {
		if _, err := encodeCompoundIndexKey(test.values, test.fields); err == nil {
			t.Fatalf("accepted invalid tuple values=%+v fields=%+v", test.values, test.fields)
		}
	}
}

func TestCompoundPartialIndexKeysRemainInPrefixAndDoNotConflict(t *testing.T) {
	fields := []IndexField{{Field: "workspace", Order: 1}}
	prefix, err := encodeCompoundIndexPrefix([]Value{String("a")}, fields)
	if err != nil {
		t.Fatal(err)
	}
	first, err := encodeCompoundPartialIndexKey([]Value{String("a")}, fields, DocumentID{1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeCompoundPartialIndexKey([]Value{String("a")}, fields, DocumentID{2})
	if err != nil {
		t.Fatal(err)
	}
	full, err := encodeCompoundIndexKey([]Value{String("a"), Int(1)}, []IndexField{{Field: "workspace", Order: 1}, {Field: "score", Order: 1}})
	if err != nil || !bytes.HasPrefix(first, prefix) || !bytes.HasPrefix(second, prefix) || !bytes.HasPrefix(full, prefix) ||
		bytes.Equal(first, second) || bytes.Compare(first, full) >= 0 {
		t.Fatalf("prefix=%x first=%x second=%x full=%x err=%v", prefix, first, second, full, err)
	}
}

func compareIndexTuple(left, right []Value, fields []IndexField) int {
	for index, field := range fields {
		leftKey, leftErr := encodeIndexKey(left[index])
		rightKey, rightErr := encodeIndexKey(right[index])
		if leftErr != nil || rightErr != nil {
			panic("test tuple is not scalar")
		}
		comparison := bytes.Compare(leftKey, rightKey)
		if comparison != 0 {
			return comparison * field.Order
		}
	}
	return 0
}
