package database

import (
	"bytes"
	"math"
	"sort"
	"testing"
	"time"
)

func TestIndexNumericBytesMatchLogicalOrderExactly(t *testing.T) {
	values := []Value{Float(-math.MaxFloat64), Int(math.MinInt64), Float(-1.5), Int(-1), Float(math.Copysign(0, -1)), Int(0), Float(0), Float(math.SmallestNonzeroFloat64), Int(1), Float(1.5), Int(9_007_199_254_740_993), Float(9_007_199_254_740_994), Int(math.MaxInt64), Float(math.MaxFloat64)}
	for i := range values {
		left, err := encodeIndexKey(values[i])
		if err != nil {
			t.Fatal(err)
		}
		for j := range values {
			right, err := encodeIndexKey(values[j])
			if err != nil {
				t.Fatal(err)
			}
			logical, comparable := compareValues(values[i], values[j])
			if !comparable {
				t.Fatalf("not comparable: %d %d", i, j)
			}
			encoded := bytes.Compare(left, right)
			if sign(encoded) != sign(logical) {
				t.Fatalf("order mismatch i=%d j=%d encoded=%d logical=%d", i, j, encoded, logical)
			}
		}
	}
	intKey, _ := encodeIndexKey(Int(10))
	floatKey, _ := encodeIndexKey(Float(10))
	if !bytes.Equal(intKey, floatKey) {
		t.Fatal("numerically equal values need identical unique-index key")
	}
}

func TestIndexScalarOrderingAndStringEscaping(t *testing.T) {
	values := []Value{
		Null(), Bool(false), Bool(true), Int(-1), Float(1.5), String(""),
		String("a"), String("a\x00"), String("aa"), String("\uE000"), String("\U00010000"),
		Time(time.UnixMilli(0)), ID(DocumentID{1}), Binary(nil), Binary([]byte{0, 1}),
	}
	keys := make([][]byte, len(values))
	for i, value := range values {
		key, err := encodeIndexKey(value)
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = key
	}
	if !sort.SliceIsSorted(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 }) {
		t.Fatalf("keys not ordered")
	}
	for i := range values {
		for j := range values {
			logical, comparable := compareValues(values[i], values[j])
			if !comparable {
				t.Fatalf("not comparable: %d %d", i, j)
			}
			if encoded := bytes.Compare(keys[i], keys[j]); sign(encoded) != sign(logical) {
				t.Fatalf("order mismatch i=%d j=%d encoded=%d logical=%d", i, j, encoded, logical)
			}
		}
	}
}
func sign(value int) int {
	if value < 0 {
		return -1
	}
	if value > 0 {
		return 1
	}
	return 0
}
