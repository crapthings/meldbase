package database

import (
	"bytes"
	"testing"
	"unicode/utf8"
)

func FuzzQueryWireRoundTrip(f *testing.F) {
	f.Add("plain")
	f.Add("上海")
	f.Add("\x00")
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 512 || !utf8.ValidString(value) {
			t.Skip()
		}
		limit := 3
		query, err := CompileQuery(Filter{
			"value": value,
			"size":  map[string]any{"$gte": int64(len(value))},
		}, QueryOptions{Sort: []SortField{{Path: "size", Direction: 1}}, Limit: &limit})
		if err != nil {
			t.Fatal(err)
		}
		first, err := MarshalQuerySpecJSON(query)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeQuerySpecJSON(first, DefaultQueryLimits)
		if err != nil {
			t.Fatal(err)
		}
		second, err := MarshalQuerySpecJSON(decoded)
		if err != nil || !bytes.Equal(first, second) {
			t.Fatalf("round trip differs: %q / %q; err=%v", first, second, err)
		}
	})
}

func FuzzScalarComparisonLaws(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3})
	f.Add([]byte("unicode"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64 {
			t.Skip()
		}
		text := string(bytes.ToValidUTF8(data, []byte("?")))
		values := []Value{
			Null(), Bool(false), Bool(true), Int(int64(len(data)) - 32), Float(float64(len(data)) / 3),
			String(text), ID(DocumentID{1}), Binary(data),
		}
		for left := range values {
			for right := range values {
				forward := compareSortValues(values[left], values[right])
				backward := compareSortValues(values[right], values[left])
				if sign(forward) != -sign(backward) {
					t.Fatalf("antisymmetry failed: %d, %d", left, right)
				}
				for third := range values {
					if forward < 0 && compareSortValues(values[right], values[third]) < 0 && compareSortValues(values[left], values[third]) >= 0 {
						t.Fatalf("transitivity failed: %d < %d < %d", left, right, third)
					}
				}
			}
		}
	})
}
