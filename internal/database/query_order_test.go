package database

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestQueryIDSortAndRangeComparisons(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	items := db.Collection("items")
	first, second, third := DocumentID{1}, DocumentID{2}, DocumentID{3}
	for _, id := range []DocumentID{third, first, second} {
		if _, err := items.InsertOne(context.Background(), Document{"_id": ID(id)}); err != nil {
			t.Fatal(err)
		}
	}

	for _, test := range []struct {
		name    string
		filter  Filter
		options QueryOptions
		want    []DocumentID
	}{
		{"ascending sort", Filter{}, QueryOptions{Sort: []SortField{{Path: "_id", Direction: 1}}}, []DocumentID{first, second, third}},
		{"descending sort", Filter{}, QueryOptions{Sort: []SortField{{Path: "_id", Direction: -1}}}, []DocumentID{third, second, first}},
		{"greater than", Filter{"_id": map[string]any{"$gt": first}}, QueryOptions{}, []DocumentID{third, second}},
		{"greater than or equal", Filter{"_id": map[string]any{"$gte": second}}, QueryOptions{}, []DocumentID{third, second}},
		{"less than", Filter{"_id": map[string]any{"$lt": third}}, QueryOptions{}, []DocumentID{first, second}},
		{"less than or equal", Filter{"_id": map[string]any{"$lte": second}}, QueryOptions{}, []DocumentID{first, second}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := queryIDs(t, items, test.filter, test.options); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ids = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSortComparisonIsTransitiveForMixedValues(t *testing.T) {
	values := []Value{
		String("z"), Int(0), String("a"), Null(), Bool(true), Time(time.UnixMilli(0)),
		ID(DocumentID{1}), Binary([]byte{1}), Array(Int(1)), Object(Document{"x": Int(1)}),
	}
	for i := range values {
		for j := range values {
			for k := range values {
				if compareSortValues(values[i], values[j]) < 0 && compareSortValues(values[j], values[k]) < 0 && compareSortValues(values[i], values[k]) >= 0 {
					t.Fatalf("non-transitive sort order: %d < %d < %d", i, j, k)
				}
			}
		}
	}
}

func TestCompileQueryRejectsDuplicateSortPaths(t *testing.T) {
	_, err := CompileQuery(Filter{}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: 1}, {Path: "rank", Direction: -1}}})
	if !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("error = %v, want ErrInvalidFilter", err)
	}
}

func TestZeroQuerySpecIsRejectedAtPublicExecutionBoundaries(t *testing.T) {
	db := New()
	t.Cleanup(func() { _ = db.Close() })
	items := db.Collection("items")
	if cursor, err := items.FindQuery(context.Background(), QuerySpec{}); cursor != nil || !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("FindQuery cursor=%v err=%v", cursor, err)
	}
	if _, err := items.SnapshotQuery(context.Background(), QuerySpec{}); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("SnapshotQuery error=%v", err)
	}
	if subscription, err := items.SubscribeQuery(context.Background(), QuerySpec{}, 1); subscription != nil || !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("SubscribeQuery subscription=%v err=%v", subscription, err)
	}
	if _, err := items.DeleteManyQuery(context.Background(), QuerySpec{}); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("DeleteManyQuery error=%v", err)
	}
	if (QuerySpec{}).Match(Document{}) || len((QuerySpec{}).Execute([]Document{{}})) != 0 {
		t.Fatal("zero query spec must never match documents")
	}
}

func TestCompileAndWireDecodeShareNestedValueLimits(t *testing.T) {
	tooManyItems := make([]Value, DefaultQueryLimits.MaxArrayItems+1)
	for index := range tooManyItems {
		tooManyItems[index] = Null()
	}
	tooDeep := Null()
	for depth := 0; depth <= DefaultQueryLimits.MaxDepth; depth++ {
		tooDeep = Array(tooDeep)
	}
	for _, test := range []struct {
		name  string
		value Value
	}{
		{"oversized scalar", String(strings.Repeat("x", DefaultQueryLimits.MaxValueBytes+1))},
		{"oversized nested array", Array(tooManyItems...)},
		{"nested value depth", tooDeep},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := CompileQuery(Filter{"value": test.value}, QueryOptions{}); !errors.Is(err, ErrInvalidFilter) {
				t.Fatalf("compile error = %v", err)
			}
			encoded, err := MarshalWireValue(test.value)
			if err != nil {
				t.Fatal(err)
			}
			query := append([]byte(`{"version":1,"where":{"op":"compare","cmp":"eq","path":"value","value":`), encoded...)
			query = append(query, []byte(`}}`)...)
			if _, err := DecodeQuerySpecJSON(query, DefaultQueryLimits); !errors.Is(err, ErrInvalidFilter) {
				t.Fatalf("decode error = %v", err)
			}
		})
	}
}

func TestQueryIDsMustBeCanonicalAndNonZero(t *testing.T) {
	for _, value := range []string{"00000000000000000000000000000000", "0000000000000000000000000000000A"} {
		if _, err := CompileQuery(Filter{"_id": value}, QueryOptions{}); !errors.Is(err, ErrInvalidFilter) {
			t.Fatalf("compile %q error = %v", value, err)
		}
		query := `{"version":1,"where":{"op":"compare","cmp":"eq","path":"_id","value":{"t":"id","v":"` + value + `"}}}`
		if _, err := DecodeQuerySpecJSON([]byte(query), DefaultQueryLimits); !errors.Is(err, ErrInvalidFilter) {
			t.Fatalf("decode %q error = %v", value, err)
		}
	}
}
