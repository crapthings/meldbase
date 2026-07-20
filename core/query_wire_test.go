package meldbase

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestSharedQueryConformanceCorpus(t *testing.T) {
	data, err := os.ReadFile("../testdata/query-conformance.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Version int `json:"version"`
		Cases   []struct {
			Name        string            `json:"name"`
			Documents   []json.RawMessage `json:"documents"`
			Query       json.RawMessage   `json:"query"`
			ExpectedIDs []string          `json:"expectedIds"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.Version != 1 {
		t.Fatalf("unexpected corpus version %d", corpus.Version)
	}
	for _, test := range corpus.Cases {
		t.Run(test.Name, func(t *testing.T) {
			documents := make([]Document, len(test.Documents))
			for i, raw := range test.Documents {
				value, err := decodeWireValue(raw, DefaultQueryLimits, 0)
				if err != nil {
					t.Fatalf("document %d: %v", i, err)
				}
				if value.kind != ObjectKind {
					t.Fatalf("document %d is not object", i)
				}
				documents[i] = value.obj
			}
			query, err := DecodeQuerySpecJSON(test.Query, DefaultQueryLimits)
			if err != nil {
				t.Fatal(err)
			}
			result := query.Execute(documents)
			ids := make([]string, len(result))
			for i, document := range result {
				value, ok := document["_id"]
				if !ok {
					t.Fatal("result is missing _id")
				}
				ids[i], ok = value.StringValue()
				if !ok {
					t.Fatal("fixture _id is not string")
				}
			}
			if !reflect.DeepEqual(ids, test.ExpectedIDs) {
				t.Fatalf("ids = %v, want %v", ids, test.ExpectedIDs)
			}
		})
	}
}

func TestDecodeQuerySpecRejectsAmbiguousOrUnsafeInput(t *testing.T) {
	tests := []struct{ name, query string }{
		{"duplicate key", `{"version":1,"version":1,"where":{"op":"true"}}`},
		{"unknown top field", `{"version":1,"where":{"op":"true"},"admin":true}`},
		{"unknown operator", `{"version":1,"where":{"op":"javascript","source":"true"}}`},
		{"extra operator field", `{"version":1,"where":{"op":"true","path":"secret"}}`},
		{"prototype path", `{"version":1,"where":{"op":"exists","path":"__proto__.admin","value":true}}`},
		{"dot object field", `{"version":1,"where":{"op":"compare","cmp":"eq","path":"x","value":{"t":"object","v":[["a.b",{"t":"null"}]]}}}`},
		{"noncanonical int", `{"version":1,"where":{"op":"compare","cmp":"eq","path":"x","value":{"t":"int64","v":"-0"}}}`},
		{"int overflow", `{"version":1,"where":{"op":"compare","cmp":"eq","path":"x","value":{"t":"int64","v":"9223372036854775808"}}}`},
		{"noncanonical date", `{"version":1,"where":{"op":"compare","cmp":"eq","path":"x","value":{"t":"date","v":"2026-07-15T00:00:00Z"}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeQuerySpecJSON([]byte(test.query), DefaultQueryLimits)
			if !errors.Is(err, ErrInvalidFilter) {
				t.Fatalf("error = %v, want ErrInvalidFilter", err)
			}
		})
	}
}

func TestDecodeQuerySpecEnforcesResourceLimits(t *testing.T) {
	query := []byte(`{"version":1,"where":{"op":"and","args":[{"op":"true"},{"op":"true"}]}}`)
	limits := DefaultQueryLimits
	limits.MaxNodes = 2
	if _, err := DecodeQuerySpecJSON(query, limits); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("expected node limit error, got %v", err)
	}
	limits = DefaultQueryLimits
	limits.MaxWireBytes = len(query) - 1
	if _, err := DecodeQuerySpecJSON(query, limits); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("expected wire limit error, got %v", err)
	}
}

func TestMarshalQuerySpecIsCanonicalAndRoundTrips(t *testing.T) {
	limit := 7
	query, err := CompileQuery(Filter{"$and": []Filter{{"rank": map[string]any{"$gte": int64(2)}}, {"state": map[string]any{"$in": []any{"open", "held"}}}}}, QueryOptions{Sort: []SortField{{Path: "rank", Direction: -1}}, Skip: 1, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	first, err := MarshalQuerySpecJSON(query)
	if err != nil {
		t.Fatal(err)
	}
	second, err := MarshalQuerySpecJSON(query)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("canonical bytes differ: %s / %s", first, second)
	}
	decoded, err := DecodeQuerySpecJSON(first, DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	again, err := MarshalQuerySpecJSON(decoded)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("round trip differs: %s / %s", first, again)
	}
}

func TestWireDocumentRoundTrip(t *testing.T) {
	id, err := NewDocumentID()
	if err != nil {
		t.Fatal(err)
	}
	document := Document{
		"_id": ID(id), "i": Int(9_007_199_254_740_993), "f": Float(1.5),
		"data": Binary([]byte{0, 255}), "nested": Object(Document{"ok": Bool(true)}),
	}
	data, err := MarshalWireDocument(document)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeWireValue(data, DefaultQueryLimits, 0)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.kind != ObjectKind || !document.Equal(decoded.obj) {
		t.Fatalf("round trip mismatch: %s", data)
	}
}

func TestDocumentValidationRejectsUnsafeNestedFields(t *testing.T) {
	document := Document{"safe": Object(Document{"__proto__": Bool(true)})}
	if !errors.Is(document.Validate(), ErrInvalidDocument) {
		t.Fatal("expected nested unsafe field error")
	}
}
