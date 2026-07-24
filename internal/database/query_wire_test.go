package database

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestSharedQueryConformanceCorpus(t *testing.T) {
	data, err := os.ReadFile("../../testdata/query-conformance.json")
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
		{"duplicate sort path", `{"version":1,"where":{"op":"true"},"sort":[{"path":"rank","direction":1},{"path":"rank","direction":-1}]}`},
		{"fractional size", `{"version":1,"where":{"op":"size","path":"items","size":1.5}}`},
		{"negative size", `{"version":1,"where":{"op":"size","path":"items","size":-1}}`},
		{"scalar wire type", `{"version":1,"where":{"op":"type","path":"value","types":"int64"}}`},
		{"empty wire types", `{"version":1,"where":{"op":"type","path":"value","types":[]}}`},
		{"unknown wire type", `{"version":1,"where":{"op":"type","path":"value","types":["number"]}}`},
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

func TestCompileSizeAndTypeCanonicalizesAndRejectsInvalidOperands(t *testing.T) {
	query, err := CompileQuery(Filter{
		"items": map[string]any{"$size": uint8(2)},
		"value": map[string]any{"$type": []any{"object", "int64", "int64"}},
	}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := query.FilterCapabilities(), []FilterCapability{
		{Path: "items", Operator: "size"},
		{Path: "value", Operator: "type"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("capabilities=%+v want=%+v", got, want)
	}
	encoded, err := MarshalQuerySpecJSON(query)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"version":1,"where":{"args":[{"op":"size","path":"items","size":2},{"op":"type","path":"value","types":["int64","object"]}],"op":"and"}}`
	if string(encoded) != want {
		t.Fatalf("canonical query=%s want=%s", encoded, want)
	}
	decoded, err := DecodeQuerySpecJSON([]byte(`{"version":1,"where":{"op":"type","path":"value","types":["object","int64","int64"]}}`), DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := MarshalQuerySpecJSON(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != `{"version":1,"where":{"op":"type","path":"value","types":["int64","object"]}}` {
		t.Fatalf("canonical decoded type=%s", canonical)
	}

	invalid := []Filter{
		{"items": map[string]any{"$size": -1}},
		{"items": map[string]any{"$size": float64(1)}},
		{"items": map[string]any{"$size": uint64(maxQueryArraySize + 1)}},
		{"value": map[string]any{"$type": []string{}}},
		{"value": map[string]any{"$type": "number"}},
		{"value": map[string]any{"$type": []any{"int64", 1}}},
	}
	for index, filter := range invalid {
		if _, err := CompileQuery(filter, QueryOptions{}); !errors.Is(err, ErrInvalidFilter) {
			t.Fatalf("invalid filter %d error=%v", index, err)
		}
	}
}

func TestCompileAllCanonicalizesAndRejectsInvalidOperands(t *testing.T) {
	query, err := CompileQuery(Filter{"tags": map[string]any{"$all": []any{"one", "two", "one"}}}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := query.FilterCapabilities(), []FilterCapability{{Path: "tags", Operator: "all"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("capabilities=%+v want=%+v", got, want)
	}
	encoded, err := MarshalQuerySpecJSON(query)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"version":1,"where":{"op":"all","path":"tags","values":[{"t":"string","v":"one"},{"t":"string","v":"two"}]}}`; got != want {
		t.Fatalf("canonical query=%s want=%s", got, want)
	}
	decoded, err := DecodeQuerySpecJSON([]byte(`{"version":1,"where":{"op":"all","path":"tags","values":[{"t":"string","v":"one"},{"t":"string","v":"two"},{"t":"string","v":"one"}]}}`), DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := MarshalQuerySpecJSON(decoded)
	if err != nil || string(canonical) != string(encoded) {
		t.Fatalf("canonical decoded all=%s err=%v", canonical, err)
	}
	invalid := []Filter{
		{"tags": map[string]any{"$all": []any{}}},
		{"tags": map[string]any{"$all": "one"}},
	}
	for index, filter := range invalid {
		if _, err := CompileQuery(filter, QueryOptions{}); !errors.Is(err, ErrInvalidFilter) {
			t.Fatalf("invalid filter %d error=%v", index, err)
		}
	}
	if _, err := DecodeQuerySpecJSON([]byte(`{"version":1,"where":{"op":"all","path":"tags","values":[]}}`), DefaultQueryLimits); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("empty all wire error=%v", err)
	}
}

func TestElemMatchWireRoundTripAndInvalidOperands(t *testing.T) {
	query, err := CompileQuery(Filter{
		"scores": map[string]any{"$elemMatch": map[string]any{"$gte": int64(90), "$lt": int64(100)}},
		"parts":  map[string]any{"$elemMatch": map[string]any{"kind": "a", "qty": map[string]any{"$gte": int64(5)}}},
	}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalQuerySpecJSON(query)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeQuerySpecJSON(encoded, DefaultQueryLimits)
	if err != nil {
		t.Fatal(err)
	}
	again, err := MarshalQuerySpecJSON(decoded)
	if err != nil || !reflect.DeepEqual(encoded, again) {
		t.Fatalf("elem match round trip=%s / %s err=%v", encoded, again, err)
	}
	invalid := []Filter{
		{"items": map[string]any{"$elemMatch": map[string]any{}}},
		{"items": map[string]any{"$elemMatch": map[string]any{"$gte": int64(1), "kind": "a"}}},
		{"items": map[string]any{"$elemMatch": map[string]any{"$where": "no"}}},
	}
	for index, filter := range invalid {
		if _, err := CompileQuery(filter, QueryOptions{}); !errors.Is(err, ErrInvalidFilter) {
			t.Fatalf("invalid elem match %d error=%v", index, err)
		}
	}
	invalidWire := []byte(`{"version":1,"where":{"op":"elem_match","path":"items","mode":"scalar","arg":{"op":"compare","cmp":"gte","path":"forbidden","value":{"t":"number","v":1}}}}`)
	if _, err := DecodeQuerySpecJSON(invalidWire, DefaultQueryLimits); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("invalid scalar elem match wire error=%v", err)
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
