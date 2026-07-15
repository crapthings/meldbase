package meldbase

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
)

func TestSharedMutationConformanceCorpus(t *testing.T) {
	data, err := os.ReadFile("testdata/mutation-conformance.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Version int `json:"version"`
		Cases   []struct {
			Name                         string `json:"name"`
			Document, Mutation, Expected json.RawMessage
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.Version != 1 {
		t.Fatalf("version = %d", corpus.Version)
	}
	for _, item := range corpus.Cases {
		t.Run(item.Name, func(t *testing.T) {
			document, err := UnmarshalWireDocument(item.Document, DefaultQueryLimits)
			if err != nil {
				t.Fatal(err)
			}
			mutation, err := DecodeMutationSpecJSON(item.Mutation, DefaultQueryLimits)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := mutation.Apply(document)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := UnmarshalWireDocument(item.Expected, DefaultQueryLimits)
			if err != nil {
				t.Fatal(err)
			}
			if !actual.Equal(expected) {
				t.Fatalf("actual = %#v, expected = %#v", actual, expected)
			}
		})
	}
}

func TestDecodeMutationRejectsAmbiguousUnsafeAndMalformedInput(t *testing.T) {
	tests := []string{
		`{"version":1,"version":1,"operations":[{"op":"unset","path":"x"}]}`,
		`{"version":1,"operations":[{"op":"unset","path":"_id"}]}`,
		`{"version":1,"operations":[{"op":"set","path":"profile","value":{"t":"object","v":[]}},{"op":"set","path":"profile.city","value":{"t":"string","v":"x"}}]}`,
		`{"version":1,"operations":[{"op":"javascript","path":"x","value":{"t":"string","v":"evil"}}]}`,
		`{"version":1,"operations":[{"op":"inc","path":"x","value":{"t":"string","v":"not numeric"}}]}`,
		`{"version":1,"operations":[{"op":"unset","path":"x","value":{"t":"null"}}]}`,
	}
	for _, raw := range tests {
		if _, err := DecodeMutationSpecJSON([]byte(raw), DefaultQueryLimits); !errors.Is(err, ErrInvalidUpdate) && !errors.Is(err, ErrImmutableID) {
			t.Fatalf("input %s error = %v", raw, err)
		}
	}
}

func TestMixedNumericIncrementRejectsInt64PrecisionLoss(t *testing.T) {
	mutation, err := CompileUpdate(Update{"$inc": map[string]any{"n": 0.5}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = mutation.Apply(Document{"_id": ID(mustID(t)), "n": Int(9_007_199_254_740_993)})
	if !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("error = %v", err)
	}
}
