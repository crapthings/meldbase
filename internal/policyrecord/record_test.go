package policyrecord

import "testing"

func TestGenerationRecordRoundTripAndValidation(t *testing.T) {
	generation := [16]byte{15: 7}
	mutation, err := GenerationMutation("orders", generation)
	if err != nil || !mutation.Unconditional || len(mutation.Key) == 0 {
		t.Fatalf("mutation=%+v err=%v", mutation, err)
	}
	decoded, err := Decode(mutation.NewValue)
	if err != nil || decoded != generation {
		t.Fatalf("decoded=%x err=%v", decoded, err)
	}
	corrupt := append([]byte(nil), mutation.NewValue...)
	corrupt[12] = 1
	if _, err := Decode(corrupt); err == nil {
		t.Fatal("reserved-byte corruption was accepted")
	}
	if _, err := GenerationMutation("bad/name", generation); err == nil {
		t.Fatal("invalid collection was accepted")
	}
	if _, err := GenerationMutation("orders", [16]byte{}); err == nil {
		t.Fatal("zero generation was accepted")
	}
}
