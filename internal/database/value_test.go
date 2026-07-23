package database

import "testing"

func TestValueIsClosedAndCloned(t *testing.T) {
	raw := []byte{1, 2}
	v := Binary(raw)
	raw[0] = 9
	got, ok := v.BinaryValue()
	if !ok || got[0] != 1 {
		t.Fatalf("binary was not isolated: %v", got)
	}
	got[0] = 8
	again, _ := v.BinaryValue()
	if again[0] != 1 {
		t.Fatal("binary accessor leaked mutable storage")
	}
}

func TestDocumentCloneIsDeep(t *testing.T) {
	d := Document{"nested": Object(Document{"items": Array(String("a"))})}
	clone := d.Clone()
	if !d.Equal(clone) {
		t.Fatal("clone differs")
	}
	clone["nested"] = Null()
	if d["nested"].Kind() != ObjectKind {
		t.Fatal("clone mutated source")
	}
}

func TestValueOfRejectsUnsupportedAndOverflow(t *testing.T) {
	if _, err := ValueOf(struct{}{}); err == nil {
		t.Fatal("expected unsupported-type error")
	}
	if _, err := ValueOf(^uint64(0)); err == nil {
		t.Fatal("expected uint overflow error")
	}
}

func TestCrossNumericEqualityDoesNotRoundInt64(t *testing.T) {
	const beyondSafe = int64(9_007_199_254_740_993)
	if Int(beyondSafe).Equal(Float(float64(beyondSafe))) {
		t.Fatal("rounded float must not equal exact int64")
	}
	if !Int(10).Equal(Float(10)) {
		t.Fatal("equal integral values should compare equal")
	}
}
