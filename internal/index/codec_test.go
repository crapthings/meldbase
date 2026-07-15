package index

import (
	"bytes"
	"errors"
	"testing"
)

func TestTreeNodeTopologyRoundTripsWithoutRebuild(t *testing.T) {
	tree := NewWithOrder(5)
	for number := 0; number < 500; number++ {
		if !tree.Insert(integer(number), integer(number+1000)) {
			t.Fatalf("insert %d", number)
		}
	}
	encoded, err := tree.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Len() != tree.Len() {
		t.Fatalf("decoded len = %d", decoded.Len())
	}
	for number := 0; number < 500; number++ {
		if got := decoded.Get(integer(number)); len(got) != 1 || !bytes.Equal(got[0], integer(number+1000)) {
			t.Fatalf("decoded key %d = %v", number, got)
		}
	}
	if !decoded.Insert(integer(700), integer(1700)) || !decoded.Delete(integer(10), integer(1010)) {
		t.Fatal("decoded tree is not mutable")
	}
}

func TestTreeCodecRejectsCorruptTopology(t *testing.T) {
	tree := NewWithOrder(5)
	for number := 0; number < 20; number++ {
		tree.Insert(integer(number), integer(number))
	}
	encoded, err := tree.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 0xff
	if _, err := Decode(encoded); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corruption error = %v", err)
	}
}
