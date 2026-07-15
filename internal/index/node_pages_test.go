package index

import (
	"bytes"
	"errors"
	"testing"
)

func TestIndependentlyAddressedNodePagesRoundTrip(t *testing.T) {
	tree := NewWithOrder(5)
	for number := 0; number < 1000; number++ {
		tree.Insert(integer(number), integer(number+2000))
	}
	header, pages, err := tree.EncodeNodePages()
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) < 2 {
		t.Fatalf("node pages = %d", len(pages))
	}
	for expected, page := range pages {
		id, ok := nodePageID(page)
		if !ok || id != uint32(expected) {
			t.Fatalf("page %d id=%d ok=%v", expected, id, ok)
		}
	}
	decoded, err := DecodeNodePages(header, pages)
	if err != nil {
		t.Fatal(err)
	}
	for number := 0; number < 1000; number++ {
		values := decoded.Get(integer(number))
		if len(values) != 1 || !bytes.Equal(values[0], integer(number+2000)) {
			t.Fatalf("key %d = %v", number, values)
		}
	}
}

func TestNodePagesRejectMissingAndCorruptLinks(t *testing.T) {
	tree := NewWithOrder(5)
	for number := 0; number < 100; number++ {
		tree.Insert(integer(number), integer(number))
	}
	header, pages, err := tree.EncodeNodePages()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeNodePages(header, pages[:len(pages)-1]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing node error = %v", err)
	}
	corrupt := make([][]byte, len(pages))
	for index := range pages {
		corrupt[index] = append([]byte(nil), pages[index]...)
	}
	corrupt[0][len(corrupt[0])-1] ^= 0xff
	if _, err := DecodeNodePages(header, corrupt); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt link error = %v", err)
	}
}
