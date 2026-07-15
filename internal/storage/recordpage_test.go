package storage

import (
	"bytes"
	"errors"
	"testing"
)

func TestRecordPageRoundTripUpdateAndGenerationSafeReuse(t *testing.T) {
	page := NewRecordPage(42)
	first, err := page.Insert([]byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := page.Insert([]byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if err := page.Update(second, []byte("second-expanded")); err != nil {
		t.Fatal(err)
	}
	if err := page.Delete(first); err != nil {
		t.Fatal(err)
	}
	reused, err := page.Insert([]byte("replacement"))
	if err != nil {
		t.Fatal(err)
	}
	if reused.Slot != first.Slot || reused.Generation == first.Generation {
		t.Fatalf("slot was not generation-safe: old=%+v new=%+v", first, reused)
	}
	if _, err := page.Get(first); !errors.Is(err, ErrStaleRecordID) {
		t.Fatalf("stale lookup error = %v", err)
	}

	encoded, err := page.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRecordPage(encoded, 42)
	if err != nil {
		t.Fatal(err)
	}
	value, err := decoded.Get(second)
	if err != nil || !bytes.Equal(value, []byte("second-expanded")) {
		t.Fatalf("decoded second = %q err=%v", value, err)
	}
	value, err = decoded.Get(reused)
	if err != nil || !bytes.Equal(value, []byte("replacement")) {
		t.Fatalf("decoded replacement = %q err=%v", value, err)
	}
}

func TestRecordPageRejectsCorruptionAndOversize(t *testing.T) {
	page := NewRecordPage(7)
	if _, err := page.Insert(make([]byte, PageSize)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversize error = %v", err)
	}
	if _, err := page.Insert([]byte("safe")); err != nil {
		t.Fatal(err)
	}
	encoded, err := page.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	encoded[recordPageHeaderSize] ^= 0xff
	if _, err := DecodeRecordPage(encoded, 7); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corruption error = %v", err)
	}
}
