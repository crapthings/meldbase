package meldbase

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"
)

func TestStoredDocumentCodecIsCanonicalTypedAndIsolated(t *testing.T) {
	id := DocumentID{1, 2, 3}
	document := Document{
		"_id": ID(id), "null": Null(), "bool": Bool(true), "int": Int(math.MaxInt64),
		"float": Float(1.25), "string": String("世界"), "binary": Binary([]byte{1, 2, 3}),
		"time":  Time(time.UnixMilli(1_700_000_000_123)),
		"array": Array(Int(1), Object(Document{"nested": String("value")})),
	}
	encoded, err := encodeStoredDocument(document)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeStoredDocument(encoded)
	if err != nil || !decoded.Equal(document) {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	encodedAgain, err := encodeStoredDocument(Document{
		"array": document["array"], "time": document["time"], "binary": document["binary"],
		"string": document["string"], "float": document["float"], "int": document["int"],
		"bool": document["bool"], "null": document["null"], "_id": document["_id"],
	})
	if err != nil || !bytes.Equal(encoded, encodedAgain) {
		t.Fatal("map insertion order changed stored encoding")
	}
	decoded["string"] = String("mutated")
	if original, _ := document["string"].StringValue(); original != "世界" {
		t.Fatal("decoded document aliases source")
	}
}

func TestStoredDocumentCodecRejectsEnvelopeAndCanonicalBodyCorruption(t *testing.T) {
	encoded, err := encodeStoredDocument(Document{"value": String("ok")})
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 8, 10, 12, 16, 20} {
		corrupt := append([]byte(nil), encoded...)
		corrupt[offset] ^= 1
		if _, err := decodeStoredDocument(corrupt); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("offset %d error = %v", offset, err)
		}
	}
	if _, err := decodeStoredDocument(append(encoded, 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing byte error = %v", err)
	}

	body := bytes.NewBuffer(nil)
	writeU32(body, 2)
	_ = writeString16(body, "b")
	_ = encodeValueBinary(body, Null(), 0)
	_ = writeString16(body, "a")
	_ = encodeValueBinary(body, Null(), 0)
	if _, err := decodeDocumentBinary(body.Bytes()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("noncanonical key order error = %v", err)
	}
}

func TestStoredDocumentCodecRejectsInvalidUTF8AndAllocationCounts(t *testing.T) {
	invalid := string([]byte{0xff})
	for _, document := range []Document{
		{invalid: Null()},
		{"value": String(invalid)},
		{"value": Array(String(invalid))},
	} {
		if _, err := encodeStoredDocument(document); !errors.Is(err, ErrInvalidDocument) {
			t.Fatalf("invalid UTF-8 document error = %v", err)
		}
	}

	object := make([]byte, 4)
	binary.LittleEndian.PutUint32(object, 1_000_000)
	if _, err := decodeDocumentBinary(object); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("impossible object count error = %v", err)
	}
	array := make([]byte, 5)
	array[0] = byte(ArrayKind)
	binary.LittleEndian.PutUint32(array[1:], 10_000_000)
	if _, err := decodeValueBinary(bytes.NewReader(array), 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("impossible array count error = %v", err)
	}
}

func TestStoredDocumentProjectionValidatesWholeDocumentAndMaterializesOnlyPath(t *testing.T) {
	id := DocumentID{7}
	document := Document{
		"_id":     ID(id),
		"ignored": Object(Document{"array": Array(String("large"), Int(2)), "flag": Bool(true)}),
		"nested":  Object(Document{"value": String("wanted")}),
		"plain":   Int(9),
	}
	encoded, err := encodeStoredDocument(document)
	if err != nil {
		t.Fatal(err)
	}
	value, found, scalar, err := projectStoredDocumentScalar(encoded, [][]byte{[]byte("nested"), []byte("value")}, id)
	actual, ok := value.StringValue()
	if err != nil || !found || !scalar || !ok || actual != "wanted" {
		t.Fatalf("projection value=%+v found=%t scalar=%t err=%v", value, found, scalar, err)
	}
	if _, found, scalar, err := projectStoredDocumentScalar(encoded, [][]byte{[]byte("missing")}, id); err != nil || found || scalar {
		t.Fatalf("missing found=%t scalar=%t err=%v", found, scalar, err)
	}
	if _, found, scalar, err := projectStoredDocumentScalar(encoded, [][]byte{[]byte("nested")}, id); err != nil || !found || scalar {
		t.Fatalf("compound found=%t scalar=%t err=%v", found, scalar, err)
	}
	if _, _, _, err := projectStoredDocumentScalar(encoded, [][]byte{[]byte("plain"), []byte("child")}, id); err != nil {
		t.Fatalf("scalar intermediate err=%v", err)
	}
	if _, _, _, err := projectStoredDocumentScalar(encoded, [][]byte{[]byte("plain")}, DocumentID{8}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mismatched id err=%v", err)
	}

	corrupt := append([]byte(nil), encoded...)
	pattern := []byte{4, 0, 'f', 'l', 'a', 'g', byte(BoolKind), 1}
	offset := bytes.Index(corrupt, pattern)
	if offset < 0 {
		t.Fatal("bool payload not found")
	}
	corrupt[offset+len(pattern)-1] = 2
	if _, _, _, err := projectStoredDocumentScalar(corrupt, [][]byte{[]byte("plain")}, id); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unselected corruption err=%v", err)
	}
}

func TestStoredDocumentProjectionPreservesEveryScalarKind(t *testing.T) {
	id := DocumentID{9}
	document := Document{
		"_id": ID(id), "null": Null(), "bool": Bool(true), "int": Int(math.MinInt64),
		"float": Float(-1.25), "string": String("世界"), "binary": Binary([]byte{0, 1, 2}),
		"time": Time(time.UnixMilli(1_700_000_000_123)), "id": ID(DocumentID{8}),
	}
	encoded, err := encodeStoredDocument(document)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"_id", "null", "bool", "int", "float", "string", "binary", "time", "id"} {
		value, found, scalar, err := projectStoredDocumentScalar(encoded, [][]byte{[]byte(field)}, id)
		if err != nil || !found || !scalar || !value.Equal(document[field]) {
			t.Fatalf("field=%s value=%+v found=%t scalar=%t err=%v", field, value, found, scalar, err)
		}
	}
}

func FuzzStoredDocumentDecoderNeverPanics(f *testing.F) {
	seed, _ := encodeStoredDocument(Document{"value": String("seed")})
	f.Add(seed)
	f.Add([]byte("not-a-document"))
	f.Fuzz(func(t *testing.T, encoded []byte) {
		_, _ = decodeStoredDocument(encoded)
		_, _, _, _ = projectStoredDocumentScalar(encoded, [][]byte{[]byte("value")}, DocumentID{1})
	})
}

func BenchmarkStoredDocumentIndexProjection(b *testing.B) {
	id := DocumentID{1}
	encoded, err := encodeStoredDocument(Document{
		"_id": ID(id), "payload": String(string(bytes.Repeat([]byte{'x'}, 1024))),
		"nested": Object(Document{"value": Int(42), "ignored": Array(Int(1), Int(2), Int(3))}),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("FullDecode", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			document, err := decodeStoredDocument(encoded)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkValue, _ = lookupInternal(document, "nested.value")
		}
	})
	b.Run("Projected", func(b *testing.B) {
		path := [][]byte{[]byte("nested"), []byte("value")}
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			value, found, scalar, err := projectStoredDocumentScalar(encoded, path, id)
			if err != nil || !found || !scalar {
				b.Fatalf("found=%t scalar=%t err=%v", found, scalar, err)
			}
			benchmarkValue = value
		}
	})
}

var benchmarkValue Value
