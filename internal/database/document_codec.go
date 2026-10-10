package database

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"sort"
	"time"
	"unicode/utf8"
)

// encodeDocumentBinary validates a document and encodes it as binary data.
func encodeDocumentBinary(document Document) ([]byte, error) {
	if err := document.Validate(); err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(nil)
	if err := encodeObjectBinary(buffer, document, 0); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// decodeDocumentBinary reads exactly one document and rejects malformed or trailing data.
func decodeDocumentBinary(data []byte) (Document, error) {
	reader := bytes.NewReader(data)
	document, err := decodeObjectBinary(reader, 0)
	if err != nil || reader.Len() != 0 {
		return nil, ErrCorrupt
	}
	return document, nil
}

// encodeValueBinary writes a value with its type tag within the nesting limit.
func encodeValueBinary(buffer *bytes.Buffer, value Value, depth int) error {
	if depth > 64 {
		return ErrInvalidDocument
	}
	buffer.WriteByte(byte(value.kind))
	switch value.kind {
	case NullKind:
	case BoolKind:
		if value.b {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
	case Int64Kind:
		writeU64(buffer, uint64(value.i))
	case Float64Kind:
		if math.IsNaN(value.f) || math.IsInf(value.f, 0) {
			return ErrInvalidDocument
		}
		writeU64(buffer, math.Float64bits(value.f))
	case StringKind:
		return writeBytes32(buffer, []byte(value.s))
	case BinaryKind:
		return writeBytes32(buffer, value.bin)
	case TimeKind:
		writeU64(buffer, uint64(value.t.UnixMilli()))
	case IDKind:
		buffer.Write(value.id[:])
	case ArrayKind:
		if len(value.arr) > math.MaxUint32 {
			return ErrInvalidDocument
		}
		writeU32(buffer, uint32(len(value.arr)))
		for _, item := range value.arr {
			if err := encodeValueBinary(buffer, item, depth+1); err != nil {
				return err
			}
		}
	case ObjectKind:
		return encodeObjectBinary(buffer, value.obj, depth+1)
	default:
		return ErrInvalidDocument
	}
	return nil
}

// encodeObjectBinary writes object fields in sorted order within the nesting limit.
func encodeObjectBinary(buffer *bytes.Buffer, document Document, depth int) error {
	if depth > 64 || len(document) > math.MaxUint32 {
		return ErrInvalidDocument
	}
	keys := make([]string, 0, len(document))
	for key := range document {
		if err := validField(key); err != nil {
			return err
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	writeU32(buffer, uint32(len(keys)))
	for _, key := range keys {
		if err := writeString16(buffer, key); err != nil {
			return err
		}
		if err := encodeValueBinary(buffer, document[key], depth+1); err != nil {
			return err
		}
	}
	return nil
}

// decodeValueBinary reads a typed value and rejects invalid or oversized data.
func decodeValueBinary(reader *bytes.Reader, depth int) (Value, error) {
	if depth > 64 {
		return Value{}, ErrCorrupt
	}
	kind, err := reader.ReadByte()
	if err != nil || Kind(kind) > IDKind {
		return Value{}, ErrCorrupt
	}
	switch Kind(kind) {
	case NullKind:
		return Null(), nil
	case BoolKind:
		value, err := reader.ReadByte()
		if err != nil || value > 1 {
			return Value{}, ErrCorrupt
		}
		return Bool(value == 1), nil
	case Int64Kind:
		value, err := readU64(reader)
		return Int(int64(value)), corruptIf(err)
	case Float64Kind:
		bits, err := readU64(reader)
		value := math.Float64frombits(bits)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return Value{}, ErrCorrupt
		}
		return Float(value), nil
	case StringKind:
		value, err := readBytes32(reader, 64<<20)
		if err != nil || !utf8.Valid(value) {
			return Value{}, ErrCorrupt
		}
		return String(string(value)), nil
	case BinaryKind:
		value, err := readBytes32(reader, 64<<20)
		if err != nil {
			return Value{}, err
		}
		return Binary(value), nil
	case TimeKind:
		value, err := readU64(reader)
		if err != nil {
			return Value{}, ErrCorrupt
		}
		return Time(time.UnixMilli(int64(value))), nil
	case IDKind:
		var id DocumentID
		if _, err := io.ReadFull(reader, id[:]); err != nil || id.IsZero() {
			return Value{}, ErrCorrupt
		}
		return ID(id), nil
	case ArrayKind:
		count, err := readU32(reader)
		if err != nil || count > 10_000_000 || uint64(count) > uint64(reader.Len()) {
			return Value{}, ErrCorrupt
		}
		values := make([]Value, count)
		for i := range values {
			values[i], err = decodeValueBinary(reader, depth+1)
			if err != nil {
				return Value{}, err
			}
		}
		return Array(values...), nil
	case ObjectKind:
		document, err := decodeObjectBinary(reader, depth+1)
		if err != nil {
			return Value{}, err
		}
		return Object(document), nil
	}
	return Value{}, ErrCorrupt
}

// decodeObjectBinary reads an object with valid field names in strictly sorted order.
func decodeObjectBinary(reader *bytes.Reader, depth int) (Document, error) {
	if depth > 64 {
		return nil, ErrCorrupt
	}
	count, err := readU32(reader)
	if err != nil || count > 1_000_000 || uint64(count) > uint64(reader.Len()/4) {
		return nil, ErrCorrupt
	}
	document := make(Document, count)
	previous := ""
	for range count {
		key, err := readString16(reader)
		if err != nil || validField(key) != nil || (previous != "" && key <= previous) {
			return nil, ErrCorrupt
		}
		if _, exists := document[key]; exists {
			return nil, ErrCorrupt
		}
		value, err := decodeValueBinary(reader, depth+1)
		if err != nil {
			return nil, err
		}
		document[key] = value
		previous = key
	}
	return document, nil
}

// writeU16 writes an unsigned 16-bit integer in little-endian order.
func writeU16(w io.Writer, value uint16) {
	var data [2]byte
	binary.LittleEndian.PutUint16(data[:], value)
	_, _ = w.Write(data[:])
}

// writeU32 writes an unsigned 32-bit integer in little-endian order.
func writeU32(w io.Writer, value uint32) {
	var data [4]byte
	binary.LittleEndian.PutUint32(data[:], value)
	_, _ = w.Write(data[:])
}

// writeU64 writes an unsigned 64-bit integer in little-endian order.
func writeU64(w io.Writer, value uint64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], value)
	_, _ = w.Write(data[:])
}

// readU16 reads an unsigned 16-bit integer in little-endian order.
func readU16(r io.Reader) (uint16, error) {
	var data [2]byte
	_, err := io.ReadFull(r, data[:])
	return binary.LittleEndian.Uint16(data[:]), err
}

// readU32 reads an unsigned 32-bit integer in little-endian order.
func readU32(r io.Reader) (uint32, error) {
	var data [4]byte
	_, err := io.ReadFull(r, data[:])
	return binary.LittleEndian.Uint32(data[:]), err
}

// readU64 reads an unsigned 64-bit integer in little-endian order.
func readU64(r io.Reader) (uint64, error) {
	var data [8]byte
	_, err := io.ReadFull(r, data[:])
	return binary.LittleEndian.Uint64(data[:]), err
}

// writeString16 writes a string preceded by its 16-bit byte length.
func writeString16(w io.Writer, value string) error {
	if len(value) > math.MaxUint16 {
		return ErrCorrupt
	}
	writeU16(w, uint16(len(value)))
	_, err := io.WriteString(w, value)
	return err
}

// readString16 reads a string preceded by its 16-bit byte length.
func readString16(r io.Reader) (string, error) {
	length, err := readU16(r)
	if err != nil {
		return "", ErrCorrupt
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", ErrCorrupt
	}
	return string(data), nil
}

// writeBytes32 writes bytes preceded by their 32-bit length.
func writeBytes32(w io.Writer, value []byte) error {
	if len(value) > math.MaxUint32 {
		return ErrInvalidDocument
	}
	writeU32(w, uint32(len(value)))
	_, err := w.Write(value)
	return err
}

// readBytes32 reads length-prefixed bytes without exceeding the size limit or remaining input.
func readBytes32(r *bytes.Reader, max uint32) ([]byte, error) {
	length, err := readU32(r)
	if err != nil || length > max || uint64(length) > uint64(r.Len()) {
		return nil, ErrCorrupt
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, ErrCorrupt
	}
	return data, nil
}

// corruptIf maps a read error to the database corruption error.
func corruptIf(err error) error {
	if err != nil {
		return ErrCorrupt
	}
	return nil
}
