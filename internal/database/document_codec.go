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
func decodeDocumentBinary(data []byte) (Document, error) {
	reader := bytes.NewReader(data)
	document, err := decodeObjectBinary(reader, 0)
	if err != nil || reader.Len() != 0 {
		return nil, ErrCorrupt
	}
	return document, nil
}
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
func writeU16(w io.Writer, value uint16) {
	var data [2]byte
	binary.LittleEndian.PutUint16(data[:], value)
	_, _ = w.Write(data[:])
}
func writeU32(w io.Writer, value uint32) {
	var data [4]byte
	binary.LittleEndian.PutUint32(data[:], value)
	_, _ = w.Write(data[:])
}
func writeU64(w io.Writer, value uint64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], value)
	_, _ = w.Write(data[:])
}
func readU16(r io.Reader) (uint16, error) {
	var data [2]byte
	_, err := io.ReadFull(r, data[:])
	return binary.LittleEndian.Uint16(data[:]), err
}
func readU32(r io.Reader) (uint32, error) {
	var data [4]byte
	_, err := io.ReadFull(r, data[:])
	return binary.LittleEndian.Uint32(data[:]), err
}
func readU64(r io.Reader) (uint64, error) {
	var data [8]byte
	_, err := io.ReadFull(r, data[:])
	return binary.LittleEndian.Uint64(data[:]), err
}
func writeString16(w io.Writer, value string) error {
	if len(value) > math.MaxUint16 {
		return ErrCorrupt
	}
	writeU16(w, uint16(len(value)))
	_, err := io.WriteString(w, value)
	return err
}
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
func writeBytes32(w io.Writer, value []byte) error {
	if len(value) > math.MaxUint32 {
		return ErrInvalidDocument
	}
	writeU32(w, uint32(len(value)))
	_, err := w.Write(value)
	return err
}
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
func corruptIf(err error) error {
	if err != nil {
		return ErrCorrupt
	}
	return nil
}
